defmodule Ferricstore.Flow.Query.IndexLifecycleWorker do
  @moduledoc false

  use GenServer

  require Logger

  alias Ferricstore.Flow.Query.{BackfillSource, CompositeBackfill, RegisteredIndex}

  alias Ferricstore.Flow.Query.{
    AdmissionController,
    IndexRegistry,
    IndexRetirement,
    IndexValidation
  }

  @default_initial_delay_ms 1_000
  @default_interval_ms 1_000
  @default_catchup_delay_ms 10
  @default_snapshot_items 8
  @default_backfill_items 8
  @default_max_bytes 8 * 1_024 * 1_024
  @default_shard_concurrency 4
  @max_page_items 16
  @max_page_bytes 16 * 1_024 * 1_024
  @max_shard_concurrency 8
  @max_storage_key_bytes 511
  @max_projection_entries_per_record 128
  @max_projection_ops_per_record 258
  @call_timeout 30_000

  @spec name(map() | atom()) :: atom()
  def name(%{name: instance_name}), do: name(instance_name)
  def name(:default), do: __MODULE__

  def name(instance_name) when is_atom(instance_name),
    do: :"#{instance_name}.Flow.Query.IndexLifecycleWorker"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, name(ctx)))
  end

  @spec run_once(GenServer.server()) :: {:ok, atom()} | {:error, term()}
  def run_once(server), do: GenServer.call(server, :run_once, @call_timeout)

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    max_bytes = bounded_positive_opt(opts, :max_bytes, @default_max_bytes, @max_page_bytes)

    project_fun =
      Keyword.get_lazy(opts, :project_fun, fn ->
        fn instance_ctx, shard_index, records, definitions ->
          CompositeBackfill.project_page(instance_ctx, shard_index, records, definitions,
            max_operation_bytes: max_bytes
          )
        end
      end)

    state = %{
      instance_ctx: ctx,
      registry: Keyword.get(opts, :registry, IndexRegistry.server_name(ctx)),
      interval_ms: nonnegative_opt(opts, :interval_ms, @default_interval_ms),
      catchup_delay_ms: nonnegative_opt(opts, :catchup_delay_ms, @default_catchup_delay_ms),
      snapshot_items:
        bounded_positive_opt(opts, :snapshot_items, @default_snapshot_items, @max_page_items),
      backfill_items:
        bounded_positive_opt(opts, :backfill_items, @default_backfill_items, @max_page_items),
      shard_concurrency:
        bounded_positive_opt(
          opts,
          :shard_concurrency,
          @default_shard_concurrency,
          @max_shard_concurrency
        ),
      max_bytes: max_bytes,
      pressure_fun: Keyword.get(opts, :pressure_fun, &Ferricstore.OperationalGuard.pressure?/0),
      ready_fun: Keyword.get(opts, :ready_fun, &backend_ready?/2),
      snapshot_page_fun: Keyword.get(opts, :snapshot_page_fun, &BackfillSource.snapshot_page/5),
      page_fun: Keyword.get(opts, :page_fun, &BackfillSource.page/7),
      project_fun: project_fun,
      cleanup_fun: Keyword.get(opts, :cleanup_fun, &BackfillSource.cleanup/3),
      barrier_fun: Keyword.get(opts, :barrier_fun, &projection_barrier/2),
      drain_fun: Keyword.get(opts, :drain_fun, &queries_drained?/2),
      manages_admission_fence?: not Keyword.has_key?(opts, :drain_fun),
      validation_fun: Keyword.get(opts, :validation_fun, &IndexValidation.step/7),
      retirement_fun: Keyword.get(opts, :retirement_fun, &IndexRetirement.step/6),
      timer_ref: nil,
      timer_token: nil
    }

    with :ok <- validate_context(ctx),
         :ok <- validate_functions(state) do
      state =
        if Keyword.get(opts, :auto_run?, true) do
          schedule(
            state,
            nonnegative_opt(opts, :initial_delay_ms, @default_initial_delay_ms)
          )
        else
          state
        end

      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:run_once, _from, state) do
    {:reply, run_step(state), state}
  end

  @impl true
  def handle_info({:run, token}, %{timer_token: token} = state) do
    state = %{state | timer_ref: nil, timer_token: nil}
    result = run_step(state)

    if match?({:error, _reason}, result) do
      Logger.warning("Flow query index lifecycle step failed: #{reason_code(result)}")
    end

    delay = if progress?(result), do: state.catchup_delay_ms, else: state.interval_ms
    {:noreply, schedule(state, delay)}
  end

  def handle_info({:run, _stale_token}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    cancel_timer(state.timer_ref)
    :ok
  end

  defp run_step(state) do
    do_run_step(state)
  rescue
    _error -> {:error, :query_index_lifecycle_callback_failed}
  catch
    :exit, _reason -> {:error, :query_index_lifecycle_dependency_unavailable}
    _kind, _reason -> {:error, :query_index_lifecycle_callback_failed}
  end

  defp do_run_step(state) do
    with {:ok, snapshot} <- FerricStore.Flow.QueryIndexProvider.snapshot(state.instance_ctx, 0) do
      indexes = snapshot.indexes
      building? = Enum.any?(indexes, &match?(%RegisteredIndex{state: :building}, &1))
      validating? = Enum.any?(indexes, &match?(%RegisteredIndex{state: :validating}, &1))
      retiring? = Enum.any?(indexes, &(&1.state in [:retiring, :failed]))
      pressure? = (building? or validating?) and safe_pressure?(state.pressure_fun)

      cond do
        pressure? and retiring? ->
          run_retirement_step(state, indexes)

        pressure? ->
          {:ok, :pressure_paused}

        building? ->
          run_build_step(state, indexes)

        validating? ->
          run_validation_step(state, indexes)

        retiring? ->
          run_retirement_step(state, indexes)

        true ->
          {:ok, :idle}
      end
    end
  end

  defp run_build_step(state, indexes) do
    building = Enum.filter(indexes, &match?(%RegisteredIndex{state: :building}, &1))

    with {:ok, build_id, build_indexes} <- select_build(building),
         {:ok, build} <- IndexRegistry.build_status(state.registry, build_id),
         {:ok, shards} <- select_build_shards(build, state) do
      case shards do
        :complete ->
          {:ok, :idle}

        :backend_not_ready ->
          {:ok, :backend_not_ready}

        shards ->
          run_shard_batch(state, shards, fn {shard_index, checkpoint} ->
            run_phase(state, build_id, build_indexes, shard_index, checkpoint)
          end)
      end
    else
      {:error, :query_index_build_not_found} -> {:ok, :idle}
      {:error, _reason} = error -> error
    end
  end

  defp select_build([]), do: {:ok, []}

  defp select_build(indexes) do
    selected = Enum.min_by(indexes, fn index -> {index.build_id, index.definition.id} end)
    build_indexes = Enum.filter(indexes, &(&1.build_id == selected.build_id))
    {:ok, selected.build_id, build_indexes}
  end

  defp select_build_shards(%{checkpoints: checkpoints}, state) when is_map(checkpoints),
    do: select_ready_shards(checkpoints, state, &empty_checkpoint/0)

  defp select_build_shards(_build, _state),
    do: {:error, :invalid_query_index_build_status}

  defp run_phase(
         state,
         build_id,
         _indexes,
         shard_index,
         %{phase: :snapshot, fenced: false} = checkpoint
       ) do
    with :ok <- state.barrier_fun.(state.instance_ctx, shard_index),
         progress <- checkpoint_progress(checkpoint, phase: :snapshot, cursor: "", fenced: true),
         :ok <- IndexRegistry.checkpoint_build(state.registry, build_id, shard_index, progress) do
      {:ok, :build_fenced}
    end
  end

  defp run_phase(
         state,
         build_id,
         _indexes,
         shard_index,
         %{phase: :snapshot, fenced: true} = checkpoint
       ) do
    case state.snapshot_page_fun.(
           state.instance_ctx,
           shard_index,
           build_id,
           state.snapshot_items,
           state.max_bytes
         ) do
      {:ok, page} ->
        with :ok <- validate_snapshot_page(page, state.snapshot_items),
             %{done?: done?, scanned_keys: scanned_keys} <- page do
          progress =
            checkpoint_progress(checkpoint,
              phase: if(done?, do: :backfill, else: :snapshot),
              cursor: "",
              fenced: true,
              scanned_records: checkpoint.scanned_records + scanned_keys
            )

          with :ok <-
                 IndexRegistry.checkpoint_build(state.registry, build_id, shard_index, progress) do
            if done?, do: {:ok, :snapshot_complete}, else: {:ok, :snapshot_progress}
          end
        else
          {:error, _reason} = error -> error
          _invalid -> {:error, :invalid_query_backfill_snapshot_result}
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_query_backfill_snapshot_result}
    end
  end

  defp run_phase(state, build_id, indexes, shard_index, %{phase: :backfill} = checkpoint) do
    with {:ok, page} <-
           state.page_fun.(
             state.instance_ctx,
             shard_index,
             build_id,
             checkpoint.cursor,
             state.backfill_items,
             state.max_bytes,
             []
           ),
         :ok <-
           validate_page(
             page,
             checkpoint.cursor,
             state.backfill_items,
             state.max_bytes
           ),
         {:ok, metrics} <-
           state.project_fun.(
             state.instance_ctx,
             shard_index,
             page.records,
             Enum.map(indexes, & &1.definition)
           ),
         :ok <- validate_metrics(metrics, length(page.records), state.max_bytes) do
      progress =
        checkpoint_progress(checkpoint,
          phase: if(page.done?, do: :done, else: :backfill),
          cursor: if(page.done?, do: "", else: page.cursor),
          fenced: true,
          scanned_records: checkpoint.scanned_records + page.scanned_entries,
          written_entries: checkpoint.written_entries + metrics.written_entries,
          written_bytes: checkpoint.written_bytes + metrics.written_bytes
        )

      if page.done? do
        with :ok <-
               IndexRegistry.complete_build_shard(
                 state.registry,
                 build_id,
                 shard_index,
                 progress
               ) do
          {:ok, :shard_complete}
        end
      else
        with :ok <-
               IndexRegistry.checkpoint_build(state.registry, build_id, shard_index, progress) do
          {:ok, :backfill_progress}
        end
      end
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_query_backfill_result}
    end
  end

  defp run_phase(_state, _build_id, _indexes, _shard_index, %{phase: :done}),
    do: {:ok, :idle}

  defp run_phase(_state, _build_id, _indexes, _shard_index, _checkpoint),
    do: {:error, :invalid_query_index_checkpoint}

  defp run_validation_step(state, indexes) do
    validating = Enum.filter(indexes, &match?(%RegisteredIndex{state: :validating}, &1))

    with {:ok, build_id, build_indexes} <- select_build(validating),
         {:ok, build} <- IndexRegistry.build_status(state.registry, build_id) do
      case select_validation_shards(build, state) do
        {:ok, :complete} ->
          case IndexRegistry.activate_build(state.registry, build_id) do
            :ok -> {:ok, :build_activated}
            {:error, _reason} = error -> error
          end

        {:ok, :backend_not_ready} ->
          {:ok, :backend_not_ready}

        {:ok, shards} ->
          run_shard_batch(state, shards, fn {shard_index, checkpoint} ->
            run_validation_phase(
              state,
              build_id,
              build_indexes,
              shard_index,
              checkpoint
            )
          end)

        {:error, _reason} = error ->
          error
      end
    else
      {:error, :query_index_build_not_found} -> {:ok, :idle}
      {:error, _reason} = error -> error
    end
  end

  defp select_validation_shards(%{validation_checkpoints: checkpoints}, state)
       when is_map(checkpoints),
       do: select_ready_shards(checkpoints, state, &empty_validation_checkpoint/0)

  defp select_validation_shards(_build, _state),
    do: {:error, :invalid_query_index_validation_status}

  defp run_validation_phase(
         state,
         build_id,
         _indexes,
         shard_index,
         %{phase: :source, fenced: false} = checkpoint
       ) do
    progress = %{checkpoint | fenced: true}

    with :ok <- state.barrier_fun.(state.instance_ctx, shard_index),
         :ok <-
           IndexRegistry.checkpoint_validation(
             state.registry,
             build_id,
             shard_index,
             Map.to_list(progress)
           ) do
      {:ok, :validation_fenced}
    end
  end

  defp run_validation_phase(
         state,
         build_id,
         indexes,
         shard_index,
         %{phase: phase, fenced: true} = checkpoint
       )
       when phase in [:source, :index, :counter] do
    definitions = Enum.map(indexes, & &1.definition)

    case state.validation_fun.(
           state.instance_ctx,
           shard_index,
           build_id,
           definitions,
           checkpoint,
           state.backfill_items,
           state.max_bytes
         ) do
      {:ok, %{phase: next_phase} = next}
      when next_phase in [:source, :index, :counter, :cleanup] ->
        with :ok <-
               IndexRegistry.checkpoint_validation(
                 state.registry,
                 build_id,
                 shard_index,
                 Map.to_list(next)
               ) do
          {:ok, :validation_progress}
        end

      {:retry, :query_index_validation_concurrent_change} ->
        {:ok, :validation_retry}

      {:restart, :query_index_validation_concurrent_change} ->
        with :ok <-
               IndexRegistry.restart_validation_shard(
                 state.registry,
                 build_id,
                 shard_index
               ) do
          {:ok, :validation_restarted}
        end

      {:mismatch, evidence} when is_map(evidence) ->
        with :ok <-
               IndexRegistry.validation_failed(state.registry, build_id, Map.to_list(evidence)) do
          {:ok, :validation_failed}
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_query_index_validation_result}
    end
  end

  defp run_validation_phase(
         state,
         build_id,
         _indexes,
         shard_index,
         %{phase: :cleanup, fenced: true}
       ) do
    case state.cleanup_fun.(state.instance_ctx, shard_index, build_id) do
      :ok ->
        with :ok <- IndexRegistry.complete_validation_shard(state.registry, build_id, shard_index) do
          {:ok, :validation_shard_complete}
        end

      {:ok, :progress} ->
        {:ok, :validation_cleanup_progress}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_query_index_cleanup_result}
    end
  end

  defp run_validation_phase(_state, _build_id, _indexes, _shard_index, _checkpoint),
    do: {:error, :invalid_query_index_validation_checkpoint}

  defp empty_validation_checkpoint do
    %{
      phase: :source,
      cursor: "",
      fenced: false,
      definition_position: 0,
      checked_records: 0,
      checked_entries: 0,
      mismatches: 0,
      counter_runs: []
    }
  end

  defp run_retirement_step(state, indexes) do
    candidates =
      indexes
      |> Enum.filter(&(&1.state in [:retiring, :failed]))
      |> Enum.sort_by(&{&1.definition.id, &1.definition.version})

    with {:ok, index, status} <- select_retirement(state.registry, candidates),
         {:ok, shards} <- select_retirement_shards(status, state) do
      case shards do
        :complete ->
          {:ok, :idle}

        :backend_not_ready ->
          {:ok, :backend_not_ready}

        shards ->
          run_shard_batch(state, shards, fn {shard_index, checkpoint} ->
            run_retirement_phase(state, index, shard_index, checkpoint)
          end)
      end
    else
      {:error, :query_index_not_found} -> {:ok, :idle}
      {:error, _reason} = error -> error
    end
  end

  defp select_retirement(_registry, []), do: {:ok, :complete}

  defp select_retirement(registry, [index | rest]) do
    case IndexRegistry.status(registry, index.definition.id, index.definition.version) do
      {:ok, %{retirement: %{status: :pending}} = status} ->
        {:ok, index, status}

      {:ok, %{retirement: %{status: :complete}}} ->
        select_retirement(registry, rest)

      {:ok, _invalid} ->
        {:error, :invalid_query_index_retirement_status}

      {:error, :query_index_not_found} ->
        select_retirement(registry, rest)

      {:error, _reason} = error ->
        error
    end
  end

  defp select_retirement_shards(
         %{retirement: %{checkpoints: checkpoints}},
         state
       )
       when is_map(checkpoints),
       do: select_ready_shards(checkpoints, state, &empty_retirement_checkpoint/0)

  defp select_retirement_shards(_status, _state),
    do: {:error, :invalid_query_index_retirement_status}

  defp run_retirement_phase(state, index, shard_index, %{phase: :fence} = checkpoint) do
    if safe_drain?(state.drain_fun, state.instance_ctx, index) do
      progress = %{checkpoint | phase: :index, cursor: ""}

      with :ok <- state.barrier_fun.(state.instance_ctx, shard_index),
           :ok <-
             IndexRegistry.checkpoint_retirement(
               state.registry,
               index.definition.id,
               index.definition.version,
               shard_index,
               Map.to_list(progress)
             ) do
        {:ok, :retirement_fenced}
      end
    else
      {:ok, :retirement_waiting_for_queries}
    end
  end

  defp run_retirement_phase(state, index, shard_index, %{phase: phase} = checkpoint)
       when phase in [:index, :counter, :reverse] do
    case state.retirement_fun.(
           state.instance_ctx,
           shard_index,
           index.definition,
           checkpoint,
           state.backfill_items,
           state.max_bytes
         ) do
      {:ok, %{phase: next_phase} = next} when next_phase in [:index, :counter, :reverse] ->
        with :ok <-
               IndexRegistry.checkpoint_retirement(
                 state.registry,
                 index.definition.id,
                 index.definition.version,
                 shard_index,
                 Map.to_list(next)
               ) do
          {:ok, :retirement_progress}
        end

      {:complete, %{phase: :reverse} = next} ->
        cleanup = %{next | phase: :cleanup, cursor: ""}

        with :ok <-
               IndexRegistry.checkpoint_retirement(
                 state.registry,
                 index.definition.id,
                 index.definition.version,
                 shard_index,
                 Map.to_list(cleanup)
               ) do
          {:ok, :retirement_cleanup_pending}
        end

      {:retry, :query_index_retirement_concurrent_change} ->
        {:ok, :retirement_retry}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_query_index_retirement_result}
    end
  end

  defp run_retirement_phase(
         state,
         index,
         shard_index,
         %{phase: :cleanup}
       ) do
    case state.cleanup_fun.(state.instance_ctx, shard_index, index.build_id) do
      :ok ->
        with {:ok, completion} <-
               IndexRegistry.complete_retirement_shard(
                 state.registry,
                 index.definition.id,
                 index.definition.version,
                 shard_index
               ),
             :ok <- release_retirement_fence(state, index, completion) do
          {:ok, :retirement_shard_complete}
        end

      {:ok, :progress} ->
        {:ok, :retirement_cleanup_progress}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_query_index_cleanup_result}
    end
  end

  defp run_retirement_phase(_state, _index, _shard_index, _checkpoint),
    do: {:error, :invalid_query_index_retirement_checkpoint}

  defp empty_retirement_checkpoint do
    %{
      phase: :fence,
      cursor: "",
      deleted_entries: 0,
      deleted_bytes: 0,
      rewritten_reverse_rows: 0
    }
  end

  defp validate_snapshot_page(
         %{done?: done?, scanned_keys: scanned_keys, staged_states: staged_states},
         max_items
       )
       when is_boolean(done?) and is_integer(scanned_keys) and scanned_keys >= 0 and
              is_integer(staged_states) and staged_states >= 0 and is_integer(max_items) and
              max_items > 0 and max_items <= @max_page_items do
    cond do
      scanned_keys > max_items -> {:error, :invalid_query_backfill_snapshot_result}
      staged_states > scanned_keys -> {:error, :invalid_query_backfill_snapshot_result}
      not done? and scanned_keys == 0 -> {:error, :query_backfill_snapshot_made_no_progress}
      true -> :ok
    end
  end

  defp validate_snapshot_page(_page, _max_items),
    do: {:error, :invalid_query_backfill_snapshot_result}

  defp validate_page(
         %{
           records: records,
           cursor: cursor,
           done?: done?,
           scanned_entries: scanned_entries,
           hydrated_bytes: hydrated_bytes
         },
         previous_cursor,
         max_items,
         max_bytes
       )
       when is_list(records) and is_binary(cursor) and
              byte_size(cursor) <= @max_storage_key_bytes and is_boolean(done?) and
              is_integer(scanned_entries) and scanned_entries >= 0 and
              is_integer(hydrated_bytes) and hydrated_bytes >= 0 and
              is_binary(previous_cursor) and is_integer(max_items) and max_items > 0 and
              max_items <= @max_page_items and is_integer(max_bytes) and max_bytes > 0 and
              max_bytes <= @max_page_bytes do
    cond do
      length(records) > max_items -> {:error, :invalid_query_backfill_page}
      scanned_entries > max_items -> {:error, :invalid_query_backfill_page}
      length(records) > scanned_entries -> {:error, :invalid_query_backfill_page}
      hydrated_bytes > max_bytes -> {:error, :invalid_query_backfill_page}
      not done? and scanned_entries == 0 -> {:error, :query_backfill_made_no_progress}
      not done? and cursor == "" -> {:error, :query_backfill_made_no_progress}
      not done? and cursor <= previous_cursor -> {:error, :query_backfill_made_no_progress}
      true -> :ok
    end
  end

  defp validate_page(_page, _previous_cursor, _max_items, _max_bytes),
    do: {:error, :invalid_query_backfill_page}

  defp validate_metrics(
         %{
           projected_records: records,
           written_entries: entries,
           write_ops: write_ops,
           written_bytes: bytes
         },
         expected_records,
         max_bytes
       )
       when is_integer(records) and records >= 0 and is_integer(entries) and entries >= 0 and
              is_integer(write_ops) and write_ops >= 0 and is_integer(bytes) and bytes >= 0 and
              is_integer(expected_records) and expected_records >= 0 and is_integer(max_bytes) and
              max_bytes > 0 do
    if records == expected_records and
         entries <= records * @max_projection_entries_per_record and
         write_ops <= records * @max_projection_ops_per_record and bytes <= max_bytes,
       do: :ok,
       else: {:error, :invalid_query_backfill_metrics}
  end

  defp validate_metrics(_metrics, _expected_records, _max_bytes),
    do: {:error, :invalid_query_backfill_metrics}

  defp checkpoint_progress(checkpoint, overrides) do
    [
      phase: Keyword.fetch!(overrides, :phase),
      cursor: Keyword.fetch!(overrides, :cursor),
      fenced: Keyword.get(overrides, :fenced, checkpoint.fenced),
      scanned_records: Keyword.get(overrides, :scanned_records, checkpoint.scanned_records),
      written_entries: Keyword.get(overrides, :written_entries, checkpoint.written_entries),
      written_bytes: Keyword.get(overrides, :written_bytes, checkpoint.written_bytes)
    ]
  end

  defp empty_checkpoint do
    %{
      phase: :snapshot,
      cursor: "",
      fenced: false,
      scanned_records: 0,
      written_entries: 0,
      written_bytes: 0
    }
  end

  defp run_shard_batch(state, shards, callback) do
    shards
    |> Task.async_stream(fn shard -> invoke_shard_callback(callback, shard) end,
      max_concurrency: state.shard_concurrency,
      ordered: true,
      timeout: @call_timeout - 1_000,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, result}}, {:ok, results} when is_atom(result) ->
        {:cont, {:ok, [result | results]}}

      {:ok, {:error, _reason} = error}, _results ->
        {:halt, error}

      {:ok, _invalid}, _results ->
        {:halt, {:error, :invalid_query_index_lifecycle_result}}

      {:exit, _reason}, _results ->
        {:halt, {:error, :query_index_lifecycle_callback_failed}}
    end)
    |> summarize_shard_batch()
  end

  defp select_ready_shards(
         checkpoints,
         %{
           instance_ctx: %{shard_count: shard_count} = ctx,
           shard_concurrency: limit,
           ready_fun: ready_fun
         },
         default_checkpoint
       )
       when is_map(checkpoints) and is_integer(shard_count) and shard_count > 0 and
              is_integer(limit) and limit > 0 and is_function(ready_fun, 2) and
              is_function(default_checkpoint, 0) do
    {ready, _count, incomplete?} =
      Enum.reduce_while(0..(shard_count - 1), {[], 0, false}, fn shard_index,
                                                                 {ready, count, incomplete?} ->
        checkpoint = Map.get_lazy(checkpoints, shard_index, default_checkpoint)

        if checkpoint.phase == :done do
          {:cont, {ready, count, incomplete?}}
        else
          if safe_ready?(ready_fun, ctx, shard_index) do
            selected = {[{shard_index, checkpoint} | ready], count + 1, true}

            if count + 1 == limit,
              do: {:halt, selected},
              else: {:cont, selected}
          else
            {:cont, {ready, count, true}}
          end
        end
      end)

    case {Enum.reverse(ready), incomplete?} do
      {[], false} -> {:ok, :complete}
      {[], true} -> {:ok, :backend_not_ready}
      {ready, true} -> {:ok, ready}
    end
  end

  defp select_ready_shards(_checkpoints, _state, _default_checkpoint),
    do: {:error, :invalid_query_index_lifecycle_state}

  defp summarize_shard_batch({:ok, results}) do
    case Enum.uniq(results) do
      [result] -> {:ok, result}
      _mixed -> {:ok, :shards_progressed}
    end
  end

  defp summarize_shard_batch({:error, _reason} = error), do: error

  defp invoke_shard_callback(callback, shard) do
    callback.(shard)
  rescue
    _error -> {:error, :query_index_lifecycle_callback_failed}
  catch
    :exit, _reason -> {:error, :query_index_lifecycle_dependency_unavailable}
    _kind, _reason -> {:error, :query_index_lifecycle_callback_failed}
  end

  defp safe_ready?(ready_fun, ctx, shard_index) do
    ready_fun.(ctx, shard_index) == true
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  defp safe_pressure?(pressure_fun) do
    case pressure_fun.() do
      false -> false
      true -> true
      _invalid -> true
    end
  rescue
    _error -> true
  catch
    _kind, _reason -> true
  end

  defp safe_drain?(drain_fun, ctx, index) do
    drain_fun.(ctx, index) == true
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  defp queries_drained?(ctx, %RegisteredIndex{} = index) do
    server =
      Map.get_lazy(ctx, :query_admission_controller, fn ->
        AdmissionController.server_name(ctx)
      end)

    identity = index_identity(index)

    with :ok <- AdmissionController.fence_index(server, ctx, identity),
         {:ok, true} <- AdmissionController.drained?(server, ctx, identity) do
      true
    else
      _not_drained_or_unavailable -> false
    end
  end

  defp index_identity(%RegisteredIndex{definition: definition, build_id: build_id}),
    do: {definition.id, definition.version, build_id}

  defp release_retirement_fence(%{manages_admission_fence?: false}, _index, _completion),
    do: :ok

  defp release_retirement_fence(_state, _index, :pending), do: :ok

  defp release_retirement_fence(state, index, :complete) do
    server =
      Map.get_lazy(state.instance_ctx, :query_admission_controller, fn ->
        AdmissionController.server_name(state.instance_ctx)
      end)

    AdmissionController.unfence_index(server, state.instance_ctx, index_identity(index))
  end

  defp backend_ready?(%{name: :default} = ctx, shard_index) do
    Ferricstore.Health.ready?() and local_shard_alive?(ctx, shard_index)
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  defp backend_ready?(ctx, shard_index), do: local_shard_alive?(ctx, shard_index)

  defp local_shard_alive?(%{shard_names: shard_names}, shard_index)
       when is_tuple(shard_names) and shard_index < tuple_size(shard_names) do
    case elem(shard_names, shard_index) do
      name when is_atom(name) -> is_pid(Process.whereis(name))
      pid when is_pid(pid) -> Process.alive?(pid)
      _invalid -> false
    end
  end

  defp local_shard_alive?(_ctx, _shard_index), do: false

  defp validate_context(%{name: name, shard_count: shard_count})
       when is_atom(name) and is_integer(shard_count) and shard_count > 0,
       do: :ok

  defp validate_context(_ctx), do: {:error, :invalid_query_index_lifecycle_context}

  defp validate_functions(state) do
    if is_function(state.pressure_fun, 0) and is_function(state.ready_fun, 2) and
         is_function(state.snapshot_page_fun, 5) and
         is_function(state.page_fun, 7) and is_function(state.project_fun, 4) and
         is_function(state.cleanup_fun, 3) and is_function(state.barrier_fun, 2) and
         is_function(state.drain_fun, 2) and
         is_function(state.validation_fun, 7) and is_function(state.retirement_fun, 6),
       do: :ok,
       else: {:error, :invalid_query_index_lifecycle_function}
  end

  defp projection_barrier(%{name: instance_name}, shard_index)
       when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index, @call_timeout)
  end

  defp projection_barrier(_ctx, _shard_index),
    do: {:error, :invalid_query_index_projection_barrier}

  defp schedule(state, delay_ms) do
    cancel_timer(state.timer_ref)
    token = make_ref()
    ref = Process.send_after(self(), {:run, token}, delay_ms)
    %{state | timer_ref: ref, timer_token: token}
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref, async: true, info: false)

  defp progress?({:ok, result}),
    do:
      result in [
        :build_fenced,
        :shards_progressed,
        :snapshot_complete,
        :snapshot_progress,
        :backfill_progress,
        :shard_complete,
        :validation_fenced,
        :validation_progress,
        :validation_cleanup_progress,
        :validation_shard_complete,
        :build_activated,
        :validation_failed,
        :retirement_fenced,
        :retirement_progress,
        :retirement_cleanup_pending,
        :retirement_cleanup_progress,
        :retirement_shard_complete
      ]

  defp progress?(_result), do: false

  defp reason_code({:error, reason}) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_code({:error, {reason, _detail}}) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_code(_result), do: "query_index_lifecycle_failed"

  defp bounded_positive_opt(opts, key, default, max) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 and value <= max -> value
      _invalid -> default
    end
  end

  defp nonnegative_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> default
    end
  end
end
