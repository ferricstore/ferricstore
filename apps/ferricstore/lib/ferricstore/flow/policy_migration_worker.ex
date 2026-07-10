defmodule Ferricstore.Flow.PolicyMigrationWorker do
  @moduledoc false

  use GenServer

  require Logger

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.PolicyMigration
  alias Ferricstore.Store.Router

  @default_initial_delay_ms 1_000
  @default_interval_ms 1_000
  @default_catchup_delay_ms 10
  @default_batch_size 32
  @default_backfill_batch_size 256
  @default_backfill_max_bytes 2 * 1_024 * 1_024
  @max_backfill_bytes 64 * 1_024 * 1_024

  @spec name(FerricStore.Instance.t() | atom()) :: atom()
  def name(%{name: instance_name}), do: name(instance_name)
  def name(:default), do: __MODULE__

  def name(instance_name) when is_atom(instance_name),
    do: :"#{instance_name}.Flow.PolicyMigrationWorker"

  def start_link(opts \\ []) do
    if enabled?(opts) do
      instance_ctx = Keyword.get(opts, :instance_ctx) || FerricStore.Instance.get(:default)
      server_name = Keyword.get(opts, :name, name(instance_ctx))

      GenServer.start_link(__MODULE__, Keyword.put(opts, :instance_ctx, instance_ctx),
        name: server_name
      )
    else
      :ignore
    end
  end

  @impl true
  def init(opts) do
    state = %{
      instance_ctx: Keyword.fetch!(opts, :instance_ctx),
      interval_ms:
        positive_opt(
          opts,
          :interval_ms,
          configured(:flow_policy_migration_worker_interval_ms, @default_interval_ms)
        ),
      catchup_delay_ms:
        nonnegative_opt(
          opts,
          :catchup_delay_ms,
          configured(:flow_policy_migration_worker_catchup_delay_ms, @default_catchup_delay_ms)
        ),
      batch_size:
        bounded_positive_opt(
          opts,
          :batch_size,
          configured(:flow_policy_migration_worker_batch_size, @default_batch_size),
          256
        ),
      backfill_batch_size:
        bounded_positive_opt(
          opts,
          :backfill_batch_size,
          configured(
            :flow_policy_migration_worker_backfill_batch_size,
            @default_backfill_batch_size
          ),
          256
        ),
      backfill_max_bytes:
        bounded_positive_opt(
          opts,
          :backfill_max_bytes,
          configured(
            :flow_policy_migration_worker_backfill_max_bytes,
            @default_backfill_max_bytes
          ),
          @max_backfill_bytes
        ),
      step_fun: Keyword.get(opts, :step_fun, &Router.flow_policy_migration_step/3),
      backfill_step_fun:
        Keyword.get(opts, :backfill_step_fun, &Router.flow_policy_catalog_backfill_step/3),
      backfill_page_fun: Keyword.get(opts, :backfill_page_fun, &PolicyMigration.backfill_page/5),
      snapshot_fun: Keyword.get(opts, :snapshot_fun, &PolicyMigration.snapshot_primary_keydir/5),
      cleanup_fun: Keyword.get(opts, :cleanup_fun, &PolicyMigration.cleanup_snapshot/3),
      backfill_runs: %{},
      run_timer_ref: nil,
      run_timer_token: nil
    }

    state =
      schedule_run(
        state,
        nonnegative_opt(
          opts,
          :initial_delay_ms,
          configured(:flow_policy_migration_worker_initial_delay_ms, @default_initial_delay_ms)
        )
      )

    {:ok, state}
  end

  @impl true
  def handle_info(:run, state) do
    state
    |> cancel_scheduled_run()
    |> run_now()
  end

  def handle_info({:run, token}, %{run_timer_token: token} = state) do
    run_now(%{state | run_timer_ref: nil, run_timer_token: nil})
  end

  def handle_info({:run, _stale_token}, state), do: {:noreply, state}

  def handle_info(
        {:policy_catalog_snapshot_finished, shard_index, run_token, snapshot_pid, result},
        state
      ) do
    state = clear_task_pid(state, shard_index, run_token, :snapshot_pid, snapshot_pid)

    if result != :ok do
      Logger.warning(
        "Flow policy catalog snapshot failed for shard #{shard_index}: #{inspect(result)}"
      )
    end

    {:noreply, schedule_run(state, 0)}
  end

  def handle_info(
        {:policy_catalog_snapshot_cleaned, shard_index, run_token, cleanup_pid, result},
        state
      ) do
    if result != :ok do
      Logger.warning(
        "Flow policy catalog snapshot cleanup failed for shard #{shard_index}: #{inspect(result)}"
      )
    end

    state = clear_task_pid(state, shard_index, run_token, :cleanup_pid, cleanup_pid)
    {:noreply, schedule_run(state, 0)}
  end

  defp run_now(state) do
    {state, more_work?} = run_shards(state)
    delay_ms = if(more_work?, do: state.catchup_delay_ms, else: state.interval_ms)
    {:noreply, schedule_run(state, delay_ms)}
  end

  @impl true
  def terminate(_reason, state) do
    _state = cancel_scheduled_run(state)

    state.backfill_runs
    |> Map.values()
    |> Enum.flat_map(&[Map.get(&1, :snapshot_pid), Map.get(&1, :cleanup_pid)])
    |> Enum.filter(&is_pid/1)
    |> Enum.each(&Process.exit(&1, :shutdown))

    :ok
  end

  defp run_shards(state) do
    Enum.reduce(
      0..max(state.instance_ctx.shard_count - 1, -1)//1,
      {state, false},
      fn shard_index, {next_state, more_work?} ->
        {next_state, result} = run_shard(next_state, shard_index)

        next_more_work? =
          case result do
            {:ok, %{idle?: true}} ->
              more_work?

            {:ok, %{processed: processed}} when is_integer(processed) and processed >= 0 ->
              true

            {:retry, _reason} ->
              more_work?

            {:error, reason} ->
              Logger.warning(
                "Flow policy migration step failed for shard #{shard_index}: #{inspect(reason)}"
              )

              more_work?

            other ->
              Logger.warning(
                "Flow policy migration step returned an invalid result for shard #{shard_index}: #{inspect(other)}"
              )

              more_work?
          end

        {next_state, next_more_work?}
      end
    )
  end

  defp run_shard(state, shard_index) do
    cond do
      not backend_ready?(state.instance_ctx, shard_index) ->
        {state, {:retry, :backend_not_ready}}

      true ->
        case run_backfill_step(state, shard_index) do
          {next_state, {:ok, %{done?: true}}} ->
            {next_state, run_policy_step(next_state, shard_index)}

          {next_state, result} ->
            {next_state, result}
        end
    end
  rescue
    error -> {state, {:error, error}}
  catch
    :exit, reason -> {state, {:error, reason}}
  end

  defp run_policy_step(state, shard_index) do
    case PolicyMigration.next_job(state.instance_ctx, shard_index) do
      {:ok, nil} ->
        {:ok, %{processed: 0, done?: true, idle?: true}}

      {:ok, %{job: %{status: :active}}} ->
        state.step_fun.(state.instance_ctx, shard_index, state.batch_size)
        |> normalize_transient_failure()

      {:error, _reason} = error ->
        error
    end
  end

  defp run_backfill_step(state, shard_index) do
    with {:ok, source_token} <- PolicyMigration.source_token(state.instance_ctx, shard_index),
         {:ok, progress_value} <-
           Router.read_shard_value(
             state.instance_ctx,
             shard_index,
             Keys.policy_catalog_backfill_key(shard_index)
           ),
         {:ok, run} <-
           backfill_run(
             source_token,
             progress_value,
             Map.get(state.backfill_runs, shard_index)
           ) do
      if run.done? do
        {ensure_cleanup_task(state, shard_index, run),
         {:ok, %{processed: 0, done?: true, idle?: true}}}
      else
        run_backfill_phase(state, shard_index, run)
      end
    else
      :unavailable -> {state, {:retry, :backfill_progress_unavailable}}
      {:error, reason} -> {state, normalize_transient_failure({:error, reason})}
    end
  end

  defp backfill_run(source_token, progress_value, local_run) do
    result =
      case PolicyMigration.decode_backfill_progress(progress_value) do
        {:ok, %{source_token: ^source_token, status: :active} = progress} ->
          {:ok,
           %{
             run_token: progress.run_token,
             source_token: source_token,
             cursor: progress.cursor,
             done?: false
           }}

        {:ok, %{source_token: ^source_token, status: :done} = progress} ->
          {:ok,
           %{
             run_token: progress.run_token,
             source_token: source_token,
             cursor: progress.cursor,
             done?: true
           }}

        {:ok, %{source_token: existing_source}}
        when existing_source != source_token ->
          {:ok, new_backfill_run(source_token)}

        :error when is_nil(progress_value) ->
          {:ok, new_backfill_run(source_token)}

        :error ->
          {:error, :corrupt_policy_catalog_backfill_progress}

        {:ok, _conflicting_active} ->
          {:error, :conflicting_policy_catalog_backfill}
      end

    case result do
      {:ok, run} -> {:ok, restore_local_tasks(run, local_run)}
      {:error, _reason} = error -> error
    end
  end

  defp run_backfill_phase(state, shard_index, %{cursor: ""} = run) do
    submit_backfill_page(state, shard_index, run, %{
      cursor: PolicyMigration.snapshot_cursor(),
      candidates: [],
      done?: false
    })
  end

  defp run_backfill_phase(state, shard_index, run) do
    snapshot_complete? =
      PolicyMigration.snapshot_complete?(state.instance_ctx, shard_index, run.run_token)

    cond do
      run.cursor == PolicyMigration.snapshot_cursor() and snapshot_complete? ->
        submit_backfill_page(state, shard_index, run, %{
          cursor: PolicyMigration.work_cursor(run.run_token),
          candidates: [],
          done?: false
        })

      run.cursor == PolicyMigration.snapshot_cursor() ->
        {ensure_snapshot_task(state, shard_index, run), {:retry, :snapshot_in_progress}}

      PolicyMigration.work_cursor?(run.cursor, run.run_token) and not snapshot_complete? ->
        {ensure_snapshot_task(state, shard_index, run), {:retry, :snapshot_in_progress}}

      true ->
        run_backfill_page(state, shard_index, run)
    end
  end

  defp run_backfill_page(state, shard_index, run) do
    case state.backfill_page_fun.(
           state.instance_ctx,
           shard_index,
           run.cursor,
           state.backfill_batch_size,
           state.backfill_max_bytes
         ) do
      {:ok, page} ->
        submit_backfill_page(state, shard_index, run, page)

      {:error, _reason} = error ->
        {state, error}
    end
  end

  defp submit_backfill_page(state, shard_index, run, page) do
    request = %{
      run_token: run.run_token,
      source_token: run.source_token,
      expected_cursor: run.cursor,
      cursor: page.cursor,
      candidates: page.candidates,
      done?: page.done?
    }

    case state.backfill_step_fun.(state.instance_ctx, shard_index, request)
         |> normalize_transient_failure() do
      {:ok, %{cursor: cursor, done?: done?} = result} ->
        next_run = %{run | cursor: cursor, done?: done?}

        next_state =
          if done?,
            do: ensure_cleanup_task(state, shard_index, next_run),
            else: put_backfill_run(state, shard_index, next_run)

        {next_state, {:ok, Map.put(result, :idle?, false)}}

      {:retry, _reason} = retry ->
        {state, retry}

      {:error, "ERR stale flow policy catalog backfill cursor"} ->
        {%{state | backfill_runs: Map.delete(state.backfill_runs, shard_index)},
         {:retry, :stale_backfill_cursor}}

      {:error, _reason} = error ->
        {state, error}

      other ->
        {state, other}
    end
  end

  defp new_backfill_run(source_token) do
    %{
      run_token: Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false),
      source_token: source_token,
      cursor: "",
      done?: false,
      snapshot_pid: nil,
      cleanup_pid: nil
    }
  end

  defp restore_local_tasks(run, %{run_token: run_token} = local_run)
       when run.run_token == run_token do
    run
    |> Map.put(:snapshot_pid, live_pid(Map.get(local_run, :snapshot_pid)))
    |> Map.put(:cleanup_pid, live_pid(Map.get(local_run, :cleanup_pid)))
  end

  defp restore_local_tasks(run, _local_run) do
    run
    |> Map.put(:snapshot_pid, nil)
    |> Map.put(:cleanup_pid, nil)
  end

  defp live_pid(pid) when is_pid(pid), do: if(Process.alive?(pid), do: pid, else: nil)
  defp live_pid(_pid), do: nil

  defp ensure_snapshot_task(state, shard_index, %{snapshot_pid: pid} = run)
       when is_pid(pid),
       do: put_backfill_run(state, shard_index, run)

  defp ensure_snapshot_task(state, shard_index, run) do
    if snapshot_task_running?(state) do
      put_backfill_run(state, shard_index, run)
    else
      worker = self()
      snapshot_fun = state.snapshot_fun
      instance_ctx = state.instance_ctx
      max_items = state.backfill_batch_size
      max_bytes = state.backfill_max_bytes

      pid =
        spawn_link(fn ->
          snapshot_pid = self()

          result =
            try do
              snapshot_fun.(instance_ctx, shard_index, run.run_token, max_items, max_bytes)
            rescue
              error -> {:error, {:snapshot_exception, error}}
            catch
              kind, reason -> {:error, {:snapshot_failure, kind, reason}}
            end

          send(
            worker,
            {:policy_catalog_snapshot_finished, shard_index, run.run_token, snapshot_pid, result}
          )
        end)

      put_backfill_run(state, shard_index, %{run | snapshot_pid: pid})
    end
  end

  defp snapshot_task_running?(state) do
    Enum.any?(state.backfill_runs, fn {_shard_index, run} ->
      is_pid(Map.get(run, :snapshot_pid)) and Process.alive?(run.snapshot_pid)
    end)
  end

  defp ensure_cleanup_task(state, shard_index, %{cleanup_pid: pid} = run)
       when is_pid(pid),
       do: put_backfill_run(state, shard_index, run)

  defp ensure_cleanup_task(state, shard_index, run) do
    if PolicyMigration.snapshot_complete?(state.instance_ctx, shard_index, run.run_token) and
         not cleanup_task_running?(state) do
      worker = self()
      instance_ctx = state.instance_ctx
      cleanup_fun = state.cleanup_fun

      pid =
        spawn_link(fn ->
          result =
            try do
              cleanup_fun.(instance_ctx, shard_index, run.run_token)
            rescue
              error -> {:error, {:cleanup_exception, error}}
            catch
              kind, reason -> {:error, {:cleanup_failure, kind, reason}}
            end

          send(
            worker,
            {:policy_catalog_snapshot_cleaned, shard_index, run.run_token, self(), result}
          )
        end)

      put_backfill_run(state, shard_index, %{run | cleanup_pid: pid})
    else
      put_backfill_run(state, shard_index, run)
    end
  end

  defp cleanup_task_running?(state) do
    Enum.any?(state.backfill_runs, fn {_shard_index, run} ->
      is_pid(Map.get(run, :cleanup_pid)) and Process.alive?(run.cleanup_pid)
    end)
  end

  defp clear_task_pid(state, shard_index, run_token, field, pid) do
    case Map.get(state.backfill_runs, shard_index) do
      %{run_token: ^run_token} = run ->
        if Map.get(run, field) == pid,
          do: put_backfill_run(state, shard_index, Map.put(run, field, nil)),
          else: state

      _missing_or_replaced ->
        state
    end
  end

  defp put_backfill_run(state, shard_index, run) do
    %{state | backfill_runs: Map.put(state.backfill_runs, shard_index, run)}
  end

  defp backend_ready?(%{name: :default}, shard_index) do
    Ferricstore.Health.ready?() and
      match?(
        {:ok, [_ | _], {_server, leader}} when leader == node(),
        Ferricstore.Raft.WARaftBackend.cached_members(shard_index)
      )
  rescue
    _error -> false
  catch
    :exit, _reason -> false
  end

  defp backend_ready?(_instance_ctx, _shard_index), do: true

  defp normalize_transient_failure({:error, {:timeout, :unknown_outcome}} = error),
    do: {:retry, error}

  defp normalize_transient_failure({:error, "ERR shard not available"} = error),
    do: {:retry, error}

  defp normalize_transient_failure({:error, "ERR leader unavailable"} = error),
    do: {:retry, error}

  defp normalize_transient_failure(
         {:error, "ERR flow policy migration projection pending"} = error
       ),
       do: {:retry, error}

  defp normalize_transient_failure(
         {:error, "ERR flow policy catalog projection pending"} = error
       ),
       do: {:retry, error}

  defp normalize_transient_failure(result), do: result

  defp schedule_run(state, delay_ms) do
    state = cancel_scheduled_run(state)
    token = make_ref()
    timer_ref = Process.send_after(self(), {:run, token}, delay_ms)
    %{state | run_timer_ref: timer_ref, run_timer_token: token}
  end

  defp cancel_scheduled_run(%{run_timer_ref: timer_ref} = state) when is_reference(timer_ref) do
    _remaining = Process.cancel_timer(timer_ref)
    %{state | run_timer_ref: nil, run_timer_token: nil}
  end

  defp cancel_scheduled_run(state), do: state

  defp enabled?(opts) do
    Keyword.get(
      opts,
      :enabled,
      Application.get_env(:ferricstore, :flow_policy_migration_worker_enabled, true)
    ) == true
  end

  defp configured(key, default), do: Application.get_env(:ferricstore, key, default)

  defp positive_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end

  defp bounded_positive_opt(opts, key, default, max_value) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> min(value, max_value)
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
