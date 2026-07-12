defmodule Ferricstore.Flow.Schedule do
  @moduledoc """
  Durable FerricFlow schedules.

  Schedules are stored as internal Flow records and fired by claiming due
  schedule records. That keeps scheduling distributed-safe without a second
  coordination path: only the shard leader can lease a due schedule, and every
  fire/reschedule is still guarded by Flow fencing tokens.
  """

  alias Ferricstore.Flow
  alias Ferricstore.Flow.{Internal, Keys}
  alias Ferricstore.Store.Router

  @schedule_type "__ferricstore_schedule"
  @schedule_id_prefix "__ferricstore_schedule__:"
  @active_state "active"
  @paused_state "paused"
  @default_state "queued"
  @default_limit 100
  @default_lease_ms 30_000
  @default_overlap_retry_ms 1_000
  @schedule_event_created "schedule_created"
  @schedule_event_fired "schedule_fired"
  @schedule_event_skipped_overlap "schedule_skipped_overlap"
  @schedule_event_failed_overlap "schedule_failed_overlap"
  @schedule_event_deleted "schedule_deleted"
  @partition_buckets 256
  @minute_ms 60_000
  @default_cron_search_minutes 366 * 24 * 60
  @max_cron_search_minutes 366 * 24 * 60 * 5
  @default_definition_max_bytes 32 * 1024
  @default_inline_value_max_bytes 8 * 1024
  @default_timezone "Etc/UTC"

  @type schedule_id :: binary()

  @spec create(FerricStore.Instance.t(), schedule_id(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         {:ok, overwrite?} <- optional_boolean(opts, :overwrite, false),
         {:ok, definition} <- definition(id, opts) do
      flow_id = flow_id(id)

      create_opts = [
        type: @schedule_type,
        state: @active_state,
        partition_key: partition_key(id),
        payload: definition,
        run_at_ms: Map.fetch!(definition, :next_run_at_ms),
        now_ms: Map.fetch!(definition, :created_at_ms)
      ]

      case Flow.create_internal(ctx, flow_id, create_opts) do
        :ok ->
          emit_schedule_event(ctx, definition, @schedule_event_created)
          {:ok, view(%{id: flow_id, state: @active_state, payload: definition})}

        {:ok, _record} ->
          emit_schedule_event(ctx, definition, @schedule_event_created)
          {:ok, view(%{id: flow_id, state: @active_state, payload: definition})}

        {:error, "ERR flow already exists"} when overwrite? ->
          replace(ctx, flow_id, definition)

        {:error, _reason} = error ->
          error
      end
    end
  end

  def create(_ctx, _id, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec get(FerricStore.Instance.t(), schedule_id(), keyword()) ::
          {:ok, map() | nil} | {:error, binary()}
  def get(ctx, id, opts \\ [])

  def get(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id) do
      ctx
      |> Flow.get(
        flow_id(id),
        Keyword.merge(
          [
            partition_key: partition_key(id),
            payload: true,
            payload_max_bytes: schedule_definition_max_bytes()
          ],
          Internal.put(opts)
        )
      )
      |> case do
        {:ok, nil} -> {:ok, nil}
        {:ok, record} -> {:ok, view(record)}
        {:error, _reason} = error -> error
      end
    end
  end

  def get(_ctx, _id, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec fire(FerricStore.Instance.t(), schedule_id(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def fire(ctx, id, opts \\ [])

  def fire(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         {:ok, now_ms} <- optional_now_ms(opts),
         {:ok, fire_at_ms} <- optional_non_neg_integer(opts, :fire_at_ms, now_ms),
         {:ok, record} <-
           Flow.get(
             ctx,
             flow_id(id),
             Internal.put(
               partition_key: partition_key(id),
               payload: true,
               payload_max_bytes: schedule_definition_max_bytes()
             )
           ),
         :ok <- require_schedule_record(record),
         :ok <- require_active_schedule(record) do
      fire_manual_one(ctx, record, fire_at_ms, now_ms)
    end
  end

  def fire(_ctx, id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def fire(_ctx, _id, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec pause(FerricStore.Instance.t(), schedule_id(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def pause(ctx, id, opts \\ [])

  def pause(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with {:ok, record, now_ms} <- mutable_schedule_record(ctx, id, opts),
         :ok <- require_active_schedule(record) do
      replace_with_state(
        ctx,
        record,
        Map.fetch!(record, :payload),
        @paused_state,
        Map.get(record, :next_run_at_ms, now_ms),
        now_ms
      )
    end
  end

  def pause(_ctx, id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def pause(_ctx, _id, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec resume(FerricStore.Instance.t(), schedule_id(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def resume(ctx, id, opts \\ [])

  def resume(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with {:ok, record, now_ms} <- mutable_schedule_record(ctx, id, opts),
         :ok <- require_paused_schedule(record),
         {:ok, run_at_ms} <- schedule_resume_run_at(record, now_ms) do
      replace_with_state(
        ctx,
        record,
        Map.fetch!(record, :payload),
        @active_state,
        run_at_ms,
        now_ms
      )
    end
  end

  def resume(_ctx, id, _opts) when not is_binary(id),
    do: {:error, "ERR flow schedule id must be a non-empty string"}

  def resume(_ctx, _id, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec list(FerricStore.Instance.t(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def list(ctx, opts \\ [])

  def list(ctx, opts) when is_list(opts) do
    with {:ok, states} <- list_states(opts),
         {:ok, count} <- list_count(opts),
         {:ok, filters} <- list_filters(opts) do
      schedules =
        states
        |> Enum.flat_map(&list_state(ctx, &1, count))
        |> Enum.uniq_by(& &1.id)
        |> Enum.filter(&schedule_matches_filters?(&1, filters))
        |> Enum.sort_by(&{schedule_sort_due(&1), &1.id})
        |> Enum.take(count)

      {:ok, schedules}
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec delete(FerricStore.Instance.t(), schedule_id(), keyword()) :: :ok | {:error, binary()}
  def delete(ctx, id, opts \\ [])

  def delete(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         {:ok, now_ms} <- optional_now_ms(opts),
         {:ok, record} <-
           Flow.get(
             ctx,
             flow_id(id),
             Internal.put(partition_key: partition_key(id), payload: false)
           ),
         :ok <- require_schedule_record(record) do
      cancel_opts =
        [
          partition_key: partition_key(id),
          fencing_token: Map.get(record, :fencing_token, 0)
        ]
        |> maybe_put(:now_ms, now_ms)
        |> Internal.put()

      case Flow.cancel(ctx, flow_id(id), cancel_opts) do
        :ok ->
          emit_schedule_event(ctx, flow_id(id), id, now_ms, @schedule_event_deleted)
          :ok

        {:ok, _record} ->
          emit_schedule_event(ctx, flow_id(id), id, now_ms, @schedule_event_deleted)
          :ok

        {:error, _reason} = error ->
          error
      end
    end
  end

  def delete(_ctx, _id, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec fire_due(FerricStore.Instance.t(), keyword()) :: {:ok, map()} | {:error, binary()}
  def fire_due(ctx, opts \\ [])

  def fire_due(ctx, opts) when is_list(opts) do
    now_ms = Keyword.get(opts, :now_ms, now_ms())
    worker = Keyword.get(opts, :worker, default_worker())
    limit = Keyword.get(opts, :limit, @default_limit)
    lease_ms = Keyword.get(opts, :lease_ms, @default_lease_ms)

    claim_opts =
      [
        state: @active_state,
        partition_key: :any,
        worker: worker,
        limit: limit,
        lease_ms: lease_ms,
        now_ms: now_ms,
        payload: true,
        payload_max_bytes: schedule_definition_max_bytes()
      ]
      |> maybe_put(:block_ms, Keyword.get(opts, :block_ms))
      |> Internal.put()

    with {:ok, claimed} <- Flow.claim_due(ctx, @schedule_type, claim_opts) do
      claimed
      |> Enum.reduce_while(
        {:ok, %{claimed: length(claimed), fired: 0, skipped: 0, errors: []}},
        fn record, {:ok, acc} ->
          case fire_one(ctx, record, now_ms) do
            {:ok, target_id} ->
              {:cont,
               {:ok, acc |> Map.update!(:fired, &(&1 + 1)) |> Map.put(:last_target_id, target_id)}}

            {:skipped, reason} ->
              {:cont,
               {:ok,
                acc
                |> Map.update!(:skipped, &(&1 + 1))
                |> Map.put(:last_skip_reason, reason)}}

            {:error, reason} ->
              {:cont, {:ok, %{acc | errors: [{Map.get(record, :id), reason} | acc.errors]}}}
          end
        end
      )
      |> normalize_fire_result()
    end
  end

  def fire_due(_ctx, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @doc false
  def flow_id(id), do: "__ferricstore_schedule__:" <> id

  defp fire_one(
         ctx,
         %{payload: definition, lease_token: lease, fencing_token: fence} = record,
         now_ms
       )
       when is_map(definition) and is_binary(lease) and is_integer(fence) do
    due_at_ms =
      Map.get(definition, :overlap_queued_due_at_ms) || Map.fetch!(definition, :next_run_at_ms)

    fire_count = Map.get(definition, :fire_count, 0) + 1
    target_id = target_id(definition, due_at_ms, fire_count)

    case overlap_action(ctx, definition) do
      :allow ->
        with :ok <- create_target(ctx, definition, target_id, now_ms),
             :ok <-
               finish_schedule_fire(
                 ctx,
                 record,
                 definition,
                 target_id,
                 due_at_ms,
                 fire_count,
                 now_ms
               ) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_fired)
          {:ok, target_id}
        end

      {:skip, reason} ->
        with :ok <- skip_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_skipped_overlap)
          {:skipped, reason}
        end

      {:queue, reason} ->
        with :ok <- queue_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_skipped_overlap)
          {:skipped, reason}
        end

      {:fail, reason} ->
        fail_schedule_overlap(ctx, record, definition, reason, now_ms)

      {:error, _reason} = error ->
        error
    end
  end

  defp fire_one(_ctx, _record, _now_ms), do: {:error, "ERR schedule payload missing"}

  defp fire_manual_one(ctx, %{payload: definition} = record, fire_at_ms, now_ms)
       when is_map(definition) do
    fire_count = Map.get(definition, :fire_count, 0) + 1
    target_id = target_id(definition, fire_at_ms, fire_count)

    case overlap_action(ctx, definition) do
      :allow ->
        with :ok <- create_target(ctx, definition, target_id, now_ms),
             {:ok, schedule} <-
               finish_manual_schedule_fire(
                 ctx,
                 record,
                 definition,
                 target_id,
                 fire_at_ms,
                 fire_count,
                 now_ms
               ) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_fired)
          {:ok, %{fired: 1, target_id: target_id, schedule: schedule}}
        end

      {:skip, reason} ->
        with {:ok, schedule} <-
               skip_manual_schedule_fire(ctx, record, definition, fire_at_ms, reason, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_skipped_overlap)
          {:ok, %{fired: 0, skipped: 1, reason: reason, schedule: schedule}}
        end

      {:queue, reason} ->
        with {:ok, schedule} <-
               queue_manual_schedule_fire(ctx, record, definition, fire_at_ms, reason, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_skipped_overlap)
          {:ok, %{fired: 0, skipped: 1, reason: reason, schedule: schedule}}
        end

      {:fail, reason} ->
        fail_manual_schedule_overlap(ctx, record, definition, reason, now_ms)

      {:error, _reason} = error ->
        error
    end
  end

  defp fire_manual_one(_ctx, _record, _fire_at_ms, _now_ms),
    do: {:error, "ERR schedule payload missing"}

  defp finish_schedule_fire(
         ctx,
         record,
         %{kind: kind} = definition,
         target_id,
         due_at_ms,
         fire_count,
         now_ms
       )
       when kind in [:one_shot, :delay] do
    completed_definition =
      definition
      |> Map.put(:fire_count, fire_count)
      |> Map.put(:last_fire_at_ms, due_at_ms)
      |> Map.put(:last_target_id, target_id)
      |> Map.put(:end_reason, "one_shot_fired")
      |> Map.delete(:next_run_at_ms)

    Flow.complete(ctx, Map.fetch!(record, :id), Map.fetch!(record, :lease_token),
      partition_key: Map.get(record, :partition_key),
      fencing_token: Map.fetch!(record, :fencing_token),
      payload: completed_definition,
      result: %{target_id: target_id, fire_count: fire_count, fired_at_ms: due_at_ms},
      now_ms: now_ms,
      __ferricstore_internal__: true
    )
    |> internal_terminal_result()
  end

  defp finish_schedule_fire(ctx, record, definition, target_id, due_at_ms, fire_count, now_ms) do
    with {:ok, next_run_at_ms} <- next_run_at_ms(definition, due_at_ms) do
      next_definition =
        next_definition_after_fire(definition, target_id, due_at_ms, fire_count, next_run_at_ms)

      case recurring_end_reason(next_definition, next_run_at_ms, fire_count) do
        nil ->
          Flow.reschedule(ctx, Map.fetch!(record, :id), Map.fetch!(record, :lease_token),
            state: @active_state,
            partition_key: Map.get(record, :partition_key),
            fencing_token: Map.fetch!(record, :fencing_token),
            payload: next_definition,
            run_at_ms: next_run_at_ms,
            now_ms: now_ms,
            __ferricstore_internal__: true
          )
          |> ok_result()

        reason ->
          completed_definition =
            next_definition
            |> Map.put(:end_reason, reason)
            |> Map.delete(:next_run_at_ms)

          Flow.complete(ctx, Map.fetch!(record, :id), Map.fetch!(record, :lease_token),
            partition_key: Map.get(record, :partition_key),
            fencing_token: Map.fetch!(record, :fencing_token),
            payload: completed_definition,
            result: %{
              target_id: target_id,
              fire_count: fire_count,
              fired_at_ms: due_at_ms,
              end_reason: reason
            },
            now_ms: now_ms,
            __ferricstore_internal__: true
          )
          |> internal_terminal_result()
      end
    end
  end

  defp internal_terminal_result(result), do: ok_result(result)

  defp overlap_action(ctx, definition) do
    policy = Map.get(definition, :overlap_policy, :allow)

    case previous_target_active?(ctx, definition) do
      {:ok, nil} ->
        :allow

      {:ok, previous_target_id} ->
        reason = "previous target still active: #{previous_target_id}"

        case policy do
          :allow -> :allow
          :skip -> {:skip, reason}
          :queue_after_previous -> {:queue, reason}
          :fail_schedule -> {:fail, reason}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp previous_target_active?(ctx, definition) do
    case Map.get(definition, :last_target_id) do
      target_id when is_binary(target_id) and target_id != "" ->
        partition_key = target_partition_key(target_id, Map.fetch!(definition, :target))

        case Flow.get(ctx, target_id, partition_key: partition_key) do
          {:ok, nil} ->
            {:ok, nil}

          {:ok, %{state: state}} ->
            if Ferricstore.Flow.LMDB.terminal_state?(state),
              do: {:ok, nil},
              else: {:ok, target_id}

          {:error, _reason} = error ->
            error
        end

      _other ->
        {:ok, nil}
    end
  end

  defp skip_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
    with {:ok, next_run_at_ms} <- next_run_at_ms(definition, due_at_ms) do
      next_definition = skipped_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms)

      reschedule_definition(ctx, record, next_definition, next_run_at_ms, now_ms)
    end
  end

  defp queue_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
    retry_ms = Map.get(definition, :overlap_retry_ms, @default_overlap_retry_ms)
    next_run_at_ms = now_ms + retry_ms

    next_definition =
      queued_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms)

    reschedule_definition(ctx, record, next_definition, next_run_at_ms, now_ms)
  end

  defp fail_schedule_overlap(ctx, record, definition, reason, now_ms) do
    Flow.fail(ctx, Map.fetch!(record, :id), Map.fetch!(record, :lease_token),
      partition_key: Map.get(record, :partition_key),
      fencing_token: Map.fetch!(record, :fencing_token),
      error: %{
        reason: reason,
        schedule_id: Map.fetch!(definition, :id),
        previous_target_id: Map.get(definition, :last_target_id)
      },
      now_ms: now_ms,
      __ferricstore_internal__: true
    )
    |> case do
      :ok -> {:error, reason}
      {:ok, _record} -> {:error, reason}
      {:error, _reason} = error -> error
    end
    |> tap(fn
      {:error, ^reason} ->
        emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_failed_overlap)

      _other ->
        :ok
    end)
  end

  defp fail_manual_schedule_overlap(ctx, record, definition, reason, now_ms) do
    failed_definition =
      definition
      |> Map.put(:end_reason, "overlap_failed")
      |> Map.put(:last_overlap_at_ms, now_ms)
      |> Map.put(:last_overlap_target_id, Map.get(definition, :last_target_id))
      |> Map.put(:last_overlap_reason, reason)

    case replace_with_state(ctx, record, failed_definition, "failed", now_ms, now_ms) do
      {:ok, _schedule} ->
        emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_failed_overlap)
        {:error, reason}

      {:error, _reason} = error ->
        error
    end
  end

  defp finish_manual_schedule_fire(
         ctx,
         record,
         %{kind: kind} = definition,
         target_id,
         due_at_ms,
         fire_count,
         now_ms
       )
       when kind in [:one_shot, :delay] do
    completed_definition =
      definition
      |> Map.put(:fire_count, fire_count)
      |> Map.put(:last_fire_at_ms, due_at_ms)
      |> Map.put(:last_target_id, target_id)
      |> Map.put(:end_reason, "one_shot_fired")
      |> Map.delete(:next_run_at_ms)

    replace_with_state(ctx, record, completed_definition, "completed", due_at_ms, now_ms)
  end

  defp finish_manual_schedule_fire(
         ctx,
         record,
         definition,
         target_id,
         due_at_ms,
         fire_count,
         now_ms
       ) do
    with {:ok, next_run_at_ms} <- next_run_at_ms(definition, due_at_ms) do
      next_definition =
        next_definition_after_fire(definition, target_id, due_at_ms, fire_count, next_run_at_ms)

      case recurring_end_reason(next_definition, next_run_at_ms, fire_count) do
        nil ->
          replace_with_state(ctx, record, next_definition, @active_state, next_run_at_ms, now_ms)

        reason ->
          completed_definition =
            next_definition
            |> Map.put(:end_reason, reason)
            |> Map.delete(:next_run_at_ms)

          replace_with_state(ctx, record, completed_definition, "completed", due_at_ms, now_ms)
      end
    end
  end

  defp skip_manual_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
    with {:ok, next_run_at_ms} <- next_run_at_ms(definition, due_at_ms) do
      next_definition = skipped_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms)

      replace_with_state(ctx, record, next_definition, @active_state, next_run_at_ms, now_ms)
    end
  end

  defp queue_manual_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
    retry_ms = Map.get(definition, :overlap_retry_ms, @default_overlap_retry_ms)
    next_run_at_ms = now_ms + retry_ms
    next_definition = queued_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms)

    replace_with_state(ctx, record, next_definition, @active_state, next_run_at_ms, now_ms)
  end

  defp replace_with_state(ctx, record, definition, state, run_at_ms, now_ms) do
    flow_id = Map.fetch!(record, :id)

    attrs = %{
      id: flow_id,
      type: @schedule_type,
      state: state,
      partition_key: Map.get(record, :partition_key),
      payload: definition,
      run_at_ms: run_at_ms,
      now_ms: now_ms
    }

    case Router.flow_schedule_replace(ctx, attrs) do
      :ok -> {:ok, view(%{id: flow_id, state: state, payload: definition})}
      {:ok, _record} -> {:ok, view(%{id: flow_id, state: state, payload: definition})}
      {:error, _reason} = error -> error
    end
  end

  defp next_definition_after_fire(definition, target_id, due_at_ms, fire_count, next_run_at_ms) do
    definition
    |> Map.put(:fire_count, fire_count)
    |> Map.put(:last_fire_at_ms, due_at_ms)
    |> Map.put(:last_target_id, target_id)
    |> Map.put(:next_run_at_ms, next_run_at_ms)
    |> Map.delete(:overlap_queued_due_at_ms)
    |> Map.delete(:last_overlap_at_ms)
    |> Map.delete(:last_overlap_target_id)
    |> Map.delete(:last_overlap_reason)
    |> Map.delete(:end_reason)
  end

  defp skipped_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms) do
    definition
    |> Map.update(:skipped_count, 1, &(&1 + 1))
    |> Map.put(:last_skipped_at_ms, due_at_ms)
    |> Map.put(:last_overlap_at_ms, now_ms)
    |> Map.put(:last_overlap_target_id, Map.get(definition, :last_target_id))
    |> Map.put(:last_overlap_reason, reason)
    |> Map.put(:next_run_at_ms, next_run_at_ms)
    |> Map.delete(:overlap_queued_due_at_ms)
  end

  defp queued_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms) do
    definition
    |> Map.put(:overlap_queued_due_at_ms, due_at_ms)
    |> Map.put(:last_overlap_at_ms, now_ms)
    |> Map.put(:last_overlap_target_id, Map.get(definition, :last_target_id))
    |> Map.put(:last_overlap_reason, reason)
    |> Map.put(:next_run_at_ms, next_run_at_ms)
  end

  defp recurring_end_reason(definition, next_run_at_ms, fire_count) do
    cond do
      is_integer(Map.get(definition, :max_fires)) and
          fire_count >= Map.fetch!(definition, :max_fires) ->
        "max_fires"

      is_integer(Map.get(definition, :end_at_ms)) and
          next_run_at_ms > Map.fetch!(definition, :end_at_ms) ->
        "end_at_ms"

      true ->
        nil
    end
  end

  defp reschedule_definition(ctx, record, definition, run_at_ms, now_ms) do
    Flow.reschedule(ctx, Map.fetch!(record, :id), Map.fetch!(record, :lease_token),
      state: @active_state,
      partition_key: Map.get(record, :partition_key),
      fencing_token: Map.fetch!(record, :fencing_token),
      payload: definition,
      run_at_ms: run_at_ms,
      now_ms: now_ms,
      __ferricstore_internal__: true
    )
    |> ok_result()
  end

  defp emit_schedule_event(ctx, %{id: id} = definition, event) do
    emit_schedule_event(
      ctx,
      flow_id(id),
      id,
      Map.get(definition, :created_at_ms, now_ms()),
      event
    )
  end

  defp emit_schedule_event(ctx, %{id: flow_id}, %{id: id}, now_ms, event) do
    emit_schedule_event(ctx, flow_id, id, now_ms, event)
  end

  defp emit_schedule_event(ctx, flow_id, id, now_ms, event) do
    case Ferricstore.Flow.Signal.run(
           ctx,
           flow_id,
           Internal.put(
             signal: event,
             partition_key: partition_key(id),
             now_ms: now_ms
           )
         ) do
      :ok -> :ok
      {:ok, _record} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp create_target(ctx, definition, target_id, now_ms) do
    target = Map.fetch!(definition, :target)
    partition_key = target_partition_key(target_id, target)
    correlation_id = target_correlation_id(definition, target)

    opts =
      [
        type: Map.fetch!(target, :type),
        state: Map.get(target, :state, @default_state),
        partition_key: partition_key,
        correlation_id: correlation_id,
        now_ms: now_ms,
        run_at_ms: Map.get(target, :run_at_ms, now_ms)
      ]
      |> maybe_put(:priority, Map.get(target, :priority))
      |> maybe_put(:parent_flow_id, Map.get(target, :parent_flow_id))
      |> maybe_put(:root_flow_id, Map.get(target, :root_flow_id))
      |> maybe_put(:payload, Map.get(target, :payload))
      |> maybe_put(:payload_ref, Map.get(target, :payload_ref))
      |> maybe_put(:values, Map.get(target, :values))
      |> maybe_put(:value_refs, Map.get(target, :value_refs))

    case Flow.create(ctx, target_id, opts) do
      :ok ->
        :ok

      {:ok, _record} ->
        :ok

      {:error, "ERR flow already exists"} ->
        verify_existing_target(ctx, target_id, target, partition_key, correlation_id)

      {:error, _reason} = error ->
        error
    end
  end

  defp verify_existing_target(ctx, target_id, target, partition_key, correlation_id) do
    case Flow.get(ctx, target_id, partition_key: partition_key) do
      {:ok, %{type: type, correlation_id: ^correlation_id}} when type == target.type ->
        :ok

      {:ok, _other} ->
        {:error, "ERR scheduled target id already exists with different owner"}

      {:error, _reason} = error ->
        error

      _other ->
        {:error, "ERR scheduled target id already exists with different owner"}
    end
  end

  defp target_partition_key(target_id, target) do
    Map.get(target, :partition_key) || Keys.auto_partition_key(target_id)
  end

  defp target_correlation_id(definition, target) do
    Map.get(target, :correlation_id) || "__ferricstore_schedule__:" <> Map.fetch!(definition, :id)
  end

  defp require_schedule_record(%{type: @schedule_type}), do: :ok
  defp require_schedule_record(nil), do: {:error, "ERR flow schedule not found"}
  defp require_schedule_record(_record), do: {:error, "ERR flow schedule not found"}

  defp require_active_schedule(%{state: @active_state}), do: :ok
  defp require_active_schedule(_record), do: {:error, "ERR flow schedule is not active"}

  defp require_paused_schedule(%{state: @paused_state}), do: :ok
  defp require_paused_schedule(_record), do: {:error, "ERR flow schedule is not paused"}

  defp mutable_schedule_record(ctx, id, opts) do
    with :ok <- validate_id(id),
         {:ok, now_ms} <- optional_now_ms(opts),
         {:ok, record} <-
           Flow.get(
             ctx,
             flow_id(id),
             Internal.put(
               partition_key: partition_key(id),
               payload: true,
               payload_max_bytes: schedule_definition_max_bytes()
             )
           ),
         :ok <- require_schedule_record(record) do
      {:ok, record, now_ms}
    end
  end

  defp schedule_resume_run_at(%{payload: %{next_run_at_ms: run_at_ms}}, _now_ms)
       when is_integer(run_at_ms),
       do: {:ok, run_at_ms}

  defp schedule_resume_run_at(_record, _now_ms),
    do: {:error, "ERR flow schedule has no next run time"}

  defp list_state(ctx, state, count) do
    schedule_partition_keys()
    |> Enum.flat_map(fn partition_key ->
      opts =
        Internal.put(
          state: state,
          partition_key: partition_key,
          count: count,
          include_cold: true,
          consistent_projection: true
        )

      result =
        if Ferricstore.Flow.LMDB.terminal_state?(state) do
          Flow.terminals(ctx, @schedule_type, opts)
        else
          Flow.list(ctx, @schedule_type, opts)
        end

      case result do
        {:ok, records} -> records
        {:error, _reason} -> []
      end
    end)
    |> Enum.map(&hydrate_schedule_record(ctx, &1))
    |> Enum.reject(&is_nil/1)
  end

  defp hydrate_schedule_record(ctx, %{id: @schedule_id_prefix <> id}) do
    case get(ctx, id) do
      {:ok, schedule} -> schedule
      _other -> nil
    end
  end

  defp hydrate_schedule_record(_ctx, _record), do: nil

  defp schedule_partition_keys do
    Enum.map(0..(@partition_buckets - 1), &schedule_partition_key/1)
  end

  defp schedule_partition_key(bucket),
    do: "__ferricstore_schedule__:" <> Integer.to_string(bucket)

  defp list_states(opts) do
    case Keyword.get(opts, :state, @active_state) do
      :all -> {:ok, ["active", "paused", "running", "completed", "failed", "cancelled"]}
      "all" -> {:ok, ["active", "paused", "running", "completed", "failed", "cancelled"]}
      state when is_binary(state) and state != "" -> {:ok, [state]}
      _ -> {:error, "ERR flow schedule state must be a non-empty string or :all"}
    end
  end

  defp list_count(opts) do
    case Keyword.get(opts, :count, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 1_000)}
      _ -> {:error, "ERR flow schedule count must be a positive integer"}
    end
  end

  defp list_filters(opts) do
    with {:ok, kind} <- optional_schedule_kind_filter(opts),
         {:ok, target_type} <- optional_binary_filter(opts, :target_type),
         {:ok, timezone} <- optional_binary_filter(opts, :timezone),
         {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil) do
      {:ok,
       %{
         kind: kind,
         target_type: target_type,
         timezone: normalize_timezone(timezone),
         from_ms: from_ms,
         to_ms: to_ms
       }}
    end
  end

  defp optional_schedule_kind_filter(opts) do
    case Keyword.get(opts, :kind) do
      nil -> {:ok, nil}
      kind when kind in [:one_shot, :delay, :interval, :cron] -> {:ok, kind}
      _ -> {:error, "ERR flow schedule kind must be :one_shot, :delay, :interval, or :cron"}
    end
  end

  defp optional_binary_filter(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow schedule #{key} must be a non-empty string"}
    end
  end

  defp schedule_matches_filters?(schedule, filters) do
    schedule_filter_match?(filters.kind, schedule.kind) and
      schedule_filter_match?(filters.target_type, get_in(schedule, [:target, :type])) and
      schedule_filter_match?(filters.timezone, Map.get(schedule, :timezone)) and
      schedule_due_in_range?(schedule.next_run_at_ms, filters.from_ms, filters.to_ms)
  end

  defp schedule_filter_match?(nil, _value), do: true
  defp schedule_filter_match?(value, value), do: true
  defp schedule_filter_match?(_expected, _value), do: false

  defp schedule_due_in_range?(nil, _from_ms, _to_ms), do: true
  defp schedule_due_in_range?(due_ms, nil, nil), do: is_integer(due_ms)
  defp schedule_due_in_range?(due_ms, from_ms, nil), do: is_integer(due_ms) and due_ms >= from_ms
  defp schedule_due_in_range?(due_ms, nil, to_ms), do: is_integer(due_ms) and due_ms <= to_ms

  defp schedule_due_in_range?(due_ms, from_ms, to_ms),
    do: is_integer(due_ms) and due_ms >= from_ms and due_ms <= to_ms

  defp schedule_sort_due(%{next_run_at_ms: due_ms}) when is_integer(due_ms), do: due_ms
  defp schedule_sort_due(_schedule), do: 9_223_372_036_854_775_807

  defp replace(ctx, flow_id, definition) do
    attrs = %{
      id: flow_id,
      type: @schedule_type,
      state: @active_state,
      partition_key: partition_key(Map.fetch!(definition, :id)),
      payload: definition,
      run_at_ms: Map.fetch!(definition, :next_run_at_ms),
      now_ms: Map.fetch!(definition, :created_at_ms)
    }

    case Router.flow_schedule_replace(ctx, attrs) do
      :ok -> {:ok, view(%{id: flow_id, state: @active_state, payload: definition})}
      {:ok, _record} -> {:ok, view(%{id: flow_id, state: @active_state, payload: definition})}
      {:error, _reason} = error -> error
    end
    |> tap(fn
      {:ok, _schedule} -> emit_schedule_event(ctx, definition, @schedule_event_created)
      _other -> :ok
    end)
  end

  defp definition(id, opts) do
    now_ms = Keyword.get(opts, :now_ms, now_ms())

    with {:ok, kind} <- schedule_kind(opts),
         {:ok, target} <- target_from_opts(opts),
         :ok <- validate_target_id_mode(kind, target),
         :ok <- validate_target_namespace(kind, target),
         :ok <- validate_inline_target_values(target),
         {:ok, timezone} <- timezone(kind, opts),
         {:ok, next_run_at_ms} <- initial_run_at_ms(kind, opts, now_ms, timezone),
         {:ok, every_ms} <- interval_ms(kind, opts),
         {:ok, cron} <- cron_expr(kind, opts),
         {:ok, overlap_policy} <- overlap_policy(kind, opts),
         {:ok, overlap_retry_ms} <- overlap_retry_ms(opts),
         {:ok, max_fires} <- max_fires(kind, opts),
         {:ok, end_at_ms} <- end_at_ms(kind, opts),
         :ok <- validate_initial_end_at_ms(next_run_at_ms, end_at_ms) do
      definition =
        %{
          id: id,
          kind: kind,
          target: target,
          created_at_ms: now_ms,
          next_run_at_ms: next_run_at_ms,
          fire_count: 0
        }
        |> maybe_put(:every_ms, every_ms)
        |> maybe_put(:cron, cron)
        |> maybe_put(:timezone, timezone)
        |> maybe_put(:overlap_policy, overlap_policy)
        |> maybe_put(:overlap_retry_ms, overlap_retry_ms)
        |> maybe_put(:max_fires, max_fires)
        |> maybe_put(:end_at_ms, end_at_ms)

      with :ok <- validate_definition_size(definition) do
        {:ok, definition}
      end
    end
  end

  defp schedule_kind(opts) do
    case Keyword.get(opts, :kind) do
      nil ->
        cond do
          Keyword.has_key?(opts, :cron) -> {:ok, :cron}
          Keyword.has_key?(opts, :every_ms) -> {:ok, :interval}
          Keyword.has_key?(opts, :delay_ms) -> {:ok, :delay}
          true -> {:ok, :one_shot}
        end

      kind when kind in [:one_shot, :delay, :interval, :cron] ->
        {:ok, kind}

      _ ->
        {:error, "ERR flow schedule kind must be :one_shot, :delay, :interval, or :cron"}
    end
  end

  defp initial_run_at_ms(:delay, opts, now_ms, _timezone) do
    case Keyword.get(opts, :delay_ms) do
      delay when is_integer(delay) and delay >= 0 -> {:ok, now_ms + delay}
      _ -> {:error, "ERR flow schedule delay_ms must be a non-negative integer"}
    end
  end

  defp initial_run_at_ms(:cron, opts, now_ms, timezone) do
    with {:ok, expr} <- cron_expr(:cron, opts),
         {:ok, start_at_ms} <-
           optional_non_neg_integer(opts, :start_at_ms, Keyword.get(opts, :at_ms, now_ms)) do
      next_cron_run_at_ms(expr, start_at_ms - 1, timezone)
    end
  end

  defp initial_run_at_ms(_kind, opts, now_ms, _timezone) do
    case Keyword.get(opts, :at_ms, Keyword.get(opts, :start_at_ms, now_ms)) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow schedule at_ms must be a non-negative integer"}
    end
  end

  defp interval_ms(:interval, opts) do
    case Keyword.get(opts, :every_ms) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow schedule every_ms must be a positive integer"}
    end
  end

  defp interval_ms(_kind, _opts), do: {:ok, nil}

  defp cron_expr(:cron, opts) do
    case Keyword.get(opts, :cron) do
      expr when is_binary(expr) and expr != "" ->
        with {:ok, _parsed} <- parse_cron(expr), do: {:ok, expr}

      _ ->
        {:error, "ERR flow schedule cron must be a non-empty string"}
    end
  end

  defp cron_expr(_kind, _opts), do: {:ok, nil}

  defp timezone(:cron, opts) do
    timezone = opts |> Keyword.get(:timezone, @default_timezone) |> normalize_timezone()

    case timezone do
      value when is_binary(value) and value != "" ->
        with {:ok, _datetime} <- cron_datetime(0, value), do: {:ok, value}

      _ ->
        {:error, "ERR flow schedule timezone must be a non-empty string"}
    end
  end

  defp timezone(_kind, opts) do
    if Keyword.has_key?(opts, :timezone) do
      {:error, "ERR flow schedule timezone is only supported for cron schedules"}
    else
      {:ok, nil}
    end
  end

  defp normalize_timezone("UTC"), do: @default_timezone
  defp normalize_timezone(timezone), do: timezone

  defp overlap_policy(kind, opts) when kind in [:interval, :cron] do
    case Keyword.get(opts, :overlap_policy, :allow) do
      policy when policy in [:allow, :skip, :queue_after_previous, :fail_schedule] ->
        {:ok, policy}

      _ ->
        {:error,
         "ERR flow schedule overlap_policy must be :allow, :skip, :queue_after_previous, or :fail_schedule"}
    end
  end

  defp overlap_policy(_kind, opts) do
    if Keyword.has_key?(opts, :overlap_policy) do
      {:error, "ERR flow schedule overlap_policy is only supported for recurring schedules"}
    else
      {:ok, nil}
    end
  end

  defp overlap_retry_ms(opts) do
    case Keyword.get(opts, :overlap_retry_ms) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow schedule overlap_retry_ms must be a positive integer"}
    end
  end

  defp max_fires(kind, opts) when kind in [:interval, :cron] do
    case Keyword.get(opts, :max_fires) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, "ERR flow schedule max_fires must be a positive integer"}
    end
  end

  defp max_fires(_kind, opts) do
    if Keyword.has_key?(opts, :max_fires) do
      {:error, "ERR flow schedule max_fires is only supported for recurring schedules"}
    else
      {:ok, nil}
    end
  end

  defp end_at_ms(kind, opts) when kind in [:interval, :cron] do
    optional_non_neg_integer(opts, :end_at_ms, nil)
  end

  defp end_at_ms(_kind, opts) do
    if Keyword.has_key?(opts, :end_at_ms) do
      {:error, "ERR flow schedule end_at_ms is only supported for recurring schedules"}
    else
      {:ok, nil}
    end
  end

  defp validate_initial_end_at_ms(_next_run_at_ms, nil), do: :ok

  defp validate_initial_end_at_ms(next_run_at_ms, end_at_ms) when next_run_at_ms <= end_at_ms,
    do: :ok

  defp validate_initial_end_at_ms(_next_run_at_ms, _end_at_ms),
    do: {:error, "ERR flow schedule end_at_ms must be at or after first run"}

  defp next_run_at_ms(%{kind: :delay}, due_at_ms), do: {:ok, due_at_ms}

  defp next_run_at_ms(%{kind: :interval, every_ms: every_ms}, due_at_ms),
    do: {:ok, due_at_ms + every_ms}

  defp next_run_at_ms(%{kind: :cron, cron: expr} = definition, due_at_ms),
    do: next_cron_run_at_ms(expr, due_at_ms, Map.get(definition, :timezone, @default_timezone))

  defp target_from_opts(opts) do
    case Keyword.get(opts, :target) do
      target when is_list(target) -> normalize_target(target)
      target when is_map(target) -> target |> normalize_target_map() |> normalize_target()
      _ -> {:error, "ERR flow schedule target is required"}
    end
  end

  defp normalize_target_map(target) do
    Enum.map(target, fn {key, value} -> {normalize_target_key(key), value} end)
  end

  defp normalize_target_key(key) when is_atom(key), do: key

  defp normalize_target_key(key) when is_binary(key) do
    case key do
      "correlation_id" -> :correlation_id
      "id" -> :id
      "id_prefix" -> :id_prefix
      "parent_flow_id" -> :parent_flow_id
      "partition_key" -> :partition_key
      "payload" -> :payload
      "payload_ref" -> :payload_ref
      "priority" -> :priority
      "root_flow_id" -> :root_flow_id
      "run_at_ms" -> :run_at_ms
      "state" -> :state
      "type" -> :type
      "value_refs" -> :value_refs
      "values" -> :values
      _ -> key
    end
  end

  defp normalize_target_key(key), do: key

  defp normalize_target(target_opts) do
    with {:ok, type} <- required_binary(target_opts, :type),
         {:ok, state} <- optional_binary(target_opts, :state, @default_state),
         {:ok, id} <- optional_binary(target_opts, :id, nil),
         {:ok, id_prefix} <- optional_binary(target_opts, :id_prefix, nil),
         {:ok, partition_key} <- optional_binary(target_opts, :partition_key, nil),
         {:ok, run_at_ms} <- optional_non_neg_integer(target_opts, :run_at_ms, nil) do
      target =
        %{
          type: type,
          state: state
        }
        |> maybe_put(:id, id)
        |> maybe_put(:id_prefix, id_prefix)
        |> maybe_put(:partition_key, partition_key)
        |> maybe_put(:run_at_ms, run_at_ms)
        |> maybe_put(:priority, Keyword.get(target_opts, :priority))
        |> maybe_put(:correlation_id, Keyword.get(target_opts, :correlation_id))
        |> maybe_put(:parent_flow_id, Keyword.get(target_opts, :parent_flow_id))
        |> maybe_put(:root_flow_id, Keyword.get(target_opts, :root_flow_id))
        |> maybe_put(:payload, Keyword.get(target_opts, :payload))
        |> maybe_put(:payload_ref, Keyword.get(target_opts, :payload_ref))
        |> maybe_put(:values, Keyword.get(target_opts, :values))
        |> maybe_put(:value_refs, Keyword.get(target_opts, :value_refs))

      {:ok, target}
    end
  end

  defp target_id(%{target: %{id: id}, kind: kind}, _due_at_ms, _fire_count)
       when kind in [:one_shot, :delay],
       do: id

  defp target_id(%{target: %{id: _id}}, _due_at_ms, _fire_count),
    do: raise(ArgumentError, "recurring schedule target must use :id_prefix, not fixed :id")

  defp target_id(%{id: schedule_id, target: target}, due_at_ms, fire_count) do
    prefix = Map.get(target, :id_prefix) || schedule_id
    prefix <> ":" <> Integer.to_string(due_at_ms) <> ":" <> Integer.to_string(fire_count)
  end

  defp view(%{payload: definition} = record) when is_map(definition) do
    %{
      id: Map.fetch!(definition, :id),
      flow_id: Map.get(record, :id),
      state: Map.get(record, :state),
      kind: Map.fetch!(definition, :kind),
      next_run_at_ms: Map.get(definition, :next_run_at_ms),
      fire_count: Map.get(definition, :fire_count, 0),
      attempts: Map.get(record, :attempts, 0),
      last_fire_at_ms: Map.get(definition, :last_fire_at_ms),
      last_target_id: Map.get(definition, :last_target_id),
      last_overlap_at_ms: Map.get(definition, :last_overlap_at_ms),
      last_overlap_target_id: Map.get(definition, :last_overlap_target_id),
      last_overlap_reason: Map.get(definition, :last_overlap_reason),
      last_skipped_at_ms: Map.get(definition, :last_skipped_at_ms),
      skipped_count: Map.get(definition, :skipped_count, 0),
      overlap_policy: Map.get(definition, :overlap_policy, :allow),
      overlap_queued_due_at_ms: Map.get(definition, :overlap_queued_due_at_ms),
      max_fires: Map.get(definition, :max_fires),
      end_at_ms: Map.get(definition, :end_at_ms),
      end_reason: Map.get(definition, :end_reason),
      timezone: Map.get(definition, :timezone),
      target: Map.get(definition, :target)
    }
  end

  defp view(record), do: record

  defp validate_target_id_mode(:interval, %{id: _id}),
    do: {:error, "ERR recurring schedule target must use id_prefix, not id"}

  defp validate_target_id_mode(:cron, %{id: _id}),
    do: {:error, "ERR recurring schedule target must use id_prefix, not id"}

  defp validate_target_id_mode(_kind, _target), do: :ok

  defp validate_target_namespace(_kind, %{id: id}) when is_binary(id) do
    if Internal.reserved_id?(id) do
      {:error, "ERR scheduled target id is reserved for internal use"}
    else
      :ok
    end
  end

  defp validate_target_namespace(_kind, %{id_prefix: prefix}) when is_binary(prefix) do
    if Internal.reserved_id?(prefix) do
      {:error, "ERR scheduled target id_prefix is reserved for internal use"}
    else
      :ok
    end
  end

  defp validate_target_namespace(_kind, _target), do: :ok

  defp validate_inline_target_values(target) do
    with :ok <- validate_inline_target_value(target, :payload),
         :ok <- validate_inline_target_value(target, :values) do
      :ok
    end
  end

  defp validate_inline_target_value(target, key) do
    case Map.fetch(target, key) do
      {:ok, value} ->
        if :erlang.external_size(value) <= inline_target_value_max_bytes() do
          :ok
        else
          {:error, "ERR flow schedule #{key} too large; use #{key}_ref/value_refs"}
        end

      :error ->
        :ok
    end
  end

  defp validate_definition_size(definition) do
    if :erlang.external_size(definition) <= schedule_definition_max_bytes() do
      :ok
    else
      {:error, "ERR flow schedule definition too large; use payload_ref/value_refs"}
    end
  end

  defp next_cron_run_at_ms(expr, after_ms, timezone) do
    with {:ok, cron} <- parse_cron(expr),
         {:ok, _datetime} <- cron_datetime(0, timezone) do
      start_ms = div(max(after_ms + @minute_ms, 0), @minute_ms) * @minute_ms

      0..cron_search_minutes()
      |> Enum.find_value(fn offset ->
        candidate_ms = start_ms + offset * @minute_ms

        with {:ok, datetime} <- cron_datetime(candidate_ms, timezone) do
          if cron_match?(cron, datetime), do: {:ok, candidate_ms}
        end
      end)
      |> case do
        {:ok, value} -> {:ok, value}
        nil -> {:error, "ERR flow schedule cron has no matching time in search window"}
      end
    end
  end

  defp parse_cron(expr) do
    case String.split(expr, ~r/\s+/, trim: true) do
      [minute, hour, day, month, weekday] ->
        with {:ok, minute_set, _minute_any?} <- cron_field(minute, 0, 59, %{}),
             {:ok, hour_set, _hour_any?} <- cron_field(hour, 0, 23, %{}),
             {:ok, day_set, day_any?} <- cron_field(day, 1, 31, %{}),
             {:ok, month_set, _month_any?} <- cron_field(month, 1, 12, month_aliases()),
             {:ok, weekday_set, weekday_any?} <- cron_field(weekday, 0, 7, weekday_aliases()) do
          {:ok,
           %{
             minute: minute_set,
             hour: hour_set,
             day: day_set,
             day_any?: day_any?,
             month: month_set,
             weekday: normalize_weekday_set(weekday_set),
             weekday_any?: weekday_any?
           }}
        end

      _ ->
        {:error, "ERR flow schedule cron must have 5 fields"}
    end
  end

  defp cron_field(field, min, max, aliases) do
    any? = field in ["*", "?"]
    parts = if any?, do: ["*"], else: String.split(field, ",", trim: true)

    parts
    |> Enum.reduce_while({:ok, MapSet.new(), any?}, fn part, {:ok, acc, any?} ->
      case cron_part(part, min, max, aliases) do
        {:ok, values} -> {:cont, {:ok, MapSet.union(acc, values), any?}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp cron_part(part, min, max, aliases) do
    [range_part, step_part] = split_cron_step(part)

    with {:ok, step} <- cron_step(step_part),
         {:ok, first, last} <- cron_range(range_part, min, max, aliases) do
      if step > 0 do
        values =
          first
          |> Stream.iterate(&(&1 + step))
          |> Enum.take_while(&(&1 <= last))
          |> MapSet.new()

        {:ok, values}
      else
        {:error, "ERR flow schedule cron step must be positive"}
      end
    end
  end

  defp split_cron_step(part) do
    case String.split(part, "/", parts: 2) do
      [range_part] -> [range_part, nil]
      [range_part, step_part] -> [range_part, step_part]
    end
  end

  defp cron_step(nil), do: {:ok, 1}

  defp cron_step(value) do
    case Integer.parse(value) do
      {step, ""} when step > 0 -> {:ok, step}
      _ -> {:error, "ERR flow schedule cron step must be positive"}
    end
  end

  defp cron_range("*", min, max, _aliases), do: {:ok, min, max}
  defp cron_range("?", min, max, _aliases), do: {:ok, min, max}

  defp cron_range(value, min, max, aliases) do
    case String.split(value, "-", parts: 2) do
      [single] ->
        with {:ok, parsed} <- cron_value(single, aliases),
             :ok <- cron_value_in_range(parsed, min, max) do
          {:ok, parsed, parsed}
        end

      [first, last] ->
        with {:ok, parsed_first} <- cron_value(first, aliases),
             {:ok, parsed_last} <- cron_value(last, aliases),
             :ok <- cron_value_in_range(parsed_first, min, max),
             :ok <- cron_value_in_range(parsed_last, min, max),
             :ok <- cron_range_order(parsed_first, parsed_last) do
          {:ok, parsed_first, parsed_last}
        end
    end
  end

  defp cron_value(value, aliases) do
    normalized = String.upcase(value)

    case Map.fetch(aliases, normalized) do
      {:ok, aliased} ->
        {:ok, aliased}

      :error ->
        case Integer.parse(value) do
          {parsed, ""} -> {:ok, parsed}
          _ -> {:error, "ERR flow schedule cron field is invalid"}
        end
    end
  end

  defp cron_value_in_range(value, min, max) when value >= min and value <= max, do: :ok

  defp cron_value_in_range(_value, _min, _max),
    do: {:error, "ERR flow schedule cron value out of range"}

  defp cron_range_order(first, last) when first <= last, do: :ok
  defp cron_range_order(_first, _last), do: {:error, "ERR flow schedule cron range is invalid"}

  defp cron_datetime(ms, timezone) do
    datetime = DateTime.from_unix!(ms, :millisecond)

    case DateTime.shift_zone(datetime, timezone, Tz.TimeZoneDatabase) do
      {:ok, shifted} -> {:ok, shifted}
      {:error, _reason} -> {:error, "ERR flow schedule timezone is invalid or unavailable"}
    end
  end

  defp cron_match?(cron, datetime) do
    weekday = datetime |> Date.day_of_week() |> rem(7)

    MapSet.member?(cron.minute, datetime.minute) and
      MapSet.member?(cron.hour, datetime.hour) and
      MapSet.member?(cron.month, datetime.month) and
      cron_day_match?(cron, datetime.day, weekday)
  end

  defp cron_day_match?(%{day_any?: true, weekday_any?: true}, _day, _weekday), do: true

  defp cron_day_match?(%{day_any?: true} = cron, _day, weekday),
    do: MapSet.member?(cron.weekday, weekday)

  defp cron_day_match?(%{weekday_any?: true} = cron, day, _weekday),
    do: MapSet.member?(cron.day, day)

  defp cron_day_match?(cron, day, weekday),
    do: MapSet.member?(cron.day, day) or MapSet.member?(cron.weekday, weekday)

  defp normalize_weekday_set(set) do
    set
    |> Enum.map(fn
      7 -> 0
      value -> value
    end)
    |> MapSet.new()
  end

  defp month_aliases do
    %{
      "JAN" => 1,
      "FEB" => 2,
      "MAR" => 3,
      "APR" => 4,
      "MAY" => 5,
      "JUN" => 6,
      "JUL" => 7,
      "AUG" => 8,
      "SEP" => 9,
      "OCT" => 10,
      "NOV" => 11,
      "DEC" => 12
    }
  end

  defp weekday_aliases do
    %{
      "SUN" => 0,
      "MON" => 1,
      "TUE" => 2,
      "WED" => 3,
      "THU" => 4,
      "FRI" => 5,
      "SAT" => 6
    }
  end

  defp partition_key(id) do
    bucket = :erlang.phash2(id, @partition_buckets)
    "__ferricstore_schedule__:" <> Integer.to_string(bucket)
  end

  defp normalize_fire_result({:ok, %{errors: []} = result}), do: {:ok, %{result | errors: []}}

  defp normalize_fire_result({:ok, result}),
    do: {:ok, %{result | errors: Enum.reverse(result.errors)}}

  defp normalize_fire_result(other), do: other

  defp ok_result(:ok), do: :ok
  defp ok_result({:ok, _record}), do: :ok
  defp ok_result({:error, _reason} = error), do: error

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow schedule id must be a non-empty string"}

  defp required_binary(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow schedule #{key} must be a non-empty string"}
    end
  end

  defp optional_binary(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow schedule #{key} must be a non-empty string"}
    end
  end

  defp optional_non_neg_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow schedule #{key} must be a non-negative integer"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, "ERR flow schedule now_ms must be a non-negative integer"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow schedule #{key} must be a boolean"}
    end
  end

  defp maybe_put(list, _key, nil) when is_list(list), do: list
  defp maybe_put(list, key, value) when is_list(list), do: Keyword.put(list, key, value)
  defp maybe_put(map, _key, nil) when is_map(map), do: map
  defp maybe_put(map, key, value) when is_map(map), do: Map.put(map, key, value)

  defp default_worker, do: "ferricstore-scheduler:" <> Atom.to_string(node())
  defp now_ms, do: System.system_time(:millisecond)

  defp schedule_definition_max_bytes do
    env_pos_integer(:flow_schedule_definition_max_bytes, @default_definition_max_bytes)
  end

  defp inline_target_value_max_bytes do
    env_pos_integer(:flow_schedule_inline_value_max_bytes, @default_inline_value_max_bytes)
  end

  defp cron_search_minutes do
    :flow_schedule_cron_search_minutes
    |> env_pos_integer(@default_cron_search_minutes)
    |> min(@max_cron_search_minutes)
  end

  defp env_pos_integer(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end
end
