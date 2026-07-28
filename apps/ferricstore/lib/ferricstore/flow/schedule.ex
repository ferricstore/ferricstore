defmodule Ferricstore.Flow.Schedule do
  @moduledoc """
  Durable FerricFlow schedules.

  Schedules are stored as internal Flow records and fired by claiming due
  schedule records. That keeps scheduling distributed-safe without a second
  coordination path: only the shard leader can lease a due schedule, and every
  fire/reschedule is still guarded by Flow fencing tokens.

  Interval schedules coalesce missed periods with the `:fire_once` catch-up
  policy. Catch-up is independent from target overlap handling and takes
  constant time regardless of how many periods elapsed.
  """

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow
  alias Ferricstore.Flow.{ClaimDueAPI, Internal, InternalLimits, Keys, MutationAttrs}

  alias Ferricstore.Flow.Schedule.{
    Catchup,
    Cron,
    Dispatcher,
    Limits,
    Listing,
    Metadata,
    ResourceGovernance,
    Summary,
    TargetOwnership
  }

  alias Ferricstore.Store.Router

  @schedule_type "__ferricstore_schedule"
  @active_state "active"
  @paused_state "paused"
  @default_state "queued"
  @default_lease_ms 30_000
  @dispatch_wave_size 16
  @dispatch_concurrency 8
  @default_overlap_retry_ms 1_000
  @schedule_event_created "schedule_created"
  @schedule_event_fired "schedule_fired"
  @schedule_event_skipped_overlap "schedule_skipped_overlap"
  @schedule_event_failed_overlap "schedule_failed_overlap"
  @schedule_event_failed_planning "schedule_failed_planning"
  @schedule_event_deleted "schedule_deleted"
  @partition_buckets 256
  @cron_search_minutes 8 * 366 * 24 * 60
  @default_definition_max_bytes 32 * 1024
  @default_inline_value_max_bytes 8 * 1024
  @default_timezone "Etc/UTC"
  @max_exact_integer 9_007_199_254_740_991
  @terminal_states ["completed", "failed", "cancelled"]
  @timestamp_limit_error "ERR flow schedule next_run_at_ms exceeds maximum #{@max_exact_integer}"
  @calendar_range_error "ERR flow schedule timestamp is outside supported calendar range"
  @create_option_keys [
    :at_ms,
    :catchup_policy,
    :cron,
    :delay_ms,
    :end_at_ms,
    :every_ms,
    :kind,
    :max_fires,
    :now_ms,
    :overlap_policy,
    :overlap_retry_ms,
    :overwrite,
    :start_at_ms,
    :target,
    :timezone
  ]
  @get_option_keys [:partition_key, :payload, :payload_max_bytes]
  @fire_option_keys [:fire_at_ms, :now_ms]
  @status_option_keys [:now_ms]
  @fire_due_option_keys [:block_ms, :lease_ms, :limit, :now_ms, :worker]
  @target_option_keys [
    :correlation_id,
    :id,
    :id_prefix,
    :parent_flow_id,
    :partition_key,
    :payload,
    :payload_ref,
    :priority,
    :root_flow_id,
    :run_at_ms,
    :state,
    :type,
    :value_refs,
    :values
  ]

  @type schedule_id :: binary()

  @spec create(FerricStore.Instance.t(), schedule_id(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def create(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_option_fields(opts, @create_option_keys, "option"),
         :ok <- validate_id(id),
         {:ok, overwrite?} <- optional_boolean(opts, :overwrite, false),
         {:ok, definition} <- definition(id, opts) do
      flow_id = flow_id(id)

      create_opts = [
        type: @schedule_type,
        state: @active_state,
        partition_key: partition_key(id),
        schedule_metadata: Metadata.from_definition(definition),
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
    with :ok <- validate_opts(opts),
         :ok <- validate_option_fields(opts, @get_option_keys, "option"),
         :ok <- validate_id(id) do
      ctx
      |> Flow.get(
        flow_id(id),
        Keyword.merge(
          Internal.put(opts),
          partition_key: partition_key(id),
          payload: true,
          payload_max_bytes: schedule_hydration_max_bytes()
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
    with :ok <- validate_opts(opts),
         :ok <- validate_option_fields(opts, @fire_option_keys, "option"),
         :ok <- validate_id(id),
         {:ok, now_ms} <- optional_now_ms(opts),
         {:ok, fire_at_ms} <- optional_non_neg_integer(opts, :fire_at_ms, now_ms),
         {:ok, record} <-
           Flow.get(
             ctx,
             flow_id(id),
             Internal.put(
               partition_key: partition_key(id),
               payload: true,
               payload_max_bytes: schedule_hydration_max_bytes()
             )
           ),
         :ok <- require_schedule_record(record),
         :ok <- require_active_schedule(record),
         {:ok, claimed} <- claim_manual_schedule(ctx, record) do
      fire_manual_one(ctx, claimed, fire_at_ms, now_ms)
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
    Listing.list(ctx, opts)
  end

  def list(_ctx, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  @spec delete(FerricStore.Instance.t(), schedule_id(), keyword()) :: :ok | {:error, binary()}
  def delete(ctx, id, opts \\ [])

  def delete(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_option_fields(opts, @status_option_keys, "option"),
         :ok <- validate_id(id),
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
    with :ok <- validate_opts(opts),
         :ok <- validate_option_fields(opts, @fire_due_option_keys, "option"),
         {:ok, limit} <- ClaimDueAPI.validate_claim_limit(opts) do
      configured_now_ms = Keyword.get(opts, :now_ms)
      worker = Keyword.get(opts, :worker, default_worker())
      lease_ms = Keyword.get(opts, :lease_ms, @default_lease_ms)

      base_claim_opts =
        [
          state: @active_state,
          partition_key: :any,
          worker: worker,
          lease_ms: lease_ms,
          payload: true,
          payload_max_bytes: schedule_hydration_max_bytes()
        ]
        |> maybe_put(:now_ms, configured_now_ms)
        |> Internal.put()

      claim = fn wave_limit, first? ->
        block_ms = if first?, do: Keyword.get(opts, :block_ms)

        claim_opts =
          base_claim_opts
          |> Keyword.put(:limit, wave_limit)
          |> maybe_put(:block_ms, block_ms)

        Flow.claim_due(ctx, @schedule_type, claim_opts)
      end

      case Dispatcher.run(
             limit,
             @dispatch_wave_size,
             @dispatch_concurrency,
             claim,
             &fire_one(ctx, &1, configured_now_ms || now_ms())
           ) do
        {:ok, results, claim_error} -> Summary.fire_results(results, claim_error)
        {:error, _reason} = error -> error
      end
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
        execute_schedule_fire(ctx, record, definition, target_id, due_at_ms, fire_count, now_ms)

      {:skip, reason} ->
        with {:ok, coalesced_count} <-
               skip_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_skipped_overlap)
          {:skipped, reason, coalesced_count}
        end

      {:queue, reason} ->
        with :ok <- queue_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_skipped_overlap)
          {:skipped, reason, 0}
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
        execute_manual_schedule_fire(
          ctx,
          record,
          definition,
          target_id,
          fire_at_ms,
          fire_count,
          now_ms
        )

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

  defp execute_schedule_fire(
         ctx,
         record,
         definition,
         target_id,
         due_at_ms,
         fire_count,
         now_ms
       ) do
    case plan_schedule_fire(definition, target_id, due_at_ms, fire_count, now_ms) do
      {:ok, plan} ->
        with :ok <- create_target(ctx, definition, target_id, now_ms),
             {:ok, coalesced_count} <-
               apply_schedule_fire_plan(ctx, record, plan, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_fired)
          {:ok, target_id, coalesced_count}
        end

      {:error, reason} when is_binary(reason) ->
        fail_schedule_planning(ctx, record, definition, reason, now_ms)

      {:error, _reason} = error ->
        error
    end
  end

  defp execute_manual_schedule_fire(
         ctx,
         record,
         definition,
         target_id,
         fire_at_ms,
         fire_count,
         now_ms
       ) do
    case plan_manual_schedule_fire(definition, target_id, fire_at_ms, fire_count) do
      {:ok, plan} ->
        with :ok <- create_target(ctx, definition, target_id, now_ms),
             {:ok, schedule} <-
               apply_manual_schedule_fire_plan(ctx, record, plan, now_ms) do
          emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_fired)
          {:ok, %{fired: 1, target_id: target_id, schedule: schedule}}
        end

      {:error, reason} when is_binary(reason) ->
        fail_schedule_planning(ctx, record, definition, reason, now_ms)

      {:error, _reason} = error ->
        error
    end
  end

  defp claim_manual_schedule(ctx, record) do
    attrs = %{
      id: Map.fetch!(record, :id),
      type: @schedule_type,
      mode: :claim,
      expected_version: Map.fetch!(record, :version),
      partition_key: Map.get(record, :partition_key),
      worker: default_worker() <> ":manual",
      lease_ms: @default_lease_ms
    }

    case Router.flow_schedule_replace(ctx, attrs) do
      {:ok, _claimed} -> hydrate_claimed_schedule(ctx, record)
      :ok -> hydrate_claimed_schedule(ctx, record)
      {:error, _reason} = error -> error
    end
  end

  defp hydrate_claimed_schedule(ctx, record) do
    Flow.get(
      ctx,
      Map.fetch!(record, :id),
      Internal.put(
        partition_key: Map.get(record, :partition_key),
        payload: true,
        payload_max_bytes: schedule_hydration_max_bytes()
      )
    )
  end

  defp plan_schedule_fire(
         %{kind: kind} = definition,
         target_id,
         due_at_ms,
         fire_count,
         _now_ms
       )
       when kind in [:one_shot, :delay] do
    completed_definition =
      definition
      |> Map.put(:fire_count, fire_count)
      |> Map.put(:last_fire_at_ms, due_at_ms)
      |> Map.put(:last_target_id, target_id)
      |> Map.put(:end_reason, "one_shot_fired")
      |> Map.delete(:next_run_at_ms)

    {:ok,
     %{
       state: "completed",
       definition: completed_definition,
       result: %{target_id: target_id, fire_count: fire_count, fired_at_ms: due_at_ms},
       coalesced_count: 0
     }}
  end

  defp plan_schedule_fire(definition, target_id, due_at_ms, fire_count, now_ms) do
    case next_automatic_run_at_ms(definition, due_at_ms, now_ms) do
      {:ok, next_run_at_ms, coalesced_count} ->
        next_definition =
          definition
          |> next_definition_after_fire(target_id, due_at_ms, fire_count, next_run_at_ms)
          |> Catchup.record(now_ms, coalesced_count)

        case recurring_end_reason(next_definition, next_run_at_ms, fire_count) do
          nil ->
            {:ok,
             %{
               state: @active_state,
               definition: next_definition,
               run_at_ms: next_run_at_ms,
               coalesced_count: coalesced_count
             }}

          reason ->
            {:ok,
             completed_fire_plan(
               next_definition,
               target_id,
               due_at_ms,
               fire_count,
               reason,
               coalesced_count
             )}
        end

      {:complete, :end_at_ms, coalesced_count} ->
        reason = completion_reason(definition, fire_count, "end_at_ms")

        definition =
          definition
          |> Catchup.record(now_ms, coalesced_count)
          |> completed_definition_after_fire(target_id, due_at_ms, fire_count, reason)

        {:ok,
         completed_fire_plan(
           definition,
           target_id,
           due_at_ms,
           fire_count,
           reason,
           coalesced_count
         )}

      {:error, :timestamp_limit, coalesced_count} ->
        definition =
          definition
          |> Catchup.record(now_ms, coalesced_count)
          |> completed_definition_after_fire(
            target_id,
            due_at_ms,
            fire_count,
            "timestamp_limit"
          )

        {:ok,
         completed_fire_plan(
           definition,
           target_id,
           due_at_ms,
           fire_count,
           "timestamp_limit",
           coalesced_count
         )}

      {:error, planning_error}
      when planning_error in [@timestamp_limit_error, @calendar_range_error] ->
        definition =
          completed_definition_after_fire(
            definition,
            target_id,
            due_at_ms,
            fire_count,
            "timestamp_limit"
          )

        {:ok,
         completed_fire_plan(
           definition,
           target_id,
           due_at_ms,
           fire_count,
           "timestamp_limit",
           0
         )}

      {:error, _reason} = error ->
        error
    end
  end

  defp completed_fire_plan(
         definition,
         target_id,
         due_at_ms,
         fire_count,
         reason,
         coalesced_count
       ) do
    completed_definition =
      definition
      |> Map.put(:end_reason, reason)
      |> Map.delete(:next_run_at_ms)

    %{
      state: "completed",
      definition: completed_definition,
      result: %{
        target_id: target_id,
        fire_count: fire_count,
        fired_at_ms: due_at_ms,
        end_reason: reason
      },
      coalesced_count: coalesced_count
    }
  end

  defp apply_schedule_fire_plan(
         ctx,
         record,
         %{state: @active_state} = plan,
         now_ms
       ) do
    Flow.reschedule(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        state: @active_state,
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        payload: Map.fetch!(plan, :definition),
        run_at_ms: Map.fetch!(plan, :run_at_ms),
        now_ms: now_ms
      )
    )
    |> ok_result()
    |> with_coalesced_count(Map.fetch!(plan, :coalesced_count))
  end

  defp apply_schedule_fire_plan(ctx, record, %{state: "completed"} = plan, now_ms) do
    Flow.complete(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        payload: Map.fetch!(plan, :definition),
        result: Map.fetch!(plan, :result),
        now_ms: now_ms
      )
    )
    |> internal_terminal_result()
    |> with_coalesced_count(Map.fetch!(plan, :coalesced_count))
  end

  defp complete_skipped_schedule(ctx, record, definition, reason, now_ms) do
    completed_definition = completed_skipped_definition(definition, reason)

    Flow.complete(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        payload: completed_definition,
        result: %{
          fire_count: Map.get(completed_definition, :fire_count, 0),
          skipped_count: Map.get(completed_definition, :skipped_count, 0),
          end_reason: reason
        },
        now_ms: now_ms
      )
    )
    |> internal_terminal_result()
  end

  defp internal_terminal_result(result), do: ok_result(result)

  defp with_coalesced_count(:ok, coalesced_count), do: {:ok, coalesced_count}
  defp with_coalesced_count({:error, _reason} = error, _coalesced_count), do: error

  defp overlap_action(ctx, definition) do
    case Map.get(definition, :overlap_policy, :allow) do
      :allow ->
        :allow

      policy when policy in [:skip, :queue_after_previous, :fail_schedule] ->
        constrained_overlap_action(ctx, definition, policy)

      _invalid ->
        {:error, "ERR flow schedule overlap_policy is invalid"}
    end
  end

  defp constrained_overlap_action(ctx, definition, policy) do
    case previous_target_active?(ctx, definition) do
      {:ok, nil} ->
        :allow

      {:ok, previous_target_id} ->
        reason = "previous target still active: #{previous_target_id}"

        case policy do
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

        case Router.flow_consistent_state(ctx, target_id, partition_key) do
          {:ok, nil} ->
            {:ok, nil}

          {:ok, state} when is_binary(state) ->
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
    case next_automatic_run_at_ms(definition, due_at_ms, now_ms) do
      {:ok, next_run_at_ms, coalesced_count} ->
        next_definition =
          definition
          |> skipped_definition(due_at_ms, next_run_at_ms, reason, now_ms)
          |> Catchup.record(now_ms, coalesced_count)

        result =
          case recurring_end_reason(
                 next_definition,
                 next_run_at_ms,
                 Map.get(next_definition, :fire_count, 0)
               ) do
            nil ->
              reschedule_definition(ctx, record, next_definition, next_run_at_ms, now_ms)

            end_reason ->
              complete_skipped_schedule(ctx, record, next_definition, end_reason, now_ms)
          end

        with_coalesced_count(result, coalesced_count)

      {:complete, :end_at_ms, coalesced_count} ->
        definition
        |> skipped_definition(due_at_ms, due_at_ms, reason, now_ms)
        |> Catchup.record(now_ms, coalesced_count)
        |> then(&complete_skipped_schedule(ctx, record, &1, "end_at_ms", now_ms))
        |> with_coalesced_count(coalesced_count)

      {:error, :timestamp_limit, coalesced_count} ->
        definition
        |> skipped_definition(due_at_ms, due_at_ms, reason, now_ms)
        |> Catchup.record(now_ms, coalesced_count)
        |> then(&complete_skipped_schedule(ctx, record, &1, "timestamp_limit", now_ms))
        |> with_coalesced_count(coalesced_count)

      {:error, reason} when reason in [@timestamp_limit_error, @calendar_range_error] ->
        definition
        |> skipped_definition(due_at_ms, due_at_ms, reason, now_ms)
        |> then(&complete_skipped_schedule(ctx, record, &1, "timestamp_limit", now_ms))
        |> with_coalesced_count(0)

      {:error, planning_error} when is_binary(planning_error) ->
        fail_schedule_planning(ctx, record, definition, planning_error, now_ms)

      {:error, _reason} = error ->
        error
    end
  end

  defp queue_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
    retry_ms = Map.get(definition, :overlap_retry_ms, @default_overlap_retry_ms)

    case safe_schedule_add(now_ms, retry_ms, :next_run_at_ms) do
      {:ok, next_run_at_ms} ->
        next_definition =
          queued_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms)

        reschedule_definition(ctx, record, next_definition, next_run_at_ms, now_ms)

      {:error, @timestamp_limit_error} ->
        definition
        |> terminal_queued_definition(due_at_ms, reason, now_ms)
        |> then(&complete_skipped_schedule(ctx, record, &1, "timestamp_limit", now_ms))
    end
  end

  defp fail_schedule_overlap(ctx, record, definition, reason, now_ms) do
    failed_definition = failed_overlap_definition(definition, reason, now_ms)

    Flow.fail(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        error: %{
          reason: reason,
          schedule_id: Map.fetch!(definition, :id),
          previous_target_id: Map.get(definition, :last_target_id)
        },
        payload: failed_definition,
        now_ms: now_ms
      )
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
    failed_definition = failed_overlap_definition(definition, reason, now_ms)

    case replace_claimed_with_state(ctx, record, failed_definition, "failed", now_ms, now_ms) do
      {:ok, _schedule} ->
        emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_failed_overlap)
        {:error, reason}

      {:error, _reason} = error ->
        error
    end
  end

  defp fail_schedule_planning(ctx, record, definition, reason, now_ms) do
    failed_definition =
      definition
      |> Map.put(:end_reason, "planning_failed")
      |> Map.put(:last_planning_error, reason)
      |> Map.delete(:next_run_at_ms)
      |> Map.delete(:overlap_queued_due_at_ms)

    Flow.fail(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        error: %{reason: reason, schedule_id: Map.fetch!(definition, :id)},
        payload: failed_definition,
        now_ms: now_ms
      )
    )
    |> case do
      :ok -> {:error, reason}
      {:ok, _record} -> {:error, reason}
      {:error, _reason} = error -> error
    end
    |> tap(fn
      {:error, ^reason} ->
        emit_schedule_event(ctx, record, definition, now_ms, @schedule_event_failed_planning)

      _other ->
        :ok
    end)
  end

  defp plan_manual_schedule_fire(
         %{kind: kind} = definition,
         target_id,
         due_at_ms,
         fire_count
       )
       when kind in [:one_shot, :delay] do
    completed_definition =
      definition
      |> Map.put(:fire_count, fire_count)
      |> Map.put(:last_fire_at_ms, due_at_ms)
      |> Map.put(:last_target_id, target_id)
      |> Map.put(:end_reason, "one_shot_fired")
      |> Map.delete(:next_run_at_ms)

    {:ok,
     %{
       state: "completed",
       definition: completed_definition,
       run_at_ms: due_at_ms
     }}
  end

  defp plan_manual_schedule_fire(definition, target_id, due_at_ms, fire_count) do
    case next_run_at_ms(definition, due_at_ms) do
      {:ok, next_run_at_ms} ->
        next_definition =
          next_definition_after_fire(definition, target_id, due_at_ms, fire_count, next_run_at_ms)

        case recurring_end_reason(next_definition, next_run_at_ms, fire_count) do
          nil ->
            {:ok,
             %{
               state: @active_state,
               definition: next_definition,
               run_at_ms: next_run_at_ms
             }}

          reason ->
            completed_definition =
              next_definition
              |> Map.put(:end_reason, reason)
              |> Map.delete(:next_run_at_ms)

            {:ok,
             %{
               state: "completed",
               definition: completed_definition,
               run_at_ms: due_at_ms
             }}
        end

      {:error, reason} when reason in [@timestamp_limit_error, @calendar_range_error] ->
        completed_definition =
          completed_definition_after_fire(
            definition,
            target_id,
            due_at_ms,
            fire_count,
            "timestamp_limit"
          )

        {:ok,
         %{
           state: "completed",
           definition: completed_definition,
           run_at_ms: due_at_ms
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_manual_schedule_fire_plan(ctx, record, plan, now_ms) do
    replace_claimed_with_state(
      ctx,
      record,
      Map.fetch!(plan, :definition),
      Map.fetch!(plan, :state),
      Map.fetch!(plan, :run_at_ms),
      now_ms
    )
  end

  defp skip_manual_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
    case next_run_at_ms(definition, due_at_ms) do
      {:ok, next_run_at_ms} ->
        next_definition =
          skipped_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms)

        case recurring_end_reason(
               next_definition,
               next_run_at_ms,
               Map.get(next_definition, :fire_count, 0)
             ) do
          nil ->
            replace_claimed_with_state(
              ctx,
              record,
              next_definition,
              @active_state,
              next_run_at_ms,
              now_ms
            )

          end_reason ->
            completed_definition = completed_skipped_definition(next_definition, end_reason)

            replace_claimed_with_state(
              ctx,
              record,
              completed_definition,
              "completed",
              due_at_ms,
              now_ms
            )
        end

      {:error, planning_error}
      when planning_error in [@timestamp_limit_error, @calendar_range_error] ->
        completed_definition =
          definition
          |> skipped_definition(due_at_ms, due_at_ms, reason, now_ms)
          |> completed_skipped_definition("timestamp_limit")

        replace_claimed_with_state(
          ctx,
          record,
          completed_definition,
          "completed",
          due_at_ms,
          now_ms
        )

      {:error, planning_error} when is_binary(planning_error) ->
        fail_schedule_planning(ctx, record, definition, planning_error, now_ms)

      {:error, _reason} = error ->
        error
    end
  end

  defp queue_manual_schedule_fire(ctx, record, definition, due_at_ms, reason, now_ms) do
    retry_ms = Map.get(definition, :overlap_retry_ms, @default_overlap_retry_ms)

    case safe_schedule_add(now_ms, retry_ms, :next_run_at_ms) do
      {:ok, next_run_at_ms} ->
        next_definition = queued_definition(definition, due_at_ms, next_run_at_ms, reason, now_ms)

        replace_claimed_with_state(
          ctx,
          record,
          next_definition,
          @active_state,
          next_run_at_ms,
          now_ms
        )

      {:error, @timestamp_limit_error} ->
        completed_definition =
          terminal_queued_definition(definition, due_at_ms, reason, now_ms)

        replace_claimed_with_state(
          ctx,
          record,
          completed_definition,
          "completed",
          now_ms,
          now_ms
        )
    end
  end

  defp replace_claimed_with_state(ctx, record, definition, @active_state, run_at_ms, now_ms) do
    Flow.reschedule(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        state: @active_state,
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        payload: definition,
        run_at_ms: run_at_ms,
        now_ms: now_ms
      )
    )
    |> claimed_schedule_result(record, definition, @active_state)
  end

  defp replace_claimed_with_state(ctx, record, definition, "completed", _run_at_ms, now_ms) do
    Flow.complete(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        payload: definition,
        result: %{schedule_id: Map.fetch!(definition, :id)},
        now_ms: now_ms
      )
    )
    |> claimed_schedule_result(record, definition, "completed")
  end

  defp replace_claimed_with_state(ctx, record, definition, "failed", _run_at_ms, now_ms) do
    Flow.fail(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        payload: definition,
        error: %{schedule_id: Map.fetch!(definition, :id), reason: "schedule overlap"},
        now_ms: now_ms
      )
    )
    |> claimed_schedule_result(record, definition, "failed")
  end

  defp claimed_schedule_result(result, record, definition, state) do
    case result do
      :ok ->
        {:ok, view(%{id: Map.fetch!(record, :id), state: state, payload: definition})}

      {:ok, _record} ->
        {:ok, view(%{id: Map.fetch!(record, :id), state: state, payload: definition})}

      {:error, _reason} = error ->
        error
    end
  end

  defp replace_with_state(ctx, record, definition, state, run_at_ms, now_ms) do
    flow_id = Map.fetch!(record, :id)

    attrs = %{
      id: flow_id,
      type: @schedule_type,
      state: state,
      partition_key: Map.get(record, :partition_key),
      expected_version: Map.fetch!(record, :version),
      schedule_metadata: Metadata.from_definition(definition),
      payload: definition,
      run_at_ms: run_at_ms,
      now_ms: now_ms
    }

    result =
      ctx
      |> Router.flow_schedule_replace(attrs)
      |> Flow.notify_claim_waiters_after_write(attrs, :state)

    case result do
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

  defp completed_definition_after_fire(definition, target_id, due_at_ms, fire_count, reason) do
    definition
    |> Map.put(:fire_count, fire_count)
    |> Map.put(:last_fire_at_ms, due_at_ms)
    |> Map.put(:last_target_id, target_id)
    |> Map.put(:end_reason, reason)
    |> Map.delete(:next_run_at_ms)
    |> Map.delete(:overlap_queued_due_at_ms)
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

  defp terminal_queued_definition(definition, due_at_ms, reason, now_ms) do
    definition
    |> queued_definition(due_at_ms, due_at_ms, reason, now_ms)
    |> Map.put(:end_reason, "timestamp_limit")
    |> Map.delete(:next_run_at_ms)
    |> Map.delete(:overlap_queued_due_at_ms)
  end

  defp completed_skipped_definition(definition, reason) do
    definition
    |> Map.put(:end_reason, reason)
    |> Map.delete(:next_run_at_ms)
  end

  defp failed_overlap_definition(definition, reason, now_ms) do
    definition
    |> Map.put(:end_reason, "overlap_failed")
    |> Map.put(:last_overlap_at_ms, now_ms)
    |> Map.put(:last_overlap_target_id, Map.get(definition, :last_target_id))
    |> Map.put(:last_overlap_reason, reason)
    |> Map.delete(:next_run_at_ms)
    |> Map.delete(:overlap_queued_due_at_ms)
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

  defp completion_reason(definition, fire_count, fallback) do
    if is_integer(Map.get(definition, :max_fires)) and
         fire_count >= Map.fetch!(definition, :max_fires),
       do: "max_fires",
       else: fallback
  end

  defp reschedule_definition(ctx, record, definition, run_at_ms, now_ms) do
    Flow.reschedule(
      ctx,
      Map.fetch!(record, :id),
      Map.fetch!(record, :lease_token),
      Internal.put(
        state: @active_state,
        partition_key: Map.get(record, :partition_key),
        fencing_token: Map.fetch!(record, :fencing_token),
        payload: definition,
        run_at_ms: run_at_ms,
        now_ms: now_ms
      )
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
    opts = target_create_opts(definition, target_id, now_ms)

    ResourceGovernance.create(ctx, target_id, target, fn ->
      case Flow.create(ctx, target_id, opts) do
        :ok ->
          {:created, :ok}

        {:ok, _record} ->
          {:created, :ok}

        {:error, "ERR flow already exists"} ->
          {:existing,
           verify_existing_target(
             ctx,
             definition,
             target_id,
             target,
             partition_key,
             correlation_id
           )}

        {:error, _reason} = error ->
          {:existing, error}
      end
    end)
  end

  defp verify_existing_target(
         ctx,
         definition,
         target_id,
         target,
         partition_key,
         correlation_id
       ) do
    case Flow.get(ctx, target_id, partition_key: partition_key) do
      {:ok, %{type: type, correlation_id: ^correlation_id} = record} when type == target.type ->
        if TargetOwnership.owned?(record, definition, target_id),
          do: :ok,
          else: {:error, "ERR scheduled target id already exists with different owner"}

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
    with :ok <- validate_opts(opts),
         :ok <- validate_option_fields(opts, @status_option_keys, "option"),
         :ok <- validate_id(id),
         {:ok, now_ms} <- optional_now_ms(opts),
         {:ok, record} <-
           Flow.get(
             ctx,
             flow_id(id),
             Internal.put(
               partition_key: partition_key(id),
               payload: true,
               payload_max_bytes: schedule_hydration_max_bytes()
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

  defp replace(ctx, flow_id, definition) do
    id = Map.fetch!(definition, :id)

    with {:ok, record} <-
           Flow.get(
             ctx,
             flow_id,
             Internal.put(
               partition_key: partition_key(id),
               payload: true,
               payload_max_bytes: schedule_hydration_max_bytes()
             )
           ),
         :ok <- require_schedule_record(record) do
      replace_with_state(
        ctx,
        record,
        definition,
        @active_state,
        Map.fetch!(definition, :next_run_at_ms),
        Map.fetch!(definition, :created_at_ms)
      )
    end
    |> tap(fn
      {:ok, _schedule} -> emit_schedule_event(ctx, definition, @schedule_event_created)
      _other -> :ok
    end)
  end

  defp definition(id, opts) do
    with {:ok, now_ms} <- optional_now_ms(opts),
         {:ok, kind} <- schedule_kind(opts),
         :ok <- validate_timing_options(kind, opts),
         {:ok, target} <- target_from_opts(opts),
         :ok <- validate_target_id_mode(kind, target),
         :ok <- validate_target_namespace(kind, target),
         :ok <- validate_inline_target_values(target),
         {:ok, timezone} <- timezone(kind, opts),
         {:ok, next_run_at_ms} <- initial_run_at_ms(kind, opts, now_ms, timezone),
         {:ok, every_ms} <- interval_ms(kind, opts),
         {:ok, cron} <- cron_expr(kind, opts),
         {:ok, catchup_policy} <- Catchup.policy(kind, opts),
         {:ok, overlap_policy} <- overlap_policy(kind, opts),
         {:ok, overlap_retry_ms} <- overlap_retry_ms(kind, overlap_policy, opts),
         {:ok, max_fires} <- max_fires(kind, opts),
         {:ok, end_at_ms} <- end_at_ms(kind, opts),
         :ok <- validate_initial_end_at_ms(next_run_at_ms, end_at_ms) do
      definition =
        %{
          id: id,
          kind: kind,
          target: target,
          ownership_secret: TargetOwnership.new_secret(),
          created_at_ms: now_ms,
          next_run_at_ms: next_run_at_ms,
          fire_count: 0
        }
        |> maybe_put(:every_ms, every_ms)
        |> maybe_put(:cron, cron)
        |> maybe_put(:timezone, timezone)
        |> maybe_put(:catchup_policy, catchup_policy)
        |> maybe_put(:overlap_policy, overlap_policy)
        |> maybe_put(:overlap_retry_ms, overlap_retry_ms)
        |> maybe_put(:max_fires, max_fires)
        |> maybe_put(:end_at_ms, end_at_ms)

      target_id = target_id(definition, next_run_at_ms, 1)
      largest_target_id = target_id(definition, @max_exact_integer, @max_exact_integer)

      with {:ok, _attrs} <-
             MutationAttrs.create_attrs(
               target_id,
               target_create_opts(definition, target_id, now_ms)
             ),
           {:ok, _attrs} <-
             MutationAttrs.create_attrs(
               largest_target_id,
               target_create_opts(definition, largest_target_id, now_ms)
             ),
           :ok <- validate_definition_size(definition) do
        {:ok, definition}
      end
    end
  end

  defp target_create_opts(definition, target_id, now_ms) do
    target = Map.fetch!(definition, :target)

    [
      type: Map.fetch!(target, :type),
      state: Map.get(target, :state, @default_state),
      partition_key: target_partition_key(target_id, target),
      correlation_id: target_correlation_id(definition, target),
      attributes: TargetOwnership.attributes(definition, target_id),
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

  defp validate_timing_options(kind, opts) do
    with :ok <- reject_ambiguous_start(opts),
         :ok <- timing_option_for_kind(opts, :delay_ms, kind, [:delay], "delay"),
         :ok <- timing_option_for_kind(opts, :every_ms, kind, [:interval], "interval"),
         :ok <- timing_option_for_kind(opts, :cron, kind, [:cron], "cron"),
         :ok <-
           timing_option_for_kind(
             opts,
             :at_ms,
             kind,
             [:one_shot, :interval, :cron],
             "one_shot, interval, or cron"
           ) do
      timing_option_for_kind(
        opts,
        :start_at_ms,
        kind,
        [:one_shot, :interval, :cron],
        "one_shot, interval, or cron"
      )
    end
  end

  defp reject_ambiguous_start(opts) do
    if Keyword.has_key?(opts, :at_ms) and Keyword.has_key?(opts, :start_at_ms),
      do: {:error, "ERR flow schedule cannot set both at_ms and start_at_ms"},
      else: :ok
  end

  defp timing_option_for_kind(opts, key, kind, allowed_kinds, label) do
    if Keyword.has_key?(opts, key) and kind not in allowed_kinds,
      do: {:error, "ERR flow schedule #{key} is only supported for #{label} schedules"},
      else: :ok
  end

  defp initial_run_at_ms(:delay, opts, now_ms, _timezone) do
    case Keyword.get(opts, :delay_ms) do
      delay when is_integer(delay) and delay >= 0 and delay <= @max_exact_integer ->
        safe_schedule_add(now_ms, delay, :at_ms)

      delay when is_integer(delay) and delay > @max_exact_integer ->
        {:error, "ERR flow schedule delay_ms exceeds maximum #{@max_exact_integer}"}

      _ ->
        {:error, "ERR flow schedule delay_ms must be a non-negative integer"}
    end
  end

  defp initial_run_at_ms(:cron, opts, now_ms, timezone) do
    with {:ok, expr} <- cron_expr(:cron, opts),
         {:ok, start_at_ms} <-
           required_non_neg_integer(
             Keyword.get(opts, :start_at_ms, Keyword.get(opts, :at_ms, now_ms)),
             :start_at_ms
           ) do
      next_cron_run_at_ms(expr, start_at_ms - 1, timezone)
    end
  end

  defp initial_run_at_ms(_kind, opts, now_ms, _timezone) do
    opts
    |> Keyword.get(:at_ms, Keyword.get(opts, :start_at_ms, now_ms))
    |> required_non_neg_integer(:at_ms)
  end

  defp interval_ms(:interval, opts) do
    case Keyword.get(opts, :every_ms) do
      value when is_integer(value) and value > 0 and value <= @max_exact_integer ->
        {:ok, value}

      value when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow schedule every_ms exceeds maximum #{@max_exact_integer}"}

      _ ->
        {:error, "ERR flow schedule every_ms must be a positive integer"}
    end
  end

  defp interval_ms(_kind, _opts), do: {:ok, nil}

  defp cron_expr(:cron, opts) do
    case Keyword.get(opts, :cron) do
      expr when is_binary(expr) and expr != "" ->
        with {:ok, _parsed} <- Cron.parse(expr), do: {:ok, expr}

      _ ->
        {:error, "ERR flow schedule cron must be a non-empty string"}
    end
  end

  defp cron_expr(_kind, _opts), do: {:ok, nil}

  defp timezone(:cron, opts) do
    timezone = opts |> Keyword.get(:timezone, @default_timezone) |> normalize_timezone()

    case timezone do
      value when is_binary(value) and value != "" ->
        with :ok <- Cron.validate_timezone(value), do: {:ok, value}

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

  defp overlap_retry_ms(kind, overlap_policy, opts) do
    case Keyword.get(opts, :overlap_retry_ms) do
      nil ->
        {:ok, nil}

      value when is_integer(value) and value > 0 and value <= @max_exact_integer ->
        if kind in [:interval, :cron] and overlap_policy == :queue_after_previous,
          do: {:ok, value},
          else:
            {:error,
             "ERR flow schedule overlap_retry_ms requires overlap_policy :queue_after_previous"}

      value when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow schedule overlap_retry_ms exceeds maximum #{@max_exact_integer}"}

      _ ->
        {:error, "ERR flow schedule overlap_retry_ms must be a positive integer"}
    end
  end

  defp max_fires(kind, opts) when kind in [:interval, :cron] do
    case Keyword.get(opts, :max_fires) do
      nil ->
        {:ok, nil}

      value when is_integer(value) and value > 0 and value <= @max_exact_integer ->
        {:ok, value}

      value when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow schedule max_fires exceeds maximum #{@max_exact_integer}"}

      _ ->
        {:error, "ERR flow schedule max_fires must be a positive integer"}
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
    do: safe_schedule_add(due_at_ms, every_ms, :next_run_at_ms)

  defp next_run_at_ms(%{kind: :cron, cron: expr} = definition, due_at_ms),
    do: next_cron_run_at_ms(expr, due_at_ms, Map.get(definition, :timezone, @default_timezone))

  defp next_automatic_run_at_ms(%{kind: :interval} = definition, due_at_ms, now_ms),
    do: Catchup.next_interval(definition, due_at_ms, now_ms)

  defp next_automatic_run_at_ms(definition, due_at_ms, _now_ms) do
    case next_run_at_ms(definition, due_at_ms) do
      {:ok, next_run_at_ms} -> {:ok, next_run_at_ms, 0}
      {:error, @calendar_range_error} -> {:error, :timestamp_limit, 0}
      {:error, _reason} = error -> error
    end
  end

  defp target_from_opts(opts) do
    case Keyword.get(opts, :target) do
      target when is_list(target) or is_map(target) ->
        with {:ok, target_opts} <- normalize_target_options(target),
             :ok <- validate_option_fields(target_opts, @target_option_keys, "target field") do
          normalize_target(target_opts)
        end

      _ ->
        {:error, "ERR flow schedule target is required"}
    end
  end

  defp normalize_target_options(target) when is_map(target),
    do: normalize_target_options(Map.to_list(target))

  defp normalize_target_options(target) when is_list(target) do
    if Enum.all?(target, &match?({_, _}, &1)) do
      {:ok, Enum.map(target, fn {key, value} -> {normalize_target_key(key), value} end)}
    else
      {:error, "ERR flow schedule target must be a keyword list or map"}
    end
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
      created_at_ms: Map.get(definition, :created_at_ms),
      every_ms: Map.get(definition, :every_ms),
      cron: Map.get(definition, :cron),
      next_run_at_ms: visible_next_run_at_ms(record, definition),
      fire_count: Map.get(definition, :fire_count, 0),
      attempts: Map.get(record, :attempts, 0),
      last_fire_at_ms: Map.get(definition, :last_fire_at_ms),
      last_target_id: Map.get(definition, :last_target_id),
      last_overlap_at_ms: Map.get(definition, :last_overlap_at_ms),
      last_overlap_target_id: Map.get(definition, :last_overlap_target_id),
      last_overlap_reason: Map.get(definition, :last_overlap_reason),
      last_skipped_at_ms: Map.get(definition, :last_skipped_at_ms),
      skipped_count: Map.get(definition, :skipped_count, 0),
      catchup_policy: Catchup.policy_for(definition),
      coalesced_count: Map.get(definition, :coalesced_count, 0),
      last_catchup_at_ms: Map.get(definition, :last_catchup_at_ms),
      last_coalesced_count: Map.get(definition, :last_coalesced_count, 0),
      last_planning_error: Map.get(definition, :last_planning_error),
      overlap_policy: Map.get(definition, :overlap_policy, :allow),
      overlap_retry_ms: Map.get(definition, :overlap_retry_ms),
      overlap_queued_due_at_ms: Map.get(definition, :overlap_queued_due_at_ms),
      max_fires: Map.get(definition, :max_fires),
      end_at_ms: Map.get(definition, :end_at_ms),
      end_reason: Map.get(definition, :end_reason),
      timezone: Map.get(definition, :timezone),
      target: Map.get(definition, :target)
    }
  end

  defp view(record), do: record

  defp visible_next_run_at_ms(%{state: state}, _definition) when state in @terminal_states,
    do: nil

  defp visible_next_run_at_ms(_record, definition),
    do: Map.get(definition, :next_run_at_ms)

  defp validate_target_id_mode(_kind, %{id: _id, id_prefix: _id_prefix}),
    do: {:error, "ERR flow schedule target cannot set both id and id_prefix"}

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
    with :ok <- validate_inline_target_value(target, :payload) do
      validate_inline_target_value(target, :values)
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
    Limits.validate_definition(
      definition,
      max_runtime_definition(definition),
      schedule_definition_max_bytes(),
      schedule_hydration_max_bytes()
    )
  end

  # Admission accounts for every bounded field a schedule can persist later.
  # Internal hydration can therefore use one fixed cap without schedules
  # becoming unreadable after a fire, catch-up, or overlap transition.
  defp max_runtime_definition(definition) do
    target_id = target_id(definition, @max_exact_integer, @max_exact_integer)
    overlap_reason = "previous target still active: " <> target_id

    definition
    |> Map.put(:fire_count, @max_exact_integer)
    |> Map.put(:last_fire_at_ms, @max_exact_integer)
    |> Map.put(:last_target_id, target_id)
    |> Map.put(:next_run_at_ms, @max_exact_integer)
    |> Map.put(:last_overlap_at_ms, @max_exact_integer)
    |> Map.put(:last_overlap_target_id, target_id)
    |> Map.put(:last_overlap_reason, overlap_reason)
    |> Map.put(:last_planning_error, overlap_reason)
    |> Map.put(:last_skipped_at_ms, @max_exact_integer)
    |> Map.put(:skipped_count, @max_exact_integer)
    |> Map.put(:coalesced_count, @max_exact_integer)
    |> Map.put(:last_catchup_at_ms, @max_exact_integer)
    |> Map.put(:last_coalesced_count, @max_exact_integer)
    |> Map.put(:overlap_queued_due_at_ms, @max_exact_integer)
    |> Map.put(:end_reason, "timestamp_limit")
  end

  defp next_cron_run_at_ms(expr, after_ms, timezone) do
    Cron.next_run_at_ms(expr, after_ms, timezone, @cron_search_minutes)
  end

  defp partition_key(id) do
    bucket = :erlang.phash2(id, @partition_buckets)
    "__ferricstore_schedule__:" <> Integer.to_string(bucket)
  end

  defp ok_result(:ok), do: :ok
  defp ok_result({:ok, _record}), do: :ok
  defp ok_result({:error, _reason} = error), do: error

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow schedule id must be a non-empty string"}

  defp validate_opts(opts) do
    if Keyword.keyword?(opts),
      do: :ok,
      else: {:error, "ERR flow schedule opts must be a keyword list"}
  end

  defp validate_option_fields(opts, allowed, scope) do
    keys = Enum.map(opts, &elem(&1, 0))

    case Enum.find(keys, &(&1 not in allowed)) do
      nil ->
        case first_duplicate(keys, MapSet.new()) do
          nil -> :ok
          key -> {:error, "ERR duplicate flow schedule #{scope} #{format_option_key(key)}"}
        end

      key ->
        {:error, "ERR unsupported flow schedule #{scope} #{format_option_key(key)}"}
    end
  end

  defp first_duplicate([], _seen), do: nil

  defp first_duplicate([key | keys], seen) do
    if MapSet.member?(seen, key),
      do: key,
      else: first_duplicate(keys, MapSet.put(seen, key))
  end

  defp format_option_key(key) when is_atom(key), do: Atom.to_string(key)
  defp format_option_key(key) when is_binary(key), do: key
  defp format_option_key(key), do: inspect(key)

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
      nil ->
        {:ok, nil}

      value when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      value when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow schedule #{key} exceeds maximum #{@max_exact_integer}"}

      _ ->
        {:error, "ERR flow schedule #{key} must be a non-negative integer"}
    end
  end

  defp required_non_neg_integer(value, key) do
    cond do
      is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow schedule #{key} exceeds maximum #{@max_exact_integer}"}

      true ->
        {:error, "ERR flow schedule #{key} must be a non-negative integer"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, now_ms()) do
      value when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      value when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow schedule now_ms exceeds maximum #{@max_exact_integer}"}

      _ ->
        {:error, "ERR flow schedule now_ms must be a non-negative integer"}
    end
  end

  defp safe_schedule_add(left, right, _key)
       when is_integer(left) and is_integer(right) and left >= 0 and right >= 0 and
              left <= @max_exact_integer - right,
       do: {:ok, left + right}

  defp safe_schedule_add(_left, _right, key),
    do: {:error, "ERR flow schedule #{key} exceeds maximum #{@max_exact_integer}"}

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
  defp now_ms, do: CommandTime.now_ms()

  defp schedule_definition_max_bytes do
    env_pos_integer(:flow_schedule_definition_max_bytes, @default_definition_max_bytes)
  end

  defp schedule_hydration_max_bytes, do: InternalLimits.payload_return_max_bytes()

  defp inline_target_value_max_bytes do
    env_pos_integer(:flow_schedule_inline_value_max_bytes, @default_inline_value_max_bytes)
  end

  defp env_pos_integer(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end
end
