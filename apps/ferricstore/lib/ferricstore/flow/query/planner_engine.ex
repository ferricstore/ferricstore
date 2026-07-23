defmodule Ferricstore.Flow.Query.PlannerEngine do
  @moduledoc false

  @behaviour FerricStore.Flow.QueryEngine

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Query.{MandatoryScope, Request, Shape, Surface}
  alias Ferricstore.Store.Router

  alias Ferricstore.Flow.Query.{
    AdmissionController,
    Budget,
    CountResult,
    Cursor,
    CursorKeyStore,
    ExecutionResult,
    Executor,
    Explain,
    FixedIndexExecutor,
    HistoryExecutor,
    LineageExecutor,
    MemoryBudget,
    Plan,
    Planner,
    PlannerDiagnostic,
    Response,
    StatisticsStore,
    StatisticsWorker
  }

  @impl true
  def capabilities, do: Surface.default_capability_manifest()

  @impl true
  def execute(ctx, %Request{} = request) do
    instance_ctx = FerricStore.Flow.QueryEngine.instance_context(ctx)
    budget = Budget.default()

    with :ok <- Request.validate_bound(request),
         {:ok, logical_partition} <- Ferricstore.Flow.Query.partition_key(request),
         {:ok, mandatory_scope} <- bind_mandatory_scope(ctx, instance_ctx, request.source),
         {:ok, scope_keys} <- MandatoryScope.derive_keys(mandatory_scope, logical_partition),
         {:ok, timing} <- timing(ctx, budget),
         {:ok, shard_index} <- route(instance_ctx, scope_keys.physical_partition_key),
         :ok <- check_deadline(timing),
         admission <- admission_server(instance_ctx) do
      AdmissionController.with_permit(
        admission,
        instance_ctx,
        scope_keys.admission_key,
        budget.planner_memory_bytes,
        fn lease ->
          execute_admitted(
            Map.put(instance_ctx, :query_mandatory_scope, mandatory_scope),
            request,
            mandatory_scope,
            scope_keys.query_binding,
            shard_index,
            budget,
            timing,
            admission,
            lease
          )
        end
      )
    end
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    _kind, _reason -> {:error, :query_engine_failure}
  end

  def execute(_ctx, _request), do: {:error, :unsupported_query_shape}

  defp execute_admitted(
         instance_ctx,
         request,
         mandatory_scope,
         query_binding,
         shard_index,
         budget,
         timing,
         admission,
         lease
       ) do
    with :ok <- check_deadline(timing),
         {:ok, cursor_auth} <-
           authenticate_cursor(instance_ctx, request, query_binding, timing),
         {:ok, plan} <-
           build_plan(
             instance_ctx,
             request,
             mandatory_scope,
             shard_index,
             budget,
             timing,
             cursor_auth
           ),
         :ok <- resize_plan_admission(admission, lease, request, plan, budget),
         :ok <- pin_plan_index(admission, instance_ctx, lease, plan),
         :ok <- schedule_statistics(instance_ctx, shard_index, request, plan),
         :ok <- check_deadline(timing) do
      case request.mode do
        :explain ->
          explain = Explain.render(plan, request)

          with :ok <- check_deadline(timing), do: {:ok, explain}

        :analyze ->
          analyze_plan(instance_ctx, request, shard_index, plan, budget, timing, cursor_auth)

        :execute ->
          execute_plan(instance_ctx, request, shard_index, plan, budget, timing, cursor_auth)
      end
    end
  end

  defp analyze_plan(
         _instance_ctx,
         request,
         _shard_index,
         %Plan{path: :reject} = plan,
         _budget,
         timing,
         _cursor_auth
       ) do
    explain = Explain.render(plan, request)
    with :ok <- check_deadline(timing), do: {:ok, explain}
  end

  defp analyze_plan(instance_ctx, request, shard_index, plan, budget, timing, cursor_auth) do
    execution_request = %{request | mode: :execute}

    with {:ok, response} <-
           execute_plan(
             instance_ctx,
             execution_request,
             shard_index,
             plan,
             budget,
             timing,
             cursor_auth
           ),
         {:ok, usage} <- response_usage(response),
         {:ok, explain} <- Explain.executed(plan, request, usage),
         :ok <- check_deadline(timing) do
      {:ok, explain}
    else
      {:error, :invalid_query_usage} -> {:error, :query_engine_failure}
      {:error, _reason} = error -> error
    end
  end

  defp response_usage(%{usage: usage}) when is_map(usage), do: {:ok, usage}
  defp response_usage(_response), do: {:error, :query_engine_failure}

  defp resize_plan_admission(admission, lease, request, plan, budget) do
    with {:ok, memory_bytes} <- plan_admission_memory(request, plan, budget) do
      case AdmissionController.resize_memory(admission, lease, memory_bytes) do
        :ok -> :ok
        {:error, :query_concurrency_exceeded} = error -> error
        {:error, _reason} -> {:error, :query_engine_failure}
      end
    end
  end

  defp plan_admission_memory(%Request{} = request, %Plan{} = plan, %Budget{} = budget) do
    planner_memory_bytes = MemoryBudget.term_bytes({request, plan})

    with true <- planner_memory_bytes <= budget.planner_memory_bytes,
         {:ok, executor_memory_bytes} <- executor_admission_memory(request, plan, budget) do
      {:ok, planner_memory_bytes + executor_memory_bytes + budget.response_bytes}
    else
      false -> {:error, :query_memory_budget_exceeded}
      {:error, _reason} = error -> error
    end
  end

  defp executor_admission_memory(%Request{mode: :explain}, %Plan{}, %Budget{}), do: {:ok, 0}

  defp executor_admission_memory(%Request{}, %Plan{path: path}, %Budget{})
       when path in [:empty, :reject],
       do: {:ok, 0}

  defp executor_admission_memory(%Request{}, %Plan{}, %Budget{} = budget),
    do: {:ok, budget.executor_memory_bytes}

  defp pin_plan_index(_admission, _instance_ctx, _lease, %Plan{definition: nil}), do: :ok

  defp pin_plan_index(
         admission,
         instance_ctx,
         lease,
         %Plan{
           index_id: index_id,
           index_version: index_version,
           index_build_id: index_build_id
         }
       ) do
    case AdmissionController.pin_index(
           admission,
           lease,
           instance_ctx,
           {index_id, index_version, index_build_id}
         ) do
      :ok -> :ok
      {:error, :query_index_retired} -> {:error, :query_storage_unavailable}
      {:error, _reason} -> {:error, :query_engine_failure}
    end
  end

  defp build_plan(
         instance_ctx,
         request,
         mandatory_scope,
         shard_index,
         budget,
         timing,
         cursor_auth
       ) do
    with {:ok, shape} <- Shape.classify(request) do
      case Shape.family(shape) do
        family when family in [:history, :lineage] ->
          with {:ok, plan} <-
                 Planner.plan(request, [],
                   mandatory_scope: mandatory_scope,
                   budget: budget,
                   now_ms: timing.now_ms
                 ),
               :ok <- validate_specialized_cursor(plan, cursor_auth) do
            {:ok, plan}
          end

        :point ->
          plan_without_indexes(request, mandatory_scope, budget, timing, cursor_auth)

        _indexed_collection_or_count ->
          if empty_time_window?(request) do
            plan_without_indexes(request, mandatory_scope, budget, timing, cursor_auth)
          else
            plan_with_indexes(
              instance_ctx,
              request,
              mandatory_scope,
              shard_index,
              budget,
              timing,
              cursor_auth
            )
          end
      end
    end
  end

  defp plan_without_indexes(request, mandatory_scope, budget, timing, cursor_auth) do
    if is_nil(cursor_auth),
      do:
        Planner.plan(request, [],
          mandatory_scope: mandatory_scope,
          budget: budget,
          now_ms: timing.now_ms
        ),
      else: {:error, :query_cursor_invalid}
  end

  defp plan_with_indexes(
         instance_ctx,
         request,
         mandatory_scope,
         shard_index,
         budget,
         timing,
         cursor_auth
       ) do
    case fixed_cursor_plan(request, mandatory_scope, budget, timing, cursor_auth) do
      {:ok, %Plan{} = plan} ->
        {:ok, plan}

      :not_fixed ->
        plan_with_active_indexes(
          instance_ctx,
          request,
          mandatory_scope,
          shard_index,
          budget,
          timing,
          cursor_auth
        )
    end
  end

  defp plan_with_active_indexes(
         instance_ctx,
         request,
         mandatory_scope,
         shard_index,
         budget,
         timing,
         cursor_auth
       ) do
    with {:ok, indexes} <-
           FerricStore.Flow.QueryIndexProvider.active_indexes(instance_ctx, shard_index),
         {:ok, indexes} <- pin_cursor_index(indexes, cursor_auth) do
      Planner.plan(request, indexes,
        mandatory_scope: mandatory_scope,
        budget: budget,
        now_ms: timing.now_ms,
        stats: fn index, requested_scope ->
          case StatisticsStore.lookup(
                 instance_ctx,
                 index.definition.id,
                 index.definition.version,
                 requested_scope
               ) do
            {:ok, stat} -> stat
            :not_found -> nil
          end
        end
      )
    else
      {:error, :query_cursor_invalid} = error -> error
      {:error, _reason} -> {:error, :query_storage_unavailable}
    end
  end

  defp fixed_cursor_plan(_request, _mandatory_scope, _budget, _timing, nil), do: :not_fixed

  defp fixed_cursor_plan(request, mandatory_scope, budget, timing, cursor_auth) do
    case Planner.plan(request, [],
           mandatory_scope: mandatory_scope,
           budget: budget,
           now_ms: timing.now_ms
         ) do
      {:ok, %Plan{path: :fixed_index} = plan} ->
        case validate_specialized_cursor(plan, cursor_auth) do
          :ok -> {:ok, plan}
          {:error, _reason} -> :not_fixed
        end

      _not_a_fixed_plan ->
        :not_fixed
    end
  end

  defp execute_plan(
         instance_ctx,
         request,
         _shard_index,
         %Plan{path: :fixed_index} = plan,
         budget,
         timing,
         cursor_auth
       ) do
    execution_request = %{request | mode: :execute}

    with :ok <- check_deadline(timing),
         {:ok, %ExecutionResult{} = result} <-
           FixedIndexExecutor.execute_page(
             instance_ctx,
             execution_request,
             plan,
             cursor_auth,
             now_ms: timing.now_ms
           ) do
      finalize_response(result, budget, timing)
    end
  end

  defp execute_plan(
         instance_ctx,
         request,
         _shard_index,
         %Plan{path: :primary_key, mandatory_scope: mandatory_scope},
         budget,
         timing,
         nil
       ) do
    with :ok <- check_deadline(timing),
         {:ok, record} <-
           Ferricstore.Flow.Query.Engine.execute_resolved(
             instance_ctx,
             request,
             mandatory_scope
           ),
         {:ok, records} <- point_records(record),
         :ok <- enforce_point_memory(records, budget),
         usage <- point_usage(records, request),
         result <- %ExecutionResult{
           records: records,
           has_more: false,
           continuation: nil,
           usage: usage,
           quality: point_quality()
         } do
      finalize_response(result, budget, timing)
    end
  end

  defp execute_plan(
         instance_ctx,
         request,
         _shard_index,
         %Plan{path: :history} = plan,
         budget,
         timing,
         cursor_auth
       ) do
    with :ok <- check_deadline(timing),
         {:ok, result} <-
           HistoryExecutor.execute(instance_ctx, request, plan, cursor_auth,
             now_ms: timing.now_ms
           ) do
      finalize_response(result, budget, timing)
    end
  end

  defp execute_plan(
         instance_ctx,
         request,
         _shard_index,
         %Plan{path: :lineage} = plan,
         budget,
         timing,
         cursor_auth
       ) do
    with :ok <- check_deadline(timing),
         {:ok, result} <-
           LineageExecutor.execute(instance_ctx, request, plan, cursor_auth,
             now_ms: timing.now_ms
           ) do
      finalize_response(result, budget, timing)
    end
  end

  defp execute_plan(
         _instance_ctx,
         request,
         _shard_index,
         %Plan{path: :reject} = plan,
         _budget,
         _timing,
         _cursor_auth
       ),
       do: {:error, PlannerDiagnostic.error(plan, request)}

  defp execute_plan(
         instance_ctx,
         %Request{return: :count} = request,
         shard_index,
         %Plan{path: path} = plan,
         budget,
         timing,
         nil
       )
       when path in [:counter_lookup, :count_scan, :empty] do
    opts = [now_ms: timing.now_ms, deadline_us: timing.deadline_us]

    with {:ok, %CountResult{} = result} <-
           Executor.execute(instance_ctx, shard_index, request, plan, opts) do
      finalize_count_response(result, budget, timing)
    end
  end

  defp execute_plan(
         instance_ctx,
         request,
         shard_index,
         %Plan{} = plan,
         budget,
         timing,
         cursor_auth
       ) do
    opts =
      [now_ms: timing.now_ms, deadline_us: timing.deadline_us]
      |> add_cursor_auth(cursor_auth)

    with {:ok, result} <-
           Executor.execute(instance_ctx, shard_index, request, plan, opts) do
      finalize_response(result, budget, timing)
    end
  end

  defp authenticate_cursor(_instance_ctx, %Request{cursor: nil}, _query_binding, _timing),
    do: {:ok, nil}

  defp authenticate_cursor(
         instance_ctx,
         %Request{cursor: {:literal, :keyword, token}} = request,
         query_binding,
         timing
       ) do
    binding = %{
      instance: instance_ctx.name,
      scope: query_binding,
      query_fingerprint: Planner.query_fingerprint(request),
      query_digest: Planner.query_digest(request),
      order_by: request.order_by
    }

    with {:ok, key} <- CursorKeyStore.key(instance_ctx),
         {:ok, claim} <- Cursor.open(binding, token, key: key, now_ms: timing.now_ms) do
      {:ok, %{claim: claim, key: key}}
    end
  end

  defp authenticate_cursor(_instance_ctx, _request, _query_binding, _timing),
    do: {:error, :query_cursor_invalid}

  defp pin_cursor_index(indexes, nil), do: {:ok, indexes}

  defp pin_cursor_index(indexes, %{claim: %Cursor.Claim{} = claim}) do
    case Enum.filter(indexes, fn index ->
           index.definition.id == claim.index_id and
             index.definition.version == claim.index_version and
             index.build_id == claim.index_build_id
         end) do
      [index] -> {:ok, [index]}
      _missing_or_ambiguous -> {:error, :query_cursor_invalid}
    end
  end

  defp validate_specialized_cursor(%Plan{}, nil), do: :ok

  defp validate_specialized_cursor(
         %Plan{
           index_id: index_id,
           index_version: index_version,
           index_build_id: index_build_id
         },
         %{
           claim: %Cursor.Claim{
             index_id: index_id,
             index_version: index_version,
             index_build_id: index_build_id
           }
         }
       ),
       do: :ok

  defp validate_specialized_cursor(%Plan{}, _cursor_auth),
    do: {:error, :query_cursor_invalid}

  defp add_cursor_auth(opts, nil), do: opts

  defp add_cursor_auth(opts, %{claim: %Cursor.Claim{} = claim, key: key}) do
    opts
    |> Keyword.put(:cursor_claim, claim)
    |> Keyword.put(:cursor_key, key)
  end

  defp finalize_response(%ExecutionResult{} = result, budget, timing) do
    with {:ok, now_us} <- monotonic_now(),
         :ok <- check_deadline(now_us, timing.deadline_us),
         usage <- %{result.usage | wall_time_us: max(now_us - timing.start_us, 0)},
         {:ok, response} <-
           Response.build(
             result.records,
             result.has_more,
             result.continuation,
             result.quality,
             usage,
             budget
           ),
         {:ok, finished_us} <- monotonic_now(),
         :ok <- check_deadline(finished_us, timing.deadline_us) do
      {:ok, response}
    end
  end

  defp finalize_count_response(%CountResult{} = result, budget, timing) do
    with {:ok, now_us} <- monotonic_now(),
         :ok <- check_deadline(now_us, timing.deadline_us),
         usage <- %{result.usage | wall_time_us: max(now_us - timing.start_us, 0)},
         {:ok, response} <-
           Response.build_count(result.count, result.quality, usage, budget),
         {:ok, finished_us} <- monotonic_now(),
         :ok <- check_deadline(finished_us, timing.deadline_us) do
      {:ok, response}
    end
  end

  defp schedule_statistics(_instance_ctx, _shard_index, %Request{mode: :explain}, %Plan{}),
    do: :ok

  defp schedule_statistics(
         instance_ctx,
         shard_index,
         %Request{mode: mode},
         %Plan{statistics_probes: probes}
       )
       when mode in [:execute, :analyze] do
    worker = StatisticsWorker.server_name(instance_ctx)

    _result =
      StatisticsWorker.probe_many_async(
        instance_ctx,
        worker,
        shard_index,
        probes
      )

    :ok
  end

  defp timing(ctx, budget) do
    with {:ok, start_us} <- monotonic_now() do
      now_ms = System.system_time(:millisecond)
      budget_deadline_us = start_us + budget.wall_time_ms * 1_000

      case FerricStore.Flow.QueryEngine.deadline_ms(ctx) do
        nil ->
          {:ok, %{start_us: start_us, now_ms: now_ms, deadline_us: budget_deadline_us}}

        deadline_ms when deadline_ms > now_ms ->
          client_deadline_us = start_us + (deadline_ms - now_ms) * 1_000

          {:ok,
           %{
             start_us: start_us,
             now_ms: now_ms,
             deadline_us: min(client_deadline_us, budget_deadline_us)
           }}

        _expired ->
          {:error, :query_deadline_exceeded}
      end
    end
  end

  defp route(%{shard_count: shard_count} = instance_ctx, physical_partition)
       when is_integer(shard_count) and shard_count > 0 do
    shard_index = Router.shard_for(instance_ctx, Keys.state_key("", physical_partition))

    if is_integer(shard_index) and shard_index >= 0 and shard_index < shard_count,
      do: {:ok, shard_index},
      else: {:error, :query_engine_failure}
  end

  defp route(_instance_ctx, _physical_partition), do: {:error, :query_engine_failure}

  defp bind_mandatory_scope(ctx, instance_ctx, source) do
    scope_context =
      Map.put(
        instance_ctx,
        :request_context,
        FerricStore.Flow.QueryEngine.request_context(ctx)
      )

    case MandatoryScope.bind(scope_context, source) do
      {:ok, scope} ->
        if MandatoryScope.branch_count(scope) == 1,
          do: {:ok, scope},
          else: {:error, :unauthorized_scope}

      {:error, reason}
      when reason in [:flow_scope_required, :invalid_flow_system_metadata] ->
        {:error, :unauthorized_scope}

      {:error, _reason} ->
        {:error, :query_engine_failure}
    end
  end

  defp admission_server(instance_ctx),
    do:
      Map.get_lazy(instance_ctx, :query_admission_controller, fn ->
        AdmissionController.server_name(instance_ctx)
      end)

  defp check_deadline(timing) do
    with {:ok, now_us} <- monotonic_now(), do: check_deadline(now_us, timing.deadline_us)
  end

  defp check_deadline(now_us, deadline_us) do
    if now_us < deadline_us,
      do: :ok,
      else: {:error, :query_deadline_exceeded}
  end

  defp monotonic_now do
    {:ok, System.monotonic_time(:microsecond)}
  rescue
    _error -> {:error, :query_engine_failure}
  end

  defp point_records(nil), do: {:ok, []}
  defp point_records(%{} = record), do: {:ok, [record]}
  defp point_records(_invalid), do: {:error, :query_storage_inconsistent}

  defp enforce_point_memory(records, budget) do
    if MemoryBudget.term_bytes(records) <= budget.executor_memory_bytes,
      do: :ok,
      else: {:error, :query_memory_budget_exceeded}
  end

  defp point_usage(records, %Request{predicate: {:and, predicates}}) do
    count = length(records)

    %{
      range_seeks: 1,
      range_pages: 0,
      scanned_entries: 1,
      scanned_bytes: 0,
      hydrated_records: count,
      residual_checks: count * length(predicates),
      duplicate_entries: 0,
      result_records: count,
      response_bytes: 0,
      memory_high_water_bytes: MemoryBudget.term_bytes(records),
      wall_time_us: 0
    }
  end

  defp point_quality do
    %{
      exactness: "authoritative",
      freshness: "current",
      coverage: "complete",
      pagination: "none"
    }
  end

  defp empty_time_window?(%Request{predicate: {:and, predicates}}) do
    Enum.any?(predicates, fn
      {:time_window, _field, {:literal, type, value}, {:literal, type, value}} -> true
      _predicate -> false
    end)
  end

  defp empty_time_window?(_request), do: false
end
