defmodule Ferricstore.Flow.Query.LineageExecutor do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Limits, MandatoryScope, Request}
  alias Ferricstore.TermCodec

  alias Ferricstore.Flow.Query.{
    Cursor,
    CursorKeyStore,
    ExecutionResult,
    MemoryBudget,
    Plan,
    Planner,
    RecordProjection
  }

  @lineage_indexes %{
    parent: "flow_runs_parent_v1",
    root: "flow_runs_root_v1",
    correlation: "flow_runs_correlation_v1"
  }
  @maximum_exact_integer 9_007_199_254_740_991

  @spec execute(map(), Request.t(), Plan.t(), map() | nil, keyword()) ::
          {:ok, ExecutionResult.t()} | {:error, atom()}
  def execute(ctx, request, plan, cursor_auth, opts \\ [])

  def execute(
        ctx,
        %Request{source: :runs} = request,
        %Plan{path: :lineage} = plan,
        cursor_auth,
        opts
      )
      when is_map(ctx) and is_list(opts) do
    with :ok <- Request.validate_bound(request),
         :ok <- MandatoryScope.validate(plan.mandatory_scope),
         {:ok, descriptor} <- lineage_descriptor(request),
         :ok <- validate_plan(plan, request, descriptor),
         {:ok, boundary} <- continuation(cursor_auth, plan),
         {:ok, page} <- read_page(ctx, request, plan, boundary, opts),
         :ok <- validate_page(page, request, plan, descriptor, boundary),
         {:ok, cursor} <- issue_cursor(ctx, request, plan, page, cursor_auth, opts),
         {:ok, records, memory_high_water_bytes} <- project_page(page, request, plan),
         usage <- usage(page, request, descriptor, memory_high_water_bytes) do
      {:ok,
       %ExecutionResult{
         records: records,
         has_more: page.has_more,
         continuation: cursor,
         usage: usage,
         quality: quality(page.has_more)
       }}
    end
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    _kind, _reason -> {:error, :query_engine_failure}
  end

  def execute(_ctx, _request, _plan, _cursor_auth, _opts),
    do: {:error, :unsupported_query_shape}

  defp lineage_descriptor(request) do
    case Request.lineage_descriptor(request) do
      {:ok, %{kind: kind} = descriptor} when kind in [:parent, :root, :correlation] ->
        {:ok, descriptor}

      _invalid ->
        {:error, :unsupported_query_shape}
    end
  end

  defp validate_plan(
         %Plan{
           index_id: index_id,
           index_version: 1,
           index_build_id: index_build_id,
           ranges: [],
           order: :native,
           estimate: %{
             range_seeks: range_seeks,
             scan_entries: scan_entries,
             hard_scan_entries: scan_entries,
             hydrated_records: hydrated_records,
             result_records: limit
           }
         },
         %Request{limit: limit},
         %{kind: kind}
       ) do
    root_probe = root_probe(kind)
    expected_scan = 2 * (limit + 1) + root_probe

    valid =
      index_id == Map.fetch!(@lineage_indexes, kind) and
        index_build_id == index_id and
        range_seeks == 2 + root_probe and
        scan_entries == expected_scan and
        hydrated_records == limit + 1 + root_probe

    if valid, do: :ok, else: {:error, :query_engine_failure}
  end

  defp validate_plan(_plan, _request, _descriptor), do: {:error, :query_engine_failure}

  defp continuation(nil, _plan), do: {:ok, nil}

  defp continuation(
         %{
           claim: %Cursor.Claim{
             index_id: index_id,
             index_version: index_version,
             index_build_id: index_build_id,
             continuation: encoded
           }
         },
         %Plan{
           index_id: index_id,
           index_version: index_version,
           index_build_id: index_build_id
         }
       ) do
    case TermCodec.decode(encoded) do
      {:ok, {:ferric_flow_lineage_seek, 1, updated_at_ms, id}} ->
        validate_boundary(updated_at_ms, id)

      _invalid ->
        {:error, :query_cursor_invalid}
    end
  end

  defp continuation(_cursor_auth, _plan), do: {:error, :query_cursor_invalid}

  defp read_page(ctx, request, plan, boundary, opts) do
    page_read =
      Keyword.get(
        opts,
        :page_read,
        &Ferricstore.Flow.Query.Engine.execute_lineage_page_resolved/4
      )

    if is_function(page_read, 4) do
      try do
        page_read.(ctx, request, plan.mandatory_scope, boundary)
      rescue
        _error -> {:error, :query_storage_unavailable}
      catch
        _kind, _reason -> {:error, :query_storage_unavailable}
      end
    else
      {:error, :query_engine_failure}
    end
  end

  defp validate_page(
         %{
           records: records,
           has_more: has_more,
           continuation: continuation,
           scanned_entries: scanned_entries,
           hydrated_records: hydrated_records,
           duplicate_entries: duplicate_entries,
           memory_high_water_bytes: memory_high_water_bytes
         } = page,
         %Request{limit: limit, order_by: [{:updated_at_ms, direction}]},
         %Plan{budget: budget},
         descriptor,
         boundary
       )
       when map_size(page) == 7 and is_list(records) and is_boolean(has_more) and
              is_integer(scanned_entries) and scanned_entries >= 0 and
              is_integer(hydrated_records) and hydrated_records >= 0 and
              direction in [:asc, :desc] do
    root_probe = root_probe(descriptor.kind)
    maximum_scanned = 2 * (limit + 1) + root_probe
    maximum_hydrated = limit + 1 + root_probe
    ids = Enum.map(records, &Map.get(&1, :id))
    keys = Enum.map(records, &record_key/1)
    memory = MemoryBudget.term_bytes(records)

    valid =
      length(records) <= limit and
        scanned_entries >= length(records) and
        scanned_entries <= maximum_scanned and
        hydrated_records >= minimum_hydrated(records, has_more, root_probe) and
        hydrated_records <= maximum_hydrated and
        is_integer(duplicate_entries) and
        duplicate_entries >= 0 and
        duplicate_entries <= scanned_entries and
        is_integer(memory_high_water_bytes) and
        memory_high_water_bytes >= memory and
        Enum.all?(records, &valid_record?(&1, descriptor)) and
        length(ids) == length(Enum.uniq(ids)) and
        keys == Enum.sort(keys, direction) and
        valid_resume_boundary?(keys, boundary, direction) and
        valid_page_boundary?(records, has_more, continuation)

    if valid,
      do: enforce_page_budget(page, budget),
      else: {:error, :query_storage_inconsistent}
  end

  defp validate_page(_page, _request, _plan, _descriptor, _boundary),
    do: {:error, :query_storage_inconsistent}

  defp valid_resume_boundary?(_keys, nil, _direction), do: true
  defp valid_resume_boundary?([], _boundary, _direction), do: true
  defp valid_resume_boundary?([first | _keys], boundary, :asc), do: first > boundary
  defp valid_resume_boundary?([first | _keys], boundary, :desc), do: first < boundary

  defp valid_record?(
         %{id: id, updated_at_ms: updated_at_ms, partition_key: partition_key} = record,
         %{field: field, value: value, partition_key: partition_key}
       ) do
    match?({:ok, {^updated_at_ms, ^id}}, validate_boundary(updated_at_ms, id)) and
      Map.get(record, field) == value and
      RecordProjection.allowlisted_record?(record, :runs)
  end

  defp valid_record?(_record, _descriptor), do: false

  defp record_key(%{id: id, updated_at_ms: updated_at_ms}), do: {updated_at_ms, id}
  defp record_key(_record), do: :invalid

  defp valid_page_boundary?([], false, nil), do: true
  defp valid_page_boundary?(_records, false, nil), do: true

  defp valid_page_boundary?(records, true, continuation),
    do: records != [] and record_key(List.last(records)) == continuation

  defp valid_page_boundary?(_records, _has_more, _continuation), do: false

  defp project_page(page, %Request{projection: :all}, %Plan{}) do
    {:ok, page.records, page.memory_high_water_bytes}
  end

  defp project_page(page, %Request{projection: projection}, %Plan{budget: budget}) do
    with {:ok, records} <- RecordProjection.project_records(page.records, :runs, projection) do
      memory_high_water_bytes =
        max(page.memory_high_water_bytes, MemoryBudget.term_bytes({page.records, records}))

      if memory_high_water_bytes <= budget.executor_memory_bytes,
        do: {:ok, records, memory_high_water_bytes},
        else: {:error, :query_memory_budget_exceeded}
    end
  end

  defp issue_cursor(_ctx, _request, _plan, %{has_more: false}, _cursor_auth, _opts),
    do: {:ok, nil}

  defp issue_cursor(
         ctx,
         request,
         plan,
         %{has_more: true, continuation: {updated_at_ms, id}},
         cursor_auth,
         opts
       ) do
    with {:ok, boundary} <- validate_boundary(updated_at_ms, id),
         {:ok, logical_partition} <- Ferricstore.Flow.Query.partition_key(request),
         {:ok, scope_keys} <-
           MandatoryScope.derive_keys(plan.mandatory_scope, logical_partition),
         {:ok, key} <- cursor_key(ctx, cursor_auth, opts),
         {:ok, now_ms} <- now_ms(opts) do
      binding = %{
        instance: ctx.name,
        scope: scope_keys.query_binding,
        query_fingerprint: Planner.query_fingerprint(request),
        query_digest: Planner.query_digest(request),
        index_id: plan.index_id,
        index_version: plan.index_version,
        index_build_id: plan.index_build_id,
        order_by: request.order_by
      }

      cursor_opts = [key: key, now_ms: now_ms]

      cursor_opts =
        case Keyword.fetch(opts, :cursor_ttl_ms) do
          {:ok, ttl_ms} -> Keyword.put(cursor_opts, :ttl_ms, ttl_ms)
          :error -> cursor_opts
        end

      {boundary_ms, boundary_id} = boundary

      Cursor.issue(
        binding,
        TermCodec.encode({:ferric_flow_lineage_seek, 1, boundary_ms, boundary_id}),
        cursor_opts
      )
    end
  end

  defp issue_cursor(_ctx, _request, _plan, _page, _cursor_auth, _opts),
    do: {:error, :query_storage_inconsistent}

  defp cursor_key(ctx, cursor_auth, opts) do
    case Keyword.fetch(opts, :cursor_key) do
      {:ok, key} when is_binary(key) and byte_size(key) == 32 -> {:ok, key}
      {:ok, _invalid} -> {:error, :query_cursor_invalid}
      :error -> cursor_key_from_auth(ctx, cursor_auth)
    end
  end

  defp cursor_key_from_auth(_ctx, %{key: key}) when is_binary(key) and byte_size(key) == 32,
    do: {:ok, key}

  defp cursor_key_from_auth(ctx, _cursor_auth), do: CursorKeyStore.key(ctx)

  defp now_ms(opts) do
    case Keyword.get(opts, :now_ms, System.system_time(:millisecond)) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp usage(
         page,
         %Request{predicate: {:and, predicates}},
         descriptor,
         memory_high_water_bytes
       ) do
    count = length(page.records)

    %{
      range_seeks: 2 + root_probe(descriptor.kind),
      range_pages: 0,
      scanned_entries: page.scanned_entries,
      scanned_bytes: 0,
      hydrated_records: page.hydrated_records,
      residual_checks: page.hydrated_records * length(predicates),
      duplicate_entries: page.duplicate_entries,
      result_records: count,
      response_bytes: 0,
      memory_high_water_bytes: memory_high_water_bytes,
      wall_time_us: 0
    }
  end

  defp quality(has_more) do
    %{
      exactness: "authoritative",
      freshness: "current",
      coverage: "complete",
      pagination: if(has_more, do: "authenticated_seek", else: "complete")
    }
  end

  defp validate_boundary(updated_at_ms, id)
       when is_integer(updated_at_ms) and updated_at_ms >= 0 and
              updated_at_ms <= @maximum_exact_integer and is_binary(id) do
    if Limits.valid_run_id?(id),
      do: {:ok, {updated_at_ms, id}},
      else: {:error, :query_cursor_invalid}
  end

  defp validate_boundary(_updated_at_ms, _id), do: {:error, :query_cursor_invalid}

  defp root_probe(:root), do: 1
  defp root_probe(kind) when kind in [:parent, :correlation], do: 0

  defp minimum_hydrated(records, true, _root_probe), do: length(records) + 1
  defp minimum_hydrated(records, false, root_probe), do: max(length(records), root_probe)

  defp enforce_page_budget(page, budget) do
    cond do
      page.scanned_entries > budget.scan_entries ->
        {:error, :query_scan_budget_exceeded}

      page.hydrated_records > budget.hydrated_records ->
        {:error, :query_hydration_budget_exceeded}

      page.memory_high_water_bytes > budget.executor_memory_bytes ->
        {:error, :query_memory_budget_exceeded}

      true ->
        :ok
    end
  end
end
