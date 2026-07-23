defmodule Ferricstore.Flow.Query.HistoryExecutor do
  @moduledoc false

  alias Ferricstore.Flow.HistoryEvent
  alias Ferricstore.Flow.Query.{MandatoryScope, Request}
  alias Ferricstore.TermCodec

  alias Ferricstore.Flow.Query.{
    Cursor,
    CursorKeyStore,
    ExecutionResult,
    MemoryBudget,
    Plan,
    Planner
  }

  @maximum_exact_integer 9_007_199_254_740_991

  @spec execute(map(), Request.t(), Plan.t(), map() | nil, keyword()) ::
          {:ok, ExecutionResult.t()} | {:error, atom()}
  def execute(ctx, request, plan, cursor_auth, opts \\ [])

  def execute(
        ctx,
        %Request{source: :events} = request,
        %Plan{path: :history} = plan,
        cursor_auth,
        opts
      )
      when is_map(ctx) and is_list(opts) do
    with :ok <- Request.validate_bound(request),
         :ok <- MandatoryScope.validate(plan.mandatory_scope),
         :ok <- validate_plan(plan, request),
         {:ok, before_event} <- continuation(cursor_auth, plan),
         {:ok, page} <- read_page(ctx, request, plan, before_event, opts),
         :ok <- validate_page(page, request, plan, before_event),
         {:ok, cursor} <- issue_cursor(ctx, request, plan, page, cursor_auth, opts),
         usage <- usage(page, request) do
      {:ok,
       %ExecutionResult{
         records: page.records,
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

  defp validate_plan(
         %Plan{
           index_id: "flow_events_history_v1",
           index_version: 1,
           index_build_id: "flow_events_history_v1",
           ranges: [],
           order: :native,
           estimate: %{
             range_seeks: 2,
             scan_entries: scan_entries,
             hard_scan_entries: scan_entries,
             hydrated_records: hydrated_records,
             result_records: limit
           }
         },
         %Request{limit: limit}
       )
       when scan_entries == 2 * (limit + 1) and hydrated_records == limit + 1,
       do: :ok

  defp validate_plan(_plan, _request), do: {:error, :query_engine_failure}

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
      {:ok, {:ferric_flow_history_seek, 1, event_id}} -> validate_event_id(event_id)
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp continuation(_cursor_auth, _plan), do: {:error, :query_cursor_invalid}

  defp read_page(ctx, request, plan, before_event, opts) do
    page_read =
      Keyword.get(
        opts,
        :page_read,
        &Ferricstore.Flow.Query.Engine.execute_history_page_resolved/4
      )

    if is_function(page_read, 4) do
      try do
        page_read.(ctx, request, plan.mandatory_scope, before_event)
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
         %Request{limit: limit, order_by: [{:event_id, direction}]},
         %Plan{budget: budget},
         before_event
       )
       when map_size(page) == 7 and is_list(records) and is_boolean(has_more) and
              is_integer(scanned_entries) and scanned_entries >= 0 and direction in [:asc, :desc] do
    maximum_scanned = 2 * (limit + 1)
    maximum_hydrated = limit + 1
    event_ids = Enum.map(records, &Map.get(&1, :event_id))
    memory = MemoryBudget.term_bytes(records)

    valid =
      length(records) <= limit and
        scanned_entries >= length(records) and
        scanned_entries <= maximum_scanned and
        is_integer(hydrated_records) and
        hydrated_records >= minimum_hydrated(records, has_more) and
        hydrated_records <= maximum_hydrated and
        is_integer(duplicate_entries) and
        duplicate_entries >= 0 and
        duplicate_entries <= scanned_entries and
        is_integer(memory_high_water_bytes) and
        memory_high_water_bytes >= memory and
        Enum.all?(records, &valid_event_record?/1) and
        length(event_ids) == length(Enum.uniq(event_ids)) and
        ordered?(event_ids, direction) and
        valid_resume_boundary?(records, before_event, direction) and
        valid_page_boundary?(records, has_more, continuation, direction)

    if valid,
      do: enforce_page_budget(page, budget),
      else: {:error, :query_storage_inconsistent}
  end

  defp validate_page(_page, _request, _plan, _before_event),
    do: {:error, :query_storage_inconsistent}

  defp valid_event_record?(%{event_id: event_id, fields: fields})
       when is_binary(event_id) and is_map(fields),
       do: match?({:ok, _event_id}, validate_event_id(event_id))

  defp valid_event_record?(_record), do: false

  defp ordered?(event_ids, direction) do
    keys = Enum.map(event_ids, &event_key/1)
    expected = Enum.sort(keys, direction)
    keys == expected
  end

  defp valid_resume_boundary?(_records, nil, _direction), do: true
  defp valid_resume_boundary?([], _before_event, _direction), do: true

  defp valid_resume_boundary?([first | _records], before_event, :asc),
    do: event_key(first.event_id) > event_key(before_event)

  defp valid_resume_boundary?([first | _records], before_event, :desc),
    do: event_key(first.event_id) < event_key(before_event)

  defp valid_page_boundary?([], false, nil, _direction), do: true
  defp valid_page_boundary?(_records, false, nil, _direction), do: true

  defp valid_page_boundary?(records, true, continuation, direction)
       when direction in [:asc, :desc],
       do: records != [] and Map.get(List.last(records), :event_id) == continuation

  defp valid_page_boundary?(_records, _has_more, _continuation, _direction), do: false

  defp issue_cursor(_ctx, _request, _plan, %{has_more: false}, _cursor_auth, _opts),
    do: {:ok, nil}

  defp issue_cursor(
         ctx,
         request,
         plan,
         %{has_more: true, continuation: event_id},
         cursor_auth,
         opts
       ) do
    with {:ok, event_id} <- validate_event_id(event_id),
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

      Cursor.issue(
        binding,
        TermCodec.encode({:ferric_flow_history_seek, 1, event_id}),
        cursor_opts
      )
    end
  end

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

  defp usage(page, %Request{predicate: {:and, predicates}}) do
    count = length(page.records)

    %{
      range_seeks: 2,
      range_pages: 0,
      scanned_entries: page.scanned_entries,
      scanned_bytes: 0,
      hydrated_records: page.hydrated_records,
      residual_checks: page.hydrated_records * length(predicates),
      duplicate_entries: page.duplicate_entries,
      result_records: count,
      response_bytes: 0,
      memory_high_water_bytes: page.memory_high_water_bytes,
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

  defp validate_event_id(event_id) when is_binary(event_id) do
    case :binary.split(event_id, "-", [:global]) do
      [milliseconds, version] ->
        if canonical_event_integer?(milliseconds) and canonical_event_integer?(version),
          do: {:ok, event_id},
          else: {:error, :query_cursor_invalid}

      _invalid ->
        {:error, :query_cursor_invalid}
    end
  end

  defp validate_event_id(_event_id), do: {:error, :query_cursor_invalid}

  defp canonical_event_integer?(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 and number <= @maximum_exact_integer ->
        value == Integer.to_string(number)

      _invalid ->
        false
    end
  end

  defp event_key(event_id), do: {HistoryEvent.ms(event_id), event_id}

  defp minimum_hydrated(records, true), do: length(records) + 1
  defp minimum_hydrated(records, false), do: length(records)

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
