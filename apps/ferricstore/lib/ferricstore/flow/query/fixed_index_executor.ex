defmodule Ferricstore.Flow.Query.FixedIndexExecutor do
  @moduledoc false

  alias Ferricstore.Flow.Query.{
    Cursor,
    CursorKeyStore,
    ExecutionResult,
    Field,
    Limits,
    MandatoryScope,
    MemoryBudget,
    Plan,
    Planner,
    RecordProjection,
    Request,
    Shape,
    Surface
  }

  alias Ferricstore.Flow.{ReadAPI, RecordQuery}
  alias Ferricstore.TermCodec

  # The fallback reader materializes a candidate window before residual checks.
  # Keep that window to one maximum response page plus look-ahead so it remains
  # inside the executor reservation even for metadata-heavy Flow records.
  @candidate_scan_limit Limits.max_results() + 1
  @maximum_exact_integer 9_007_199_254_740_991

  @spec plan(Request.t()) ::
          {:ok,
           %{
             index_id: binary(),
             range_seeks: pos_integer(),
             scan_entries: pos_integer()
           }}
          | {:error, atom()}
  def plan(%Request{} = request) do
    with :ok <- Request.validate_bound(request),
         {:ok, descriptor} <- Shape.fixed_descriptor(request),
         :ok <- validate_descriptor(descriptor) do
      range_seeks = fixed_read_count(descriptor)

      {:ok,
       %{
         index_id: "flow_runs_fixed_#{physical_path(descriptor)}_v1",
         range_seeks: range_seeks,
         scan_entries: @candidate_scan_limit * range_seeks
       }}
    end
  end

  def execute(_ctx, %Request{mode: :explain} = request) do
    with :ok <- Request.validate_bound(request),
         {:ok, descriptor} <- Shape.fixed_descriptor(request) do
      {:ok, explain(request, descriptor)}
    end
  end

  def execute(_ctx, %Request{}), do: {:error, :unsupported_query_shape}

  @spec execute_page(map(), Request.t(), Plan.t(), map() | nil, keyword()) ::
          {:ok, ExecutionResult.t()} | {:error, atom()}
  def execute_page(ctx, request, plan, cursor_auth, opts \\ [])

  def execute_page(
        ctx,
        %Request{mode: :execute} = request,
        %Plan{path: :fixed_index} = plan,
        cursor_auth,
        opts
      )
      when is_map(ctx) and is_list(opts) do
    with :ok <- Request.validate_bound(request),
         {:ok, descriptor} <- Shape.fixed_descriptor(request),
         :ok <- validate_descriptor(descriptor),
         :ok <- validate_plan(plan, descriptor),
         {:ok, boundary} <- continuation(cursor_auth, plan, descriptor),
         fetch_request <- %{request | limit: request.limit + 1},
         {:ok, records} <- read(ctx, fetch_request, descriptor, boundary),
         {:ok, output} <-
           prepare_output(
             ctx,
             records,
             request,
             plan,
             descriptor,
             boundary,
             cursor_auth,
             opts
           ) do
      {:ok,
       %ExecutionResult{
         records: output.records,
         has_more: output.has_more,
         continuation: output.cursor,
         usage:
           usage(
             output.fetched_count,
             output.records,
             plan,
             request,
             output.memory_high_water_bytes
           ),
         quality: quality(output.has_more)
       }}
    end
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    _kind, _reason -> {:error, :query_engine_failure}
  end

  def execute_page(_ctx, _request, _plan, _cursor_auth, _opts),
    do: {:error, :unsupported_query_shape}

  defp read(ctx, request, %{lineage: {field, id}} = descriptor, boundary) do
    opts = common_opts(request, descriptor, boundary)

    if Shape.terminal_subset?(descriptor.states) do
      read_each_state(descriptor.states, request.limit, descriptor.direction, fn state ->
        read_lineage(ctx, field, id, Keyword.put(opts, :state, state))
      end)
    else
      read_lineage(ctx, field, id, opts)
    end
  end

  defp read(ctx, request, %{lease_range: {lower, cutoff}, type: type} = descriptor, boundary) do
    ctx
    |> ReadAPI.stuck_between(
      type,
      descriptor.partition_key,
      lower,
      cutoff,
      request.limit,
      descriptor.direction == :desc,
      boundary
    )
    |> normalize_read_result()
  end

  defp read(ctx, request, descriptor, boundary) do
    opts = common_opts(request, descriptor, boundary)

    cond do
      map_size(descriptor.state_meta) > 0 or map_size(descriptor.attributes) > 0 ->
        search(ctx, descriptor, opts)

      Shape.terminal_states?(descriptor.states) and map_size(descriptor.attributes) == 0 ->
        read_terminals(ctx, descriptor.states, descriptor.type, opts, request.limit)

      descriptor.type != nil and is_list(descriptor.states) and length(descriptor.states) == 1 ->
        ctx
        |> ReadAPI.list(descriptor.type, Keyword.put(opts, :state, hd(descriptor.states)))
        |> normalize_read_result()

      true ->
        {:error, :query_no_bounded_plan}
    end
  end

  defp validate_descriptor(%{lineage: {_field, _id}}), do: :ok
  defp validate_descriptor(%{lease_range: {_lower, _upper}}), do: :ok

  defp validate_descriptor(%{type: type, state_meta: state_meta})
       when is_binary(type) and map_size(state_meta) > 0,
       do: :ok

  defp validate_descriptor(%{type: type, attributes: attributes})
       when is_binary(type) and map_size(attributes) > 0,
       do: :ok

  defp validate_descriptor(%{type: type, states: states, attributes: attributes})
       when is_binary(type) and is_list(states) and map_size(attributes) == 0 do
    if Shape.terminal_states?(states), do: :ok, else: validate_single_state(states)
  end

  defp validate_descriptor(_descriptor), do: {:error, :query_no_bounded_plan}

  defp validate_single_state([_state]), do: :ok
  defp validate_single_state(_states), do: {:error, :query_no_bounded_plan}

  defp read_lineage(ctx, field, id, opts) do
    result =
      case field do
        :parent_flow_id -> ReadAPI.by_parent(ctx, id, opts)
        :root_flow_id -> ReadAPI.by_root(ctx, id, opts)
        :correlation_id -> ReadAPI.by_correlation(ctx, id, opts)
      end

    normalize_read_result(result)
  end

  defp search(ctx, descriptor, opts) do
    if Shape.terminal_subset?(descriptor.states) do
      read_each_state(
        descriptor.states,
        Keyword.fetch!(opts, :count),
        descriptor.direction,
        fn state -> search_once(ctx, %{descriptor | states: [state]}, opts) end
      )
    else
      search_once(ctx, descriptor, opts)
    end
  end

  defp search_once(ctx, descriptor, opts) do
    opts =
      opts
      |> Keyword.delete(:include_cold)
      |> maybe_put(:type, descriptor.type)
      |> maybe_put(:state, single_state(descriptor.states))
      |> maybe_put(:attributes, descriptor.attributes, map_size(descriptor.attributes) > 0)
      |> maybe_put(:state_meta, descriptor.state_meta, map_size(descriptor.state_meta) > 0)

    ctx
    |> ReadAPI.search(opts)
    |> normalize_read_result()
  end

  defp read_terminals(ctx, states, type, opts, limit) do
    if Shape.terminal_subset?(states) do
      direction = if Keyword.get(opts, :rev), do: :desc, else: :asc

      read_each_state(states, limit, direction, fn state ->
        ctx
        |> ReadAPI.terminals(type, Keyword.put(opts, :state, state))
        |> normalize_read_result()
      end)
    else
      state = if length(states) == 1, do: hd(states), else: "any"

      ctx
      |> ReadAPI.terminals(type, Keyword.put(opts, :state, state))
      |> normalize_read_result()
    end
  end

  defp read_each_state(states, limit, direction, read_fun) do
    states
    |> Enum.reduce_while({:ok, []}, fn state, {:ok, batches} ->
      case read_fun.(state) do
        {:ok, records} -> {:cont, {:ok, [records | batches]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, batches} ->
        records =
          batches
          |> Enum.reverse()
          |> RecordQuery.merge_ordered_record_chunks(limit, direction == :desc)

        {:ok, records}

      {:error, _reason} = error ->
        error
    end
  end

  defp common_opts(request, descriptor, boundary) do
    [
      partition_key: descriptor.partition_key,
      count: request.limit,
      query_scan_limit: @candidate_scan_limit,
      include_cold: true,
      consistent_projection: false,
      rev: descriptor.direction == :desc
    ]
    |> maybe_put_range(descriptor.updated_range)
    |> maybe_put(:attributes, descriptor.attributes, map_size(descriptor.attributes) > 0)
    |> maybe_put(:state, single_state(descriptor.states))
    |> maybe_put(
      :terminal_only,
      true,
      Shape.terminal_states?(descriptor.states) and length(descriptor.states) > 1
    )
    |> maybe_put_updated_boundary(descriptor, boundary)
  end

  defp maybe_put_updated_boundary(opts, _descriptor, nil), do: opts

  defp maybe_put_updated_boundary(
         opts,
         %{order_field: :updated_at_ms, direction: :asc},
         {updated_at_ms, id}
       ),
       do: opts |> Keyword.put(:from_ms, updated_at_ms) |> Keyword.put(:after_id, id)

  defp maybe_put_updated_boundary(
         opts,
         %{order_field: :updated_at_ms, direction: :desc},
         {updated_at_ms, id}
       ),
       do: opts |> Keyword.put(:to_ms, updated_at_ms) |> Keyword.put(:before_id, id)

  defp maybe_put_updated_boundary(_opts, _descriptor, _boundary),
    do: raise(ArgumentError, "invalid fixed-index continuation")

  defp maybe_put_range(opts, nil), do: opts

  defp maybe_put_range(opts, {from_ms, to_ms}),
    do: opts |> Keyword.put(:from_ms, from_ms) |> Keyword.put(:to_ms, to_ms)

  defp maybe_put(opts, _key, _value, false), do: opts
  defp maybe_put(opts, key, value, true), do: Keyword.put(opts, key, value)
  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp single_state([state]), do: state
  defp single_state(_states), do: nil

  defp normalize_read_result({:ok, records}) when is_list(records), do: {:ok, records}
  defp normalize_read_result({:error, "NOPERM" <> _rest}), do: {:error, :unauthorized_scope}

  defp normalize_read_result({:error, {kind, _details}})
       when kind in [:flow_lmdb_reconcile_unhealthy, :flow_lmdb_mirror_unhealthy],
       do: {:error, :query_storage_unavailable}

  defp normalize_read_result({:error, reason}) when is_binary(reason) do
    cond do
      String.contains?(reason, "candidate limit exceeded") ->
        {:error, :query_scan_budget_exceeded}

      String.contains?(reason, "not indexed") or
          reason == "ERR flow state_meta search requires type" ->
        {:error, :query_no_bounded_plan}

      String.contains?(reason, "storage") ->
        {:error, :query_storage_unavailable}

      true ->
        {:error, :query_engine_failure}
    end
  end

  defp normalize_read_result(_invalid), do: {:error, :query_engine_failure}

  @doc false
  def normalize_read_result_for_test(result), do: normalize_read_result(result)

  defp prepare_output(
         ctx,
         records,
         %Request{projection: :all} = request,
         plan,
         descriptor,
         boundary,
         cursor_auth,
         opts
       ) do
    with {:ok, projected} <- project(records),
         :ok <- validate_candidates(projected, request, descriptor, boundary, plan),
         page <- Enum.take(projected, request.limit),
         has_more <- length(projected) > request.limit,
         {:ok, cursor} <-
           issue_cursor(ctx, request, plan, descriptor, page, has_more, cursor_auth, opts),
         {:ok, output, memory_high_water_bytes} <- project_output(projected, page, plan) do
      {:ok,
       %{
         records: output,
         has_more: has_more,
         cursor: cursor,
         fetched_count: length(projected),
         memory_high_water_bytes: memory_high_water_bytes
       }}
    end
  end

  defp prepare_output(
         ctx,
         records,
         %Request{projection: projection} = request,
         plan,
         descriptor,
         boundary,
         cursor_auth,
         opts
       )
       when is_list(projection) do
    with {:ok, keys} <- record_keys(records, descriptor),
         :ok <- validate_candidate_keys(keys, request, boundary, descriptor.direction),
         page_records <- Enum.take(records, request.limit),
         page_keys <- Enum.take(keys, request.limit),
         {:ok, output} <- RecordProjection.project_records(page_records, :runs, projection),
         memory_high_water_bytes <- MemoryBudget.term_bytes({records, keys, output}),
         true <- memory_high_water_bytes <= plan.budget.executor_memory_bytes,
         has_more <- length(keys) > request.limit,
         {:ok, cursor} <-
           issue_cursor_key(
             ctx,
             request,
             plan,
             descriptor,
             List.last(page_keys),
             has_more,
             cursor_auth,
             opts
           ) do
      {:ok,
       %{
         records: output,
         has_more: has_more,
         cursor: cursor,
         fetched_count: length(keys),
         memory_high_water_bytes: memory_high_water_bytes
       }}
    else
      false -> {:error, :query_memory_budget_exceeded}
      {:error, _reason} = error -> error
    end
  end

  defp project(records) do
    Enum.reduce_while(records, {:ok, []}, fn record, {:ok, acc} ->
      case RecordProjection.project_result({:ok, record}) do
        {:ok, projected} when is_map(projected) -> {:cont, {:ok, [projected | acc]}}
        _invalid -> {:halt, {:error, :query_storage_inconsistent}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp project_output(fetched, page, %Plan{budget: budget}) do
    memory_high_water_bytes = MemoryBudget.term_bytes(fetched)

    if memory_high_water_bytes <= budget.executor_memory_bytes,
      do: {:ok, page, memory_high_water_bytes},
      else: {:error, :query_memory_budget_exceeded}
  end

  defp validate_plan(
         %Plan{
           index_id: index_id,
           index_version: 1,
           index_build_id: index_id,
           ranges: [],
           order: :native,
           definition: nil
         },
         descriptor
       ) do
    expected_id = "flow_runs_fixed_#{physical_path(descriptor)}_v1"
    if index_id == expected_id, do: :ok, else: {:error, :query_engine_failure}
  end

  defp validate_plan(_plan, _descriptor), do: {:error, :query_engine_failure}

  defp continuation(nil, _plan, _descriptor), do: {:ok, nil}

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
         },
         %{order_field: order_field, direction: direction}
       ) do
    case TermCodec.decode(encoded) do
      {:ok, {:ferric_flow_fixed_seek, 1, ^order_field, ^direction, value, id}} ->
        validate_boundary(value, id)

      _invalid ->
        {:error, :query_cursor_invalid}
    end
  end

  defp continuation(_cursor_auth, _plan, _descriptor), do: {:error, :query_cursor_invalid}

  defp validate_candidates(records, request, descriptor, boundary, plan) do
    with {:ok, keys} <- record_keys(records, descriptor),
         true <- length(records) <= request.limit + 1,
         true <- keys == Enum.sort(keys, descriptor.direction),
         true <- unique_ids?(records),
         true <- valid_resume?(keys, boundary, descriptor.direction),
         true <- MemoryBudget.term_bytes(records) <= plan.budget.executor_memory_bytes do
      :ok
    else
      false -> {:error, :query_storage_inconsistent}
      {:error, _reason} = error -> error
    end
  end

  defp validate_candidate_keys(keys, request, boundary, direction) do
    ids = Enum.map(keys, &elem(&1, 1))

    valid =
      length(keys) <= request.limit + 1 and
        keys == Enum.sort(keys, direction) and
        length(ids) == length(Enum.uniq(ids)) and
        valid_resume?(keys, boundary, direction)

    if valid, do: :ok, else: {:error, :query_storage_inconsistent}
  end

  defp record_keys(records, %{order_field: order_field}) do
    Enum.reduce_while(records, {:ok, []}, fn record, {:ok, acc} ->
      case validate_boundary(Map.get(record, order_field), Map.get(record, :id)) do
        {:ok, key} -> {:cont, {:ok, [key | acc]}}
        {:error, _reason} -> {:halt, {:error, :query_storage_inconsistent}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_boundary(value, id)
       when is_integer(value) and value >= 0 and value <= @maximum_exact_integer do
    if Limits.valid_run_id?(id),
      do: {:ok, {value, id}},
      else: {:error, :query_cursor_invalid}
  end

  defp validate_boundary(_value, _id), do: {:error, :query_cursor_invalid}

  defp unique_ids?(records) do
    ids = Enum.map(records, &Map.get(&1, :id))
    length(ids) == length(Enum.uniq(ids))
  end

  defp valid_resume?(_keys, nil, _direction), do: true
  defp valid_resume?([], _boundary, _direction), do: true
  defp valid_resume?([first | _rest], boundary, :asc), do: first > boundary
  defp valid_resume?([first | _rest], boundary, :desc), do: first < boundary

  defp issue_cursor(_ctx, _request, _plan, _descriptor, _page, false, _cursor_auth, _opts),
    do: {:ok, nil}

  defp issue_cursor(
         ctx,
         request,
         plan,
         descriptor,
         page,
         true,
         cursor_auth,
         opts
       ) do
    with record when is_map(record) <- List.last(page),
         {:ok, {value, id}} <-
           validate_boundary(Map.get(record, descriptor.order_field), Map.get(record, :id)) do
      issue_cursor_from_boundary(
        ctx,
        request,
        plan,
        descriptor,
        value,
        id,
        cursor_auth,
        opts
      )
    else
      nil -> {:error, :query_storage_inconsistent}
      {:error, _reason} = error -> error
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp issue_cursor_key(
         _ctx,
         _request,
         _plan,
         _descriptor,
         _boundary,
         false,
         _cursor_auth,
         _opts
       ),
       do: {:ok, nil}

  defp issue_cursor_key(
         ctx,
         request,
         plan,
         descriptor,
         {value, id},
         true,
         cursor_auth,
         opts
       ) do
    issue_cursor_from_boundary(
      ctx,
      request,
      plan,
      descriptor,
      value,
      id,
      cursor_auth,
      opts
    )
  end

  defp issue_cursor_key(
         _ctx,
         _request,
         _plan,
         _descriptor,
         _boundary,
         true,
         _cursor_auth,
         _opts
       ),
       do: {:error, :query_storage_inconsistent}

  defp issue_cursor_from_boundary(
         ctx,
         request,
         plan,
         descriptor,
         value,
         id,
         cursor_auth,
         opts
       ) do
    with {:ok, {value, id}} <- validate_boundary(value, id),
         {:ok, logical_partition} <- Ferricstore.Flow.Query.partition_key(request),
         {:ok, scope_keys} <-
           MandatoryScope.derive_keys(plan.mandatory_scope, logical_partition),
         {:ok, key} <- cursor_key(ctx, cursor_auth, opts),
         {:ok, now_ms} <- now_ms(opts) do
      binding = %{
        instance: ctx.name,
        scope: scope_keys.query_binding,
        query_fingerprint: plan.query_fingerprint,
        query_digest: Planner.query_digest(request),
        index_id: plan.index_id,
        index_version: plan.index_version,
        index_build_id: plan.index_build_id,
        order_by: request.order_by
      }

      cursor_opts = maybe_put_cursor_ttl([key: key, now_ms: now_ms], opts)

      Cursor.issue(
        binding,
        TermCodec.encode(
          {:ferric_flow_fixed_seek, 1, descriptor.order_field, descriptor.direction, value, id}
        ),
        cursor_opts
      )
    end
  end

  defp cursor_key(_ctx, %{key: key}, _opts) when is_binary(key) and byte_size(key) == 32,
    do: {:ok, key}

  defp cursor_key(ctx, _cursor_auth, opts) do
    case Keyword.fetch(opts, :cursor_key) do
      {:ok, key} when is_binary(key) and byte_size(key) == 32 -> {:ok, key}
      {:ok, _invalid} -> {:error, :query_cursor_invalid}
      :error -> CursorKeyStore.key(ctx)
    end
  end

  defp now_ms(opts) do
    case Keyword.get(opts, :now_ms, System.system_time(:millisecond)) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp maybe_put_cursor_ttl(cursor_opts, opts) do
    case Keyword.fetch(opts, :cursor_ttl_ms) do
      {:ok, ttl_ms} -> Keyword.put(cursor_opts, :ttl_ms, ttl_ms)
      :error -> cursor_opts
    end
  end

  defp usage(
         fetched_count,
         page,
         plan,
         %Request{predicate: {:and, predicates}},
         memory_high_water_bytes
       ) do
    %{
      range_seeks: plan.estimate.range_seeks,
      range_pages: plan.estimate.range_seeks,
      scanned_entries: fetched_count,
      scanned_bytes: 0,
      hydrated_records: fetched_count,
      residual_checks: fetched_count * length(predicates),
      duplicate_entries: 0,
      result_records: length(page),
      response_bytes: 0,
      memory_high_water_bytes: memory_high_water_bytes,
      wall_time_us: 0
    }
  end

  defp quality(has_more) do
    %{
      exactness: "projected_exact",
      freshness: "projection_watermark",
      coverage: "complete",
      pagination: if(has_more, do: "authenticated_seek", else: "complete")
    }
  end

  defp explain(request, descriptor) do
    request_predicates = elem(request.predicate, 1)

    predicates =
      request_predicates
      |> Enum.map(fn predicate ->
        %{
          field: predicate |> elem(1) |> Field.external_name(),
          operator: predicate |> elem(0) |> Atom.to_string()
        }
      end)
      |> Enum.sort_by(&{&1.field, &1.operator})

    fingerprint = Planner.query_fingerprint(request)

    projection_fields =
      case RecordProjection.external_names(request.projection) do
        :all -> "all_allowlisted_fields"
        fields -> fields
      end

    %{
      version: Surface.default_explain_contract(),
      query_fingerprint: fingerprint,
      status: "planned",
      capabilities: %{
        requested: [],
        available: Surface.default_capability_manifest().capabilities,
        missing: []
      },
      plan: %{
        path: descriptor |> physical_path() |> Atom.to_string(),
        fallback_reason: "none",
        projection: %{
          fields: projection_fields,
          application: "after_authoritative_recheck",
          index_only: false
        },
        predicates: predicates,
        order: %{
          field: Atom.to_string(descriptor.order_field),
          direction: Atom.to_string(descriptor.direction)
        }
      },
      estimate: %{scan_records: "bounded_by_index", result_records: request.limit},
      bounds: %{
        scan_records: @candidate_scan_limit * fixed_read_count(descriptor),
        result_records: request.limit,
        groups: 0
      }
    }
  end

  defp fixed_read_count(%{states: states}) do
    if Shape.terminal_subset?(states), do: length(states), else: 1
  end

  defp physical_path(%{lineage: {_field, _id}}), do: :lineage_index
  defp physical_path(%{lease_range: {_lower, _upper}}), do: :inflight_index

  defp physical_path(%{state_meta: state_meta}) when map_size(state_meta) > 0,
    do: :state_metadata_index

  defp physical_path(%{attributes: attributes}) when map_size(attributes) > 0,
    do: :attribute_index

  defp physical_path(%{states: states}) when is_list(states), do: :state_index
end
