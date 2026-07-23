Code.require_file("query_dataset.exs", __DIR__)

defmodule Ferricstore.Bench.QueryWorkloadMatrix do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, Request}
  alias Ferricstore.Bench.QueryDataset
  alias Ferricstore.Flow.Query.{Budget, Plan}

  @tenant QueryDataset.tenant()
  @page_limit 25
  @maximum_exact_integer 9_007_199_254_740_991
  @bounded_scan_entries 1_000

  @type scenario :: %{
          name: binary(),
          class: atom(),
          weight: pos_integer(),
          request: Request.t(),
          budget: Budget.t(),
          index_ids: :all | [binary()],
          plan: map(),
          outcome:
            {:records, [binary()], boolean(), [binary()], boolean()}
            | {:count, non_neg_integer()}
            | {:error, atom()},
          cursor?: boolean(),
          usage: map()
        }

  @spec records(pos_integer()) :: [map()]
  defdelegate records(count), to: QueryDataset

  @spec scenarios(pos_integer()) :: [scenario()]
  def scenarios(record_count)
      when is_integer(record_count) and record_count > @bounded_scan_entries,
      do: scenarios(record_count, records(record_count))

  @spec scenarios(pos_integer(), [map()]) :: [scenario()]
  def scenarios(record_count, records)
      when is_integer(record_count) and record_count > @bounded_scan_entries and
             is_list(records) and length(records) == record_count do
    full_updated_window = time_window(:updated_at_ms, 0, record_count + 1)
    full_lease_range = range(:lease_deadline_ms, 0, record_count + 1)
    narrow_lower = max(record_count - 3_000, 0)
    narrow_updated_window = time_window(:updated_at_ms, narrow_lower, record_count + 1)
    default = Budget.default()
    {:ok, bounded_scan} = Budget.lower(default, scan_entries: @bounded_scan_entries)

    [
      record_scenario(
        "tenant_updated_broad_page",
        :broad_native_page,
        20,
        collection([full_updated_window]),
        default,
        plan("flow_runs_tenant_updated", :ordered_range, :native, 1, false, 0),
        records,
        true,
        exact_page_usage()
      ),
      record_scenario(
        "tenant_state_updated_broad_page",
        :broad_native_page,
        20,
        collection([eq(:state, "failed"), full_updated_window]),
        default,
        plan("flow_runs_tenant_state_updated", :ordered_range, :native, 1, false, 0),
        records,
        true,
        exact_page_usage()
      ),
      record_scenario(
        "tenant_type_updated_broad_page",
        :broad_native_page,
        20,
        collection([eq(:type, "invoice"), full_updated_window]),
        default,
        plan("flow_runs_tenant_type_updated", :ordered_range, :native, 1, false, 0),
        records,
        true,
        exact_page_usage()
      ),
      record_scenario(
        "tenant_type_state_updated_broad_page",
        :broad_native_page,
        20,
        collection([
          eq(:type, "invoice"),
          eq(:state, "failed"),
          full_updated_window
        ]),
        default,
        plan(
          "flow_runs_tenant_type_state_updated",
          :ordered_range,
          :native,
          1,
          false,
          0
        ),
        records,
        true,
        exact_page_usage()
      ),
      record_scenario(
        "tenant_type_state_lease_broad_page",
        :broad_native_page,
        20,
        collection(
          [eq(:type, "workflow"), eq(:state, "running"), full_lease_range],
          [{:lease_deadline_ms, :asc}]
        ),
        default,
        plan(
          "flow_runs_tenant_type_state_lease_deadline",
          :ordered_range,
          :native,
          1,
          false,
          0
        ),
        records,
        true,
        exact_page_usage()
      ),
      count_scenario(
        "tenant_count_counter",
        :counter_lookup,
        16,
        count([]),
        default,
        plan("flow_runs_tenant_state_updated", :counter_lookup, :none, 1, false, 0),
        records,
        %{scanned_entries: {:eq, 1}, hydrated_records: {:eq, 0}, range_seeks: {:eq, 1}}
      ),
      count_scenario(
        "state_count_counter",
        :counter_lookup,
        16,
        count([eq(:state, "failed")]),
        default,
        plan("flow_runs_tenant_state_updated", :counter_lookup, :none, 1, false, 0),
        records,
        %{scanned_entries: {:eq, 1}, hydrated_records: {:eq, 0}, range_seeks: {:eq, 1}}
      ),
      count_scenario(
        "state_union_count_counter",
        :counter_lookup,
        16,
        count([in_values(:state, ["failed", "completed"])]),
        default,
        plan("flow_runs_tenant_state_updated", :counter_lookup, :none, 2, false, 0),
        records,
        %{scanned_entries: {:eq, 2}, hydrated_records: {:eq, 0}, range_seeks: {:eq, 2}}
      ),
      count_scenario(
        "type_count_counter",
        :counter_lookup,
        16,
        count([eq(:type, "invoice")]),
        default,
        plan("flow_runs_tenant_type_updated", :counter_lookup, :none, 1, false, 0),
        records,
        %{scanned_entries: {:eq, 1}, hydrated_records: {:eq, 0}, range_seeks: {:eq, 1}}
      ),
      count_scenario(
        "type_state_count_counter",
        :counter_lookup,
        16,
        count([eq(:type, "invoice"), eq(:state, "failed")]),
        default,
        plan("flow_runs_tenant_type_state_updated", :counter_lookup, :none, 1, false, 0),
        records,
        %{scanned_entries: {:eq, 1}, hydrated_records: {:eq, 0}, range_seeks: {:eq, 1}}
      ),
      count_scenario(
        "lease_broad_count_scan",
        :count_scan,
        1,
        count([eq(:type, "workflow"), eq(:state, "running"), full_lease_range]),
        default,
        plan(
          "flow_runs_tenant_type_state_lease_deadline",
          :count_scan,
          :none,
          1,
          false,
          0
        ),
        records,
        :exact_match_usage
      ),
      record_scenario(
        "state_union_narrow_window",
        :multi_range_union,
        3,
        collection([
          in_values(:state, ["failed", "completed"]),
          narrow_updated_window
        ]),
        default,
        plan(
          "flow_runs_tenant_state_updated",
          :ordered_range_union,
          :bounded_top_k,
          2,
          true,
          0
        ),
        records,
        true,
        :exact_match_usage,
        ["flow_runs_tenant_state_updated"]
      ),
      record_scenario(
        "type_state_union_narrow_window",
        :multi_range_union,
        3,
        collection([
          in_values(:type, ["invoice", "workflow"]),
          in_values(:state, ["failed", "running"]),
          narrow_updated_window
        ]),
        default,
        plan(
          "flow_runs_tenant_type_state_updated",
          :ordered_range_union,
          :bounded_top_k,
          4,
          true,
          0
        ),
        records,
        true,
        :exact_match_usage,
        ["flow_runs_tenant_type_state_updated"]
      ),
      record_scenario(
        "priority_sparse_residual_page",
        :residual_filter,
        3,
        collection([eq(:priority, 9)]),
        default,
        plan("flow_runs_tenant_updated", :ordered_filter, :native, 1, false, 1),
        records,
        true,
        %{
          scanned_entries: {:max, 512},
          hydrated_records: {:max, 512},
          range_seeks: {:eq, 1}
        }
      ),
      record_scenario(
        "state_broad_non_native_top_k",
        :bounded_top_k,
        1,
        collection([eq(:state, "failed")], [{:created_at_ms, :asc}]),
        default,
        plan(
          "flow_runs_tenant_state_updated",
          :ordered_filter,
          :bounded_top_k,
          1,
          false,
          0
        ),
        records,
        true,
        :exact_match_usage,
        :all
      ),
      error_scenario(
        "tenant_broad_count_scan_budget",
        :budget_rejection,
        1,
        count([full_updated_window]),
        bounded_scan,
        plan("flow_runs_tenant_updated", :count_scan, :none, 1, false, 0),
        :query_scan_budget_exceeded
      ),
      error_scenario(
        "impossible_residual_scan_budget",
        :budget_rejection,
        1,
        collection([eq(:priority, 99)]),
        bounded_scan,
        plan("flow_runs_tenant_updated", :ordered_filter, :native, 1, false, 1),
        :query_scan_budget_exceeded
      ),
      error_scenario(
        "type_state_range_explosion",
        :budget_rejection,
        1,
        collection([
          in_values(:type, Enum.map(1..8, &"type-#{&1}")),
          in_values(:state, Enum.map(1..5, &"state-#{&1}"))
        ]),
        default,
        plan(nil, :reject, :none, 0, true, 3, :range_budget_exceeded),
        :query_range_budget_exceeded,
        ["flow_runs_tenant_type_state_updated"]
      ),
      record_scenario(
        "empty_time_window",
        :empty,
        2,
        collection([time_window(:updated_at_ms, 100, 100)]),
        default,
        plan(nil, :empty, :native, 0, true, 0),
        records,
        false,
        %{scanned_entries: {:eq, 0}, hydrated_records: {:eq, 0}, range_seeks: {:eq, 0}}
      )
    ]
  end

  @spec select_indexes([struct()], scenario()) :: [struct()]
  def select_indexes(indexes, %{index_ids: :all}), do: indexes

  def select_indexes(indexes, %{index_ids: ids}) when is_list(ids) do
    selected = Enum.filter(indexes, &(&1.definition.id in ids))

    if length(selected) == length(ids),
      do: selected,
      else: raise("query matrix references an index absent from the launch catalog")
  end

  @spec verify_plan(scenario(), Plan.t()) :: :ok | {:error, term()}
  def verify_plan(%{plan: expected}, %Plan{} = actual) do
    observed = %{
      index_id: actual.index_id,
      path: actual.path,
      order: actual.order,
      ranges: length(actual.ranges),
      deduplicate: actual.deduplicate,
      residual_count: length(actual.residual_predicates),
      fallback_reason: actual.fallback_reason
    }

    if observed == expected, do: :ok, else: {:error, {:unexpected_plan, expected, observed}}
  end

  @spec verify_result(scenario(), struct(), :first | :second) :: :ok | {:error, term()}
  def verify_result(
        %{outcome: {:records, first, first_more, second, second_more}} = scenario,
        result,
        page
      )
      when page in [:first, :second] do
    {expected_ids, expected_more} =
      if page == :first, do: {first, first_more}, else: {second, second_more}

    observed_ids = Enum.map(result.records, &Map.fetch!(&1, :id))

    cond do
      observed_ids != expected_ids ->
        {:error, {:unexpected_records, expected_ids, observed_ids}}

      result.has_more != expected_more ->
        {:error, {:unexpected_lookahead, expected_more, result.has_more}}

      expected_more and not is_binary(result.continuation) ->
        {:error, :missing_cursor}

      not expected_more and not is_nil(result.continuation) ->
        {:error, :unexpected_cursor}

      true ->
        verify_usage(scenario, result.usage)
    end
  end

  def verify_result(%{outcome: {:count, expected}} = scenario, result, :first) do
    if result.count == expected,
      do: verify_usage(scenario, result.usage),
      else: {:error, {:unexpected_count, expected, result.count}}
  end

  def verify_result(_scenario, _result, _page), do: {:error, :unexpected_query_success}

  @spec verify_error(scenario(), atom()) :: :ok | {:error, term()}
  def verify_error(%{outcome: {:error, expected}}, expected), do: :ok

  def verify_error(%{outcome: {:error, expected}}, actual),
    do: {:error, {:unexpected_query_error, expected, actual}}

  def verify_error(_scenario, actual),
    do: {:error, {:unexpected_query_error, :success, actual}}

  @spec verify_oracle(scenario(), [map()]) :: :ok | {:error, term()}
  def verify_oracle(
        %{outcome: {:records, first, first_more, second, second_more}} = scenario,
        records
      ) do
    matching = matching_records(scenario.request, records)
    expected_first = matching |> Enum.take(scenario.request.limit) |> Enum.map(& &1.id)

    expected_second =
      matching
      |> Enum.drop(scenario.request.limit)
      |> Enum.take(scenario.request.limit)
      |> Enum.map(& &1.id)

    if {first, first_more, second, second_more} ==
         {expected_first, length(matching) > scenario.request.limit, expected_second,
          length(matching) > scenario.request.limit * 2},
       do: :ok,
       else: {:error, :invalid_record_oracle}
  end

  def verify_oracle(%{outcome: {:count, expected}, request: request}, records) do
    if expected == match_count(request, records), do: :ok, else: {:error, :invalid_count_oracle}
  end

  def verify_oracle(%{outcome: {:error, :query_scan_budget_exceeded}} = scenario, records) do
    if length(records) > scenario.budget.scan_entries,
      do: :ok,
      else: {:error, :scan_budget_oracle_not_broad}
  end

  def verify_oracle(%{outcome: {:error, :query_range_budget_exceeded}}, _records), do: :ok

  @spec match_count(Request.t(), [map()]) :: non_neg_integer()
  def match_count(%Request{} = request, records), do: Enum.count(records, &matches?(&1, request))

  defp record_scenario(
         name,
         class,
         weight,
         request,
         budget,
         plan,
         records,
         cursor?,
         usage,
         index_ids \\ :all
       ) do
    matching = matching_records(request, records)
    limit = request.limit
    first = matching |> Enum.take(limit) |> Enum.map(& &1.id)
    second = matching |> Enum.drop(limit) |> Enum.take(limit) |> Enum.map(& &1.id)

    %{
      name: name,
      class: class,
      weight: weight,
      request: request,
      budget: budget,
      index_ids: index_ids,
      plan: plan,
      outcome: {:records, first, length(matching) > limit, second, length(matching) > limit * 2},
      cursor?: cursor?,
      usage: materialize_usage(usage, length(matching))
    }
  end

  defp count_scenario(
         name,
         class,
         weight,
         request,
         budget,
         plan,
         records,
         usage
       ) do
    count = match_count(request, records)

    %{
      name: name,
      class: class,
      weight: weight,
      request: request,
      budget: budget,
      index_ids: :all,
      plan: plan,
      outcome: {:count, count},
      cursor?: false,
      usage: materialize_usage(usage, count)
    }
  end

  defp error_scenario(name, class, weight, request, budget, plan, error, index_ids \\ :all) do
    %{
      name: name,
      class: class,
      weight: weight,
      request: request,
      budget: budget,
      index_ids: index_ids,
      plan: plan,
      outcome: {:error, error},
      cursor?: false,
      usage: %{}
    }
  end

  defp materialize_usage(:exact_match_usage, matching) do
    %{
      scanned_entries: {:eq, matching},
      hydrated_records: {:eq, matching},
      residual_checks: {:eq, 0}
    }
  end

  defp materialize_usage(usage, _matching), do: usage

  defp exact_page_usage do
    %{
      scanned_entries: {:eq, @page_limit + 1},
      hydrated_records: {:eq, @page_limit + 1},
      range_seeks: {:eq, 1}
    }
  end

  defp verify_usage(%{usage: contract}, usage) do
    Enum.reduce_while(contract, :ok, fn {field, bound}, :ok ->
      value = Map.fetch!(usage, field)

      valid? =
        case bound do
          {:eq, expected} -> value == expected
          {:max, maximum} -> value <= maximum
          {:min, minimum} -> value >= minimum
        end

      if valid?,
        do: {:cont, :ok},
        else: {:halt, {:error, {:unexpected_usage, field, bound, value}}}
    end)
  end

  defp matching_records(request, records) do
    records
    |> Enum.filter(&matches?(&1, request))
    |> sort_records(request.order_by)
  end

  defp matches?(record, %Request{predicate: {:and, predicates}}) do
    Enum.all?(predicates, &matches_predicate?(record, &1))
  end

  defp matches_predicate?(record, {:eq, field, literal}),
    do: fetch(record, field) == literal_value(literal)

  defp matches_predicate?(record, {:in, field, literals}),
    do: fetch(record, field) in Enum.map(literals, &literal_value/1)

  defp matches_predicate?(record, {:range, field, lower, upper}) do
    value = fetch(record, field)
    value >= literal_value(lower) and value <= literal_value(upper)
  end

  defp matches_predicate?(record, {:time_window, field, lower, upper}) do
    value = fetch(record, field)
    value >= literal_value(lower) and value < literal_value(upper)
  end

  defp sort_records(records, []), do: records
  defp sort_records(records, [{field, :asc}]), do: Enum.sort_by(records, &fetch(&1, field), :asc)

  defp sort_records(records, [{field, :desc}]),
    do: Enum.sort_by(records, &fetch(&1, field), :desc)

  defp fetch(record, field) do
    case Field.fetch(record, field) do
      {:ok, value} -> value
      :missing -> raise "query matrix fixture record is missing #{inspect(field)}"
    end
  end

  defp plan(index_id, path, order, ranges, deduplicate, residual_count, fallback \\ :none) do
    %{
      index_id: index_id,
      path: path,
      order: order,
      ranges: ranges,
      deduplicate: deduplicate,
      residual_count: residual_count,
      fallback_reason: fallback
    }
  end

  defp collection(predicates, order_by \\ [{:updated_at_ms, :desc}]) do
    Request.collection(
      :execute,
      [eq(:partition_key, @tenant) | predicates],
      order_by,
      @page_limit,
      :record
    )
  end

  defp count(predicates),
    do: Request.count(:execute, [eq(:partition_key, @tenant) | predicates])

  defp eq(field, value) when is_binary(value), do: {:eq, field, keyword(value)}
  defp eq(field, value) when is_integer(value), do: {:eq, field, integer(value)}

  defp in_values(field, values),
    do: {:in, field, Enum.map(values, &keyword/1)}

  defp range(field, lower, upper),
    do: {:range, field, integer(lower), integer(upper)}

  defp time_window(field, lower, upper),
    do: {:time_window, field, integer(lower), integer(upper)}

  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value) when value <= @maximum_exact_integer, do: {:literal, :integer, value}
  defp literal_value({:literal, _type, value}), do: value
end
