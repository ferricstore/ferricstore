defmodule Ferricstore.Flow.Query.Planner do
  @moduledoc false

  alias Ferricstore.TermCodec

  alias Ferricstore.Flow.Query.{
    CompositeIndex,
    CompositeRange,
    Field,
    FixedIndexExecutor,
    IndexDefinition,
    MandatoryScope,
    RegisteredIndex,
    Request,
    Shape,
    TupleCodec
  }

  alias Ferricstore.Flow.Query.{Budget, IndexStatistics, MemoryBudget, Plan}

  @primary_index "flow_runs_primary_v1"
  @history_index "flow_events_history_v1"
  @lineage_indexes %{
    parent: "flow_runs_parent_v1",
    root: "flow_runs_root_v1",
    correlation: "flow_runs_correlation_v1"
  }
  @default_page_entries 512
  @dedupe_entry_bytes 128
  @decoded_entry_overhead 192
  @hydrated_record_overhead 128
  @selection_entry_overhead 256
  @counter_entry_overhead 256

  @spec plan(Request.t(), [RegisteredIndex.t()], keyword()) ::
          {:ok, Plan.t()} | {:error, atom()}
  def plan(request, indexes, opts \\ [])

  def plan(%Request{} = request, indexes, opts) when is_list(indexes) and is_list(opts) do
    budget = Keyword.get(opts, :budget, Budget.default())
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    mandatory_scope = Keyword.get(opts, :mandatory_scope, MandatoryScope.dedicated())

    with :ok <- Request.validate_bound(request),
         :ok <- validate_budget(budget),
         :ok <- MandatoryScope.validate(mandatory_scope),
         true <- is_integer(now_ms) and now_ms >= 0 do
      do_plan(request, indexes, mandatory_scope, budget, now_ms, opts)
    else
      false -> {:error, :invalid_query_budget}
      {:error, _reason} = error -> error
    end
  end

  def plan(%Request{}, _indexes, _opts), do: {:error, :invalid_query_budget}

  @doc false
  @spec physical_contract(Request.t(), IndexDefinition.t(), pos_integer()) ::
          {:ok,
           %{
             path: Plan.path(),
             ranges: [CompositeRange.t()],
             deduplicate: boolean(),
             order: :none | :native | :bounded_top_k,
             residual_predicates: [term()],
             constraint_shapes: [map()]
           }}
          | {:error, atom()}
  def physical_contract(%Request{} = request, %IndexDefinition{} = definition, max_ranges)
      when is_integer(max_ranges) and max_ranges > 0 do
    physical_contract(request, definition, MandatoryScope.dedicated(), max_ranges)
  end

  def physical_contract(%Request{}, %IndexDefinition{}, _max_ranges),
    do: {:error, :range_budget_exceeded}

  @doc false
  @spec physical_contract(Request.t(), IndexDefinition.t(), MandatoryScope.t(), pos_integer()) ::
          {:ok, map()} | {:error, atom()}
  def physical_contract(
        %Request{} = request,
        %IndexDefinition{} = definition,
        %MandatoryScope{} = mandatory_scope,
        max_ranges
      )
      when is_integer(max_ranges) and max_ranges > 0 do
    with :ok <- Request.validate_bound(request),
         :ok <- IndexDefinition.validate(definition),
         :ok <- MandatoryScope.validate(mandatory_scope),
         {:ok, partition} <- request_scope(request),
         {:ok, contract} <-
           build_physical_contract(
             request,
             definition,
             mandatory_scope,
             partition,
             max_ranges
           ) do
      {:ok, Map.delete(contract, :built)}
    end
  end

  def physical_contract(%Request{}, %IndexDefinition{}, %MandatoryScope{}, _max_ranges),
    do: {:error, :range_budget_exceeded}

  @doc false
  @spec query_fingerprint(Request.t()) :: binary()
  def query_fingerprint(request) do
    canonical = {
      request.version,
      request.source,
      request_predicates(request) |> Enum.map(&predicate_shape/1) |> Enum.sort(),
      request.order_by,
      request.limit,
      request.return
    }

    canonical
    |> :erlang.term_to_binary(minor_version: 2)
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @doc false
  @spec query_digest(Request.t()) :: <<_::256>>
  def query_digest(%Request{} = request) do
    {request.version, request.source, request.predicate, request.order_by, request.limit,
     request.return}
    |> TermCodec.encode()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp validate_budget(%Budget{} = budget) do
    case Budget.new(Map.from_struct(budget)) do
      {:ok, _validated} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_budget(_budget), do: {:error, :invalid_query_budget}

  defp do_plan(request, indexes, mandatory_scope, budget, now_ms, opts) do
    fingerprint = query_fingerprint(request)

    with {:ok, shape} <- Shape.classify(request) do
      case Shape.family(shape) do
        :history ->
          request
          |> history_plan(mandatory_scope, budget, fingerprint)
          |> enforce_specialized_budget(request, mandatory_scope, budget, fingerprint)

        :lineage ->
          request
          |> lineage_plan(mandatory_scope, budget, fingerprint)
          |> enforce_specialized_budget(request, mandatory_scope, budget, fingerprint)

        :point ->
          request
          |> primary_plan(mandatory_scope, budget, fingerprint)
          |> enforce_specialized_budget(request, mandatory_scope, budget, fingerprint)

        _collection_or_count ->
          if empty_time_window?(request),
            do: {:ok, empty_plan(request, mandatory_scope, budget, fingerprint)},
            else:
              plan_collection(
                request,
                indexes,
                mandatory_scope,
                budget,
                fingerprint,
                now_ms,
                opts
              )
      end
    end
  end

  defp plan_collection(
         request,
         indexes,
         mandatory_scope,
         budget,
         fingerprint,
         now_ms,
         opts
       ) do
    with {:ok, partition} <- request_scope(request),
         {:ok, scope_keys} <- MandatoryScope.derive_keys(mandatory_scope, partition),
         {:ok, scope_prefix} <- MandatoryScope.single_prefix(mandatory_scope) do
      scope_runtime = %{
        prefix: scope_prefix,
        physical_partition_key: scope_keys.physical_partition_key,
        statistics_key: scope_keys.statistics_key
      }

      {candidates, rejection_reasons} =
        indexes
        |> Enum.filter(&safe_active_index?/1)
        |> Enum.sort_by(&index_identity/1)
        |> Enum.reduce({[], []}, fn index, {candidates, reasons} ->
          case candidate(
                 request,
                 index,
                 mandatory_scope,
                 partition,
                 scope_runtime,
                 budget,
                 fingerprint,
                 now_ms,
                 opts
               ) do
            {:ok, candidate} -> {[candidate | candidates], reasons}
            {:error, reason} -> {candidates, [reason | reasons]}
          end
        end)

      case Enum.sort_by(candidates, &candidate_sort_key/1) do
        [chosen | alternatives] ->
          {:ok,
           %{
             chosen
             | alternatives: explain_alternatives(request, alternatives),
               statistics_probes: selected_statistics_probes(request, chosen)
           }}

        [] ->
          case fixed_index_plan(request, mandatory_scope, budget, fingerprint) do
            {:ok, plan} ->
              {:ok, plan}

            {:error, fixed_reason} ->
              reason = rejection_reason([fixed_reason | rejection_reasons])
              {:ok, reject_plan(request, mandatory_scope, budget, fingerprint, reason)}
          end
      end
    else
      {:error, _reason} ->
        {:ok, reject_plan(request, mandatory_scope, budget, fingerprint, :unscoped_query)}
    end
  end

  defp candidate(
         request,
         %RegisteredIndex{definition: definition} = index,
         mandatory_scope,
         partition,
         scope_runtime,
         budget,
         fingerprint,
         now_ms,
         opts
       ) do
    with {:ok, contract} <-
           build_physical_contract(
             request,
             definition,
             mandatory_scope,
             partition,
             budget.range_seeks
           ),
         built <- contract.built do
      case contract.path do
        :counter_lookup ->
          counter_candidate(
            request,
            index,
            contract,
            built,
            mandatory_scope,
            budget,
            fingerprint
          )

        _scan_path ->
          scan_candidate(
            request,
            index,
            contract,
            built,
            scope_runtime,
            mandatory_scope,
            budget,
            fingerprint,
            now_ms,
            opts
          )
      end
    else
      {:error, _reason} = error -> error
    end
  end

  defp scan_candidate(
         request,
         %RegisteredIndex{definition: definition} = index,
         contract,
         built,
         scope_runtime,
         mandatory_scope,
         budget,
         fingerprint,
         now_ms,
         opts
       ) do
    predicates = request_predicates(request)
    stats_scope = scope_runtime.statistics_key
    stat = lookup_stats(index, stats_scope, opts)
    estimate = estimate(request, built, stat, budget, now_ms, contract.deduplicate)

    with :ok <- within_budget(estimate, budget) do
      order = contract.order
      estimate = add_result_estimate(request, estimate, order, stat, now_ms)

      with :ok <- within_budget(estimate, budget) do
        {:ok,
         %Plan{
           path: contract.path,
           index_id: definition.id,
           index_version: definition.version,
           index_build_id: index.build_id,
           definition: definition,
           ranges: Enum.map(built.ranges, & &1.range),
           deduplicate: contract.deduplicate,
           order: order,
           residual_predicates: contract.residual_predicates,
           recheck_predicates: predicates,
           constraint_shapes: contract.constraint_shapes,
           estimate: estimate,
           stats: stats_evidence(stat, stats_scope, estimate, now_ms),
           budget: budget,
           fallback_reason: :none,
           query_fingerprint: fingerprint,
           mandatory_scope: mandatory_scope,
           alternatives: [],
           statistics_probes:
             candidate_statistics_probes(
               definition,
               built,
               stat,
               now_ms,
               scope_runtime
             )
         }}
      end
    end
  end

  defp counter_candidate(
         request,
         %RegisteredIndex{definition: definition} = index,
         contract,
         built,
         mandatory_scope,
         budget,
         fingerprint
       ) do
    range_count = length(built.ranges)
    planner_memory_bytes = MemoryBudget.term_bytes({request, built.ranges})
    hard_scan_bytes = range_count * 2_048

    estimate = %{
      range_seeks: range_count,
      scan_entries: range_count,
      hard_scan_entries: range_count,
      hydrated_records: 0,
      hard_hydrated_records: 0,
      result_records: 1,
      scan_bytes: hard_scan_bytes,
      hard_scan_bytes: hard_scan_bytes,
      hydration_bytes: 0,
      residual_checks: 0,
      sort_rows: 0,
      planner_memory_bytes: planner_memory_bytes,
      memory_bytes:
        hard_scan_bytes + range_count * @counter_entry_overhead + planner_memory_bytes,
      cost: range_count * 5,
      estimate_source: :transactional_counter,
      confidence: :exact
    }

    with :ok <- within_budget(estimate, budget) do
      {:ok,
       %Plan{
         path: :counter_lookup,
         index_id: definition.id,
         index_version: definition.version,
         index_build_id: index.build_id,
         definition: definition,
         ranges: Enum.map(built.ranges, & &1.range),
         deduplicate: false,
         order: :none,
         residual_predicates: contract.residual_predicates,
         recheck_predicates: request_predicates(request),
         constraint_shapes: contract.constraint_shapes,
         estimate: estimate,
         stats: %{source: :transactional_counter, confidence: :exact},
         budget: budget,
         fallback_reason: :none,
         query_fingerprint: fingerprint,
         mandatory_scope: mandatory_scope,
         alternatives: [],
         statistics_probes: []
       }}
    end
  end

  defp build_physical_contract(
         request,
         definition,
         mandatory_scope,
         partition,
         max_ranges
       ) do
    predicates = request_predicates(request)

    with {:ok, scope_prefix} <- MandatoryScope.single_prefix(mandatory_scope),
         {:ok, built} <- build_ranges(definition, scope_prefix, predicates, max_ranges),
         true <- scope_contained?(definition, scope_prefix, partition, built.ranges) do
      residual = residual_predicates(predicates, built.used_predicates)
      counter_lookup? = counter_lookup?(request, definition, built, residual)

      deduplicate =
        if counter_lookup?, do: false, else: deduplication_required?(definition, built)

      {:ok,
       %{
         path:
           if(counter_lookup?,
             do: :counter_lookup,
             else: candidate_path(request, built.ranges, built.range_predicate?)
           ),
         ranges: Enum.map(built.ranges, & &1.range),
         deduplicate: deduplicate,
         order: order_mode(request, definition, built),
         residual_predicates: residual,
         constraint_shapes: constraint_shapes(built.used_predicates),
         built: built
       }}
    else
      false -> {:error, :tenant_range_violation}
      {:error, _reason} = error -> error
    end
  end

  defp build_ranges(
         %IndexDefinition{fields: fields} = definition,
         scope_prefix,
         predicates,
         max_ranges
       ) do
    walk_fields(definition, scope_prefix, fields, predicates, [[]], [], false, max_ranges)
  end

  defp walk_fields(
         definition,
         scope_prefix,
         [],
         _predicates,
         prefixes,
         used,
         range?,
         max_ranges
       ) do
    build_prefix_ranges(definition, scope_prefix, prefixes, used, range?, max_ranges)
  end

  defp walk_fields(
         definition,
         scope_prefix,
         [{field, _direction, encoding} | rest],
         predicates,
         prefixes,
         used,
         range?,
         max_ranges
       ) do
    case equality_constraint(predicates, field) do
      {:ok, predicate, values} ->
        with {:ok, prefixes} <- expand_prefixes(prefixes, values, max_ranges) do
          walk_fields(
            definition,
            scope_prefix,
            rest,
            predicates,
            prefixes,
            [predicate | used],
            range?,
            max_ranges
          )
        end

      :none ->
        case if(encoding == :ordered, do: range_constraint(predicates, field), else: :none) do
          {:ok, predicate, lower, lower_kind, upper, upper_kind} ->
            build_bounded_ranges(
              definition,
              scope_prefix,
              prefixes,
              predicate,
              lower,
              lower_kind,
              upper,
              upper_kind,
              used,
              max_ranges
            )

          :none ->
            build_prefix_ranges(definition, scope_prefix, prefixes, used, range?, max_ranges)
        end
    end
  end

  defp build_prefix_ranges(definition, scope_prefix, prefixes, used, range?, max_ranges) do
    if length(prefixes) > max_ranges do
      {:error, :range_budget_exceeded}
    else
      ranges =
        Enum.reduce_while(prefixes, {:ok, []}, fn values, {:ok, acc} ->
          case CompositeRange.prefix(definition, scope_prefix, values) do
            {:ok, range} -> {:cont, {:ok, [%{range: range, equality_values: values} | acc]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)

      case ranges do
        {:ok, reversed} ->
          {:ok,
           %{
             ranges: Enum.reverse(reversed),
             used_predicates: Enum.reverse(used),
             equality_predicates: Enum.reverse(used),
             range_predicate?: range?
           }}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp build_bounded_ranges(
         definition,
         scope_prefix,
         prefixes,
         predicate,
         lower,
         lower_kind,
         upper,
         upper_kind,
         used,
         max_ranges
       ) do
    if length(prefixes) > max_ranges do
      {:error, :range_budget_exceeded}
    else
      ranges =
        Enum.reduce_while(prefixes, {:ok, []}, fn values, {:ok, acc} ->
          case CompositeRange.bounded(
                 definition,
                 scope_prefix,
                 values,
                 lower,
                 lower_kind,
                 upper,
                 upper_kind
               ) do
            {:ok, range} ->
              spec = %{
                range: range,
                equality_values: values,
                logical_range:
                  {predicate_field(predicate), lower, upper, upper_kind == :inclusive}
              }

              {:cont, {:ok, [spec | acc]}}

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        end)

      case ranges do
        {:ok, reversed} ->
          {:ok,
           %{
             ranges: Enum.reverse(reversed),
             used_predicates: Enum.reverse([predicate | used]),
             equality_predicates: Enum.reverse(used),
             range_predicate?: true
           }}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp expand_prefixes(prefixes, values, max_ranges) do
    if length(prefixes) * length(values) > max_ranges do
      {:error, :range_budget_exceeded}
    else
      {:ok, for(prefix <- prefixes, value <- values, do: prefix ++ [value])}
    end
  end

  defp equality_constraint(predicates, field) do
    predicates
    |> Enum.flat_map(fn
      {:eq, ^field, value} = predicate -> [{0, predicate, [literal_value(value)]}]
      {:in, ^field, values} = predicate -> [{1, predicate, Enum.map(values, &literal_value/1)}]
      {:is, ^field, :null} = predicate -> [{2, predicate, [nil]}]
      {:is, ^field, :missing} = predicate -> [{3, predicate, [Field.missing()]}]
      _predicate -> []
    end)
    |> Enum.sort_by(fn {rank, predicate, _values} -> {rank, stable_term(predicate)} end)
    |> case do
      [{_rank, predicate, values} | _rest] ->
        {:ok, predicate, Enum.sort_by(values, &TupleCodec.encode_component(&1, :asc))}

      [] ->
        :none
    end
  end

  defp range_constraint(predicates, field) do
    predicates
    |> Enum.flat_map(fn
      {:range, ^field, lower, upper} = predicate ->
        [{0, predicate, literal_value(lower), :inclusive, literal_value(upper), :inclusive}]

      {:time_window, ^field, lower, upper} = predicate ->
        [{1, predicate, literal_value(lower), :inclusive, literal_value(upper), :exclusive}]

      _predicate ->
        []
    end)
    |> Enum.sort_by(fn {rank, predicate, _lower, _lower_kind, _upper, _upper_kind} ->
      {rank, stable_term(predicate)}
    end)
    |> case do
      [{_rank, predicate, lower, lower_kind, upper, upper_kind} | _rest] ->
        {:ok, predicate, lower, lower_kind, upper, upper_kind}

      [] ->
        :none
    end
  end

  defp estimate(request, built, stat, budget, now_ms, deduplicate?) do
    {scan_entries, hard_scan_entries, estimate_source, confidence} =
      scan_estimate(built.ranges, stat, budget, now_ms)

    average_entry_bytes = sample_value(stat, :average_entry_bytes, 128, now_ms)
    average_row_bytes = sample_value(stat, :average_row_bytes, 512, now_ms)
    hydrated_records = min(scan_entries, budget.hydrated_records)
    hard_hydrated_records = min(hard_scan_entries, budget.hydrated_records)

    residual_checks =
      hydrated_records *
        length(residual_predicates(request_predicates(request), built.used_predicates))

    scan_bytes = scan_entries * average_entry_bytes
    hard_scan_bytes = hard_scan_entries * average_entry_bytes
    hydration_bytes = hydrated_records * average_row_bytes
    range_seeks = length(built.ranges)
    page_entries = min(hard_scan_entries, @default_page_entries)
    page_bytes = page_entries * (average_entry_bytes + @decoded_entry_overhead)

    hydration_peak_bytes =
      min(hard_hydrated_records, page_entries) *
        (average_row_bytes + @hydrated_record_overhead)

    planner_memory_bytes = MemoryBudget.term_bytes({request, built.ranges})

    dedupe_bytes = if deduplicate?, do: hard_scan_entries * @dedupe_entry_bytes, else: 0

    %{
      range_seeks: range_seeks,
      scan_entries: scan_entries,
      hard_scan_entries: hard_scan_entries,
      hydrated_records: hydrated_records,
      hard_hydrated_records: hard_hydrated_records,
      result_records: result_records(request, hydrated_records),
      scan_bytes: scan_bytes,
      hard_scan_bytes: hard_scan_bytes,
      hydration_bytes: hydration_bytes,
      residual_checks: residual_checks,
      sort_rows: 0,
      planner_memory_bytes: planner_memory_bytes,
      memory_bytes: dedupe_bytes + page_bytes + hydration_peak_bytes,
      cost:
        range_seeks * 20 + scan_entries + hydrated_records * 4 + div(scan_bytes, 256) +
          residual_checks,
      estimate_source: estimate_source,
      confidence: confidence
    }
  end

  defp scan_estimate(ranges, %IndexStatistics{} = stat, budget, now_ms) do
    if IndexStatistics.fresh?(stat, now_ms) do
      estimates = Enum.map(ranges, &range_estimate(stat, &1, now_ms))

      if Enum.all?(estimates, &match?({:ok, _estimate, _hard, _source}, &1)) do
        {estimate, hard} =
          Enum.reduce(estimates, {0, 0}, fn {:ok, value, upper, _source}, {total, bound} ->
            {total + value, bound + upper}
          end)

        source =
          if Enum.any?(estimates, fn {:ok, _value, _upper, source} -> source == :histogram end),
            do: :histogram,
            else: :exact_prefix

        # Prefix probes are exact only at their LMDB snapshot. Mutations after
        # that snapshot can grow the range, so an observed count may raise but
        # never lower the execution ceiling used as the hard planning bound.
        {estimate, max(hard, budget.scan_entries), source, stat.confidence}
      else
        {budget.scan_entries, budget.scan_entries, :default_upper_bound, :none}
      end
    else
      source = if stat.collected_at_ms <= now_ms, do: :stale, else: :invalid_clock
      {budget.scan_entries, budget.scan_entries, source, :low}
    end
  end

  defp scan_estimate(_ranges, _stat, budget, _now_ms),
    do: {budget.scan_entries, budget.scan_entries, :default_upper_bound, :none}

  defp range_estimate(stat, %{equality_values: values} = spec, now_ms) do
    case IndexStatistics.prefix_count(stat, values, now_ms) do
      {:ok, count} ->
        case Map.get(spec, :logical_range) do
          {field, lower, upper, upper_inclusive?} ->
            case IndexStatistics.histogram_fraction_ppm(
                   stat,
                   field,
                   lower,
                   upper,
                   upper_inclusive?,
                   now_ms
                 ) do
              :unknown -> {:ok, count, count, :exact_prefix}
              ppm -> {:ok, div(count * ppm + 999_999, 1_000_000), count, :histogram}
            end

          nil ->
            {:ok, count, count, :exact_prefix}
        end

      :unknown ->
        :unknown
    end
  end

  defp add_order_estimate(estimate, order, limit, stat, now_ms) do
    average_entry_bytes = sample_value(stat, :average_entry_bytes, 128, now_ms)
    average_row_bytes = sample_value(stat, :average_row_bytes, 512, now_ms)
    retained_rows = min(estimate.hard_hydrated_records, limit + 1)

    retained_bytes =
      retained_rows *
        (average_entry_bytes + average_row_bytes + @selection_entry_overhead)

    case order do
      :bounded_top_k ->
        memory_bytes = estimate.memory_bytes + retained_bytes
        sort_cost = estimate.scan_entries * 2

        %{
          estimate
          | sort_rows: estimate.scan_entries,
            memory_bytes: memory_bytes,
            cost: estimate.cost + sort_cost
        }

      :native ->
        %{
          estimate
          | memory_bytes: estimate.memory_bytes + retained_bytes
        }
    end
  end

  defp add_result_estimate(%Request{return: :count}, estimate, :none, _stat, _now_ms),
    do: estimate

  defp add_result_estimate(%Request{} = request, estimate, order, stat, now_ms),
    do: add_order_estimate(estimate, order, request.limit, stat, now_ms)

  defp result_records(%Request{return: :count}, _hydrated_records), do: 1
  defp result_records(%Request{limit: limit}, hydrated_records), do: min(limit, hydrated_records)

  defp within_budget(estimate, budget) do
    hard_scan_bytes = Map.get(estimate, :hard_scan_bytes, estimate.scan_bytes)

    hard_hydrated_records =
      Map.get(estimate, :hard_hydrated_records, estimate.hydrated_records)

    cond do
      estimate.range_seeks > budget.range_seeks ->
        {:error, :range_budget_exceeded}

      estimate.hard_scan_entries > budget.scan_entries ->
        {:error, :scan_budget_exceeded}

      hard_scan_bytes > budget.scan_bytes ->
        {:error, :scan_byte_budget_exceeded}

      hard_hydrated_records > budget.hydrated_records ->
        {:error, :hydration_budget_exceeded}

      estimate.result_records > budget.result_records ->
        {:error, :result_budget_exceeded}

      estimate.planner_memory_bytes > budget.planner_memory_bytes ->
        {:error, :memory_budget_exceeded}

      estimate.memory_bytes > budget.executor_memory_bytes ->
        {:error, :memory_budget_exceeded}

      true ->
        :ok
    end
  end

  defp enforce_specialized_budget(
         %Plan{} = plan,
         request,
         mandatory_scope,
         budget,
         fingerprint
       ) do
    case within_budget(plan.estimate, budget) do
      :ok ->
        {:ok, plan}

      {:error, reason} ->
        {:ok, reject_plan(request, mandatory_scope, budget, fingerprint, reason)}
    end
  end

  defp order_mode(%Request{return: :count}, _definition, _built), do: :none

  defp order_mode(request, definition, built) do
    requested = remove_constant_order_fields(request.order_by, built.equality_predicates)
    equality_fields = Enum.map(built.equality_predicates, &predicate_field/1)

    remaining_fields =
      definition.fields
      |> Enum.drop_while(fn {field, _direction, _encoding} -> field in equality_fields end)

    physical =
      Enum.map(remaining_fields, fn {field, direction, _encoding} -> {field, direction} end)

    exact_order? =
      requested != [] and requested == physical and
        Enum.all?(remaining_fields, fn {_field, _direction, encoding} -> encoding == :ordered end)

    if exact_order? and length(built.ranges) == 1 do
      :native
    else
      :bounded_top_k
    end
  end

  defp remove_constant_order_fields(order_by, equality_predicates) do
    constants =
      equality_predicates
      |> Enum.flat_map(fn
        {:eq, field, _value} -> [field]
        {:is, field, _kind} -> [field]
        {:in, field, [_single]} -> [field]
        _predicate -> []
      end)
      |> MapSet.new()

    Enum.reject(order_by, fn {field, _direction} -> MapSet.member?(constants, field) end)
  end

  defp deduplication_required?(%IndexDefinition{fields: fields}, built) do
    constrained = MapSet.new(built.equality_predicates, &predicate_field/1)

    length(built.ranges) > 1 or
      Enum.any?(fields, fn {field, _direction, _encoding} ->
        match?({:attribute, _name}, field) and not MapSet.member?(constrained, field)
      end)
  end

  defp counter_lookup?(%Request{return: :count}, definition, built, []) do
    prefix_lengths = Enum.map(built.ranges, &length(&1.equality_values))

    prefix_lengths != [] and not built.range_predicate? and
      Enum.all?(prefix_lengths, &(&1 in definition.count_prefixes)) and
      not multivalue_union?(built.equality_predicates)
  end

  defp counter_lookup?(_request, _definition, _built, _residual), do: false

  defp multivalue_union?(predicates) do
    Enum.any?(predicates, fn
      {:in, {:attribute, _name}, values} -> length(values) > 1
      _predicate -> false
    end)
  end

  defp lookup_stats(index, scope, opts) do
    source = Keyword.get(opts, :stats)
    identity = index_identity(index)
    scope_digest = IndexStatistics.scope_digest(scope)

    case safe_stats_lookup(source, index, scope, identity) do
      %IndexStatistics{
        index_id: id,
        index_version: version,
        scope_digest: digest
      } = stat
      when {id, version} == identity and digest == scope_digest ->
        case IndexStatistics.new(Map.from_struct(stat)) do
          {:ok, validated} -> validated
          {:error, _reason} -> nil
        end

      _invalid ->
        nil
    end
  end

  defp safe_stats_lookup(source, index, scope, identity) do
    cond do
      is_map(source) -> Map.get(source, identity)
      is_function(source, 2) -> source.(index, scope)
      match?(%IndexStatistics{}, index.stats) -> index.stats
      true -> nil
    end
  rescue
    _error -> nil
  catch
    _kind, _reason -> nil
  end

  defp stats_evidence(stat, _scope, estimate, now_ms) do
    base = %{source: estimate.estimate_source, confidence: estimate.confidence}

    case stat do
      %IndexStatistics{} ->
        Map.merge(base, %{
          version: stat.version,
          age_ms: max(now_ms - stat.collected_at_ms, 0),
          source_watermark: stat.source_watermark,
          sample_rate_ppm: stat.sample_rate_ppm
        })

      _missing ->
        Map.merge(base, %{version: nil, age_ms: nil, source_watermark: nil, sample_rate_ppm: 0})
    end
  end

  defp safe_active_index?(%RegisteredIndex{
         definition: definition,
         state: :active,
         coverage: %{
           complete_shards: complete,
           total_shards: total,
           validation: :passed
         }
       })
       when is_integer(total) and total > 0 and complete == total,
       do: IndexDefinition.validate(definition) == :ok

  defp safe_active_index?(_index), do: false

  defp scope_contained?(definition, scope_prefix, partition, ranges) do
    with {:ok, physical_prefix} <-
           CompositeIndex.encode_prefix(definition, scope_prefix, [partition]) do
      Enum.all?(ranges, fn %{range: range} ->
        String.starts_with?(range.prefix, physical_prefix) and
          valid_bound_prefix?(range.after_key, physical_prefix) and
          valid_bound_prefix?(range.before_key, physical_prefix)
      end)
    else
      _error -> false
    end
  end

  defp valid_bound_prefix?("", _physical_prefix), do: true

  defp valid_bound_prefix?(bound, physical_prefix),
    do: String.starts_with?(bound, physical_prefix)

  defp candidate_sort_key(plan) do
    order_rank = if plan.order == :native, do: 0, else: 1

    {
      plan.estimate.cost,
      plan.estimate.hard_scan_entries,
      length(plan.residual_predicates),
      order_rank,
      length(plan.ranges),
      plan.index_id,
      plan.index_version
    }
  end

  defp alternative(plan) do
    %{
      path: plan.path,
      index_id: plan.index_id,
      index_version: plan.index_version,
      index_build_id: plan.index_build_id,
      order: plan.order,
      range_count: length(plan.ranges),
      residual_predicate_count: length(plan.residual_predicates),
      estimate: plan.estimate,
      stats: plan.stats
    }
  end

  defp explain_alternatives(%Request{mode: mode}, plans) when mode in [:explain, :analyze],
    do: Enum.map(plans, &alternative/1)

  defp explain_alternatives(%Request{mode: :execute}, _plans), do: []

  defp statistics_probes(plans) do
    plans
    |> Enum.flat_map(& &1.statistics_probes)
    |> Enum.uniq_by(fn probe ->
      {probe.definition.id, probe.definition.version, probe.scope_digest, probe.equality_values}
    end)
    |> Enum.sort_by(fn probe ->
      {probe.definition.id, probe.definition.version, stable_term(probe.equality_values)}
    end)
  end

  defp selected_statistics_probes(%Request{mode: :explain}, _chosen), do: []

  defp selected_statistics_probes(%Request{mode: mode}, chosen)
       when mode in [:execute, :analyze],
       do: statistics_probes([chosen])

  defp candidate_statistics_probes(definition, built, stat, now_ms, scope_runtime) do
    ranges =
      if match?(%IndexStatistics{}, stat) and IndexStatistics.fresh?(stat, now_ms) do
        Enum.filter(built.ranges, fn range ->
          IndexStatistics.prefix_count(stat, range.equality_values, now_ms) == :unknown
        end)
      else
        built.ranges
      end

    scope_digest = IndexStatistics.scope_digest(scope_runtime.statistics_key)

    ranges
    |> Enum.map(fn range ->
      %{
        definition: definition,
        equality_values: range.equality_values,
        range: range.range,
        scope_prefix: scope_runtime.prefix,
        physical_partition_key: scope_runtime.physical_partition_key,
        statistics_key: scope_runtime.statistics_key,
        scope_digest: scope_digest,
        prefix_digest: IndexStatistics.prefix_digest(range.equality_values)
      }
    end)
    |> Enum.uniq_by(& &1.equality_values)
    |> Enum.sort_by(&stable_term(&1.equality_values))
  end

  defp rejection_reason(reasons) do
    priority = [
      :tenant_range_violation,
      :range_budget_exceeded,
      :scan_budget_exceeded,
      :scan_byte_budget_exceeded,
      :hydration_budget_exceeded,
      :result_budget_exceeded,
      :memory_budget_exceeded
    ]

    Enum.find(priority, :no_active_bounded_index, &(&1 in reasons))
  end

  defp candidate_path(%Request{return: :count}, _ranges, _range?), do: :count_scan
  defp candidate_path(%Request{}, [_one], true), do: :ordered_range
  defp candidate_path(%Request{}, [_one], false), do: :ordered_filter
  defp candidate_path(%Request{}, _many, _range?), do: :ordered_range_union

  defp primary_plan(request, mandatory_scope, budget, fingerprint) do
    %Plan{
      path: :primary_key,
      index_id: @primary_index,
      index_version: 1,
      index_build_id: @primary_index,
      definition: nil,
      ranges: [],
      order: :native,
      residual_predicates: [],
      recheck_predicates: request_predicates(request),
      constraint_shapes: constraint_shapes(request_predicates(request)),
      estimate: %{
        range_seeks: 1,
        scan_entries: 1,
        hard_scan_entries: 1,
        hydrated_records: 1,
        result_records: 1,
        scan_bytes: 0,
        hydration_bytes: 0,
        residual_checks: length(request_predicates(request)),
        sort_rows: 0,
        planner_memory_bytes: 0,
        memory_bytes: 0,
        cost: 1
      },
      stats: %{source: :primary_key, confidence: :exact},
      budget: budget,
      fallback_reason: :none,
      query_fingerprint: fingerprint,
      mandatory_scope: mandatory_scope,
      alternatives: []
    }
  end

  defp history_plan(request, mandatory_scope, budget, fingerprint) do
    checks = length(request_predicates(request))
    limit = request.limit
    scan_bound = 2 * (limit + 1)
    hydration_bound = limit + 1

    %Plan{
      path: :history,
      index_id: @history_index,
      index_version: 1,
      index_build_id: @history_index,
      definition: nil,
      ranges: [],
      order: :native,
      residual_predicates: [],
      recheck_predicates: request_predicates(request),
      constraint_shapes: constraint_shapes(request_predicates(request)),
      estimate: %{
        range_seeks: 2,
        scan_entries: scan_bound,
        hard_scan_entries: scan_bound,
        hydrated_records: hydration_bound,
        result_records: limit,
        scan_bytes: 0,
        hydration_bytes: 0,
        residual_checks: hydration_bound * checks,
        sort_rows: 0,
        planner_memory_bytes: 0,
        memory_bytes: 0,
        cost: scan_bound
      },
      stats: %{source: :history_index, confidence: :exact},
      budget: budget,
      fallback_reason: :none,
      query_fingerprint: fingerprint,
      mandatory_scope: mandatory_scope,
      alternatives: []
    }
  end

  defp lineage_plan(request, mandatory_scope, budget, fingerprint) do
    {:ok, lineage} = Request.lineage_descriptor(request)
    checks = length(request_predicates(request))
    limit = request.limit
    root_probe = if lineage.kind == :root, do: 1, else: 0
    range_seeks = 2 + root_probe
    scan_bound = 2 * (limit + 1) + root_probe
    hydration_bound = limit + 1 + root_probe
    index_id = Map.fetch!(@lineage_indexes, lineage.kind)

    %Plan{
      path: :lineage,
      index_id: index_id,
      index_version: 1,
      index_build_id: index_id,
      definition: nil,
      ranges: [],
      order: :native,
      residual_predicates: [],
      recheck_predicates: request_predicates(request),
      constraint_shapes: constraint_shapes(request_predicates(request)),
      estimate: %{
        range_seeks: range_seeks,
        scan_entries: scan_bound,
        hard_scan_entries: scan_bound,
        hydrated_records: hydration_bound,
        result_records: limit,
        scan_bytes: 0,
        hydration_bytes: 0,
        residual_checks: hydration_bound * checks,
        sort_rows: 0,
        planner_memory_bytes: 0,
        memory_bytes: 0,
        cost: scan_bound
      },
      stats: %{source: :lineage_index, confidence: :exact, kind: lineage.kind},
      budget: budget,
      fallback_reason: :none,
      query_fingerprint: fingerprint,
      mandatory_scope: mandatory_scope,
      alternatives: []
    }
  end

  defp empty_plan(request, mandatory_scope, budget, fingerprint) do
    estimate =
      if request.return == :count,
        do: %{zero_estimate() | result_records: 1},
        else: zero_estimate()

    %Plan{
      path: :empty,
      ranges: [],
      order: :native,
      residual_predicates: [],
      recheck_predicates: request_predicates(request),
      constraint_shapes: constraint_shapes(request_predicates(request)),
      estimate: estimate,
      stats: %{source: :logical_empty, confidence: :exact},
      budget: budget,
      fallback_reason: :none,
      query_fingerprint: fingerprint,
      mandatory_scope: mandatory_scope,
      alternatives: []
    }
  end

  defp fixed_index_plan(request, mandatory_scope, budget, fingerprint) do
    with {:ok, fixed} <- FixedIndexExecutor.plan(request) do
      checks = length(request_predicates(request))

      estimate = %{
        range_seeks: fixed.range_seeks,
        scan_entries: fixed.scan_entries,
        hard_scan_entries: fixed.scan_entries,
        scan_bytes: 0,
        hard_scan_bytes: budget.scan_bytes,
        hydrated_records: fixed.scan_entries,
        hard_hydrated_records: fixed.scan_entries,
        hydration_bytes: 0,
        result_records: request.limit,
        residual_checks: fixed.scan_entries * checks,
        sort_rows: 0,
        planner_memory_bytes: 0,
        memory_bytes: budget.executor_memory_bytes,
        cost: fixed.scan_entries
      }

      with :ok <- within_budget(estimate, budget) do
        {:ok,
         %Plan{
           path: :fixed_index,
           index_id: fixed.index_id,
           index_version: 1,
           index_build_id: fixed.index_id,
           definition: nil,
           ranges: [],
           deduplicate: false,
           order: :native,
           residual_predicates: [],
           recheck_predicates: request_predicates(request),
           constraint_shapes: constraint_shapes(request_predicates(request)),
           estimate: estimate,
           stats: %{source: :fixed_index, confidence: :exact},
           budget: budget,
           fallback_reason: :none,
           query_fingerprint: fingerprint,
           mandatory_scope: mandatory_scope,
           alternatives: []
         }}
      end
    end
  end

  defp reject_plan(request, mandatory_scope, budget, fingerprint, reason) do
    %Plan{
      path: :reject,
      ranges: [],
      order: :none,
      residual_predicates: request_predicates(request),
      recheck_predicates: request_predicates(request),
      constraint_shapes: [],
      estimate: zero_estimate(),
      stats: %{source: :none, confidence: :none},
      budget: budget,
      fallback_reason: reason,
      query_fingerprint: fingerprint,
      mandatory_scope: mandatory_scope,
      alternatives: []
    }
  end

  defp zero_estimate do
    %{
      range_seeks: 0,
      scan_entries: 0,
      hard_scan_entries: 0,
      hydrated_records: 0,
      result_records: 0,
      scan_bytes: 0,
      hydration_bytes: 0,
      residual_checks: 0,
      sort_rows: 0,
      planner_memory_bytes: 0,
      memory_bytes: 0,
      cost: 0
    }
  end

  defp empty_time_window?(request) do
    Enum.any?(request_predicates(request), fn
      {:time_window, _field, lower, upper} -> literal_value(lower) == literal_value(upper)
      _predicate -> false
    end)
  end

  defp request_scope(%Request{predicate: {:and, predicates}}) do
    Enum.find_value(predicates, {:error, :unscoped_query}, fn
      {:eq, :partition_key, {:literal, :keyword, value}}
      when is_binary(value) and value != "" ->
        {:ok, value}

      _predicate ->
        false
    end)
  end

  defp request_predicates(%Request{predicate: {:and, predicates}}), do: predicates

  defp residual_predicates(predicates, used) do
    Enum.reject(predicates, fn predicate -> Enum.any?(used, &(&1 == predicate)) end)
  end

  defp constraint_shapes(predicates) do
    predicates
    |> Enum.map(fn predicate ->
      %{
        field: Field.external_name(predicate_field(predicate)),
        operator: predicate_operator(predicate)
      }
    end)
    |> Enum.sort_by(&{&1.field, &1.operator})
  end

  defp predicate_field({_operator, field, _value}), do: field
  defp predicate_field({_operator, field, _lower, _upper}), do: field

  defp predicate_operator({operator, _field, _value}), do: operator
  defp predicate_operator({operator, _field, _lower, _upper}), do: operator

  defp literal_value({:literal, _type, value}), do: value

  defp index_identity(%RegisteredIndex{definition: definition}),
    do: {definition.id, definition.version}

  defp sample_value(%IndexStatistics{} = stat, field, default, now_ms) do
    if IndexStatistics.sample_fresh?(stat, field, now_ms),
      do: Map.fetch!(stat, field),
      else: default
  end

  defp sample_value(_stat, _field, default, _now_ms), do: default

  defp predicate_shape({:eq, field, value}),
    do: {:eq, Field.external_name(field), value_shape(value)}

  defp predicate_shape({:in, field, values}),
    do: {:in, Field.external_name(field), Enum.map(values, &value_shape/1) |> Enum.sort()}

  defp predicate_shape({operator, field, lower, upper}) when operator in [:range, :time_window],
    do: {operator, Field.external_name(field), value_shape(lower), value_shape(upper)}

  defp predicate_shape({:is, field, kind}), do: {:is, Field.external_name(field), kind}

  defp value_shape({:literal, type, _value}), do: {:literal, type}
  defp value_shape({:parameter, type, _name}), do: {:parameter, type}

  defp stable_term(term), do: :erlang.term_to_binary(term, minor_version: 2)
end
