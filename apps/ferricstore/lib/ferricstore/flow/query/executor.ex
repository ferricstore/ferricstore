defmodule Ferricstore.Flow.Query.Executor do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB, Locator}
  alias Ferricstore.Flow.RecordProjection, as: FlowRecordProjection
  alias Ferricstore.TermCodec

  alias Ferricstore.Flow.Query.{
    CompositeCounter,
    CompositeIndex,
    CompositeRange,
    CompositeRangeReader,
    CoveringProjection,
    ExecutionSource,
    IndexDefinition,
    Limits,
    MandatoryScope,
    PageMemory,
    QueryRecordStore,
    QueryRow,
    QueryRowProjection,
    QueryRowStore,
    RecordOrder,
    RecordProjection,
    ReferenceEvaluator,
    Request
  }

  alias Ferricstore.Store.Router
  alias FerricStore.Flow.MetadataExtension

  alias Ferricstore.Flow.Query.{
    Budget,
    CountResult,
    Cursor,
    CursorKeyStore,
    ExecutionResult,
    MemoryBudget,
    Plan,
    Planner
  }

  @default_page_entries 512
  @maximum_page_entries 4_096
  @default_page_bytes 1 * 1_024 * 1_024
  @maximum_page_bytes 8 * 1_024 * 1_024
  @seen_entry_overhead 96
  @hydrated_record_overhead 128
  @deadline_check_interval 64
  @maximum_exact_integer 9_007_199_254_740_991
  @maximum_expire_at_ms 0xFFFF_FFFF_FFFF_FFFF
  @maximum_storage_key_bytes 511
  @maximum_run_id_bytes Limits.max_run_id_bytes()
  @maximum_sort_key_bytes Limits.max_sort_key_bytes()

  @spec execute(map(), non_neg_integer(), Request.t(), Plan.t(), keyword()) ::
          {:ok, ExecutionResult.t()} | {:error, atom()}
  def execute(ctx, shard_index, request, plan, opts \\ [])

  def execute(ctx, shard_index, %Request{} = request, %Plan{} = plan, opts)
      when is_map(ctx) and is_integer(shard_index) and is_list(opts) do
    with {:ok, state} <- initialize(ctx, shard_index, request, plan, opts) do
      case plan.path do
        :empty -> finalize(state)
        :reject -> {:error, rejection_error(plan.fallback_reason)}
        :counter_lookup -> run_counter(state)
        :count_scan -> run(state)
        path when path in [:ordered_range, :ordered_range_union, :ordered_filter] -> run(state)
        _unsupported -> {:error, :unsupported_query_shape}
      end
    end
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    _kind, _reason -> {:error, :query_engine_failure}
  end

  def execute(_ctx, _shard_index, _request, _plan, _opts),
    do: {:error, :unsupported_query_shape}

  defp initialize(ctx, shard_index, request, plan, opts) do
    budget = plan.budget
    page_entries = Keyword.get(opts, :page_entries, @default_page_entries)
    page_bytes = Keyword.get(opts, :page_bytes, @default_page_bytes)
    range_read = Keyword.get(opts, :range_read, &default_range_read/5)

    record_read =
      Keyword.get_lazy(opts, :record_read, fn ->
        fn path, state_keys, now_ms, max_value_bytes, timeout_ms ->
          default_record_read(
            ctx,
            shard_index,
            path,
            state_keys,
            now_ms,
            max_value_bytes,
            timeout_ms
          )
        end
      end)

    query_row_read = Keyword.get(opts, :query_row_read, &QueryRowStore.read_many/4)
    clock_us = Keyword.get(opts, :clock_us, fn -> System.monotonic_time(:microsecond) end)
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))

    with :ok <- Request.validate_bound(request),
         {:ok, _validated_budget} <- validate_budget(budget),
         :ok <- validate_scope_authority(ctx, plan.mandatory_scope),
         {:ok, logical_partition} <- Ferricstore.Flow.Query.partition_key(request),
         {:ok, scope_keys} <-
           MandatoryScope.derive_keys(plan.mandatory_scope, logical_partition),
         :ok <-
           validate_options(
             ctx,
             shard_index,
             page_entries,
             page_bytes,
             range_read,
             record_read,
             query_row_read,
             clock_us,
             now_ms
           ),
         :ok <- validate_route(ctx, shard_index, scope_keys.physical_partition_key),
         :ok <- validate_plan(plan, request, logical_partition),
         {:ok, record_matcher} <- prepare_record_matcher(plan, logical_partition),
         {:ok, prepared_residual_predicates} <-
           prepare_residual_predicates(plan.residual_predicates),
         covering_execution <-
           covering_execution(request, plan),
         query_row_allowed <- query_row_execution_enabled?(opts),
         query_row_execution <-
           plan.record_source == :query_row and QueryRowProjection.eligible?(request, plan) and
             query_row_allowed,
         {:ok, cursor_seek, cursor_key} <-
           initialize_cursor(ctx, request, plan, scope_keys.query_binding, now_ms, opts),
         {:ok, start_us} <- call_clock(clock_us),
         {:ok, deadline_us} <- deadline(start_us, budget, opts) do
      path =
        Keyword.get_lazy(opts, :path, fn ->
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> LMDB.path()
        end)

      if is_binary(path) and path != "" do
        {:ok,
         %{
           ctx: ctx,
           shard_index: shard_index,
           request: request,
           plan: plan,
           budget: budget,
           logical_partition: logical_partition,
           physical_partition: scope_keys.physical_partition_key,
           query_binding: scope_keys.query_binding,
           mandatory_scope: plan.mandatory_scope,
           record_matcher: record_matcher,
           prepared_residual_predicates: prepared_residual_predicates,
           prepared_residual_bytes: MemoryBudget.term_bytes(prepared_residual_predicates),
           covering_execution: covering_execution,
           query_row_allowed: query_row_allowed,
           query_row_execution: query_row_execution,
           path: path,
           range_read: range_read,
           record_read: record_read,
           query_row_read: query_row_read,
           page_entries: page_entries,
           page_bytes: page_bytes,
           clock_us: clock_us,
           start_us: start_us,
           deadline_us: deadline_us,
           now_ms: now_ms,
           cursor_seek: cursor_seek,
           cursor_key: cursor_key,
           cursor_ttl_ms: Keyword.get(opts, :cursor_ttl_ms),
           deduplicate: plan.deduplicate,
           seen: MapSet.new(),
           seen_bytes: 0,
           selected: initial_selection(plan.order),
           selected_bytes: 0,
           matched_count: 0,
           live_page_bytes: 0,
           live_hydrated_bytes: 0,
           last_native_key: cursor_sort_key(cursor_seek, plan.order),
           has_more: false,
           usage: initial_usage(length(plan.ranges)),
           checks_since_deadline: 0
         }}
      else
        {:error, :query_storage_unavailable}
      end
    end
  end

  defp prepare_residual_predicates(predicates) do
    case ReferenceEvaluator.prepare(predicates) do
      {:ok, prepared} -> {:ok, prepared}
      {:error, _reason} -> {:error, :query_storage_inconsistent}
    end
  end

  defp query_row_execution_enabled?(opts) do
    not Keyword.has_key?(opts, :record_read) or Keyword.has_key?(opts, :query_row_read)
  end

  defp covering_execution(request, %Plan{record_source: :covering_index} = plan),
    do: CoveringProjection.field_types(request, plan.definition, plan.residual_predicates)

  defp covering_execution(_request, %Plan{}), do: nil

  defp run(state) do
    with :ok <- check_deadline(state),
         {:ok, state} <- scan_ranges(state.plan.ranges, state),
         :ok <- check_deadline(state) do
      finalize(state)
    end
  end

  defp run_counter(state) do
    prefixes = Enum.map(state.plan.ranges, & &1.prefix)

    with :ok <- check_deadline(state),
         :ok <- enforce_counter_entry_budget(state, prefixes),
         {:ok, read} <-
           read_counters(
             state.path,
             state.plan.definition,
             prefixes,
             state.budget.scan_bytes - state.usage.scanned_bytes
           ),
         :ok <- validate_counter_read(read, prefixes),
         {:ok, state} <- account_counter_read(state, read, 0),
         :ok <- check_deadline(state) do
      if Enum.any?(read.expiring_counts, &(&1 > 0)) do
        state |> prepare_counter_scan_source() |> run()
      else
        with {:ok, count} <- sum_counters(read.counts) do
          state |> Map.put(:matched_count, count) |> finalize()
        end
      end
    end
  end

  defp enforce_counter_entry_budget(state, prefixes) do
    if length(prefixes) + state.usage.scanned_entries <= state.budget.scan_entries,
      do: :ok,
      else: {:error, :query_scan_budget_exceeded}
  end

  defp prepare_counter_scan_source(state) do
    source =
      ExecutionSource.classify(
        state.request,
        :count_scan,
        state.plan.definition,
        state.plan.residual_predicates
      )

    covering_execution =
      if source == :covering_index,
        do:
          CoveringProjection.field_types(
            state.request,
            state.plan.definition,
            state.plan.residual_predicates
          ),
        else: nil

    %{
      state
      | covering_execution: covering_execution,
        query_row_execution: source == :query_row and state.query_row_allowed
    }
  end

  defp read_counters(path, definition, prefixes, max_bytes) when max_bytes > 0 do
    case CompositeCounter.read_prefixes(path, definition, prefixes, max_bytes) do
      {:ok, read} -> {:ok, read}
      {:error, :batch_value_budget_exceeded} -> {:error, :query_scan_byte_budget_exceeded}
      {:error, :invalid_composite_counter} -> {:error, :query_storage_inconsistent}
      {:error, :invalid_composite_counter_read} -> {:error, :query_storage_inconsistent}
      {:error, :invalid_composite_counter_prefixes} -> {:error, :query_storage_inconsistent}
      {:error, _reason} -> {:error, :query_storage_unavailable}
      _invalid -> {:error, :query_storage_inconsistent}
    end
  rescue
    _error -> {:error, :query_storage_unavailable}
  catch
    _kind, _reason -> {:error, :query_storage_unavailable}
  end

  defp read_counters(_path, _definition, _prefixes, _max_bytes),
    do: {:error, :query_scan_byte_budget_exceeded}

  defp validate_counter_read(
         %{
           counts: counts,
           expiring_counts: expiring_counts,
           scanned_entries: scanned_entries,
           scanned_bytes: scanned_bytes,
           memory_bytes: memory_bytes
         },
         prefixes
       )
       when is_list(counts) and is_list(expiring_counts) and
              length(counts) == length(prefixes) and
              length(expiring_counts) == length(prefixes) and
              is_integer(scanned_entries) and scanned_entries == length(prefixes) and
              is_integer(scanned_bytes) and scanned_bytes >= 0 and is_integer(memory_bytes) and
              memory_bytes >= scanned_bytes do
    if valid_counter_states?(counts, expiring_counts),
      do: :ok,
      else: {:error, :query_storage_inconsistent}
  end

  defp validate_counter_read(_read, _prefixes),
    do: {:error, :query_storage_inconsistent}

  defp valid_counter_states?(counts, expiring_counts) do
    counts
    |> Enum.zip(expiring_counts)
    |> Enum.all?(fn {count, expiring_count} ->
      is_integer(count) and count >= 0 and count <= @maximum_expire_at_ms and
        is_integer(expiring_count) and expiring_count >= 0 and expiring_count <= count
    end)
  end

  defp sum_counters(counts) do
    Enum.reduce_while(counts, {:ok, 0}, fn count, {:ok, total} ->
      if is_integer(count) and count >= 0 and count <= @maximum_expire_at_ms - total,
        do: {:cont, {:ok, total + count}},
        else: {:halt, {:error, :query_storage_inconsistent}}
    end)
  end

  defp account_counter_read(state, read, count) do
    scanned_entries = state.usage.scanned_entries + read.scanned_entries
    scanned_bytes = state.usage.scanned_bytes + read.scanned_bytes
    memory_high_water = max(state.usage.memory_high_water_bytes, read.memory_bytes)

    cond do
      scanned_entries > state.budget.scan_entries ->
        {:error, :query_scan_budget_exceeded}

      scanned_bytes > state.budget.scan_bytes ->
        {:error, :query_scan_byte_budget_exceeded}

      memory_high_water > state.budget.executor_memory_bytes ->
        {:error, :query_memory_budget_exceeded}

      true ->
        usage = %{
          state.usage
          | scanned_entries: scanned_entries,
            scanned_bytes: scanned_bytes,
            memory_high_water_bytes: memory_high_water
        }

        {:ok, %{state | matched_count: count, usage: usage}}
    end
  end

  defp scan_ranges([], state), do: {:ok, state}

  defp scan_ranges([range], %{plan: %{order: :native}} = state) do
    case scan_range(range, native_storage_cursor(state.cursor_seek), state) do
      {:ok, state} -> {:ok, state}
      {:halt, state} -> {:ok, state}
      {:error, _reason} = error -> error
    end
  end

  defp scan_ranges([range | ranges], state) do
    case scan_range(range, nil, state) do
      {:ok, state} -> scan_ranges(ranges, state)
      {:halt, state} -> {:ok, state}
      {:error, _reason} = error -> error
    end
  end

  defp scan_range(range, cursor, state) do
    if native_complete?(state) do
      {:halt, %{state | has_more: true}}
    else
      with :ok <- check_deadline(state),
           {:ok, max_entries, max_bytes} <- page_budget(state),
           {:ok, page} <- read_page(state, range, cursor, max_entries, max_bytes),
           :ok <- check_deadline(state),
           {:ok, state} <- account_page(state, page),
           {:ok, state} <- retain_page(state, page),
           {:ok, state} <- process_entries(state, range, cursor, page.entries) do
        state = release_page(state)

        cond do
          native_complete?(state) ->
            {:halt, %{state | has_more: true}}

          page.exhausted ->
            {:ok, state}

          true ->
            scan_range(range, page.cursor, state)
        end
      end
    end
  end

  defp page_budget(state) do
    remaining_entries = state.budget.scan_entries - state.usage.scanned_entries
    remaining_bytes = state.budget.scan_bytes - state.usage.scanned_bytes
    page_entries = effective_page_entries(state)

    cond do
      remaining_entries <= 0 ->
        {:error, :query_scan_budget_exceeded}

      remaining_bytes <= 0 ->
        {:error, :query_scan_byte_budget_exceeded}

      true ->
        {:ok, min(page_entries, remaining_entries), min(state.page_bytes, remaining_bytes)}
    end
  end

  defp effective_page_entries(
         %{
           plan: %{order: :native, residual_predicates: [], deduplicate: false}
         } = state
       ) do
    lookahead_remaining = state.request.limit + 1 - selection_count(state)
    min(state.page_entries, max(lookahead_remaining, 1))
  end

  defp effective_page_entries(state), do: state.page_entries

  defp read_page(state, range, cursor, max_entries, max_bytes) do
    result =
      try do
        state.range_read.(state.path, range, cursor, max_entries, max_bytes)
      rescue
        _error -> {:error, :query_storage_unavailable}
      catch
        _kind, _reason -> {:error, :query_storage_unavailable}
      end

    case result do
      {:ok, page} -> validate_page(page, range, cursor, max_entries, max_bytes)
      {:error, :range_entry_too_large} -> {:error, :query_scan_byte_budget_exceeded}
      {:error, :invalid_composite_entry} -> {:error, :query_storage_inconsistent}
      {:error, :invalid_composite_cursor} -> {:error, :query_storage_inconsistent}
      {:error, :invalid_lmdb_range} -> {:error, :query_storage_inconsistent}
      {:error, _reason} -> {:error, :query_storage_unavailable}
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp validate_page(
         %{
           entries: entries,
           cursor: next_cursor,
           exhausted: exhausted,
           scanned_entries: scanned_entries,
           scanned_bytes: scanned_bytes
         } = page,
         range,
         cursor,
         max_entries,
         max_bytes
       )
       when is_list(entries) and is_boolean(exhausted) and is_integer(scanned_entries) and
              is_integer(scanned_bytes) and scanned_entries >= 0 and scanned_bytes >= 0 and
              scanned_entries == length(entries) and scanned_entries <= max_entries and
              scanned_bytes <= max_bytes do
    with :ok <- validate_entries(entries, range, cursor),
         :ok <- validate_page_cursor(entries, next_cursor, exhausted),
         {:ok, memory_bytes} <- validate_scanned_bytes(entries, scanned_bytes) do
      {:ok, Map.put(page, :memory_bytes, memory_bytes)}
    end
  end

  defp validate_page(_page, _range, _cursor, _max_entries, _max_bytes),
    do: {:error, :query_storage_inconsistent}

  defp validate_page_cursor(_entries, nil, true), do: :ok

  defp validate_page_cursor(entries, cursor, false) when entries != [] and is_binary(cursor) do
    if List.last(entries).storage_key == cursor,
      do: :ok,
      else: {:error, :query_storage_inconsistent}
  end

  defp validate_page_cursor(_entries, _cursor, _exhausted),
    do: {:error, :query_storage_inconsistent}

  defp validate_entries(entries, range, cursor) do
    lower = if is_binary(cursor), do: cursor, else: range.after_key

    entries
    |> Enum.reduce_while({:ok, lower}, fn entry, {:ok, previous_key} ->
      case validate_entry(entry, range, previous_key) do
        :ok -> {:cont, {:ok, entry.storage_key}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, _last_key} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_entry(
         %{
           id: id,
           state_key: state_key,
           record_version: record_version,
           expire_at_ms: expire_at_ms,
           storage_key: storage_key,
           storage_bytes: storage_bytes,
           covering_record: covering_record
         },
         range,
         previous_key
       )
       when is_binary(id) and id != "" and byte_size(id) <= @maximum_run_id_bytes and
              is_binary(state_key) and state_key != "" and
              is_integer(record_version) and record_version >= 0 and
              record_version <= @maximum_exact_integer and is_integer(expire_at_ms) and
              expire_at_ms >= 0 and expire_at_ms <= @maximum_expire_at_ms and
              is_binary(storage_key) and byte_size(storage_key) <= @maximum_storage_key_bytes and
              is_integer(storage_bytes) and
              storage_bytes > 0 and storage_bytes <= @maximum_page_bytes and
              (is_nil(covering_record) or is_map(covering_record)) do
    valid =
      storage_bytes >= byte_size(storage_key) + byte_size(id) + byte_size(state_key) and
        String.starts_with?(storage_key, range.prefix) and
        (previous_key == "" or storage_key > previous_key) and
        (range.before_key == "" or storage_key < range.before_key) and
        CompositeIndex.entry_key_matches_id?(storage_key, id)

    if valid, do: :ok, else: {:error, :query_storage_inconsistent}
  end

  defp validate_entry(_entry, _range, _previous_key),
    do: {:error, :query_storage_inconsistent}

  defp validate_scanned_bytes(entries, scanned_bytes) do
    {actual_bytes, memory_bytes} = PageMemory.measure(entries)

    if actual_bytes == scanned_bytes,
      do: {:ok, memory_bytes},
      else: {:error, :query_storage_inconsistent}
  end

  defp account_page(state, page) do
    usage = %{
      state.usage
      | range_pages: state.usage.range_pages + 1,
        scanned_entries: state.usage.scanned_entries + page.scanned_entries,
        scanned_bytes: state.usage.scanned_bytes + page.scanned_bytes
    }

    cond do
      usage.scanned_entries > state.budget.scan_entries ->
        {:error, :query_scan_budget_exceeded}

      usage.scanned_bytes > state.budget.scan_bytes ->
        {:error, :query_scan_byte_budget_exceeded}

      true ->
        {:ok, %{state | usage: usage}}
    end
  end

  defp process_entries(state, _range, _cursor, []), do: {:ok, state}

  defp process_entries(state, _range, _cursor, entries) do
    with {:ok, candidates, state} <- unique_candidates(entries, state),
         {:ok, state} <- process_candidates(candidates, state) do
      {:ok, state}
    end
  end

  defp process_candidates(candidates, %{covering_execution: nil} = state),
    do: process_uncovered_candidates(candidates, state)

  defp process_candidates(candidates, state) do
    cond do
      Enum.any?(candidates, &missing_covering_projection?/1) ->
        {:error, :query_storage_inconsistent}

      Enum.all?(candidates, &covering_candidate?/1) ->
        process_covering_candidates(candidates, state)

      true ->
        process_uncovered_candidates(candidates, state)
    end
  end

  defp missing_covering_projection?(%{covering_record: nil}), do: true
  defp missing_covering_projection?(_entry), do: false

  defp process_uncovered_candidates(candidates, %{query_row_execution: true} = state),
    do: process_query_row_candidates(candidates, state)

  defp process_uncovered_candidates(candidates, state), do: hydrate_candidates(candidates, state)

  defp covering_candidate?(%{covering_record: record, expire_at_ms: expire_at_ms})
       when is_map(record) and is_integer(expire_at_ms) and expire_at_ms >= 0,
       do: true

  defp covering_candidate?(_entry), do: false

  defp process_covering_candidates(candidates, state) do
    Enum.reduce_while(candidates, {:ok, state}, fn entry, {:ok, state} ->
      with {:ok, state} <- account_deadline_work(state) do
        cond do
          native_complete?(state) ->
            {:halt, {:ok, state}}

          not valid_covering_entry?(entry, state) ->
            {:halt, {:error, :query_storage_inconsistent}}

          projected_entry_expired?(entry, state.now_ms) ->
            {:cont, {:ok, state}}

          true ->
            case process_current_record(entry.covering_record, entry, state) do
              {:ok, state} -> {:cont, {:ok, state}}
              {:error, _reason} = error -> {:halt, error}
            end
        end
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp valid_covering_entry?(entry, state) do
    record = entry.covering_record

    Map.get(record, :id) == entry.id and
      Map.get(record, :version) == entry.record_version and
      Map.get(record, :partition_key) == state.logical_partition and
      valid_covering_shape?(record, state.covering_execution) and
      CompositeIndex.entry_key_matches_record_validated?(
        state.record_matcher,
        record,
        entry.storage_key
      )
  end

  defp valid_covering_shape?(record, covering_fields) when is_map(covering_fields) do
    Enum.all?(record, fn {field, value} ->
      case Map.fetch(covering_fields, field) do
        {:ok, type} -> valid_covering_value?(value, type)
        :error -> false
      end
    end)
  end

  defp valid_covering_shape?(_record, _covering_fields), do: false

  defp valid_covering_value?(nil, _type), do: true
  defp valid_covering_value?(value, :integer), do: is_integer(value)
  defp valid_covering_value?(value, :keyword), do: is_binary(value)

  defp valid_covering_value?(value, :dynamic) when is_list(value),
    do: Enum.all?(value, &valid_dynamic_covering_scalar?/1)

  defp valid_covering_value?(value, :dynamic), do: valid_dynamic_covering_scalar?(value)
  defp valid_covering_value?(_value, _type), do: false

  defp valid_dynamic_covering_scalar?(value)
       when is_nil(value) or is_boolean(value) or is_integer(value) or is_binary(value),
       do: true

  defp valid_dynamic_covering_scalar?(value) when is_float(value) do
    <<_sign::1, exponent::unsigned-big-11, _fraction::unsigned-big-52>> =
      <<value::float-big-64>>

    exponent != 0x7FF
  end

  defp valid_dynamic_covering_scalar?(_value), do: false

  defp unique_candidates(entries, %{deduplicate: false} = state) do
    entries
    |> Enum.reduce_while({:ok, [], state}, fn entry, {:ok, candidates, state} ->
      with {:ok, state} <- account_deadline_work(state) do
        cond do
          entry.state_key != Keys.state_key(entry.id, state.physical_partition) ->
            {:halt, {:error, :query_storage_inconsistent}}

          true ->
            {:cont, {:ok, [entry | candidates], state}}
        end
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, candidates, state} -> {:ok, Enum.reverse(candidates), state}
      {:error, _reason} = error -> error
    end
  end

  defp unique_candidates(entries, %{deduplicate: true} = state) do
    do_unique_candidates(entries, state)
  end

  defp unique_candidates(_entries, _state), do: {:error, :query_storage_inconsistent}

  defp do_unique_candidates(entries, state) do
    entries
    |> Enum.reduce_while({:ok, [], state}, fn entry, {:ok, candidates, state} ->
      with {:ok, state} <- account_deadline_work(state) do
        cond do
          entry.state_key != Keys.state_key(entry.id, state.physical_partition) ->
            {:halt, {:error, :query_storage_inconsistent}}

          MapSet.member?(state.seen, entry.id) ->
            usage = %{state.usage | duplicate_entries: state.usage.duplicate_entries + 1}
            {:cont, {:ok, candidates, %{state | usage: usage}}}

          true ->
            seen = MapSet.put(state.seen, entry.id)
            seen_bytes = state.seen_bytes + byte_size(entry.id) + @seen_entry_overhead
            state = %{state | seen: seen, seen_bytes: seen_bytes}

            if estimated_memory(state, 0) > state.budget.executor_memory_bytes do
              {:halt, {:error, :query_memory_budget_exceeded}}
            else
              {:cont, {:ok, [entry | candidates], state}}
            end
        end
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, candidates, state} -> {:ok, Enum.reverse(candidates), state}
      {:error, _reason} = error -> error
    end
  end

  defp process_query_row_candidates([], state), do: {:ok, state}

  defp process_query_row_candidates(candidates, state) do
    case read_query_rows(state, Enum.map(candidates, & &1.state_key)) do
      {:ok, rows, complete?} ->
        process_query_row_prefix(candidates, rows, complete?, state)

      {:error, :query_row_batch_too_large} when length(candidates) > 1 ->
        {left, right} = Enum.split(candidates, div(length(candidates), 2))

        with {:ok, state} <- process_query_row_candidates(left, state) do
          if native_complete?(state),
            do: {:ok, state},
            else: process_query_row_candidates(right, state)
        end

      {:error, :query_row_batch_too_large} ->
        {:error, :query_memory_budget_exceeded}

      {:error, :query_storage_inconsistent} = error ->
        error

      {:error, :query_deadline_exceeded} = error ->
        error

      {:error, _reason} ->
        {:error, :query_storage_unavailable}

      _invalid ->
        {:error, :query_storage_inconsistent}
    end
  end

  defp read_query_rows(state, state_keys) do
    available_bytes = state.budget.executor_memory_bytes - estimated_memory(state, 0)
    max_value_bytes = MemoryBudget.encoded_record_input_bytes(available_bytes)

    if max_value_bytes > 0 do
      try do
        case state.query_row_read.(state.path, state_keys, state.now_ms, max_value_bytes) do
          {:ok, rows, value_bytes, complete?}
          when is_list(rows) and is_integer(value_bytes) and value_bytes >= 0 and
                 value_bytes <= max_value_bytes and is_boolean(complete?) ->
            with :ok <- check_deadline(state), do: {:ok, rows, complete?}

          {:error, :batch_value_budget_exceeded} ->
            {:error, :query_row_batch_too_large}

          {:error, reason}
          when reason in [
                 :invalid_query_row,
                 :invalid_query_row_read,
                 :query_row_result_count_mismatch
               ] ->
            {:error, :query_storage_inconsistent}

          {:error, _reason} = error ->
            error

          _invalid ->
            {:error, :query_storage_inconsistent}
        end
      rescue
        _error -> {:error, :query_storage_unavailable}
      catch
        _kind, _reason -> {:error, :query_storage_unavailable}
      end
    else
      {:error, :query_row_batch_too_large}
    end
  end

  defp process_query_row_prefix(candidates, rows, complete?, state)
       when is_list(rows) and is_boolean(complete?) do
    returned_count = length(rows)
    requested_count = length(candidates)

    valid_count? =
      (complete? and returned_count == requested_count) or
        (not complete? and returned_count > 0 and returned_count < requested_count)

    if valid_count? do
      {read_candidates, remaining_candidates} = Enum.split(candidates, returned_count)

      with {:ok, pairs} <- query_row_pairs(read_candidates, rows, state),
           {:ok, state} <- process_hydrated(pairs, state) do
        if native_complete?(state),
          do: {:ok, state},
          else: process_query_row_candidates(remaining_candidates, state)
      end
    else
      {:error, :query_storage_inconsistent}
    end
  end

  defp process_query_row_prefix(_candidates, _rows, _complete?, _state),
    do: {:error, :query_storage_inconsistent}

  defp query_row_pairs(candidates, rows, state) do
    with {:ok, expected_scope_prefix} <- MandatoryScope.single_prefix(state.mandatory_scope) do
      candidates
      |> Enum.zip(rows)
      |> Enum.reduce_while({:ok, []}, fn
        {entry, nil}, {:ok, acc} ->
          {:cont, {:ok, [{entry, nil} | acc]}}

        {entry,
         %QueryRow{
           state_key: state_key,
           record: record,
           locator: %Locator{} = locator,
           expire_at_ms: expire_at_ms,
           scope_prefix: scope_prefix
         }},
        {:ok, acc}
        when is_map(record) and is_integer(expire_at_ms) and expire_at_ms >= 0 ->
          valid =
            state_key == entry.state_key and
              state_key == Keys.state_key(entry.id, state.physical_partition) and
              scope_prefix == expected_scope_prefix and
              Locator.hydration_ready?(locator) and locator.flow_id == entry.id and
              locator.version == entry.record_version and Map.get(record, :id) == entry.id and
              Map.get(record, :version) == entry.record_version and
              Map.get(record, :partition_key) == state.logical_partition and
              (expire_at_ms == 0 or expire_at_ms > state.now_ms)

          if valid,
            do: {:cont, {:ok, [{entry, record} | acc]}},
            else: {:halt, {:error, :query_storage_inconsistent}}

        _invalid, _acc ->
          {:halt, {:error, :query_storage_inconsistent}}
      end)
      |> case do
        {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
        {:error, _reason} = error -> error
      end
    else
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp hydrate_candidates([], state), do: {:ok, state}

  defp hydrate_candidates(candidates, state) do
    remaining_hydrations = state.budget.hydrated_records - state.usage.hydrated_records

    if remaining_hydrations <= 0 do
      {:error, :query_hydration_budget_exceeded}
    else
      {read_candidates, deferred_candidates} = Enum.split(candidates, remaining_hydrations)
      state_keys = Enum.map(read_candidates, & &1.state_key)
      result = read_records(state, state_keys)

      case result do
        {:ok, records} ->
          process_hydrated_prefix(read_candidates, deferred_candidates, records, true, state)

        {:ok, records, complete?} ->
          process_hydrated_prefix(
            read_candidates,
            deferred_candidates,
            records,
            complete?,
            state
          )

        {:error, :query_hydration_batch_too_large} when length(read_candidates) > 1 ->
          {left, right} = Enum.split(read_candidates, div(length(read_candidates), 2))

          with {:ok, state} <- hydrate_candidates(left, state) do
            remaining = right ++ deferred_candidates

            if native_complete?(state),
              do: {:ok, state},
              else: hydrate_candidates(remaining, state)
          end

        {:error, :query_hydration_batch_too_large} ->
          {:error, :query_memory_budget_exceeded}

        {:error, :query_storage_inconsistent} ->
          {:error, :query_storage_inconsistent}

        {:error, :query_deadline_exceeded} = error ->
          error

        {:error, _reason} ->
          {:error, :query_storage_unavailable}

        _invalid ->
          {:error, :query_storage_inconsistent}
      end
    end
  end

  defp process_hydrated_prefix(
         read_candidates,
         deferred_candidates,
         records,
         complete?,
         state
       )
       when is_list(records) and is_boolean(complete?) do
    returned_count = length(records)
    requested_count = length(read_candidates)

    valid_count? =
      (complete? and returned_count == requested_count) or
        (not complete? and returned_count > 0 and returned_count < requested_count)

    if valid_count? do
      {hydrated_candidates, unconsumed_candidates} =
        Enum.split(read_candidates, returned_count)

      usage = %{
        state.usage
        | hydrated_records: state.usage.hydrated_records + returned_count
      }

      with {:ok, state} <-
             process_hydrated(Enum.zip(hydrated_candidates, records), %{state | usage: usage}) do
        remaining = unconsumed_candidates ++ deferred_candidates
        if native_complete?(state), do: {:ok, state}, else: hydrate_candidates(remaining, state)
      end
    else
      {:error, :query_storage_inconsistent}
    end
  end

  defp process_hydrated_prefix(
         _read_candidates,
         _deferred_candidates,
         _records,
         _complete?,
         _state
       ),
       do: {:error, :query_storage_inconsistent}

  defp read_records(state, state_keys) do
    available_bytes = state.budget.executor_memory_bytes - estimated_memory(state, 0)
    max_value_bytes = MemoryBudget.encoded_record_input_bytes(available_bytes)

    if max_value_bytes > 0 do
      try do
        with {:ok, timeout_ms} <- remaining_timeout_ms(state),
             result <-
               call_record_read(
                 state.record_read,
                 state.path,
                 state_keys,
                 state.now_ms,
                 max_value_bytes,
                 timeout_ms
               ),
             :ok <- check_deadline(state) do
          result
        end
      rescue
        _error -> {:error, :query_storage_unavailable}
      catch
        _kind, _reason -> {:error, :query_storage_unavailable}
      end
    else
      {:error, :query_hydration_batch_too_large}
    end
  end

  defp call_record_read(reader, path, state_keys, now_ms, max_value_bytes, timeout_ms) do
    if is_function(reader, 5),
      do: reader.(path, state_keys, now_ms, max_value_bytes, timeout_ms),
      else: reader.(path, state_keys, now_ms, max_value_bytes)
  end

  defp remaining_timeout_ms(state) do
    with {:ok, now_us} <- call_clock(state.clock_us) do
      remaining_us = state.deadline_us - now_us

      if remaining_us > 0,
        do: {:ok, div(remaining_us + 999, 1_000)},
        else: {:error, :query_deadline_exceeded}
    end
  end

  defp process_hydrated(pairs, state) do
    hydrated_bytes = MemoryBudget.term_bytes(pairs) + length(pairs) * @hydrated_record_overhead
    state = %{state | live_hydrated_bytes: hydrated_bytes}
    memory = estimated_memory(state, 0)

    usage = %{
      state.usage
      | memory_high_water_bytes: max(state.usage.memory_high_water_bytes, memory)
    }

    state = %{state | usage: usage}

    if memory > state.budget.executor_memory_bytes do
      {:error, :query_memory_budget_exceeded}
    else
      pairs
      |> Enum.reduce_while({:ok, state}, fn {entry, record}, {:ok, state} ->
        with {:ok, state} <- account_deadline_work(state) do
          cond do
            native_complete?(state) ->
              {:halt, {:ok, state}}

            expired_projected_row_missing?(record, entry, state.now_ms) ->
              {:cont, {:ok, state}}

            not is_map(record) ->
              {:halt, {:error, :query_projection_changed}}

            not matching_record_identity?(record, entry, state) ->
              {:halt, {:error, :query_storage_inconsistent}}

            record_version(record) != entry.record_version ->
              {:halt, {:error, :query_projection_changed}}

            verify_execution_record_scope(record, state) != :ok ->
              {:halt, {:error, :query_storage_inconsistent}}

            not CompositeIndex.entry_key_matches_record_validated?(
              state.record_matcher,
              record,
              entry.storage_key
            ) ->
              {:halt, {:error, :query_storage_inconsistent}}

            true ->
              case process_current_record(record, entry, state) do
                {:ok, state} -> {:cont, {:ok, state}}
                {:error, _reason} = error -> {:halt, error}
              end
          end
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> release_hydrated()
    end
  end

  defp process_current_record(
         record,
         entry,
         %{request: %Request{return: :count}, plan: %Plan{residual_predicates: []}} = state
       ),
       do: retain_match(record, entry, state)

  defp process_current_record(record, entry, state) do
    with {:ok, logical_record} <- public_record(record),
         {:ok, matched, state} <-
           matches_all?(logical_record, state.prepared_residual_predicates, state) do
      if matched, do: retain_match(logical_record, entry, state), else: {:ok, state}
    end
  end

  defp release_hydrated({:ok, state}), do: {:ok, %{state | live_hydrated_bytes: 0}}
  defp release_hydrated({:error, _reason} = error), do: error

  defp matches_all?(record, predicates, state) do
    Enum.reduce_while(predicates, {:ok, true, state}, fn predicate, {:ok, true, state} ->
      usage = %{state.usage | residual_checks: state.usage.residual_checks + 1}
      state = %{state | usage: usage}

      with {:ok, state} <- account_deadline_work(state) do
        matched =
          try do
            ReferenceEvaluator.matches_prepared?(record, predicate)
          rescue
            _error -> :invalid
          catch
            _kind, _reason -> :invalid
          end

        case matched do
          true -> {:cont, {:ok, true, state}}
          false -> {:halt, {:ok, false, state}}
          :invalid -> {:halt, {:error, :query_storage_inconsistent}}
        end
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp select_record(record, index_entry, state) do
    with {:ok, sort_key} <- RecordOrder.sort_key(record, state.request.order_by) do
      if cursor_allows?(state.cursor_seek, sort_key) do
        case state.plan.order do
          :native -> project_and_select_native(record, index_entry, sort_key, state)
          _top_k_or_merge -> maybe_project_and_select_top_k(record, index_entry, sort_key, state)
        end
      else
        {:ok, state}
      end
    else
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp project_and_select_native(record, index_entry, sort_key, state) do
    with {:ok, entry, entry_bytes} <-
           selection_entry(record, index_entry, sort_key, state.request.projection) do
      select_native(entry, entry_bytes, state)
    end
  end

  defp maybe_project_and_select_top_k(record, index_entry, sort_key, state) do
    if top_k_candidate?(sort_key, index_entry.id, state) do
      with {:ok, entry, entry_bytes} <-
             selection_entry(record, index_entry, sort_key, state.request.projection) do
        select_top_k(entry, entry_bytes, state)
      end
    else
      {:ok, state}
    end
  end

  defp selection_entry(record, index_entry, sort_key, projection) do
    with {:ok, projected} <- RecordProjection.project_validated(record, :runs, projection) do
      entry = {sort_key, index_entry.id, projected, index_entry.storage_key}
      {:ok, entry, selection_size(entry)}
    end
  end

  defp top_k_candidate?(sort_key, id, state) do
    target = state.request.limit + 1

    if :gb_sets.size(state.selected) < target do
      true
    else
      {largest_sort_key, largest_id, _record, _storage_key} =
        :gb_sets.largest(state.selected)

      {sort_key, id} < {largest_sort_key, largest_id}
    end
  end

  defp retain_match(_record, _index_entry, %{request: %Request{return: :count}} = state) do
    if state.matched_count < @maximum_expire_at_ms,
      do: {:ok, %{state | matched_count: state.matched_count + 1}},
      else: {:error, :query_storage_inconsistent}
  end

  defp retain_match(record, index_entry, %{request: %Request{return: :record}} = state),
    do: select_record(record, index_entry, state)

  defp retain_match(_record, _index_entry, _state), do: {:error, :query_storage_inconsistent}

  defp select_native({sort_key, _id, _record, _storage_key} = entry, entry_bytes, state) do
    if is_binary(state.last_native_key) and sort_key < state.last_native_key do
      {:error, :query_storage_inconsistent}
    else
      selected = [entry | state.selected]
      selected_bytes = state.selected_bytes + entry_bytes

      state = %{
        state
        | selected: selected,
          selected_bytes: selected_bytes,
          last_native_key: sort_key
      }

      enforce_selection_memory(state)
    end
  end

  defp select_top_k(entry, entry_bytes, state) do
    selected = :gb_sets.add(entry, state.selected)
    selected_bytes = state.selected_bytes + entry_bytes
    target = state.request.limit + 1

    {selected, selected_bytes} =
      if :gb_sets.size(selected) > target do
        largest = :gb_sets.largest(selected)
        {:gb_sets.delete(largest, selected), selected_bytes - selection_size(largest)}
      else
        {selected, selected_bytes}
      end

    enforce_selection_memory(%{state | selected: selected, selected_bytes: selected_bytes})
  end

  defp enforce_selection_memory(state) do
    memory = estimated_memory(state, 0)

    usage = %{
      state.usage
      | memory_high_water_bytes: max(state.usage.memory_high_water_bytes, memory)
    }

    if memory <= state.budget.executor_memory_bytes,
      do: {:ok, %{state | usage: usage}},
      else: {:error, :query_memory_budget_exceeded}
  end

  defp retain_page(state, page) do
    state = %{state | live_page_bytes: page.memory_bytes}
    memory = estimated_memory(state, 0)

    usage = %{
      state.usage
      | memory_high_water_bytes: max(state.usage.memory_high_water_bytes, memory)
    }

    if memory <= state.budget.executor_memory_bytes,
      do: {:ok, %{state | usage: usage}},
      else: {:error, :query_memory_budget_exceeded}
  end

  defp release_page(state), do: %{state | live_page_bytes: 0}

  defp finalize(%{request: %Request{return: :count}} = state) do
    with :ok <- check_deadline(state),
         quality <- execution_quality(state.plan),
         {:ok, finished_us} <- call_clock(state.clock_us),
         wall_time_us <- max(finished_us - state.start_us, 0),
         :ok <- enforce_deadline(finished_us, state.deadline_us) do
      usage = %{state.usage | result_records: 1, wall_time_us: wall_time_us}

      {:ok,
       %CountResult{
         count: state.matched_count,
         usage: usage,
         quality: quality
       }}
    else
      {:error, _reason} = error -> error
    end
  end

  defp finalize(%{request: %Request{return: :record}} = state) do
    with :ok <- check_deadline(state),
         {:ok, selected} <- ordered_selection(state),
         page <- Enum.take(selected, state.request.limit),
         records <- Enum.map(page, &elem(&1, 2)),
         :ok <- enforce_result_budget(records, state.budget),
         has_more <- state.has_more or length(selected) > state.request.limit,
         {:ok, continuation, state} <- issue_continuation(page, has_more, state),
         quality <- execution_quality(state.plan),
         {:ok, finished_us} <- call_clock(state.clock_us),
         wall_time_us <- max(finished_us - state.start_us, 0),
         :ok <- enforce_deadline(finished_us, state.deadline_us) do
      usage = %{
        state.usage
        | result_records: length(records),
          wall_time_us: wall_time_us
      }

      {:ok,
       %ExecutionResult{
         records: records,
         has_more: has_more,
         continuation: continuation,
         usage: usage,
         quality: quality
       }}
    else
      {:error, _reason} = error -> error
    end
  end

  defp ordered_selection(%{plan: %{order: :native}, selected: selected}),
    do: {:ok, Enum.reverse(selected)}

  defp ordered_selection(%{selected: selected}), do: {:ok, :gb_sets.to_list(selected)}

  defp issue_continuation(_page, false, state), do: {:ok, nil, state}

  defp issue_continuation([], true, _state), do: {:error, :query_storage_inconsistent}

  defp issue_continuation(page, true, state) do
    {sort_key, _id, _record, storage_key} = List.last(page)
    native_storage_key = if state.plan.order == :native, do: storage_key, else: nil

    with {:ok, key} <- resolve_cursor_key(state.ctx, state.cursor_key),
         {:ok, token} <- issue_seek_cursor(state, key, sort_key, native_storage_key) do
      {:ok, token, %{state | cursor_key: key}}
    end
  end

  defp issue_seek_cursor(state, key, sort_key, native_storage_key) do
    continuation = encode_seek(sort_key, native_storage_key)
    opts = cursor_issue_options(state, key)

    case Cursor.issue(cursor_binding(state), continuation, opts) do
      {:error, :query_cursor_invalid} when is_binary(native_storage_key) ->
        Cursor.issue(cursor_binding(state), encode_seek(sort_key, nil), opts)

      result ->
        result
    end
  end

  defp cursor_issue_options(state, key) do
    opts = [key: key, now_ms: state.now_ms]

    case state.cursor_ttl_ms do
      nil -> opts
      ttl_ms -> Keyword.put(opts, :ttl_ms, ttl_ms)
    end
  end

  defp execution_quality(%Plan{path: :empty}) do
    %{
      exactness: "exact",
      freshness: "not_applicable",
      coverage: "complete",
      pagination: "none"
    }
  end

  defp execution_quality(%Plan{path: path}) when path in [:counter_lookup, :count_scan] do
    %{
      exactness: "projected_exact",
      freshness: "projection_watermark",
      coverage: "complete",
      pagination: "none"
    }
  end

  defp execution_quality(%Plan{}) do
    %{
      exactness: "projected_exact",
      freshness: "projection_watermark",
      coverage: "complete",
      pagination: "live_seek"
    }
  end

  defp enforce_result_budget(records, budget) do
    if length(records) <= budget.result_records,
      do: :ok,
      else: {:error, :query_result_budget_exceeded}
  end

  defp enforce_deadline(finished_us, deadline_us) do
    if finished_us < deadline_us,
      do: :ok,
      else: {:error, :query_deadline_exceeded}
  end

  defp matching_record_identity?(record, entry, %{query_row_execution: true} = state) do
    Map.get(record, :id) == entry.id and
      Map.get(record, :partition_key) == state.logical_partition
  end

  defp matching_record_identity?(record, entry, state) do
    Map.get(record, :id) == entry.id and
      Map.get(record, :partition_key) == state.physical_partition
  end

  defp public_record(record) do
    {:ok, FlowRecordProjection.public(record)}
  rescue
    _error -> {:error, :query_storage_inconsistent}
  catch
    _kind, _reason -> {:error, :query_storage_inconsistent}
  end

  defp prepare_record_matcher(%Plan{definition: nil}, _logical_partition), do: {:ok, nil}

  defp prepare_record_matcher(
         %Plan{definition: %IndexDefinition{} = definition, mandatory_scope: mandatory_scope},
         logical_partition
       ) do
    with {:ok, scope_prefix} <- MandatoryScope.single_prefix(mandatory_scope),
         {:ok, matcher} <-
           CompositeIndex.prepare_record_matcher_validated(
             definition,
             scope_prefix,
             logical_partition
           ) do
      {:ok, matcher}
    else
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp verify_record_scope(record, mandatory_scope) do
    case MandatoryScope.verify_record(mandatory_scope, record) do
      :ok -> :ok
      {:error, _reason} -> {:error, :query_storage_inconsistent}
    end
  rescue
    _error -> {:error, :query_storage_inconsistent}
  catch
    _kind, _reason -> {:error, :query_storage_inconsistent}
  end

  defp verify_execution_record_scope(_record, %{query_row_execution: true}), do: :ok

  defp verify_execution_record_scope(record, state),
    do: verify_record_scope(record, state.mandatory_scope)

  defp expired_projected_row_missing?(record, entry, now_ms) do
    is_nil(record) and projected_entry_expired?(entry, now_ms)
  end

  defp projected_entry_expired?(%{expire_at_ms: expire_at_ms}, now_ms),
    do: expire_at_ms > 0 and expire_at_ms <= now_ms

  defp record_version(record) do
    case Map.get(record, :version) do
      version when is_integer(version) and version >= 0 -> version
      _invalid -> :invalid
    end
  end

  defp native_complete?(%{plan: %{order: :native}} = state),
    do: selection_count(state) >= state.request.limit + 1

  defp native_complete?(_state), do: false

  defp selection_count(%{plan: %{order: :native}, selected: selected}), do: length(selected)
  defp selection_count(%{selected: selected}), do: :gb_sets.size(selected)

  defp initial_selection(:native), do: []
  defp initial_selection(_other), do: :gb_sets.empty()

  defp selection_size({sort_key, id, record, storage_key}),
    do: byte_size(sort_key) + byte_size(id) + byte_size(storage_key) + term_size(record) + 128

  defp estimated_memory(state, hydrated_bytes) do
    state.prepared_residual_bytes + state.seen_bytes + state.selected_bytes +
      state.live_page_bytes +
      state.live_hydrated_bytes + hydrated_bytes
  end

  defp term_size(term) do
    MemoryBudget.term_bytes(term)
  end

  defp check_deadline(state) do
    with {:ok, now_us} <- call_clock(state.clock_us) do
      if now_us < state.deadline_us,
        do: :ok,
        else: {:error, :query_deadline_exceeded}
    end
  end

  defp maybe_check_deadline(state, checks) do
    if rem(checks, @deadline_check_interval) == 0,
      do: check_deadline(state),
      else: :ok
  end

  defp account_deadline_work(state) do
    checks = state.checks_since_deadline + 1
    state = %{state | checks_since_deadline: checks}

    with :ok <- maybe_check_deadline(state, checks) do
      {:ok, reset_deadline_checks(state, checks)}
    end
  end

  defp reset_deadline_checks(state, checks) do
    if rem(checks, @deadline_check_interval) == 0,
      do: %{state | checks_since_deadline: 0},
      else: state
  end

  defp call_clock(clock_us) do
    case clock_us.() do
      value when is_integer(value) -> {:ok, value}
      _invalid -> {:error, :query_engine_failure}
    end
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    _kind, _reason -> {:error, :query_engine_failure}
  end

  defp deadline(start_us, budget, opts) do
    budget_deadline = start_us + budget.wall_time_ms * 1_000

    case Keyword.get(opts, :deadline_us) do
      nil -> {:ok, budget_deadline}
      value when is_integer(value) and value > start_us -> {:ok, min(value, budget_deadline)}
      _invalid -> {:error, :query_deadline_exceeded}
    end
  end

  defp initialize_cursor(
         ctx,
         request,
         %Plan{path: :reject} = plan,
         query_binding,
         now_ms,
         opts
       ) do
    with {:ok, configured_key} <- optional_cursor_key(opts),
         {:ok, claim} <- optional_cursor_claim(opts) do
      case {request.cursor, claim} do
        {nil, nil} ->
          {:ok, nil, configured_key}

        {{:literal, :keyword, token}, nil} ->
          with {:ok, key} <- resolve_cursor_key(ctx, configured_key),
               binding <-
                 cursor_request_binding(ctx, request, plan.query_fingerprint, query_binding),
               {:ok, _claim} <- Cursor.open(binding, token, key: key, now_ms: now_ms) do
            {:ok, nil, key}
          end

        {{:literal, :keyword, token}, %Cursor.Claim{} = claim} ->
          binding =
            cursor_request_binding(ctx, request, plan.query_fingerprint, query_binding)

          with {:ok, _continuation} <- Cursor.verify_request_claim(binding, token, claim) do
            {:ok, nil, configured_key}
          end

        _invalid ->
          {:error, :query_cursor_invalid}
      end
    end
  end

  defp initialize_cursor(ctx, request, plan, query_binding, now_ms, opts) do
    with {:ok, configured_key} <- optional_cursor_key(opts),
         {:ok, claim} <- optional_cursor_claim(opts) do
      case request.cursor do
        nil when is_nil(claim) ->
          {:ok, nil, configured_key}

        {:literal, :keyword, token} ->
          with {:ok, key} <- resolve_cursor_key(ctx, configured_key),
               binding <- cursor_binding(ctx, request, plan, query_binding),
               {:ok, encoded_seek} <- verify_cursor(binding, token, claim, key, now_ms),
               {:ok, seek} <- decode_seek(encoded_seek, plan) do
            {:ok, seek, key}
          end

        _invalid ->
          {:error, :query_cursor_invalid}
      end
    end
  end

  defp optional_cursor_claim(opts) do
    case Keyword.get(opts, :cursor_claim) do
      nil -> {:ok, nil}
      %Cursor.Claim{} = claim -> {:ok, claim}
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp verify_cursor(binding, token, nil, key, now_ms),
    do: Cursor.verify(binding, token, key: key, now_ms: now_ms)

  defp verify_cursor(binding, token, %Cursor.Claim{} = claim, _key, _now_ms),
    do: Cursor.verify_claim(binding, token, claim)

  defp optional_cursor_key(opts) do
    case Keyword.get(opts, :cursor_key) do
      nil -> {:ok, nil}
      key when is_binary(key) and byte_size(key) == 32 -> {:ok, key}
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp resolve_cursor_key(_ctx, key) when is_binary(key) and byte_size(key) == 32, do: {:ok, key}
  defp resolve_cursor_key(ctx, nil), do: CursorKeyStore.key(ctx)
  defp resolve_cursor_key(_ctx, _key), do: {:error, :query_cursor_invalid}

  defp cursor_binding(state) do
    cursor_binding(state.ctx, state.request, state.plan, state.query_binding)
  end

  defp cursor_binding(ctx, request, plan, query_binding) do
    ctx
    |> cursor_request_binding(request, plan.query_fingerprint, query_binding)
    |> Map.merge(%{
      index_id: plan.index_id,
      index_version: plan.index_version,
      index_build_id: plan.index_build_id
    })
  end

  defp cursor_request_binding(ctx, request, query_fingerprint, query_binding) do
    %{
      instance: ctx.name,
      scope: query_binding,
      query_fingerprint: query_fingerprint,
      query_digest: Planner.query_digest(request),
      order_by: request.order_by
    }
  end

  defp encode_seek(sort_key, native_storage_key) do
    TermCodec.encode({:ferric_flow_query_seek, 1, sort_key, native_storage_key})
  end

  defp decode_seek(encoded, plan) do
    with {:ok, {:ferric_flow_query_seek, 1, sort_key, native_storage_key}} <-
           TermCodec.decode(encoded),
         true <-
           is_binary(sort_key) and sort_key != "" and
             byte_size(sort_key) <= @maximum_sort_key_bytes,
         true <- is_nil(native_storage_key) or is_binary(native_storage_key),
         :ok <- validate_seek_storage_key(native_storage_key, plan) do
      {:ok, %{sort_key: sort_key, native_storage_key: native_storage_key}}
    else
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp validate_seek_storage_key(nil, _plan), do: :ok

  defp validate_seek_storage_key(storage_key, %Plan{order: :native, ranges: [range]})
       when is_binary(storage_key) do
    valid =
      byte_size(storage_key) <= @maximum_storage_key_bytes and
        String.starts_with?(storage_key, range.prefix) and
        (range.after_key == "" or storage_key > range.after_key) and
        (range.before_key == "" or storage_key < range.before_key)

    if valid, do: :ok, else: {:error, :query_cursor_invalid}
  end

  defp validate_seek_storage_key(_storage_key, _plan), do: {:error, :query_cursor_invalid}

  defp cursor_sort_key(%{sort_key: sort_key}, :native), do: sort_key
  defp cursor_sort_key(_seek, _order), do: nil

  defp native_storage_cursor(%{native_storage_key: storage_key}), do: storage_key
  defp native_storage_cursor(_seek), do: nil

  defp cursor_allows?(%{sort_key: cursor_key}, sort_key), do: sort_key > cursor_key
  defp cursor_allows?(_seek, _sort_key), do: true

  defp validate_options(
         %{data_dir: data_dir, shard_count: shard_count},
         shard_index,
         page_entries,
         page_bytes,
         range_read,
         record_read,
         query_row_read,
         clock_us,
         now_ms
       ) do
    valid =
      is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and shard_count > 0 and
        shard_index >= 0 and shard_index < shard_count and is_integer(page_entries) and
        page_entries > 0 and page_entries <= @maximum_page_entries and is_integer(page_bytes) and
        page_bytes > 0 and page_bytes <= @maximum_page_bytes and is_function(range_read, 5) and
        (is_function(record_read, 4) or is_function(record_read, 5)) and
        is_function(query_row_read, 4) and
        is_function(clock_us, 0) and is_integer(now_ms) and now_ms >= 0

    if valid, do: :ok, else: {:error, :query_engine_failure}
  end

  defp validate_options(
         _ctx,
         _shard_index,
         _page_entries,
         _page_bytes,
         _range_read,
         _record_read,
         _query_row_read,
         _clock_us,
         _now_ms
       ),
       do: {:error, :query_engine_failure}

  defp validate_route(ctx, shard_index, physical_partition) do
    if Router.shard_for(ctx, Keys.state_key("", physical_partition)) == shard_index,
      do: :ok,
      else: {:error, :query_storage_inconsistent}
  rescue
    _error -> {:error, :query_storage_inconsistent}
  catch
    _kind, _reason -> {:error, :query_storage_inconsistent}
  end

  defp validate_budget(%Budget{} = budget), do: Budget.new(Map.from_struct(budget))
  defp validate_budget(_budget), do: {:error, :invalid_query_budget}

  defp validate_scope_authority(
         %{query_mandatory_scope: %MandatoryScope{} = expected_scope} = ctx,
         %MandatoryScope{} = plan_scope
       ) do
    with true <- expected_scope == plan_scope,
         {:ok, snapshot} <- MetadataExtension.snapshot(ctx),
         :ok <- MandatoryScope.validate_against(plan_scope, snapshot) do
      :ok
    else
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp validate_scope_authority(_ctx, _plan_scope),
    do: {:error, :query_storage_inconsistent}

  defp validate_plan(%Plan{path: :empty} = plan, %Request{mode: :execute} = request, _scope) do
    valid =
      logically_empty_request?(request) and request_within_result_budget?(request, plan.budget) and
        plan.version == 1 and plan.index_id == nil and plan.index_version == nil and
        plan.index_build_id == nil and plan.definition == nil and plan.ranges == [] and
        plan.order == :native and plan.record_source == :none and
        plan.residual_predicates == [] and
        plan.recheck_predicates == request_predicates(request) and plan.fallback_reason == :none and
        plan.query_fingerprint == Planner.query_fingerprint(request)

    if valid, do: :ok, else: {:error, :query_storage_inconsistent}
  end

  defp validate_plan(
         %Plan{path: :reject, ranges: []} = plan,
         %Request{mode: :execute} = request,
         _scope
       ) do
    valid =
      plan.version == 1 and plan.index_id == nil and plan.index_version == nil and
        plan.index_build_id == nil and plan.definition == nil and plan.order == :none and
        plan.record_source == :none and
        plan.recheck_predicates == request_predicates(request) and
        plan.query_fingerprint == Planner.query_fingerprint(request)

    if valid, do: :ok, else: {:error, :query_storage_inconsistent}
  end

  defp validate_plan(%Plan{} = plan, request, logical_partition) do
    with true <- request.mode == :execute,
         true <- plan.version == 1,
         :ok <- MandatoryScope.validate(plan.mandatory_scope),
         true <- plan.query_fingerprint == Planner.query_fingerprint(request),
         true <- request_within_result_budget?(request, plan.budget),
         true <- plan.recheck_predicates == request_predicates(request),
         true <- plan.ranges != [] and length(plan.ranges) <= plan.budget.range_seeks,
         true <- valid_execution_order?(request, plan),
         true <- not (plan.order == :native and length(plan.ranges) != 1),
         %IndexDefinition{id: id, version: version} = definition <- plan.definition,
         :ok <- IndexDefinition.validate(definition),
         true <- plan.index_id == id and plan.index_version == version,
         true <-
           is_binary(plan.index_build_id) and plan.index_build_id != "" and
             byte_size(plan.index_build_id) <= 128,
         {:ok, physical} <-
           Planner.physical_contract(
             request,
             definition,
             plan.mandatory_scope,
             plan.budget.range_seeks
           ),
         true <- plan.path == physical.path,
         true <- plan.ranges == physical.ranges,
         true <- plan.deduplicate == physical.deduplicate,
         true <- plan.order == physical.order,
         true <- plan.residual_predicates == physical.residual_predicates,
         true <-
           plan.record_source ==
             ExecutionSource.classify(
               request,
               physical.path,
               definition,
               physical.residual_predicates
             ),
         true <- plan.constraint_shapes == physical.constraint_shapes,
         {:ok, scope_prefix} <- MandatoryScope.single_prefix(plan.mandatory_scope),
         {:ok, physical_prefix} <-
           CompositeIndex.encode_prefix(definition, scope_prefix, [logical_partition]),
         true <- Enum.all?(plan.ranges, &valid_plan_range?(&1, plan, physical_prefix)) do
      :ok
    else
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp valid_plan_range?(
         %CompositeRange{
           index_id: index_id,
           index_version: index_version,
           prefix: prefix,
           after_key: after_key,
           before_key: before_key
         } = range,
         plan,
         physical_prefix
       )
       when is_binary(index_id) and is_integer(index_version) and index_version > 0 and
              is_binary(prefix) and byte_size(prefix) > 0 and
              byte_size(prefix) <= @maximum_storage_key_bytes and is_binary(after_key) and
              byte_size(after_key) <= @maximum_storage_key_bytes and is_binary(before_key) and
              byte_size(before_key) <= @maximum_storage_key_bytes do
    range.index_id == plan.index_id and range.index_version == plan.index_version and
      String.starts_with?(range.prefix, physical_prefix) and
      valid_physical_bound?(range.after_key, physical_prefix) and
      valid_physical_bound?(range.before_key, physical_prefix) and
      (range.before_key == "" or range.after_key == "" or range.after_key < range.before_key)
  end

  defp valid_plan_range?(_range, _plan, _physical_prefix), do: false

  defp valid_physical_bound?("", _prefix), do: true
  defp valid_physical_bound?(bound, prefix), do: String.starts_with?(bound, prefix)

  defp initial_usage(range_seeks) do
    %{
      range_seeks: range_seeks,
      range_pages: 0,
      scanned_entries: 0,
      scanned_bytes: 0,
      hydrated_records: 0,
      residual_checks: 0,
      duplicate_entries: 0,
      result_records: 0,
      response_bytes: 0,
      memory_high_water_bytes: 0,
      wall_time_us: 0
    }
  end

  defp rejection_error(:range_budget_exceeded), do: :query_range_budget_exceeded
  defp rejection_error(:scan_budget_exceeded), do: :query_scan_budget_exceeded
  defp rejection_error(:scan_byte_budget_exceeded), do: :query_scan_byte_budget_exceeded
  defp rejection_error(:hydration_budget_exceeded), do: :query_hydration_budget_exceeded
  defp rejection_error(:result_budget_exceeded), do: :query_result_budget_exceeded
  defp rejection_error(:memory_budget_exceeded), do: :query_memory_budget_exceeded
  defp rejection_error(_reason), do: :query_no_bounded_plan

  defp request_predicates(%Request{predicate: {:and, predicates}}), do: predicates

  defp request_within_result_budget?(%Request{return: :count}, budget),
    do: budget.result_records >= 1

  defp request_within_result_budget?(%Request{return: :record, limit: limit}, budget)
       when is_integer(limit),
       do: limit <= budget.result_records

  defp request_within_result_budget?(_request, _budget), do: false

  defp valid_execution_order?(%Request{return: :count}, %Plan{path: path, order: :none})
       when path in [:counter_lookup, :count_scan],
       do: true

  defp valid_execution_order?(%Request{return: :record}, %Plan{order: order}),
    do: order in [:native, :bounded_top_k]

  defp valid_execution_order?(_request, _plan), do: false

  defp logically_empty_request?(request) do
    Enum.any?(request_predicates(request), fn
      {:time_window, _field, lower, upper} -> lower == upper
      _predicate -> false
    end)
  end

  defp default_range_read(path, range, cursor, max_entries, max_bytes),
    do: CompositeRangeReader.read(path, range, cursor, max_entries, max_bytes)

  defp default_record_read(
         ctx,
         shard_index,
         path,
         state_keys,
         now_ms,
         max_value_bytes,
         timeout_ms
       ) do
    QueryRecordStore.read_many(
      ctx,
      shard_index,
      path,
      state_keys,
      now_ms,
      max_value_bytes,
      timeout_ms: timeout_ms
    )
  end
end
