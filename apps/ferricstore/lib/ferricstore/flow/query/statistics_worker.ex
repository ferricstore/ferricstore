defmodule Ferricstore.Flow.Query.StatisticsWorker do
  @moduledoc false

  use GenServer

  alias Ferricstore.Flow.{Keys, LMDB, StorageScope}

  alias Ferricstore.Flow.Query.{
    CompositeRange,
    CompositeRangeReader,
    IndexDefinition,
    Limits,
    TupleCodec
  }

  alias Ferricstore.Store.Router

  alias Ferricstore.Flow.Query.{IndexStatistics, StatisticsStore}

  @probe_items 257
  @probe_bytes 1 * 1_024 * 1_024
  @max_pending 1_024
  @max_async_batch 32
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @min_i64 -0x8000_0000_0000_0000
  @max_i64 0x7FFF_FFFF_FFFF_FFFF
  @max_probe_value_bytes 1_024
  @async_signal_key :"$async_probe_signal"
  @async_ticket_key :"$async_probe_ticket"
  @async_slot_key :"$async_probe_slot"
  @async_dedupe_key :"$async_probe_dedupe"
  @default_max_seen_entries 2_048
  @maximum_seen_entries 65_536
  @seen_ttl_ms 60_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    name = Keyword.get(opts, :name, server_name(ctx))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec server_name(map() | atom()) :: atom()
  def server_name(%{name: name}), do: server_name(name)
  def server_name(:default), do: __MODULE__
  def server_name(name) when is_atom(name), do: :"#{name}.Flow.Query.StatisticsWorker"

  @spec probe(GenServer.server(), non_neg_integer(), IndexDefinition.t(), binary(), [term()]) ::
          :ok | {:error, :invalid_query_statistics_probe | :query_statistics_probe_queue_full}
  def probe(server, shard_index, %IndexDefinition{} = definition, scope, equality_values) do
    GenServer.call(server, {:probe, shard_index, definition, scope, equality_values})
  end

  def probe(_server, _shard_index, _definition, _scope, _equality_values),
    do: {:error, :invalid_query_statistics_probe}

  @spec probe_async(
          map(),
          GenServer.server(),
          non_neg_integer(),
          map()
        ) ::
          :ok | {:error, :invalid_query_statistics_probe | :query_statistics_probe_queue_full}
  def probe_async(ctx, server, shard_index, prepared_probe) do
    with {:ok, spec} <- prepare_async_probe(ctx, shard_index, prepared_probe),
         :ok <- admit_async_probe(ctx, server, spec),
         do: :ok
  end

  @spec probe_many_async(map(), GenServer.server(), non_neg_integer(), [map()]) ::
          :ok | {:error, :invalid_query_statistics_probe | :query_statistics_probe_queue_full}
  def probe_many_async(ctx, server, shard_index, prepared_probes)
      when is_list(prepared_probes) and length(prepared_probes) <= @max_async_batch do
    with {:ok, payloads} <- prepare_async_admissions(shard_index, prepared_probes) do
      Enum.reduce_while(payloads, :ok, fn payload, :ok ->
        case admit_async_probe(ctx, server, payload) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  def probe_many_async(_ctx, _server, _shard_index, _prepared_probes),
    do: {:error, :invalid_query_statistics_probe}

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    store = Keyword.get(opts, :statistics_store, StatisticsStore.server_name(ctx))

    read_fun =
      Keyword.get(opts, :read_fun, fn path, range, max_items, max_bytes ->
        CompositeRangeReader.read(path, range, nil, max_items, max_bytes)
      end)

    interval = Keyword.get(opts, :probe_interval_ms, 10)
    max_seen_entries = Keyword.get(opts, :max_seen_entries, @default_max_seen_entries)

    if valid_context?(ctx) and is_function(read_fun, 4) and is_integer(interval) and interval >= 0 and
         is_integer(max_seen_entries) and max_seen_entries > 0 and
         max_seen_entries <= @maximum_seen_entries do
      case create_admission_table(ctx) do
        {:ok, admission_table} ->
          {:ok,
           %{
             ctx: ctx,
             store: store,
             read_fun: read_fun,
             interval: interval,
             queue: :queue.new(),
             pending: MapSet.new(),
             seen: %{},
             seen_order: :queue.new(),
             max_seen_entries: max_seen_entries,
             dropped_probes: 0,
             scheduled?: false,
             admission_table: admission_table
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:stop, :invalid_query_statistics_worker_options}
    end
  end

  @impl true
  def handle_call(
        {:probe, shard_index, %IndexDefinition{} = definition, scope, equality_values},
        _from,
        state
      ) do
    case enqueue_probe(state, shard_index, definition, scope, equality_values) do
      {:ok, next_state} -> {:reply, :ok, next_state}
      {:error, reason, next_state} -> {:reply, {:error, reason}, next_state}
    end
  end

  @impl true
  def handle_info(:drain_async_probes, state) do
    :ets.delete(state.admission_table, @async_signal_key)

    next_state =
      state.admission_table
      |> async_probe_entries()
      |> Enum.reduce(state, &enqueue_admitted_probe(state.admission_table, &1, &2))

    {:noreply, next_state}
  end

  @impl true
  def handle_info(:run_probe, state) do
    case :queue.out(state.queue) do
      {{:value, spec}, queue} ->
        state = %{state | queue: queue, scheduled?: false}
        run_probe(state, spec)
        now = System.monotonic_time(:millisecond)

        state =
          state
          |> Map.update!(:pending, &MapSet.delete(&1, spec.key))
          |> remember_seen(spec.key, now)

        {:noreply, schedule(state)}

      {:empty, _queue} ->
        {:noreply, %{state | scheduled?: false}}
    end
  end

  defp enqueue_probe(state, shard_index, definition, scope, equality_values) do
    with :ok <- validate_probe(state.ctx, shard_index, scope, equality_values, definition),
         {:ok, range} <- CompositeRange.prefix(definition, equality_values) do
      spec = probe_spec(state.ctx, shard_index, definition, scope, equality_values, range)
      enqueue_spec(state, spec)
    else
      _invalid -> {:error, :invalid_query_statistics_probe, state}
    end
  end

  defp enqueue_spec(state, spec) do
    now = System.monotonic_time(:millisecond)
    state = prune_seen(state, now)

    cond do
      MapSet.member?(state.pending, spec.key) or recently_seen?(state.seen, spec.key, now) ->
        {:ok, state}

      :queue.len(state.queue) >= @max_pending ->
        {:error, :query_statistics_probe_queue_full, state}

      true ->
        state = %{
          state
          | queue: :queue.in(spec, state.queue),
            pending: MapSet.put(state.pending, spec.key)
        }

        {:ok, schedule(state)}
    end
  end

  defp run_probe(state, spec) do
    case state.read_fun.(spec.path, spec.range, @probe_items, @probe_bytes) do
      {:ok,
       %{
         entries: entries,
         cursor: nil,
         exhausted: true,
         scanned_entries: count,
         scanned_bytes: bytes
       }}
      when is_list(entries) and is_integer(count) and count >= 0 and length(entries) == count and
             count <= @probe_items and is_integer(bytes) and bytes >= 0 and
             bytes <= @probe_bytes and
             ((count == 0 and bytes == 0) or (count > 0 and bytes >= count)) ->
        publish_exact_probe(state, spec, count, bytes)

      _partial_or_failed ->
        :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp publish_exact_probe(state, spec, count, bytes) do
    observed_at_ms = System.system_time(:millisecond)

    existing =
      case StatisticsStore.lookup_digest(
             state.ctx,
             spec.index_id,
             spec.index_version,
             spec.scope_digest
           ) do
        {:ok, stat} -> stat
        :not_found -> nil
      end

    {prefix_counts, prefix_observed_at_ms} =
      merge_prefix_observation(existing, spec.prefix_digest, count, observed_at_ms)

    average_entry_bytes =
      cond do
        count > 0 -> max(div(bytes, count), 1)
        match?(%IndexStatistics{}, existing) -> existing.average_entry_bytes
        true -> 128
      end

    sample_observed_at_ms =
      existing
      |> stat_map(:sample_observed_at_ms)
      |> maybe_observe_entry_size(count, observed_at_ms)

    stat =
      IndexStatistics.new!(%{
        index_id: spec.index_id,
        index_version: spec.index_version,
        scope_digest: spec.scope_digest,
        collected_at_ms: observed_at_ms,
        source_watermark: stat_value(existing, :source_watermark, 0),
        total_entries: max(stat_value(existing, :total_entries, 0), count),
        distinct_runs: max(stat_value(existing, :distinct_runs, 0), count),
        prefix_counts: prefix_counts,
        prefix_observed_at_ms: prefix_observed_at_ms,
        histograms: stat_map(existing, :histograms),
        null_counts: stat_map(existing, :null_counts),
        missing_counts: stat_map(existing, :missing_counts),
        sample_observed_at_ms: sample_observed_at_ms,
        average_entry_bytes: average_entry_bytes,
        average_row_bytes: stat_value(existing, :average_row_bytes, 512),
        sample_rate_ppm: 1_000_000,
        confidence: :high
      })

    StatisticsStore.put(state.store, stat)
  end

  defp maybe_observe_entry_size(observed, count, observed_at_ms) when count > 0,
    do: Map.put(observed, :average_entry_bytes, observed_at_ms)

  defp maybe_observe_entry_size(observed, _count, _observed_at_ms), do: observed

  defp merge_prefix_observation(existing, digest, count, observed_at_ms) do
    counts = stat_map(existing, :prefix_counts)
    observed = stat_map(existing, :prefix_observed_at_ms)

    {counts, observed} =
      if not Map.has_key?(counts, digest) and
           map_size(counts) >= IndexStatistics.max_prefix_counts() do
        {victim, _seen_at} = Enum.min_by(observed, fn {key, seen_at} -> {seen_at, key} end)
        {Map.delete(counts, victim), Map.delete(observed, victim)}
      else
        {counts, observed}
      end

    {Map.put(counts, digest, count), Map.put(observed, digest, observed_at_ms)}
  end

  defp probe_spec(ctx, shard_index, definition, scope, equality_values, range) do
    scope_digest = IndexStatistics.scope_digest(scope)
    prefix_digest = IndexStatistics.prefix_digest(equality_values)

    probe_spec(
      ctx,
      shard_index,
      definition.id,
      definition.version,
      scope_digest,
      prefix_digest,
      range
    )
  end

  defp probe_spec(
         ctx,
         shard_index,
         index_id,
         index_version,
         scope_digest,
         prefix_digest,
         range
       ) do
    %{
      key: {shard_index, index_id, index_version, scope_digest, prefix_digest},
      shard_index: shard_index,
      path: lmdb_path(ctx, shard_index),
      range: range,
      index_id: index_id,
      index_version: index_version,
      scope_digest: scope_digest,
      prefix_digest: prefix_digest
    }
  end

  defp prepare_async_probe(
         ctx,
         shard_index,
         %{
           definition: %IndexDefinition{} = definition,
           equality_values: equality_values,
           range: %CompositeRange{} = range,
           scope_prefix: scope_prefix,
           physical_partition_key: physical_partition_key,
           statistics_key: statistics_key,
           scope_digest: scope_digest,
           prefix_digest: prefix_digest
         }
       ) do
    with :ok <-
           validate_prepared_probe(
             ctx,
             shard_index,
             definition,
             equality_values,
             range,
             scope_prefix,
             physical_partition_key,
             statistics_key,
             scope_digest,
             prefix_digest
           ) do
      {:ok,
       probe_spec(
         ctx,
         shard_index,
         range.index_id,
         range.index_version,
         scope_digest,
         prefix_digest,
         range
       )}
    end
  end

  defp prepare_async_probe(_ctx, _shard_index, _prepared_probe),
    do: {:error, :invalid_query_statistics_probe}

  defp prepare_async_admissions(shard_index, prepared_probes) do
    Enum.reduce_while(prepared_probes, {:ok, []}, fn probe, {:ok, acc} ->
      case prepare_async_admission(shard_index, probe) do
        {:ok, payload} -> {:cont, {:ok, [payload | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_async_admission(
         shard_index,
         %{
           range: %CompositeRange{index_id: index_id, index_version: index_version},
           scope_digest: scope_digest,
           prefix_digest: prefix_digest
         } = prepared_probe
       )
       when is_integer(shard_index) and shard_index >= 0 and is_binary(index_id) and
              index_id != "" and byte_size(index_id) <= 64 and is_integer(index_version) and
              index_version > 0 and index_version <= @max_u64 and is_binary(scope_digest) and
              byte_size(scope_digest) == 32 and is_binary(prefix_digest) and
              byte_size(prefix_digest) == 32 do
    key = {shard_index, index_id, index_version, scope_digest, prefix_digest}

    {:ok,
     %{
       key: key,
       shard_index: shard_index,
       prepared_probe: prepared_probe
     }}
  end

  defp prepare_async_admission(_shard_index, _prepared_probe),
    do: {:error, :invalid_query_statistics_probe}

  defp validate_prepared_probe(
         ctx,
         shard_index,
         definition,
         equality_values,
         range,
         scope_prefix,
         physical_partition_key,
         statistics_key,
         scope_digest,
         prefix_digest
       ) do
    with [logical_partition | _rest] <- equality_values,
         :ok <-
           validate_prepared_scope(
             ctx,
             shard_index,
             definition,
             logical_partition,
             equality_values,
             scope_prefix,
             physical_partition_key,
             statistics_key
           ),
         {:ok, expected_range} <-
           CompositeRange.prefix(definition, scope_prefix, equality_values),
         true <- expected_range == range,
         true <- scope_digest == IndexStatistics.scope_digest(statistics_key),
         true <- prefix_digest == IndexStatistics.prefix_digest(equality_values) do
      :ok
    else
      _invalid -> {:error, :invalid_query_statistics_probe}
    end
  end

  defp valid_probe_range?(%CompositeRange{
         index_id: index_id,
         index_version: index_version,
         prefix: prefix,
         after_key: after_key,
         before_key: before_key
       }) do
    is_binary(index_id) and index_id != "" and byte_size(index_id) <= 64 and
      is_integer(index_version) and index_version > 0 and index_version <= @max_u64 and
      is_binary(prefix) and prefix != "" and byte_size(prefix) <= 511 and
      String.starts_with?(prefix, IndexDefinition.global_storage_prefix()) and
      valid_range_bound?(after_key, prefix) and valid_range_bound?(before_key, prefix) and
      valid_range_order?(after_key, before_key)
  end

  defp valid_probe_range?(_range), do: false

  defp valid_range_bound?("", _prefix), do: true

  defp valid_range_bound?(bound, prefix),
    do: is_binary(bound) and byte_size(bound) <= 511 and String.starts_with?(bound, prefix)

  defp valid_range_order?("", ""), do: true
  defp valid_range_order?(after_key, before_key), do: after_key < before_key

  defp valid_digest?(digest), do: is_binary(digest) and byte_size(digest) == 32

  defp valid_shard?(%{shard_count: shard_count}, shard_index),
    do: is_integer(shard_index) and shard_index >= 0 and shard_index < shard_count

  defp validate_probe(
         %{shard_count: shard_count} = ctx,
         shard_index,
         scope,
         equality_values,
         %IndexDefinition{fields: fields} = definition
       )
       when is_integer(shard_count) and shard_count > 0 and is_list(fields) do
    valid =
      IndexDefinition.validate(definition) == :ok and is_integer(shard_index) and
        shard_index >= 0 and shard_index < shard_count and
        Limits.valid_partition_key?(scope) and is_list(equality_values) and equality_values != [] and
        length(equality_values) <= length(fields) and hd(equality_values) == scope and
        Enum.all?(equality_values, &valid_probe_value?/1) and
        routed_to_shard?(ctx, scope, shard_index)

    if valid, do: :ok, else: {:error, :invalid_query_statistics_probe}
  end

  defp validate_probe(_ctx, _shard_index, _scope, _equality_values, _definition),
    do: {:error, :invalid_query_statistics_probe}

  defp validate_prepared_scope(
         %{shard_count: shard_count} = ctx,
         shard_index,
         %IndexDefinition{fields: fields} = definition,
         logical_partition,
         equality_values,
         scope_prefix,
         physical_partition_key,
         statistics_key
       )
       when is_integer(shard_count) and shard_count > 0 and is_list(fields) do
    with true <- IndexDefinition.validate(definition) == :ok,
         true <- is_integer(shard_index) and shard_index >= 0 and shard_index < shard_count,
         true <- Limits.valid_partition_key?(logical_partition),
         true <- valid_equality_values?(equality_values, logical_partition, fields),
         true <- valid_scope_prefix?(definition, scope_prefix),
         {:ok, expected_partition} <-
           StorageScope.physical_partition_key(logical_partition, scope_prefix),
         true <- expected_partition == physical_partition_key,
         true <- Limits.valid_partition_key?(physical_partition_key),
         true <- Limits.valid_partition_key?(statistics_key),
         true <- routed_to_shard?(ctx, physical_partition_key, shard_index) do
      :ok
    else
      _invalid -> {:error, :invalid_query_statistics_probe}
    end
  end

  defp validate_prepared_scope(
         _ctx,
         _shard_index,
         _definition,
         _logical_partition,
         _equality_values,
         _scope_prefix,
         _physical_partition_key,
         _statistics_key
       ),
       do: {:error, :invalid_query_statistics_probe}

  defp valid_equality_values?(equality_values, logical_partition, fields) do
    is_list(equality_values) and equality_values != [] and
      length(equality_values) <= length(fields) and hd(equality_values) == logical_partition and
      Enum.all?(equality_values, &valid_probe_value?/1)
  end

  defp valid_scope_prefix?(%IndexDefinition{scope_bytes: 0}, nil), do: true

  defp valid_scope_prefix?(%IndexDefinition{scope_bytes: scope_bytes}, scope_prefix)
       when scope_bytes > 0 and is_binary(scope_prefix),
       do: byte_size(scope_prefix) == scope_bytes

  defp valid_scope_prefix?(%IndexDefinition{}, _scope_prefix), do: false

  defp valid_probe_value?(value) when is_binary(value),
    do: value != "" and byte_size(value) <= @max_probe_value_bytes

  defp valid_probe_value?(value) when is_integer(value),
    do: value >= @min_i64 and value <= @max_i64

  defp valid_probe_value?(value)
       when is_float(value) or is_boolean(value) or is_nil(value) or
              value == {:ferric_query, :missing},
       do: match?({:ok, _encoded}, TupleCodec.encode_component_safe(value, :asc))

  defp valid_probe_value?(_value), do: false

  defp admit_async_probe(ctx, server, payload) do
    with pid when is_pid(pid) <- GenServer.whereis(server),
         table <- admission_table_name(ctx),
         ^pid <- :ets.info(table, :owner) do
      claim = {@async_dedupe_key, payload.key}

      if :ets.insert_new(table, {claim, true}) do
        admit_unique_async_probe(table, pid, claim, payload)
      else
        :ok
      end
    else
      nil -> {:error, :invalid_query_statistics_probe}
      _unavailable -> {:error, :invalid_query_statistics_probe}
    end
  rescue
    ArgumentError -> {:error, :invalid_query_statistics_probe}
  end

  defp admit_unique_async_probe(table, pid, claim, payload) do
    ticket = :ets.update_counter(table, @async_ticket_key, {2, 1})
    slot = Integer.mod(ticket, @max_pending)

    if :ets.insert_new(table, {{@async_slot_key, slot}, ticket, payload}) do
      signal_async_probes(table, pid)
      :ok
    else
      :ets.delete(table, claim)
      {:error, :query_statistics_probe_queue_full}
    end
  end

  defp create_admission_table(ctx) do
    table =
      :ets.new(admission_table_name(ctx), [
        :named_table,
        :public,
        :set,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    true = :ets.insert(table, {@async_ticket_key, 0})
    {:ok, table}
  rescue
    ArgumentError -> {:error, :invalid_query_statistics_worker_options}
  end

  defp admission_table_name(%{name: name}) when is_atom(name),
    do: :"#{name}.Flow.Query.StatisticsAdmission"

  defp signal_async_probes(table, pid) do
    if :ets.insert_new(table, {@async_signal_key, true}), do: send(pid, :drain_async_probes)
  end

  defp async_probe_entries(table) do
    table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{@async_slot_key, slot}, ticket, payload} -> [{ticket, slot, payload}]
      _metadata -> []
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp enqueue_admitted_probe(
         table,
         {ticket, slot, admitted},
         state
       ) do
    case :ets.take(table, {@async_slot_key, slot}) do
      [{{@async_slot_key, ^slot}, ^ticket, _payload}] ->
        release_async_probe_claim(table, admitted)

        case admitted_probe_spec(state, admitted) do
          {:ok, spec} ->
            case enqueue_spec(state, spec) do
              {:ok, next_state} ->
                next_state

              {:error, :query_statistics_probe_queue_full, next_state} ->
                Map.update!(next_state, :dropped_probes, &(&1 + 1))
            end

          {:error, :invalid_query_statistics_probe} ->
            state
        end

      _missing_or_replaced ->
        state
    end
  end

  defp release_async_probe_claim(table, %{key: key}),
    do: :ets.delete(table, {@async_dedupe_key, key})

  defp release_async_probe_claim(_table, _spec), do: true

  defp admitted_probe_spec(state, %{
         prepared_probe: prepared_probe,
         shard_index: shard_index,
         key: key
       }) do
    with {:ok, %{key: ^key} = spec} <-
           prepare_async_probe(state.ctx, shard_index, prepared_probe),
         true <- valid_probe_spec?(state, spec) do
      {:ok, spec}
    else
      _invalid -> {:error, :invalid_query_statistics_probe}
    end
  end

  defp admitted_probe_spec(state, %{key: _key} = spec) do
    if valid_probe_spec?(state, spec),
      do: {:ok, spec},
      else: {:error, :invalid_query_statistics_probe}
  end

  defp admitted_probe_spec(_state, _admitted), do: {:error, :invalid_query_statistics_probe}

  defp valid_probe_spec?(
         state,
         %{
           key: {shard_index, index_id, index_version, scope_digest, prefix_digest},
           shard_index: shard_index,
           path: path,
           range: %CompositeRange{index_id: index_id, index_version: index_version} = range,
           index_id: index_id,
           index_version: index_version,
           scope_digest: scope_digest,
           prefix_digest: prefix_digest
         }
       ) do
    valid_shard?(state.ctx, shard_index) and path == lmdb_path(state.ctx, shard_index) and
      valid_probe_range?(range) and valid_digest?(scope_digest) and valid_digest?(prefix_digest)
  end

  defp valid_probe_spec?(_state, _spec), do: false

  defp schedule(%{scheduled?: true} = state), do: state

  defp schedule(state) do
    if :queue.is_empty(state.queue) do
      state
    else
      Process.send_after(self(), :run_probe, state.interval)
      %{state | scheduled?: true}
    end
  end

  defp prune_seen(state, now) do
    trim_seen(state, now, false)
  end

  defp remember_seen(state, key, now) do
    state = %{
      state
      | seen: Map.put(state.seen, key, now),
        seen_order: :queue.in({now, key}, state.seen_order)
    }

    trim_seen(state, now, true)
  end

  defp trim_seen(state, now, enforce_limit?) do
    case :queue.out(state.seen_order) do
      {{:value, {seen_at, key}}, queue} ->
        stale_queue_entry? = Map.get(state.seen, key) != seen_at
        expired? = now - seen_at > @seen_ttl_ms
        over_limit? = enforce_limit? and map_size(state.seen) > state.max_seen_entries

        if stale_queue_entry? or expired? or over_limit? do
          seen =
            if Map.get(state.seen, key) == seen_at,
              do: Map.delete(state.seen, key),
              else: state.seen

          trim_seen(%{state | seen: seen, seen_order: queue}, now, enforce_limit?)
        else
          state
        end

      {:empty, _queue} ->
        state
    end
  end

  defp recently_seen?(seen, key, now) do
    case Map.get(seen, key) do
      seen_at when is_integer(seen_at) -> now - seen_at <= @seen_ttl_ms
      _missing -> false
    end
  end

  defp stat_map(%IndexStatistics{} = stat, field), do: Map.fetch!(stat, field)
  defp stat_map(_stat, _field), do: %{}
  defp stat_value(%IndexStatistics{} = stat, field, _default), do: Map.fetch!(stat, field)
  defp stat_value(_stat, _field, default), do: default

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end

  defp routed_to_shard?(ctx, physical_partition_key, shard_index) do
    Router.shard_for(ctx, Keys.state_key("", physical_partition_key)) == shard_index
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  defp valid_context?(%{
         name: name,
         data_dir: data_dir,
         shard_count: shard_count,
         slot_map: slot_map
       }) do
    is_atom(name) and is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and
      shard_count > 0 and is_tuple(slot_map) and tuple_size(slot_map) == 1_024 and
      Enum.all?(Tuple.to_list(slot_map), &(&1 in 0..(shard_count - 1)))
  end

  defp valid_context?(_ctx), do: false
end
