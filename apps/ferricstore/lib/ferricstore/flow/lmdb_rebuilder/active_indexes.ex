defmodule Ferricstore.Flow.LMDBRebuilder.ActiveIndexes do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.FifoLane
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Store.Shard.ZSetIndex

  @active_index_rebuild_batch_size 4096
  @startup_active_rebuild_slots_key {__MODULE__, :startup_active_rebuild_slots}

  def init_startup_active_rebuild_limiter do
    case :persistent_term.get(@startup_active_rebuild_slots_key, nil) do
      ref when is_reference(ref) ->
        :ok

      _missing_or_invalid ->
        ref = :atomics.new(1, signed: false)
        :persistent_term.put(@startup_active_rebuild_slots_key, ref)
    end

    :ok
  end

  def rebuild_score_indexes(zset_score_index, zset_score_lookup, record) do
    do_rebuild_score_indexes(
      zset_score_index,
      zset_score_lookup,
      LMDB.active_projection_entries(record)
    )
  end

  def rebuild_flow_indexes(nil, _flow_lookup, _record), do: :ok
  def rebuild_flow_indexes(_flow_index, nil, _record), do: :ok

  def rebuild_flow_indexes(flow_index, flow_lookup, record) do
    entries = active_flow_index_entries(record) ++ hot_query_index_entries(record)

    case NativeFlowIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> NativeFlowIndex.put_entries(native, entries)
    end
  end

  def rebuild_flow_indexes_from_lmdb(
        lmdb_path,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup
      ) do
    if not LMDB.env_present?(lmdb_path) do
      {:ok, 0}
    else
      do_rebuild_flow_indexes_from_lmdb(
        lmdb_path,
        zset_score_index,
        zset_score_lookup,
        flow_index,
        flow_lookup
      )
    end
  end

  def with_startup_active_rebuild_slot(fun) when is_function(fun, 0) do
    ref = :persistent_term.get(@startup_active_rebuild_slots_key)
    lease = acquire_startup_active_rebuild_slot(ref, startup_active_rebuild_concurrency())

    try do
      fun.()
    after
      release_startup_active_rebuild_slot(lease)
    end
  end

  def startup_active_rebuild_concurrency do
    System.schedulers_online()
    |> div(8)
    |> max(1)
    |> min(2)
  end

  defp do_rebuild_score_indexes(nil, _zset_score_lookup, _entries), do: :ok
  defp do_rebuild_score_indexes(_zset_score_index, nil, _entries), do: :ok

  defp do_rebuild_score_indexes(zset_score_index, zset_score_lookup, entries) do
    entries
    |> Enum.group_by(fn {index_key, _id, _score} -> index_key end, fn {_index_key, id, score} ->
      {id, score_string(score)}
    end)
    |> Enum.each(fn {index_key, member_score_pairs} ->
      ZSetIndex.put_members(zset_score_index, zset_score_lookup, index_key, member_score_pairs)
    end)
  end

  defp do_rebuild_flow_indexes_from_lmdb(
         lmdb_path,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup
       ) do
    now_ms = Ferricstore.CommandTime.now_ms()

    with_startup_active_rebuild_slot(fn ->
      fetch_page = fn after_key ->
        LMDB.prefix_entries_after(
          lmdb_path,
          LMDB.active_index_global_prefix(),
          after_key,
          @active_index_rebuild_batch_size
        )
      end

      fetch_lane_page = fn after_key ->
        LMDB.prefix_entries_after(
          lmdb_path,
          LMDB.active_by_state_global_prefix(),
          after_key,
          @active_index_rebuild_batch_size
        )
      end

      with {:ok, total_active} <-
             rebuild_flow_indexes_from_lmdb_page(
               lmdb_path,
               zset_score_index,
               zset_score_lookup,
               flow_index,
               flow_lookup,
               now_ms,
               <<>>,
               0,
               fetch_page,
               fn rows -> validate_active_index_projection_page(lmdb_path, rows, now_ms) end
             ),
           {:ok, _total_lanes} <-
             rebuild_fifo_lanes_from_lmdb_page(
               lmdb_path,
               flow_index,
               flow_lookup,
               now_ms,
               <<>>,
               0,
               fetch_lane_page
             ) do
        {:ok, total_active}
      end
    end)
  end

  @doc false
  def __rebuild_flow_indexes_from_lmdb_pages_for_test__(fetch_page)
      when is_function(fetch_page, 1) do
    rebuild_flow_indexes_from_lmdb_page(
      "test",
      nil,
      nil,
      nil,
      nil,
      0,
      <<>>,
      0,
      fetch_page,
      fn rows ->
        entries = Enum.map(rows, &Map.fetch!(&1, :entry))

        {:ok,
         %{
           score_entries: entries,
           native_entries: entries,
           hot_query_records: []
         }}
      end
    )
  end

  defp acquire_startup_active_rebuild_slot(ref, limit) do
    owner = self()
    token = make_ref()

    # The guardian owns the counter lease so an untrappable owner exit still releases it.
    {guardian, monitor_ref} =
      spawn_monitor(fn ->
        startup_active_rebuild_slot_guardian(owner, token, ref, limit)
      end)

    receive do
      {:startup_active_rebuild_slot_acquired, ^guardian, ^token} ->
        {guardian, monitor_ref, token}

      {:DOWN, ^monitor_ref, :process, ^guardian, reason} ->
        raise "startup active LMDB rebuild slot acquisition failed: #{inspect(reason)}"
    end
  end

  defp startup_active_rebuild_slot_guardian(owner, token, ref, limit) do
    owner_monitor = Process.monitor(owner)

    case guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor) do
      :acquired ->
        send(owner, {:startup_active_rebuild_slot_acquired, self(), token})
        guardian_await_startup_active_rebuild_release(ref, owner, owner_monitor, token)

      :owner_down ->
        :ok
    end
  end

  defp guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor) do
    receive do
      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :owner_down
    after
      0 ->
        guardian_try_startup_active_rebuild_slot(ref, limit, owner, owner_monitor)
    end
  end

  defp guardian_try_startup_active_rebuild_slot(ref, limit, owner, owner_monitor) do
    count = :atomics.get(ref, 1)

    cond do
      count >= limit ->
        receive do
          {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
            :owner_down
        after
          10 -> guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor)
        end

      :atomics.compare_exchange(ref, 1, count, count + 1) == :ok ->
        :acquired

      true ->
        guardian_acquire_startup_active_rebuild_slot(ref, limit, owner, owner_monitor)
    end
  end

  defp guardian_await_startup_active_rebuild_release(ref, owner, owner_monitor, token) do
    receive do
      {:release_startup_active_rebuild_slot, ^owner, ^token} ->
        Process.demonitor(owner_monitor, [:flush])
        release_startup_active_rebuild_counter(ref)

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        release_startup_active_rebuild_counter(ref)
    end
  end

  defp release_startup_active_rebuild_slot({guardian, monitor_ref, token}) do
    send(guardian, {:release_startup_active_rebuild_slot, self(), token})

    receive do
      {:DOWN, ^monitor_ref, :process, ^guardian, :normal} ->
        :ok

      {:DOWN, ^monitor_ref, :process, ^guardian, reason} ->
        raise "startup active LMDB rebuild slot release failed: #{inspect(reason)}"
    end
  end

  defp release_startup_active_rebuild_counter(ref) do
    case :atomics.sub_get(ref, 1, 1) do
      count when count >= 0 ->
        :ok

      _negative ->
        _ = :atomics.add_get(ref, 1, 1)
        raise "startup active LMDB rebuild slot released without a matching acquire"
    end
  rescue
    ArgumentError ->
      raise "startup active LMDB rebuild slot released without a matching acquire"
  end

  defp rebuild_flow_indexes_from_lmdb_page(
         lmdb_path,
         zset_score_index,
         zset_score_lookup,
         flow_index,
         flow_lookup,
         now_ms,
         after_key,
         total_active,
         fetch_page,
         validate_page
       ) do
    case fetch_page.(after_key) do
      {:ok, []} ->
        {:ok, total_active}

      {:ok, entries} when is_list(entries) ->
        with {:ok, decoded_rows} <- decode_active_index_page(entries, now_ms),
             {:ok,
              %{
                score_entries: score_entries,
                native_entries: native_entries,
                hot_query_records: hot_query_records
              }} <-
               validate_page.(decoded_rows),
             {:ok, last_key} <- active_index_page_last_key(entries),
             :ok <- ensure_active_index_cursor_advanced(after_key, last_key),
             :ok <-
               do_rebuild_score_indexes(zset_score_index, zset_score_lookup, score_entries),
             :ok <- rebuild_flow_index_entries(flow_index, flow_lookup, native_entries),
             :ok <-
               rebuild_hot_query_index_records(flow_index, flow_lookup, hot_query_records) do
          next_total = total_active + length(score_entries)

          :telemetry.execute(
            [:ferricstore, :flow, :lmdb_startup_active_index_chunk],
            %{
              entries: length(entries),
              active: length(score_entries),
              total_active: next_total
            },
            %{path: lmdb_path}
          )

          rebuild_flow_indexes_from_lmdb_page(
            lmdb_path,
            zset_score_index,
            zset_score_lookup,
            flow_index,
            flow_lookup,
            now_ms,
            last_key,
            next_total,
            fetch_page,
            validate_page
          )
        end

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_active_index_page, invalid}}
    end
  end

  defp decode_active_index_page(entries, now_ms) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {key, blob}, {:ok, acc} when is_binary(key) and is_binary(blob) ->
        case LMDB.decode_active_index_value(blob) do
          {:ok, {index_key, id, score, expire_at_ms, state_key}} ->
            if LMDB.active_index_entry_key?(key, index_key, id, score) do
              if expire_at_ms <= 0 or expire_at_ms > now_ms do
                row = %{
                  active_key: key,
                  state_key: state_key,
                  entry: {index_key, id, score}
                }

                {:cont, {:ok, [row | acc]}}
              else
                {:cont, {:ok, acc}}
              end
            else
              {:halt, {:error, {:active_index_key_value_mismatch, key}}}
            end

          :error ->
            {:halt, {:error, {:invalid_active_index_value, key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_active_index_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_active_index_projection_page(_lmdb_path, [], _now_ms) do
    {:ok, %{score_entries: [], native_entries: [], hot_query_records: []}}
  end

  defp validate_active_index_projection_page(lmdb_path, rows, now_ms) do
    state_keys = rows |> Enum.map(&Map.fetch!(&1, :state_key)) |> Enum.uniq()
    reverse_keys = Enum.map(state_keys, &LMDB.active_by_state_key_key/1)

    with {:ok, reverse_results} <- LMDB.get_many(lmdb_path, reverse_keys),
         {:ok, state_results} <- LMDB.get_many(lmdb_path, state_keys),
         {:ok, ownership} <-
           active_index_projection_ownership(
             state_keys,
             reverse_keys,
             reverse_results,
             state_results,
             now_ms
           ) do
      with {:ok, active_entries} <- validate_active_index_projection_rows(rows, ownership) do
        {:ok,
         %{
           score_entries: active_entries,
           native_entries: active_entries,
           hot_query_records: hot_query_projection_records(ownership, rows)
         }}
      end
    end
  end

  defp active_index_projection_ownership(
         state_keys,
         reverse_keys,
         reverse_results,
         state_results,
         now_ms
       )
       when length(state_keys) == length(reverse_keys) and
              length(state_keys) == length(reverse_results) and
              length(state_keys) == length(state_results) do
    [state_keys, reverse_keys, reverse_results, state_results]
    |> Enum.zip()
    |> Enum.reduce_while({:ok, %{}}, fn
      {state_key, reverse_key, {:ok, reverse_blob}, {:ok, state_blob}}, {:ok, acc}
      when is_binary(reverse_blob) and is_binary(state_blob) ->
        with {:ok, active_keys} <- LMDB.decode_active_index_reverse_value(reverse_blob),
             {:ok, {record, authoritative_entries}} <-
               decode_authoritative_active_entries(state_blob, state_key, now_ms) do
          hot_projection? =
            MapSet.new(active_keys) ==
              active_projection_key_set(LMDB.active_projection_entries(record))

          metadata = %{
            active_keys: MapSet.new(active_keys),
            authoritative_entries: authoritative_entries,
            hot_projection?: hot_projection?,
            record: record
          }

          {:cont, {:ok, Map.put(acc, state_key, metadata)}}
        else
          :error -> {:halt, {:error, {:invalid_active_reverse_value, reverse_key}}}
          {:error, _reason} = error -> {:halt, error}
        end

      {_state_key, reverse_key, _reverse_result, _state_result}, _acc ->
        {:halt, {:error, {:invalid_active_reverse_value, reverse_key}}}
    end)
  end

  defp active_index_projection_ownership(
         _state_keys,
         _reverse_keys,
         _reverse_results,
         _state_results,
         _now_ms
       ),
       do: {:error, :active_index_projection_lookup_mismatch}

  defp validate_active_index_projection_rows(rows, ownership) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, entries} ->
      active_key = Map.fetch!(row, :active_key)
      state_key = Map.fetch!(row, :state_key)
      entry = Map.fetch!(row, :entry)

      case Map.get(ownership, state_key) do
        %{active_keys: active_keys, authoritative_entries: authoritative_entries} ->
          cond do
            not MapSet.member?(active_keys, active_key) ->
              {:halt, {:error, {:active_index_reverse_membership_mismatch, active_key}}}

            not MapSet.member?(authoritative_entries, entry) ->
              {:halt, {:error, {:active_index_record_mismatch, active_key}}}

            true ->
              {:cont, {:ok, [entry | entries]}}
          end

        _missing ->
          {:halt, {:error, {:active_index_record_mismatch, active_key}}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_authoritative_active_entries(blob, state_key, now_ms) when is_binary(blob) do
    with {:ok, record} <- decode_authoritative_record(blob, state_key, now_ms) do
      {:ok, {record, MapSet.new(LMDB.active_projection_entries(record))}}
    end
  rescue
    _error -> {:error, {:active_index_record_mismatch, state_key}}
  end

  defp hot_query_projection_records(ownership, rows) do
    entries_by_state_key =
      Enum.reduce(rows, %{}, fn row, acc ->
        state_key = Map.fetch!(row, :state_key)
        entry = Map.fetch!(row, :entry)
        Map.update(acc, state_key, MapSet.new([entry]), &MapSet.put(&1, entry))
      end)

    ownership
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.flat_map(fn
      {state_key, %{hot_projection?: true, record: record}} ->
        case Map.get(entries_by_state_key, state_key) do
          %MapSet{} = entries ->
            if MapSet.member?(entries, hot_query_anchor_entry(record)), do: [record], else: []

          _missing ->
            []
        end

      {_state_key, _cold_or_invalid_projection} ->
        []
    end)
  end

  defp hot_query_anchor_entry(record) do
    state_index_key =
      Flow.Keys.state_index_key(
        Map.fetch!(record, :type),
        Map.fetch!(record, :state),
        Map.get(record, :partition_key)
      )

    Enum.find(LMDB.active_projection_entries(record), fn
      {^state_index_key, _id, _score} -> true
      _other -> false
    end)
  end

  defp rebuild_hot_query_index_records(_flow_index, _flow_lookup, []), do: :ok
  defp rebuild_hot_query_index_records(nil, _flow_lookup, _records), do: :ok
  defp rebuild_hot_query_index_records(_flow_index, nil, _records), do: :ok

  defp rebuild_hot_query_index_records(flow_index, flow_lookup, records) do
    case NativeFlowIndex.get(flow_index, flow_lookup) do
      nil ->
        :ok

      native ->
        records
        |> Enum.chunk_every(32)
        |> Enum.reduce_while(:ok, fn records, :ok ->
          entries = Enum.flat_map(records, &hot_query_index_entries/1)

          case rebuild_native_entries(native, entries) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
    end
  end

  defp hot_query_index_entries(record) do
    metadata_query_index_entries(record) ++
      Ferricstore.Flow.Attributes.index_entries(record) ++
      Ferricstore.Flow.StateMeta.index_entries(record)
  end

  defp metadata_query_index_entries(record) do
    id = Map.get(record, :id)
    partition_key = Map.get(record, :partition_key)
    score = Map.get(record, :updated_at_ms, 0)

    [
      {Map.get(record, :parent_flow_id), &Flow.Keys.parent_index_key(&1, partition_key)},
      {non_default_root_flow_id(record), &Flow.Keys.root_index_key(&1, partition_key)},
      {Map.get(record, :correlation_id), &Flow.Keys.correlation_index_key(&1, partition_key)}
    ]
    |> Enum.flat_map(fn
      {value, key_fun} when is_binary(value) and value != "" and is_binary(id) ->
        [{key_fun.(value), id, score}]

      _missing_or_invalid ->
        []
    end)
  end

  defp non_default_root_flow_id(record) do
    id = Map.get(record, :id)

    case Map.get(record, :root_flow_id) do
      root_flow_id when root_flow_id in [nil, "", id] -> nil
      root_flow_id -> root_flow_id
    end
  end

  defp rebuild_fifo_lanes_from_lmdb_page(
         lmdb_path,
         flow_index,
         flow_lookup,
         now_ms,
         after_key,
         total_lanes,
         fetch_page
       ) do
    case fetch_page.(after_key) do
      {:ok, []} ->
        {:ok, total_lanes}

      {:ok, entries} when is_list(entries) ->
        with {:ok, lane_entries} <- decode_fifo_lane_page(lmdb_path, entries, now_ms),
             {:ok, last_key} <- active_index_page_last_key(entries),
             :ok <- ensure_active_index_cursor_advanced(after_key, last_key),
             :ok <- rebuild_flow_index_entries(flow_index, flow_lookup, lane_entries) do
          next_total = total_lanes + length(lane_entries)

          :telemetry.execute(
            [:ferricstore, :flow, :lmdb_startup_fifo_lane_chunk],
            %{entries: length(entries), lanes: length(lane_entries), total_lanes: next_total},
            %{path: lmdb_path}
          )

          rebuild_fifo_lanes_from_lmdb_page(
            lmdb_path,
            flow_index,
            flow_lookup,
            now_ms,
            last_key,
            next_total,
            fetch_page
          )
        end

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_active_reverse_page, invalid}}
    end
  end

  defp decode_fifo_lane_page(lmdb_path, entries, now_ms) do
    prefix = LMDB.active_by_state_global_prefix()

    with {:ok, reverse_rows} <- decode_fifo_lane_reverse_rows(entries, prefix),
         {:ok, state_results} <-
           LMDB.get_many(lmdb_path, Enum.map(reverse_rows, &Map.fetch!(&1, :state_key))),
         {:ok, lane_entries} <-
           validate_fifo_lane_reverse_records(reverse_rows, state_results, now_ms),
         :ok <- validate_referenced_active_index_rows(lmdb_path, reverse_rows, now_ms) do
      {:ok, lane_entries}
    end
  end

  defp decode_fifo_lane_reverse_rows(entries, prefix) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {key, blob}, {:ok, acc} when is_binary(key) and is_binary(blob) ->
        case {String.starts_with?(key, prefix), LMDB.decode_active_index_reverse_metadata(blob)} do
          {true, {:ok, {active_keys, {lane_key, lane_member, lane_score}}}}
          when lane_score in [-1, 0] ->
            state_key = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))

            if valid_fifo_lane_reverse_owner?(state_key, lane_key, lane_member) do
              row = %{
                reverse_key: key,
                state_key: state_key,
                lane_key: lane_key,
                lane_member: lane_member,
                lane_score: lane_score,
                active_keys: active_keys
              }

              {:cont, {:ok, [row | acc]}}
            else
              {:halt, {:error, {:fifo_lane_reverse_owner_mismatch, key}}}
            end

          {true, {:ok, {_active_keys, nil}}} ->
            {:halt, {:error, {:missing_fifo_lane_projection, key}}}

          _invalid ->
            {:halt, {:error, {:invalid_active_reverse_value, key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_active_reverse_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_fifo_lane_reverse_records(reverse_rows, state_results, now_ms)
       when length(reverse_rows) == length(state_results) do
    reverse_rows
    |> Enum.zip(state_results)
    |> Enum.reduce_while({:ok, []}, fn
      {%{
         reverse_key: reverse_key,
         state_key: state_key,
         lane_key: lane_key,
         lane_member: lane_member,
         lane_score: lane_score,
         active_keys: active_keys
       }, {:ok, blob}},
      {:ok, acc} ->
        with {:ok, record} <- decode_authoritative_record(blob, state_key, now_ms),
             {^lane_key, ^lane_member, ^lane_score} = entry <- FifoLane.index_entry(record),
             :ok <- validate_active_reverse_projection(reverse_key, active_keys, record) do
          {:cont, {:ok, [entry | acc]}}
        else
          {:error, {:active_index_reverse_projection_mismatch, ^reverse_key}} = error ->
            {:halt, error}

          _missing_or_mismatched ->
            {:halt, {:error, {:fifo_lane_reverse_record_mismatch, reverse_key}}}
        end

      {%{reverse_key: reverse_key}, _missing_or_invalid}, _acc ->
        {:halt, {:error, {:fifo_lane_reverse_record_mismatch, reverse_key}}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_fifo_lane_reverse_records(_reverse_rows, _state_results, _now_ms),
    do: {:error, :fifo_lane_reverse_state_result_mismatch}

  defp validate_active_reverse_projection(reverse_key, active_keys, record) do
    actual = MapSet.new(active_keys)
    hot = active_projection_key_set(LMDB.active_projection_entries(record))
    cold = active_projection_key_set(LMDB.active_timeout_projection_entries(record))

    if actual == hot or actual == cold do
      :ok
    else
      {:error, {:active_index_reverse_projection_mismatch, reverse_key}}
    end
  rescue
    _error -> {:error, {:active_index_reverse_projection_mismatch, reverse_key}}
  end

  defp active_projection_key_set(entries) do
    MapSet.new(entries, fn {index_key, id, score} ->
      LMDB.active_index_key(index_key, id, score)
    end)
  end

  defp validate_referenced_active_index_rows(lmdb_path, reverse_rows, now_ms) do
    descriptors =
      Enum.flat_map(reverse_rows, fn row ->
        state_key = Map.fetch!(row, :state_key)

        Enum.map(Map.fetch!(row, :active_keys), fn active_key ->
          {state_key, active_key}
        end)
      end)

    active_keys = Enum.map(descriptors, &elem(&1, 1))

    with {:ok, active_results} <- LMDB.get_many(lmdb_path, active_keys) do
      validate_referenced_active_index_results(descriptors, active_results, now_ms)
    end
  end

  defp validate_referenced_active_index_results(descriptors, active_results, now_ms)
       when length(descriptors) == length(active_results) do
    descriptors
    |> Enum.zip(active_results)
    |> Enum.reduce_while(:ok, fn
      {{state_key, active_key}, {:ok, blob}}, :ok when is_binary(blob) ->
        case LMDB.decode_active_index_value(blob) do
          {:ok, {index_key, id, score, expire_at_ms, ^state_key}} ->
            cond do
              not LMDB.active_index_entry_key?(active_key, index_key, id, score) ->
                {:halt, {:error, {:invalid_active_index_projection, active_key}}}

              expire_at_ms > 0 and expire_at_ms <= now_ms ->
                {:halt, {:error, {:expired_active_index_projection, active_key}}}

              true ->
                {:cont, :ok}
            end

          {:ok, {_index_key, _id, _score, _expire_at_ms, _foreign_state_key}} ->
            {:halt, {:error, {:active_index_reverse_state_mismatch, active_key}}}

          :error ->
            {:halt, {:error, {:invalid_active_index_projection, active_key}}}
        end

      {{_state_key, active_key}, :not_found}, :ok ->
        {:halt, {:error, {:missing_active_index_projection, active_key}}}

      {{_state_key, active_key}, _invalid}, :ok ->
        {:halt, {:error, {:invalid_active_index_projection, active_key}}}
    end)
  end

  defp validate_referenced_active_index_results(_descriptors, _active_results, _now_ms),
    do: {:error, :active_index_projection_lookup_mismatch}

  defp decode_authoritative_record(blob, state_key, now_ms)
       when is_binary(blob) and is_binary(state_key) and is_integer(now_ms) do
    with {:ok, encoded} <- LMDB.decode_value(blob, now_ms),
         record when is_map(record) <- Flow.decode_record(encoded),
         true <-
           Ferricstore.Flow.Keys.state_key(record.id, Map.get(record, :partition_key)) ==
             state_key do
      {:ok, record}
    else
      _invalid -> {:error, {:active_index_record_mismatch, state_key}}
    end
  rescue
    _error -> {:error, {:active_index_record_mismatch, state_key}}
  end

  defp valid_fifo_lane_reverse_owner?(state_key, lane_key, lane_member) do
    with {:ok, {_sequence, id}} <- FifoLane.decode_member(lane_member),
         true <- flow_state_key_id(state_key) == id,
         {:ok, state_tag} <- flow_key_tag(state_key),
         {:ok, lane_tag} <- flow_key_tag(lane_key) do
      state_tag == lane_tag
    else
      _invalid -> false
    end
  end

  defp flow_state_key_id(state_key) when is_binary(state_key) do
    case :binary.match(state_key, "}:s:") do
      {pos, marker_size} ->
        start = pos + marker_size
        binary_part(state_key, start, byte_size(state_key) - start)

      :nomatch ->
        nil
    end
  end

  defp flow_key_tag(key) when is_binary(key) do
    case :binary.match(key, "}") do
      {pos, 1} when pos > 2 -> {:ok, binary_part(key, 0, pos + 1)}
      _invalid -> :error
    end
  end

  defp active_index_page_last_key(entries) do
    case List.last(entries) do
      {last_key, _last_value} when is_binary(last_key) -> {:ok, last_key}
      invalid -> {:error, {:invalid_active_index_entry, invalid}}
    end
  end

  defp ensure_active_index_cursor_advanced(after_key, last_key) when last_key > after_key, do: :ok

  defp ensure_active_index_cursor_advanced(after_key, last_key),
    do: {:error, {:active_index_scan_stalled, after_key, last_key}}

  defp rebuild_flow_index_entries(nil, _flow_lookup, _entries), do: :ok
  defp rebuild_flow_index_entries(_flow_index, nil, _entries), do: :ok

  defp rebuild_flow_index_entries(flow_index, flow_lookup, entries) do
    case NativeFlowIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> rebuild_native_entries(native, entries)
    end
  end

  defp rebuild_native_entries(_native, []), do: :ok

  defp rebuild_native_entries(native, entries) do
    with {:ok, batches} <- NativeFlowIndex.chunk_batch_ops([{:put_entries, entries}]) do
      Enum.reduce_while(batches, :ok, fn batch, :ok ->
        case NativeFlowIndex.apply_batch(native, batch) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp active_flow_index_entries(record) do
    record
    |> LMDB.active_projection_entries()
    |> maybe_add_fifo_lane_entry(record)
  end

  defp maybe_add_fifo_lane_entry(entries, record) do
    case FifoLane.index_entry(record) do
      {lane_key, member, score} -> [{lane_key, member, score} | entries]
      _terminal_or_invalid -> entries
    end
  end

  defp score_string(value) when is_integer(value), do: Float.to_string(value * 1.0)
  defp score_string(value) when is_float(value), do: Float.to_string(value)
  defp score_string(_value), do: "0.0"
end
