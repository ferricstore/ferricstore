defmodule Ferricstore.Flow.LMDB do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @default_map_size 16 * 1024 * 1024 * 1024

  def enabled?, do: true
  def projection_enabled?, do: true
  def mode, do: :lagged
  def refresh_config!, do: :lagged
  def normalize_mode(_value), do: :lagged

  def map_size do
    Application.get_env(:ferricstore, :flow_lmdb_map_size, @default_map_size)
  end

  def path(shard_data_path), do: Path.join(shard_data_path, "flow_lmdb")

  def ensure_shard_dirs(data_dir, shard_count)
      when is_binary(data_dir) and is_integer(shard_count) and shard_count >= 0 do
    Enum.reduce_while(0..max(shard_count - 1, -1)//1, :ok, fn shard_index, :ok ->
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> path()
      |> Ferricstore.FS.mkdir_p()
      |> case do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  def ensure_shard_dirs(_data_dir, _shard_count), do: {:error, :badarg}

  # LMDB stores Flow state as a wrapper around the already-versioned Flow record
  # bytes: {expire_at_ms, encoded_record}. The wrapper owns TTL semantics only.
  # Do not version Flow metadata here or decode user payload here. If the wrapper
  # itself changes, add a wrapper version and keep decoding this tuple form so
  # old LMDB mirrors can survive restart/rebuild after upgrade.
  def encode_value(value, expire_at_ms), do: :erlang.term_to_binary({expire_at_ms, value})

  # Returns the wrapped encoded Flow record when it is still live. Callers must
  # pass the returned value through Ferricstore.Flow.decode_record/1 so the Flow
  # schema version gate stays centralized in one place.
  def decode_value(blob, now_ms) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {expire_at_ms, value} when is_integer(expire_at_ms) and expire_at_ms > 0 ->
        if expire_at_ms <= now_ms, do: :expired, else: {:ok, value}

      {0, value} ->
        {:ok, value}

      {_expire_at_ms, value} ->
        {:ok, value}
    end
  rescue
    _ -> :error
  end

  def get(path, key) when is_binary(path) and is_binary(key) do
    if Ferricstore.FS.dir?(path), do: NIF.lmdb_get(path, key, map_size()), else: :not_found
  end

  def encode_value_locator(expire_at_ms, file_id, offset, value_size) do
    :erlang.term_to_binary({:flow_value_locator, 1, expire_at_ms, file_id, offset, value_size})
  end

  def decode_value_locator(blob, now_ms) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {:flow_value_locator, 1, expire_at_ms, file_id, offset, value_size}
      when is_integer(expire_at_ms) and expire_at_ms > 0 ->
        if expire_at_ms <= now_ms do
          :expired
        else
          decode_live_value_locator(file_id, offset, value_size)
        end

      {:flow_value_locator, 1, _expire_at_ms, file_id, offset, value_size} ->
        decode_live_value_locator(file_id, offset, value_size)

      _other ->
        :not_locator
    end
  rescue
    _ -> :error
  end

  defp decode_live_value_locator(file_id, offset, value_size)
       when is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 do
    {:ok, {file_id, offset, value_size}}
  end

  defp decode_live_value_locator(_file_id, _offset, _value_size), do: :error

  def cold_park_key(flow_id) when is_binary(flow_id),
    do: Ferricstore.Flow.LMDB.Cold.park_key(flow_id)

  def cold_park_key_for_state_key(state_key) when is_binary(state_key),
    do: Ferricstore.Flow.LMDB.Cold.park_key_for_state_key(state_key)

  def cold_due_bucket_ms(due_at_ms, bucket_ms \\ 60_000)

  def cold_due_bucket_ms(due_at_ms, bucket_ms),
    do: Ferricstore.Flow.LMDB.Cold.due_bucket_ms(due_at_ms, bucket_ms)

  def cold_due_key(attrs), do: Ferricstore.Flow.LMDB.Cold.due_key(attrs)

  def cold_due_bucket_prefix(bucket_ms),
    do: Ferricstore.Flow.LMDB.Cold.due_bucket_prefix(bucket_ms)

  def cold_due_type_bucket_prefix(bucket_ms, type),
    do: Ferricstore.Flow.LMDB.Cold.due_type_bucket_prefix(bucket_ms, type)

  def cold_due_claim_prefix(attrs), do: Ferricstore.Flow.LMDB.Cold.due_claim_prefix(attrs)

  def cold_by_segment_key(locator), do: Ferricstore.Flow.LMDB.Cold.by_segment_key(locator)

  def cold_by_segment_prefix(file_id), do: Ferricstore.Flow.LMDB.Cold.by_segment_prefix(file_id)

  def encode_cold_park(locator, attrs),
    do: Ferricstore.Flow.LMDB.Cold.encode_park(locator, attrs)

  def decode_cold_park(blob), do: Ferricstore.Flow.LMDB.Cold.decode_park(blob)

  def encode_cold_value_locator(value_ref, owner_flow_id, owner_version, locator, attrs \\ []),
    do:
      Ferricstore.Flow.LMDB.Cold.encode_value_locator(
        value_ref,
        owner_flow_id,
        owner_version,
        locator,
        attrs
      )

  def decode_cold_value_locator(blob), do: Ferricstore.Flow.LMDB.Cold.decode_value_locator(blob)

  def get_many(_path, []), do: {:ok, []}

  def get_many(path, keys) when is_binary(path) and is_list(keys) do
    if Ferricstore.FS.dir?(path) do
      NIF.lmdb_get_many(path, keys, map_size())
    else
      {:ok, Enum.map(keys, fn _key -> :not_found end)}
    end
  end

  def warm(path) when is_binary(path) do
    case NIF.lmdb_get(path, <<0>>, map_size()) do
      {:ok, _value} -> :ok
      :not_found -> :ok
      {:error, _reason} = error -> error
    end
  end

  def write_batch(_path, []), do: :ok

  def write_batch(path, ops) when is_binary(path) and is_list(ops) do
    NIF.lmdb_write_batch(path, ops, map_size())
  end

  def clear_all(data_dir, shard_count)
      when is_binary(data_dir) and is_integer(shard_count) and shard_count >= 0 do
    Enum.reduce_while(0..max(shard_count - 1, -1)//1, :ok, fn shard_index, :ok ->
      path =
        data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> path()

      case clear(path) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  def clear(path) when is_binary(path) do
    clear_cached_terminal_counts_for_path(path)

    if Ferricstore.FS.dir?(path) do
      NIF.lmdb_clear(path, map_size())
    else
      :ok
    end
  end

  def release_all do
    NIF.lmdb_release_all()
  end

  def flush_in_progress_key, do: "flow-lmdb-flush-in-progress"

  def flush_in_progress?(path) when is_binary(path) do
    if env_present?(path) do
      case get(path, flush_in_progress_key()) do
        {:ok, _value} -> true
        _ -> false
      end
    else
      false
    end
  end

  def env_present?(path) when is_binary(path) do
    File.regular?(Path.join(path, "data.mdb")) or File.regular?(Path.join(path, "lock.mdb"))
  end

  def env_present?(_path), do: false

  def flush_in_progress_put_op, do: {:put, flush_in_progress_key(), <<1>>}
  def flush_in_progress_delete_op, do: {:delete, flush_in_progress_key()}

  def write_batch_with_originals(_path, []), do: {:ok, []}

  def write_batch_with_originals(path, ops) when is_binary(path) and is_list(ops) do
    NIF.lmdb_write_batch_with_originals(path, ops, map_size())
  end

  def prefix_entries(path, prefix, limit)
      when is_binary(path) and is_binary(prefix) and is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries(path, prefix, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_entries_after(path, prefix, after_key, limit)
      when is_binary(path) and is_binary(prefix) and is_binary(after_key) and
             is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries_after(path, prefix, after_key, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_entries(path, prefix, limit, false), do: prefix_entries(path, prefix, limit)

  def prefix_entries(path, prefix, limit, true)
      when is_binary(path) and is_binary(prefix) and is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries_reverse(path, prefix, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_entries_reverse_before(path, prefix, before_key, limit)
      when is_binary(path) and is_binary(prefix) and is_binary(before_key) and
             is_integer(limit) and limit >= 0 do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_entries_reverse_before(path, prefix, before_key, limit, map_size()),
      else: {:ok, []}
  end

  def prefix_count(path, prefix) when is_binary(path) and is_binary(prefix) do
    if Ferricstore.FS.dir?(path),
      do: NIF.lmdb_prefix_count(path, prefix, map_size()),
      else: {:ok, 0}
  end

  def terminal_state?(state) when state in ["completed", "failed", "cancelled"], do: true
  def terminal_state?(_state), do: false

  def terminal_index_prefix(state_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_prefix(state_index_key)

  def terminal_index_global_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_global_prefix()

  def terminal_index_key(state_index_key, id, updated_at_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_key(state_index_key, id, updated_at_ms)

  def terminal_count_key(state_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_count_key(state_index_key)

  def terminal_count_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.terminal_count_prefix()

  def terminal_expire_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.terminal_expire_prefix()

  def terminal_expire_key(expire_at_ms, terminal_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_expire_key(expire_at_ms, terminal_key)

  def terminal_by_state_key_key(state_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_by_state_key_key(state_key)

  def terminal_by_state_global_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_by_state_global_prefix()

  def active_index_global_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.active_index_global_prefix()

  def active_index_prefix(index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.active_index_prefix(index_key)

  def active_index_key(index_key, id, score),
    do: Ferricstore.Flow.LMDB.IndexCodec.active_index_key(index_key, id, score)

  def active_by_state_key_key(state_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.active_by_state_key_key(state_key)

  def active_by_state_global_prefix,
    do: Ferricstore.Flow.LMDB.IndexCodec.active_by_state_global_prefix()

  def active_index_put_ops(state_key, record, expire_at_ms)
      when is_binary(state_key) and is_map(record) and is_integer(expire_at_ms) do
    {ops, _reverse_value} = active_index_put_ops_with_reverse(state_key, record, expire_at_ms)
    ops
  end

  def active_index_put_ops_with_reverse(state_key, record, expire_at_ms)
      when is_binary(state_key) and is_map(record) and is_integer(expire_at_ms) do
    entries = active_flow_index_entries(record)

    active_ops =
      Enum.map(entries, fn {index_key, id, score} ->
        active_key = active_index_key(index_key, id, score)
        value = encode_active_index_value(index_key, id, score, expire_at_ms, state_key)
        {:put, active_key, value}
      end)

    active_keys = Enum.map(active_ops, fn {:put, key, _value} -> key end)
    reverse_value = encode_active_index_reverse_value(active_keys)

    {
      [{:put, active_by_state_key_key(state_key), reverse_value} | active_ops],
      reverse_value
    }
  end

  def active_index_delete_ops_from_reverse(state_key, reverse_value) when is_binary(state_key) do
    reverse_key = active_by_state_key_key(state_key)

    case decode_active_index_reverse_value(reverse_value) do
      {:ok, active_keys} ->
        [{:delete, reverse_key} | Enum.map(active_keys, &{:delete, &1})]

      :error ->
        [{:delete, reverse_key}]
    end
  end

  def active_index_delete_ops(path, state_key) when is_binary(path) and is_binary(state_key) do
    reverse_key = active_by_state_key_key(state_key)

    case get(path, reverse_key) do
      {:ok, reverse_value} -> active_index_delete_ops_from_reverse(state_key, reverse_value)
      _ -> [{:delete, reverse_key}]
    end
  end

  def query_index_prefix(index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.query_index_prefix(index_key)

  def query_index_key(index_key, id, updated_at_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.query_index_key(index_key, id, updated_at_ms)

  def history_index_prefix(history_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_index_prefix(history_key)

  def history_index_key(history_key, event_id, event_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_index_key(history_key, event_id, event_ms)

  def history_expire_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.history_expire_prefix()

  def history_flow_expire_prefix, do: Ferricstore.Flow.LMDB.IndexCodec.history_flow_expire_prefix()

  def segment_value_pin_prefix, do: Ferricstore.Flow.LMDB.SegmentPins.prefix()

  def segment_value_pin_prefix(tag) when tag in [:waraft_segment, :waraft_apply_projection],
    do: Ferricstore.Flow.LMDB.SegmentPins.prefix(tag)

  def segment_value_pin_key(file_id, value_key),
    do: Ferricstore.Flow.LMDB.SegmentPins.key(file_id, value_key)

  def encode_segment_value_pin_batch(file_id, entries),
    do: Ferricstore.Flow.LMDB.SegmentPins.encode_batch(file_id, entries)

  def segment_value_pin_put_ops(value_key, expire_at_ms, file_id, offset, value_size),
    do: Ferricstore.Flow.LMDB.SegmentPins.put_ops(value_key, expire_at_ms, file_id, offset, value_size)

  def segment_value_pin_batch_put_ops(entries),
    do: Ferricstore.Flow.LMDB.SegmentPins.batch_put_ops(entries)

  def segment_value_pin_delete_ops(value_key, file_id),
    do: Ferricstore.Flow.LMDB.SegmentPins.delete_ops(value_key, file_id)

  def segment_value_pin_entries_before(path, trim_index, limit),
    do: Ferricstore.Flow.LMDB.SegmentPins.entries_before(path, trim_index, limit)

  def segment_value_pin_entries_before_page(path, trim_index, after_key, limit),
    do: Ferricstore.Flow.LMDB.SegmentPins.entries_before_page(path, trim_index, after_key, limit)

  def history_expire_key(expire_at_ms, history_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_expire_key(expire_at_ms, history_index_key)

  def history_flow_expire_key(expire_at_ms, history_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.history_flow_expire_key(expire_at_ms, history_key)

  def encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms \\ 0),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_history_index_value(
        event_id,
        event_ms,
        compound_key,
        expire_at_ms
      )

  def encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms, file_id, offset, value_size),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_history_index_value(
        event_id,
        event_ms,
        compound_key,
        expire_at_ms,
        file_id,
        offset,
        value_size
      )

  def encode_history_expire_value(history_index_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.encode_history_expire_value(history_index_key)

  def encode_history_flow_expire_value(history_key, expire_at_ms),
    do: Ferricstore.Flow.LMDB.IndexCodec.encode_history_flow_expire_value(history_key, expire_at_ms)

  def decode_history_expire_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_expire_value(blob)

  def decode_history_flow_expire_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_flow_expire_value(blob)

  def encode_query_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil),
    do: Ferricstore.Flow.LMDB.IndexCodec.encode_query_index_value(id, updated_at_ms, expire_at_ms, state_key)

  def encode_active_index_value(index_key, id, score, expire_at_ms, state_key),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_active_index_value(
        index_key,
        id,
        score,
        expire_at_ms,
        state_key
      )

  def decode_active_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_active_index_value(blob)

  def encode_active_index_reverse_value(active_keys),
    do: Ferricstore.Flow.LMDB.IndexCodec.encode_active_index_reverse_value(active_keys)

  def decode_active_index_reverse_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_active_index_reverse_value(blob)

  def delete_state_artifacts(path, state_key) when is_binary(path) and is_binary(state_key) do
    reverse_key = terminal_by_state_key_key(state_key)
    active_ops = active_index_delete_ops(path, state_key)

    case get(path, reverse_key) do
      {:ok, terminal_key} when is_binary(terminal_key) ->
        ops = terminal_index_delete_ops(path, terminal_key, state_key) ++ active_ops
        write_batch(path, ops)

      _ ->
        write_batch(path, [{:delete, state_key}, {:delete, reverse_key} | active_ops])
    end
  end

  def delete_terminal_index_entry(path, terminal_key, state_key)
      when is_binary(path) and is_binary(terminal_key) do
    count_key = terminal_count_key_for_index_entry(path, terminal_key)
    ops = terminal_index_delete_ops(path, terminal_key, state_key)

    case write_batch(path, ops) do
      :ok ->
        refresh_terminal_count_cache_after_delete(path, count_key)
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  def delete_history_index_entry(path, history_index_key)
      when is_binary(path) and is_binary(history_index_key) do
    ops = history_index_delete_ops(path, history_index_key)
    write_batch(path, ops)
  end

  def terminal_index_delete_ops(path, terminal_key, state_key)
      when is_binary(path) and is_binary(terminal_key) do
    ops = [{:delete, terminal_key}]

    ops =
      if is_binary(state_key) do
        [{:delete, state_key}, {:delete, terminal_by_state_key_key(state_key)} | ops]
      else
        ops
      end

    case get(path, terminal_key) do
      {:ok, terminal_value} ->
        ops
        |> maybe_decrement_count(path, terminal_value)
        |> maybe_delete_expire_key(terminal_key, terminal_value)

      _ ->
        ops
    end
  end

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_terminal_index_value(
        id,
        updated_at_ms,
        expire_at_ms,
        state_key
      )

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms, state_key, count_key),
    do:
      Ferricstore.Flow.LMDB.IndexCodec.encode_terminal_index_value(
        id,
        updated_at_ms,
        expire_at_ms,
        state_key,
        count_key
      )

  def encode_terminal_expire_value(terminal_key, state_key, count_key),
    do: Ferricstore.Flow.LMDB.IndexCodec.encode_terminal_expire_value(terminal_key, state_key, count_key)

  def decode_terminal_expire_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_terminal_expire_value(blob)

  def encode_count(count), do: Ferricstore.Flow.LMDB.IndexCodec.encode_count(count)

  def decode_count(blob), do: Ferricstore.Flow.LMDB.IndexCodec.decode_count(blob)

  def terminal_count(path, state_index_key),
    do: Ferricstore.Flow.LMDB.TerminalCounts.terminal_count(path, state_index_key)

  def terminal_counts(path, state_index_keys),
    do: Ferricstore.Flow.LMDB.TerminalCounts.terminal_counts(path, state_index_keys)

  def refresh_terminal_count_key(path, count_key),
    do: Ferricstore.Flow.LMDB.TerminalCounts.refresh_count_key(path, count_key)

  def put_cached_terminal_count_key(path, count_key, count),
    do: Ferricstore.Flow.LMDB.TerminalCounts.put_cached_count_key(path, count_key, count)

  def delete_cached_terminal_count_key(path, count_key),
    do: Ferricstore.Flow.LMDB.TerminalCounts.delete_cached_count_key(path, count_key)

  def clear_cached_terminal_counts_for_path(path),
    do: Ferricstore.Flow.LMDB.TerminalCounts.clear_cached_for_path(path)

  def put_terminal_count(path, state_index_key, count),
    do: Ferricstore.Flow.LMDB.TerminalCounts.put_terminal_count(path, state_index_key, count)

  def sweep_expired_terminal(path, now_ms, limit),
    do: Ferricstore.Flow.LMDB.Retention.sweep_expired_terminal(path, now_ms, limit)

  def expired_terminal_state_keys(path, now_ms, limit),
    do: Ferricstore.Flow.LMDB.Retention.expired_terminal_state_keys(path, now_ms, limit)

  def sweep_expired_history(path, now_ms, limit),
    do: Ferricstore.Flow.LMDB.Retention.sweep_expired_history(path, now_ms, limit)

  def history_compound_entries(path, history_key, limit)
      when is_binary(path) and is_binary(history_key) and is_integer(limit) and limit > 0 do
    with {:ok, decoded} <- history_compound_location_entries(path, history_key, limit) do
      decoded =
        decoded
        |> Enum.map(fn {compound_key, event_id, _value} -> {compound_key, event_id} end)
        |> Enum.uniq()

      {:ok, decoded}
    end
  end

  def history_compound_entries(_path, _history_key, _limit), do: {:ok, []}

  def history_compound_location_entries(path, history_key, limit)
      when is_binary(path) and is_binary(history_key) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <- prefix_entries(path, history_index_prefix(history_key), limit) do
      decoded =
        entries
        |> Enum.flat_map(fn {_history_index_key, value} ->
          case decode_history_index_value(value) do
            {:ok, {event_id, _event_ms, _expire_at_ms, compound_key}} ->
              [{compound_key, event_id, value}]

            :error ->
              []
          end
        end)
        |> Enum.uniq_by(fn {compound_key, event_id, _value} -> {compound_key, event_id} end)

      {:ok, decoded}
    end
  end

  def history_compound_location_entries(_path, _history_key, _limit), do: {:ok, []}

  def decode_terminal_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_terminal_index_value(blob)

  def decode_query_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_query_index_value(blob)

  def decode_history_index_value(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_index_value(blob)

  def decode_history_index_location(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.decode_history_index_location(blob)

  defp active_flow_index_entries(record) do
    id = Map.fetch!(record, :id)
    type = Map.fetch!(record, :type)
    state = Map.fetch!(record, :state)
    partition_key = Map.get(record, :partition_key)
    updated_score = normalize_ms(Map.get(record, :updated_at_ms, 0))
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, state, partition_key)

    [{state_index_key, id, updated_score}]
    |> maybe_add_due_active_entry(record, partition_key)
    |> maybe_add_running_active_entries(record, partition_key)
  end

  defp maybe_add_due_active_entry(
         entries,
         %{next_run_at_ms: next_run_at_ms} = record,
         partition_key
       )
       when is_integer(next_run_at_ms) do
    priority = Map.get(record, :priority, 0)
    due_key = Ferricstore.Flow.Keys.due_key(record.type, record.state, priority, partition_key)
    [{due_key, record.id, normalize_ms(next_run_at_ms)} | entries]
  end

  defp maybe_add_due_active_entry(entries, _record, _partition_key), do: entries

  defp maybe_add_running_active_entries(
         entries,
         %{state: "running", lease_deadline_ms: lease_deadline_ms} = record,
         partition_key
       )
       when is_integer(lease_deadline_ms) do
    score = normalize_ms(lease_deadline_ms)
    inflight_key = Ferricstore.Flow.Keys.inflight_index_key(record.type, partition_key)

    worker_key =
      Ferricstore.Flow.Keys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

    [
      {worker_key, record.id, score},
      {inflight_key, record.id, score}
      | entries
    ]
  end

  defp maybe_add_running_active_entries(entries, _record, _partition_key), do: entries

  def terminal_index_count_key(blob),
    do: Ferricstore.Flow.LMDB.IndexCodec.terminal_index_count_key(blob)

  defp terminal_count_key_for_index_entry(path, terminal_key) do
    case get(path, terminal_key) do
      {:ok, terminal_value} -> terminal_index_count_key(terminal_value)
      _ -> :missing
    end
  end

  defp refresh_terminal_count_cache_after_delete(path, {:ok, count_key}) do
    Ferricstore.Flow.LMDB.TerminalCounts.refresh_after_delete(path, {:ok, count_key})
  end

  defp refresh_terminal_count_cache_after_delete(path, :missing),
    do: Ferricstore.Flow.LMDB.TerminalCounts.refresh_after_delete(path, :missing)

  defp maybe_decrement_count(ops, path, terminal_value) do
    case terminal_index_count_key(terminal_value) do
      {:ok, count_key} ->
        count = max(read_count_key(path, count_key) - 1, 0)
        [{:put, count_key, encode_count(count)} | ops]

      :missing ->
        ops
    end
  end

  defp maybe_delete_expire_key(ops, terminal_key, terminal_value) do
    case decode_terminal_index_value(terminal_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = terminal_expire_key(expire_at_ms, terminal_key)
        [{:delete, expire_key} | ops]

      _ ->
        ops
    end
  end

  def history_index_delete_ops(path, history_index_key) do
    ops = [{:delete, history_index_key}]

    case get(path, history_index_key) do
      {:ok, history_value} ->
        history_index_delete_ops_from_value(ops, history_index_key, history_value)

      _ ->
        ops
    end
  end

  defp history_index_delete_ops_from_value(ops, history_index_key, history_value) do
    maybe_delete_history_expire_key(ops, history_index_key, history_value)
  end

  defp maybe_delete_history_expire_key(ops, history_index_key, history_value) do
    case decode_history_index_value(history_value) do
      {:ok, {_event_id, _event_ms, expire_at_ms, _compound_key}} when expire_at_ms > 0 ->
        [{:delete, history_expire_key(expire_at_ms, history_index_key)} | ops]

      _ ->
        ops
    end
  end

  defp read_count_key(path, count_key) do
    case get(path, count_key) do
      {:ok, blob} ->
        case decode_count(blob) do
          {:ok, count} -> count
          :error -> 0
        end

      _ ->
        0
    end
  end

  defp normalize_ms(value) when is_integer(value), do: value
  defp normalize_ms(value) when is_float(value), do: trunc(value)

  defp normalize_ms(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        case Float.parse(value) do
          {float, _rest} -> trunc(float)
          :error -> 0
        end
    end
  end

  defp normalize_ms(_value), do: 0
end
