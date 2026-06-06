defmodule Ferricstore.Flow.LMDB do
  @moduledoc false

  alias Ferricstore.Flow.Locator
  alias Ferricstore.Bitcask.NIF

  @default_map_size 16 * 1024 * 1024 * 1024
  @terminal_count_cache :ferricstore_flow_lmdb_terminal_count_cache
  @u64_decimal_zero_pad "00000000000000000000"

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

  def cold_park_key(flow_id) when is_binary(flow_id), do: "flow:park:v1:" <> flow_id

  def cold_park_key_for_state_key(state_key) when is_binary(state_key),
    do: "flow:park:v1:key:" <> escape_key_part(state_key)

  def cold_due_bucket_ms(due_at_ms, bucket_ms \\ 60_000)

  def cold_due_bucket_ms(due_at_ms, bucket_ms)
      when is_integer(due_at_ms) and due_at_ms >= 0 and is_integer(bucket_ms) and bucket_ms > 0 do
    div(due_at_ms, bucket_ms) * bucket_ms
  end

  def cold_due_key(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)
    due_at_ms = Map.fetch!(attrs, :due_at_ms)
    bucket_ms = Map.get(attrs, :bucket_ms, cold_due_bucket_ms(due_at_ms))

    [
      "flow:due:v1",
      encode_u64(bucket_ms),
      escape_key_part(Map.fetch!(attrs, :type)),
      escape_key_part(Map.fetch!(attrs, :state)),
      escape_key_part(Map.get(attrs, :partition_key, "")),
      encode_i64(Map.get(attrs, :priority, 0)),
      encode_u64(due_at_ms),
      escape_key_part(Map.fetch!(attrs, :flow_id)),
      encode_u64(Map.fetch!(attrs, :version))
    ]
    |> Enum.join(":")
  end

  def cold_due_bucket_prefix(bucket_ms) when is_integer(bucket_ms) and bucket_ms >= 0 do
    "flow:due:v1:" <> encode_u64(bucket_ms)
  end

  def cold_due_type_bucket_prefix(bucket_ms, type)
      when is_integer(bucket_ms) and bucket_ms >= 0 and is_binary(type) do
    ["flow:due:v1", encode_u64(bucket_ms), escape_key_part(type)]
    |> Enum.join(":")
    |> Kernel.<>(":")
  end

  def cold_due_claim_prefix(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    [
      "flow:due:v1",
      encode_u64(Map.fetch!(attrs, :bucket_ms)),
      escape_key_part(Map.fetch!(attrs, :type)),
      escape_key_part(Map.fetch!(attrs, :state)),
      escape_key_part(Map.get(attrs, :partition_key, "")),
      encode_i64(Map.get(attrs, :priority, 0))
    ]
    |> Enum.join(":")
    |> Kernel.<>(":")
  end

  def cold_by_segment_key(%Locator{} = locator) do
    cold_by_segment_prefix(locator.file_id) <>
      ":" <>
      Enum.join(
        [
          encode_u64(locator.offset),
          escape_key_part(locator.flow_id),
          encode_u64(locator.version)
        ],
        ":"
      )
  end

  def cold_by_segment_prefix(file_id) do
    ["flow:cold:by-segment:v1", escape_key_part(:erlang.term_to_binary(file_id))]
    |> Enum.join(":")
  end

  def encode_cold_park(%Locator{kind: :state} = locator, attrs)
      when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    :erlang.term_to_binary(
      {:flow_cold_park, 1,
       %{
         locator: locator,
         due_at_ms: Map.get(attrs, :due_at_ms),
         type: Map.get(attrs, :type),
         state: Map.get(attrs, :state),
         partition_key: Map.get(attrs, :partition_key),
         state_key: Map.get(attrs, :state_key),
         priority: Map.get(attrs, :priority, 0),
         lease_until_ms: Map.get(attrs, :lease_until_ms),
         fencing_token: Map.get(attrs, :fencing_token),
         retention_at_ms: Map.get(attrs, :retention_at_ms),
         value_refs_digest: Map.get(attrs, :value_refs_digest),
         state_value: Map.get(attrs, :state_value),
         checksum: Map.get(attrs, :checksum)
       }}
    )
  end

  def decode_cold_park(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {:flow_cold_park, 1, %{locator: %Locator{} = locator} = fields} ->
        if Locator.valid?(locator), do: {:ok, fields}, else: :error

      _other ->
        :not_cold_park
    end
  rescue
    _ -> :error
  end

  def encode_cold_value_locator(
        value_ref,
        owner_flow_id,
        owner_version,
        %Locator{kind: :value} = locator,
        attrs \\ []
      )
      when is_binary(value_ref) and is_binary(owner_flow_id) and is_integer(owner_version) and
             owner_version >= 0 and (is_map(attrs) or is_list(attrs)) do
    attrs = Map.new(attrs)

    :erlang.term_to_binary(
      {:flow_cold_value_locator, 1,
       %{
         value_ref: value_ref,
         owner_flow_id: owner_flow_id,
         owner_version: owner_version,
         locator: locator,
         ref_kind: Map.get(attrs, :ref_kind),
         expire_at_ms: Map.get(attrs, :expire_at_ms),
         checksum: Map.get(attrs, :checksum)
       }}
    )
  end

  def decode_cold_value_locator(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {:flow_cold_value_locator, 1, %{locator: %Locator{} = locator} = fields} ->
        if Locator.valid?(locator), do: {:ok, fields}, else: :error

      _other ->
        :not_cold_value_locator
    end
  rescue
    _ -> :error
  end

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

  def terminal_index_prefix(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-index:" <> state_index_key <> <<0>>
  end

  def terminal_index_global_prefix, do: "flow-terminal-index:"

  def terminal_index_key(state_index_key, id, updated_at_ms)
      when is_binary(state_index_key) and is_binary(id) and is_integer(updated_at_ms) do
    terminal_index_prefix(state_index_key) <>
      pad_u64(updated_at_ms) <> <<0>> <> id
  end

  def terminal_count_key(state_index_key) when is_binary(state_index_key) do
    "flow-terminal-count:" <> state_index_key
  end

  def terminal_count_prefix, do: "flow-terminal-count:"

  def terminal_expire_prefix, do: "flow-terminal-expire:"

  def terminal_expire_key(expire_at_ms, terminal_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(terminal_key) do
    terminal_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> terminal_key
  end

  def terminal_expire_key(_expire_at_ms, _terminal_key), do: nil

  def terminal_by_state_key_key(state_key) when is_binary(state_key) do
    "flow-terminal-by-state:" <> state_key
  end

  def terminal_by_state_global_prefix, do: "flow-terminal-by-state:"

  def active_index_global_prefix, do: "flow-active-index:"

  def active_index_prefix(index_key) when is_binary(index_key) do
    active_index_global_prefix() <> index_key <> <<0>>
  end

  def active_index_key(index_key, id, score)
      when is_binary(index_key) and is_binary(id) and is_integer(score) do
    active_index_prefix(index_key) <> pad_u64(score) <> <<0>> <> id
  end

  def active_by_state_key_key(state_key) when is_binary(state_key) do
    "flow-active-by-state:" <> state_key
  end

  def active_by_state_global_prefix, do: "flow-active-by-state:"

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

  def query_index_prefix(index_key) when is_binary(index_key) do
    "flow-query-index:" <> index_key <> <<0>>
  end

  def query_index_key(index_key, id, updated_at_ms)
      when is_binary(index_key) and is_binary(id) and is_integer(updated_at_ms) do
    query_index_prefix(index_key) <> pad_u64(updated_at_ms) <> <<0>> <> id
  end

  def query_index_key(index_key, id, updated_at_ms)
      when is_binary(index_key) and is_binary(id) and is_float(updated_at_ms) do
    query_index_key(index_key, id, trunc(updated_at_ms))
  end

  def query_index_key(index_key, id, updated_at_ms)
      when is_binary(index_key) and is_binary(id) and is_binary(updated_at_ms) do
    score =
      case Integer.parse(updated_at_ms) do
        {int, ""} ->
          int

        _ ->
          case Float.parse(updated_at_ms) do
            {float, _rest} -> trunc(float)
            :error -> 0
          end
      end

    query_index_key(index_key, id, score)
  end

  def history_index_prefix(history_key) when is_binary(history_key) do
    "flow-history-index:" <> history_key <> <<0>>
  end

  def history_index_key(history_key, event_id, event_ms)
      when is_binary(history_key) and is_binary(event_id) and is_integer(event_ms) do
    history_index_prefix(history_key) <> pad_u64(event_ms) <> <<0>> <> event_id
  end

  def history_expire_prefix, do: "flow-history-expire:"

  def history_flow_expire_prefix, do: "flow-history-flow-expire:"

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

  def history_expire_key(expire_at_ms, history_index_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(history_index_key) do
    history_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> history_index_key
  end

  def history_expire_key(_expire_at_ms, _history_index_key), do: nil

  def history_flow_expire_key(expire_at_ms, history_key)
      when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(history_key) do
    history_flow_expire_prefix() <> pad_u64(expire_at_ms) <> <<0>> <> history_key
  end

  def history_flow_expire_key(_expire_at_ms, _history_key), do: nil

  def encode_history_index_value(event_id, event_ms, compound_key, expire_at_ms \\ 0)
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) do
    :erlang.term_to_binary({event_id, event_ms, expire_at_ms, compound_key})
  end

  def encode_history_index_value(
        event_id,
        event_ms,
        compound_key,
        expire_at_ms,
        {:flow_history, file_id},
        offset,
        value_size
      )
      when is_binary(event_id) and is_integer(event_ms) and is_binary(compound_key) and
             is_integer(expire_at_ms) and is_integer(file_id) and file_id >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 do
    :erlang.term_to_binary(
      {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
       value_size}
    )
  end

  def encode_history_expire_value(history_index_key) when is_binary(history_index_key) do
    :erlang.term_to_binary(history_index_key)
  end

  def encode_history_flow_expire_value(history_key, expire_at_ms)
      when is_binary(history_key) and is_integer(expire_at_ms) do
    :erlang.term_to_binary({history_key, expire_at_ms})
  end

  def decode_history_expire_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      history_index_key when is_binary(history_index_key) -> {:ok, history_index_key}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def decode_history_flow_expire_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {history_key, expire_at_ms} when is_binary(history_key) and is_integer(expire_at_ms) ->
        {:ok, {history_key, expire_at_ms}}

      history_key when is_binary(history_key) ->
        {:ok, {history_key, :infinity}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_query_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil) do
    :erlang.term_to_binary({id, normalize_ms(updated_at_ms), expire_at_ms, state_key})
  end

  def encode_active_index_value(index_key, id, score, expire_at_ms, state_key)
      when is_binary(index_key) and is_binary(id) and is_integer(score) and
             is_integer(expire_at_ms) and is_binary(state_key) do
    :erlang.term_to_binary({index_key, id, score, expire_at_ms, state_key})
  end

  def decode_active_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {index_key, id, score, expire_at_ms, state_key}
      when is_binary(index_key) and is_binary(id) and is_integer(score) and
             is_integer(expire_at_ms) and is_binary(state_key) ->
        {:ok, {index_key, id, score, expire_at_ms, state_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_active_index_reverse_value(active_keys) when is_list(active_keys) do
    active_keys =
      Enum.filter(active_keys, fn
        active_key when is_binary(active_key) -> true
        _ -> false
      end)

    :erlang.term_to_binary(active_keys)
  end

  def decode_active_index_reverse_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      active_keys when is_list(active_keys) ->
        active_keys =
          Enum.filter(active_keys, fn
            active_key when is_binary(active_key) -> true
            _ -> false
          end)

        {:ok, active_keys}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

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

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms \\ 0, state_key \\ nil) do
    :erlang.term_to_binary({id, updated_at_ms, expire_at_ms, state_key})
  end

  def encode_terminal_index_value(id, updated_at_ms, expire_at_ms, state_key, count_key)
      when is_binary(count_key) do
    :erlang.term_to_binary({id, updated_at_ms, expire_at_ms, state_key, count_key})
  end

  def encode_terminal_expire_value(terminal_key, state_key, count_key)
      when is_binary(terminal_key) and is_binary(count_key) do
    :erlang.term_to_binary({terminal_key, state_key, count_key})
  end

  def decode_terminal_expire_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {terminal_key, state_key, count_key}
      when is_binary(terminal_key) and (is_binary(state_key) or is_nil(state_key)) and
             is_binary(count_key) ->
        {:ok, {terminal_key, state_key, count_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def encode_count(count) when is_integer(count) and count >= 0 do
    :erlang.term_to_binary(count)
  end

  def decode_count(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      count when is_integer(count) and count >= 0 -> {:ok, count}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  def terminal_count(path, state_index_key) when is_binary(path) and is_binary(state_index_key) do
    count_key = terminal_count_key(state_index_key)

    case cached_terminal_count_key(path, count_key) do
      {:ok, count} -> {:ok, count}
      :miss -> terminal_count_key_uncached(path, count_key)
    end
  end

  def terminal_counts(path, state_index_keys)
      when is_binary(path) and is_list(state_index_keys) do
    count_keys = Enum.map(state_index_keys, &terminal_count_key/1)

    if count_keys == [] do
      {:ok, []}
    else
      {cached, missing} =
        count_keys
        |> Enum.with_index()
        |> Enum.reduce({%{}, []}, fn {count_key, index}, {cached_acc, missing_acc} ->
          case cached_terminal_count_key(path, count_key) do
            {:ok, count} -> {Map.put(cached_acc, index, count), missing_acc}
            :miss -> {cached_acc, [{index, count_key} | missing_acc]}
          end
        end)

      missing = Enum.reverse(missing)

      with {:ok, fetched} <- terminal_count_keys_uncached(path, Enum.map(missing, &elem(&1, 1))) do
        counts =
          missing
          |> Enum.zip(fetched)
          |> Enum.reduce(cached, fn
            {{index, count_key}, {:cache, count}}, acc ->
              put_cached_terminal_count_key(path, count_key, count)
              Map.put(acc, index, count)

            {{index, _count_key}, :missing}, acc ->
              Map.put(acc, index, 0)
          end)

        {:ok, Enum.map(0..(length(count_keys) - 1)//1, &Map.fetch!(counts, &1))}
      end
    end
  end

  def refresh_terminal_count_key(path, count_key) when is_binary(path) and is_binary(count_key) do
    case terminal_count_key_uncached(path, count_key) do
      {:ok, count} ->
        put_cached_terminal_count_key(path, count_key, count)
        {:ok, count}

      :not_found ->
        delete_cached_terminal_count_key(path, count_key)
        :not_found

      {:error, _reason} = error ->
        error
    end
  end

  def put_cached_terminal_count_key(path, count_key, count)
      when is_binary(path) and is_binary(count_key) and is_integer(count) and count >= 0 do
    ensure_terminal_count_cache()
    :ets.insert(@terminal_count_cache, {{path, count_key}, count})
    :ok
  end

  def delete_cached_terminal_count_key(path, count_key)
      when is_binary(path) and is_binary(count_key) do
    ensure_terminal_count_cache()
    :ets.delete(@terminal_count_cache, {path, count_key})
    :ok
  end

  def clear_cached_terminal_counts_for_path(path) when is_binary(path) do
    ensure_terminal_count_cache()
    :ets.match_delete(@terminal_count_cache, {{path, :_}, :_})
    :ok
  end

  defp terminal_count_key_uncached(path, count_key) do
    case get(path, count_key) do
      {:ok, blob} ->
        case decode_count(blob) do
          {:ok, count} -> {:ok, count}
          :error -> :not_found
        end

      :not_found ->
        :not_found

      {:error, _reason} = error ->
        error
    end
  end

  defp terminal_count_keys_uncached(_path, []), do: {:ok, []}

  defp terminal_count_keys_uncached(path, count_keys) do
    case get_many(path, count_keys) do
      {:ok, results} ->
        counts =
          Enum.map(results, fn
            {:ok, blob} ->
              case decode_count(blob) do
                {:ok, count} -> {:cache, count}
                :error -> :missing
              end

            :not_found ->
              :missing
          end)

        {:ok, counts}

      {:error, _reason} = error ->
        error
    end
  end

  defp cached_terminal_count_key(path, count_key) do
    ensure_terminal_count_cache()

    case :ets.lookup(@terminal_count_cache, {path, count_key}) do
      [{{^path, ^count_key}, count}] when is_integer(count) and count >= 0 -> {:ok, count}
      _ -> :miss
    end
  end

  defp ensure_terminal_count_cache do
    case :ets.whereis(@terminal_count_cache) do
      :undefined ->
        try do
          :ets.new(@terminal_count_cache, [
            :named_table,
            :public,
            :set,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> @terminal_count_cache
        end

      _table ->
        @terminal_count_cache
    end
  end

  def put_terminal_count(path, state_index_key, count)
      when is_binary(path) and is_binary(state_index_key) and is_integer(count) and count >= 0 do
    count_key = terminal_count_key(state_index_key)

    case write_batch(path, [{:put, count_key, encode_count(count)}]) do
      :ok ->
        put_cached_terminal_count_key(path, count_key, count)
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  def sweep_expired_terminal(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <- prefix_entries(path, terminal_expire_prefix(), limit) do
      {ops, counts, swept} = expired_terminal_sweep_ops(path, entries, now_ms)

      count_ops =
        Enum.map(counts, fn {count_key, count} -> {:put, count_key, encode_count(count)} end)

      case write_batch(path, count_ops ++ ops) do
        :ok ->
          Enum.each(counts, fn {count_key, count} ->
            put_cached_terminal_count_key(path, count_key, count)
          end)

          {:ok, swept}

        {:error, _reason} = error ->
          error
      end
    end
  end

  def sweep_expired_terminal(_path, _now_ms, _limit), do: {:ok, 0}

  def expired_terminal_state_keys(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <- prefix_entries(path, terminal_expire_prefix(), limit) do
      keys =
        Enum.reduce_while(entries, [], fn {expire_key, expire_value}, acc ->
          case terminal_expire_key_time(expire_key) do
            {:ok, expire_at_ms} when expire_at_ms > now_ms ->
              {:halt, acc}

            {:ok, _expire_at_ms} ->
              case expired_terminal_state_key(path, expire_value, now_ms) do
                state_key when is_binary(state_key) -> {:cont, [state_key | acc]}
                _ -> {:cont, acc}
              end

            :error ->
              {:cont, acc}
          end
        end)

      {:ok, keys |> Enum.reverse() |> Enum.uniq()}
    end
  end

  def expired_terminal_state_keys(_path, _now_ms, _limit), do: {:ok, []}

  def sweep_expired_history(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <- prefix_entries(path, history_expire_prefix(), limit),
         {:ok, flow_entries} <- prefix_entries(path, history_flow_expire_prefix(), limit) do
      {ops, swept} = expired_history_sweep_ops(path, entries, now_ms)
      {flow_ops, flow_swept} = expired_history_flow_sweep_ops(path, flow_entries, now_ms, limit)

      case write_batch(path, flow_ops ++ ops) do
        :ok -> {:ok, swept + flow_swept}
        {:error, _reason} = error -> error
      end
    end
  end

  def sweep_expired_history(_path, _now_ms, _limit), do: {:ok, 0}

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

  def decode_terminal_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {id, updated_at_ms, expire_at_ms, state_key, count_key}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) and
             (is_binary(state_key) or is_nil(state_key)) and is_binary(count_key) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      {id, updated_at_ms, expire_at_ms, state_key}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) and
             (is_binary(state_key) or is_nil(state_key)) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      {id, updated_at_ms, expire_at_ms}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) ->
        {:ok, {id, updated_at_ms, expire_at_ms, nil}}

      {id, updated_at_ms} when is_binary(id) and is_integer(updated_at_ms) ->
        {:ok, {id, updated_at_ms, 0, nil}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_query_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {id, updated_at_ms, expire_at_ms, state_key}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) and
             (is_binary(state_key) or is_nil(state_key)) ->
        {:ok, {id, updated_at_ms, expire_at_ms, state_key}}

      {id, updated_at_ms, expire_at_ms}
      when is_binary(id) and is_integer(updated_at_ms) and is_integer(expire_at_ms) ->
        {:ok, {id, updated_at_ms, expire_at_ms, nil}}

      {id, updated_at_ms}
      when is_binary(id) and is_integer(updated_at_ms) ->
        {:ok, {id, updated_at_ms, 0, nil}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_history_index_value(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
       value_size}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) and is_integer(file_id) and file_id >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key}}

      {event_id, event_ms, expire_at_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key}}

      {event_id, event_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_binary(compound_key) ->
        {:ok, {event_id, event_ms, 0, compound_key}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_history_index_location(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
       value_size}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) and is_integer(file_id) and file_id >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 ->
        {:ok,
         {event_id, event_ms, expire_at_ms, compound_key, {:flow_history, file_id}, offset,
          value_size}}

      {event_id, event_ms, expire_at_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_integer(expire_at_ms) and
             is_binary(compound_key) ->
        {:ok, {event_id, event_ms, expire_at_ms, compound_key, nil, nil, nil}}

      {event_id, event_ms, compound_key}
      when is_binary(event_id) and is_integer(event_ms) and is_binary(compound_key) ->
        {:ok, {event_id, event_ms, 0, compound_key, nil, nil, nil}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

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

  def terminal_index_count_key(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {_id, _updated_at_ms, _expire_at_ms, _state_key, count_key} when is_binary(count_key) ->
        {:ok, count_key}

      _ ->
        :missing
    end
  rescue
    _ -> :missing
  end

  defp terminal_count_key_for_index_entry(path, terminal_key) do
    case get(path, terminal_key) do
      {:ok, terminal_value} -> terminal_index_count_key(terminal_value)
      _ -> :missing
    end
  end

  defp refresh_terminal_count_cache_after_delete(path, {:ok, count_key}) do
    case terminal_count_key_uncached(path, count_key) do
      {:ok, count} -> put_cached_terminal_count_key(path, count_key, count)
      :not_found -> put_cached_terminal_count_key(path, count_key, 0)
      {:error, _reason} -> delete_cached_terminal_count_key(path, count_key)
    end
  end

  defp refresh_terminal_count_cache_after_delete(_path, :missing), do: :ok

  defp expired_terminal_sweep_ops(path, entries, now_ms) do
    Enum.reduce_while(entries, {[], %{}, 0}, fn {expire_key, expire_value},
                                                {ops, counts, swept} ->
      case terminal_expire_key_time(expire_key) do
        {:ok, expire_at_ms} when expire_at_ms > now_ms ->
          {:halt, {ops, counts, swept}}

        {:ok, _expire_at_ms} ->
          {entry_ops, counts, entry_swept} =
            expired_terminal_entry_ops(path, expire_key, expire_value, counts, now_ms)

          {:cont, {entry_ops ++ ops, counts, swept + entry_swept}}

        :error ->
          {:cont, {[{:delete, expire_key} | ops], counts, swept}}
      end
    end)
  end

  defp expired_history_sweep_ops(path, entries, now_ms) do
    Enum.reduce_while(entries, {[], 0}, fn {expire_key, expire_value}, {ops, swept} ->
      case expire_key_time(expire_key, history_expire_prefix()) do
        {:ok, expire_at_ms} when expire_at_ms > now_ms ->
          {:halt, {ops, swept}}

        {:ok, _expire_at_ms} ->
          {entry_ops, entry_swept} =
            expired_history_entry_ops(path, expire_key, expire_value, now_ms)

          {:cont, {entry_ops ++ ops, swept + entry_swept}}

        :error ->
          {:cont, {[{:delete, expire_key} | ops], swept}}
      end
    end)
  end

  defp expired_history_flow_sweep_ops(path, entries, now_ms, limit) do
    Enum.reduce_while(entries, {[], 0}, fn {expire_key, expire_value}, {ops, swept} ->
      case expire_key_time(expire_key, history_flow_expire_prefix()) do
        {:ok, expire_at_ms} when expire_at_ms > now_ms ->
          {:halt, {ops, swept}}

        {:ok, _expire_at_ms} ->
          {entry_ops, entry_swept} =
            expired_history_flow_entry_ops(path, expire_key, expire_value, now_ms, limit)

          {:cont, {entry_ops ++ ops, swept + entry_swept}}

        :error ->
          {:cont, {[{:delete, expire_key} | ops], swept}}
      end
    end)
  end

  defp expired_history_flow_entry_ops(path, expire_key, expire_value, now_ms, limit) do
    case decode_history_flow_expire_value(expire_value) do
      {:ok, {history_key, history_cutoff_ms}} ->
        prefix = history_index_prefix(history_key)
        read_limit = max(limit, 1) + 1

        case prefix_entries(path, prefix, read_limit) do
          {:ok, entries} ->
            {entries, keep_marker?} =
              if length(entries) > limit do
                {Enum.take(entries, limit), true}
              else
                {entries, false}
              end

            base_ops = if keep_marker?, do: [], else: [{:delete, expire_key}]

            {ops, swept} =
              Enum.reduce(entries, {base_ops, 0}, fn {history_index_key, history_value},
                                                     {ops_acc, swept_acc} ->
                case decode_history_index_value(history_value) do
                  {:ok, {_event_id, event_ms, expire_at_ms, _compound_key}}
                  when expire_at_ms <= 0 or expire_at_ms <= now_ms ->
                    if history_event_before_cutoff?(event_ms, history_cutoff_ms) do
                      {history_index_delete_ops_from_value(
                         [{:delete, history_index_key} | ops_acc],
                         history_index_key,
                         history_value
                       ), swept_acc + 1}
                    else
                      {ops_acc, swept_acc}
                    end

                  {:ok, {_event_id, event_ms, _expire_at_ms, _compound_key}} ->
                    if history_event_before_cutoff?(event_ms, history_cutoff_ms) do
                      {history_index_delete_ops_from_value(
                         [{:delete, history_index_key} | ops_acc],
                         history_index_key,
                         history_value
                       ), swept_acc + 1}
                    else
                      {ops_acc, swept_acc}
                    end

                  _ ->
                    {ops_acc, swept_acc}
                end
              end)

            {ops, swept}

          {:error, _reason} ->
            {[], 0}
        end

      :error ->
        {[{:delete, expire_key}], 0}
    end
  end

  defp history_event_before_cutoff?(event_ms, :infinity) when is_integer(event_ms), do: true

  defp history_event_before_cutoff?(event_ms, cutoff_ms)
       when is_integer(event_ms) and is_integer(cutoff_ms),
       do: event_ms <= cutoff_ms

  defp history_event_before_cutoff?(_event_ms, _cutoff_ms), do: false

  defp expired_history_entry_ops(path, expire_key, expire_value, now_ms) do
    case decode_history_expire_value(expire_value) do
      {:ok, history_index_key} ->
        case get(path, history_index_key) do
          {:ok, history_value} ->
            expired_live_history_ops(expire_key, history_index_key, history_value, now_ms)

          :not_found ->
            {[{:delete, expire_key}], 0}

          {:error, _reason} ->
            {[], 0}
        end

      :error ->
        {[{:delete, expire_key}], 0}
    end
  end

  defp expired_live_history_ops(expire_key, history_index_key, history_value, now_ms) do
    case decode_history_index_value(history_value) do
      {:ok, {_event_id, _event_ms, expire_at_ms, _compound_key}}
      when expire_at_ms > 0 and expire_at_ms <= now_ms ->
        {[{:delete, expire_key}, {:delete, history_index_key}], 1}

      _ ->
        {[{:delete, expire_key}], 0}
    end
  end

  defp expired_terminal_entry_ops(path, expire_key, expire_value, counts, now_ms) do
    case decode_terminal_expire_value(expire_value) do
      {:ok, {terminal_key, state_key, count_key}} ->
        case get(path, terminal_key) do
          {:ok, terminal_value} ->
            expired_live_terminal_ops(
              path,
              expire_key,
              terminal_key,
              terminal_value,
              state_key,
              count_key,
              counts,
              now_ms
            )

          :not_found ->
            expired_missing_terminal_ops(path, expire_key, state_key, counts)

          {:error, _reason} ->
            {[], counts, 0}
        end

      :error ->
        {[{:delete, expire_key}], counts, 0}
    end
  end

  defp expired_terminal_state_key(path, expire_value, now_ms) do
    with {:ok, {terminal_key, state_key, _count_key}} <-
           decode_terminal_expire_value(expire_value),
         {:ok, terminal_value} <- get(path, terminal_key),
         {:ok, {_id, _updated_at_ms, expire_at_ms, decoded_state_key}} <-
           decode_terminal_index_value(terminal_value),
         true <- expire_at_ms > 0 and expire_at_ms <= now_ms do
      decoded_state_key || state_key
    else
      :not_found ->
        case decode_terminal_expire_value(expire_value) do
          {:ok, {_terminal_key, state_key, _count_key}} -> state_key
          :error -> nil
        end

      _ ->
        nil
    end
  end

  defp expired_live_terminal_ops(
         path,
         expire_key,
         terminal_key,
         terminal_value,
         state_key,
         count_key,
         counts,
         now_ms
       ) do
    case decode_terminal_index_value(terminal_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, decoded_state_key}}
      when expire_at_ms > 0 and expire_at_ms <= now_ms ->
        state_key = decoded_state_key || state_key
        reverse_key = if is_binary(state_key), do: terminal_by_state_key_key(state_key), else: nil
        current_count = Map.get_lazy(counts, count_key, fn -> read_count_key(path, count_key) end)
        count = max(current_count - 1, 0)

        ops =
          [{:delete, terminal_key}]
          |> maybe_delete_key(reverse_key)
          |> maybe_delete_expire_key(path, expire_key, state_key)

        {ops, Map.put(counts, count_key, count), 1}

      _ ->
        {[{:delete, expire_key}], counts, 0}
    end
  end

  defp expired_missing_terminal_ops(_path, expire_key, state_key, counts)
       when not is_binary(state_key) do
    {[{:delete, expire_key}], counts, 0}
  end

  defp expired_missing_terminal_ops(path, expire_key, state_key, counts) do
    case get(path, state_key) do
      {:ok, _state_value} ->
        {[], counts, 0}

      :not_found ->
        {[{:delete, expire_key}], counts, 0}

      {:error, _reason} ->
        {[], counts, 0}
    end
  end

  defp terminal_expire_key_time(key) do
    expire_key_time(key, terminal_expire_prefix())
  end

  defp maybe_delete_expire_key(ops, path, expire_key, state_key)
       when is_binary(expire_key) and is_binary(state_key) do
    case get(path, state_key) do
      :not_found -> [{:delete, expire_key} | ops]
      {:ok, _state_value} -> ops
      {:error, _reason} -> ops
    end
  end

  defp maybe_delete_expire_key(ops, _path, expire_key, _state_key) when is_binary(expire_key),
    do: [{:delete, expire_key} | ops]

  defp maybe_delete_expire_key(ops, _path, _expire_key, _state_key), do: ops

  defp expire_key_time(key, prefix) do
    size = byte_size(prefix)

    if byte_size(key) > size + 21 and binary_part(key, 0, size) == prefix do
      digits = binary_part(key, size, 20)

      case Integer.parse(digits) do
        {value, ""} -> {:ok, value}
        _ -> :error
      end
    else
      :error
    end
  end

  defp maybe_delete_key(ops, key) when is_binary(key), do: [{:delete, key} | ops]
  defp maybe_delete_key(ops, _key), do: ops

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

  defp pad_u64(value) do
    encoded = Integer.to_string(value)

    case byte_size(encoded) do
      size when size < 20 -> binary_part(@u64_decimal_zero_pad, 0, 20 - size) <> encoded
      _size -> encoded
    end
  end

  defp encode_u64(value) when is_integer(value) and value >= 0, do: pad_u64(value)

  defp encode_i64(value) when is_integer(value) do
    value
    |> Kernel.+(9_223_372_036_854_775_808)
    |> encode_u64()
  end

  defp escape_key_part(value) when is_binary(value), do: Base.url_encode64(value, padding: false)

  defp escape_key_part(value) when is_atom(value),
    do: value |> Atom.to_string() |> escape_key_part()

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
