defmodule Ferricstore.Raft.WARaftSegmentReader do
  @moduledoc false

  @table_prefix "raft_log_ferricstore_waraft_backend_"
  @storage_root "ferricstore_waraft_backend"
  @projection_dir "segment_projection_log"
  @apply_projection_dir "apply_projection_log"
  @apply_projection_table :ferricstore_waraft_apply_projection_cache

  @spec put_apply_projection(binary(), non_neg_integer(), pos_integer(), [
          {binary(), binary(), non_neg_integer()}
        ]) ::
          :ok
  def put_apply_projection(data_dir, shard_index, index, entries)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(index) and index > 0 and is_list(entries) do
    table = ensure_apply_projection_table!()
    root = storage_root(%{data_dir: data_dir}, shard_index)

    Enum.each(entries, fn
      {key, value, expire_at_ms}
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
        :ets.insert(table, {{root, index, key}, value, expire_at_ms})

      _invalid ->
        :ok
    end)

    :ok
  end

  @spec read_value(FerricStore.Instance.t(), non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def read_value(ctx, shard_index, index, key)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(index) and index > 0 and
             is_binary(key) do
    case read_main_log_value(ctx, shard_index, index, key) do
      {:error, :segment_entry_not_found} ->
        :not_found

      {:error, :key_not_in_segment_entry} ->
        :not_found

      other ->
        other
    end
  end

  def read_value(_ctx, _shard_index, _index, _key), do: {:error, :bad_segment_location}

  @spec read_value_from_location(FerricStore.Instance.t(), non_neg_integer(), term(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def read_value_from_location(ctx, shard_index, {:waraft_segment, index}, key),
    do: read_value(ctx, shard_index, index, key)

  def read_value_from_location(ctx, shard_index, {:waraft_projection, index}, key),
    do: read_projection_value_at(ctx, shard_index, index, key)

  def read_value_from_location(ctx, shard_index, {:waraft_apply_projection, index}, key),
    do: read_apply_projection_value_at(ctx, shard_index, index, key)

  def read_value_from_location(_ctx, _shard_index, _file_id, _key),
    do: {:error, :not_waraft_segment_location}

  defp read_main_log_value(ctx, shard_index, index, key) do
    with {:ok, entry} <- read_main_log_entry(ctx, shard_index, index) do
      case value_from_entry(entry, key) do
        {:ok, value} -> {:ok, value}
        :deleted -> :not_found
        :not_found -> {:error, :key_not_in_segment_entry}
        {:error, _reason} = error -> error
      end
    end
  end

  defp read_main_log_entry(ctx, shard_index, index) do
    table = log_table(shard_index)

    case ets_log_lookup(table, index) do
      {:ok, entry} ->
        {:ok, entry}

      :not_found ->
        read_main_log_entry_from_disk(ctx, shard_index, index)
    end
  end

  defp ets_log_lookup(table, index) do
    case :ets.info(table) do
      :undefined ->
        :not_found

      _info ->
        case :ets.lookup(table, index) do
          [{^index, entry}] -> {:ok, entry}
          [] -> :not_found
        end
    end
  rescue
    ArgumentError -> :not_found
  end

  defp read_main_log_entry_from_disk(ctx, shard_index, wanted_index) do
    root = storage_root(ctx, shard_index)

    root_chars = to_charlist(root)

    case :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, wanted_index) do
      {:ok, {_ordinal, offset, encoded_size}} ->
        read_main_log_entry_from_disk_at(root_chars, wanted_index, offset, encoded_size)

      :not_found ->
        {:error, :segment_entry_not_found}

      {:error, _reason} = error ->
        error
    end
  end

  defp read_main_log_entry_from_disk_at(root, wanted_index, offset, encoded_size) do
    case :ferricstore_waraft_spike_segment_log.read_disk_at(
           root,
           wanted_index,
           offset,
           encoded_size
         ) do
      {:ok, entry} -> {:ok, entry}
      :not_found -> {:error, :segment_entry_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_projection_value_at(ctx, shard_index, index, key)
       when is_integer(index) and index > 0 and is_binary(key) do
    projection_root = Path.join(storage_root(ctx, shard_index), @projection_dir)
    root_chars = to_charlist(projection_root)

    with {:ok, {_ordinal, offset, encoded_size}} <-
           :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index),
         {:ok, entry} <-
           :ferricstore_waraft_spike_segment_log.read_disk_at(
             root_chars,
             index,
             offset,
             encoded_size
           ) do
      case entry do
        {0, {:ferricstore_segment_projection_entry, ^key, value, _expire_at_ms}}
        when is_binary(value) ->
          {:ok, value}

        {0, {:ferricstore_segment_projection_entry, _other_key, _value, _expire_at_ms}} ->
          :not_found

        _other ->
          {:error, :bad_segment_projection_entry}
      end
    else
      :not_found -> :not_found
      {:error, :enoent} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_projection_value_at(_ctx, _shard_index, _index, _key),
    do: {:error, :bad_segment_projection_location}

  defp read_apply_projection_value_at(ctx, shard_index, index, key)
       when is_integer(index) and index > 0 and is_binary(key) do
    root = storage_root(ctx, shard_index)

    case read_apply_projection_cache(root, index, key) do
      {:ok, value} ->
        {:ok, value}

      :not_found ->
        read_apply_projection_value_from_disk(root, index, key)
    end
  end

  defp read_apply_projection_value_at(_ctx, _shard_index, _index, _key),
    do: {:error, :bad_segment_apply_projection_location}

  defp read_apply_projection_cache(root, index, key) do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        :not_found

      table ->
        case :ets.lookup(table, {root, index, key}) do
          [{{^root, ^index, ^key}, value, expire_at_ms}] ->
            if live_expire_at?(expire_at_ms), do: {:ok, value}, else: :not_found

          [] ->
            :not_found
        end
    end
  rescue
    ArgumentError -> :not_found
  end

  defp read_apply_projection_value_from_disk(root, index, key) do
    projection_root = Path.join(root, @apply_projection_dir)
    root_chars = to_charlist(projection_root)

    with {:ok, {_ordinal, offset, encoded_size}} <-
           :ferricstore_waraft_spike_segment_log.location_for_index(root_chars, index),
         {:ok, entry} <-
           :ferricstore_waraft_spike_segment_log.read_disk_at(
             root_chars,
             index,
             offset,
             encoded_size
           ) do
      case entry do
        {0, {:ferricstore_segment_apply_projection_batch, _position, entries}}
        when is_list(entries) ->
          value_from_projection_entries(entries, key)

        _other ->
          {:error, :bad_segment_apply_projection_entry}
      end
    else
      :not_found -> :not_found
      {:error, :enoent} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  defp value_from_projection_entries(entries, key) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms}, _acc when is_binary(value) -> {:ok, value}
      _entry, acc -> acc
    end)
  end

  defp ensure_apply_projection_table! do
    case :ets.whereis(@apply_projection_table) do
      :undefined ->
        try do
          :ets.new(@apply_projection_table, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])
        rescue
          ArgumentError -> @apply_projection_table
        end

      table ->
        table
    end
  end

  defp live_expire_at?(0), do: true
  defp live_expire_at?(expire_at_ms) when is_integer(expire_at_ms), do: expire_at_ms > now_ms()
  defp live_expire_at?(_expire_at_ms), do: false

  defp now_ms, do: System.system_time(:millisecond)

  defp value_from_entry(entry, key) do
    case command_from_entry(entry) do
      {:ok, command} -> value_from_command(decode_replay_command(command), key)
      :skip -> :not_found
    end
  end

  defp command_from_entry({_term, {:default, {corr, command}}}) when is_reference(corr),
    do: {:ok, command}

  defp command_from_entry({_term, {corr, command}}) when is_reference(corr),
    do: {:ok, command}

  defp command_from_entry({_term, command}) when is_tuple(command), do: {:ok, command}
  defp command_from_entry(_entry), do: :skip

  defp decode_replay_command({:ttb, binary}) when is_binary(binary) do
    try do
      binary
      |> :erlang.binary_to_term([:safe])
      |> decode_replay_command()
    rescue
      _ -> {:ttb, binary}
    end
  end

  defp decode_replay_command({inner_command, %{hlc_ts: {physical_ms, logical}}})
       when is_tuple(inner_command) and is_integer(physical_ms) and is_integer(logical) do
    decode_replay_command(inner_command)
  end

  defp decode_replay_command(command), do: command

  defp value_from_command({:put, key, value, _expire_at_ms}, key) when is_binary(value),
    do: {:ok, value}

  defp value_from_command({:put_blob_ref, key, encoded_ref, _expire_at_ms}, key)
       when is_binary(encoded_ref),
       do: {:ok, encoded_ref}

  defp value_from_command({:set, key, value, _expire_at_ms, _opts}, key) when is_binary(value),
    do: {:ok, value}

  defp value_from_command({:delete, key}, key), do: :deleted

  defp value_from_command({:compound_put, key, value, _expire_at_ms}, key)
       when is_binary(value),
       do: {:ok, value}

  defp value_from_command({:compound_put_blob_ref, key, encoded_ref, _expire_at_ms}, key)
       when is_binary(encoded_ref),
       do: {:ok, encoded_ref}

  defp value_from_command({:compound_delete, key}, key), do: :deleted

  defp value_from_command({:put_batch, entries}, key) when is_list(entries) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms}, _acc when is_binary(value) -> {:ok, value}
      _entry, acc -> acc
    end)
  end

  defp value_from_command({:put_blob_batch, entries}, key) when is_list(entries) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms, :value}, _acc when is_binary(value) ->
        {:ok, value}

      {^key, encoded_ref, _expire_at_ms, :blob_ref}, _acc when is_binary(encoded_ref) ->
        {:ok, encoded_ref}

      _entry, acc ->
        acc
    end)
  end

  defp value_from_command({:compound_batch_put, _redis_key, entries}, key)
       when is_list(entries) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms}, _acc when is_binary(value) -> {:ok, value}
      _entry, acc -> acc
    end)
  end

  defp value_from_command({:compound_blob_batch_put, _redis_key, entries}, key)
       when is_list(entries) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms, :value}, _acc when is_binary(value) ->
        {:ok, value}

      {^key, encoded_ref, _expire_at_ms, :blob_ref}, _acc when is_binary(encoded_ref) ->
        {:ok, encoded_ref}

      _entry, acc ->
        acc
    end)
  end

  defp value_from_command({:delete_batch, keys}, key) when is_list(keys) do
    if key in keys, do: :deleted, else: :not_found
  end

  defp value_from_command({:compound_batch_delete, _redis_key, keys}, key) when is_list(keys) do
    if key in keys, do: :deleted, else: :not_found
  end

  defp value_from_command({:compound_delete_prefix, prefix}, key) when is_binary(prefix) do
    if String.starts_with?(key, prefix), do: :deleted, else: :not_found
  end

  defp value_from_command({:batch, commands}, key) when is_list(commands) do
    Enum.reduce(commands, :not_found, fn command, acc ->
      case value_from_command(decode_replay_command(command), key) do
        :not_found -> acc
        result -> result
      end
    end)
  end

  defp value_from_command(_command, _key), do: :not_found

  defp storage_root(%{data_dir: data_dir}, shard_index) do
    Path.join([data_dir, "waraft", "#{@storage_root}.#{shard_index + 1}"])
  end

  defp log_table(shard_index), do: String.to_atom("#{@table_prefix}#{shard_index + 1}")
end
