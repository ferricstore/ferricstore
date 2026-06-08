defmodule Ferricstore.Raft.WARaftSegmentReader.CommandValues do
  @moduledoc false

  def live_expire_at?(0), do: true
  def live_expire_at?(expire_at_ms) when is_integer(expire_at_ms), do: expire_at_ms > now_ms()
  def live_expire_at?(_expire_at_ms), do: false

  def now_ms, do: System.system_time(:millisecond)

  def value_from_entry(entry, key) do
    case command_from_entry(entry) do
      {:ok, command} -> value_from_command(decode_replay_command(command), key)
      :skip -> :not_found
    end
  end

  def values_from_entry(entry, keys) do
    case command_from_entry(entry) do
      {:ok, command} -> values_from_command(decode_replay_command(command), keys)
      :skip -> %{}
    end
  end

  def command_from_entry({_term, {:default, {corr, command}}}) when is_reference(corr),
    do: {:ok, command}

  def command_from_entry({_term, {corr, command}}) when is_reference(corr),
    do: {:ok, command}

  def command_from_entry({_term, command}) when is_tuple(command), do: {:ok, command}
  def command_from_entry(_entry), do: :skip

  def decode_replay_command({:ttb, binary}) when is_binary(binary) do
    try do
      binary
      |> :erlang.binary_to_term([:safe])
      |> decode_replay_command()
    rescue
      _ -> {:ttb, binary}
    end
  end

  def decode_replay_command({inner_command, %{hlc_ts: {physical_ms, logical}}})
      when is_tuple(inner_command) and is_integer(physical_ms) and is_integer(logical) do
    decode_replay_command(inner_command)
  end

  def decode_replay_command(command), do: command

  def value_from_command({:put, key, value, _expire_at_ms}, key) when is_binary(value),
    do: {:ok, value}

  def value_from_command({:put_blob_ref, key, encoded_ref, _expire_at_ms}, key)
      when is_binary(encoded_ref),
      do: {:ok, encoded_ref}

  def value_from_command({:set, key, value, _expire_at_ms, _opts}, key) when is_binary(value),
    do: {:ok, value}

  def value_from_command({:delete, key}, key), do: :deleted

  def value_from_command({:compound_put, key, value, _expire_at_ms}, key)
      when is_binary(value),
      do: {:ok, value}

  def value_from_command({:compound_put_blob_ref, key, encoded_ref, _expire_at_ms}, key)
      when is_binary(encoded_ref),
      do: {:ok, encoded_ref}

  def value_from_command({:compound_delete, key}, key), do: :deleted

  def value_from_command({:put_batch, entries}, key) when is_list(entries) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms}, _acc when is_binary(value) -> {:ok, value}
      _entry, acc -> acc
    end)
  end

  def value_from_command({:put_blob_batch, entries}, key) when is_list(entries) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms, :value}, _acc when is_binary(value) ->
        {:ok, value}

      {^key, encoded_ref, _expire_at_ms, :blob_ref}, _acc when is_binary(encoded_ref) ->
        {:ok, encoded_ref}

      _entry, acc ->
        acc
    end)
  end

  def value_from_command({:compound_batch_put, _redis_key, entries}, key)
      when is_list(entries) do
    Enum.reduce(entries, :not_found, fn
      {^key, value, _expire_at_ms}, _acc when is_binary(value) -> {:ok, value}
      _entry, acc -> acc
    end)
  end

  def value_from_command({:compound_blob_batch_put, _redis_key, entries}, key)
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

  def value_from_command({:delete_batch, keys}, key) when is_list(keys) do
    if key in keys, do: :deleted, else: :not_found
  end

  def value_from_command({:compound_batch_delete, _redis_key, keys}, key) when is_list(keys) do
    if key in keys, do: :deleted, else: :not_found
  end

  def value_from_command({:compound_delete_prefix, prefix}, key) when is_binary(prefix) do
    if String.starts_with?(key, prefix), do: :deleted, else: :not_found
  end

  def value_from_command({:batch, commands}, key) when is_list(commands) do
    Enum.reduce(commands, :not_found, fn command, acc ->
      case value_from_command(decode_replay_command(command), key) do
        :not_found -> acc
        result -> result
      end
    end)
  end

  def value_from_command(_command, _key), do: :not_found

  def values_from_command(command, keys) do
    keyset = MapSet.new(keys)
    collect_values_from_command(command, keyset, %{})
  end

  def collect_values_from_command({:put, key, value, _expire_at_ms}, keyset, acc)
      when is_binary(value),
      do: put_requested_value(acc, keyset, key, value)

  def collect_values_from_command({:put_blob_ref, key, encoded_ref, _expire_at_ms}, keyset, acc)
      when is_binary(encoded_ref),
      do: put_requested_value(acc, keyset, key, encoded_ref)

  def collect_values_from_command({:set, key, value, _expire_at_ms, _opts}, keyset, acc)
      when is_binary(value),
      do: put_requested_value(acc, keyset, key, value)

  def collect_values_from_command({:delete, key}, _keyset, acc), do: Map.delete(acc, key)

  def collect_values_from_command({:compound_put, key, value, _expire_at_ms}, keyset, acc)
      when is_binary(value),
      do: put_requested_value(acc, keyset, key, value)

  def collect_values_from_command(
        {:compound_put_blob_ref, key, encoded_ref, _expire_at_ms},
        keyset,
        acc
      )
      when is_binary(encoded_ref),
      do: put_requested_value(acc, keyset, key, encoded_ref)

  def collect_values_from_command({:compound_delete, key}, _keyset, acc),
    do: Map.delete(acc, key)

  def collect_values_from_command({:put_batch, entries}, keyset, acc) when is_list(entries) do
    Enum.reduce(entries, acc, fn
      {key, value, _expire_at_ms}, values when is_binary(value) ->
        put_requested_value(values, keyset, key, value)

      _entry, values ->
        values
    end)
  end

  def collect_values_from_command({:put_blob_batch, entries}, keyset, acc)
      when is_list(entries) do
    Enum.reduce(entries, acc, fn
      {key, value, _expire_at_ms, :value}, values when is_binary(value) ->
        put_requested_value(values, keyset, key, value)

      {key, encoded_ref, _expire_at_ms, :blob_ref}, values when is_binary(encoded_ref) ->
        put_requested_value(values, keyset, key, encoded_ref)

      _entry, values ->
        values
    end)
  end

  def collect_values_from_command({:compound_batch_put, _redis_key, entries}, keyset, acc)
      when is_list(entries) do
    Enum.reduce(entries, acc, fn
      {key, value, _expire_at_ms}, values when is_binary(value) ->
        put_requested_value(values, keyset, key, value)

      _entry, values ->
        values
    end)
  end

  def collect_values_from_command({:compound_blob_batch_put, _redis_key, entries}, keyset, acc)
      when is_list(entries) do
    Enum.reduce(entries, acc, fn
      {key, value, _expire_at_ms, :value}, values when is_binary(value) ->
        put_requested_value(values, keyset, key, value)

      {key, encoded_ref, _expire_at_ms, :blob_ref}, values when is_binary(encoded_ref) ->
        put_requested_value(values, keyset, key, encoded_ref)

      _entry, values ->
        values
    end)
  end

  def collect_values_from_command({:delete_batch, keys}, keyset, acc) when is_list(keys),
    do: delete_requested_keys(acc, keyset, keys)

  def collect_values_from_command({:compound_batch_delete, _redis_key, keys}, keyset, acc)
      when is_list(keys),
      do: delete_requested_keys(acc, keyset, keys)

  def collect_values_from_command({:compound_delete_prefix, prefix}, keyset, acc)
      when is_binary(prefix) do
    Enum.reduce(keyset, acc, fn
      key, values when is_binary(key) ->
        if String.starts_with?(key, prefix), do: Map.delete(values, key), else: values

      _key, values ->
        values
    end)
  end

  def collect_values_from_command({:batch, commands}, keyset, acc) when is_list(commands) do
    Enum.reduce(commands, acc, fn command, values ->
      collect_values_from_command(decode_replay_command(command), keyset, values)
    end)
  end

  def collect_values_from_command(_command, _keyset, acc), do: acc

  def put_requested_value(acc, keyset, key, value) do
    if MapSet.member?(keyset, key), do: Map.put(acc, key, value), else: acc
  end

  def delete_requested_keys(acc, keyset, keys) do
    Enum.reduce(keys, acc, fn key, values ->
      if MapSet.member?(keyset, key), do: Map.delete(values, key), else: values
    end)
  end
end
