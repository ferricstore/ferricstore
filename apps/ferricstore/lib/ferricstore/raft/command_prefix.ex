defmodule Ferricstore.Raft.CommandPrefix do
  @moduledoc false

  alias Ferricstore.Raft.CommandStamp

  @spec extract(tuple()) :: binary()
  def extract({:ttb, binary}) when is_binary(binary) do
    case CommandStamp.decode_ttb(binary) do
      {:ok, command} -> extract(command)
      {:error, :invalid_preencoded_command} -> "_root"
    end
  end

  def extract({:ferricstore_latency_trace, inner}) when is_tuple(inner), do: extract(inner)

  def extract({:ferricstore_apply_context, _encoded, inner}) when is_tuple(inner),
    do: extract(inner)

  def extract({:flow_policy_fence, _installs, inner}) when is_tuple(inner), do: extract(inner)

  def extract({:flow_shared_ref_write, _shard_index, inner}) when is_tuple(inner),
    do: extract(inner)

  def extract({:async, _origin, inner}) when is_tuple(inner), do: extract(inner)

  def extract({inner, %{hlc_ts: {physical_ms, logical}, wall_time_ms: wall_time_ms}})
      when is_tuple(inner) and is_integer(physical_ms) and physical_ms >= 0 and
             is_integer(logical) and logical >= 0 and is_integer(wall_time_ms) and
             wall_time_ms >= 0 and wall_time_ms <= physical_ms,
      do: extract(inner)

  def extract(command) when is_tuple(command) do
    key =
      case command do
        {:put_batch, [{first_key, _value, _expire_at_ms} | _rest]} ->
          first_key

        {operation, [{first_key, _value, _expire_at_ms} | _rest]}
        when operation in [:mset, :msetnx] ->
          first_key

        {operation, [{first_key, _value, _expire_at_ms, _representation} | _rest]}
        when operation in [:mset_blob_batch, :msetnx_blob_batch] ->
          first_key

        {:delete_batch, [first_key | _rest]} ->
          first_key

        {:origin_checked, key, _inner, _before_value, _before_exp, _expected_value, _expire_at_ms} ->
          key

        {:origin_checked, key, _inner, _expected_value, _expire_at_ms} ->
          key

        command when tuple_size(command) >= 2 ->
          elem(command, 1)

        _ ->
          nil
      end

    if is_binary(key) do
      key
      |> Ferricstore.Store.CompoundKey.extract_redis_key()
      |> extract_namespace_prefix()
    else
      "_root"
    end
  end

  defp extract_namespace_prefix(""), do: "_root"

  defp extract_namespace_prefix(key) do
    case :binary.split(key, ":") do
      [^key] -> "_root"
      [prefix | _rest] -> prefix
    end
  end
end
