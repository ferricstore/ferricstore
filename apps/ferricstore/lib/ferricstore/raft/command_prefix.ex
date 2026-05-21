defmodule Ferricstore.Raft.CommandPrefix do
  @moduledoc false

  @spec extract(tuple()) :: binary()
  def extract(command) when is_tuple(command) do
    key =
      case command do
        {:put_batch, [{first_key, _value, _expire_at_ms} | _rest]} ->
          first_key

        {:delete_batch, [first_key | _rest]} ->
          first_key

        {:origin_checked, key, _inner, _before_value, _before_exp, _expected_value, _expire_at_ms} ->
          key

        {:origin_checked, key, _inner, _expected_value, _expire_at_ms} ->
          key

        _ ->
          elem(command, 1)
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
