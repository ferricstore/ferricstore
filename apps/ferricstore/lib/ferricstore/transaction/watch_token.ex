defmodule Ferricstore.Transaction.WatchToken do
  @moduledoc false

  @raft_location_tags [:waraft_segment, :waraft_projection, :waraft_apply_projection]

  @spec from_entry(tuple() | [tuple()] | [], non_neg_integer(), (-> binary() | nil)) ::
          term() | {:error, :watch_value_unavailable}
  def from_entry([], _now_ms, _materialize), do: :missing

  def from_entry([entry], now_ms, materialize),
    do: from_entry(entry, now_ms, materialize)

  def from_entry(
        {_key, value, expire_at_ms, _lfu, file_id, _offset, _value_size},
        now_ms,
        materialize
      )
      when is_integer(expire_at_ms) and is_integer(now_ms) and is_function(materialize, 0) do
    if expire_at_ms != 0 and expire_at_ms <= now_ms do
      :missing
    else
      case raft_version(file_id) do
        {:ok, version} -> {:watch, {:raft, version}, expire_at_ms}
        :error -> content_token(value, expire_at_ms, materialize)
      end
    end
  end

  defp raft_version({tag, index}) when tag in @raft_location_tags and is_integer(index),
    do: {:ok, index}

  defp raft_version(_file_id), do: :error

  defp content_token(value, expire_at_ms, _materialize) when is_binary(value),
    do: {:watch, {:sha256, :crypto.hash(:sha256, value)}, expire_at_ms}

  defp content_token(_opaque_or_cold_value, expire_at_ms, materialize) do
    case materialize.() do
      value when is_binary(value) ->
        {:watch, {:sha256, :crypto.hash(:sha256, value)}, expire_at_ms}

      _unavailable ->
        {:error, :watch_value_unavailable}
    end
  end
end
