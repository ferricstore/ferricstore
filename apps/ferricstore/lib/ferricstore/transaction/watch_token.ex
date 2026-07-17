defmodule Ferricstore.Transaction.WatchToken do
  @moduledoc false

  alias Ferricstore.ExpiryContext

  @raft_location_tags [:waraft_segment, :waraft_projection, :waraft_apply_projection]

  @spec from_entry(tuple() | [tuple()] | [], ExpiryContext.t(), (-> binary() | nil)) ::
          term() | {:error, :watch_value_unavailable | :hlc_drift_exceeded}
  def from_entry([], _expiry_context, _materialize), do: :missing

  def from_entry([entry], expiry_context, materialize),
    do: from_entry(entry, expiry_context, materialize)

  def from_entry(
        {_key, value, expire_at_ms, _lfu, file_id, _offset, _value_size},
        expiry_context,
        materialize
      )
      when is_integer(expire_at_ms) and expire_at_ms >= 0 and is_tuple(expiry_context) and
             is_function(materialize, 0) do
    case ExpiryContext.classify(ExpiryContext.normalize(expiry_context), expire_at_ms) do
      :expired ->
        :missing

      {:unsafe, reason} ->
        {:error, reason}

      :live ->
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
