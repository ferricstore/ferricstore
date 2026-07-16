defmodule Ferricstore.Flow.HistoryProjector.KeyCodec do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  @prefix "X:"
  @separator <<0>>
  @max_exact_integer 9_007_199_254_740_991

  def parse_history_entry_key(@prefix <> rest) do
    case :binary.split(rest, @separator, [:global]) do
      [history_key, event_id] when history_key != "" and event_id != "" ->
        with true <- Keys.history_key?(history_key),
             {:ok, event_ms, _version} <- parse_event_id(event_id) do
          {:ok, history_key, event_id, event_ms}
        else
          _invalid -> :error
        end

      _ ->
        :error
    end
  end

  def parse_history_entry_key(_key), do: :error

  def history_entry_key(history_key, event_id),
    do: @prefix <> history_key <> @separator <> event_id

  def parse_event_ms(event_id) do
    case parse_event_id(event_id) do
      {:ok, event_ms, _version} -> {:ok, event_ms}
      :error -> :error
    end
  end

  def parse_event_version(event_id) do
    case parse_event_id(event_id) do
      {:ok, _event_ms, version} -> {:ok, version}
      :error -> :error
    end
  end

  defp parse_event_id(event_id) when is_binary(event_id) do
    with [encoded_ms, encoded_version] <- :binary.split(event_id, "-", [:global]),
         {:ok, event_ms} <- canonical_non_neg_integer(encoded_ms),
         {:ok, version} <- canonical_non_neg_integer(encoded_version) do
      {:ok, event_ms, version}
    else
      _invalid -> :error
    end
  end

  defp parse_event_id(_event_id), do: :error

  defp canonical_non_neg_integer(encoded) do
    case Integer.parse(encoded) do
      {value, ""} when value >= 0 and value <= @max_exact_integer ->
        if encoded == Integer.to_string(value), do: {:ok, value}, else: :error

      _invalid ->
        :error
    end
  end
end
