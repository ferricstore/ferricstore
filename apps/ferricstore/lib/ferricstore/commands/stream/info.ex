defmodule Ferricstore.Commands.Stream.Info do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{Entries, Groups, Meta}

  @spec stream(binary(), map()) :: map() | {:error, binary()}
  def stream(key, store) do
    with :ok <- Meta.ensure_read_type(key, store) do
      case Meta.entries(key, store) do
        [] ->
          {:error, "ERR no such key"}

        [{^key, len, first, last, _ms, _seq}] ->
          {first_entry, last_entry} = first_last_entries(key, len, first, last, store)

          %{
            "length" => len,
            "first-entry" => first_entry,
            "last-entry" => last_entry,
            "last-generated-id" => last,
            "groups" => Groups.count(key, store)
          }
      end
    end
  end

  defp first_last_entries(_key, len, _first, _last, _store) when len <= 0, do: {nil, nil}

  defp first_last_entries(key, _len, first, last, store) do
    last_key = Entries.entry_key(key, last)

    {first_raw, last_raw} =
      if first != "0-0" do
        [first_raw, last_raw] =
          Entries.batch_get(store, key, [Entries.entry_key(key, first), last_key])

        {first_raw, last_raw}
      else
        [last_raw] = Entries.batch_get(store, key, [last_key])
        {nil, last_raw}
      end

    {
      Entries.decode_entry(first, first_raw),
      Entries.decode_entry(last, last_raw)
    }
  end
end
