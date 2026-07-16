defmodule Ferricstore.Commands.Stream.Info do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{Entries, Groups, Meta}
  alias Ferricstore.Store.ReadResult

  @spec stream(binary(), map()) :: map() | {:error, binary()}
  def stream(key, store) do
    with :ok <- Meta.ensure_read_type(key, store) do
      case Meta.entries(key, store) do
        [] ->
          {:error, "ERR no such key"}

        [{^key, len, first, last, _ms, _seq}] ->
          with {:ok, {first_entry, last_entry}} <-
                 first_last_entries(key, len, first, last, store),
               groups when is_integer(groups) <- Groups.count(key, store) do
            %{
              "length" => len,
              "first-entry" => first_entry,
              "last-entry" => last_entry,
              "last-generated-id" => last,
              "groups" => groups
            }
          else
            {:error, {:storage_read_failed, _reason}} = failure ->
              ReadResult.command_error(failure)

            {:error, _reason} = error ->
              error
          end

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp first_last_entries(_key, len, _first, _last, _store) when len <= 0,
    do: {:ok, {nil, nil}}

  defp first_last_entries(key, _len, first, last, store) do
    last_key = Entries.entry_key(key, last)

    values =
      if first != "0-0" do
        Entries.batch_get(store, key, [Entries.entry_key(key, first), last_key])
      else
        [nil | Entries.batch_get(store, key, [last_key])]
      end

    case ReadResult.first_failure(values) do
      nil ->
        [first_raw, last_raw] = values

        {:ok,
         {
           Entries.decode_entry(first, first_raw),
           Entries.decode_entry(last, last_raw)
         }}

      failure ->
        ReadResult.command_error(failure)
    end
  end
end
