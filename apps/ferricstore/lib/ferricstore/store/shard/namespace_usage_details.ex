defmodule Ferricstore.Store.Shard.NamespaceUsageDetails do
  @moduledoc false

  @spec build(:ets.tid() | atom(), [binary()], map()) :: map()
  def build(usage, logical_keys, aggregate) do
    Enum.reduce(
      Enum.uniq(logical_keys),
      %{
        keys: aggregate.keys,
        bytes: aggregate.bytes,
        flow_count: aggregate.flow_count,
        counted_by_key: %{},
        bytes_by_key: %{},
        entries_by_key: %{},
        plain_entries_by_key: %{},
        internal_entries_by_key: %{},
        top_transfer_base_bytes_by_key: %{}
      },
      fn logical_key, details ->
        {countable, bytes, entries, plain_entries, internal_entries} =
          logical_usage(usage, logical_key)

        details
        |> put_in([:counted_by_key, logical_key], countable > 0)
        |> put_in([:bytes_by_key, logical_key], bytes)
        |> put_in([:entries_by_key, logical_key], entries)
        |> put_in([:plain_entries_by_key, logical_key], plain_entries)
        |> put_in([:internal_entries_by_key, logical_key], internal_entries)
        |> put_in(
          [:top_transfer_base_bytes_by_key, logical_key],
          top_three_transfer_bytes(usage, logical_key)
        )
      end
    )
  end

  defp logical_usage(usage, logical_key) do
    case :ets.lookup(usage, {:logical, logical_key}) do
      [
        {
          {:logical, ^logical_key},
          countable,
          bytes,
          entries,
          plain_entries,
          internal_entries
        }
      ] ->
        {countable, bytes, entries, plain_entries, internal_entries}

      [] ->
        {0, 0, 0, 0, 0}
    end
  end

  defp top_three_transfer_bytes(usage, logical_key) do
    match_spec = [{{{:transfer, logical_key, :"$1", :_}, true}, [], [:"$1"]}]

    values =
      case :ets.select_reverse(usage, match_spec, 3) do
        {matches, _continuation} -> matches
        :"$end_of_table" -> []
      end

    case values do
      [first, second, third] -> {first, second, third, 3}
      [first, second] -> {first, second, 0, 2}
      [first] -> {first, 0, 0, 1}
      [] -> {0, 0, 0, 0}
    end
  end
end
