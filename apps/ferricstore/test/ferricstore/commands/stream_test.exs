Code.require_file("stream_test/sections/part_01.exs", __DIR__)
Code.require_file("stream_test/sections/part_02.exs", __DIR__)
Code.require_file("stream_test/sections/part_03.exs", __DIR__)

defmodule Ferricstore.Commands.StreamTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Expiry, Stream, Strings}
  alias Ferricstore.Test.MockStore

  # Each test gets a unique stream key to avoid interference.
  defp ustream, do: "stream_#{:rand.uniform(999_999)}"
  defp ids(entries), do: Enum.map(entries, &hd/1)
  defp stream_entry_key(key, id), do: "X:#{key}" <> <<0>> <> id

  defp corrupt_stream_entry(store, key, id) do
    store.compound_put.(key, stream_entry_key(key, id), <<131, 100, 0, 12, "made_up_atom">>, 0)
  end

  # Clean up ETS tables between tests to prevent state leaking.
  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups, Ferricstore.Stream.Index] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  # ===========================================================================
  # XADD
  # ===========================================================================

  use Ferricstore.Commands.StreamTest.Sections.Part01

  use Ferricstore.Commands.StreamTest.Sections.Part02

  use Ferricstore.Commands.StreamTest.Sections.Part03
end
