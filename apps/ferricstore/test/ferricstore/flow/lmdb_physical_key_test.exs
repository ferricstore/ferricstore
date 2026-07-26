defmodule Ferricstore.Flow.LMDB.PhysicalKeyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB.PhysicalKey

  @vectors "../../fixtures/flow_lmdb_physical_key_vectors.txt"
           |> Path.expand(__DIR__)
           |> File.read!()
           |> String.split("\n", trim: true)
           |> Enum.reject(&String.starts_with?(&1, "#"))
           |> Enum.map(fn row ->
             [name, prefix, fill, logical_bytes, expected] = String.split(row, "|")
             {name, prefix, fill, String.to_integer(logical_bytes), expected}
           end)

  for {name, prefix, fill, logical_bytes, expected} <- @vectors do
    test "matches the shared #{name} physical-key vector" do
      prefix = unquote(prefix)
      logical_bytes = unquote(logical_bytes)
      logical_key = prefix <> String.duplicate(unquote(fill), logical_bytes - byte_size(prefix))
      expected = unquote(expected)

      assert byte_size(logical_key) == logical_bytes

      if expected == "direct" do
        refute PhysicalKey.compact?(logical_key)
        assert PhysicalKey.derive(logical_key) == logical_key
      else
        assert PhysicalKey.compact?(logical_key)
        assert PhysicalKey.derive(logical_key) == Base.decode16!(expected, case: :mixed)
      end
    end
  end
end
