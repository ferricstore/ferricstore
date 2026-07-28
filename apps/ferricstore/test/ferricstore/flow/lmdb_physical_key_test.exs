defmodule Ferricstore.Flow.LMDB.PhysicalKeyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB}
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

  test "compacts oversized Flow value references through the same access contract" do
    logical_key = Keys.value_key(String.duplicate("v", 600), :payload, 1, "partition")
    physical_key = PhysicalKey.derive(logical_key)

    assert byte_size(logical_key) > 511
    assert PhysicalKey.compact?(logical_key)
    assert physical_key != logical_key
    assert byte_size(physical_key) == 40

    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-lmdb-value-key-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(path) end)

    assert :ok = LMDB.write_batch(path, [{:put, logical_key, "value"}])
    assert {:ok, "value"} = LMDB.get(path, logical_key)
  end

  test "does not compact unrelated oversized keys" do
    logical_key = String.duplicate("x", 512)

    refute PhysicalKey.compact?(logical_key)
    assert PhysicalKey.derive(logical_key) == logical_key
  end
end
