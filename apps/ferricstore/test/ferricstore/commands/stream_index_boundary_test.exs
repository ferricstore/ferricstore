defmodule Ferricstore.Commands.StreamIndexBoundaryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream.{Index, Meta, Tables}
  alias Ferricstore.Store.CompoundKey

  test "reverse traversal includes valid unsigned 64-bit stream IDs" do
    Tables.ensure_all()
    key = "stream-u64-#{System.unique_integer([:positive])}"
    on_exit(fn -> Index.clear(key) end)
    max_u64 = 18_446_744_073_709_551_615
    id = "#{max_u64}-#{max_u64}"

    Index.insert_entry(key, id, "compound-key")
    Index.mark_ready(key)

    assert [{^id, "compound-key"}] = Index.slice(key, :min, :max, :infinity, true)
    assert {^id, ^id} = Index.first_last(key)
  end

  test "count_after is exclusive, bounded, and allocation-free" do
    Tables.ensure_all()
    key = "stream-count-after-#{System.unique_integer([:positive])}"
    on_exit(fn -> Index.clear(key) end)

    Enum.each(1..5, fn sequence ->
      Index.insert_entry(key, "1-#{sequence}", "compound-#{sequence}")
    end)

    Index.mark_ready(key)

    assert 0 == Index.count_after(key, {1, 5}, :infinity, nil)
    assert 3 == Index.count_after(key, {1, 2}, :infinity, nil)
    assert 2 == Index.count_after(key, {1, 2}, 2, nil)
    assert 0 == Index.count_after(key, {1, 2}, 0, nil)
  end

  test "index rebuild fails closed on a corrupt persisted stream ID" do
    Tables.ensure_all()
    key = "stream-corrupt-#{System.unique_integer([:positive])}"
    on_exit(fn -> Index.clear(key) end)

    store = %{
      compound_scan: fn ^key, _prefix -> [{"not-an-id", "payload"}] end
    }

    assert {:error, "ERR storage read failed"} = Index.ensure(key, store)
    refute Index.ready?(key)
  end

  test "metadata rebuild fails closed on a corrupt persisted stream ID" do
    Tables.ensure_all()
    key = "stream-meta-corrupt-#{System.unique_integer([:positive])}"
    Meta.cleanup_local(key)
    on_exit(fn -> Meta.cleanup_local(key) end)
    type_key = CompoundKey.type_key(key)

    store = %{
      compound_get: fn
        ^key, ^type_key -> "stream"
        ^key, _compound_key -> nil
      end,
      compound_scan: fn ^key, _prefix -> [{"not-an-id", "payload"}] end
    }

    assert {:error, "ERR storage read failed"} = Meta.entries(key, store)
  end
end
