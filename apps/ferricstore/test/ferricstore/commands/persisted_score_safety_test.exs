defmodule Ferricstore.Commands.PersistedScoreSafetyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Geo, SortedSet}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  test "ZADD handles an unreadable persisted score without raising" do
    store = MockStore.make()
    assert 1 == SortedSet.handle("ZADD", ["zset", "1", "member"], store)

    corrupt_score = String.duplicate("9", 1_000)
    compound_key = CompoundKey.zset_member("zset", "member")
    assert :ok == store.compound_put.("zset", compound_key, corrupt_score, 0)

    assert 0 == SortedSet.handle("ZADD", ["zset", "GT", "2", "member"], store)
  end

  test "GEOPOS handles an unreadable persisted score without raising" do
    store = MockStore.make()
    assert 1 == SortedSet.handle("ZADD", ["geo", "1", "member"], store)

    corrupt_score = String.duplicate("9", 1_000)
    compound_key = CompoundKey.zset_member("geo", "member")
    assert :ok == store.compound_put.("geo", compound_key, corrupt_score, 0)

    assert [[longitude, latitude]] = Geo.handle("GEOPOS", ["geo", "member"], store)
    assert is_binary(longitude)
    assert is_binary(latitude)
  end

  test "ZINCRBY rejects a finite score sum that exceeds the VM float range" do
    base = MockStore.make()
    assert 1 == SortedSet.handle("ZADD", ["zset", "1e308", "member"], base)

    store =
      Map.put(base, :compound_put, fn _key, _compound_key, _value, _expire_at_ms ->
        flunk("an overflowing score must not be written")
      end)

    assert {:error, "ERR resulting score is not a number (NaN)"} =
             SortedSet.handle("ZINCRBY", ["zset", "1e308", "member"], store)
  end
end
