defmodule Ferricstore.Commands.SetStoreCommandsTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Set
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # SDIFFSTORE
  # ===========================================================================

  describe "SDIFFSTORE" do
    test "stores difference and returns count" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c", "d"], store)
      Set.handle("SADD", ["s2", "b", "c"], store)

      assert 2 == Set.handle("SDIFFSTORE", ["dst", "s1", "s2"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["a", "d"]
    end

    test "overwrites existing destination" do
      store = MockStore.make()
      Set.handle("SADD", ["dst", "old1", "old2"], store)
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "b"], store)

      assert 1 == Set.handle("SDIFFSTORE", ["dst", "s1", "s2"], store)
      assert Set.handle("SMEMBERS", ["dst"], store) == ["a"]
    end

    test "overwrites an existing string destination" do
      store = MockStore.make()
      store.put.("dst", "old-string", 0)
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "b"], store)

      assert 1 == Set.handle("SDIFFSTORE", ["dst", "s1", "s2"], store)
      assert store.get.("dst") == nil
      assert Set.handle("SMEMBERS", ["dst"], store) == ["a"]
      assert store.compound_get.("dst", CompoundKey.type_key("dst")) == "set"
    end

    test "empty difference stores empty set and returns 0" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "a", "b"], store)

      assert 0 == Set.handle("SDIFFSTORE", ["dst", "s1", "s2"], store)
      assert Set.handle("SMEMBERS", ["dst"], store) == []
    end

    test "single source key copies all members" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)

      assert 3 == Set.handle("SDIFFSTORE", ["dst", "s1"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["a", "b", "c"]
    end

    test "nonexistent source keys treated as empty" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)

      assert 2 == Set.handle("SDIFFSTORE", ["dst", "s1", "nosuch"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["a", "b"]
    end

    test "all sources nonexistent results in empty set" do
      store = MockStore.make()
      assert 0 == Set.handle("SDIFFSTORE", ["dst", "nope1", "nope2"], store)
    end

    test "sets type registry for destination" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a"], store)
      Set.handle("SDIFFSTORE", ["dst", "s1"], store)

      assert store.compound_get.("dst", CompoundKey.type_key("dst")) == "set"
    end

    test "compound keys exist for stored members" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "x", "y"], store)
      Set.handle("SADD", ["s2", "y"], store)
      Set.handle("SDIFFSTORE", ["dst", "s1", "s2"], store)

      assert store.compound_get.("dst", CompoundKey.set_member("dst", "x")) == "1"
      assert store.compound_get.("dst", CompoundKey.set_member("dst", "y")) == nil
    end

    test "wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SDIFFSTORE", ["dst"], store)
      assert {:error, _} = Set.handle("SDIFFSTORE", [], store)
    end
  end

  # ===========================================================================
  # SINTERSTORE
  # ===========================================================================

  describe "SINTERSTORE" do
    test "stores intersection and returns count" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)
      Set.handle("SADD", ["s2", "b", "c", "d"], store)

      assert 2 == Set.handle("SINTERSTORE", ["dst", "s1", "s2"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["b", "c"]
    end

    test "empty intersection returns 0" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a"], store)
      Set.handle("SADD", ["s2", "b"], store)

      assert 0 == Set.handle("SINTERSTORE", ["dst", "s1", "s2"], store)
      assert Set.handle("SMEMBERS", ["dst"], store) == []
    end

    test "single key copies all members" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)

      assert 2 == Set.handle("SINTERSTORE", ["dst", "s1"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["a", "b"]
    end

    test "three-key intersection" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c", "d"], store)
      Set.handle("SADD", ["s2", "b", "c", "d", "e"], store)
      Set.handle("SADD", ["s3", "c", "d", "e", "f"], store)

      assert 2 == Set.handle("SINTERSTORE", ["dst", "s1", "s2", "s3"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["c", "d"]
    end

    test "overwrites existing destination" do
      store = MockStore.make()
      Set.handle("SADD", ["dst", "old"], store)
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "a", "b"], store)

      assert 2 == Set.handle("SINTERSTORE", ["dst", "s1", "s2"], store)
      members = Enum.sort(Set.handle("SMEMBERS", ["dst"], store))
      assert members == ["a", "b"]
      refute "old" in members
    end

    test "nonexistent source gives empty intersection" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)

      assert 0 == Set.handle("SINTERSTORE", ["dst", "s1", "nosuch"], store)
    end

    test "empty result deletes an existing string destination" do
      store = MockStore.make()
      store.put.("dst", "old-string", 0)
      Set.handle("SADD", ["s1", "a"], store)
      Set.handle("SADD", ["s2", "b"], store)

      assert 0 == Set.handle("SINTERSTORE", ["dst", "s1", "s2"], store)
      assert store.get.("dst") == nil
      assert Set.handle("SMEMBERS", ["dst"], store) == []
      assert store.compound_get.("dst", CompoundKey.type_key("dst")) == nil
    end

    test "wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SINTERSTORE", ["dst"], store)
      assert {:error, _} = Set.handle("SINTERSTORE", [], store)
    end
  end

  # ===========================================================================
  # SUNIONSTORE
  # ===========================================================================

  describe "SUNIONSTORE" do
    test "stores union and returns count" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "c", "d"], store)

      assert 4 == Set.handle("SUNIONSTORE", ["dst", "s1", "s2"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["a", "b", "c", "d"]
    end

    test "overlapping sets deduplicates" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)
      Set.handle("SADD", ["s2", "b", "c", "d"], store)

      assert 4 == Set.handle("SUNIONSTORE", ["dst", "s1", "s2"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["dst"], store)) == ["a", "b", "c", "d"]
    end

    test "destination same as source" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "c"], store)

      # Store union of s1 and s2 into s1
      assert 3 == Set.handle("SUNIONSTORE", ["s1", "s1", "s2"], store)
      assert Enum.sort(Set.handle("SMEMBERS", ["s1"], store)) == ["a", "b", "c"]
    end

    test "overwrites existing destination" do
      store = MockStore.make()
      Set.handle("SADD", ["dst", "old1", "old2"], store)
      Set.handle("SADD", ["s1", "new"], store)

      assert 1 == Set.handle("SUNIONSTORE", ["dst", "s1"], store)
      assert Set.handle("SMEMBERS", ["dst"], store) == ["new"]
    end

    test "all nonexistent sources gives empty set" do
      store = MockStore.make()
      assert 0 == Set.handle("SUNIONSTORE", ["dst", "nope1", "nope2"], store)
    end

    test "wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SUNIONSTORE", ["dst"], store)
      assert {:error, _} = Set.handle("SUNIONSTORE", [], store)
    end
  end

  # ===========================================================================
  # SINTERCARD
  # ===========================================================================

  describe "SINTERCARD" do
    test "returns cardinality of intersection" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c", "d"], store)
      Set.handle("SADD", ["s2", "b", "c", "d", "e"], store)

      assert 3 == Set.handle("SINTERCARD", ["2", "s1", "s2"], store)
    end

    test "with LIMIT stops early" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c", "d"], store)
      Set.handle("SADD", ["s2", "a", "b", "c", "d"], store)

      assert 2 == Set.handle("SINTERCARD", ["2", "s1", "s2", "LIMIT", "2"], store)
    end

    test "batches membership probes while preserving LIMIT" do
      parent = self()
      type_base = CompoundKey.type_key("base")
      type_other = CompoundKey.type_key("other")

      present =
        MapSet.new([
          CompoundKey.set_member("other", "a"),
          CompoundKey.set_member("other", "b"),
          CompoundKey.set_member("other", "c")
        ])

      store = %{
        compound_get: fn
          "base", ^type_base -> "set"
          "other", ^type_other -> "set"
          key, "S:" <> _rest -> flunk("SINTERCARD should batch membership probes for #{key}")
        end,
        compound_count: fn
          "base", _prefix -> 4
          "other", _prefix -> 10
        end,
        compound_scan: fn
          "base", _prefix -> [{"a", "1"}, {"b", "1"}, {"c", "1"}, {"d", "1"}]
          "other", _prefix -> flunk("SINTERCARD should not scan the larger set")
        end,
        compound_batch_get: fn "other", keys ->
          send(parent, {:batch_probe, keys})

          Enum.map(keys, fn key ->
            if MapSet.member?(present, key), do: "1", else: nil
          end)
        end
      }

      assert 2 == Set.handle("SINTERCARD", ["2", "base", "other", "LIMIT", "2"], store)

      assert_receive {:batch_probe, keys}
      assert length(keys) <= 4
    end

    test "LIMIT 0 means no limit" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)
      Set.handle("SADD", ["s2", "a", "b", "c"], store)

      assert 3 == Set.handle("SINTERCARD", ["2", "s1", "s2", "LIMIT", "0"], store)
    end

    test "LIMIT greater than intersection size returns full count" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "a", "b"], store)

      assert 2 == Set.handle("SINTERCARD", ["2", "s1", "s2", "LIMIT", "100"], store)
    end

    test "empty intersection returns 0" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a"], store)
      Set.handle("SADD", ["s2", "b"], store)

      assert 0 == Set.handle("SINTERCARD", ["2", "s1", "s2"], store)
    end

    test "single key returns its cardinality" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)

      assert 3 == Set.handle("SINTERCARD", ["1", "s1"], store)
    end

    test "nonexistent key gives 0" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)

      assert 0 == Set.handle("SINTERCARD", ["2", "s1", "nosuch"], store)
    end

    test "wrong numkeys returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a"], store)

      # numkeys says 3 but only 1 key provided
      assert {:error, _} = Set.handle("SINTERCARD", ["3", "s1"], store)
    end

    test "numkeys 0 returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SINTERCARD", ["0"], store)
    end

    test "non-integer numkeys returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SINTERCARD", ["abc", "s1"], store)
    end

    test "negative LIMIT returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a"], store)
      assert {:error, _} = Set.handle("SINTERCARD", ["1", "s1", "LIMIT", "-1"], store)
    end

    test "no arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SINTERCARD", [], store)
    end

    test "case-insensitive LIMIT" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "a", "b"], store)

      assert 1 == Set.handle("SINTERCARD", ["2", "s1", "s2", "limit", "1"], store)
    end
  end

  # ===========================================================================
  # SMISMEMBER
  # ===========================================================================

  describe "SMISMEMBER" do
    test "mixed membership — 2 exist, 1 does not" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)

      assert [1, 1, 0] == Set.handle("SMISMEMBER", ["myset", "a", "c", "z"], store)
    end

    test "all members exist" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "x", "y", "z"], store)

      assert [1, 1, 1] == Set.handle("SMISMEMBER", ["myset", "x", "y", "z"], store)
    end

    test "no members exist" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)

      assert [0, 0, 0] == Set.handle("SMISMEMBER", ["myset", "x", "y", "z"], store)
    end

    test "key does not exist — all zeros" do
      store = MockStore.make()

      assert [0, 0] == Set.handle("SMISMEMBER", ["nosuch", "a", "b"], store)
    end

    test "single member present" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)

      assert [1] == Set.handle("SMISMEMBER", ["myset", "a"], store)
    end

    test "single member absent" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)

      assert [0] == Set.handle("SMISMEMBER", ["myset", "z"], store)
    end

    test "wrong type returns WRONGTYPE error" do
      store = MockStore.make()
      store.put.("strkey", "hello", 0)

      assert {:error, "WRONGTYPE" <> _} = Set.handle("SMISMEMBER", ["strkey", "a", "b"], store)
    end

    test "wrong number of arguments — no members" do
      store = MockStore.make()

      assert {:error, _} = Set.handle("SMISMEMBER", ["myset"], store)
      assert {:error, _} = Set.handle("SMISMEMBER", [], store)
    end
  end
end
