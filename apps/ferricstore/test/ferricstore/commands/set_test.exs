defmodule Ferricstore.Commands.SetTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # SADD
  # ---------------------------------------------------------------------------

  describe "SADD" do
    test "SADD adds new members and returns count" do
      store = MockStore.make()
      assert 3 == Set.handle("SADD", ["myset", "a", "b", "c"], store)
    end

    test "SADD ignores duplicate members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b"], store)
      assert 1 == Set.handle("SADD", ["myset", "b", "c"], store)
    end

    test "SADD batches member existence reads and writes new unique members only" do
      parent = self()
      type_key = CompoundKey.type_key("myset")

      member_keys = [
        CompoundKey.set_member("myset", "a"),
        CompoundKey.set_member("myset", "b"),
        CompoundKey.set_member("myset", "existing")
      ]

      store = %{
        compound_get: fn
          "myset", ^type_key ->
            "set"

          "myset", compound_key ->
            flunk(
              "SADD should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "myset", ^member_keys ->
          send(parent, {:compound_batch_get, member_keys})
          [nil, nil, "1"]
        end,
        compound_batch_put: fn "myset", entries ->
          send(parent, {:compound_batch_put, entries})
          :ok
        end,
        compound_put: fn "myset", compound_key, "1", 0 ->
          flunk(
            "SADD should use compound_batch_put, got per-member write #{inspect(compound_key)}"
          )
        end
      }

      assert 2 == Set.handle("SADD", ["myset", "a", "a", "b", "existing"], store)
      assert_received {:compound_batch_get, ^member_keys}
      assert_received {:compound_batch_put, entries}

      assert Enum.sort(entries) ==
               member_keys
               |> Enum.take(2)
               |> Enum.map(&{&1, "1", 0})
               |> Enum.sort()

      refute_received {:compound_batch_put, _}
    end

    test "SADD with all duplicates returns 0" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert 0 == Set.handle("SADD", ["myset", "a"], store)
    end

    test "SADD with no members returns error" do
      assert {:error, _} = Set.handle("SADD", ["myset"], MockStore.make())
    end

    test "SADD with no args returns error" do
      assert {:error, _} = Set.handle("SADD", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SREM
  # ---------------------------------------------------------------------------

  describe "SREM" do
    test "SREM removes existing member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert 1 == Set.handle("SREM", ["myset", "b"], store)
    end

    test "SREM on missing member returns 0" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert 0 == Set.handle("SREM", ["myset", "missing"], store)
    end

    test "SREM multiple members returns count removed" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert 2 == Set.handle("SREM", ["myset", "a", "c", "d"], store)
    end

    test "SREM batches member existence reads and removes duplicates once" do
      parent = self()
      type_key = CompoundKey.type_key("myset")

      member_keys = [
        CompoundKey.set_member("myset", "a"),
        CompoundKey.set_member("myset", "b"),
        CompoundKey.set_member("myset", "missing")
      ]

      store = %{
        compound_get: fn
          "myset", ^type_key ->
            nil

          "myset", compound_key ->
            flunk(
              "SREM should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "myset", ^member_keys ->
          send(parent, {:compound_batch_get, member_keys})
          ["1", "1", nil]
        end,
        compound_batch_delete: fn "myset", compound_keys ->
          send(parent, {:compound_batch_delete, compound_keys})
          :ok
        end,
        compound_delete: fn "myset", compound_key ->
          flunk(
            "SREM should use compound_batch_delete, got per-member delete #{inspect(compound_key)}"
          )
        end,
        compound_count: fn "myset", _prefix -> 1 end
      }

      assert 2 == Set.handle("SREM", ["myset", "a", "a", "b", "missing"], store)
      assert_received {:compound_batch_get, ^member_keys}
      assert_received {:compound_batch_delete, deleted_keys}
      assert Enum.sort(deleted_keys) == Enum.sort(Enum.take(member_keys, 2))
      refute_received {:compound_batch_delete, _}
    end

    test "SREM cleans up type metadata when set becomes empty" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "only"], store)
      Set.handle("SREM", ["myset", "only"], store)
      assert nil == store.compound_get.("myset", "T:myset")
    end

    test "SREM with no members returns error" do
      assert {:error, _} = Set.handle("SREM", ["myset"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SMEMBERS
  # ---------------------------------------------------------------------------

  describe "SMEMBERS" do
    test "SMEMBERS returns all members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      members = Set.handle("SMEMBERS", ["myset"], store)
      assert Enum.sort(members) == ["a", "b", "c"]
    end

    test "SMEMBERS on nonexistent set returns empty list" do
      assert [] == Set.handle("SMEMBERS", ["nonexistent"], MockStore.make())
    end

    test "SMEMBERS with wrong arity returns error" do
      assert {:error, _} = Set.handle("SMEMBERS", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SISMEMBER
  # ---------------------------------------------------------------------------

  describe "SISMEMBER" do
    test "SISMEMBER returns 1 for existing member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b"], store)
      assert 1 == Set.handle("SISMEMBER", ["myset", "a"], store)
    end

    test "SISMEMBER returns 0 for missing member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert 0 == Set.handle("SISMEMBER", ["myset", "missing"], store)
    end

    test "SISMEMBER returns 0 for nonexistent key" do
      assert 0 == Set.handle("SISMEMBER", ["nonexistent", "a"], MockStore.make())
    end

    test "SISMEMBER with wrong arity returns error" do
      assert {:error, _} = Set.handle("SISMEMBER", ["key"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SMISMEMBER
  # ---------------------------------------------------------------------------

  describe "SMISMEMBER" do
    test "SMISMEMBER returns one result per requested member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b"], store)

      assert [1, 0, 1] == Set.handle("SMISMEMBER", ["myset", "a", "missing", "b"], store)
    end

    test "SMISMEMBER uses compound_batch_get when the store provides it" do
      type_key = CompoundKey.type_key("myset")

      member_keys = [
        CompoundKey.set_member("myset", "a"),
        CompoundKey.set_member("myset", "missing"),
        CompoundKey.set_member("myset", "b")
      ]

      store = %{
        compound_get: fn
          "myset", ^type_key ->
            nil

          "myset", compound_key ->
            flunk(
              "SMISMEMBER should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "myset", ^member_keys -> ["1", nil, "1"] end
      }

      assert [1, 0, 1] == Set.handle("SMISMEMBER", ["myset", "a", "missing", "b"], store)
    end

    test "SMISMEMBER with no members returns error" do
      assert {:error, _} = Set.handle("SMISMEMBER", ["myset"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SCARD
  # ---------------------------------------------------------------------------

  describe "SCARD" do
    test "SCARD returns set cardinality" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      assert 3 == Set.handle("SCARD", ["myset"], store)
    end

    test "SCARD returns 0 for nonexistent key" do
      assert 0 == Set.handle("SCARD", ["nonexistent"], MockStore.make())
    end

    test "SCARD reflects removals" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      Set.handle("SREM", ["myset", "b"], store)
      assert 2 == Set.handle("SCARD", ["myset"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SINTER
  # ---------------------------------------------------------------------------

  describe "SINTER" do
    test "SINTER returns intersection of two sets" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)
      Set.handle("SADD", ["s2", "b", "c", "d"], store)
      result = Set.handle("SINTER", ["s1", "s2"], store)
      assert Enum.sort(result) == ["b", "c"]
    end

    test "SINTER with disjoint sets returns empty list" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "c", "d"], store)
      assert [] == Set.handle("SINTER", ["s1", "s2"], store)
    end

    test "SINTER with nonexistent key returns empty list" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      assert [] == Set.handle("SINTER", ["s1", "nonexistent"], store)
    end

    test "SINTER single set returns its members" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      result = Set.handle("SINTER", ["s1"], store)
      assert Enum.sort(result) == ["a", "b"]
    end
  end

  # ---------------------------------------------------------------------------
  # SUNION
  # ---------------------------------------------------------------------------

  describe "SUNION" do
    test "SUNION returns union of two sets" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Set.handle("SADD", ["s2", "b", "c"], store)
      result = Set.handle("SUNION", ["s1", "s2"], store)
      assert Enum.sort(result) == ["a", "b", "c"]
    end

    test "SUNION with nonexistent key returns other set" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      result = Set.handle("SUNION", ["s1", "nonexistent"], store)
      assert Enum.sort(result) == ["a", "b"]
    end
  end

  # ---------------------------------------------------------------------------
  # SDIFF
  # ---------------------------------------------------------------------------

  describe "SDIFF" do
    test "SDIFF returns difference" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b", "c"], store)
      Set.handle("SADD", ["s2", "b", "c", "d"], store)
      result = Set.handle("SDIFF", ["s1", "s2"], store)
      assert result == ["a"]
    end

    test "SDIFF with nonexistent second set returns first set" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      result = Set.handle("SDIFF", ["s1", "nonexistent"], store)
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SDIFF of same set returns empty" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      assert [] == Set.handle("SDIFF", ["s1", "s1"], store)
    end
  end

  describe "set STORE commands" do
    test "SUNIONSTORE writes destination members as one compound batch" do
      parent = self()
      s1_type = CompoundKey.type_key("s1")
      s2_type = CompoundKey.type_key("s2")
      dst_type = CompoundKey.type_key("dst")

      store = %{
        get: fn _key -> nil end,
        delete: fn "dst" ->
          send(parent, :delete_dst)
          :ok
        end,
        compound_get: fn
          "s1", ^s1_type -> "set"
          "s2", ^s2_type -> "set"
          "dst", ^dst_type -> nil
        end,
        compound_scan: fn
          "s1", _prefix -> [{"a", "1"}, {"b", "1"}]
          "s2", _prefix -> [{"b", "1"}, {"c", "1"}]
        end,
        compound_delete_prefix: fn "dst", _prefix ->
          send(parent, :delete_dst_prefix)
          :ok
        end,
        compound_delete: fn "dst", ^dst_type ->
          send(parent, :delete_dst_type)
          :ok
        end,
        compound_put: fn
          "dst", ^dst_type, "set", 0 ->
            send(parent, :put_dst_type)
            :ok

          "dst", compound_key, "1", 0 ->
            flunk(
              "SUNIONSTORE should use compound_batch_put, got per-member write #{inspect(compound_key)}"
            )
        end,
        compound_batch_put: fn "dst", entries ->
          send(parent, {:compound_batch_put, entries})
          :ok
        end
      }

      assert 3 == Set.handle("SUNIONSTORE", ["dst", "s1", "s2"], store)
      assert_received :delete_dst
      assert_received :delete_dst_prefix
      assert_received :delete_dst_type
      assert_received :put_dst_type
      assert_received {:compound_batch_put, entries}

      assert Enum.sort(entries) ==
               ["a", "b", "c"]
               |> Enum.map(&{CompoundKey.set_member("dst", &1), "1", 0})
               |> Enum.sort()

      refute_received {:compound_batch_put, _}
    end
  end

  # ---------------------------------------------------------------------------
  # SSCAN
  # ---------------------------------------------------------------------------

  describe "SSCAN" do
    test "SSCAN with cursor 0 returns all members when count >= size" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      [cursor, elements] = Set.handle("SSCAN", ["myset", "0"], store)
      assert cursor == "0"
      assert Enum.sort(elements) == ["a", "b", "c"]
    end

    test "SSCAN with COUNT limits batch size" do
      store = MockStore.make()

      for i <- 1..20 do
        Set.handle(
          "SADD",
          ["myset", "member#{String.pad_leading(Integer.to_string(i), 2, "0")}"],
          store
        )
      end

      [cursor, elements] = Set.handle("SSCAN", ["myset", "0", "COUNT", "5"], store)
      assert cursor != "0"
      assert length(elements) == 5
    end

    test "SSCAN full iteration collects all members exactly once" do
      store = MockStore.make()
      expected = for i <- 1..15, do: "m#{String.pad_leading(Integer.to_string(i), 2, "0")}"

      for m <- expected do
        Set.handle("SADD", ["myset", m], store)
      end

      all_members = collect_sscan_members(store, "myset", "0", 4)
      assert Enum.sort(all_members) == Enum.sort(expected)
    end

    test "SSCAN with MATCH filters members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "apple", "banana", "avocado", "blueberry"], store)
      [cursor, elements] = Set.handle("SSCAN", ["myset", "0", "MATCH", "a*"], store)
      assert cursor == "0"
      assert Enum.sort(elements) == ["apple", "avocado"]
    end

    test "SSCAN on nonexistent key returns cursor 0 and empty list" do
      store = MockStore.make()
      [cursor, elements] = Set.handle("SSCAN", ["nonexistent", "0"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "SSCAN with invalid cursor returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SSCAN", ["myset", "notanumber"], store)
    end

    test "SSCAN with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SSCAN", ["key"], store)
      assert {:error, _} = Set.handle("SSCAN", [], store)
    end

    test "SSCAN on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SSCAN", ["mykey", "0"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SRANDMEMBER
  # ---------------------------------------------------------------------------

  describe "SRANDMEMBER" do
    test "SRANDMEMBER returns a single random member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      result = Set.handle("SRANDMEMBER", ["myset"], store)
      assert result in ["a", "b", "c"]
    end

    test "SRANDMEMBER with positive count returns unique members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c", "d", "e"], store)
      result = Set.handle("SRANDMEMBER", ["myset", "3"], store)
      assert is_list(result)
      assert length(result) == 3
      assert length(Enum.uniq(result)) == 3
      assert Enum.all?(result, &(&1 in ["a", "b", "c", "d", "e"]))
    end

    test "SRANDMEMBER with count > set size returns all members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b"], store)
      result = Set.handle("SRANDMEMBER", ["myset", "10"], store)
      assert Enum.sort(result) == ["a", "b"]
    end

    test "SRANDMEMBER with negative count allows duplicates" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "x"], store)
      result = Set.handle("SRANDMEMBER", ["myset", "-5"], store)
      assert is_list(result)
      assert length(result) == 5
      assert Enum.all?(result, &(&1 == "x"))
    end

    test "SRANDMEMBER with count 0 returns empty list" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      result = Set.handle("SRANDMEMBER", ["myset", "0"], store)
      assert result == []
    end

    test "SRANDMEMBER does not remove members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      Set.handle("SRANDMEMBER", ["myset", "2"], store)
      assert 3 == Set.handle("SCARD", ["myset"], store)
    end

    test "SRANDMEMBER on nonexistent key returns nil" do
      store = MockStore.make()
      result = Set.handle("SRANDMEMBER", ["nonexistent"], store)
      assert result == nil
    end

    test "SRANDMEMBER with count on nonexistent key returns empty list" do
      store = MockStore.make()
      result = Set.handle("SRANDMEMBER", ["nonexistent", "5"], store)
      assert result == []
    end

    test "SRANDMEMBER with non-integer count returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert {:error, _} = Set.handle("SRANDMEMBER", ["myset", "abc"], store)
    end

    test "SRANDMEMBER with wrong arity returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SRANDMEMBER", [], store)
    end

    test "SRANDMEMBER on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SRANDMEMBER", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SPOP
  # ---------------------------------------------------------------------------

  describe "SPOP" do
    test "SPOP removes and returns a random member" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c"], store)
      result = Set.handle("SPOP", ["myset"], store)
      assert result in ["a", "b", "c"]
      assert 2 == Set.handle("SCARD", ["myset"], store)
    end

    test "SPOP with count removes and returns multiple members" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b", "c", "d", "e"], store)
      result = Set.handle("SPOP", ["myset", "3"], store)
      assert is_list(result)
      assert length(result) == 3
      assert length(Enum.uniq(result)) == 3
      assert 2 == Set.handle("SCARD", ["myset"], store)
    end

    test "SPOP with count batches member deletes" do
      parent = self()
      type_key = CompoundKey.type_key("myset")

      member_keys = [
        CompoundKey.set_member("myset", "a"),
        CompoundKey.set_member("myset", "b"),
        CompoundKey.set_member("myset", "c")
      ]

      store = %{
        compound_get: fn
          "myset", ^type_key -> "set"
          "myset", _compound_key -> nil
        end,
        compound_scan: fn "myset", _prefix ->
          [{"a", "1"}, {"b", "1"}, {"c", "1"}]
        end,
        compound_batch_delete: fn "myset", compound_keys ->
          send(parent, {:compound_batch_delete, compound_keys})
          :ok
        end,
        compound_delete: fn "myset", compound_key ->
          flunk(
            "SPOP should use compound_batch_delete, got per-member delete #{inspect(compound_key)}"
          )
        end,
        compound_count: fn "myset", _prefix -> 1 end
      }

      result = Set.handle("SPOP", ["myset", "2"], store)
      assert length(result) == 2
      assert_received {:compound_batch_delete, deleted_keys}
      assert length(deleted_keys) == 2
      assert Enum.all?(deleted_keys, &(&1 in member_keys))
      refute_received {:compound_batch_delete, _}
    end

    test "SPOP with count > set size returns all members and empties set" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a", "b"], store)
      result = Set.handle("SPOP", ["myset", "10"], store)
      assert Enum.sort(result) == ["a", "b"]
      assert 0 == Set.handle("SCARD", ["myset"], store)
    end

    test "SPOP with count 0 returns empty list" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      result = Set.handle("SPOP", ["myset", "0"], store)
      assert result == []
      assert 1 == Set.handle("SCARD", ["myset"], store)
    end

    test "SPOP cleans up type metadata when set becomes empty" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "only"], store)
      Set.handle("SPOP", ["myset"], store)
      assert nil == store.compound_get.("myset", "T:myset")
    end

    test "SPOP on nonexistent key returns nil" do
      store = MockStore.make()
      result = Set.handle("SPOP", ["nonexistent"], store)
      assert result == nil
    end

    test "SPOP with count on nonexistent key returns empty list" do
      store = MockStore.make()
      result = Set.handle("SPOP", ["nonexistent", "5"], store)
      assert result == []
    end

    test "SPOP with non-integer count returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert {:error, _} = Set.handle("SPOP", ["myset", "abc"], store)
    end

    test "SPOP with negative count returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert {:error, _} = Set.handle("SPOP", ["myset", "-1"], store)
    end

    test "SPOP with wrong arity returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SPOP", [], store)
    end

    test "SPOP on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SPOP", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SMOVE
  # ---------------------------------------------------------------------------

  describe "SMOVE" do
    test "SMOVE moves member from source to destination" do
      store = MockStore.make()
      Set.handle("SADD", ["src", "a", "b", "c"], store)
      Set.handle("SADD", ["dst", "x", "y"], store)
      assert 1 == Set.handle("SMOVE", ["src", "dst", "b"], store)
      # b should be removed from src
      assert 0 == Set.handle("SISMEMBER", ["src", "b"], store)
      # b should be in dst
      assert 1 == Set.handle("SISMEMBER", ["dst", "b"], store)
      # cardinalities should reflect the move
      assert 2 == Set.handle("SCARD", ["src"], store)
      assert 3 == Set.handle("SCARD", ["dst"], store)
    end

    test "SMOVE returns 0 when member not in source" do
      store = MockStore.make()
      Set.handle("SADD", ["src", "a"], store)
      Set.handle("SADD", ["dst", "x"], store)
      assert 0 == Set.handle("SMOVE", ["src", "dst", "missing"], store)
    end

    test "SMOVE to nonexistent destination creates it" do
      store = MockStore.make()
      Set.handle("SADD", ["src", "a", "b"], store)
      assert 1 == Set.handle("SMOVE", ["src", "dst", "a"], store)
      assert 1 == Set.handle("SISMEMBER", ["dst", "a"], store)
    end

    test "SMOVE from nonexistent source returns 0" do
      store = MockStore.make()
      assert 0 == Set.handle("SMOVE", ["nonexistent", "dst", "a"], store)
    end

    test "SMOVE cleans up source type metadata when source becomes empty" do
      store = MockStore.make()
      Set.handle("SADD", ["src", "only"], store)
      Set.handle("SADD", ["dst", "x"], store)
      Set.handle("SMOVE", ["src", "dst", "only"], store)
      assert nil == store.compound_get.("src", "T:src")
    end

    test "SMOVE member already in destination is a no-op for destination" do
      store = MockStore.make()
      Set.handle("SADD", ["src", "a", "b"], store)
      Set.handle("SADD", ["dst", "a", "x"], store)
      assert 1 == Set.handle("SMOVE", ["src", "dst", "a"], store)
      # a should be removed from src
      assert 0 == Set.handle("SISMEMBER", ["src", "a"], store)
      # dst cardinality should not increase (a was already there)
      assert 2 == Set.handle("SCARD", ["dst"], store)
    end

    test "SMOVE with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Set.handle("SMOVE", ["src", "dst"], store)
      assert {:error, _} = Set.handle("SMOVE", ["src"], store)
      assert {:error, _} = Set.handle("SMOVE", [], store)
    end

    test "SMOVE on wrong type source returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SMOVE", ["mykey", "dst", "a"], store)
    end

    test "SMOVE on wrong type destination returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["src", "a"], store)
      Hash.handle("HSET", ["dst", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SMOVE", ["src", "dst", "a"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Type enforcement
  # ---------------------------------------------------------------------------

  describe "type enforcement" do
    test "SADD on a key used as hash returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SADD", ["mykey", "a"], store)
    end

    test "SMEMBERS on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SMEMBERS", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Member IS the key — presence marker
  # ---------------------------------------------------------------------------

  describe "member storage" do
    test "member is stored as compound key with presence marker" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "elixir"], store)
      # The compound key should have the member name and value "1"
      assert "1" == store.compound_get.("myset", <<"S:myset", 0, "elixir">>)
    end
  end

  # ---------------------------------------------------------------------------
  # Private test helpers
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Edge cases: arity, WRONGTYPE for multi-set ops, SSCAN cursor
  # ---------------------------------------------------------------------------

  describe "arity edge cases" do
    test "SCARD with no args returns error" do
      assert {:error, msg} = Set.handle("SCARD", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "SCARD with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Set.handle("SCARD", ["set", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "SINTER with no args returns error" do
      assert {:error, msg} = Set.handle("SINTER", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "SUNION with no args returns error" do
      assert {:error, msg} = Set.handle("SUNION", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "SDIFF with no args returns error" do
      assert {:error, msg} = Set.handle("SDIFF", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "SMOVE with only key returns error" do
      assert {:error, msg} = Set.handle("SMOVE", ["src"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "SPOP with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Set.handle("SPOP", ["set", "1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "SRANDMEMBER with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Set.handle("SRANDMEMBER", ["set", "1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "SISMEMBER with no args returns error" do
      assert {:error, msg} = Set.handle("SISMEMBER", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "SISMEMBER with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Set.handle("SISMEMBER", ["set", "member", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "SMEMBERS with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Set.handle("SMEMBERS", ["set", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "WRONGTYPE enforcement for multi-set operations" do
    test "SINTER with one key being wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      Hash.handle("HSET", ["s2", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SINTER", ["s1", "s2"], store)
    end

    test "SUNION with one key being wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      List.handle("LPUSH", ["s2", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SUNION", ["s1", "s2"], store)
    end

    test "SDIFF with one key being wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["s1", "a", "b"], store)
      SortedSet.handle("ZADD", ["s2", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SDIFF", ["s1", "s2"], store)
    end

    test "SCARD on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SCARD", ["mykey"], store)
    end

    test "SISMEMBER on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SISMEMBER", ["mykey", "m"], store)
    end

    test "SREM on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SREM", ["mykey", "m"], store)
    end
  end

  describe "SSCAN cursor edge cases" do
    test "SSCAN with negative cursor returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert {:error, "ERR invalid cursor"} = Set.handle("SSCAN", ["myset", "-1"], store)
    end

    test "SSCAN with very large cursor returns cursor 0 and empty list" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      [cursor, elements] = Set.handle("SSCAN", ["myset", "999999"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "SSCAN with unknown option returns error" do
      store = MockStore.make()

      assert {:error, "ERR syntax error"} =
               Set.handle("SSCAN", ["myset", "0", "BOGUS", "val"], store)
    end

    test "SSCAN with odd trailing option returns error" do
      store = MockStore.make()
      assert {:error, "ERR syntax error"} = Set.handle("SSCAN", ["myset", "0", "MATCH"], store)
    end

    test "SSCAN with COUNT 0 returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert {:error, _} = Set.handle("SSCAN", ["myset", "0", "COUNT", "0"], store)
    end

    test "SSCAN with negative COUNT returns error" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert {:error, _} = Set.handle("SSCAN", ["myset", "0", "COUNT", "-1"], store)
    end
  end

  describe "empty member handling" do
    test "SADD with empty string member works" do
      store = MockStore.make()
      assert 1 == Set.handle("SADD", ["myset", ""], store)
      assert 1 == Set.handle("SISMEMBER", ["myset", ""], store)
    end

    test "SRANDMEMBER with negative count on empty set returns empty list" do
      store = MockStore.make()
      result = Set.handle("SRANDMEMBER", ["myset", "-5"], store)
      assert result == []
    end
  end

  defp collect_sscan_members(store, key, cursor, count) do
    collect_sscan_members(store, key, cursor, count, [])
  end

  defp collect_sscan_members(store, key, cursor, count, acc) do
    [next_cursor, elements] =
      Set.handle("SSCAN", [key, cursor, "COUNT", Integer.to_string(count)], store)

    new_acc = acc ++ elements

    if next_cursor == "0" do
      new_acc
    else
      collect_sscan_members(store, key, next_cursor, count, new_acc)
    end
  end
end
