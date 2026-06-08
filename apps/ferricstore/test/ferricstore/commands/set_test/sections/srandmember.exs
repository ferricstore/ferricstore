defmodule Ferricstore.Commands.SetTest.Sections.Srandmember do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

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

    test "SRANDMEMBER with count 0 does not scan members" do
      type_key = CompoundKey.type_key("myset")

      store = %{
        compound_get: fn "myset", ^type_key -> "set" end,
        compound_scan: fn "myset", _prefix ->
          flunk("SRANDMEMBER count 0 should not scan members")
        end
      }

      assert [] == Set.handle("SRANDMEMBER", ["myset", "0"], store)
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

    test "SPOP with count 0 does not scan or write" do
      type_key = CompoundKey.type_key("myset")

      store = %{
        compound_get: fn "myset", ^type_key -> "set" end,
        compound_scan: fn "myset", _prefix ->
          flunk("SPOP count 0 should not scan members")
        end,
        compound_batch_delete: fn "myset", _compound_keys ->
          flunk("SPOP count 0 should not write tombstones")
        end
      }

      assert [] == Set.handle("SPOP", ["myset", "0"], store)
    end

    test "SPOP cleans up type metadata when set becomes empty" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "only"], store)
      Set.handle("SPOP", ["myset"], store)
      assert nil == store.compound_get.("myset", "T:myset")
    end

    test "SPOP returns type cleanup errors after removing the last member" do
      store = set_cleanup_failure_store()

      assert {:error, :disk_full} == Set.handle("SPOP", ["myset"], store)
    end

    test "SPOP preserves the last member when type cleanup fails" do
      base = MockStore.make()
      Set.handle("SADD", ["myset", "only"], base)
      type_key = CompoundKey.type_key("myset")

      store =
        Map.put(base, :compound_delete, fn
          "myset", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == Set.handle("SPOP", ["myset"], store)
      assert ["only"] == Set.handle("SMEMBERS", ["myset"], base)
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

    test "SMOVE missing source member ignores wrong-type destination" do
      store = MockStore.make()
      Set.handle("SADD", ["src", "a"], store)
      Hash.handle("HSET", ["dst", "field", "value"], store)

      assert 0 == Set.handle("SMOVE", ["src", "dst", "missing"], store)
      assert "value" == Hash.handle("HGET", ["dst", "field"], store)
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

    test "SMOVE does not delete source when destination write fails" do
      parent = self()
      src_type_key = CompoundKey.type_key("src")
      dst_type_key = CompoundKey.type_key("dst")
      src_member_key = CompoundKey.set_member("src", "a")
      dst_member_key = CompoundKey.set_member("dst", "a")

      store = %{
        get: fn _key -> nil end,
        compound_get: fn
          "src", ^src_type_key -> "set"
          "dst", ^dst_type_key -> "set"
          "src", ^src_member_key -> "1"
          "dst", ^dst_member_key -> nil
          _redis_key, _compound_key -> nil
        end,
        compound_put: fn "dst", ^dst_member_key, "1", 0 ->
          {:error, :disk_full}
        end,
        compound_batch_delete: fn "src", compound_keys ->
          send(parent, {:source_deleted, compound_keys})
          :ok
        end,
        compound_delete: fn "src", compound_key ->
          send(parent, {:source_deleted, [compound_key]})
          :ok
        end,
        compound_count: fn "src", _prefix -> 1 end
      }

      assert {:error, :disk_full} == Set.handle("SMOVE", ["src", "dst", "a"], store)
      refute_received {:source_deleted, _}
    end

    test "SMOVE rolls back new destination type metadata when destination write fails" do
      parent = self()
      src_type_key = CompoundKey.type_key("src")
      dst_type_key = CompoundKey.type_key("dst")
      src_member_key = CompoundKey.set_member("src", "a")
      dst_member_key = CompoundKey.set_member("dst", "a")

      store = %{
        get: fn _key -> nil end,
        compound_get: fn
          "src", ^src_type_key -> "set"
          "dst", ^dst_type_key -> nil
          "src", ^src_member_key -> "1"
          "dst", ^dst_member_key -> nil
          _redis_key, _compound_key -> nil
        end,
        compound_put: fn
          "dst", ^dst_type_key, "set", 0 ->
            send(parent, :type_written)
            :ok

          "dst", ^dst_member_key, "1", 0 ->
            {:error, :disk_full}
        end,
        compound_delete: fn "dst", ^dst_type_key ->
          send(parent, :type_deleted)
          :ok
        end,
        compound_batch_delete: fn "src", compound_keys ->
          send(parent, {:source_deleted, compound_keys})
          :ok
        end,
        compound_count: fn "src", _prefix -> 1 end
      }

      assert {:error, :disk_full} == Set.handle("SMOVE", ["src", "dst", "a"], store)
      assert_received :type_written
      assert_received :type_deleted
      refute_received {:source_deleted, _}
    end

    test "SMOVE returns rollback failure when source delete and destination rollback both fail" do
      parent = self()
      src_type_key = CompoundKey.type_key("src")
      dst_type_key = CompoundKey.type_key("dst")
      src_member_key = CompoundKey.set_member("src", "a")
      dst_member_key = CompoundKey.set_member("dst", "a")

      store = %{
        get: fn _key -> nil end,
        compound_get: fn
          "src", ^src_type_key -> "set"
          "dst", ^dst_type_key -> "set"
          "src", ^src_member_key -> "1"
          "dst", ^dst_member_key -> nil
          _redis_key, _compound_key -> nil
        end,
        compound_put: fn "dst", ^dst_member_key, "1", 0 ->
          send(parent, :destination_written)
          :ok
        end,
        compound_batch_delete: fn
          "src", [^src_member_key] ->
            {:error, :source_disk_full}

          "dst", [^dst_member_key] ->
            {:error, :rollback_disk_full}
        end,
        compound_count: fn _redis_key, _prefix -> 1 end
      }

      assert {:error,
              {:smove_rollback_failed, {:error, :source_disk_full}, {:error, :rollback_disk_full}}} ==
               Set.handle("SMOVE", ["src", "dst", "a"], store)

      assert_received :destination_written
    end

    test "SMOVE preserves source and destination when source cleanup fails" do
      base = MockStore.make()
      assert 1 == Set.handle("SADD", ["src", "a"], base)
      assert 1 == Set.handle("SADD", ["dst", "x"], base)

      src_type_key = CompoundKey.type_key("src")

      store =
        Map.put(base, :compound_delete, fn
          "src", ^src_type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == Set.handle("SMOVE", ["src", "dst", "a"], store)
      assert 1 == Set.handle("SISMEMBER", ["src", "a"], base)
      assert 0 == Set.handle("SISMEMBER", ["dst", "a"], base)
      assert 1 == Set.handle("SISMEMBER", ["dst", "x"], base)
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
  describe "member storage" do
    test "member is stored as compound key with presence marker" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "elixir"], store)
      # The compound key should have the member name and value "1"
      assert "1" == store.compound_get.("myset", <<"S:myset", 0, "elixir">>)
    end
  end
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

    test "AST SSCAN with negative cursor returns invalid cursor" do
      store = MockStore.make()
      Set.handle("SADD", ["myset", "a"], store)
      assert {:error, "ERR invalid cursor"} = Set.handle_ast({:sscan, "myset", -1, []}, store)
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
    end
  end
end
