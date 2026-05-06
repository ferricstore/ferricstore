defmodule Ferricstore.Commands.ListTest do
  @moduledoc """
  Comprehensive tests for the List command handler.

  Tests cover all 15 list commands (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN,
  LINDEX, LSET, LREM, LTRIM, LPOS, LINSERT, LMOVE, LPUSHX, RPUSHX),
  including happy paths, error cases, edge cases, and WRONGTYPE checking.

  All tests use the MockStore which delegates to the same `List.execute/4`
  logic used by the Shard GenServer, ensuring behavioral parity.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, Hash, List, Strings}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  describe "storage path guards" do
    test "list operations expose only the compound-key store API" do
      source = File.read!(app_path("lib/ferricstore/store/list_ops.ex"))

      # The closure-based API wrote whole lists as serialized plain values.
      # New list code must stay on the compound-key path so async/read flows
      # see one canonical representation.
      refute source =~ "def execute(get_fn"
      refute source =~ "legacy_execute_blob"
      refute source =~ "legacy_execute_lmove"
      refute source =~ "decode_stored"
      refute source =~ "encode_list"
    end
  end

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  defp list_cleanup_failure_store do
    type_key = CompoundKey.type_key("mylist")
    meta_key = CompoundKey.list_meta_key("mylist")
    element_key = CompoundKey.list_element("mylist", 0)
    {:ok, meta_deleted} = Agent.start_link(fn -> false end)

    %{
      compound_get: fn
        "mylist", ^type_key ->
          "list"

        "mylist", ^meta_key ->
          unless Agent.get(meta_deleted, & &1) do
            :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
          end
      end,
      compound_batch_delete: fn "mylist", [^element_key] -> :ok end,
      compound_delete: fn
        "mylist", ^meta_key ->
          Agent.update(meta_deleted, fn _ -> true end)
          :ok

        "mylist", ^type_key ->
          {:error, :disk_full}
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_scan: fn "mylist", _prefix -> [{CompoundKey.encode_position(0), "a"}] end
    }
  end

  # ===========================================================================
  # LPUSH
  # ===========================================================================

  describe "LPUSH" do
    test "creates list and returns length 1 for single element" do
      store = MockStore.make()
      assert 1 == List.handle("LPUSH", ["mylist", "a"], store)
    end

    test "prepends to existing list and returns new length" do
      store = MockStore.make()
      assert 1 == List.handle("LPUSH", ["mylist", "a"], store)
      assert 2 == List.handle("LPUSH", ["mylist", "b"], store)
    end

    test "multiple elements inserted left-to-right (last arg is leftmost)" do
      store = MockStore.make()
      assert 3 == List.handle("LPUSH", ["mylist", "a", "b", "c"], store)
      # "c" should be leftmost, then "b", then "a"
      assert ["c", "b", "a"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "returns error for missing arguments" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LPUSH", [], store)
      assert msg =~ "wrong number of arguments"
    end

    test "returns error for key only (no elements)" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LPUSH", ["mylist"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "WRONGTYPE: LPUSH on string key returns error" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)
      assert {:error, msg} = List.handle("LPUSH", ["mykey", "a"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "LPUSH can reuse a fully expired hash before TYPE cleanup" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      compound_key = <<"H:mykey", 0, "field">>
      store.compound_put.("mykey", compound_key, "value", System.os_time(:millisecond) - 1)

      assert 1 == List.handle("LPUSH", ["mykey", "a"], store)
      assert ["a"] == List.handle("LRANGE", ["mykey", "0", "-1"], store)
    end

    test "rolls back new type metadata when element write fails" do
      parent = self()
      type_key = CompoundKey.type_key("mylist")
      meta_key = CompoundKey.list_meta_key("mylist")

      store = %{
        compound_get: fn
          "mylist", ^type_key -> nil
          "mylist", ^meta_key -> nil
        end,
        compound_put: fn "mylist", ^type_key, "list", 0 ->
          send(parent, :type_written)
          :ok
        end,
        compound_batch_put: fn "mylist", entries when length(entries) == 1 ->
          {:error, :disk_full}
        end,
        compound_delete: fn "mylist", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, :disk_full} == List.handle("LPUSH", ["mylist", "a"], store)
      assert_received :type_written
      assert_received :type_deleted
    end
  end

  # ===========================================================================
  # RPUSH
  # ===========================================================================

  describe "RPUSH" do
    test "creates list and returns length" do
      store = MockStore.make()
      assert 1 == List.handle("RPUSH", ["mylist", "a"], store)
    end

    test "appends elements" do
      store = MockStore.make()
      assert 1 == List.handle("RPUSH", ["mylist", "a"], store)
      assert 2 == List.handle("RPUSH", ["mylist", "b"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "multiple elements appended in order" do
      store = MockStore.make()
      assert 3 == List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "returns error for missing arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("RPUSH", [], store)
    end

    test "WRONGTYPE: RPUSH on string key returns error" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "hello"], store)
      assert {:error, msg} = List.handle("RPUSH", ["mykey", "a"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  # ===========================================================================
  # LPOP
  # ===========================================================================

  describe "LPOP" do
    test "returns and removes leftmost element" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert "a" == List.handle("LPOP", ["mylist"], store)
      assert ["b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "with count returns multiple elements" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d"], store)
      assert ["a", "b"] == List.handle("LPOP", ["mylist", "2"], store)
      assert ["c", "d"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "with count larger than list length returns all elements" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert ["a", "b"] == List.handle("LPOP", ["mylist", "10"], store)
      assert 0 == List.handle("LLEN", ["mylist"], store)
    end

    test "returns nil on empty list" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      List.handle("LPOP", ["mylist"], store)
      assert nil == List.handle("LPOP", ["mylist"], store)
    end

    test "cleans up type metadata when list becomes empty" do
      store = MockStore.make()
      List.handle("LPUSH", ["mylist", "a"], store)

      assert "a" == List.handle("LPOP", ["mylist"], store)
      assert nil == store.compound_get.("mylist", "T:mylist")
    end

    test "returns type cleanup errors after removing the last element" do
      store = list_cleanup_failure_store()

      assert {:error, :disk_full} == List.handle("LPOP", ["mylist"], store)
    end

    test "returns nil on non-existent key" do
      store = MockStore.make()
      assert nil == List.handle("LPOP", ["nokey"], store)
    end

    test "with count=0 returns nil for non-existent key" do
      store = MockStore.make()
      assert nil == List.handle("LPOP", ["nokey", "0"], store)
    end

    test "returns error for non-integer count" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LPOP", ["mylist", "abc"], store)
    end

    test "WRONGTYPE: LPOP on string key returns error" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "val"], store)
      assert {:error, msg} = List.handle("LPOP", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  # ===========================================================================
  # RPOP
  # ===========================================================================

  describe "RPOP" do
    test "returns and removes rightmost element" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert "c" == List.handle("RPOP", ["mylist"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "with count returns multiple elements (rightmost first)" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d"], store)
      assert ["d", "c"] == List.handle("RPOP", ["mylist", "2"], store)
    end

    test "returns nil on non-existent key" do
      store = MockStore.make()
      assert nil == List.handle("RPOP", ["nokey"], store)
    end

    test "WRONGTYPE: RPOP on string key returns error" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "val"], store)
      assert {:error, msg} = List.handle("RPOP", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  # ===========================================================================
  # LRANGE
  # ===========================================================================

  describe "LRANGE" do
    test "returns subrange" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d", "e"], store)
      assert ["b", "c", "d"] == List.handle("LRANGE", ["mylist", "1", "3"], store)
    end

    test "with negative indices" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d", "e"], store)
      assert ["d", "e"] == List.handle("LRANGE", ["mylist", "-2", "-1"], store)
    end

    test "returns full list with 0 -1" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "scans list elements once for a single LRANGE" do
      base = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], base)
      parent = self()

      store =
        Map.put(base, :compound_scan, fn key, prefix ->
          send(parent, {:compound_scan, key, prefix})
          base.compound_scan.(key, prefix)
        end)

      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)

      assert_received {:compound_scan, "mylist", "L:mylist" <> <<0>>}
      refute_received {:compound_scan, "mylist", "L:mylist" <> <<0>>}
    end

    test "returns empty list for out-of-range start" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert [] == List.handle("LRANGE", ["mylist", "10", "20"], store)
    end

    test "returns empty list for reversed range" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert [] == List.handle("LRANGE", ["mylist", "3", "1"], store)
    end

    test "returns empty list for non-existent key" do
      store = MockStore.make()
      assert [] == List.handle("LRANGE", ["nokey", "0", "-1"], store)
    end

    test "treats a fully expired hash as a missing list before TYPE cleanup" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      compound_key = <<"H:mykey", 0, "field">>
      store.compound_put.("mykey", compound_key, "value", System.os_time(:millisecond) - 1)

      assert [] == List.handle("LRANGE", ["mykey", "0", "-1"], store)
    end

    test "stop beyond list length clamps to end" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["mylist", "0", "100"], store)
    end

    test "returns error for non-integer indices" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LRANGE", ["mylist", "a", "b"], store)
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LRANGE", ["mylist", "0"], store)
    end
  end

  # ===========================================================================
  # LLEN
  # ===========================================================================

  describe "LLEN" do
    test "returns list length" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert 3 == List.handle("LLEN", ["mylist"], store)
    end

    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 == List.handle("LLEN", ["nokey"], store)
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LLEN", [], store)
    end

    test "WRONGTYPE: LLEN on string key returns error" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "val"], store)
      assert {:error, msg} = List.handle("LLEN", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  # ===========================================================================
  # LINDEX
  # ===========================================================================

  describe "LINDEX" do
    test "returns element at positive index" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert "a" == List.handle("LINDEX", ["mylist", "0"], store)
      assert "b" == List.handle("LINDEX", ["mylist", "1"], store)
      assert "c" == List.handle("LINDEX", ["mylist", "2"], store)
    end

    test "returns element at negative index" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert "c" == List.handle("LINDEX", ["mylist", "-1"], store)
      assert "b" == List.handle("LINDEX", ["mylist", "-2"], store)
      assert "a" == List.handle("LINDEX", ["mylist", "-3"], store)
    end

    test "returns nil for out of range index" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert nil == List.handle("LINDEX", ["mylist", "10"], store)
    end

    test "returns nil for non-existent key" do
      store = MockStore.make()
      assert nil == List.handle("LINDEX", ["nokey", "0"], store)
    end

    test "returns error for non-integer index" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LINDEX", ["mylist", "abc"], store)
    end
  end

  # ===========================================================================
  # LSET
  # ===========================================================================

  describe "LSET" do
    test "updates element at index" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert :ok = List.handle("LSET", ["mylist", "1", "B"], store)
      assert "B" == List.handle("LINDEX", ["mylist", "1"], store)
    end

    test "updates element at negative index" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert :ok = List.handle("LSET", ["mylist", "-1", "C"], store)
      assert "C" == List.handle("LINDEX", ["mylist", "-1"], store)
    end

    test "returns error for out of range index" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert {:error, msg} = List.handle("LSET", ["mylist", "10", "x"], store)
      assert msg =~ "index out of range"
    end

    test "returns error for non-existent key" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LSET", ["nokey", "0", "x"], store)
      assert msg =~ "no such key"
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LSET", ["mylist", "0"], store)
    end
  end

  # ===========================================================================
  # LREM
  # ===========================================================================

  describe "LREM" do
    test "count > 0 removes from head" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "c", "a"], store)
      assert 2 == List.handle("LREM", ["mylist", "2", "a"], store)
      assert ["b", "c", "a"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "count < 0 removes from tail" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "c", "a"], store)
      assert 2 == List.handle("LREM", ["mylist", "-2", "a"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "count = 0 removes all occurrences" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "c", "a"], store)
      assert 3 == List.handle("LREM", ["mylist", "0", "a"], store)
      assert ["b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "returns 0 when element not found" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert 0 == List.handle("LREM", ["mylist", "0", "z"], store)
    end

    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 == List.handle("LREM", ["nokey", "0", "a"], store)
    end

    test "deletes key when all elements removed" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "a", "a"], store)
      assert 3 == List.handle("LREM", ["mylist", "0", "a"], store)
      assert 0 == List.handle("LLEN", ["mylist"], store)
    end
  end

  # ===========================================================================
  # LTRIM
  # ===========================================================================

  describe "LTRIM" do
    test "trims list to range" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d", "e"], store)
      assert :ok = List.handle("LTRIM", ["mylist", "1", "3"], store)
      assert ["b", "c", "d"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "trims with negative indices" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d", "e"], store)
      assert :ok = List.handle("LTRIM", ["mylist", "0", "-2"], store)
      assert ["a", "b", "c", "d"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "out of range is safe (deletes key)" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert :ok = List.handle("LTRIM", ["mylist", "10", "20"], store)
      assert 0 == List.handle("LLEN", ["mylist"], store)
    end

    test "returns :ok for non-existent key" do
      store = MockStore.make()
      assert :ok = List.handle("LTRIM", ["nokey", "0", "-1"], store)
    end
  end

  # ===========================================================================
  # LPOS
  # ===========================================================================

  describe "LPOS" do
    test "finds position of element" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "b", "d"], store)
      assert 1 == List.handle("LPOS", ["mylist", "b"], store)
    end

    test "returns nil when element not found" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert nil == List.handle("LPOS", ["mylist", "z"], store)
    end

    test "RANK finds n-th match" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "b", "a"], store)
      assert 3 == List.handle("LPOS", ["mylist", "b", "RANK", "2"], store)
    end

    test "negative RANK searches from end" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "b", "a"], store)
      assert 3 == List.handle("LPOS", ["mylist", "b", "RANK", "-1"], store)
    end

    test "COUNT returns multiple positions" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "b", "a"], store)
      assert [0, 2, 4] == List.handle("LPOS", ["mylist", "a", "COUNT", "0"], store)
    end

    test "COUNT with limit" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "b", "a"], store)
      assert [0, 2] == List.handle("LPOS", ["mylist", "a", "COUNT", "2"], store)
    end

    test "MAXLEN limits scan" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "a"], store)
      # MAXLEN 2 means only scan first 2 elements
      assert 0 == List.handle("LPOS", ["mylist", "a", "MAXLEN", "2"], store)
    end

    test "returns nil for non-existent key" do
      store = MockStore.make()
      assert nil == List.handle("LPOS", ["nokey", "a"], store)
    end

    test "RANK 0 returns error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert {:error, msg} = List.handle("LPOS", ["mylist", "a", "RANK", "0"], store)
      assert msg =~ "RANK can't be zero"
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LPOS", ["mylist"], store)
    end
  end

  # ===========================================================================
  # LINSERT
  # ===========================================================================

  describe "LINSERT" do
    test "BEFORE inserts element before pivot" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "c"], store)
      assert 3 == List.handle("LINSERT", ["mylist", "BEFORE", "c", "b"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "AFTER inserts element after pivot" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "c"], store)
      assert 3 == List.handle("LINSERT", ["mylist", "AFTER", "a", "b"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "returns -1 if pivot not found" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert -1 == List.handle("LINSERT", ["mylist", "BEFORE", "z", "x"], store)
    end

    test "returns 0 for non-existent key" do
      store = MockStore.make()
      assert 0 == List.handle("LINSERT", ["nokey", "BEFORE", "a", "b"], store)
    end

    test "case-insensitive direction" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "c"], store)
      assert 3 == List.handle("LINSERT", ["mylist", "before", "c", "b"], store)
    end

    test "returns error for invalid direction" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LINSERT", ["mylist", "MIDDLE", "a", "b"], store)
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LINSERT", ["mylist", "BEFORE", "a"], store)
    end
  end

  # ===========================================================================
  # LMOVE
  # ===========================================================================

  describe "LMOVE" do
    test "moves element from left of source to right of destination" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)
      assert "a" == List.handle("LMOVE", ["src", "dst", "LEFT", "RIGHT"], store)
      assert ["b", "c"] == List.handle("LRANGE", ["src", "0", "-1"], store)
      assert ["x", "y", "a"] == List.handle("LRANGE", ["dst", "0", "-1"], store)
    end

    test "moves element from right of source to left of destination" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)
      assert "c" == List.handle("LMOVE", ["src", "dst", "RIGHT", "LEFT"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["src", "0", "-1"], store)
      assert ["c", "x", "y"] == List.handle("LRANGE", ["dst", "0", "-1"], store)
    end

    test "moves between same list (rotate)" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert "a" == List.handle("LMOVE", ["mylist", "mylist", "LEFT", "RIGHT"], store)
      assert ["b", "c", "a"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "returns nil for non-existent source" do
      store = MockStore.make()
      assert nil == List.handle("LMOVE", ["nokey", "dst", "LEFT", "RIGHT"], store)
    end

    test "creates destination if it does not exist" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], store)
      assert "a" == List.handle("LMOVE", ["src", "dst", "LEFT", "LEFT"], store)
      assert ["a"] == List.handle("LRANGE", ["dst", "0", "-1"], store)
    end

    test "deletes source when last element is moved" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a"], store)
      assert "a" == List.handle("LMOVE", ["src", "dst", "LEFT", "RIGHT"], store)
      assert 0 == List.handle("LLEN", ["src"], store)
    end

    test "does not drop source element when destination write fails" do
      base = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], base)
      List.handle("RPUSH", ["dst", "x"], base)

      store =
        Map.put(base, :compound_put, fn
          "dst", "L:dst" <> <<0>> <> _pos, _value, 0 ->
            {:error, :disk_full}

          key, compound_key, value, expire_at_ms ->
            base.compound_put.(key, compound_key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} == List.handle("LMOVE", ["src", "dst", "LEFT", "RIGHT"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["src", "0", "-1"], base)
      assert ["x"] == List.handle("LRANGE", ["dst", "0", "-1"], base)
    end

    test "does not drop source element when source metadata update fails" do
      base = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], base)
      List.handle("RPUSH", ["dst", "x"], base)
      src_meta_key = CompoundKey.list_meta_key("src")

      store =
        Map.put(base, :compound_put, fn
          "src", ^src_meta_key, _value, 0 ->
            {:error, :disk_full}

          key, compound_key, value, expire_at_ms ->
            base.compound_put.(key, compound_key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} == List.handle("LMOVE", ["src", "dst", "LEFT", "RIGHT"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["src", "0", "-1"], base)
      assert ["x"] == List.handle("LRANGE", ["dst", "0", "-1"], base)
    end

    test "returns error for invalid direction" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LMOVE", ["src", "dst", "UP", "DOWN"], store)
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LMOVE", ["src", "dst", "LEFT"], store)
    end
  end

  # ===========================================================================
  # LPUSHX
  # ===========================================================================

  describe "LPUSHX" do
    test "prepends when key exists" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert 2 == List.handle("LPUSHX", ["mylist", "b"], store)
      assert ["b", "a"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "returns 0 when key does not exist" do
      store = MockStore.make()
      assert 0 == List.handle("LPUSHX", ["nokey", "a"], store)
      assert 0 == List.handle("LLEN", ["nokey"], store)
    end

    test "multiple elements when key exists" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert 4 == List.handle("LPUSHX", ["mylist", "b", "c", "d"], store)
    end

    test "returns error for missing arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("LPUSHX", ["mylist"], store)
    end
  end

  # ===========================================================================
  # RPUSHX
  # ===========================================================================

  describe "RPUSHX" do
    test "appends when key exists" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert 2 == List.handle("RPUSHX", ["mylist", "b"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "returns 0 when key does not exist" do
      store = MockStore.make()
      assert 0 == List.handle("RPUSHX", ["nokey", "a"], store)
      assert 0 == List.handle("LLEN", ["nokey"], store)
    end

    test "returns error for missing arguments" do
      store = MockStore.make()
      assert {:error, _} = List.handle("RPUSHX", ["mylist"], store)
    end
  end

  # ===========================================================================
  # WRONGTYPE: GET on list key
  # ===========================================================================

  describe "WRONGTYPE: GET on list key" do
    test "GET on list key returns WRONGTYPE error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert {:error, msg} = Strings.handle("GET", ["mylist"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "Dispatcher routes list commands" do
    test "LPUSH via dispatcher" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("lpush", ["mylist", "a"], store)
    end

    test "RPUSH via dispatcher" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("rpush", ["mylist", "a"], store)
    end

    test "LRANGE via dispatcher" do
      store = MockStore.make()
      Dispatcher.dispatch("RPUSH", ["mylist", "a", "b", "c"], store)
      assert ["a", "b", "c"] == Dispatcher.dispatch("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "LLEN via dispatcher" do
      store = MockStore.make()
      Dispatcher.dispatch("RPUSH", ["mylist", "a", "b"], store)
      assert 2 == Dispatcher.dispatch("LLEN", ["mylist"], store)
    end

    test "LPOP via dispatcher" do
      store = MockStore.make()
      Dispatcher.dispatch("RPUSH", ["mylist", "a", "b"], store)
      assert "a" == Dispatcher.dispatch("LPOP", ["mylist"], store)
    end

    test "RPOP via dispatcher" do
      store = MockStore.make()
      Dispatcher.dispatch("RPUSH", ["mylist", "a", "b"], store)
      assert "b" == Dispatcher.dispatch("RPOP", ["mylist"], store)
    end

    test "all list commands are dispatched (case-insensitive)" do
      store = MockStore.make()
      # Just verify they don't return "ERR unknown command"
      Dispatcher.dispatch("rpush", ["mylist", "a", "b", "c"], store)

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("lindex", ["mylist", "0"], store)
             )

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("lset", ["mylist", "0", "x"], store)
             )

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("lrem", ["mylist", "0", "a"], store)
             )

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("ltrim", ["mylist", "0", "-1"], store)
             )

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("lpos", ["mylist", "a"], store)
             )

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("linsert", ["mylist", "BEFORE", "a", "z"], store)
             )

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("lpushx", ["mylist", "x"], store)
             )

      refute match?(
               {:error, "ERR unknown command" <> _},
               Dispatcher.dispatch("rpushx", ["mylist", "x"], store)
             )
    end
  end

  # ===========================================================================
  # Edge cases and cross-command interactions
  # ===========================================================================

  describe "edge cases" do
    test "LPUSH then LPOP cycles through all elements" do
      store = MockStore.make()
      List.handle("LPUSH", ["mylist", "a", "b", "c"], store)
      assert "c" == List.handle("LPOP", ["mylist"], store)
      assert "b" == List.handle("LPOP", ["mylist"], store)
      assert "a" == List.handle("LPOP", ["mylist"], store)
      assert nil == List.handle("LPOP", ["mylist"], store)
    end

    test "RPUSH then RPOP cycles through all elements" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert "c" == List.handle("RPOP", ["mylist"], store)
      assert "b" == List.handle("RPOP", ["mylist"], store)
      assert "a" == List.handle("RPOP", ["mylist"], store)
      assert nil == List.handle("RPOP", ["mylist"], store)
    end

    test "empty string elements are valid" do
      store = MockStore.make()
      assert 2 == List.handle("RPUSH", ["mylist", "", ""], store)
      assert ["", ""] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "LSET then LRANGE reflects update" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      List.handle("LSET", ["mylist", "1", "B"], store)
      assert ["a", "B", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "LTRIM to single element" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d", "e"], store)
      List.handle("LTRIM", ["mylist", "2", "2"], store)
      assert ["c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "LINSERT at head of list" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "b", "c"], store)
      assert 3 == List.handle("LINSERT", ["mylist", "BEFORE", "b", "a"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "LINSERT at tail of list" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert 3 == List.handle("LINSERT", ["mylist", "AFTER", "b", "c"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "LPOS with RANK and COUNT combined" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "b", "a", "b"], store)
      # Find second occurrence of "a" onwards, return up to 2
      assert [2, 4] == List.handle("LPOS", ["mylist", "a", "RANK", "2", "COUNT", "2"], store)
    end

    test "LPOP with count=0 on existing list returns nil" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      # count=0 means no elements popped, but Redis returns empty list
      # Our implementation: count=0 means take 0 elements
      result = List.handle("LPOP", ["mylist", "0"], store)
      # Redis returns empty list for LPOP key 0 when key exists
      # With count=0, we take 0 elements from head
      assert result == [] || result == nil
    end

    test "DEL removes a list key" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert 2 == List.handle("LLEN", ["mylist"], store)
      Strings.handle("DEL", ["mylist"], store)
      assert 0 == List.handle("LLEN", ["mylist"], store)
    end

    test "EXISTS works on list keys" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert 1 == Strings.handle("EXISTS", ["mylist"], store)
    end
  end

  # ===========================================================================
  # WRONGTYPE enforcement edge cases
  # ===========================================================================

  describe "WRONGTYPE enforcement for all list commands" do
    test "LRANGE on string key returns WRONGTYPE" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "val"], store)
      assert {:error, msg} = List.handle("LRANGE", ["mykey", "0", "-1"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "LINDEX on string key returns WRONGTYPE" do
      store = MockStore.make()
      Strings.handle("SET", ["mykey", "val"], store)
      assert {:error, msg} = List.handle("LINDEX", ["mykey", "0"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  # ===========================================================================
  # LPOP/RPOP negative count edge cases
  # ===========================================================================

  describe "LPOP/RPOP negative count" do
    test "LPOP with negative count returns error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert {:error, msg} = List.handle("LPOP", ["mylist", "-1"], store)
      assert msg =~ "not an integer or out of range"
    end

    test "RPOP with negative count returns error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert {:error, msg} = List.handle("RPOP", ["mylist", "-1"], store)
      assert msg =~ "not an integer or out of range"
    end

    test "LPOP with three args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LPOP", ["mylist", "1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "RPOP with three args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("RPOP", ["mylist", "1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # LMOVE direction edge cases
  # ===========================================================================

  describe "LMOVE direction edge cases" do
    test "LMOVE with case-insensitive directions" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], store)
      assert "a" == List.handle("LMOVE", ["src", "dst", "left", "right"], store)
      assert ["a"] == List.handle("LRANGE", ["dst", "0", "-1"], store)
    end
  end

  # ===========================================================================
  # LPOS edge cases
  # ===========================================================================

  describe "LPOS option edge cases" do
    test "LPOS with unrecognized option returns error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert {:error, msg} = List.handle("LPOS", ["mylist", "a", "BOGUS"], store)
      assert msg =~ "not recognized"
    end

    test "LPOS with non-integer RANK returns error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert {:error, msg} = List.handle("LPOS", ["mylist", "a", "RANK", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "LPOS with non-integer COUNT returns error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert {:error, msg} = List.handle("LPOS", ["mylist", "a", "COUNT", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "LPOS with negative COUNT returns error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], store)
      assert {:error, msg} = List.handle("LPOS", ["mylist", "a", "COUNT", "-1"], store)
      assert msg =~ "not an integer"
    end
  end

  # ===========================================================================
  # LSET / LREM / LTRIM non-integer index edge cases
  # ===========================================================================

  describe "integer parsing edge cases" do
    test "LSET with non-integer index returns error" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LSET", ["mylist", "abc", "val"], store)
      assert msg =~ "not an integer"
    end

    test "LREM with non-integer count returns error" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LREM", ["mylist", "abc", "a"], store)
      assert msg =~ "not an integer"
    end

    test "LTRIM with non-integer start returns error" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LTRIM", ["mylist", "abc", "5"], store)
      assert msg =~ "not an integer"
    end

    test "LTRIM with non-integer stop returns error" do
      store = MockStore.make()
      assert {:error, msg} = List.handle("LTRIM", ["mylist", "0", "abc"], store)
      assert msg =~ "not an integer"
    end
  end
end
