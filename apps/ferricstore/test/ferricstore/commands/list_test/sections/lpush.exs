defmodule Ferricstore.Commands.ListTest.Sections.Lpush do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Dispatcher, Hash, List, Strings}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

  describe "LPUSH" do
    test "creates list and returns length 1 for single element" do
      store = MockStore.make()
      assert 1 == List.handle("LPUSH", ["mylist", "a"], store)
    end

    test "rolls back new list elements when metadata write fails" do
      parent = self()
      type_key = CompoundKey.type_key("mylist")
      meta_key = CompoundKey.list_meta_key("mylist")

      store = %{
        compound_get: fn
          "mylist", ^type_key -> nil
          "mylist", ^meta_key -> nil
        end,
        compound_put: fn
          "mylist", ^type_key, "list", 0 ->
            :ok

          "mylist", ^meta_key, _meta, 0 ->
            {:error, :disk_full}
        end,
        compound_batch_put: fn "mylist", entries ->
          send(parent, {:element_writes, entries})
          :ok
        end,
        compound_batch_delete: fn "mylist", element_keys ->
          send(parent, {:element_rollback, element_keys})
          :ok
        end,
        compound_delete: fn
          "mylist", ^type_key ->
            send(parent, :type_rollback)
            :ok
        end
      }

      assert {:error, :disk_full} == List.handle("LPUSH", ["mylist", "a", "b"], store)
      assert_received {:element_writes, entries}
      assert_received {:element_rollback, rollback_keys}

      assert Enum.sort(rollback_keys) ==
               Enum.sort(Enum.map(entries, fn {compound_key, _, _} -> compound_key end))

      assert_received :type_rollback
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

    test "notifies one blocking waiter so FIFO pops can chain wakes" do
      parent = self()

      store =
        MockStore.make()
        |> Map.put(:on_push, fn key, count ->
          send(parent, {:on_push, key, count})
        end)

      assert 3 == List.handle("LPUSH", ["mylist", "a", "b", "c"], store)
      assert_receive {:on_push, "mylist", 1}
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

    test "notifies one blocking waiter so FIFO pops can chain wakes" do
      parent = self()

      store =
        MockStore.make()
        |> Map.put(:on_push, fn key, count ->
          send(parent, {:on_push, key, count})
        end)

      assert 3 == List.handle("RPUSH", ["mylist", "a", "b", "c"], store)
      assert_receive {:on_push, "mylist", 1}
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

    test "preserves last element when type cleanup fails after pop" do
      base = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], base)
      type_key = CompoundKey.type_key("mylist")

      store =
        Map.put(base, :compound_delete, fn
          "mylist", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == List.handle("LPOP", ["mylist"], store)
      assert ["a"] == List.handle("LRANGE", ["mylist", "0", "-1"], base)
    end

    test "preserves all elements when type cleanup fails after counted pop" do
      base = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], base)
      type_key = CompoundKey.type_key("mylist")

      store =
        Map.put(base, :compound_delete, fn
          "mylist", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == List.handle("LPOP", ["mylist", "2"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["mylist", "0", "-1"], base)
    end

    test "preserves element when metadata delete fails after last pop" do
      base = MockStore.make()
      List.handle("RPUSH", ["mylist", "a"], base)
      meta_key = CompoundKey.list_meta_key("mylist")

      store =
        Map.put(base, :compound_delete, fn
          "mylist", ^meta_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == List.handle("LPOP", ["mylist"], store)
      assert ["a"] == List.handle("LRANGE", ["mylist", "0", "-1"], base)
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

    test "preserves order when type cleanup fails after counted right pop" do
      base = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], base)
      type_key = CompoundKey.type_key("mylist")

      store =
        Map.put(base, :compound_delete, fn
          "mylist", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == List.handle("RPOP", ["mylist", "2"], store)
      assert ["a", "b"] == List.handle("LRANGE", ["mylist", "0", "-1"], base)
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

    test "options are case-insensitive" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "a", "b", "a"], store)

      assert [2, 4] ==
               List.handle(
                 "LPOS",
                 ["mylist", "a", "rank", "2", "count", "2", "maxlen", "5"],
                 store
               )
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
  describe "LINSERT" do
    test "BEFORE inserts element before pivot" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "c"], store)
      assert 3 == List.handle("LINSERT", ["mylist", "BEFORE", "c", "b"], store)
      assert ["a", "b", "c"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "preserves list when rebalance element rewrite fails" do
      base = MockStore.make()
      type_key = CompoundKey.type_key("mylist")
      meta_key = CompoundKey.list_meta_key("mylist")

      base.compound_put.("mylist", type_key, "list", 0)
      base.compound_put.("mylist", meta_key, :erlang.term_to_binary({2, -1, 2}), 0)
      base.compound_put.("mylist", CompoundKey.list_element("mylist", 0), "a", 0)
      base.compound_put.("mylist", CompoundKey.list_element("mylist", 1), "b", 0)

      store =
        Map.put(base, :compound_batch_put, fn "mylist", entries ->
          if Enum.any?(entries, fn {compound_key, _value, _expire_at_ms} ->
               compound_key == CompoundKey.list_element("mylist", 1_000_000_000)
             end) do
            {:error, :disk_full}
          else
            base.compound_batch_put.("mylist", entries)
          end
        end)

      assert {:error, :disk_full} ==
               List.handle("LINSERT", ["mylist", "BEFORE", "b", "x"], store)

      assert ["a", "b"] == List.handle("LRANGE", ["mylist", "0", "-1"], base)
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
    end
  end
end
