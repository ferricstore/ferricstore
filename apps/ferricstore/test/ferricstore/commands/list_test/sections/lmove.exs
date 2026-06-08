defmodule Ferricstore.Commands.ListTest.Sections.Lmove do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Dispatcher, Hash, List, Strings}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

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

    test "rolls back new destination type metadata when destination write fails" do
      parent = self()
      base = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], base)
      dst_type_key = CompoundKey.type_key("dst")

      store =
        base
        |> Map.put(:compound_put, fn
          "dst", ^dst_type_key, "list", 0 ->
            send(parent, :type_written)
            base.compound_put.("dst", dst_type_key, "list", 0)

          "dst", "L:dst" <> <<0>> <> _pos, _value, 0 ->
            {:error, :disk_full}

          key, compound_key, value, expire_at_ms ->
            base.compound_put.(key, compound_key, value, expire_at_ms)
        end)
        |> Map.put(:compound_delete, fn
          "dst", ^dst_type_key ->
            send(parent, :type_deleted)
            base.compound_delete.("dst", dst_type_key)

          key, compound_key ->
            base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == List.handle("LMOVE", ["src", "dst", "LEFT", "RIGHT"], store)
      assert_received :type_written
      assert_received :type_deleted
      assert nil == base.compound_get.("dst", dst_type_key)
      assert ["a", "b"] == List.handle("LRANGE", ["src", "0", "-1"], base)
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
  describe "WRONGTYPE: GET on list key" do
    test "GET on list key returns WRONGTYPE error" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)
      assert {:error, msg} = Strings.handle("GET", ["mylist"], store)
      assert msg =~ "WRONGTYPE"
    end
  end
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
  describe "LMOVE direction edge cases" do
    test "LMOVE with case-insensitive directions" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], store)
      assert "a" == List.handle("LMOVE", ["src", "dst", "left", "right"], store)
      assert ["a"] == List.handle("LRANGE", ["dst", "0", "-1"], store)
    end
  end
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
  end
end
