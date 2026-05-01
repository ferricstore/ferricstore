defmodule Ferricstore.EmbeddedExtendedListTest do
  @moduledoc """
  List operation tests extracted from EmbeddedExtendedTest.

  Covers: lindex, lset, lrem, linsert, lmove, lpos.
  """
  use ExUnit.Case, async: false

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # LIST extended: lindex, lset, lrem, linsert, lmove, lpos
  # ===========================================================================

  describe "WRONGTYPE propagation" do
    test "embedded list commands return WRONGTYPE directly for string keys" do
      assert :ok = FerricStore.set("list:wrongtype:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.lpush("list:wrongtype:string", ["x"])
      assert {:error, "WRONGTYPE" <> _} = FerricStore.lrange("list:wrongtype:string", 0, -1)
      assert {:error, "WRONGTYPE" <> _} = FerricStore.llen("list:wrongtype:string")
    end
  end

  describe "lindex/2" do
    test "returns element at index" do
      FerricStore.rpush("li:key", ["a", "b", "c"])
      assert {:ok, "a"} = FerricStore.lindex("li:key", 0)
      assert {:ok, "b"} = FerricStore.lindex("li:key", 1)
      assert {:ok, "c"} = FerricStore.lindex("li:key", 2)
    end

    test "supports negative index" do
      FerricStore.rpush("li:neg", ["a", "b", "c"])
      assert {:ok, "c"} = FerricStore.lindex("li:neg", -1)
    end

    test "returns nil for out-of-range index" do
      FerricStore.rpush("li:oor", ["a"])
      assert {:ok, nil} = FerricStore.lindex("li:oor", 5)
    end

    test "returns nil for nonexistent key" do
      assert {:ok, nil} = FerricStore.lindex("li:missing", 0)
    end
  end

  describe "lset/3" do
    test "sets element at index" do
      FerricStore.rpush("ls:key", ["a", "b", "c"])
      assert :ok = FerricStore.lset("ls:key", 1, "X")
      assert {:ok, "X"} = FerricStore.lindex("ls:key", 1)
    end

    test "sets at negative index" do
      FerricStore.rpush("ls:neg", ["a", "b", "c"])
      assert :ok = FerricStore.lset("ls:neg", -1, "Z")
      assert {:ok, "Z"} = FerricStore.lindex("ls:neg", -1)
    end

    test "returns error for out-of-range index" do
      FerricStore.rpush("ls:oor", ["a"])
      assert {:error, _} = FerricStore.lset("ls:oor", 10, "X")
    end
  end

  describe "lrem/3" do
    test "removes all occurrences with count 0" do
      FerricStore.rpush("lr:key", ["a", "b", "a", "c", "a"])
      assert {:ok, 3} = FerricStore.lrem("lr:key", 0, "a")
      assert {:ok, ["b", "c"]} = FerricStore.lrange("lr:key", 0, -1)
    end

    test "removes count occurrences from head with positive count" do
      FerricStore.rpush("lr:head", ["a", "b", "a", "c", "a"])
      assert {:ok, 2} = FerricStore.lrem("lr:head", 2, "a")
      assert {:ok, ["b", "c", "a"]} = FerricStore.lrange("lr:head", 0, -1)
    end

    test "removes count occurrences from tail with negative count" do
      FerricStore.rpush("lr:tail", ["a", "b", "a", "c", "a"])
      assert {:ok, 2} = FerricStore.lrem("lr:tail", -2, "a")
      assert {:ok, ["a", "b", "c"]} = FerricStore.lrange("lr:tail", 0, -1)
    end

    test "returns 0 when element not found" do
      FerricStore.rpush("lr:none", ["a", "b"])
      assert {:ok, 0} = FerricStore.lrem("lr:none", 0, "z")
    end
  end

  describe "linsert/4" do
    test "inserts before pivot" do
      FerricStore.rpush("lin:key", ["a", "b", "c"])
      assert {:ok, 4} = FerricStore.linsert("lin:key", :before, "b", "X")
      assert {:ok, ["a", "X", "b", "c"]} = FerricStore.lrange("lin:key", 0, -1)
    end

    test "inserts after pivot" do
      FerricStore.rpush("lin:key2", ["a", "b", "c"])
      assert {:ok, 4} = FerricStore.linsert("lin:key2", :after, "b", "Y")
      assert {:ok, ["a", "b", "Y", "c"]} = FerricStore.lrange("lin:key2", 0, -1)
    end

    test "returns -1 when pivot not found" do
      FerricStore.rpush("lin:nop", ["a", "b"])
      assert {:ok, -1} = FerricStore.linsert("lin:nop", :before, "z", "X")
    end
  end

  describe "lmove/4" do
    test "moves element from source left to destination right" do
      FerricStore.rpush("lm:src", ["a", "b", "c"])
      FerricStore.rpush("lm:dst", ["x"])
      assert {:ok, "a"} = FerricStore.lmove("lm:src", "lm:dst", :left, :right)
      assert {:ok, ["b", "c"]} = FerricStore.lrange("lm:src", 0, -1)
      assert {:ok, ["x", "a"]} = FerricStore.lrange("lm:dst", 0, -1)
    end

    test "moves element from source right to destination left" do
      FerricStore.rpush("lm:src2", ["a", "b", "c"])
      FerricStore.rpush("lm:dst2", ["x"])
      assert {:ok, "c"} = FerricStore.lmove("lm:src2", "lm:dst2", :right, :left)
      assert {:ok, ["a", "b"]} = FerricStore.lrange("lm:src2", 0, -1)
      assert {:ok, ["c", "x"]} = FerricStore.lrange("lm:dst2", 0, -1)
    end

    test "returns nil when source is empty" do
      assert {:ok, nil} = FerricStore.lmove("lm:empty", "lm:dst3", :left, :right)
    end
  end

  describe "lpos/3" do
    test "finds position of element" do
      FerricStore.rpush("lp:key", ["a", "b", "c", "b", "d"])
      assert {:ok, 1} = FerricStore.lpos("lp:key", "b")
    end

    test "returns nil when element not found" do
      FerricStore.rpush("lp:key2", ["a", "b"])
      assert {:ok, nil} = FerricStore.lpos("lp:key2", "z")
    end

    test "finds multiple positions with count option" do
      FerricStore.rpush("lp:multi", ["a", "b", "a", "c", "a"])
      assert {:ok, positions} = FerricStore.lpos("lp:multi", "a", count: 0)
      assert positions == [0, 2, 4]
    end

    test "finds from specified rank" do
      FerricStore.rpush("lp:rank", ["a", "b", "a", "c"])
      assert {:ok, 2} = FerricStore.lpos("lp:rank", "a", rank: 2)
    end
  end
end
