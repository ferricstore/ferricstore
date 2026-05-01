defmodule Ferricstore.EmbeddedExtendedSetTest do
  @moduledoc """
  Set operation tests extracted from EmbeddedExtendedTest.

  Covers: smismember, srandmember, spop, sdiff, sinter, sunion.
  """
  use ExUnit.Case, async: false

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # SET extended: smismember, srandmember, spop, sdiff, sinter, sunion
  # ===========================================================================

  describe "smismember/2" do
    test "returns membership for multiple members" do
      FerricStore.sadd("smm:key", ["a", "b", "c"])
      assert {:ok, [1, 0, 1]} = FerricStore.smismember("smm:key", ["a", "z", "c"])
    end

    test "returns all zeros for nonexistent set" do
      assert {:ok, [0, 0]} = FerricStore.smismember("smm:missing", ["a", "b"])
    end
  end

  describe "srandmember/2" do
    test "returns a random member" do
      FerricStore.sadd("srm:key", ["a", "b", "c"])
      assert {:ok, member} = FerricStore.srandmember("srm:key")
      assert member in ["a", "b", "c"]
    end

    test "returns multiple random members with count" do
      FerricStore.sadd("srm:key2", ["a", "b", "c"])
      assert {:ok, members} = FerricStore.srandmember("srm:key2", 2)
      assert is_list(members) and length(members) == 2
    end

    test "returns nil for nonexistent set" do
      assert {:ok, nil} = FerricStore.srandmember("srm:missing")
    end

    test "returns WRONGTYPE directly for string keys" do
      assert :ok = FerricStore.set("srm:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.srandmember("srm:string")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.srandmember("srm:string", 2)
    end
  end

  describe "spop/2" do
    test "pops a random member" do
      FerricStore.sadd("sp:key", ["a", "b", "c"])
      assert {:ok, member} = FerricStore.spop("sp:key")
      assert member in ["a", "b", "c"]
      # Verify it was removed
      assert FerricStore.sismember("sp:key", member) == {:ok, false}
    end

    test "pops multiple random members" do
      FerricStore.sadd("sp:key2", ["a", "b", "c", "d", "e"])
      assert {:ok, members} = FerricStore.spop("sp:key2", 2)
      assert is_list(members) and length(members) == 2
    end

    test "returns nil for nonexistent set" do
      assert {:ok, nil} = FerricStore.spop("sp:missing")
    end

    test "returns WRONGTYPE directly for string keys" do
      assert :ok = FerricStore.set("sp:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.spop("sp:string")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.spop("sp:string", 2)
    end
  end

  describe "sdiff/1" do
    test "returns difference between sets" do
      FerricStore.sadd("sd:s1", ["a", "b", "c"])
      FerricStore.sadd("sd:s2", ["b", "c", "d"])
      assert {:ok, diff} = FerricStore.sdiff(["sd:s1", "sd:s2"])
      assert Enum.sort(diff) == ["a"]
    end

    test "returns all members for single-set diff" do
      FerricStore.sadd("sd:only", ["a", "b"])
      assert {:ok, diff} = FerricStore.sdiff(["sd:only"])
      assert Enum.sort(diff) == ["a", "b"]
    end

    test "returns WRONGTYPE when any input key is a string" do
      assert {:ok, 2} = FerricStore.sadd("sd:set", ["a", "b"])
      assert :ok = FerricStore.set("sd:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.sdiff(["sd:string", "sd:set"])
      assert {:error, "WRONGTYPE" <> _} = FerricStore.sdiff(["sd:set", "sd:string"])
    end
  end

  describe "sinter/1" do
    test "returns intersection of sets" do
      FerricStore.sadd("si:s1", ["a", "b", "c"])
      FerricStore.sadd("si:s2", ["b", "c", "d"])
      assert {:ok, inter} = FerricStore.sinter(["si:s1", "si:s2"])
      assert Enum.sort(inter) == ["b", "c"]
    end

    test "returns all members for single-set intersection" do
      FerricStore.sadd("si:only", ["a", "b"])
      assert {:ok, inter} = FerricStore.sinter(["si:only"])
      assert Enum.sort(inter) == ["a", "b"]
    end

    test "returns WRONGTYPE when any input key is a string" do
      assert {:ok, 2} = FerricStore.sadd("si:set", ["a", "b"])
      assert :ok = FerricStore.set("si:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.sinter(["si:set", "si:string"])
    end
  end

  describe "sunion/1" do
    test "returns union of sets" do
      FerricStore.sadd("su:s1", ["a", "b"])
      FerricStore.sadd("su:s2", ["b", "c"])
      assert {:ok, union} = FerricStore.sunion(["su:s1", "su:s2"])
      assert Enum.sort(union) == ["a", "b", "c"]
    end

    test "returns all members for single-set union" do
      FerricStore.sadd("su:only", ["a", "b"])
      assert {:ok, union} = FerricStore.sunion(["su:only"])
      assert Enum.sort(union) == ["a", "b"]
    end

    test "returns WRONGTYPE when any input key is a string" do
      assert {:ok, 2} = FerricStore.sadd("su:set", ["a", "b"])
      assert :ok = FerricStore.set("su:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.sunion(["su:set", "su:string"])
    end
  end
end
