defmodule Ferricstore.EmbeddedExtendedHashTest do
  @moduledoc """
  Hash operation tests extracted from EmbeddedExtendedTest.

  Covers: hdel, hexists, hlen, hkeys, hvals, hmget, hincrby,
  hincrbyfloat, hsetnx, hrandfield, hstrlen.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # HASH extended: hdel, hexists, hlen, hkeys, hvals, hmget, hincrby,
  # hincrbyfloat, hsetnx, hrandfield, hstrlen
  # ===========================================================================

  describe "hdel/2" do
    test "deletes existing hash fields" do
      FerricStore.hset("hd:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, 2} = FerricStore.hdel("hd:key", ["a", "c"])
      assert {:ok, nil} = FerricStore.hget("hd:key", "a")
      assert {:ok, "2"} = FerricStore.hget("hd:key", "b")
    end

    test "ignores nonexistent fields" do
      FerricStore.hset("hd:key2", %{"a" => "1"})
      assert {:ok, 0} = FerricStore.hdel("hd:key2", ["z"])
    end

    test "hdel on nonexistent hash returns 0" do
      assert {:ok, 0} = FerricStore.hdel("hd:missing", ["f"])
    end
  end

  describe "hexists/2" do
    test "returns true for existing field" do
      FerricStore.hset("hex:key", %{"name" => "alice"})
      assert FerricStore.hexists("hex:key", "name") == true
    end

    test "returns false for missing field" do
      FerricStore.hset("hex:key2", %{"name" => "alice"})
      assert FerricStore.hexists("hex:key2", "age") == false
    end

    test "returns false for nonexistent hash" do
      assert FerricStore.hexists("hex:missing", "field") == false
    end
  end

  describe "hlen/1" do
    test "returns number of fields" do
      FerricStore.hset("hl:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, 3} = FerricStore.hlen("hl:key")
    end

    test "returns 0 for nonexistent hash" do
      assert {:ok, 0} = FerricStore.hlen("hl:missing")
    end
  end

  describe "hkeys/1" do
    test "returns all field names" do
      FerricStore.hset("hk:key", %{"name" => "alice", "age" => "30"})
      assert {:ok, keys} = FerricStore.hkeys("hk:key")
      assert Enum.sort(keys) == ["age", "name"]
    end

    test "returns empty list for nonexistent hash" do
      assert {:ok, []} = FerricStore.hkeys("hk:missing")
    end
  end

  describe "hvals/1" do
    test "returns all field values" do
      FerricStore.hset("hv:key", %{"a" => "1", "b" => "2"})
      assert {:ok, vals} = FerricStore.hvals("hv:key")
      assert Enum.sort(vals) == ["1", "2"]
    end

    test "returns empty list for nonexistent hash" do
      assert {:ok, []} = FerricStore.hvals("hv:missing")
    end
  end

  describe "hmget/2" do
    test "returns values for requested fields" do
      FerricStore.hset("hmg:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, ["1", "3"]} = FerricStore.hmget("hmg:key", ["a", "c"])
    end

    test "returns nil for missing fields" do
      FerricStore.hset("hmg:key2", %{"a" => "1"})
      assert {:ok, ["1", nil]} = FerricStore.hmget("hmg:key2", ["a", "z"])
    end

    test "returns all nils for nonexistent hash" do
      assert {:ok, [nil, nil]} = FerricStore.hmget("hmg:missing", ["a", "b"])
    end
  end

  describe "hincrby/3" do
    test "increments hash field by integer" do
      FerricStore.hset("hib:key", %{"count" => "10"})
      assert {:ok, 15} = FerricStore.hincrby("hib:key", "count", 5)
    end

    test "creates field if it does not exist" do
      FerricStore.hset("hib:key2", %{"other" => "x"})
      assert {:ok, 3} = FerricStore.hincrby("hib:key2", "count", 3)
    end

    test "decrements with negative amount" do
      FerricStore.hset("hib:key3", %{"count" => "10"})
      assert {:ok, 7} = FerricStore.hincrby("hib:key3", "count", -3)
    end
  end

  describe "hincrbyfloat/3" do
    test "increments hash field by float" do
      FerricStore.hset("hif:key", %{"val" => "10"})
      assert {:ok, result} = FerricStore.hincrbyfloat("hif:key", "val", 2.5)
      {f, _} = Float.parse(result)
      assert_in_delta f, 12.5, 0.001
    end

    test "creates field if it does not exist" do
      FerricStore.hset("hif:key2", %{"other" => "x"})
      assert {:ok, result} = FerricStore.hincrbyfloat("hif:key2", "val", 3.14)
      {f, _} = Float.parse(result)
      assert_in_delta f, 3.14, 0.001
    end
  end

  describe "hsetnx/3" do
    test "sets field when it does not exist" do
      FerricStore.hset("hsn:key", %{"a" => "1"})
      assert {:ok, true} = FerricStore.hsetnx("hsn:key", "b", "2")
      assert {:ok, "2"} = FerricStore.hget("hsn:key", "b")
    end

    test "does not overwrite existing field" do
      FerricStore.hset("hsn:key2", %{"a" => "1"})
      assert {:ok, false} = FerricStore.hsetnx("hsn:key2", "a", "999")
      assert {:ok, "1"} = FerricStore.hget("hsn:key2", "a")
    end
  end

  describe "hrandfield/2" do
    test "returns a random field from hash" do
      FerricStore.hset("hrf:key", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, field} = FerricStore.hrandfield("hrf:key")
      assert field in ["a", "b", "c"]
    end

    test "returns multiple random fields with count" do
      FerricStore.hset("hrf:key2", %{"a" => "1", "b" => "2", "c" => "3"})
      assert {:ok, fields} = FerricStore.hrandfield("hrf:key2", 2)
      assert is_list(fields) and length(fields) == 2
    end

    test "returns nil for nonexistent hash" do
      assert {:ok, nil} = FerricStore.hrandfield("hrf:missing")
    end
  end

  describe "hstrlen/2" do
    test "returns length of hash field value" do
      FerricStore.hset("hsl:key", %{"name" => "alice"})
      assert {:ok, 5} = FerricStore.hstrlen("hsl:key", "name")
    end

    test "returns 0 for missing field" do
      FerricStore.hset("hsl:key2", %{"a" => "1"})
      assert {:ok, 0} = FerricStore.hstrlen("hsl:key2", "missing")
    end
  end
end
