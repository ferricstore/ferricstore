defmodule Ferricstore.Commands.HashAtomicTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  defp put_live_compound_key(store, key, "set") do
    store.compound_put.(key, CompoundKey.type_key(key), "set", 0)
    store.compound_put.(key, CompoundKey.set_member(key, "member"), "1", 0)
  end

  defp put_live_compound_key(store, key, "list") do
    store.compound_put.(key, CompoundKey.type_key(key), "list", 0)
    store.compound_put.(key, CompoundKey.list_meta_key(key), :erlang.term_to_binary({0, 1}), 0)
    store.compound_put.(key, CompoundKey.list_element(key, 0), "member", 0)
  end

  defp put_live_compound_key(store, key, "zset") do
    store.compound_put.(key, CompoundKey.type_key(key), "zset", 0)
    store.compound_put.(key, CompoundKey.zset_member(key, "member"), "1", 0)
  end

  # ---------------------------------------------------------------------------
  # HGETDEL key field [field ...]
  # ---------------------------------------------------------------------------

  describe "HGETDEL" do
    test "returns values and removes fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2", "f3", "v3"], store)

      result = Hash.handle("HGETDEL", ["hash", "FIELDS", "2", "f1", "f2"], store)
      assert result == ["v1", "v2"]

      # Fields should be deleted
      assert nil == Hash.handle("HGET", ["hash", "f1"], store)
      assert nil == Hash.handle("HGET", ["hash", "f2"], store)
      # Remaining field untouched
      assert "v3" == Hash.handle("HGET", ["hash", "f3"], store)
    end

    test "returns nil for missing fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      result = Hash.handle("HGETDEL", ["hash", "FIELDS", "2", "f1", "missing"], store)
      assert result == ["v1", nil]

      # f1 should be deleted even though missing was nil
      assert nil == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "on empty/nonexistent hash returns all nils" do
      store = MockStore.make()
      result = Hash.handle("HGETDEL", ["nonexistent", "FIELDS", "2", "f1", "f2"], store)
      assert result == [nil, nil]
    end

    test "cleans up type metadata when hash becomes empty after HGETDEL" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HGETDEL", ["hash", "FIELDS", "1", "f1"], store)
      # After deleting all fields, the type metadata should be gone
      assert nil == store.compound_get.("hash", "T:hash")
    end

    test "with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HGETDEL", ["hash", "FIELDS", "3", "f1"], store)
    end

    test "with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HGETDEL", ["hash"], store)
      assert {:error, _} = Hash.handle("HGETDEL", [], store)
    end

    test "on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      put_live_compound_key(store, "mykey", "set")
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HGETDEL", ["mykey", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HGETEX key [PERSIST|EX sec|PX ms|EXAT ts|PXAT ms_ts] field [field ...]
  # ---------------------------------------------------------------------------

  describe "HGETEX" do
    test "with EX sets TTL in seconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)

      result = Hash.handle("HGETEX", ["hash", "EX", "60", "FIELDS", "2", "f1", "f2"], store)
      assert result == ["v1", "v2"]

      # Fields should now have a TTL
      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl >= 58 and ttl <= 60
    end

    test "with PX sets TTL in milliseconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      result = Hash.handle("HGETEX", ["hash", "PX", "60000", "FIELDS", "1", "f1"], store)
      assert result == ["v1"]

      # Field should have TTL close to 60s
      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl >= 58 and ttl <= 60
    end

    test "with EXAT sets absolute expiry timestamp in seconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      future_ts = div(System.os_time(:millisecond), 1000) + 120
      result = Hash.handle("HGETEX", ["hash", "EXAT", Integer.to_string(future_ts), "FIELDS", "1", "f1"], store)
      assert result == ["v1"]

      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl >= 118 and ttl <= 120
    end

    test "with PXAT sets absolute expiry timestamp in milliseconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      future_ts_ms = System.os_time(:millisecond) + 120_000
      result = Hash.handle("HGETEX", ["hash", "PXAT", Integer.to_string(future_ts_ms), "FIELDS", "1", "f1"], store)
      assert result == ["v1"]

      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl >= 118 and ttl <= 120
    end

    test "with PERSIST removes TTL" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "1", "f1"], store)

      # Verify TTL is set
      [ttl_before] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl_before > 0

      result = Hash.handle("HGETEX", ["hash", "PERSIST", "FIELDS", "1", "f1"], store)
      assert result == ["v1"]

      # TTL should now be -1 (no expiry)
      assert [-1] == Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
    end

    test "returns nil for missing fields without modifying them" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      result = Hash.handle("HGETEX", ["hash", "EX", "60", "FIELDS", "2", "f1", "missing"], store)
      assert result == ["v1", nil]
    end

    test "on nonexistent key returns all nils" do
      store = MockStore.make()
      result = Hash.handle("HGETEX", ["hash", "EX", "60", "FIELDS", "1", "f1"], store)
      assert result == [nil]
    end

    test "with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HGETEX", ["hash"], store)
      assert {:error, _} = Hash.handle("HGETEX", [], store)
    end

    test "on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      put_live_compound_key(store, "mykey", "set")
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HGETEX", ["mykey", "EX", "60", "FIELDS", "1", "f1"], store)
    end

    test "with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HGETEX", ["hash", "EX", "60", "FIELDS", "3", "f1"], store)
    end

    test "with invalid EX value returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HGETEX", ["hash", "EX", "abc", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HSETEX key seconds field value [field value ...]
  # ---------------------------------------------------------------------------

  describe "HSETEX" do
    test "sets fields with TTL" do
      store = MockStore.make()

      result = Hash.handle("HSETEX", ["hash", "60", "f1", "v1", "f2", "v2"], store)
      assert result == 2

      # Fields should exist
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
      assert "v2" == Hash.handle("HGET", ["hash", "f2"], store)

      # Fields should have TTL
      [ttl1, ttl2] = Hash.handle("HTTL", ["hash", "FIELDS", "2", "f1", "f2"], store)
      assert ttl1 >= 58 and ttl1 <= 60
      assert ttl2 >= 58 and ttl2 <= 60
    end

    test "updates existing fields with new TTL" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "old"], store)

      result = Hash.handle("HSETEX", ["hash", "120", "f1", "new"], store)
      assert result == 0  # existing field update, not new

      assert "new" == Hash.handle("HGET", ["hash", "f1"], store)
      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl >= 118 and ttl <= 120
    end

    test "with odd field/value count returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSETEX", ["hash", "60", "f1"], store)
    end

    test "with non-integer seconds returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSETEX", ["hash", "abc", "f1", "v1"], store)
    end

    test "with negative seconds returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSETEX", ["hash", "-1", "f1", "v1"], store)
    end

    test "with zero seconds returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSETEX", ["hash", "0", "f1", "v1"], store)
    end

    test "with no args returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSETEX", [], store)
      assert {:error, _} = Hash.handle("HSETEX", ["hash"], store)
      assert {:error, _} = Hash.handle("HSETEX", ["hash", "60"], store)
    end

    test "on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      put_live_compound_key(store, "mykey", "set")
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HSETEX", ["mykey", "60", "f1", "v1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HPEXPIRE key ms FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  describe "HPEXPIRE" do
    test "sets expiry in milliseconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)

      result = Hash.handle("HPEXPIRE", ["hash", "60000", "FIELDS", "2", "f1", "f2"], store)
      assert result == [1, 1]

      # HPTTL should return remaining ms close to 60000
      [pttl] = Hash.handle("HPTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert pttl >= 59_000 and pttl <= 60_000
    end

    test "returns -2 for non-existent fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      result = Hash.handle("HPEXPIRE", ["hash", "60000", "FIELDS", "2", "f1", "missing"], store)
      assert result == [1, -2]
    end

    test "returns all -2 for non-existent key" do
      store = MockStore.make()
      result = Hash.handle("HPEXPIRE", ["hash", "60000", "FIELDS", "1", "f1"], store)
      assert result == [-2]
    end

    test "preserves the field value" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HPEXPIRE", ["hash", "60000", "FIELDS", "1", "f1"], store)
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HPEXPIRE", ["hash"], store)
    end

    test "with non-integer ms returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HPEXPIRE", ["hash", "abc", "FIELDS", "1", "f1"], store)
    end

    test "with negative ms returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HPEXPIRE", ["hash", "-1", "FIELDS", "1", "f1"], store)
    end

    test "with zero ms returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HPEXPIRE", ["hash", "0", "FIELDS", "1", "f1"], store)
    end

    test "with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HPEXPIRE", ["hash", "60000", "FIELDS", "3", "f1"], store)
    end

    test "on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      put_live_compound_key(store, "mykey", "set")
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HPEXPIRE", ["mykey", "60000", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HPTTL key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  describe "HPTTL" do
    test "returns remaining TTL in milliseconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HPEXPIRE", ["hash", "60000", "FIELDS", "1", "f1"], store)

      [pttl] = Hash.handle("HPTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert pttl >= 59_000 and pttl <= 60_000
    end

    test "returns -1 for fields with no expiry" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [-1] == Hash.handle("HPTTL", ["hash", "FIELDS", "1", "f1"], store)
    end

    test "returns -2 for non-existent fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [-1, -2] == Hash.handle("HPTTL", ["hash", "FIELDS", "2", "f1", "missing"], store)
    end

    test "returns all -2 for non-existent key" do
      store = MockStore.make()
      assert [-2, -2] == Hash.handle("HPTTL", ["hash", "FIELDS", "2", "f1", "f2"], store)
    end

    test "with multiple fields, mixed expiry" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      Hash.handle("HPEXPIRE", ["hash", "120000", "FIELDS", "1", "f1"], store)

      [pttl_f1, pttl_f2] = Hash.handle("HPTTL", ["hash", "FIELDS", "2", "f1", "f2"], store)
      assert pttl_f1 >= 119_000 and pttl_f1 <= 120_000
      assert pttl_f2 == -1
    end

    test "with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HPTTL", ["hash"], store)
    end

    test "with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HPTTL", ["hash", "FIELDS", "3", "f1"], store)
    end

    test "on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      put_live_compound_key(store, "mykey", "set")
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HPTTL", ["mykey", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HEXPIRETIME key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  describe "HEXPIRETIME" do
    test "returns correct absolute Unix timestamp in seconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "120", "FIELDS", "1", "f1"], store)

      [expire_time] = Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "1", "f1"], store)
      now_sec = div(System.os_time(:millisecond), 1000)
      # Should be approximately now + 120 seconds
      assert expire_time >= now_sec + 118
      assert expire_time <= now_sec + 122
    end

    test "returns -1 for fields with no expiry" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [-1] == Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "1", "f1"], store)
    end

    test "returns -2 for non-existent fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [-1, -2] == Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "2", "f1", "missing"], store)
    end

    test "returns all -2 for non-existent key" do
      store = MockStore.make()
      assert [-2, -2] == Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "2", "f1", "f2"], store)
    end

    test "with multiple fields, mixed expiry" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      Hash.handle("HEXPIRE", ["hash", "300", "FIELDS", "1", "f1"], store)

      [et_f1, et_f2] = Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "2", "f1", "f2"], store)
      now_sec = div(System.os_time(:millisecond), 1000)
      assert et_f1 >= now_sec + 298 and et_f1 <= now_sec + 302
      assert et_f2 == -1
    end

    test "with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HEXPIRETIME", ["hash"], store)
    end

    test "with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "3", "f1"], store)
    end

    test "on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      put_live_compound_key(store, "mykey", "set")
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HEXPIRETIME", ["mykey", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: 100 concurrent HGETDEL operations
  # ---------------------------------------------------------------------------

  describe "stress: concurrent HGETDEL" do
    test "100 concurrent HGETDEL operations on separate fields" do
      store = MockStore.make()

      # Create 100 fields
      for i <- 1..100 do
        Hash.handle("HSET", ["hash", "field_#{i}", "value_#{i}"], store)
      end

      assert 100 == Hash.handle("HLEN", ["hash"], store)

      # Run 100 concurrent HGETDEL operations, each deleting one field
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            Hash.handle("HGETDEL", ["hash", "FIELDS", "1", "field_#{i}"], store)
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # Each result should be a list with one element
      assert length(results) == 100

      # All returned values should be either the expected value or nil
      # (nil if another concurrent HGETDEL already deleted it, but
      # since each targets a unique field, all should succeed)
      for i <- 1..100 do
        result = Enum.at(results, i - 1)
        assert result == ["value_#{i}"]
      end

      # All fields should be deleted
      assert 0 == Hash.handle("HLEN", ["hash"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "HGETDEL on empty hash returns nils" do
      store = MockStore.make()
      # Create and then empty the hash
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HDEL", ["hash", "f1"], store)

      result = Hash.handle("HGETDEL", ["hash", "FIELDS", "1", "f1"], store)
      assert result == [nil]
    end

    test "HGETEX on non-hash key returns WRONGTYPE" do
      store = MockStore.make()
      # Create a set-type key
      put_live_compound_key(store, "mykey", "list")

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HGETEX", ["mykey", "EX", "60", "FIELDS", "1", "f1"], store)
    end

    test "HSETEX on non-hash key returns WRONGTYPE" do
      store = MockStore.make()
      put_live_compound_key(store, "mykey", "zset")

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HSETEX", ["mykey", "60", "f1", "v1"], store)
    end

    test "HPEXPIRE followed by HPTTL gives consistent result" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HPEXPIRE", ["hash", "5000", "FIELDS", "1", "f1"], store)

      [pttl] = Hash.handle("HPTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert pttl > 0 and pttl <= 5000
    end

    test "HEXPIRETIME after HPEXPIRE returns correct timestamp" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HPEXPIRE", ["hash", "120000", "FIELDS", "1", "f1"], store)

      [expire_time] = Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "1", "f1"], store)
      now_sec = div(System.os_time(:millisecond), 1000)
      assert expire_time >= now_sec + 118 and expire_time <= now_sec + 122
    end

    test "HGETDEL with single field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      result = Hash.handle("HGETDEL", ["hash", "FIELDS", "1", "f1"], store)
      assert result == ["v1"]
      assert nil == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HSETEX mixed new and existing fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "existing", "old"], store)

      result = Hash.handle("HSETEX", ["hash", "60", "existing", "updated", "new", "val"], store)
      # 1 new field, 1 existing updated
      assert result == 1

      assert "updated" == Hash.handle("HGET", ["hash", "existing"], store)
      assert "val" == Hash.handle("HGET", ["hash", "new"], store)
    end
  end
end
