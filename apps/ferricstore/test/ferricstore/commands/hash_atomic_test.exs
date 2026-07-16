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

    test "batches field reads and returns nil for duplicate fields after first delete" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      field_keys = [
        CompoundKey.hash_field("hash", "f1"),
        CompoundKey.hash_field("hash", "f2"),
        CompoundKey.hash_field("hash", "missing")
      ]

      store = %{
        compound_get: fn
          "hash", ^type_key ->
            nil

          "hash", compound_key ->
            flunk(
              "HGETDEL should use compound_batch_get, got per-field lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get_meta: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get_meta, field_keys})
          [{"v1", 0}, {"v2", 0}, nil]
        end,
        compound_batch_delete: fn "hash", compound_keys ->
          send(parent, {:compound_batch_delete, compound_keys})
          :ok
        end,
        compound_batch_put: fn "hash", _entries ->
          flunk("HGETDEL should not roll back on success")
        end,
        compound_count: fn "hash", _prefix -> 1 end
      }

      assert ["v1", nil, "v2", nil] ==
               Hash.handle("HGETDEL", ["hash", "FIELDS", "4", "f1", "f1", "f2", "missing"], store)

      assert_received {:compound_batch_get_meta, ^field_keys}
      assert_received {:compound_batch_delete, deleted_keys}
      assert Enum.sort(deleted_keys) == Enum.sort(Enum.take(field_keys, 2))
      refute_received {:compound_batch_delete, _}
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

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HGETDEL", ["mykey", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HGETEX key [PERSIST|EX sec|PX ms|EXAT ts|PXAT ms_ts] field [field ...]
  # ---------------------------------------------------------------------------

  describe "HGETEX" do
    test "without an expiry option reads fields and preserves their TTLs" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "1", "f1"], store)

      [ttl_before] = Hash.handle("HPTTL", ["hash", "FIELDS", "1", "f1"], store)

      assert ["v1", "v2", nil] ==
               Hash.handle("HGETEX", ["hash", "FIELDS", "3", "f1", "f2", "missing"], store)

      [ttl_after] = Hash.handle("HPTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl_after <= ttl_before
      assert ttl_after >= ttl_before - 100
      assert [-1] == Hash.handle("HTTL", ["hash", "FIELDS", "1", "f2"], store)
    end

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

      result =
        Hash.handle(
          "HGETEX",
          ["hash", "EXAT", Integer.to_string(future_ts), "FIELDS", "1", "f1"],
          store
        )

      assert result == ["v1"]

      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl >= 118 and ttl <= 120
    end

    test "with PXAT sets absolute expiry timestamp in milliseconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      future_ts_ms = System.os_time(:millisecond) + 120_000

      result =
        Hash.handle(
          "HGETEX",
          ["hash", "PXAT", Integer.to_string(future_ts_ms), "FIELDS", "1", "f1"],
          store
        )

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

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HGETEX", ["mykey", "EX", "60", "FIELDS", "1", "f1"], store)
    end

    test "with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HGETEX", ["hash", "EX", "60", "FIELDS", "3", "f1"], store)
    end

    test "with invalid EX value returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      assert {:error, _} =
               Hash.handle("HGETEX", ["hash", "EX", "abc", "FIELDS", "1", "f1"], store)
    end

    test "batches field meta reads and writes each existing duplicate field once" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      field_keys = [
        CompoundKey.hash_field("hash", "f1"),
        CompoundKey.hash_field("hash", "missing"),
        CompoundKey.hash_field("hash", "f2")
      ]

      store = %{
        compound_get: fn
          "hash", ^type_key ->
            "hash"

          "hash", compound_key ->
            flunk("HGETEX should only use compound_get for type, got #{inspect(compound_key)}")
        end,
        compound_get_meta: fn "hash", compound_key ->
          flunk("HGETEX should use compound_batch_get_meta, got #{inspect(compound_key)}")
        end,
        compound_batch_get_meta: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get_meta, field_keys})
          [{"v1", 0}, nil, {"v2", 123}]
        end,
        compound_put: fn "hash", compound_key, value, expire_at_ms ->
          send(parent, {:compound_put, compound_key, value, expire_at_ms})
          :ok
        end
      }

      assert ["v1", "v1", nil, "v2"] ==
               Hash.handle(
                 "HGETEX",
                 ["hash", "EX", "60", "FIELDS", "4", "f1", "f1", "missing", "f2"],
                 store
               )

      assert_received {:compound_batch_get_meta, ^field_keys}
      assert_received {:compound_put, f1_key, "v1", f1_expire_at_ms}
      assert_received {:compound_put, f2_key, "v2", f2_expire_at_ms}
      assert Enum.sort([f1_key, f2_key]) == Enum.sort([hd(field_keys), List.last(field_keys)])
      assert f1_expire_at_ms > 0
      assert f2_expire_at_ms == f1_expire_at_ms
      refute_received {:compound_put, _, _, _}
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

    test "batches field existence reads and writes final duplicate value once" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      field_keys = [
        CompoundKey.hash_field("hash", "f1"),
        CompoundKey.hash_field("hash", "existing"),
        CompoundKey.hash_field("hash", "f2")
      ]

      store = %{
        compound_get: fn
          "hash", ^type_key ->
            "hash"

          "hash", compound_key ->
            flunk(
              "HSETEX should use compound_batch_get, got per-field lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get, field_keys})
          [nil, "old", nil]
        end,
        compound_put: fn "hash", compound_key, value, expire_at_ms ->
          send(parent, {:compound_put, compound_key, value, expire_at_ms})
          :ok
        end
      }

      assert 2 ==
               Hash.handle(
                 "HSETEX",
                 ["hash", "60", "f1", "v1", "existing", "new", "f1", "v2", "f2", "v3"],
                 store
               )

      assert_received {:compound_batch_get, ^field_keys}
      assert_received {:compound_put, f1_key, "v2", f1_expire_at_ms}
      assert_received {:compound_put, existing_key, "new", existing_expire_at_ms}
      assert_received {:compound_put, f2_key, "v3", f2_expire_at_ms}
      assert Enum.sort([f1_key, existing_key, f2_key]) == Enum.sort(field_keys)
      assert f1_expire_at_ms > 0
      assert existing_expire_at_ms == f1_expire_at_ms
      assert f2_expire_at_ms == f1_expire_at_ms
      refute_received {:compound_put, _, _, _}
    end

    test "updates existing fields with new TTL" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "old"], store)

      result = Hash.handle("HSETEX", ["hash", "120", "f1", "new"], store)
      # existing field update, not new
      assert result == 0

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

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HSETEX", ["mykey", "60", "f1", "v1"], store)
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

    test "batches field meta reads and writes each existing duplicate field once" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      field_keys = [
        CompoundKey.hash_field("hash", "f1"),
        CompoundKey.hash_field("hash", "missing"),
        CompoundKey.hash_field("hash", "f2")
      ]

      store = %{
        compound_get: fn
          "hash", ^type_key ->
            "hash"

          "hash", compound_key ->
            flunk("HPEXPIRE should only use compound_get for type, got #{inspect(compound_key)}")
        end,
        compound_get_meta: fn "hash", compound_key ->
          flunk("HPEXPIRE should use compound_batch_get_meta, got #{inspect(compound_key)}")
        end,
        compound_batch_get_meta: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get_meta, field_keys})
          [{"v1", 0}, nil, {"v2", 123}]
        end,
        compound_put: fn "hash", compound_key, value, expire_at_ms ->
          send(parent, {:compound_put, compound_key, value, expire_at_ms})
          :ok
        end
      }

      assert [1, 1, -2, 1] ==
               Hash.handle(
                 "HPEXPIRE",
                 ["hash", "60000", "FIELDS", "4", "f1", "f1", "missing", "f2"],
                 store
               )

      assert_received {:compound_batch_get_meta, ^field_keys}
      assert_received {:compound_put, f1_key, "v1", expire_at_ms}
      assert_received {:compound_put, f2_key, "v2", ^expire_at_ms}
      assert Enum.sort([f1_key, f2_key]) == Enum.sort([hd(field_keys), Enum.at(field_keys, -1)])
      assert expire_at_ms > 0
      refute_received {:compound_put, _, _, _}
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

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HPEXPIRE", ["mykey", "60000", "FIELDS", "1", "f1"], store)
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

    test "batches field meta reads and preserves duplicate field results" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      field_keys = [
        CompoundKey.hash_field("hash", "expiring"),
        CompoundKey.hash_field("hash", "persistent"),
        CompoundKey.hash_field("hash", "missing")
      ]

      store = %{
        compound_get: fn
          "hash", ^type_key ->
            "hash"

          "hash", compound_key ->
            flunk("HPTTL should only use compound_get for type, got #{inspect(compound_key)}")
        end,
        compound_get_meta: fn "hash", compound_key ->
          flunk("HPTTL should use compound_batch_get_meta, got #{inspect(compound_key)}")
        end,
        compound_batch_get_meta: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get_meta, field_keys})
          [{"v1", Ferricstore.CommandTime.now_ms() + 60_000}, {"v2", 0}, nil]
        end
      }

      [pttl1, pttl2, persistent_pttl, missing_pttl] =
        Hash.handle(
          "HPTTL",
          ["hash", "FIELDS", "4", "expiring", "expiring", "persistent", "missing"],
          store
        )

      assert_received {:compound_batch_get_meta, ^field_keys}
      assert pttl1 >= 59_000 and pttl1 <= 60_000
      assert pttl2 == pttl1
      assert persistent_pttl == -1
      assert missing_pttl == -2
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

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HPTTL", ["mykey", "FIELDS", "1", "f1"], store)
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

      assert [-1, -2] ==
               Hash.handle("HEXPIRETIME", ["hash", "FIELDS", "2", "f1", "missing"], store)
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

    test "batches field meta reads and preserves duplicate field results" do
      parent = self()
      type_key = CompoundKey.type_key("hash")
      expire_at_ms = Ferricstore.CommandTime.now_ms() + 60_000

      field_keys = [
        CompoundKey.hash_field("hash", "expiring"),
        CompoundKey.hash_field("hash", "persistent"),
        CompoundKey.hash_field("hash", "missing")
      ]

      store = %{
        compound_get: fn
          "hash", ^type_key ->
            "hash"

          "hash", compound_key ->
            flunk(
              "HEXPIRETIME should only use compound_get for type, got #{inspect(compound_key)}"
            )
        end,
        compound_get_meta: fn "hash", compound_key ->
          flunk("HEXPIRETIME should use compound_batch_get_meta, got #{inspect(compound_key)}")
        end,
        compound_batch_get_meta: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get_meta, field_keys})
          [{"v1", expire_at_ms}, {"v2", 0}, nil]
        end
      }

      assert [div(expire_at_ms, 1000), div(expire_at_ms, 1000), -1, -2] ==
               Hash.handle(
                 "HEXPIRETIME",
                 ["hash", "FIELDS", "4", "expiring", "expiring", "persistent", "missing"],
                 store
               )

      assert_received {:compound_batch_get_meta, ^field_keys}
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

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HEXPIRETIME", ["mykey", "FIELDS", "1", "f1"], store)
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
