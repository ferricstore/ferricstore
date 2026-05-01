defmodule Ferricstore.Commands.HashTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Generic
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.List
  alias Ferricstore.Commands.Set
  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Commands.Strings
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # HSET
  # ---------------------------------------------------------------------------

  describe "HSET" do
    test "HSET creates new fields and returns count of added" do
      store = MockStore.make()
      assert 2 == Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
    end

    test "HSET on existing field returns 0 (update, not new)" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 0 == Hash.handle("HSET", ["hash", "f1", "v2"], store)
    end

    test "HSET updates value of existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "old"], store)
      Hash.handle("HSET", ["hash", "f1", "new"], store)
      assert "new" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HSET with odd field/value count returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSET", ["hash", "f1"], store)
    end

    test "HSET with no fields returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSET", ["hash"], store)
    end

    test "HSET with no args returns error" do
      assert {:error, _} = Hash.handle("HSET", [], MockStore.make())
    end

    test "HSET mixed new and existing fields returns only new count" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "existing", "old"], store)
      assert 1 == Hash.handle("HSET", ["hash", "existing", "updated", "new", "val"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HGET
  # ---------------------------------------------------------------------------

  describe "HGET" do
    test "HGET returns value for existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice"], store)
      assert "alice" == Hash.handle("HGET", ["hash", "name"], store)
    end

    test "HGET returns nil for missing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert nil == Hash.handle("HGET", ["hash", "missing"], store)
    end

    test "HGET returns nil for missing key" do
      store = MockStore.make()
      assert nil == Hash.handle("HGET", ["nonexistent", "field"], store)
    end

    test "HGET with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HGET", ["key"], MockStore.make())
      assert {:error, _} = Hash.handle("HGET", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HDEL
  # ---------------------------------------------------------------------------

  describe "HDEL" do
    test "HDEL removes existing field and returns 1" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 1 == Hash.handle("HDEL", ["hash", "f1"], store)
      assert nil == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HDEL on missing field returns 0" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 0 == Hash.handle("HDEL", ["hash", "missing"], store)
    end

    test "HDEL multiple fields returns count of removed" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2", "f3", "v3"], store)
      assert 2 == Hash.handle("HDEL", ["hash", "f1", "f3", "f4"], store)
    end

    test "HDEL with no fields returns error" do
      assert {:error, _} = Hash.handle("HDEL", ["hash"], MockStore.make())
    end

    test "HDEL cleans up type metadata when hash becomes empty" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HDEL", ["hash", "f1"], store)
      # After deleting all fields, the type metadata should be gone
      assert nil == store.compound_get.("hash", "T:hash")
    end
  end

  # ---------------------------------------------------------------------------
  # HMGET
  # ---------------------------------------------------------------------------

  describe "HMGET" do
    test "HMGET returns values for multiple fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      assert ["v1", "v2"] == Hash.handle("HMGET", ["hash", "f1", "f2"], store)
    end

    test "HMGET returns nil for missing fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert ["v1", nil] == Hash.handle("HMGET", ["hash", "f1", "missing"], store)
    end

    test "HMGET on nonexistent key returns all nils" do
      store = MockStore.make()
      assert [nil, nil] == Hash.handle("HMGET", ["nonexistent", "f1", "f2"], store)
    end

    test "HMGET uses compound_batch_get when the store provides it" do
      type_key = CompoundKey.type_key("hash")

      field_keys = [
        CompoundKey.hash_field("hash", "f1"),
        CompoundKey.hash_field("hash", "missing")
      ]

      store = %{
        compound_get: fn
          "hash", ^type_key ->
            nil

          "hash", compound_key ->
            flunk(
              "HMGET should use compound_batch_get, got per-field lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "hash", ^field_keys -> ["v1", nil] end
      }

      assert ["v1", nil] == Hash.handle("HMGET", ["hash", "f1", "missing"], store)
    end

    test "HMGET with no fields returns error" do
      assert {:error, _} = Hash.handle("HMGET", ["hash"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HGETALL
  # ---------------------------------------------------------------------------

  describe "HGETALL" do
    test "HGETALL returns flat field-value list" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice", "age", "30"], store)
      result = Hash.handle("HGETALL", ["hash"], store)
      assert is_list(result)
      assert length(result) == 4
      # Convert flat list to map for easier assertion
      map = result |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
      assert map["name"] == "alice"
      assert map["age"] == "30"
    end

    test "HGETALL on empty/nonexistent hash returns empty list" do
      store = MockStore.make()
      assert [] == Hash.handle("HGETALL", ["nonexistent"], store)
    end

    test "HGETALL with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HGETALL", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HLEN
  # ---------------------------------------------------------------------------

  describe "HLEN" do
    test "HLEN returns number of fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2", "f3", "v3"], store)
      assert 3 == Hash.handle("HLEN", ["hash"], store)
    end

    test "HLEN returns 0 for nonexistent key" do
      store = MockStore.make()
      assert 0 == Hash.handle("HLEN", ["nonexistent"], store)
    end

    test "HLEN with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HLEN", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HEXISTS
  # ---------------------------------------------------------------------------

  describe "HEXISTS" do
    test "HEXISTS returns 1 for existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 1 == Hash.handle("HEXISTS", ["hash", "f1"], store)
    end

    test "HEXISTS returns 0 for missing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert 0 == Hash.handle("HEXISTS", ["hash", "missing"], store)
    end

    test "HEXISTS returns 0 for nonexistent key" do
      store = MockStore.make()
      assert 0 == Hash.handle("HEXISTS", ["nonexistent", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HKEYS and HVALS
  # ---------------------------------------------------------------------------

  describe "HKEYS" do
    test "HKEYS returns all field names" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2"], store)
      keys = Hash.handle("HKEYS", ["hash"], store)
      assert Enum.sort(keys) == ["a", "b"]
    end

    test "HKEYS on nonexistent key returns empty list" do
      assert [] == Hash.handle("HKEYS", ["nonexistent"], MockStore.make())
    end
  end

  describe "HVALS" do
    test "HVALS returns all field values" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2"], store)
      vals = Hash.handle("HVALS", ["hash"], store)
      assert Enum.sort(vals) == ["1", "2"]
    end

    test "HVALS on nonexistent key returns empty list" do
      assert [] == Hash.handle("HVALS", ["nonexistent"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HSETNX
  # ---------------------------------------------------------------------------

  describe "HSETNX" do
    test "HSETNX sets field when not present" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSETNX", ["hash", "f1", "v1"], store)
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HSETNX does not overwrite existing field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "original"], store)
      assert 0 == Hash.handle("HSETNX", ["hash", "f1", "new"], store)
      assert "original" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HSETNX with wrong arity returns error" do
      assert {:error, _} = Hash.handle("HSETNX", ["hash", "f1"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # HINCRBY
  # ---------------------------------------------------------------------------

  describe "HINCRBY" do
    test "HINCRBY increments integer field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "counter", "10"], store)
      assert 15 == Hash.handle("HINCRBY", ["hash", "counter", "5"], store)
    end

    test "HINCRBY creates field with increment when missing" do
      store = MockStore.make()
      assert 5 == Hash.handle("HINCRBY", ["hash", "counter", "5"], store)
      assert "5" == Hash.handle("HGET", ["hash", "counter"], store)
    end

    test "HINCRBY allows exact int64 boundaries for missing fields" do
      store = MockStore.make()

      assert 9_223_372_036_854_775_807 ==
               Hash.handle("HINCRBY", ["hash", "max", "9223372036854775807"], store)

      assert -9_223_372_036_854_775_808 ==
               Hash.handle("HINCRBY", ["hash", "min", "-9223372036854775808"], store)
    end

    test "HINCRBY errors when increment exceeds int64 bounds and leaves field missing" do
      store = MockStore.make()

      assert {:error, "ERR increment or decrement would overflow"} =
               Hash.handle("HINCRBY", ["hash", "counter", "9223372036854775808"], store)

      assert nil == Hash.handle("HGET", ["hash", "counter"], store)
    end

    test "HINCRBY with negative increment decrements" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "counter", "10"], store)
      assert 7 == Hash.handle("HINCRBY", ["hash", "counter", "-3"], store)
    end

    test "HINCRBY errors when result exceeds max int64 and leaves field unchanged" do
      store = MockStore.make()
      max_minus_one = "9223372036854775806"
      Hash.handle("HSET", ["hash", "counter", max_minus_one], store)

      assert {:error, "ERR increment or decrement would overflow"} =
               Hash.handle("HINCRBY", ["hash", "counter", "2"], store)

      assert max_minus_one == Hash.handle("HGET", ["hash", "counter"], store)
    end

    test "HINCRBY errors when result is below min int64 and leaves field unchanged" do
      store = MockStore.make()
      min_plus_one = "-9223372036854775807"
      Hash.handle("HSET", ["hash", "counter", min_plus_one], store)

      assert {:error, "ERR increment or decrement would overflow"} =
               Hash.handle("HINCRBY", ["hash", "counter", "-2"], store)

      assert min_plus_one == Hash.handle("HGET", ["hash", "counter"], store)
    end

    test "HINCRBY on non-integer field returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice"], store)
      assert {:error, _} = Hash.handle("HINCRBY", ["hash", "name", "1"], store)
    end

    test "HINCRBY with non-integer increment returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HINCRBY", ["hash", "f1", "abc"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HINCRBYFLOAT
  # ---------------------------------------------------------------------------

  describe "HINCRBYFLOAT" do
    test "HINCRBYFLOAT increments float field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "price", "10.5"], store)
      result = Hash.handle("HINCRBYFLOAT", ["hash", "price", "1.5"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 12.0, val, 0.001
    end

    test "HINCRBYFLOAT creates field when missing" do
      store = MockStore.make()
      result = Hash.handle("HINCRBYFLOAT", ["hash", "price", "3.14"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 3.14, val, 0.001
    end

    test "HINCRBYFLOAT on non-numeric field returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice"], store)
      assert {:error, _} = Hash.handle("HINCRBYFLOAT", ["hash", "name", "1.0"], store)
    end

    test "HINCRBYFLOAT with non-float increment returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HINCRBYFLOAT", ["hash", "f1", "abc"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HEXPIRE
  # ---------------------------------------------------------------------------

  describe "HEXPIRE" do
    test "HEXPIRE sets expiry on existing fields and returns 1 per field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      assert [1, 1] == Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "2", "f1", "f2"], store)
    end

    test "HEXPIRE returns -2 for non-existent fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)

      assert [1, -2] ==
               Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "2", "f1", "missing"], store)
    end

    test "HEXPIRE returns all -2 for non-existent key" do
      store = MockStore.make()
      assert [-2, -2] == Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "2", "f1", "f2"], store)
    end

    test "HEXPIRE with single field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [1] == Hash.handle("HEXPIRE", ["hash", "5", "FIELDS", "1", "f1"], store)
    end

    test "HEXPIRE preserves the field value" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "1", "f1"], store)
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HEXPIRE with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HEXPIRE", ["hash"], store)
    end

    test "HEXPIRE with non-integer seconds returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HEXPIRE", ["hash", "abc", "FIELDS", "1", "f1"], store)
    end

    test "HEXPIRE with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "3", "f1"], store)
    end

    test "HEXPIRE with negative seconds returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HEXPIRE", ["hash", "-1", "FIELDS", "1", "f1"], store)
    end

    test "HEXPIRE with zero seconds returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HEXPIRE", ["hash", "0", "FIELDS", "1", "f1"], store)
    end

    test "HEXPIRE on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HEXPIRE", ["mykey", "10", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HTTL
  # ---------------------------------------------------------------------------

  describe "HTTL" do
    test "HTTL returns -1 for fields with no expiry" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      assert [-1, -1] == Hash.handle("HTTL", ["hash", "FIELDS", "2", "f1", "f2"], store)
    end

    test "HTTL returns -2 for non-existent fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [-1, -2] == Hash.handle("HTTL", ["hash", "FIELDS", "2", "f1", "missing"], store)
    end

    test "HTTL returns all -2 for non-existent key" do
      store = MockStore.make()
      assert [-2, -2] == Hash.handle("HTTL", ["hash", "FIELDS", "2", "f1", "f2"], store)
    end

    test "HTTL after HEXPIRE returns remaining seconds" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "1", "f1"], store)
      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      # TTL should be close to 60 (within a small delta for test execution time)
      assert ttl >= 58 and ttl <= 60
    end

    test "HTTL with multiple fields, mixed expiry" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)
      Hash.handle("HEXPIRE", ["hash", "120", "FIELDS", "1", "f1"], store)
      [ttl_f1, ttl_f2] = Hash.handle("HTTL", ["hash", "FIELDS", "2", "f1", "f2"], store)
      assert ttl_f1 >= 118 and ttl_f1 <= 120
      assert ttl_f2 == -1
    end

    test "HTTL with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HTTL", ["hash"], store)
    end

    test "HTTL with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HTTL", ["hash", "FIELDS", "3", "f1"], store)
    end

    test "HTTL on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HTTL", ["mykey", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HPERSIST
  # ---------------------------------------------------------------------------

  describe "HPERSIST" do
    test "HPERSIST removes expiry and returns 1" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "1", "f1"], store)
      assert [1] == Hash.handle("HPERSIST", ["hash", "FIELDS", "1", "f1"], store)
    end

    test "HPERSIST on field without expiry returns -1" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [-1] == Hash.handle("HPERSIST", ["hash", "FIELDS", "1", "f1"], store)
    end

    test "HPERSIST returns -2 for non-existent fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert [-2] == Hash.handle("HPERSIST", ["hash", "FIELDS", "1", "missing"], store)
    end

    test "HPERSIST returns all -2 for non-existent key" do
      store = MockStore.make()
      assert [-2, -2] == Hash.handle("HPERSIST", ["hash", "FIELDS", "2", "f1", "f2"], store)
    end

    test "HPERSIST with multiple fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2", "f3", "v3"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "2", "f1", "f2"], store)

      assert [1, 1, -1] ==
               Hash.handle("HPERSIST", ["hash", "FIELDS", "3", "f1", "f2", "f3"], store)
    end

    test "HPERSIST after removing expiry, HTTL returns -1" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "1", "f1"], store)
      Hash.handle("HPERSIST", ["hash", "FIELDS", "1", "f1"], store)
      assert [-1] == Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
    end

    test "HPERSIST preserves the field value" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "60", "FIELDS", "1", "f1"], store)
      Hash.handle("HPERSIST", ["hash", "FIELDS", "1", "f1"], store)
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HPERSIST with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HPERSIST", ["hash"], store)
    end

    test "HPERSIST with mismatched field count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HPERSIST", ["hash", "FIELDS", "3", "f1"], store)
    end

    test "HPERSIST on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HPERSIST", ["mykey", "FIELDS", "1", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HEXPIRE + field expiration integration
  # ---------------------------------------------------------------------------

  describe "hash field TTL integration" do
    test "field becomes invisible after very short expiry" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      # Set expiry to 1 second, then manually expire it via compound_put
      # with an expire_at_ms in the past
      compound_key = <<"H:hash", 0, "f1">>
      store.compound_put.("hash", compound_key, "v1", System.os_time(:millisecond) - 1)
      # Now the field should appear expired
      assert nil == Hash.handle("HGET", ["hash", "f1"], store)
    end

    test "HTTL returns -2 for expired field" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      # Set expire_at_ms in the past
      compound_key = <<"H:hash", 0, "f1">>
      store.compound_put.("hash", compound_key, "v1", System.os_time(:millisecond) - 1)
      assert [-2] == Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
    end

    test "fully expired hash is no longer visible to TYPE or EXISTS" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      compound_key = <<"H:hash", 0, "f1">>
      store.compound_put.("hash", compound_key, "v1", System.os_time(:millisecond) - 1)

      assert nil == Hash.handle("HGET", ["hash", "f1"], store)
      assert 0 == Hash.handle("HLEN", ["hash"], store)
      assert {:simple, "none"} == Generic.handle("TYPE", ["hash"], store)
      assert 0 == Strings.handle("EXISTS", ["hash"], store)
    end

    test "fully expired hash does not block string reads and writes before TYPE cleanup" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      compound_key = <<"H:hash", 0, "f1">>
      store.compound_put.("hash", compound_key, "v1", System.os_time(:millisecond) - 1)

      assert nil == Strings.handle("GET", ["hash"], store)
      assert 1 == Strings.handle("SETNX", ["hash", "fresh"], store)
      assert "fresh" == Strings.handle("GET", ["hash"], store)
    end

    test "HEXPIRE then HEXPIRE overwrites the TTL" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "1", "f1"], store)
      Hash.handle("HEXPIRE", ["hash", "300", "FIELDS", "1", "f1"], store)
      [ttl] = Hash.handle("HTTL", ["hash", "FIELDS", "1", "f1"], store)
      assert ttl >= 298 and ttl <= 300
    end
  end

  # ---------------------------------------------------------------------------
  # Type enforcement
  # ---------------------------------------------------------------------------

  describe "type enforcement" do
    test "HSET on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HSET", ["mykey", "f", "v"], store)
    end

    test "HGET on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HGET", ["mykey", "f"], store)
    end

    test "HGETALL on a key used as zset returns WRONGTYPE" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HGETALL", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HSCAN
  # ---------------------------------------------------------------------------

  describe "HSCAN" do
    test "HSCAN with cursor 0 returns first batch of field-value pairs" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2", "c", "3"], store)
      [cursor, elements] = Hash.handle("HSCAN", ["hash", "0"], store)
      # With default count 10 and only 3 fields, should return all and cursor "0"
      assert cursor == "0"
      # Elements is a flat list [field, value, field, value, ...]
      assert length(elements) == 6
      pairs = elements |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
      assert pairs["a"] == "1"
      assert pairs["b"] == "2"
      assert pairs["c"] == "3"
    end

    test "HSCAN with COUNT limits batch size and returns continuation cursor" do
      store = MockStore.make()

      for i <- 1..20 do
        Hash.handle(
          "HSET",
          ["hash", "field#{String.pad_leading(Integer.to_string(i), 2, "0")}", "val#{i}"],
          store
        )
      end

      [cursor, elements] = Hash.handle("HSCAN", ["hash", "0", "COUNT", "5"], store)
      assert cursor != "0"
      # Should return exactly 5 field-value pairs (10 elements)
      assert length(elements) == 10
    end

    test "HSCAN iterates through all elements with cursor continuation" do
      store = MockStore.make()

      for i <- 1..10 do
        Hash.handle(
          "HSET",
          ["hash", "f#{String.pad_leading(Integer.to_string(i), 2, "0")}", "v#{i}"],
          store
        )
      end

      # First batch: 3 elements
      [cursor1, batch1] = Hash.handle("HSCAN", ["hash", "0", "COUNT", "3"], store)
      assert cursor1 != "0"
      assert length(batch1) == 6

      # Continue scanning
      all_fields = collect_hscan_fields(store, "hash", "0", 3)
      assert length(all_fields) == 10
    end

    test "HSCAN with MATCH filters fields by pattern" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "name", "alice", "age", "30", "nickname", "ali"], store)
      [cursor, elements] = Hash.handle("HSCAN", ["hash", "0", "MATCH", "na*"], store)
      assert cursor == "0"
      pairs = elements |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
      assert Map.has_key?(pairs, "name")
      refute Map.has_key?(pairs, "age")
      refute Map.has_key?(pairs, "nickname")
    end

    test "HSCAN with MATCH and COUNT together" do
      store = MockStore.make()

      for i <- 1..10 do
        Hash.handle("HSET", ["hash", "alpha_#{i}", "a#{i}"], store)
        Hash.handle("HSET", ["hash", "beta_#{i}", "b#{i}"], store)
      end

      [_cursor, elements] =
        Hash.handle("HSCAN", ["hash", "0", "MATCH", "alpha_*", "COUNT", "5"], store)

      # All matched fields should be alpha_*
      fields = elements |> Enum.chunk_every(2) |> Enum.map(fn [k, _v] -> k end)
      assert Enum.all?(fields, &String.starts_with?(&1, "alpha_"))
    end

    test "HSCAN on nonexistent key returns cursor 0 and empty list" do
      store = MockStore.make()
      [cursor, elements] = Hash.handle("HSCAN", ["nonexistent", "0"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "HSCAN with invalid cursor returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSCAN", ["hash", "notanumber"], store)
    end

    test "HSCAN with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HSCAN", ["hash"], store)
      assert {:error, _} = Hash.handle("HSCAN", [], store)
    end

    test "HSCAN with invalid COUNT returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, _} = Hash.handle("HSCAN", ["hash", "0", "COUNT", "abc"], store)
    end

    test "HSCAN on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HSCAN", ["mykey", "0"], store)
    end

    test "HSCAN full iteration collects all fields exactly once" do
      store = MockStore.make()

      expected =
        for i <- 1..15,
            into: %{},
            do: {"key#{String.pad_leading(Integer.to_string(i), 2, "0")}", "val#{i}"}

      for {k, v} <- expected do
        Hash.handle("HSET", ["hash", k, v], store)
      end

      all_fields = collect_hscan_fields(store, "hash", "0", 4)
      result_map = Map.new(all_fields, fn {k, v} -> {k, v} end)
      assert result_map == expected
    end
  end

  # ---------------------------------------------------------------------------
  # HRANDFIELD
  # ---------------------------------------------------------------------------

  describe "HRANDFIELD" do
    test "HRANDFIELD returns a single random field name" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2", "c", "3"], store)
      result = Hash.handle("HRANDFIELD", ["hash"], store)
      assert result in ["a", "b", "c"]
    end

    test "HRANDFIELD with positive count returns up to count unique fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2", "c", "3"], store)
      result = Hash.handle("HRANDFIELD", ["hash", "2"], store)
      assert is_list(result)
      assert length(result) == 2
      # All elements should be unique
      assert length(Enum.uniq(result)) == 2
      assert Enum.all?(result, &(&1 in ["a", "b", "c"]))
    end

    test "HRANDFIELD with count > hash size returns all fields" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2"], store)
      result = Hash.handle("HRANDFIELD", ["hash", "10"], store)
      assert Enum.sort(result) == ["a", "b"]
    end

    test "HRANDFIELD with negative count allows duplicates" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1"], store)
      result = Hash.handle("HRANDFIELD", ["hash", "-5"], store)
      assert is_list(result)
      assert length(result) == 5
      # With only one field, all should be "a"
      assert Enum.all?(result, &(&1 == "a"))
    end

    test "HRANDFIELD with count 0 returns empty list" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1"], store)
      result = Hash.handle("HRANDFIELD", ["hash", "0"], store)
      assert result == []
    end

    test "HRANDFIELD with WITHVALUES returns field-value pairs" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1", "b", "2", "c", "3"], store)
      result = Hash.handle("HRANDFIELD", ["hash", "2", "WITHVALUES"], store)
      assert is_list(result)
      # 2 fields * 2 (field + value) = 4
      assert length(result) == 4
      pairs = result |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
      # Verify field-value correspondence
      Enum.each(pairs, fn {k, v} ->
        assert Hash.handle("HGET", ["hash", k], store) == v
      end)
    end

    test "HRANDFIELD with negative count and WITHVALUES" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1"], store)
      result = Hash.handle("HRANDFIELD", ["hash", "-3", "WITHVALUES"], store)
      assert length(result) == 6
      pairs = result |> Enum.chunk_every(2)
      assert Enum.all?(pairs, fn [k, v] -> k == "a" and v == "1" end)
    end

    test "HRANDFIELD on nonexistent key returns nil" do
      store = MockStore.make()
      result = Hash.handle("HRANDFIELD", ["nonexistent"], store)
      assert result == nil
    end

    test "HRANDFIELD with count on nonexistent key returns empty list" do
      store = MockStore.make()
      result = Hash.handle("HRANDFIELD", ["nonexistent", "5"], store)
      assert result == []
    end

    test "HRANDFIELD with non-integer count returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1"], store)
      assert {:error, _} = Hash.handle("HRANDFIELD", ["hash", "abc"], store)
    end

    test "HRANDFIELD with wrong number of arguments returns error" do
      store = MockStore.make()
      assert {:error, _} = Hash.handle("HRANDFIELD", [], store)
    end

    test "HRANDFIELD on wrong type returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HRANDFIELD", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Individual field storage verification
  # ---------------------------------------------------------------------------

  describe "individual field storage" do
    test "fields are stored as separate compound keys" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)

      # Each field should be individually accessible via compound_get
      assert "v1" == store.compound_get.("hash", <<"H:hash", 0, "f1">>)
      assert "v2" == store.compound_get.("hash", <<"H:hash", 0, "f2">>)
    end

    test "HGET reads individual field without loading entire hash" do
      store = MockStore.make()
      # Pre-populate compound keys directly (simulating stored data)
      store.compound_put.("hash", <<"H:hash", 0, "f1">>, "v1", 0)
      store.compound_put.("hash", <<"H:hash", 0, "f2">>, "v2", 0)
      store.compound_put.("hash", "T:hash", "hash", 0)

      # HGET should read just the one field
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Private test helpers
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Edge cases: arity, WRONGTYPE, empty field, HSCAN cursor
  # ---------------------------------------------------------------------------

  describe "arity edge cases" do
    test "HEXISTS with no args returns error" do
      assert {:error, msg} = Hash.handle("HEXISTS", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HEXISTS with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Hash.handle("HEXISTS", ["hash", "f1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "HLEN with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Hash.handle("HLEN", ["hash", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "HKEYS with no args returns error" do
      assert {:error, msg} = Hash.handle("HKEYS", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HKEYS with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Hash.handle("HKEYS", ["hash", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "HVALS with no args returns error" do
      assert {:error, msg} = Hash.handle("HVALS", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HVALS with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Hash.handle("HVALS", ["hash", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "HINCRBY with no args returns error" do
      assert {:error, msg} = Hash.handle("HINCRBY", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HINCRBY with only key returns error" do
      assert {:error, msg} = Hash.handle("HINCRBY", ["hash"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HINCRBY with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Hash.handle("HINCRBY", ["hash", "f", "1", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "HINCRBYFLOAT with no args returns error" do
      assert {:error, msg} = Hash.handle("HINCRBYFLOAT", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HINCRBYFLOAT with only key returns error" do
      assert {:error, msg} = Hash.handle("HINCRBYFLOAT", ["hash"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HINCRBYFLOAT with extra args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Hash.handle("HINCRBYFLOAT", ["hash", "f", "1.0", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "HSETNX with no args returns error" do
      assert {:error, msg} = Hash.handle("HSETNX", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "HRANDFIELD with 4+ args returns error" do
      store = MockStore.make()
      assert {:error, msg} = Hash.handle("HRANDFIELD", ["h", "2", "WITHVALUES", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "WRONGTYPE enforcement edge cases" do
    test "HDEL on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HDEL", ["mykey", "f1"], store)
    end

    test "HMGET on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HMGET", ["mykey", "f1"], store)
    end

    test "HLEN on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HLEN", ["mykey"], store)
    end

    test "HEXISTS on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HEXISTS", ["mykey", "f"], store)
    end

    test "HKEYS on a key used as zset returns WRONGTYPE" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HKEYS", ["mykey"], store)
    end

    test "HVALS on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HVALS", ["mykey"], store)
    end

    test "HSETNX on a key used as set returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HSETNX", ["mykey", "f", "v"], store)
    end

    test "HINCRBY on a key used as list returns WRONGTYPE" do
      store = MockStore.make()
      List.handle("LPUSH", ["mykey", "elem"], store)
      assert {:error, "WRONGTYPE" <> _} = Hash.handle("HINCRBY", ["mykey", "f", "1"], store)
    end

    test "HINCRBYFLOAT on a key used as zset returns WRONGTYPE" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["mykey", "1.0", "member"], store)

      assert {:error, "WRONGTYPE" <> _} =
               Hash.handle("HINCRBYFLOAT", ["mykey", "f", "1.0"], store)
    end
  end

  describe "empty field handling" do
    test "HSET with empty string field name works" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["hash", "", "value"], store)
      assert "value" == Hash.handle("HGET", ["hash", ""], store)
    end

    test "HGET with empty string field on existing hash returns nil" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert nil == Hash.handle("HGET", ["hash", ""], store)
    end
  end

  describe "HSCAN cursor edge cases" do
    test "HSCAN with negative cursor returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, "ERR invalid cursor"} = Hash.handle("HSCAN", ["hash", "-1"], store)
    end

    test "HSCAN with very large cursor beyond set size returns cursor 0 and empty list" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      [cursor, elements] = Hash.handle("HSCAN", ["hash", "999999"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "HSCAN with odd trailing option returns error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert {:error, "ERR syntax error"} = Hash.handle("HSCAN", ["hash", "0", "MATCH"], store)
    end

    test "HSCAN with unknown option returns error" do
      store = MockStore.make()

      assert {:error, "ERR syntax error"} =
               Hash.handle("HSCAN", ["hash", "0", "BOGUS", "val"], store)
    end
  end

  describe "HRANDFIELD edge cases" do
    test "HRANDFIELD with invalid WITHVALUES string returns syntax error" do
      store = MockStore.make()
      Hash.handle("HSET", ["hash", "a", "1"], store)

      assert {:error, "ERR syntax error"} =
               Hash.handle("HRANDFIELD", ["hash", "1", "BOGUS"], store)
    end

    test "HRANDFIELD with negative count on empty hash returns empty list" do
      store = MockStore.make()
      result = Hash.handle("HRANDFIELD", ["hash", "-5"], store)
      assert result == []
    end
  end

  defp collect_hscan_fields(store, key, cursor, count) do
    collect_hscan_fields(store, key, cursor, count, [])
  end

  defp collect_hscan_fields(store, key, cursor, count, acc) do
    [next_cursor, elements] =
      Hash.handle("HSCAN", [key, cursor, "COUNT", Integer.to_string(count)], store)

    pairs = elements |> Enum.chunk_every(2) |> Enum.map(fn [k, v] -> {k, v} end)
    new_acc = acc ++ pairs

    if next_cursor == "0" do
      new_acc
    else
      collect_hscan_fields(store, key, next_cursor, count, new_acc)
    end
  end
end
