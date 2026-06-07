defmodule Ferricstore.Commands.HashTest.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Generic
      alias Ferricstore.Commands.Hash
      alias Ferricstore.Commands.List
      alias Ferricstore.Commands.Set
      alias Ferricstore.Commands.SortedSet
      alias Ferricstore.Commands.Strings
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

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

    test "HSET batches field existence reads and writes final duplicate value once" do
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
              "HSET should use compound_batch_get, got per-field lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get, field_keys})
          [nil, "old", nil]
        end,
        compound_batch_put: fn "hash", entries ->
          send(parent, {:compound_batch_put, entries})
          :ok
        end,
        compound_put: fn "hash", compound_key, _value, 0 ->
          flunk(
            "HSET should use compound_batch_put, got per-field write #{inspect(compound_key)}"
          )
        end
      }

      assert 2 ==
               Hash.handle(
                 "HSET",
                 ["hash", "f1", "v1", "existing", "new", "f1", "v2", "f2", "v3"],
                 store
               )

      assert_received {:compound_batch_get, ^field_keys}
      assert_received {:compound_batch_put, entries}

      assert Enum.sort(entries) ==
               Enum.sort([
                 {Enum.at(field_keys, 0), "v2", 0},
                 {Enum.at(field_keys, 1), "new", 0},
                 {Enum.at(field_keys, 2), "v3", 0}
               ])

      refute_received {:compound_batch_put, _}
    end

    test "HSET builds write entries and added count in one pass" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/commands/hash/field_ops.ex", __DIR__))

      [hset_source] = Regex.run(~r/defp set_pairs\(.*?^  end/ms, source)

      assert hset_source =~ "put_entries(fields, compound_keys, existing_values,"
      refute hset_source =~ "Enum.count(&is_nil/1)"
      refute hset_source =~ "|> Enum.zip(compound_keys)"
    end

    test "multi-field hash metadata maps are built in one pass" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/commands/hash/field_ops.ex", __DIR__))

      [batch_meta_source] = Regex.run(~r/defp batch_field_metas\(.*?^  end/ms, source)

      assert batch_meta_source =~ "metas_by_field(unique_fields, metas, %{})"
      refute batch_meta_source =~ "then(&Enum.zip(unique_fields, &1))"
      refute batch_meta_source =~ "|> Map.new()"
    end

    test "multi-field hash metadata writes are built in one pass" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/commands/hash/field_ops.ex", __DIR__))

      assert source =~ "persistent_field_entries(unique_fields, compound_keys, metas_by_field, [])"

      [existing_source] = Regex.run(~r/defp existing_field_entries\(.*?^  end/ms, source)

      assert existing_source =~ "existing_field_entries(unique_fields, compound_keys,"
      refute existing_source =~ "Enum.zip(compound_keys)"
      refute existing_source =~ "Enum.flat_map"
    end

    test "HSET rolls back new type metadata when field write fails" do
      parent = self()
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "f1")

      store = %{
        compound_get: fn
          "hash", ^type_key -> nil
          "hash", ^field_key -> nil
        end,
        compound_put: fn "hash", ^type_key, "hash", 0 ->
          send(parent, :type_written)
          :ok
        end,
        compound_batch_get: fn "hash", [^field_key] -> [nil] end,
        compound_batch_put: fn "hash", [{^field_key, "v1", 0}] ->
          {:error, :disk_full}
        end,
        compound_delete: fn "hash", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, :disk_full} == Hash.handle("HSET", ["hash", "f1", "v1"], store)
      assert_received :type_written
      assert_received :type_deleted
    end

    test "HSET preserves existing type metadata when later field write fails" do
      parent = self()
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "f1")

      store = %{
        compound_get: fn
          "hash", ^type_key -> "hash"
          "hash", ^field_key -> nil
        end,
        compound_batch_get: fn "hash", [^field_key] -> [nil] end,
        compound_batch_put: fn "hash", [{^field_key, "v1", 0}] ->
          {:error, :disk_full}
        end,
        compound_delete: fn "hash", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, :disk_full} == Hash.handle("HSET", ["hash", "f1", "v1"], store)
      refute_received :type_deleted
    end
  end
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

    test "HDEL batches field existence reads and removes duplicates once" do
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
              "HDEL should use compound_batch_get, got per-field lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get_meta: fn "hash", ^field_keys ->
          send(parent, {:compound_batch_get_meta, field_keys})
          [{"v1", 0}, {"v2", 0}, nil]
        end,
        compound_batch_get: fn "hash", compound_keys ->
          flunk(
            "HDEL should use compound_batch_get_meta, got value-only lookup #{inspect(compound_keys)}"
          )
        end,
        compound_batch_delete: fn "hash", compound_keys ->
          send(parent, {:compound_batch_delete, compound_keys})
          :ok
        end,
        compound_delete: fn "hash", compound_key ->
          flunk(
            "HDEL should use compound_batch_delete, got per-field delete #{inspect(compound_key)}"
          )
        end,
        compound_count: fn "hash", _prefix -> 1 end
      }

      assert 2 == Hash.handle("HDEL", ["hash", "f1", "f1", "f2", "missing"], store)
      assert_received {:compound_batch_get_meta, ^field_keys}
      assert_received {:compound_batch_delete, deleted_keys}
      assert Enum.sort(deleted_keys) == Enum.sort(Enum.take(field_keys, 2))
      refute_received {:compound_batch_delete, _}
    end

    test "HDEL builds deleted entries directly from batch metadata" do
      source = File.read!(Path.expand("../../../lib/ferricstore/commands/hash.ex", __DIR__))

      [hdel_source] = Regex.run(~r/defp hdel_args\(\[key \| fields\].*?^  end/ms, source)

      assert hdel_source =~ "hash_deleted_entries(compound_keys, metas, [])"
      refute hdel_source =~ "metas_by_key"
      refute hdel_source =~ "Enum.flat_map"
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

    test "HDEL returns type cleanup errors after removing the last field" do
      store = hash_cleanup_failure_store()

      assert {:error, :disk_full} == Hash.handle("HDEL", ["hash", "f1"], store)
    end

    test "HDEL preserves the last field when type cleanup fails" do
      base = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], base)
      type_key = CompoundKey.type_key("hash")

      store =
        Map.put(base, :compound_delete, fn
          "hash", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == Hash.handle("HDEL", ["hash", "f1"], store)
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], base)
    end
  end
  describe "HGETDEL" do
    test "HGETDEL builds results and deleted entries in one reducer" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/commands/hash/field_ops.ex", __DIR__))

      [hgetdel_source] = Regex.run(~r/def getdel_fields\(.*?^  end/ms, source)

      assert hgetdel_source =~ "getdel_results(fields, key, metas_by_key, [], %{}, [])"
      refute hgetdel_source =~ "deleted_entries_by_key"
      refute hgetdel_source =~ "Enum.map(deleted_entries"
    end

    test "HGETDEL preserves the last field when type cleanup fails" do
      base = MockStore.make()
      Hash.handle("HSET", ["hash", "f1", "v1"], base)
      type_key = CompoundKey.type_key("hash")

      store =
        Map.put(base, :compound_delete, fn
          "hash", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == Hash.handle("HGETDEL", ["hash", "FIELDS", "1", "f1"], store)
      assert "v1" == Hash.handle("HGET", ["hash", "f1"], base)
    end
  end
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

    test "hash field-value responses use shared flat-list helper" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/commands/hash.ex", __DIR__)) <>
          File.read!(Path.expand("../../../lib/ferricstore/commands/hash/helpers.ex", __DIR__))

      assert source =~ "Helpers.hash_pairs_to_flat_list(pairs)"
      assert source =~ "Helpers.hash_pairs_to_flat_list(batch)"
      assert source =~ "hash_pairs_to_flat_list(selected)"
      refute source =~ "Enum.flat_map(pairs, fn {field, value} -> [field, value] end)"
      refute source =~ "Enum.flat_map(batch, fn {field, value} -> [field, value] end)"
      refute source =~ "Enum.flat_map(selected, fn {field, value} -> [field, value] end)"
    end
  end
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

    test "HSETNX returns field write errors" do
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "f1")

      store = %{
        compound_get: fn
          "hash", ^type_key -> "hash"
          "hash", ^field_key -> nil
        end,
        compound_put: fn "hash", ^field_key, "v1", 0 -> {:error, "disk full"} end
      }

      assert {:error, "disk full"} == Hash.handle("HSETNX", ["hash", "f1", "v1"], store)
    end

    test "HSETNX rolls back new type metadata when field write fails" do
      parent = self()
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "f1")

      store = %{
        compound_get: fn
          "hash", ^type_key -> nil
          "hash", ^field_key -> nil
        end,
        compound_put: fn
          "hash", ^type_key, "hash", 0 ->
            send(parent, :type_written)
            :ok

          "hash", ^field_key, "v1", 0 ->
            {:error, "disk full"}
        end,
        compound_delete: fn "hash", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, "disk full"} == Hash.handle("HSETNX", ["hash", "f1", "v1"], store)
      assert_received :type_written
      assert_received :type_deleted
    end
  end
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

    test "HINCRBY with invalid increment does not create type metadata" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      store = %{
        compound_get: fn "hash", ^type_key -> nil end,
        compound_put: fn "hash", ^type_key, "hash", 0 ->
          send(parent, :type_written)
          :ok
        end
      }

      assert {:error, "ERR value is not an integer or out of range"} ==
               Hash.handle("HINCRBY", ["hash", "counter", "abc"], store)

      refute_received :type_written
    end

    test "HINCRBY with out-of-range increment does not create type metadata" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      store = %{
        compound_get: fn "hash", ^type_key -> nil end,
        compound_put: fn "hash", ^type_key, "hash", 0 ->
          send(parent, :type_written)
          :ok
        end
      }

      assert {:error, "ERR increment or decrement would overflow"} ==
               Hash.handle("HINCRBY", ["hash", "counter", "9223372036854775808"], store)

      refute_received :type_written
    end

    test "HINCRBY returns field write errors" do
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "counter")

      store = %{
        compound_get: fn
          "hash", ^type_key -> "hash"
          "hash", ^field_key -> nil
        end,
        compound_put: fn "hash", ^field_key, "5", 0 -> {:error, "disk full"} end
      }

      assert {:error, "disk full"} == Hash.handle("HINCRBY", ["hash", "counter", "5"], store)
    end

    test "HINCRBY rolls back new type metadata when field write fails" do
      parent = self()
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "counter")

      store = %{
        compound_get: fn
          "hash", ^type_key -> nil
          "hash", ^field_key -> nil
        end,
        compound_put: fn
          "hash", ^type_key, "hash", 0 ->
            send(parent, :type_written)
            :ok

          "hash", ^field_key, "5", 0 ->
            {:error, "disk full"}
        end,
        compound_delete: fn "hash", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, "disk full"} == Hash.handle("HINCRBY", ["hash", "counter", "5"], store)
      assert_received :type_written
      assert_received :type_deleted
    end
  end
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

    test "HINCRBYFLOAT with invalid increment does not create type metadata" do
      parent = self()
      type_key = CompoundKey.type_key("hash")

      store = %{
        compound_get: fn "hash", ^type_key -> nil end,
        compound_put: fn "hash", ^type_key, "hash", 0 ->
          send(parent, :type_written)
          :ok
        end
      }

      assert {:error, "ERR value is not a valid float"} ==
               Hash.handle("HINCRBYFLOAT", ["hash", "price", "abc"], store)

      refute_received :type_written
    end

    test "HINCRBYFLOAT returns field write errors" do
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "price")

      store = %{
        compound_get: fn
          "hash", ^type_key -> "hash"
          "hash", ^field_key -> nil
        end,
        compound_put: fn "hash", ^field_key, value, 0 when is_binary(value) ->
          {:error, "disk full"}
        end
      }

      assert {:error, "disk full"} ==
               Hash.handle("HINCRBYFLOAT", ["hash", "price", "3.14"], store)
    end

    test "HINCRBYFLOAT rolls back new type metadata when field write fails" do
      parent = self()
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "price")

      store = %{
        compound_get: fn
          "hash", ^type_key -> nil
          "hash", ^field_key -> nil
        end,
        compound_put: fn
          "hash", ^type_key, "hash", 0 ->
            send(parent, :type_written)
            :ok

          "hash", ^field_key, value, 0 when is_binary(value) ->
            {:error, "disk full"}
        end,
        compound_delete: fn "hash", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, "disk full"} ==
               Hash.handle("HINCRBYFLOAT", ["hash", "price", "3.14"], store)

      assert_received :type_written
      assert_received :type_deleted
    end
  end
  describe "HSETEX" do
    test "HSETEX rolls back new type metadata when field write fails" do
      parent = self()
      type_key = CompoundKey.type_key("hash")
      field_key = CompoundKey.hash_field("hash", "f1")

      store = %{
        compound_get: fn
          "hash", ^type_key -> nil
          "hash", ^field_key -> nil
        end,
        compound_put: fn "hash", ^type_key, "hash", 0 ->
          send(parent, :type_written)
          :ok
        end,
        compound_batch_get: fn "hash", [^field_key] -> [nil] end,
        compound_batch_put: fn "hash", [{^field_key, "v1", expire_at_ms}]
                               when is_integer(expire_at_ms) ->
          {:error, :disk_full}
        end,
        compound_delete: fn "hash", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, :disk_full} == Hash.handle("HSETEX", ["hash", "60", "f1", "v1"], store)
      assert_received :type_written
      assert_received :type_deleted
    end
  end
    end
  end
end
