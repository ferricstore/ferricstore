defmodule Ferricstore.Commands.HashTest.Sections.Hexpire do
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

      describe "HEXPIRE" do
        test "HEXPIRE sets expiry on existing fields and returns 1 per field" do
          store = MockStore.make()
          Hash.handle("HSET", ["hash", "f1", "v1", "f2", "v2"], store)

          assert [1, 1] ==
                   Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "2", "f1", "f2"], store)
        end

        test "HEXPIRE returns -2 for non-existent fields" do
          store = MockStore.make()
          Hash.handle("HSET", ["hash", "f1", "v1"], store)

          assert [1, -2] ==
                   Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "2", "f1", "missing"], store)
        end

        test "HEXPIRE returns all -2 for non-existent key" do
          store = MockStore.make()

          assert [-2, -2] ==
                   Hash.handle("HEXPIRE", ["hash", "10", "FIELDS", "2", "f1", "f2"], store)
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

        test "HEXPIRE batches field meta reads and writes each existing duplicate field once" do
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
                flunk(
                  "HEXPIRE should only use compound_get for type, got #{inspect(compound_key)}"
                )
            end,
            compound_get_meta: fn "hash", compound_key ->
              flunk("HEXPIRE should use compound_batch_get_meta, got #{inspect(compound_key)}")
            end,
            compound_batch_get_meta: fn "hash", ^field_keys ->
              send(parent, {:compound_batch_get_meta, field_keys})
              [{"v1", 0}, nil, {"v2", 123}]
            end,
            compound_batch_put: fn "hash", entries ->
              send(parent, {:compound_batch_put, entries})
              :ok
            end,
            compound_put: fn "hash", compound_key, _value, _expire_at_ms ->
              flunk(
                "HEXPIRE should use compound_batch_put, got per-field write #{inspect(compound_key)}"
              )
            end
          }

          assert [1, 1, -2, 1] ==
                   Hash.handle(
                     "HEXPIRE",
                     ["hash", "60", "FIELDS", "4", "f1", "f1", "missing", "f2"],
                     store
                   )

          assert_received {:compound_batch_get_meta, ^field_keys}
          assert_received {:compound_batch_put, entries}

          assert [{first_key, "v1", expire_at_ms}, {second_key, "v2", same_expire_at_ms}] =
                   entries

          assert first_key == Enum.at(field_keys, 0)
          assert second_key == Enum.at(field_keys, 2)
          assert same_expire_at_ms == expire_at_ms
          assert expire_at_ms > 0
          refute_received {:compound_batch_put, _}
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

        test "HTTL batches field meta reads and preserves duplicate field results" do
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
                flunk("HTTL should only use compound_get for type, got #{inspect(compound_key)}")
            end,
            compound_get_meta: fn "hash", compound_key ->
              flunk("HTTL should use compound_batch_get_meta, got #{inspect(compound_key)}")
            end,
            compound_batch_get_meta: fn "hash", ^field_keys ->
              send(parent, {:compound_batch_get_meta, field_keys})
              [{"v1", Ferricstore.CommandTime.now_ms() + 60_000}, {"v2", 0}, nil]
            end
          }

          [ttl1, ttl2, persistent_ttl, missing_ttl] =
            Hash.handle(
              "HTTL",
              ["hash", "FIELDS", "4", "expiring", "expiring", "persistent", "missing"],
              store
            )

          assert_received {:compound_batch_get_meta, ^field_keys}
          assert ttl1 >= 58 and ttl1 <= 60
          assert ttl2 == ttl1
          assert persistent_ttl == -1
          assert missing_ttl == -2
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

        test "HPERSIST batches field meta reads and writes each expiring duplicate field once" do
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
                flunk(
                  "HPERSIST should only use compound_get for type, got #{inspect(compound_key)}"
                )
            end,
            compound_get_meta: fn "hash", compound_key ->
              flunk("HPERSIST should use compound_batch_get_meta, got #{inspect(compound_key)}")
            end,
            compound_batch_get_meta: fn "hash", ^field_keys ->
              send(parent, {:compound_batch_get_meta, field_keys})
              [{"v1", Ferricstore.CommandTime.now_ms() + 60_000}, {"v2", 0}, nil]
            end,
            compound_batch_put: fn "hash", entries ->
              send(parent, {:compound_batch_put, entries})
              :ok
            end,
            compound_put: fn "hash", compound_key, _value, 0 ->
              flunk(
                "HPERSIST should use compound_batch_put, got per-field write #{inspect(compound_key)}"
              )
            end
          }

          assert [1, 1, -1, -2] ==
                   Hash.handle(
                     "HPERSIST",
                     ["hash", "FIELDS", "4", "expiring", "expiring", "persistent", "missing"],
                     store
                   )

          assert_received {:compound_batch_get_meta, ^field_keys}
          assert_received {:compound_batch_put, [{expiring_key, "v1", 0}]}
          assert expiring_key == hd(field_keys)
          refute_received {:compound_batch_put, _}
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

        test "AST HSCAN returns field-value pairs like text HSCAN" do
          store = MockStore.make()
          Hash.handle("HSET", ["hash", "a", "1", "b", "2", "c", "3"], store)

          [cursor, elements] = Hash.handle_ast({:hscan, "hash", 0, []}, store)

          assert cursor == "0"
          assert length(elements) == 6

          assert elements |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end) == %{
                   "a" => "1",
                   "b" => "2",
                   "c" => "3"
                 }
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

        test "HRANDFIELD with count 0 does not scan hash fields" do
          type_key = CompoundKey.type_key("hash")

          store = %{
            compound_get: fn "hash", ^type_key -> "hash" end,
            compound_scan: fn "hash", _prefix ->
              flunk("HRANDFIELD count 0 should not scan fields")
            end
          }

          assert [] == Hash.handle("HRANDFIELD", ["hash", "0"], store)
        end

        test "HRANDFIELD with count 0 WITHVALUES does not scan hash fields" do
          type_key = CompoundKey.type_key("hash")

          store = %{
            compound_get: fn "hash", ^type_key -> "hash" end,
            compound_scan: fn "hash", _prefix ->
              flunk("HRANDFIELD count 0 WITHVALUES should not scan fields")
            end
          }

          assert [] == Hash.handle("HRANDFIELD", ["hash", "0", "WITHVALUES"], store)
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

          assert {:error, msg} =
                   Hash.handle("HRANDFIELD", ["h", "2", "WITHVALUES", "extra"], store)

          assert msg =~ "wrong number of arguments"
        end
      end
    end
  end
end
