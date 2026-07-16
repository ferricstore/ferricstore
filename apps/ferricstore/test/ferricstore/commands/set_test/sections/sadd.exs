defmodule Ferricstore.Commands.SetTest.Sections.Sadd do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

      describe "SADD" do
        test "SADD adds new members and returns count" do
          store = MockStore.make()
          assert 3 == Set.handle("SADD", ["myset", "a", "b", "c"], store)
        end

        test "SADD ignores duplicate members" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b"], store)
          assert 1 == Set.handle("SADD", ["myset", "b", "c"], store)
        end

        test "SADD batches member existence reads and writes new unique members only" do
          parent = self()
          type_key = CompoundKey.type_key("myset")

          member_keys = [
            CompoundKey.set_member("myset", "a"),
            CompoundKey.set_member("myset", "b"),
            CompoundKey.set_member("myset", "existing")
          ]

          store = %{
            compound_get: fn
              "myset", ^type_key ->
                "set"

              "myset", compound_key ->
                flunk(
                  "SADD should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
                )
            end,
            compound_batch_get: fn "myset", ^member_keys ->
              send(parent, {:compound_batch_get, member_keys})
              [nil, nil, "1"]
            end,
            compound_batch_put: fn "myset", entries ->
              send(parent, {:compound_batch_put, entries})
              :ok
            end,
            compound_put: fn "myset", compound_key, "1", 0 ->
              flunk(
                "SADD should use compound_batch_put, got per-member write #{inspect(compound_key)}"
              )
            end
          }

          assert 2 == Set.handle("SADD", ["myset", "a", "a", "b", "existing"], store)
          assert_received {:compound_batch_get, ^member_keys}
          assert_received {:compound_batch_put, entries}

          assert Enum.sort(entries) ==
                   member_keys
                   |> Enum.take(2)
                   |> Enum.map(&{&1, "1", 0})
                   |> Enum.sort()

          refute_received {:compound_batch_put, _}
        end

        test "SADD builds new member entries directly from batch reads" do
          source = File.read!(Path.expand("../../../lib/ferricstore/commands/set.ex", __DIR__))

          [sadd_source] = Regex.run(~r/defp sadd_members\(.*?^  end/ms, source)

          assert sadd_source =~
                   "new_entries = set_member_entries_for_missing(compound_keys, values, [])"

          refute sadd_source =~ "Enum.zip(compound_keys)"
          refute sadd_source =~ "Enum.flat_map"
        end

        test "SADD with all duplicates returns 0" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a"], store)
          assert 0 == Set.handle("SADD", ["myset", "a"], store)
        end

        test "SADD rolls back new type metadata when member write fails" do
          parent = self()
          type_key = CompoundKey.type_key("myset")
          member_key = CompoundKey.set_member("myset", "a")

          store = %{
            compound_get: fn
              "myset", ^type_key -> nil
              "myset", ^member_key -> nil
            end,
            compound_put: fn "myset", ^type_key, "set", 0 ->
              send(parent, :type_written)
              :ok
            end,
            compound_batch_get: fn "myset", [^member_key] -> [nil] end,
            compound_batch_put: fn "myset", [{^member_key, "1", 0}] ->
              {:error, :disk_full}
            end,
            compound_delete: fn "myset", ^type_key ->
              send(parent, :type_deleted)
              :ok
            end
          }

          assert {:error, :disk_full} == Set.handle("SADD", ["myset", "a"], store)
          assert_received :type_written
          assert_received :type_deleted
        end

        test "SADD preserves existing type metadata when later member write fails" do
          parent = self()
          type_key = CompoundKey.type_key("myset")
          member_key = CompoundKey.set_member("myset", "a")

          store = %{
            compound_get: fn
              "myset", ^type_key -> "set"
              "myset", ^member_key -> nil
            end,
            compound_batch_get: fn "myset", [^member_key] -> [nil] end,
            compound_batch_put: fn "myset", [{^member_key, "1", 0}] ->
              {:error, :disk_full}
            end,
            compound_delete: fn "myset", ^type_key ->
              send(parent, :type_deleted)
              :ok
            end
          }

          assert {:error, :disk_full} == Set.handle("SADD", ["myset", "a"], store)
          refute_received :type_deleted
        end

        test "SADD with no members returns error" do
          assert {:error, _} = Set.handle("SADD", ["myset"], MockStore.make())
        end

        test "SADD with no args returns error" do
          assert {:error, _} = Set.handle("SADD", [], MockStore.make())
        end
      end

      describe "SREM" do
        test "SREM removes existing member" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b", "c"], store)
          assert 1 == Set.handle("SREM", ["myset", "b"], store)
        end

        test "SREM on missing member returns 0" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a"], store)
          assert 0 == Set.handle("SREM", ["myset", "missing"], store)
        end

        test "SREM multiple members returns count removed" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b", "c"], store)
          assert 2 == Set.handle("SREM", ["myset", "a", "c", "d"], store)
        end

        test "SREM batches member existence reads and removes duplicates once" do
          parent = self()
          type_key = CompoundKey.type_key("myset")

          member_keys = [
            CompoundKey.set_member("myset", "a"),
            CompoundKey.set_member("myset", "b"),
            CompoundKey.set_member("myset", "missing")
          ]

          store = %{
            compound_get: fn
              "myset", ^type_key ->
                nil

              "myset", compound_key ->
                flunk(
                  "SREM should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
                )
            end,
            compound_batch_get: fn "myset", ^member_keys ->
              send(parent, {:compound_batch_get, member_keys})
              ["1", "1", nil]
            end,
            compound_batch_delete: fn "myset", compound_keys ->
              send(parent, {:compound_batch_delete, compound_keys})
              :ok
            end,
            compound_delete: fn "myset", compound_key ->
              flunk(
                "SREM should use compound_batch_delete, got per-member delete #{inspect(compound_key)}"
              )
            end,
            compound_count: fn "myset", _prefix -> 1 end
          }

          assert 2 == Set.handle("SREM", ["myset", "a", "a", "b", "missing"], store)
          assert_received {:compound_batch_get, ^member_keys}
          assert_received {:compound_batch_delete, deleted_keys}
          assert Enum.sort(deleted_keys) == Enum.sort(Enum.take(member_keys, 2))
          refute_received {:compound_batch_delete, _}
        end

        test "SREM builds removed member entries directly from batch reads" do
          source = File.read!(Path.expand("../../../lib/ferricstore/commands/set.ex", __DIR__))

          [srem_source] = Regex.run(~r/defp srem_args\(\[key \| members\].*?^  end/ms, source)

          assert srem_source =~
                   "removed_entries = set_member_entries_for_present(compound_keys, values, [])"

          refute srem_source =~ "Enum.zip(compound_keys)"
          refute srem_source =~ "Enum.flat_map"
        end

        test "SREM cleans up type metadata when set becomes empty" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "only"], store)
          Set.handle("SREM", ["myset", "only"], store)
          assert nil == store.compound_get.("myset", "T:myset")
        end

        test "SREM returns type cleanup errors after removing the last member" do
          store = set_cleanup_failure_store()

          assert {:error, :disk_full} == Set.handle("SREM", ["myset", "only"], store)
        end

        test "SREM preserves the last member when type cleanup fails" do
          base = MockStore.make()
          Set.handle("SADD", ["myset", "only"], base)
          type_key = CompoundKey.type_key("myset")

          store =
            Map.put(base, :compound_delete, fn
              "myset", ^type_key -> {:error, :disk_full}
              key, compound_key -> base.compound_delete.(key, compound_key)
            end)

          assert {:error, :disk_full} == Set.handle("SREM", ["myset", "only"], store)
          assert ["only"] == Set.handle("SMEMBERS", ["myset"], base)
        end

        test "SREM with no members returns error" do
          assert {:error, _} = Set.handle("SREM", ["myset"], MockStore.make())
        end
      end

      describe "SMEMBERS" do
        test "SMEMBERS returns all members" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b", "c"], store)
          members = Set.handle("SMEMBERS", ["myset"], store)
          assert Enum.sort(members) == ["a", "b", "c"]
        end

        test "SMEMBERS on nonexistent set returns empty list" do
          assert [] == Set.handle("SMEMBERS", ["nonexistent"], MockStore.make())
        end

        test "SMEMBERS with wrong arity returns error" do
          assert {:error, _} = Set.handle("SMEMBERS", [], MockStore.make())
        end
      end

      describe "SISMEMBER" do
        test "SISMEMBER returns 1 for existing member" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b"], store)
          assert 1 == Set.handle("SISMEMBER", ["myset", "a"], store)
        end

        test "SISMEMBER returns 0 for missing member" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a"], store)
          assert 0 == Set.handle("SISMEMBER", ["myset", "missing"], store)
        end

        test "SISMEMBER returns 0 for nonexistent key" do
          assert 0 == Set.handle("SISMEMBER", ["nonexistent", "a"], MockStore.make())
        end

        test "SISMEMBER with wrong arity returns error" do
          assert {:error, _} = Set.handle("SISMEMBER", ["key"], MockStore.make())
        end
      end

      describe "SMISMEMBER" do
        test "SMISMEMBER returns one result per requested member" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b"], store)

          assert [1, 0, 1] == Set.handle("SMISMEMBER", ["myset", "a", "missing", "b"], store)
        end

        test "SMISMEMBER uses compound_batch_get when the store provides it" do
          type_key = CompoundKey.type_key("myset")

          member_keys = [
            CompoundKey.set_member("myset", "a"),
            CompoundKey.set_member("myset", "missing"),
            CompoundKey.set_member("myset", "b")
          ]

          store = %{
            compound_get: fn
              "myset", ^type_key ->
                nil

              "myset", compound_key ->
                flunk(
                  "SMISMEMBER should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
                )
            end,
            compound_batch_get: fn "myset", ^member_keys -> ["1", nil, "1"] end
          }

          assert [1, 0, 1] == Set.handle("SMISMEMBER", ["myset", "a", "missing", "b"], store)
        end

        test "SMISMEMBER with no members returns error" do
          assert {:error, _} = Set.handle("SMISMEMBER", ["myset"], MockStore.make())
        end
      end

      describe "SCARD" do
        test "SCARD returns set cardinality" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b", "c"], store)
          assert 3 == Set.handle("SCARD", ["myset"], store)
        end

        test "SCARD returns 0 for nonexistent key" do
          assert 0 == Set.handle("SCARD", ["nonexistent"], MockStore.make())
        end

        test "SCARD reflects removals" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b", "c"], store)
          Set.handle("SREM", ["myset", "b"], store)
          assert 2 == Set.handle("SCARD", ["myset"], store)
        end
      end

      describe "SINTER" do
        test "SINTER returns intersection of two sets" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b", "c"], store)
          Set.handle("SADD", ["s2", "b", "c", "d"], store)
          result = Set.handle("SINTER", ["s1", "s2"], store)
          assert Enum.sort(result) == ["b", "c"]
        end

        test "SINTER with disjoint sets returns empty list" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b"], store)
          Set.handle("SADD", ["s2", "c", "d"], store)
          assert [] == Set.handle("SINTER", ["s1", "s2"], store)
        end

        test "SINTER with nonexistent key returns empty list" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b"], store)
          assert [] == Set.handle("SINTER", ["s1", "nonexistent"], store)
        end

        test "SINTER single set returns its members" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b"], store)
          result = Set.handle("SINTER", ["s1"], store)
          assert Enum.sort(result) == ["a", "b"]
        end

        test "SINTER scans only the smallest set and probes larger sets by member" do
          type_s1 = CompoundKey.type_key("s1")
          type_s2 = CompoundKey.type_key("s2")
          s1_member = CompoundKey.set_member("s1", "b")

          store = %{
            compound_get: fn
              "s1", ^type_s1 -> "set"
              "s2", ^type_s2 -> "set"
              "s1", ^s1_member -> "1"
            end,
            compound_count: fn
              "s1", _prefix -> 10_000
              "s2", _prefix -> 1
            end,
            compound_scan: fn
              "s1", _prefix ->
                flunk("SINTER should not scan the larger set")

              "s2", _prefix ->
                [{"b", "1"}]
            end
          }

          assert ["b"] == Set.handle("SINTER", ["s1", "s2"], store)
        end

        test "SINTER batches membership probes per remaining set" do
          parent = self()
          type_base = CompoundKey.type_key("base")
          type_other1 = CompoundKey.type_key("other1")
          type_other2 = CompoundKey.type_key("other2")

          other1_present =
            MapSet.new([
              CompoundKey.set_member("other1", "a"),
              CompoundKey.set_member("other1", "b")
            ])

          other2_present = CompoundKey.set_member("other2", "b")

          store = %{
            compound_get: fn
              "base", ^type_base -> "set"
              "other1", ^type_other1 -> "set"
              "other2", ^type_other2 -> "set"
              key, "S:" <> _rest -> flunk("SINTER should batch membership probes for #{key}")
            end,
            compound_count: fn
              "base", _prefix -> 3
              "other1", _prefix -> 10
              "other2", _prefix -> 10
            end,
            compound_scan: fn
              "base", _prefix -> [{"a", "1"}, {"b", "1"}, {"c", "1"}]
              key, _prefix -> flunk("SINTER should not scan larger set #{key}")
            end,
            compound_batch_get: fn
              "other1", keys ->
                send(parent, {:batch_probe, "other1", keys})

                Enum.map(keys, fn key ->
                  if MapSet.member?(other1_present, key), do: "1", else: nil
                end)

              "other2", keys ->
                send(parent, {:batch_probe, "other2", keys})

                Enum.map(keys, fn key ->
                  if key == other2_present, do: "1", else: nil
                end)
            end
          }

          assert ["b"] == Set.handle("SINTER", ["base", "other1", "other2"], store)

          assert_receive {:batch_probe, "other1", other1_keys}
          assert length(other1_keys) == 3
          assert_receive {:batch_probe, "other2", other2_keys}
          assert length(other2_keys) == 2
        end

        test "SINTER returns empty without member scans when any set is empty" do
          type_s1 = CompoundKey.type_key("s1")

          store = %{
            compound_get: fn
              "s1", ^type_s1 -> "set"
              "missing", _type_key -> nil
            end,
            compound_count: fn
              "s1", _prefix -> 10_000
              "missing", _prefix -> 0
            end,
            compound_scan: fn key, _prefix ->
              flunk("SINTER should not scan #{inspect(key)} after finding an empty set")
            end
          }

          assert [] == Set.handle("SINTER", ["s1", "missing"], store)
        end
      end

      describe "SUNION" do
        test "SUNION returns union of two sets" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b"], store)
          Set.handle("SADD", ["s2", "b", "c"], store)
          result = Set.handle("SUNION", ["s1", "s2"], store)
          assert Enum.sort(result) == ["a", "b", "c"]
        end

        test "SUNION with nonexistent key returns other set" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b"], store)
          result = Set.handle("SUNION", ["s1", "nonexistent"], store)
          assert Enum.sort(result) == ["a", "b"]
        end
      end

      describe "SDIFF" do
        test "SDIFF returns difference" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b", "c"], store)
          Set.handle("SADD", ["s2", "b", "c", "d"], store)
          result = Set.handle("SDIFF", ["s1", "s2"], store)
          assert result == ["a"]
        end

        test "SDIFF with nonexistent second set returns first set" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b"], store)
          result = Set.handle("SDIFF", ["s1", "nonexistent"], store)
          assert Enum.sort(result) == ["a", "b"]
        end

        test "SDIFF of same set returns empty" do
          store = MockStore.make()
          Set.handle("SADD", ["s1", "a", "b"], store)
          assert [] == Set.handle("SDIFF", ["s1", "s1"], store)
        end
      end

      describe "set STORE commands" do
        test "STORE commands remove old compound destination members from other types" do
          store = MockStore.make()

          Hash.handle("HSET", ["dst", "old", "hash-value"], store)
          Set.handle("SADD", ["src", "set-value"], store)

          assert 1 == Set.handle("SUNIONSTORE", ["dst", "src"], store)
          assert 1 == Set.handle("SREM", ["dst", "set-value"], store)

          Hash.handle("HSET", ["dst", "new", "hash-value"], store)
          assert nil == Hash.handle("HGET", ["dst", "old"], store)
          assert "hash-value" == Hash.handle("HGET", ["dst", "new"], store)
        end

        test "SUNIONSTORE writes destination members as one compound batch" do
          parent = self()
          s1_type = CompoundKey.type_key("s1")
          s2_type = CompoundKey.type_key("s2")
          dst_type = CompoundKey.type_key("dst")
          dst_list_meta = CompoundKey.list_meta_key("dst")
          dst_stream_meta = CompoundKey.stream_meta_key("dst")

          store = %{
            get: fn _key -> nil end,
            delete: fn "dst" ->
              send(parent, :delete_dst)
              :ok
            end,
            compound_get: fn
              "s1", ^s1_type -> "set"
              "s2", ^s2_type -> "set"
              "dst", ^dst_type -> nil
            end,
            compound_scan: fn
              "s1", _prefix -> [{"a", "1"}, {"b", "1"}]
              "s2", _prefix -> [{"b", "1"}, {"c", "1"}]
            end,
            compound_delete_prefix: fn "dst", _prefix ->
              send(parent, :delete_dst_prefix)
              :ok
            end,
            compound_delete: fn
              "dst", ^dst_list_meta ->
                :ok

              "dst", ^dst_stream_meta ->
                :ok

              "dst", ^dst_type ->
                send(parent, :delete_dst_type)
                :ok
            end,
            compound_put: fn
              "dst", ^dst_type, "set", 0 ->
                send(parent, :put_dst_type)
                :ok

              "dst", compound_key, "1", 0 ->
                flunk(
                  "SUNIONSTORE should use compound_batch_put, got per-member write #{inspect(compound_key)}"
                )
            end,
            compound_batch_put: fn "dst", entries ->
              send(parent, {:compound_batch_put, entries})
              :ok
            end
          }

          assert 3 == Set.handle("SUNIONSTORE", ["dst", "s1", "s2"], store)
          assert_received :delete_dst
          assert_received :delete_dst_prefix
          assert_received :delete_dst_type
          assert_received :put_dst_type
          assert_received {:compound_batch_put, entries}

          assert Enum.sort(entries) ==
                   ["a", "b", "c"]
                   |> Enum.map(&{CompoundKey.set_member("dst", &1), "1", 0})
                   |> Enum.sort()

          refute_received {:compound_batch_put, _}
        end

        test "SUNIONSTORE returns destination clear errors before writing replacement set" do
          s1_type = CompoundKey.type_key("s1")
          s2_type = CompoundKey.type_key("s2")

          store = %{
            get: fn _key -> nil end,
            delete: fn "dst" -> {:error, :disk_full} end,
            compound_get: fn
              "s1", ^s1_type -> "set"
              "s2", ^s2_type -> "set"
              "dst", _compound_key -> nil
            end,
            compound_scan: fn
              "s1", _prefix -> [{"a", "1"}]
              "s2", _prefix -> [{"b", "1"}]
            end,
            compound_delete_prefix: fn "dst", _prefix ->
              flunk("SUNIONSTORE must not delete destination prefix after key delete failure")
            end,
            compound_delete: fn "dst", _type_key ->
              flunk("SUNIONSTORE must not delete destination type after key delete failure")
            end,
            compound_put: fn "dst", _compound_key, _value, 0 ->
              flunk("SUNIONSTORE must not write destination type after key delete failure")
            end,
            compound_batch_put: fn "dst", _entries ->
              flunk("SUNIONSTORE must not write destination members after key delete failure")
            end
          }

          assert {:error, :disk_full} == Set.handle("SUNIONSTORE", ["dst", "s1", "s2"], store)
        end

        test "SUNIONSTORE rolls back destination type metadata when member batch write fails" do
          parent = self()
          s1_type = CompoundKey.type_key("s1")
          s2_type = CompoundKey.type_key("s2")
          dst_type = CompoundKey.type_key("dst")
          dst_list_meta = CompoundKey.list_meta_key("dst")
          dst_stream_meta = CompoundKey.stream_meta_key("dst")

          store = %{
            get: fn _key -> nil end,
            delete: fn "dst" -> :ok end,
            compound_get: fn
              "s1", ^s1_type -> "set"
              "s2", ^s2_type -> "set"
              "dst", ^dst_type -> nil
            end,
            compound_scan: fn
              "s1", _prefix -> [{"a", "1"}]
              "s2", _prefix -> [{"b", "1"}]
            end,
            compound_delete_prefix: fn "dst", _prefix -> :ok end,
            compound_delete: fn
              "dst", ^dst_list_meta ->
                :ok

              "dst", ^dst_stream_meta ->
                :ok

              "dst", ^dst_type ->
                if Process.get(:dst_type_written?) do
                  send(parent, :type_rolled_back)
                else
                  send(parent, :type_cleared)
                end

                :ok
            end,
            compound_put: fn "dst", ^dst_type, "set", 0 ->
              Process.put(:dst_type_written?, true)
              send(parent, :type_written)
              :ok
            end,
            compound_batch_put: fn "dst", entries when length(entries) == 2 ->
              {:error, :disk_full}
            end
          }

          assert {:error, :disk_full} == Set.handle("SUNIONSTORE", ["dst", "s1", "s2"], store)
          assert_received :type_cleared
          assert_received :type_written
          assert_received :type_rolled_back
        end

        test "SUNIONSTORE preserves existing destination when member batch write fails" do
          base = MockStore.make()
          assert 1 == Set.handle("SADD", ["src", "new"], base)
          assert 1 == Set.handle("SADD", ["dst", "old"], base)

          store =
            Map.put(base, :compound_batch_put, fn
              "dst", entries ->
                if Enum.any?(entries, fn {compound_key, _value, _expire_at_ms} ->
                     compound_key == CompoundKey.set_member("dst", "new")
                   end) do
                  {:error, :disk_full}
                else
                  base.compound_batch_put.("dst", entries)
                end

              key, entries ->
                base.compound_batch_put.(key, entries)
            end)

          assert {:error, :disk_full} == Set.handle("SUNIONSTORE", ["dst", "src"], store)
          assert ["old"] == Set.handle("SMEMBERS", ["dst"], base)
        end

        @tag :stream_destination_cleanup
        test "SUNIONSTORE clears stream destination cache and durable rows" do
          Ferricstore.Commands.Stream.ensure_meta_table()
          key = "set_store_stream_dest_#{System.unique_integer([:positive])}"
          store = MockStore.make()

          on_exit(fn ->
            if :ets.whereis(Ferricstore.Stream.Meta) != :undefined do
              :ets.delete(Ferricstore.Stream.Meta, key)
            end
          end)

          assert "1-0" ==
                   Ferricstore.Commands.Stream.handle(
                     "XADD",
                     [key, "1-0", "f", "v"],
                     store
                   )

          assert 1 == Set.handle("SADD", ["src", "member"], store)
          assert 1 == Set.handle("SUNIONSTORE", [key, "src"], store)
          assert [] == :ets.lookup(Ferricstore.Stream.Meta, key)
          assert [] == store.compound_scan.(key, CompoundKey.stream_prefix(key))
          assert ["member"] == Set.handle("SMEMBERS", [key], store)
        end
      end

      describe "SSCAN" do
        test "SSCAN with cursor 0 returns all members when count >= size" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "a", "b", "c"], store)
          [cursor, elements] = Set.handle("SSCAN", ["myset", "0"], store)
          assert cursor == "0"
          assert Enum.sort(elements) == ["a", "b", "c"]
        end

        test "SSCAN with COUNT limits batch size" do
          store = MockStore.make()

          for i <- 1..20 do
            Set.handle(
              "SADD",
              ["myset", "member#{String.pad_leading(Integer.to_string(i), 2, "0")}"],
              store
            )
          end

          [cursor, elements] = Set.handle("SSCAN", ["myset", "0", "COUNT", "5"], store)
          assert cursor != "0"
          assert length(elements) == 5
        end

        test "SSCAN full iteration collects all members exactly once" do
          store = MockStore.make()
          expected = for i <- 1..15, do: "m#{String.pad_leading(Integer.to_string(i), 2, "0")}"

          for m <- expected do
            Set.handle("SADD", ["myset", m], store)
          end

          all_members = collect_sscan_members(store, "myset", "0", 4)
          assert Enum.sort(all_members) == Enum.sort(expected)
        end

        test "SSCAN with MATCH filters members" do
          store = MockStore.make()
          Set.handle("SADD", ["myset", "apple", "banana", "avocado", "blueberry"], store)
          [cursor, elements] = Set.handle("SSCAN", ["myset", "0", "MATCH", "a*"], store)
          assert cursor == "0"
          assert Enum.sort(elements) == ["apple", "avocado"]
        end

        test "SSCAN on nonexistent key returns cursor 0 and empty list" do
          store = MockStore.make()
          [cursor, elements] = Set.handle("SSCAN", ["nonexistent", "0"], store)
          assert cursor == "0"
          assert elements == []
        end

        test "SSCAN with invalid cursor returns error" do
          store = MockStore.make()
          assert {:error, _} = Set.handle("SSCAN", ["myset", "notanumber"], store)
        end

        test "SSCAN with wrong number of arguments returns error" do
          store = MockStore.make()
          assert {:error, _} = Set.handle("SSCAN", ["key"], store)
          assert {:error, _} = Set.handle("SSCAN", [], store)
        end

        test "SSCAN on wrong type returns WRONGTYPE" do
          store = MockStore.make()
          Hash.handle("HSET", ["mykey", "field", "value"], store)
          assert {:error, "WRONGTYPE" <> _} = Set.handle("SSCAN", ["mykey", "0"], store)
        end
      end
    end
  end
end
