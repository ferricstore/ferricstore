defmodule Ferricstore.Commands.SortedSetTest.Sections.Zadd do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

  describe "ZADD" do
    test "ZADD adds new members and returns count" do
      store = MockStore.make()
      assert 3 == SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
    end

    test "ZADD updating existing member returns 0" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
    end

    test "ZADD with NX only adds new members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 1 == SortedSet.handle("ZADD", ["zs", "NX", "5.0", "a", "2.0", "b"], store)
      # a should still have score 1.0
      assert "1.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZADD options are case-insensitive" do
      store = MockStore.make()
      assert 1 == SortedSet.handle("ZADD", ["zs", "nx", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZADD", ["zs", "xx", "2.0", "b"], store)
      assert "1.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "b"], store)
    end

    test "ZADD with XX only updates existing members" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZADD", ["zs", "XX", "5.0", "a", "2.0", "b"], store)
      # a should be updated to 5.0
      assert "5.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
      # b should not exist
      assert nil == SortedSet.handle("ZSCORE", ["zs", "b"], store)
    end

    test "ZADD with CH returns count of changed (added + updated)" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      # Adding b (new) + updating a (changed) = 2
      assert 2 == SortedSet.handle("ZADD", ["zs", "CH", "5.0", "a", "2.0", "b"], store)
    end

    test "ZADD with GT only updates if new score > current" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      SortedSet.handle("ZADD", ["zs", "GT", "3.0", "a"], store)
      # Should stay at 5.0 since 3.0 < 5.0
      assert "5.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
      SortedSet.handle("ZADD", ["zs", "GT", "10.0", "a"], store)
      assert "10.0" == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZADD with integer scores" do
      store = MockStore.make()
      assert 1 == SortedSet.handle("ZADD", ["zs", "5", "a"], store)
    end

    test "ZADD with invalid score returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZADD", ["zs", "abc", "a"], store)
    end

    test "ZADD with odd score/member count returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZADD", ["zs", "1.0"], store)
    end

    test "ZADD with no args returns error" do
      assert {:error, _} = SortedSet.handle("ZADD", [], MockStore.make())
    end

    test "ZADD batches member reads and collapses duplicate final writes" do
      parent = self()
      type_key = CompoundKey.type_key("zs")

      member_keys = [
        CompoundKey.zset_member("zs", "a"),
        CompoundKey.zset_member("zs", "b"),
        CompoundKey.zset_member("zs", "c")
      ]

      store = %{
        compound_get: fn
          "zs", ^type_key ->
            nil

          "zs", compound_key ->
            flunk(
              "ZADD should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "zs", ^member_keys ->
          send(parent, {:compound_batch_get, member_keys})
          ["1.0", nil, nil]
        end,
        compound_put: fn
          "zs", ^type_key, "zset", 0 ->
            :ok

          "zs", compound_key, score, 0 ->
            send(parent, {:compound_put, compound_key, score})
            :ok
        end
      }

      assert 2 ==
               SortedSet.handle(
                 "ZADD",
                 ["zs", "2.0", "a", "3.0", "b", "4.0", "b", "5.0", "c"],
                 store
               )

      assert_received {:compound_batch_get, ^member_keys}
      assert_received {:compound_put, a_key, "2.0"}
      assert_received {:compound_put, b_key, "4.0"}
      assert_received {:compound_put, c_key, "5.0"}
      assert Enum.sort([a_key, b_key, c_key]) == Enum.sort(member_keys)
      refute_received {:compound_put, _, _}
    end

    test "ZADD batches member writes when store supports compound_batch_put" do
      parent = self()
      base_store = MockStore.make()

      store =
        Map.put(base_store, :compound_batch_put, fn redis_key, entries ->
          send(parent, {:compound_batch_put, redis_key, entries})

          Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
            base_store.compound_put.(redis_key, compound_key, value, expire_at_ms)
          end)

          :ok
        end)

      assert 3 == SortedSet.handle("ZADD", ["zs", "1", "a", "2", "b", "3", "c"], store)

      assert_receive {:compound_batch_put, "zs", entries}
      assert length(entries) == 3
      assert Enum.all?(entries, fn {compound_key, _value, 0} -> is_binary(compound_key) end)
      refute_receive {:compound_batch_put, _, _}
    end

    test "ZADD builds batch write entries without zip flat_map" do
      source = File.read!(Path.expand("../../../lib/ferricstore/commands/sorted_set.ex", __DIR__))

      assert source =~ "zset_current_by_member(unique_members, current_values, %{})"
      assert source =~ "zset_write_entries(unique_members, compound_keys, writes_by_member, [])"
      refute source =~ "then(&Enum.zip(unique_members, &1))"
      refute source =~ "Enum.flat_map(Enum.zip(unique_members, compound_keys)"
    end

    test "WITHSCORES responses use shared flat-list helpers" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/commands/sorted_set.ex", __DIR__)) <>
          File.read!(Path.expand("../../../lib/ferricstore/commands/sorted_set/helpers.ex", __DIR__))

      assert source =~ "Helpers.score_pairs_to_flat_list(filtered)"
      assert source =~ "Helpers.score_pairs_to_flat_list(members)"
      assert source =~ "score_string_pairs_to_flat_list(selected)"

      refute source =~
               "Enum.flat_map(filtered, fn {member, score} -> [member, format_score(score)] end)"

      refute source =~
               "Enum.flat_map(members, fn {member, score} -> [member, format_score(score)] end)"

      refute source =~
               "Enum.flat_map(selected, fn {member, score} -> [member, format_score_str(score)] end)"
    end

    test "ZADD rolls back new type metadata when member write fails" do
      parent = self()
      type_key = CompoundKey.type_key("zs")
      member_key = CompoundKey.zset_member("zs", "a")

      store = %{
        compound_get: fn
          "zs", ^type_key -> nil
          "zs", ^member_key -> nil
        end,
        compound_put: fn "zs", ^type_key, "zset", 0 ->
          send(parent, :type_written)
          :ok
        end,
        compound_batch_get: fn "zs", [^member_key] -> [nil] end,
        compound_batch_put: fn "zs", [{^member_key, "1.0", 0}] ->
          {:error, :disk_full}
        end,
        compound_delete: fn "zs", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, :disk_full} == SortedSet.handle("ZADD", ["zs", "1", "a"], store)
      assert_received :type_written
      assert_received :type_deleted
    end

    test "ZADD preserves existing type metadata when member write fails" do
      parent = self()
      type_key = CompoundKey.type_key("zs")
      member_key = CompoundKey.zset_member("zs", "a")

      store = %{
        compound_get: fn
          "zs", ^type_key -> "zset"
          "zs", ^member_key -> nil
        end,
        compound_batch_get: fn "zs", [^member_key] -> [nil] end,
        compound_batch_put: fn "zs", [{^member_key, "1.0", 0}] ->
          {:error, :disk_full}
        end,
        compound_delete: fn "zs", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, :disk_full} == SortedSet.handle("ZADD", ["zs", "1", "a"], store)
      refute_received :type_deleted
    end
  end

  # ---------------------------------------------------------------------------
  # ZSCORE
  # ---------------------------------------------------------------------------

  describe "ZSCORE" do
    test "ZSCORE returns score string" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.14", "pi"], store)
      # ZSCORE now formats scores consistently with ZRANGE WITHSCORES
      assert "3.14000000000000012" == SortedSet.handle("ZSCORE", ["zs", "pi"], store)
    end

    test "ZSCORE returns nil for missing member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "missing"], store)
    end

    test "ZSCORE returns nil for nonexistent key" do
      assert nil == SortedSet.handle("ZSCORE", ["nonexistent", "a"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANK
  # ---------------------------------------------------------------------------

  describe "ZRANK" do
    test "ZRANK returns rank (0-indexed) by ascending score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      assert 0 == SortedSet.handle("ZRANK", ["zs", "a"], store)
      assert 1 == SortedSet.handle("ZRANK", ["zs", "b"], store)
      assert 2 == SortedSet.handle("ZRANK", ["zs", "c"], store)
    end

    test "ZRANK returns nil for missing member" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert nil == SortedSet.handle("ZRANK", ["zs", "missing"], store)
    end

    test "ZRANK uses score-index member rank when available" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], base_store)

      store =
        Map.put(base_store, :zset_member_rank, fn redis_key, member, reverse? ->
          send(parent, {:zset_member_rank, redis_key, member, reverse?})
          {:ok, 1}
        end)

      assert 1 == SortedSet.handle("ZRANK", ["zs", "b"], store)
      assert_receive {:zset_member_rank, "zs", "b", false}
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGE
  # ---------------------------------------------------------------------------

  describe "ZRANGE" do
    test "ZRANGE returns members by rank" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "3.0", "c", "1.0", "a", "2.0", "b"], store)
      assert ["a", "b", "c"] == SortedSet.handle("ZRANGE", ["zs", "0", "-1"], store)
    end

    test "ZRANGE with sub-range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      assert ["b", "c"] == SortedSet.handle("ZRANGE", ["zs", "1", "2"], store)
    end

    test "ZRANGE WITHSCORES returns members and scores" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.5", "a", "2.5", "b"], store)
      result = SortedSet.handle("ZRANGE", ["zs", "0", "-1", "WITHSCORES"], store)
      assert length(result) == 4
      assert Enum.at(result, 0) == "a"
      assert Enum.at(result, 2) == "b"
    end

    test "ZRANGE on nonexistent key returns empty list" do
      assert [] == SortedSet.handle("ZRANGE", ["nonexistent", "0", "-1"], MockStore.make())
    end

    test "ZRANGE with start > stop returns empty list" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert [] == SortedSet.handle("ZRANGE", ["zs", "5", "1"], store)
    end

    test "ZRANGE uses score-index rank range when available" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], base_store)

      store =
        Map.put(base_store, :zset_rank_range, fn redis_key, start_idx, stop_idx, reverse? ->
          send(parent, {:zset_rank_range, redis_key, start_idx, stop_idx, reverse?})
          {:ok, [{"b", 2.0}, {"c", 3.0}]}
        end)

      assert ["b", "2.0", "c", "3.0"] ==
               SortedSet.handle("ZRANGE", ["zs", "1", "2", "WITHSCORES"], store)

      assert_receive {:zset_rank_range, "zs", 1, 2, false}
    end

    test "ZRANGE negative bounds use score-index cardinality when available" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], base_store)

      store =
        base_store
        |> Map.put(:zset_score_count, fn redis_key, min_bound, max_bound ->
          send(parent, {:zset_score_count, redis_key, min_bound, max_bound})
          {:ok, 3}
        end)
        |> Map.put(:zset_rank_range, fn redis_key, start_idx, stop_idx, reverse? ->
          send(parent, {:zset_rank_range, redis_key, start_idx, stop_idx, reverse?})
          {:ok, [{"a", 1.0}, {"b", 2.0}, {"c", 3.0}]}
        end)
        |> Map.put(:compound_count, fn _redis_key, _prefix ->
          flunk("ZRANGE should not scan/count zset members when score-index cardinality exists")
        end)

      assert ["a", "b", "c"] == SortedSet.handle("ZRANGE", ["zs", "0", "-1"], store)

      assert_receive {:zset_score_count, "zs", :neg_inf, :inf}
      assert_receive {:zset_rank_range, "zs", 0, 2, false}
    end
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGE
  # ---------------------------------------------------------------------------

  describe "ZREVRANGE" do
    test "ZREVRANGE returns members in descending score order" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert ["c", "b", "a"] == SortedSet.handle("ZREVRANGE", ["zs", "0", "-1"], store)
    end

    test "ZREVRANGE with sub-range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert ["c", "b"] == SortedSet.handle("ZREVRANGE", ["zs", "0", "1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZCARD
  # ---------------------------------------------------------------------------

  describe "ZCARD" do
    test "ZCARD returns sorted set cardinality" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      assert 3 == SortedSet.handle("ZCARD", ["zs"], store)
    end

    test "ZCARD returns 0 for nonexistent key" do
      assert 0 == SortedSet.handle("ZCARD", ["nonexistent"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # ZREM
  # ---------------------------------------------------------------------------

  describe "ZREM" do
    test "ZREM removes member and returns 1" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 1 == SortedSet.handle("ZREM", ["zs", "a"], store)
      assert nil == SortedSet.handle("ZSCORE", ["zs", "a"], store)
    end

    test "ZREM on missing member returns 0" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      assert 0 == SortedSet.handle("ZREM", ["zs", "missing"], store)
    end

    test "ZREM batches member existence reads and removes duplicates once" do
      parent = self()
      type_key = CompoundKey.type_key("zs")

      member_keys = [
        CompoundKey.zset_member("zs", "a"),
        CompoundKey.zset_member("zs", "b"),
        CompoundKey.zset_member("zs", "missing")
      ]

      store = %{
        compound_get: fn
          "zs", ^type_key ->
            nil

          "zs", compound_key ->
            flunk(
              "ZREM should use compound_batch_get, got per-member lookup #{inspect(compound_key)}"
            )
        end,
        compound_batch_get: fn "zs", ^member_keys ->
          send(parent, {:compound_batch_get, member_keys})
          ["1.0", "2.0", nil]
        end,
        compound_batch_delete: fn "zs", compound_keys ->
          send(parent, {:compound_batch_delete, compound_keys})
          :ok
        end,
        compound_delete: fn "zs", compound_key ->
          flunk(
            "ZREM should use compound_batch_delete, got per-member delete #{inspect(compound_key)}"
          )
        end,
        compound_count: fn "zs", _prefix -> 1 end
      }

      assert 2 == SortedSet.handle("ZREM", ["zs", "a", "a", "b", "missing"], store)
      assert_received {:compound_batch_get, ^member_keys}
      assert_received {:compound_batch_delete, deleted_keys}
      assert Enum.sort(deleted_keys) == Enum.sort(Enum.take(member_keys, 2))
      refute_received {:compound_batch_delete, _}
    end

    test "ZREM cleans up type metadata when sorted set becomes empty" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], store)
      SortedSet.handle("ZREM", ["zs", "a"], store)
      assert nil == store.compound_get.("zs", "T:zs")
    end

    test "ZREM cleanup uses score-index cardinality when available" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a"], base_store)

      store =
        base_store
        |> Map.put(:zset_score_count, fn redis_key, min_bound, max_bound ->
          send(parent, {:zset_score_count, redis_key, min_bound, max_bound})
          {:ok, 0}
        end)
        |> Map.put(:compound_count, fn _redis_key, _prefix ->
          flunk("ZREM cleanup should not scan/count members when score-index cardinality exists")
        end)

      assert 1 == SortedSet.handle("ZREM", ["zs", "a"], store)
      assert_receive {:zset_score_count, "zs", :neg_inf, :inf}
      assert nil == base_store.compound_get.("zs", "T:zs")
    end

    test "ZREM returns type cleanup errors after removing the last member" do
      store = zset_cleanup_failure_store()

      assert {:error, :disk_full} == SortedSet.handle("ZREM", ["zs", "only"], store)
    end

    test "ZREM preserves the last member when type cleanup fails" do
      base = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1", "only"], base)
      type_key = CompoundKey.type_key("zs")

      store =
        Map.put(base, :compound_delete, fn
          "zs", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == SortedSet.handle("ZREM", ["zs", "only"], store)
      assert "1.0" == SortedSet.handle("ZSCORE", ["zs", "only"], base)
    end
  end

  # ---------------------------------------------------------------------------
  # ZINCRBY
  # ---------------------------------------------------------------------------

  describe "ZINCRBY" do
    test "ZINCRBY increments score" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      result = SortedSet.handle("ZINCRBY", ["zs", "3.0", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 8.0, val, 0.001
    end

    test "ZINCRBY creates member if missing" do
      store = MockStore.make()
      result = SortedSet.handle("ZINCRBY", ["zs", "5.0", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 5.0, val, 0.001
    end

    test "ZINCRBY with integer increment" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "5.0", "a"], store)
      result = SortedSet.handle("ZINCRBY", ["zs", "3", "a"], store)
      {val, ""} = Float.parse(result)
      assert_in_delta 8.0, val, 0.001
    end

    test "ZINCRBY with invalid increment returns error" do
      store = MockStore.make()
      assert {:error, _} = SortedSet.handle("ZINCRBY", ["zs", "abc", "a"], store)
    end

    test "ZINCRBY with invalid increment does not create type metadata" do
      parent = self()
      type_key = CompoundKey.type_key("zs")

      store = %{
        compound_get: fn "zs", ^type_key -> nil end,
        compound_put: fn "zs", ^type_key, "zset", 0 ->
          send(parent, :type_written)
          :ok
        end
      }

      assert {:error, "ERR value is not a valid float"} ==
               SortedSet.handle("ZINCRBY", ["zs", "abc", "a"], store)

      refute_received :type_written
    end

    test "ZINCRBY returns member write errors" do
      type_key = CompoundKey.type_key("zs")
      member_key = CompoundKey.zset_member("zs", "a")

      store = %{
        compound_get: fn
          "zs", ^type_key -> "zset"
          "zs", ^member_key -> nil
        end,
        compound_put: fn "zs", ^member_key, "5.0", 0 -> {:error, "disk full"} end
      }

      assert {:error, "disk full"} == SortedSet.handle("ZINCRBY", ["zs", "5.0", "a"], store)
    end

    test "ZINCRBY rolls back new type metadata when member write fails" do
      parent = self()
      type_key = CompoundKey.type_key("zs")
      member_key = CompoundKey.zset_member("zs", "a")

      store = %{
        compound_get: fn
          "zs", ^type_key -> nil
          "zs", ^member_key -> nil
        end,
        compound_put: fn
          "zs", ^type_key, "zset", 0 ->
            send(parent, :type_written)
            :ok

          "zs", ^member_key, "5.0", 0 ->
            {:error, "disk full"}
        end,
        compound_delete: fn "zs", ^type_key ->
          send(parent, :type_deleted)
          :ok
        end
      }

      assert {:error, "disk full"} == SortedSet.handle("ZINCRBY", ["zs", "5.0", "a"], store)
      assert_received :type_written
      assert_received :type_deleted
    end
  end

  # ---------------------------------------------------------------------------
  # ZCOUNT
  # ---------------------------------------------------------------------------

  describe "ZCOUNT" do
    test "ZCOUNT with inclusive range" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c", "4.0", "d"], store)
      assert 3 == SortedSet.handle("ZCOUNT", ["zs", "1.0", "3.0"], store)
    end

    test "ZCOUNT with -inf to +inf returns all" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b"], store)
      assert 2 == SortedSet.handle("ZCOUNT", ["zs", "-inf", "+inf"], store)
    end

    test "ZCOUNT with exclusive bounds" do
      store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1.0", "a", "2.0", "b", "3.0", "c"], store)
      # (1.0 to (3.0 = only score 2.0
      assert 1 == SortedSet.handle("ZCOUNT", ["zs", "(1.0", "(3.0"], store)
    end

    test "ZCOUNT on nonexistent key returns 0" do
      assert 0 == SortedSet.handle("ZCOUNT", ["nonexistent", "-inf", "+inf"], MockStore.make())
    end

    test "ZCOUNT uses score-index count when store supports it" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1", "a", "2", "b", "3", "c"], base_store)

      store =
        Map.put(base_store, :zset_score_count, fn redis_key, min_bound, max_bound ->
          send(parent, {:zset_score_count, redis_key, min_bound, max_bound})
          {:ok, 2}
        end)

      assert 2 == SortedSet.handle("ZCOUNT", ["zs", "1", "2"], store)

      assert_receive {:zset_score_count, "zs", {:inclusive, 1.0}, {:inclusive, 2.0}}
    end
  end

  describe "ZRANGEBYSCORE ordering" do
    test "ZRANGEBYSCORE sorts only matching members by score then member" do
      store = MockStore.make()

      SortedSet.handle(
        "ZADD",
        ["zs", "9", "outside", "2", "b", "1", "z", "2", "a", "3", "c"],
        store
      )

      assert ["a", "b"] == SortedSet.handle("ZRANGEBYSCORE", ["zs", "2", "2"], store)
    end

    test "ZREVRANGEBYSCORE reverses score and member tie order after filtering" do
      store = MockStore.make()

      SortedSet.handle(
        "ZADD",
        ["zs", "9", "outside", "2", "b", "1", "z", "2", "a", "3", "c"],
        store
      )

      assert ["b", "a"] == SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "2", "2"], store)
    end

    test "ZRANGEBYSCORE applies LIMIT after score ordering" do
      store = MockStore.make()

      SortedSet.handle(
        "ZADD",
        ["zs", "3", "c", "1", "a", "2", "b", "5", "e", "4", "d"],
        store
      )

      assert ["b", "c"] ==
               SortedSet.handle("ZRANGEBYSCORE", ["zs", "-inf", "+inf", "LIMIT", "1", "2"], store)
    end

    test "ZRANGEBYSCORE uses score-index range before applying LIMIT and WITHSCORES" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1", "a", "2", "b", "3", "c"], base_store)

      store =
        Map.put(base_store, :zset_score_range, fn redis_key, min_bound, max_bound, reverse? ->
          send(parent, {:zset_score_range, redis_key, min_bound, max_bound, reverse?})
          {:ok, [{"a", 1.0}, {"b", 2.0}, {"c", 3.0}]}
        end)

      assert ["b", "2.0"] ==
               SortedSet.handle(
                 "ZRANGEBYSCORE",
                 ["zs", "1", "3", "WITHSCORES", "LIMIT", "1", "1"],
                 store
               )

      assert_receive {:zset_score_range, "zs", {:inclusive, 1.0}, {:inclusive, 3.0}, false}
    end

    test "ZRANGEBYSCORE pushes LIMIT into score-index slice when available" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1", "a", "2", "b", "3", "c"], base_store)

      store =
        Map.put(base_store, :zset_score_range_slice, fn redis_key,
                                                        min_bound,
                                                        max_bound,
                                                        reverse?,
                                                        offset,
                                                        count ->
          send(
            parent,
            {:zset_score_range_slice, redis_key, min_bound, max_bound, reverse?, offset, count}
          )

          {:ok, [{"b", 2.0}]}
        end)

      assert ["b", "2.0"] ==
               SortedSet.handle(
                 "ZRANGEBYSCORE",
                 ["zs", "1", "3", "WITHSCORES", "LIMIT", "1", "1"],
                 store
               )

      assert_receive {:zset_score_range_slice, "zs", {:inclusive, 1.0}, {:inclusive, 3.0}, false,
                      1, 1}
    end

    test "ZREVRANGEBYSCORE passes reverse flag to score-index range" do
      parent = self()
      base_store = MockStore.make()
      SortedSet.handle("ZADD", ["zs", "1", "a", "2", "b"], base_store)

      store =
        Map.put(base_store, :zset_score_range, fn redis_key, min_bound, max_bound, reverse? ->
          send(parent, {:zset_score_range, redis_key, min_bound, max_bound, reverse?})
          {:ok, [{"b", 2.0}, {"a", 1.0}]}
        end)

      assert ["b"] ==
               SortedSet.handle("ZREVRANGEBYSCORE", ["zs", "2", "1", "LIMIT", "0", "1"], store)

      assert_receive {:zset_score_range, "zs", {:inclusive, 1.0}, {:inclusive, 2.0}, true}
    end
  end

  # ---------------------------------------------------------------------------
  # ZPOPMIN and ZPOPMAX
  # ---------------------------------------------------------------------------
    end
  end
end
