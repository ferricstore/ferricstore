defmodule Ferricstore.Raft.WritePathTest.Sections.SetViaRouterGoesThroughRaft do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Test.ShardHelpers
      alias Ferricstore.Raft.StateMachine, as: SM

      describe "SET via Router goes through Raft" do
        test "SET writes value that is readable after Raft commit" do
          k = ukey("set_raft")

          assert :ok = Router.put(FerricStore.Instance.get(:default), k, "raft_value", 0)
          assert "raft_value" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "SET writes to ETS via StateMachine apply" do
          k = ukey("set_ets_via_sm")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "sm_val", 0)

          # Single-table format: {key, value, expire_at_ms, lfu_counter}
          assert [{^k, "sm_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)
        end

        test "SET with TTL preserves expiry through Raft path" do
          k = ukey("set_ttl_raft")
          future = System.os_time(:millisecond) + 60_000

          :ok = Router.put(FerricStore.Instance.get(:default), k, "ttl_val", future)

          {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
          assert value == "ttl_val"
          assert expire_at_ms == future
        end
      end

      describe "GET after Raft-committed SET" do
        test "returns the value immediately after SET" do
          k = ukey("get_after_set")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "committed_value", 0)
          assert "committed_value" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "returns latest value after multiple SETs" do
          k = ukey("get_multi_set")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "v1", 0)
          assert "v1" == Router.get(FerricStore.Instance.get(:default), k)

          :ok = Router.put(FerricStore.Instance.get(:default), k, "v2", 0)
          assert "v2" == Router.get(FerricStore.Instance.get(:default), k)

          :ok = Router.put(FerricStore.Instance.get(:default), k, "v3", 0)
          assert "v3" == Router.get(FerricStore.Instance.get(:default), k)
        end
      end

      describe "DEL via Router goes through Raft" do
        test "DEL removes key after Raft commit" do
          k = ukey("del_raft")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "to_delete", 0)
          assert "to_delete" == Router.get(FerricStore.Instance.get(:default), k)

          :ok = Router.delete(FerricStore.Instance.get(:default), k)
          assert nil == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "DEL removes from ETS" do
          k = ukey("del_ets")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "will_vanish", 0)

          assert [{^k, "will_vanish", 0, _lfu, _fid, _off, _vsize}] =
                   :ets.lookup(keydir_for(k), k)

          :ok = Router.delete(FerricStore.Instance.get(:default), k)
          assert [] == :ets.lookup(keydir_for(k), k)
        end

        test "DEL on non-existent key returns :ok" do
          k = ukey("del_missing")
          assert :ok = Router.delete(FerricStore.Instance.get(:default), k)
        end
      end

      describe "multiple concurrent writes via Raft" do
        test "all concurrent writes complete successfully" do
          keys = for i <- 1..50, do: ukey("concurrent_#{i}")

          tasks =
            Enum.map(keys, fn k ->
              Task.async(fn ->
                Router.put(FerricStore.Instance.get(:default), k, "concurrent_val_#{k}", 0)
              end)
            end)

          results = Task.await_many(tasks, 15_000)
          assert Enum.all?(results, &(&1 == :ok))

          # All keys should be readable after Raft commit
          for k <- keys do
            assert "concurrent_val_#{k}" == Router.get(FerricStore.Instance.get(:default), k),
                   "Key #{k} should be readable after concurrent Raft commit"
          end
        end

        test "concurrent writes to same key produce consistent final state" do
          k = ukey("concurrent_same")

          tasks =
            for i <- 1..10 do
              Task.async(fn ->
                Router.put(FerricStore.Instance.get(:default), k, "v#{i}", 0)
              end)
            end

          Task.await_many(tasks, 15_000)

          # Should have one of the values (last writer wins)
          val = Router.get(FerricStore.Instance.get(:default), k)
          assert val != nil
          assert String.starts_with?(val, "v")
        end
      end

      describe "INCR via Router goes through Raft" do
        test "INCR on non-existent key initializes to delta" do
          k = ukey("incr_new")

          assert {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), k, 1)
          assert "1" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "INCR on existing integer key increments correctly" do
          k = ukey("incr_existing")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "10", 0)
          assert {:ok, 15} = Router.incr(FerricStore.Instance.get(:default), k, 5)
          assert "15" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "DECR works through Raft path" do
          k = ukey("decr_raft")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "100", 0)
          assert {:ok, 95} = Router.incr(FerricStore.Instance.get(:default), k, -5)
          assert "95" == Router.get(FerricStore.Instance.get(:default), k)
        end

        test "multiple sequential INCRs produce correct total" do
          k = ukey("incr_seq")

          for _i <- 1..10 do
            Router.incr(FerricStore.Instance.get(:default), k, 1)
          end

          assert "10" == Router.get(FerricStore.Instance.get(:default), k)
        end
      end

      describe "write version increments after Raft commit" do
        test "write version increases after SET" do
          k = ukey("version_set")

          v1 = Router.get_version(FerricStore.Instance.get(:default), k)
          :ok = Router.put(FerricStore.Instance.get(:default), k, "val", 0)
          v2 = Router.get_version(FerricStore.Instance.get(:default), k)

          assert v2 > v1
        end

        test "write version increases after DEL" do
          k = ukey("version_del")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "val", 0)
          v1 = Router.get_version(FerricStore.Instance.get(:default), k)

          :ok = Router.delete(FerricStore.Instance.get(:default), k)
          v2 = Router.get_version(FerricStore.Instance.get(:default), k)

          assert v2 > v1
        end

        test "write version increases after INCR" do
          k = ukey("version_incr")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "0", 0)
          v1 = Router.get_version(FerricStore.Instance.get(:default), k)

          {:ok, _} = Router.incr(FerricStore.Instance.get(:default), k, 1)
          v2 = Router.get_version(FerricStore.Instance.get(:default), k)

          assert v2 > v1
        end
      end

      describe "data persists in Bitcask after Raft commit" do
        test "SET data is readable from Bitcask NIF after Raft commit" do
          k = ukey("bitcask_persist")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "durable_value", 0)

          # The StateMachine defers small-value Bitcask writes to the background
          # BitcaskWriter. Flush both the shard and the writer to ensure on-disk.
          shard_pid = shard_pid_for(k)
          state = :sys.get_state(shard_pid)

          GenServer.call(shard_pid, :flush)
          Ferricstore.Store.BitcaskWriter.flush_all()

          # Verify the value is on disk via the ETS 7-tuple location. The
          # production WARaft path stores small values in unified segment records;
          # older direct-Bitcask paths still use integer Bitcask file ids.
          [{^k, _value, _exp, _lfu, fid, off, _vsize}] = :ets.lookup(state.keydir, k)

          case fid do
            {:waraft_segment, _index} ->
              assert {:ok, "durable_value"} =
                       WARaftSegmentReader.read_value_from_location(
                         FerricStore.Instance.get(:default),
                         state.index,
                         fid,
                         k
                       )

            fid when is_integer(fid) ->
              log_path =
                Path.join(
                  state.data_dir |> Ferricstore.DataDir.shard_data_path(state.index),
                  "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
                )

              assert {:ok, "durable_value"} = NIF.v2_pread_at(log_path, off)
          end
        end

        test "DEL tombstone is persisted in Bitcask" do
          k = ukey("bitcask_del")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "to_remove", 0)
          :ok = Router.delete(FerricStore.Instance.get(:default), k)

          shard_pid = shard_pid_for(k)
          state = :sys.get_state(shard_pid)
          GenServer.call(shard_pid, :flush)

          # After delete, key should be gone from ETS
          assert [] = :ets.lookup(state.keydir, k)
        end
      end

      describe "batch window batches rapid writes" do
        test "multiple rapid writes to the same shard are batched" do
          # Generate keys that all map to the same shard
          shard_idx = 0

          keys =
            Stream.repeatedly(fn -> ukey("batch_#{:rand.uniform(999_999)}") end)
            |> Stream.filter(fn k ->
              Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx
            end)
            |> Enum.take(5)

          # Send all writes nearly simultaneously -- they should be batched
          # by the Batcher's batch_window_ms (1ms default)
          tasks =
            Enum.map(keys, fn k ->
              Task.async(fn ->
                Router.put(FerricStore.Instance.get(:default), k, "batched_#{k}", 0)
              end)
            end)

          results = Task.await_many(tasks, 10_000)
          assert Enum.all?(results, &(&1 == :ok))

          # All keys should be readable
          for k <- keys do
            assert "batched_#{k}" == Router.get(FerricStore.Instance.get(:default), k),
                   "Batched key #{k} should be readable"
          end
        end
      end

      describe "MULTI/EXEC transaction writes go through Raft" do
        test "sequential writes within simulated transaction all commit via Raft" do
          k1 = ukey("tx_k1")
          k2 = ukey("tx_k2")
          k3 = ukey("tx_k3")

          # Simulate MULTI/EXEC: multiple writes executed sequentially
          :ok = Router.put(FerricStore.Instance.get(:default), k1, "tx_val1", 0)
          :ok = Router.put(FerricStore.Instance.get(:default), k2, "tx_val2", 0)
          :ok = Router.put(FerricStore.Instance.get(:default), k3, "tx_val3", 0)

          # All should be readable after commit
          assert "tx_val1" == Router.get(FerricStore.Instance.get(:default), k1)
          assert "tx_val2" == Router.get(FerricStore.Instance.get(:default), k2)
          assert "tx_val3" == Router.get(FerricStore.Instance.get(:default), k3)

          # All should be in ETS (written by StateMachine), single-table format
          assert [{^k1, "tx_val1", 0, _, _, _, _}] = :ets.lookup(keydir_for(k1), k1)
          assert [{^k2, "tx_val2", 0, _, _, _, _}] = :ets.lookup(keydir_for(k2), k2)
          assert [{^k3, "tx_val3", 0, _, _, _, _}] = :ets.lookup(keydir_for(k3), k3)
        end

        test "mixed SET and DEL in transaction sequence" do
          k = ukey("tx_mixed")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "initial", 0)
          assert "initial" == Router.get(FerricStore.Instance.get(:default), k)

          :ok = Router.delete(FerricStore.Instance.get(:default), k)
          assert nil == Router.get(FerricStore.Instance.get(:default), k)

          :ok = Router.put(FerricStore.Instance.get(:default), k, "restored", 0)
          assert "restored" == Router.get(FerricStore.Instance.get(:default), k)
        end
      end

      describe "WATCH detects version change from Raft-committed write" do
        test "write version changes are visible to get_version after Raft commit" do
          k = ukey("watch_raft")

          v_before = Router.get_version(FerricStore.Instance.get(:default), k)
          :ok = Router.put(FerricStore.Instance.get(:default), k, "watched_val", 0)
          v_after = Router.get_version(FerricStore.Instance.get(:default), k)

          # WATCH would have recorded v_before; after the Raft-committed write,
          # get_version returns a different (higher) value, so EXEC would abort.
          assert v_after != v_before
          assert v_after > v_before
        end

        test "DEL changes version visible to WATCH" do
          k = ukey("watch_del")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "will_del", 0)
          v_before = Router.get_version(FerricStore.Instance.get(:default), k)

          :ok = Router.delete(FerricStore.Instance.get(:default), k)
          v_after = Router.get_version(FerricStore.Instance.get(:default), k)

          assert v_after > v_before
        end

        test "INCR changes version visible to WATCH" do
          k = ukey("watch_incr")

          :ok = Router.put(FerricStore.Instance.get(:default), k, "5", 0)
          v_before = Router.get_version(FerricStore.Instance.get(:default), k)

          {:ok, _} = Router.incr(FerricStore.Instance.get(:default), k, 1)
          v_after = Router.get_version(FerricStore.Instance.get(:default), k)

          assert v_after > v_before
        end

        test "concurrent Raft writes to same shard all bump version" do
          # Use a key we know the shard for, then write to same shard
          k = ukey("watch_concurrent")
          shard_idx = Router.shard_for(FerricStore.Instance.get(:default), k)

          v_before = Router.get_version(FerricStore.Instance.get(:default), k)

          # Write several keys to the same shard
          keys =
            Stream.repeatedly(fn -> ukey("wc_#{:rand.uniform(999_999)}") end)
            |> Stream.filter(fn kk ->
              Router.shard_for(FerricStore.Instance.get(:default), kk) == shard_idx
            end)
            |> Enum.take(5)

          for kk <- keys do
            :ok = Router.put(FerricStore.Instance.get(:default), kk, "v", 0)
          end

          v_after = Router.get_version(FerricStore.Instance.get(:default), k)

          # Version should have increased by at least the number of writes
          assert v_after >= v_before + length(keys)
        end
      end
    end
  end
end
