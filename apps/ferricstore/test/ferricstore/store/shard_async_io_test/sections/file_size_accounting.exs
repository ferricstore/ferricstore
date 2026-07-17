defmodule Ferricstore.Store.ShardAsyncIoTest.Sections.FileSizeAccounting do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.{CompoundKey, Promotion}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.Shard
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.Reads, as: ShardReads
      alias Ferricstore.Store.ShardAsyncIoTest.SlowFlushWriter

      describe "file size accounting" do
        @tag :hlc_drift_guard
        test "recovery stats preserve wall-live bytes during unsafe HLC drift" do
          dir =
            Path.join(
              System.tmp_dir!(),
              "unsafe_drift_file_stats_#{System.unique_integer([:positive])}"
            )

          keydir = :ets.new(:unsafe_drift_file_stats, [:set])
          hlc_ref = :persistent_term.get(:ferricstore_hlc_ref)
          previous_hlc = :atomics.get(hlc_ref, 1)

          try do
            key = "wall-live-file-stats"
            value_size = 8
            total_bytes = 100
            wall_ms = System.os_time(:millisecond)
            expire_at_ms = wall_ms + 30_000

            File.mkdir_p!(dir)
            File.write!(Path.join(dir, "00000.log"), :binary.copy("x", total_bytes))

            :ets.insert(
              keydir,
              {key, nil, expire_at_ms, LFU.initial(), 0, 0, value_size}
            )

            :atomics.put(hlc_ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

            live_bytes = 26 + byte_size(key) + value_size

            assert ShardFlush.compute_file_stats(dir, keydir)[0] ==
                     {total_bytes, total_bytes - live_bytes}
          after
            :atomics.put(hlc_ref, 1, previous_hlc)
            :ets.delete(keydir)
            File.rm_rf(dir)
          end
        end

        test "active_file_size tracks full record bytes after batch flush" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          key = "size_accounting_#{:erlang.unique_integer([:positive])}"
          value = "value"

          try do
            baseline_size = :sys.get_state(pid).active_file_size
            assert :ok == GenServer.call(pid, {:put, key, value, 0})
            assert :ok == GenServer.call(pid, :flush)

            {_fid, active_path} = GenServer.call(pid, :get_active_file)
            state = :sys.get_state(pid)

            assert state.active_file_size == File.stat!(active_path).size

            assert state.active_file_size ==
                     baseline_size + 26 + byte_size(key) + byte_size(value)
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "preserves tombstones in compacted mixed live/deleted files" do
          previous_trap_exit = Process.flag(:trap_exit, true)

          dir =
            Path.join(System.tmp_dir!(), "mixed_tombstone_compaction_#{:rand.uniform(9_999_999)}")

          File.mkdir_p!(dir)

          name = :"mixed_tombstone_compaction_#{:erlang.unique_integer([:positive])}"

          ctx =
            FerricStore.Instance.build(name,
              data_dir: dir,
              shard_count: 1
            )

          try do
            :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
            shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)

            log0 = Path.join(shard_dir, "00000.log")
            log1 = Path.join(shard_dir, "00001.log")
            log2 = Path.join(shard_dir, "00002.log")

            {:ok, [_]} = NIF.v2_append_batch(log0, [{"a", "old", 0}])
            {:ok, [_]} = NIF.v2_append_batch(log1, [{"b", "live", 0}])
            {:ok, _} = NIF.v2_append_tombstone(log1, "a")
            File.touch!(log2)

            {:ok, pid1} =
              Shard.start_link(
                index: 0,
                data_dir: dir,
                flush_interval_ms: 5000,
                instance_ctx: ctx
              )

            assert nil == GenServer.call(pid1, {:get, "a"})
            assert "live" == GenServer.call(pid1, {:get, "b"})

            assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid1, {:run_compaction, [1]})

            :ok = GenServer.stop(pid1, :normal, 5_000)

            pid2 = restart_shard(dir, ctx, 5000)
            assert nil == GenServer.call(pid2, {:get, "a"})
            assert "live" == GenServer.call(pid2, {:get, "b"})
          after
            case Process.whereis(Router.shard_name(ctx, 0)) do
              pid when is_pid(pid) ->
                cleanup_shard(pid, ctx, dir)

              _ ->
                FerricStore.Instance.cleanup(ctx.name)
                File.rm_rf(dir)
            end

            Process.flag(:trap_exit, previous_trap_exit)
          end
        end
      end
    end
  end
end
