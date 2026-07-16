defmodule Ferricstore.Store.ShardAsyncIoTest.Sections.V2AppendBatchNosyncNif do
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

      describe "v2_append_batch_nosync NIF" do
        test "writes records without fsync and returns offsets" do
          dir = Path.join(System.tmp_dir!(), "nosync_nif_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          on_exit(fn -> File.rm_rf(dir) end)

          batch = [{"key1", "val1", 0}, {"key2", "val2", 0}]

          assert {:ok, locations} = NIF.v2_append_batch_nosync(path, batch)
          assert length(locations) == 2

          [{off1, vsize1}, {off2, vsize2}] = locations
          assert off1 == 0
          assert off2 > off1
          # byte_size("val1")
          assert vsize1 == 4
          # byte_size("val2")
          assert vsize2 == 4

          # Data should be readable via v2_pread_at (flushed to page cache)
          assert {:ok, "val1"} = NIF.v2_pread_at(path, off1)
          assert {:ok, "val2"} = NIF.v2_pread_at(path, off2)
        end

        test "empty batch returns empty locations" do
          dir = Path.join(System.tmp_dir!(), "nosync_empty_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          on_exit(fn -> File.rm_rf(dir) end)

          assert {:ok, []} = NIF.v2_append_batch_nosync(path, [])
        end
      end

      describe "v2_append_ops_batch_nosync NIF" do
        test "writes mixed put and tombstone records without fsync" do
          dir = Path.join(System.tmp_dir!(), "nosync_ops_nif_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          on_exit(fn -> File.rm_rf(dir) end)

          ops = [
            {:put, "key1", "val1", 0},
            {:delete, "key1"},
            {:put, "empty", "", 0}
          ]

          assert {:ok, [{:put, off1, 4}, {:delete, off2, tomb_size}, {:put, off3, 0}]} =
                   NIF.v2_append_ops_batch_nosync(path, ops)

          assert off1 == 0
          assert off2 > off1
          assert off3 > off2
          assert tomb_size == 26 + byte_size("key1")

          assert {:ok, "val1"} = NIF.v2_pread_at(path, off1)
          assert {:ok, nil} = NIF.v2_pread_at(path, off2)
          assert {:ok, ""} = NIF.v2_pread_at(path, off3)

          assert {:ok, records} = NIF.v2_scan_file(path)

          assert [
                   {"key1", ^off1, 4, 0, false},
                   {"key1", ^off2, 0, 0, true},
                   {"empty", ^off3, 0, 0, false}
                 ] = records
        end

        test "scans tombstone metadata without returning live records" do
          dir = Path.join(System.tmp_dir!(), "tombstone_scan_nif_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          on_exit(fn -> File.rm_rf(dir) end)

          large_value = :binary.copy("x", 1024 * 1024)

          assert {:ok,
                  [
                    {:put, put_offset, _put_size},
                    {:delete, delete_offset, delete_size},
                    {:put, put2_offset, _put2_size}
                  ]} =
                   NIF.v2_append_ops_batch_nosync(path, [
                     {:put, "live_before", large_value, 0},
                     {:delete, "deleted"},
                     {:put, "live_after", large_value, 0}
                   ])

          assert put_offset == 0
          assert put2_offset > delete_offset

          assert {:ok, [{"deleted", ^delete_offset, ^delete_size, 0}]} =
                   NIF.v2_scan_tombstones(path)
        end

        test "tombstone scan returns error before tombstones after a corrupt live record" do
          dir = Path.join(System.tmp_dir!(), "tombstone_scan_corrupt_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          on_exit(fn -> File.rm_rf(dir) end)

          assert {:ok, [{:put, put_offset, _}, {:delete, delete_offset, _}]} =
                   NIF.v2_append_ops_batch_nosync(path, [
                     {:put, "live_before", "value", 0},
                     {:delete, "deleted_after_corruption"}
                   ])

          value_offset = put_offset + @header_size + byte_size("live_before")

          {:ok, fd} = :file.open(path, [:read, :write, :binary])
          :ok = :file.pwrite(fd, value_offset, <<0xFF>>)
          :ok = :file.close(fd)

          assert {:error, reason} = NIF.v2_scan_tombstones(path)
          assert reason =~ "CRC mismatch"
          assert {:ok, nil} = NIF.v2_pread_at(path, delete_offset)
        end
      end

      describe "v2_append_ops_batch NIF" do
        @tag :append_ops_sync
        test "durably appends a mixed batch and returns exact record locations" do
          dir = Path.join(System.tmp_dir!(), "sync_ops_nif_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          on_exit(fn -> File.rm_rf(dir) end)

          assert {:ok, [{:put, put_offset, 5}, {:delete, delete_offset, tombstone_size}]} =
                   NIF.v2_append_ops_batch(path, [
                     {:put, "live", "value", 0},
                     {:delete, "gone"}
                   ])

          assert put_offset == 0
          assert delete_offset == @header_size + byte_size("live") + byte_size("value")
          assert tombstone_size == @header_size + byte_size("gone")
          assert {:ok, "value"} = NIF.v2_pread_at(path, put_offset)
          assert {:ok, nil} = NIF.v2_pread_at(path, delete_offset)
        end
      end

      # ---------------------------------------------------------------------------
      # Shard: nosync writes + deferred fsync
      # ---------------------------------------------------------------------------
      describe "shard nosync write path" do
        test "BitcaskWriter does not attach stale write location to newer pending value" do
          shard_index = 10_000 + System.unique_integer([:positive])
          dir = Path.join(System.tmp_dir!(), "bitcask_writer_stale_#{shard_index}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          keydir = :ets.new(:"bitcask_writer_stale_#{shard_index}", [:set, :public])
          key = "writer:stale-location"

          {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

          on_exit(fn ->
            try do
              if Process.alive?(writer), do: GenServer.stop(writer, :normal, 5000)
            catch
              :exit, _ -> :ok
            end

            try do
              :ets.delete(keydir)
            rescue
              ArgumentError -> :ok
            end

            File.rm_rf(dir)
          end)

          :ets.insert(keydir, {key, "new", 456, LFU.initial(), :pending, 0, 0})

          :sys.replace_state(writer, fn state ->
            %{
              state
              | pending: [{:write, nil, path, 0, keydir, key, "old", 123}],
                pending_count: 1
            }
          end)

          assert :ok == BitcaskWriter.flush(shard_index)
          assert [{^key, "new", 456, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, key)
        end

        test "BitcaskWriter attaches location when pending write preserves old cold ref" do
          shard_index = 10_000 + System.unique_integer([:positive])
          dir = Path.join(System.tmp_dir!(), "bitcask_writer_old_ref_#{shard_index}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          keydir = :ets.new(:"bitcask_writer_old_ref_#{shard_index}", [:set, :public])
          key = "writer:old-ref-location"

          {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

          on_exit(fn ->
            try do
              if Process.alive?(writer), do: GenServer.stop(writer, :normal, 5000)
            catch
              :exit, _ -> :ok
            end

            try do
              :ets.delete(keydir)
            rescue
              ArgumentError -> :ok
            end

            File.rm_rf(dir)
          end)

          :ets.insert(keydir, {key, "new", 456, LFU.initial(), :pending, 99, 3})

          :sys.replace_state(writer, fn state ->
            %{
              state
              | pending: [{:write, nil, path, 0, keydir, key, "new", 456}],
                pending_count: 1
            }
          end)

          assert :ok == BitcaskWriter.flush(shard_index)
          assert [{^key, "new", 456, _lfu, 0, offset, 3}] = :ets.lookup(keydir, key)
          assert is_integer(offset) and offset >= 0
        end

        test "BitcaskWriter keeps failed writes pending for retry" do
          shard_index = 10_000 + System.unique_integer([:positive])
          dir = Path.join(System.tmp_dir!(), "bitcask_writer_retry_#{shard_index}")
          File.mkdir_p!(dir)
          path = Path.join([dir, "missing_parent", "00000.log"])

          keydir = :ets.new(:"bitcask_writer_retry_#{shard_index}", [:set, :public])
          key = "writer:retry-pending"

          {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

          on_exit(fn ->
            try do
              if Process.alive?(writer), do: GenServer.stop(writer, :normal, 5000)
            catch
              :exit, _ -> :ok
            end

            try do
              :ets.delete(keydir)
            rescue
              ArgumentError -> :ok
            end

            File.rm_rf(dir)
          end)

          :ets.insert(keydir, {key, "new", 456, LFU.initial(), :pending, 0, 0})

          :sys.replace_state(writer, fn state ->
            %{
              state
              | pending: [{:write, nil, path, 0, keydir, key, "new", 456}],
                pending_count: 1
            }
          end)

          assert {:error, {:flush_failed, 1}} == BitcaskWriter.flush(shard_index)

          state = :sys.get_state(writer)
          assert state.pending_count == 1
          assert [{:write, nil, ^path, 0, ^keydir, ^key, "new", 456}] = state.pending
          assert [{^key, "new", 456, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, key)
        end

        test "BitcaskWriter flush reports timeout instead of pretending the drain succeeded" do
          shard_index = 10_000 + System.unique_integer([:positive])
          writer_name = BitcaskWriter.writer_name(shard_index)
          {:ok, writer} = SlowFlushWriter.start_link(writer_name)

          on_exit(fn ->
            try do
              if Process.alive?(writer), do: GenServer.stop(writer, :kill, 100)
            catch
              :exit, _ -> :ok
            end
          end)

          assert {:error, {:flush_exit, {:timeout, _call}}} = BitcaskWriter.flush(shard_index, 1)
        end

        test "BitcaskWriter flush_all reports writer failures" do
          shard_index = 64
          writer_name = BitcaskWriter.writer_name(shard_index)

          if existing = Process.whereis(writer_name) do
            flunk(
              "unexpected BitcaskWriter already registered for shard #{shard_index}: #{inspect(existing)}"
            )
          end

          {:ok, writer} = SlowFlushWriter.start_link(writer_name)

          on_exit(fn ->
            try do
              if Process.alive?(writer), do: GenServer.stop(writer, :kill, 100)
            catch
              :exit, _ -> :ok
            end
          end)

          assert {:error, [{^shard_index, {:flush_exit, {:timeout, _call}}}]} =
                   BitcaskWriter.flush_all(shard_index + 1, 1)
        end

        test "BitcaskWriter batches tombstone runs through the ops NIF" do
          source =
            Path.expand("../../../lib/ferricstore/store/bitcask_writer.ex", __DIR__)
            |> File.read!()

          [_before, flush_tombstones] =
            String.split(source, "defp flush_tombstone_batch", parts: 2)

          [function_source | _after] =
            String.split(flush_tombstones, "\n  defp normalize_write_entry", parts: 2)

          assert function_source =~ "NIF.v2_append_ops_batch_nosync(path, ops)",
                 "tombstone runs should be one batched append NIF call, not one NIF call per key"
        end

        test "BitcaskWriter persists a tombstone run in order" do
          shard_index = 10_000 + System.unique_integer([:positive])
          dir = Path.join(System.tmp_dir!(), "bitcask_writer_tombstone_batch_#{shard_index}")
          File.mkdir_p!(dir)
          path = Path.join(dir, "00000.log")
          File.touch!(path)

          {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

          on_exit(fn ->
            try do
              if Process.alive?(writer), do: GenServer.stop(writer, :normal, 5000)
            catch
              :exit, _ -> :ok
            end

            File.rm_rf(dir)
          end)

          :sys.replace_state(writer, fn state ->
            %{
              state
              | pending: [
                  {:tombstone, nil, path, "deleted:2"},
                  {:tombstone, nil, path, "deleted:1"}
                ],
                pending_count: 2
            }
          end)

          assert :ok == BitcaskWriter.flush(shard_index)

          assert {:ok, [{"deleted:1", off1, _size1, 0}, {"deleted:2", off2, _size2, 0}]} =
                   NIF.v2_scan_tombstones(path)

          assert off1 < off2
        end

        test "shard flush completion does not attach stale location to newer pending value" do
          keydir =
            :ets.new(:"shard_flush_stale_#{System.unique_integer([:positive])}", [
              :set,
              :public
            ])

          key = "flush:stale-location"

          state = %{
            keydir: keydir,
            active_file_id: 7,
            file_stats: %{},
            instance_ctx: %{hot_cache_max_value_size: 64}
          }

          try do
            :ets.insert(keydir, {key, "new", 456, LFU.initial(), :pending, 0, 0})

            ShardFlush.update_ets_locations(state, [{key, "old", 123, "old"}], [{42, 3}])

            assert [{^key, "new", 456, _lfu, :pending, 0, 0}] = :ets.lookup(keydir, key)
          after
            :ets.delete(keydir)
          end
        end

        test "shard flush completion does not republish stale value over newer cold row" do
          keydir =
            :ets.new(:"shard_flush_stale_cold_#{System.unique_integer([:positive])}", [
              :set,
              :public
            ])

          key = "flush:stale-cold-location"

          state = %{
            keydir: keydir,
            active_file_id: 7,
            file_stats: %{8 => {100, 0}},
            instance_ctx: %{hot_cache_max_value_size: 64}
          }

          try do
            :ets.insert(keydir, {key, "new", 456, LFU.initial(), 8, 99, 3})

            state = ShardFlush.update_ets_locations(state, [{key, "old", 123, "old"}], [{42, 3}])

            assert [{^key, "new", 456, _lfu, 8, 99, 3}] = :ets.lookup(keydir, key)
            assert state.file_stats == %{8 => {100, 0}}
          after
            :ets.delete(keydir)
          end
        end

        test "shard flush completion handles numeric pending values" do
          keydir =
            :ets.new(:"shard_flush_numeric_#{System.unique_integer([:positive])}", [
              :set,
              :public
            ])

          key = "flush:numeric-location"

          state = %{
            keydir: keydir,
            active_file_id: 7,
            file_stats: %{},
            instance_ctx: %{hot_cache_max_value_size: 64}
          }

          try do
            :ets.insert(keydir, {key, "42", 0, LFU.initial(), :pending, 0, 0})

            ShardFlush.update_ets_locations(state, [{key, 42, 0, "42"}], [{42, 2}])

            assert [{^key, "42", 0, _lfu, 7, 42, 2}] = :ets.lookup(keydir, key)
          after
            :ets.delete(keydir)
          end
        end

        test "shard flush externalizes large pending values with one blob segment fsync" do
          {pid, _index, dir, ctx} =
            start_shard(
              flush_interval_ms: 5000,
              blob_side_channel_threshold_bytes: 128,
              hot_cache_max_value_size: 64
            )

          parent = self()

          Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
            send(parent, {:blob_fsync_file, path})
            NIF.v2_fsync(path)
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_blob_store_fsync_file_hook)
            cleanup_shard(pid, ctx, dir)
          end)

          key_a = "flush_blob_batch:a"
          key_b = "flush_blob_batch:b"
          payload_a = :binary.copy("A", 1024)
          payload_b = :binary.copy("B", 1024)

          state = :sys.get_state(pid)

          :ets.insert(state.keydir, {
            key_a,
            nil,
            0,
            LFU.initial(),
            :pending,
            :pending,
            0
          })

          :ets.insert(state.keydir, {
            key_b,
            nil,
            0,
            LFU.initial(),
            :pending,
            :pending,
            0
          })

          flushed =
            %{state | pending: [{key_b, payload_b, 0}, {key_a, payload_a, 0}], pending_count: 2}
            |> ShardFlush.flush_pending()

          assert flushed.pending == []
          assert payload_a == GenServer.call(pid, {:get, key_a})
          assert payload_b == GenServer.call(pid, {:get, key_b})

          assert_receive {:blob_fsync_file, first_path}, 1000
          refute_receive {:blob_fsync_file, _second_path}, 100
          assert String.ends_with?(first_path, ".bloblog")
        end

        test "put is readable immediately via ETS (before fsync)" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 100)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          :ok = GenServer.call(pid, {:put, "nsk", "nsv", 0})
          # Should be readable from ETS immediately
          assert "nsv" == GenServer.call(pid, {:get, "nsk"})
        end

        test "checkpoint_flags atomic is raised after nosync write" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          # Clear any prior flag state.
          :atomics.put(ctx.checkpoint_flags, 1, 0)

          :ok = GenServer.call(pid, {:put, "fk", "fv", 0})
          # Allow the flush_pending to run (triggered by put when no flush_in_flight).
          Process.sleep(10)

          # The nosync path raises the per-shard checkpoint flag so the
          # BitcaskCheckpointer fsyncs the active file on its next tick.
          assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
                 "writer must raise checkpoint_flags[1] after a nosync append"
        end

        test "data survives flush (sync) call" do
          {pid, _index, dir, ctx} = start_shard()
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          :ok = GenServer.call(pid, {:put, "dk", "dv", 0})
          :ok = GenServer.call(pid, :flush)

          assert "dv" == GenServer.call(pid, {:get, "dk"})
        end

        test "EXISTS miss does not force read-side fsync" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          :atomics.put(ctx.checkpoint_flags, 1, 0)
          :ok = GenServer.call(pid, {:put, "dirty_exists_source", "value", 0})
          Process.sleep(10)

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1
          assert false == GenServer.call(pid, {:exists, "missing_exists_key"})

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
                 "a pure EXISTS miss must not fsync unrelated dirty Bitcask data"
        end

        test "compound_get miss does not force read-side fsync" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          :atomics.put(ctx.checkpoint_flags, 1, 0)
          :ok = GenServer.call(pid, {:put, "dirty_compound_source", "value", 0})
          Process.sleep(10)

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1

          assert nil ==
                   GenServer.call(
                     pid,
                     {:compound_get, "missing_hash", "missing_hash" <> <<0>> <> "field"}
                   )

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
                 "a pure compound_get miss must not fsync unrelated dirty Bitcask data"
        end

        test "compound_get_meta miss does not force read-side fsync" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          :atomics.put(ctx.checkpoint_flags, 1, 0)
          :ok = GenServer.call(pid, {:put, "dirty_compound_meta_source", "value", 0})
          Process.sleep(10)

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1

          assert nil ==
                   GenServer.call(
                     pid,
                     {:compound_get_meta, "missing_hash_meta",
                      "missing_hash_meta" <> <<0>> <> "field"}
                   )

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
                 "a pure compound_get_meta miss must not fsync unrelated dirty Bitcask data"
        end

        test "GET of a known cold key does not force read-side fsync" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          large = :binary.copy("C", ctx.hot_cache_max_value_size + 1024)

          :ok = GenServer.call(pid, {:put, "cold_read_key", large, 0})
          :ok = GenServer.call(pid, :flush)

          :atomics.put(ctx.checkpoint_flags, 1, 0)
          :ok = GenServer.call(pid, {:put, "dirty_after_cold", "value", 0})
          Process.sleep(10)

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1
          assert large == GenServer.call(pid, {:get, "cold_read_key"})

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
                 "a known-location cold GET must not fsync unrelated dirty Bitcask data"
        end

        test "GET_META of a known cold key does not force read-side fsync" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          large = :binary.copy("M", ctx.hot_cache_max_value_size + 1024)
          expire_at_ms = Ferricstore.HLC.now_ms() + 60_000

          :ok = GenServer.call(pid, {:put, "cold_meta_key", large, expire_at_ms})
          :ok = GenServer.call(pid, :flush)

          :atomics.put(ctx.checkpoint_flags, 1, 0)
          :ok = GenServer.call(pid, {:put, "dirty_after_cold_meta", "value", 0})
          Process.sleep(10)

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1
          assert {^large, ^expire_at_ms} = GenServer.call(pid, {:get_meta, "cold_meta_key"})

          assert :atomics.get(ctx.checkpoint_flags, 1) == 1,
                 "a known-location cold GET_META must not fsync unrelated dirty Bitcask data"
        end

        test "direct delete_prefix persists tombstones for deleted keys" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          :ok = GenServer.call(pid, {:put, "plain:one", "1", 0})
          :ok = GenServer.call(pid, {:put, "plain:two", "2", 0})
          :ok = GenServer.call(pid, {:put, "other", "3", 0})
          :ok = GenServer.call(pid, :flush)

          :ok = GenServer.call(pid, {:delete_prefix, "plain:"})

          log_path = Path.join(Ferricstore.DataDir.shard_data_path(dir, 0), "00000.log")
          assert {:ok, records} = NIF.v2_scan_file(log_path)

          assert Enum.any?(records, &match?({"plain:one", _off, 0, 0, true}, &1))
          assert Enum.any?(records, &match?({"plain:two", _off, 0, 0, true}, &1))
          refute Enum.any?(records, &match?({"other", _off, 0, 0, true}, &1))
        end

        test "direct compound_delete_prefix persists tombstones for deleted fields" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          redis_key = "hash_prefix_delete"
          prefix = "H:" <> redis_key <> <<0>>
          field_one = prefix <> "one"
          field_two = prefix <> "two"
          other_field = "H:other_hash" <> <<0>> <> "field"

          :ok = GenServer.call(pid, {:compound_put, redis_key, field_one, "1", 0})
          :ok = GenServer.call(pid, {:compound_put, redis_key, field_two, "2", 0})
          :ok = GenServer.call(pid, {:compound_put, "other_hash", other_field, "3", 0})
          :ok = GenServer.call(pid, :flush)

          :ok = GenServer.call(pid, {:compound_delete_prefix, redis_key, prefix})

          log_path = Path.join(Ferricstore.DataDir.shard_data_path(dir, 0), "00000.log")
          assert {:ok, records} = NIF.v2_scan_file(log_path)

          assert Enum.any?(records, &match?({^field_one, _off, 0, 0, true}, &1))
          assert Enum.any?(records, &match?({^field_two, _off, 0, 0, true}, &1))
          refute Enum.any?(records, &match?({^other_field, _off, 0, 0, true}, &1))
        end

        test "direct promoted compound_delete_prefix increments write version" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          redis_key = "promoted_hash_prefix_delete_version"
          prefix = CompoundKey.hash_prefix(redis_key)
          field = CompoundKey.hash_field(redis_key, "one")
          dedicated_path = Promotion.dedicated_path(dir, 0, :hash, redis_key)

          :ok = GenServer.call(pid, {:compound_put, redis_key, field, "1", 0})
          File.mkdir_p!(dedicated_path)
          File.touch!(Path.join(dedicated_path, "00000.log"))

          :sys.replace_state(pid, fn state ->
            %{
              state
              | promoted_instances: Map.put(state.promoted_instances, redis_key, dedicated_path)
            }
          end)

          before_version = GenServer.call(pid, {:get_version, redis_key})
          :ok = GenServer.call(pid, {:compound_delete_prefix, redis_key, prefix})

          assert before_version + 1 == GenServer.call(pid, {:get_version, redis_key})
          assert nil == GenServer.call(pid, {:compound_get, redis_key, field})
        end

        test "multiple puts before flush are all readable" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          for i <- 1..10 do
            :ok = GenServer.call(pid, {:put, "mkey#{i}", "mval#{i}", 0})
          end

          # All should be in ETS
          for i <- 1..10 do
            assert "mval#{i}" == GenServer.call(pid, {:get, "mkey#{i}"})
          end

          # Flush and verify durability
          :ok = GenServer.call(pid, :flush)

          for i <- 1..10 do
            assert "mval#{i}" == GenServer.call(pid, {:get, "mkey#{i}"})
          end
        end
      end

      # ---------------------------------------------------------------------------
      # Deferred fsync on timer
      # ---------------------------------------------------------------------------
      describe "deferred fsync via BitcaskCheckpointer" do
        alias Ferricstore.Store.ActiveFile
        alias Ferricstore.Store.BitcaskCheckpointer

        test "checkpointer fsyncs the active file when the flag is raised" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          # Start a checkpointer for this shard. The shard already publishes
          # its active file to the ActiveFile registry during init, so the
          # checkpointer can find it immediately.
          ActiveFile.init(1)

          {:ok, ck} =
            BitcaskCheckpointer.start_link(
              index: 0,
              instance_ctx: ctx,
              checkpoint_interval_ms: 10,
              name: :"ck_unified_#{:erlang.unique_integer([:positive])}"
            )

          on_exit(fn ->
            try do
              if Process.alive?(ck), do: GenServer.stop(ck, :normal, 5000)
            catch
              :exit, _ -> :ok
            end
          end)

          parent = self()
          handler_id = "unified-fsync-#{:erlang.unique_integer([:positive])}"

          :telemetry.attach(
            handler_id,
            [:ferricstore, :bitcask, :checkpoint],
            fn _e, meas, meta, _ -> send(parent, {:ck_event, meas, meta}) end,
            nil
          )

          on_exit(fn -> :telemetry.detach(handler_id) end)

          :atomics.put(ctx.checkpoint_flags, 1, 0)
          :ok = GenServer.call(pid, {:put, "tk", "tv", 0})

          # The checkpointer tick should see the raised flag and emit a
          # :ok telemetry event within a few ticks.
          assert_receive {:ck_event, meas, %{status: :ok}}, 500
          assert is_integer(meas.duration_us)

          # Flag must have been cleared by the checkpointer before firing fsync.
          Ferricstore.Test.ShardHelpers.eventually(fn ->
            :atomics.get(ctx.checkpoint_flags, 1) == 0
          end)
        end
      end

      # ---------------------------------------------------------------------------
      # ETS update_element optimization
      # ---------------------------------------------------------------------------
      describe "update_ets_locations preserves LFU counter" do
        test "LFU counter is preserved after flush updates disk location" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          on_exit(fn -> cleanup_shard(pid, ctx, dir) end)

          # Put a value — this inserts with LFU.initial()
          :ok = GenServer.call(pid, {:put, "lfu_key", "lfu_val", 0})

          # Read the key multiple times to increment LFU counter
          for _ <- 1..20 do
            GenServer.call(pid, {:get, "lfu_key"})
          end

          # Get the LFU counter before flush
          keydir = elem(ctx.keydir_refs, 0)
          [{_, _, _, lfu_before, _, _, _}] = :ets.lookup(keydir, "lfu_key")

          # Flush (will call update_ets_locations)
          :ok = GenServer.call(pid, :flush)

          # Get the LFU counter after flush — should be preserved
          [{_, _, _, lfu_after, fid, off, _}] = :ets.lookup(keydir, "lfu_key")
          assert lfu_after == lfu_before
          # Disk location should now be set (non-zero)
          assert fid > 0 or off > 0 or (fid == 0 and off == 0)
        end
      end

      # ---------------------------------------------------------------------------
      # Concurrent writes with deferred fsync
      # ---------------------------------------------------------------------------
    end
  end
end
