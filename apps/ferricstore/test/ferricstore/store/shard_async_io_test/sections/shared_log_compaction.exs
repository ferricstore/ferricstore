defmodule Ferricstore.Store.ShardAsyncIoTest.Sections.SharedLogCompaction do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Flow.{LMDB, Locator}
      alias Ferricstore.Store.{CompoundKey, Promotion}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.Shard
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.Reads, as: ShardReads
      alias Ferricstore.Store.ShardAsyncIoTest.SlowFlushWriter

      describe "shared log compaction" do
        test "manual compaction is rejected while writes are paused for promotion" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            assert :ok = GenServer.call(pid, {:pause_writes})

            assert {:error, "ERR shard writes paused for sync"} =
                     GenServer.call(pid, {:run_compaction, [0]})

            assert :ok = GenServer.call(pid, {:resume_writes})
            assert {:ok, {0, 0, 0}} = GenServer.call(pid, {:run_compaction, [0]})
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "manual compaction skips the active log file" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            active_path = Path.join([dir, "data", "shard_0", "00000.log"])
            assert File.exists?(active_path)

            assert {:ok, {0, 0, 0}} = GenServer.call(pid, {:run_compaction, [0]})
            assert File.exists?(active_path)

            assert :ok = GenServer.call(pid, {:put, "active_compaction_survives", "v", 0})
            assert :ok = GenServer.call(pid, :flush)
            assert "v" == GenServer.call(pid, {:get, "active_compaction_survives"})
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "manual compaction skips the active log file even when it has live entries" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            active_path = Path.join([dir, "data", "shard_0", "00000.log"])
            assert :ok = GenServer.call(pid, {:put, "active_live_compaction", "value", 0})
            assert :ok = GenServer.call(pid, :flush)

            size_before = File.stat!(active_path).size
            {_total_before, dead_before} = Map.fetch!(:sys.get_state(pid).file_stats, 0)

            assert {:ok, {0, 0, 0}} = GenServer.call(pid, {:run_compaction, [0]})
            assert File.exists?(active_path)
            assert File.stat!(active_path).size == size_before
            assert "value" == GenServer.call(pid, {:get, "active_live_compaction"})

            state = :sys.get_state(pid)
            assert state.active_file_size == size_before
            assert Map.fetch!(state.file_stats, 0) == {size_before, dead_before}
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "manual compaction truncates leftover temp log before copying live records" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            shard_path = Path.join([dir, "data", "shard_0"])
            compact_path = Path.join(shard_path, "compact_0.log")
            live_key = "compact_live_#{System.unique_integer([:positive])}"
            stale_key = "compact_stale_#{System.unique_integer([:positive])}"

            assert :ok = GenServer.call(pid, {:put, live_key, "live", 0})
            assert :ok = GenServer.call(pid, :flush)
            force_rotate_active_file(pid)

            assert {:ok, [_stale_location]} =
                     NIF.v2_append_batch(compact_path, [{stale_key, "stale", 0}])

            assert {:ok, {copied, 0, _reclaimed}} =
                     GenServer.call(pid, {:run_compaction, [0]})

            assert copied >= 1
            refute File.exists?(compact_path)

            GenServer.stop(pid, :normal, 5000)
            restarted = restart_shard(dir, ctx, 5000)

            assert "live" == GenServer.call(restarted, {:get, live_key})
            assert nil == GenServer.call(restarted, {:get, stale_key})
          after
            case Process.whereis(elem(ctx.shard_names, 0)) do
              nil ->
                FerricStore.Instance.cleanup(ctx.name)
                File.rm_rf(dir)

              live_pid ->
                cleanup_shard(live_pid, ctx, dir)
            end
          end
        end

        test "manual compaction skips the registry active log after external rotation" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            shard_path = Path.join([dir, "data", "shard_0"])
            registry_active_path = Path.join(shard_path, "00001.log")
            key = "registry_active_compaction_survives"
            value = "registry-active-value"

            Ferricstore.FS.touch!(registry_active_path)
            Ferricstore.Store.ActiveFile.publish(ctx, 0, 1, registry_active_path, shard_path)

            {:ok, [{offset, _value_size}]} =
              NIF.v2_append_batch(registry_active_path, [{key, value, 0}])

            state = :sys.get_state(pid)
            assert state.active_file_id == 0

            assert {1, ^registry_active_path, ^shard_path} =
                     Ferricstore.Store.ActiveFile.get(ctx, 0)

            :ets.insert(state.keydir, {key, value, 0, LFU.initial(), 1, offset, byte_size(value)})

            size_before = File.stat!(registry_active_path).size

            assert {:ok, {0, 0, 0}} = GenServer.call(pid, {:run_compaction, [1]})
            assert File.stat!(registry_active_path).size == size_before
            assert "registry-active-value" == GenServer.call(pid, {:get, key})

            state = :sys.get_state(pid)
            assert state.active_file_id == 1
            assert Map.fetch!(state.file_stats, 1) == {size_before, 0}
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "manual compaction drops expired-only inactive files" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            key = "expired_only_compaction"
            expired_at = System.os_time(:millisecond) - 1_000
            assert :ok = force_rotate_active_file(pid)
            {source_id, source} = GenServer.call(pid, :get_active_file)

            assert :ok = GenServer.call(pid, {:put, key, "expired", expired_at})
            assert :ok = GenServer.call(pid, :flush)
            old_size = File.stat!(source).size

            assert :ok = force_rotate_active_file(pid)

            assert {:ok, {0, 0, reclaimed}} =
                     GenServer.call(pid, {:run_compaction, [source_id]})

            assert reclaimed >= old_size
            refute File.exists?(source)
            assert nil == GenServer.call(pid, {:get, key})
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        @tag :hlc_drift_guard
        test "manual compaction preserves wall-live records during unsafe HLC drift" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          hlc_ref = :persistent_term.get(:ferricstore_hlc_ref)
          previous_hlc = :atomics.get(hlc_ref, 1)

          try do
            key = "unsafe_drift_compaction"
            wall_ms = System.os_time(:millisecond)
            expire_at_ms = wall_ms + 30_000

            assert :ok = force_rotate_active_file(pid)
            {source_id, source} = GenServer.call(pid, :get_active_file)

            assert :ok = GenServer.call(pid, {:put, key, "wall-live", expire_at_ms})
            assert :ok = GenServer.call(pid, :flush)
            assert :ok = force_rotate_active_file(pid)

            future_hlc_ms = wall_ms + 60_000
            :atomics.put(hlc_ref, 1, Bitwise.bsl(future_hlc_ms, 16))

            assert {:ok, {1, 0, _reclaimed}} =
                     GenServer.call(pid, {:run_compaction, [source_id]})

            assert File.exists?(source)

            assert [
                     {^key, _value, ^expire_at_ms, _lfu, ^source_id, _offset, _value_size}
                   ] = :ets.lookup(:sys.get_state(pid).keydir, key)
          after
            :atomics.put(hlc_ref, 1, previous_hlc)
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "manual compaction reports all-dead source removal failure" do
          previous_trap_exit = Process.flag(:trap_exit, true)

          dir =
            Path.join(System.tmp_dir!(), "shared_compaction_rm_fail_#{:rand.uniform(9_999_999)}")

          File.mkdir_p!(dir)

          name = :"shared_compaction_rm_fail_#{:erlang.unique_integer([:positive])}"

          ctx =
            FerricStore.Instance.build(name,
              data_dir: dir,
              shard_count: 1
            )

          try do
            :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)
            shard_dir = Ferricstore.DataDir.shard_data_path(dir, 0)
            source = Path.join(shard_dir, "00000.log")
            active = Path.join(shard_dir, "00001.log")

            File.rm(source)
            File.mkdir!(source)
            File.touch!(active)

            {:ok, pid} =
              Shard.start_link(
                index: 0,
                data_dir: dir,
                flush_interval_ms: 5000,
                instance_ctx: ctx
              )

            assert {:error, {:compaction_failed, [{0, {:remove_failed, _reason}}]}} =
                     GenServer.call(pid, {:run_compaction, [0]})

            assert File.dir?(source)
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

        test "drains deferred BitcaskWriter writes before selecting compacted records" do
          source = Ferricstore.Test.SourceFiles.shard_source()

          [_before, run_compaction_section] =
            String.split(
              source,
              "def handle_call({:run_compaction, file_ids}, _from, state) do",
              parts: 2
            )

          [run_compaction_body | _after] =
            Regex.split(~r/^\s*def handle_call/ms, run_compaction_section, parts: 2)

          flush_pos =
            :binary.match(
              run_compaction_body,
              "BitcaskWriter.flush(state.instance_ctx, state.index)"
            )

          reduce_pos = :binary.match(run_compaction_body, "Enum.reduce(file_ids")

          assert {flush_offset, _} = flush_pos
          assert {reduce_offset, _} = reduce_pos
          assert flush_offset < reduce_offset
        end

        test "aborts compaction when deferred BitcaskWriter flush fails" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
          source = Path.join(shard_path, "00000.log")
          {:ok, writer} = BitcaskWriter.start_link(shard_index: 0, instance_ctx: ctx)

          try do
            {:ok, {_dead_offset, _dead_record_size}} =
              NIF.v2_append_record(source, "dead", "old", 0)

            {:ok, {live_offset, _live_record_size}} =
              NIF.v2_append_record(source, "live", "value", 0)

            source_size = File.stat!(source).size

            keydir = :sys.get_state(pid).keydir

            :ets.insert(
              keydir,
              {"live", nil, 0, LFU.initial(), 0, live_offset, byte_size("value")}
            )

            assert :ok = force_rotate_active_file(pid)

            bad_path = Path.join([dir, "missing_parent", "00000.log"])

            :sys.replace_state(writer, fn state ->
              %{
                state
                | pending: [{:write, ctx, bad_path, 0, keydir, "pending", "new", 0}],
                  pending_count: 1
              }
            end)

            assert {:error, {:bitcask_writer_flush_failed, {:flush_failed, 1}}} =
                     GenServer.call(pid, {:run_compaction, [0]})

            assert File.stat!(source).size == source_size
            assert {:ok, "value"} = NIF.v2_pread_at(source, live_offset)
          after
            if writer && Process.alive?(writer) do
              :sys.replace_state(writer, fn state -> %{state | pending: [], pending_count: 0} end)
              GenServer.stop(writer, :normal, 5_000)
            end

            cleanup_shard(pid, ctx, dir)
          end
        end

        test "aborts before replacing a segment when an exact cold catalog batch fails" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            source = Path.join([dir, "data", "shard_0", "00000.log"])

            assert :ok = GenServer.call(pid, {:put, "catalog_scan_live", "live", 0})
            assert :ok = GenServer.call(pid, {:put, "catalog_scan_dead", "dead", 0})
            assert :ok = GenServer.call(pid, :flush)
            assert :ok = GenServer.call(pid, {:delete, "catalog_scan_dead"})
            assert :ok = GenServer.call(pid, :flush)
            assert :ok = force_rotate_active_file(pid)

            original = File.read!(source)

            :sys.replace_state(pid, fn state ->
              Map.put(state, :compaction_cold_get_many_fun, fn _path, _keys ->
                {:error, :catalog_io_failed}
              end)
            end)

            assert {:error,
                    {:compaction_failed,
                     [
                       {0,
                        {:compaction_plan_failed, {:cold_catalog_read_failed, :catalog_io_failed}}}
                     ]}} =
                     GenServer.call(pid, {:run_compaction, [0]})

            assert File.read!(source) == original
            assert "live" == GenServer.call(pid, {:get, "catalog_scan_live"})
            refute File.exists?(Path.join(Path.dirname(source), "compact_0.log"))
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "resolves cold records with exact batched catalog reads" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            state = :sys.get_state(pid)
            source = Path.join(state.shard_data_path, "00000.log")
            lmdb_path = LMDB.path(state.shard_data_path)
            key1 = "flow:state:catalog-page-1"
            key2 = "flow:state:catalog-page-2"
            value1 = "cold-state-one"
            value2 = "cold-state-two"

            assert {:ok, [{offset1, _record_size1}, {offset2, _record_size2}]} =
                     NIF.v2_append_batch(source, [{key1, value1, 0}, {key2, value2, 0}])

            locator1 = %Locator{
              flow_id: "catalog-page-1",
              kind: :state,
              version: 1,
              raft_index: 1,
              file_id: 0,
              offset: offset1,
              value_size: byte_size(value1)
            }

            locator2 = %Locator{
              flow_id: "catalog-page-2",
              kind: :state,
              version: 1,
              raft_index: 2,
              file_id: 0,
              offset: offset2,
              value_size: byte_size(value2)
            }

            park_key1 = LMDB.cold_park_key_for_state_key(key1)
            park_key2 = LMDB.cold_park_key_for_state_key(key2)
            reverse_key1 = LMDB.cold_by_segment_key(locator1)
            reverse_key2 = LMDB.cold_by_segment_key(locator2)

            assert :ok =
                     LMDB.write_batch(lmdb_path, [
                       {:put, park_key1,
                        LMDB.encode_cold_park(locator1,
                          state_key: key1,
                          due_at_ms: 900_000
                        )},
                       {:put, park_key2,
                        LMDB.encode_cold_park(locator2,
                          state_key: key2,
                          due_at_ms: 900_000
                        )},
                       {:put, reverse_key1, park_key1},
                       {:put, reverse_key2, park_key2}
                     ])

            parent = self()

            :sys.replace_state(pid, fn shard_state ->
              Map.put(
                shard_state,
                :compaction_cold_get_many_fun,
                fn ^lmdb_path, keys ->
                  send(parent, {:catalog_batch, keys})
                  LMDB.get_many(lmdb_path, keys)
                end
              )
            end)

            assert :ok = force_rotate_active_file(pid)
            assert {:ok, {2, 0, _reclaimed}} = GenServer.call(pid, {:run_compaction, [0]})

            assert_receive {:catalog_batch, [^reverse_key1, ^reverse_key2]}
            assert_receive {:catalog_batch, [^park_key1, ^park_key2]}

            assert {:ok, encoded1} = LMDB.get(lmdb_path, park_key1)
            assert {:ok, encoded2} = LMDB.get(lmdb_path, park_key2)
            assert {:ok, %{locator: relocated1}} = LMDB.decode_cold_park(encoded1)
            assert {:ok, %{locator: relocated2}} = LMDB.decode_cold_park(encoded2)
            assert {:ok, ^value1} = NIF.v2_pread_at(source, relocated1.offset)
            assert {:ok, ^value2} = NIF.v2_pread_at(source, relocated2.offset)
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        @tag :compaction_publication
        test "cold reads remain available while compacted offsets are being published" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            dead_key = "compaction-publication-dead"
            key = "compaction-publication-live"
            value = "live-value-through-publication"

            assert :ok = GenServer.call(pid, {:put, dead_key, "dead", 0})
            assert :ok = GenServer.call(pid, {:put, key, value, 0})
            assert :ok = GenServer.call(pid, :flush)
            assert :ok = GenServer.call(pid, {:delete, dead_key})
            assert :ok = GenServer.call(pid, :flush)
            assert :ok = force_rotate_active_file(pid)

            keydir = :sys.get_state(pid).keydir

            [{^key, ^value, expire_at_ms, lfu, 0, old_offset, value_size}] =
              :ets.lookup(keydir, key)

            true =
              :ets.insert(
                keydir,
                {key, nil, expire_at_ms, lfu, 0, old_offset, value_size}
              )

            parent = self()

            :sys.replace_state(pid, fn shard_state ->
              Map.put(shard_state, :compaction_cold_write_fun, fn path, ops ->
                send(parent, {:compaction_publication_open, self()})

                receive do
                  :finish_compaction_publication -> LMDB.write_batch(path, ops)
                end
              end)
            end)

            task = Task.async(fn -> GenServer.call(pid, {:run_compaction, [0]}, 10_000) end)
            assert_receive {:compaction_publication_open, worker}, 5_000

            source = Path.join(:sys.get_state(pid).shard_data_path, "00000.log")
            backup = Path.join(:sys.get_state(pid).shard_data_path, "compaction_backup_0.log")
            assert File.regular?(backup)

            assert {:ok, ^value} =
                     Ferricstore.Store.ColdRead.pread_keyed(source, old_offset, key, 5_000)

            assert {:cold_ref, ref_path, value_offset, ^value_size} =
                     Router.get_with_file_ref(ctx, key)

            assert ref_path == source
            assert binary_part(File.read!(ref_path), value_offset, value_size) == value
            assert Router.getrange(ctx, key, 0, -1) == value
            assert Router.get(ctx, key) == value

            send(worker, :finish_compaction_publication)
            assert {:ok, {1, 0, _reclaimed}} = Task.await(task, 10_000)
            assert Router.get(ctx, key) == value
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "restores the original segment when cold locator publication fails" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            state = :sys.get_state(pid)
            source = Path.join(state.shard_data_path, "00000.log")
            lmdb_path = LMDB.path(state.shard_data_path)
            state_key = "flow:state:locator-publish-failure"
            state_value = "cold-state-survives"

            assert {:ok, [_dead, {old_offset, _record_size}]} =
                     NIF.v2_append_batch(source, [
                       {"dead-before-cold", "dead", 0},
                       {state_key, state_value, 0}
                     ])

            locator = %Locator{
              flow_id: "locator-publish-failure",
              kind: :state,
              version: 1,
              raft_index: 1,
              file_id: 0,
              offset: old_offset,
              value_size: byte_size(state_value)
            }

            park_key = LMDB.cold_park_key_for_state_key(state_key)
            reverse_key = LMDB.cold_by_segment_key(locator)

            encoded_park =
              LMDB.encode_cold_park(locator,
                state_key: state_key,
                due_at_ms: 900_000
              )

            assert :ok =
                     LMDB.write_batch(lmdb_path, [
                       {:put, park_key, encoded_park},
                       {:put, reverse_key, park_key}
                     ])

            original = File.read!(source)

            :sys.replace_state(pid, fn shard_state ->
              Map.put(shard_state, :compaction_cold_write_fun, fn _path, _ops ->
                {:error, :forced_locator_write_failure}
              end)
            end)

            assert :ok = force_rotate_active_file(pid)

            assert {:error,
                    {:compaction_failed,
                     [
                       {0, :compaction_publication_failed, :forced_locator_write_failure}
                     ]}} = GenServer.call(pid, {:run_compaction, [0]})

            assert File.read!(source) == original
            assert {:ok, ^state_value} = NIF.v2_pread_at(source, old_offset)
            assert {:ok, ^encoded_park} = LMDB.get(lmdb_path, park_key)
            assert {:ok, ^park_key} = LMDB.get(lmdb_path, reverse_key)
            refute File.exists?(Path.join(Path.dirname(source), "compaction_backup_0.log"))
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "emits telemetry when raw-copy compaction hits a CRC mismatch" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
          source = Path.join(shard_path, "00000.log")
          parent = self()
          handler_id = "compaction-crc-mismatch-#{:erlang.unique_integer([:positive])}"

          :telemetry.attach(
            handler_id,
            [:ferricstore, :bitcask, :compaction_crc_mismatch],
            fn _event, measurements, metadata, _config ->
              send(parent, {:compaction_crc_mismatch, measurements, metadata})
            end,
            nil
          )

          try do
            key = "crc_raw_copy_live"
            value = "value-before-corruption"

            assert :ok = GenServer.call(pid, {:put, key, value, 0})
            assert :ok = GenServer.call(pid, :flush)

            keydir = :sys.get_state(pid).keydir
            [{^key, _cached_value, 0, _lfu, 0, offset, value_size}] = :ets.lookup(keydir, key)

            assert :ok = force_rotate_active_file(pid)

            value_pos = offset + @header_size + byte_size(key)
            {:ok, file} = :file.open(source, [:read, :write, :binary])

            try do
              assert {:ok, <<byte>>} = :file.pread(file, value_pos, 1)
              assert :ok = :file.pwrite(file, value_pos, <<Bitwise.bxor(byte, 0xFF)>>)
            after
              :ok = :file.close(file)
            end

            assert {:error,
                    {:compaction_failed,
                     [
                       {0, {:compaction_plan_failed, {:source_scan_failed, reason}}}
                     ]}} =
                     GenServer.call(pid, {:run_compaction, [0]})

            assert value_size == byte_size(value)
            assert inspect(reason) =~ "CRC mismatch"

            assert_receive {:compaction_crc_mismatch, %{count: 1},
                            %{shard_index: 0, file_id: 0, path: ^source, reason: reason_text}},
                           500

            assert reason_text =~ "CRC mismatch"
          after
            :telemetry.detach(handler_id)
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "streams compaction records without a keydir-wide planning pass" do
          source = shard_source()

          assert source =~ "NIF.v2_scan_file_page"
          assert source =~ "CompactionPlan.append"
          refute source =~ "group_compaction_live_entries"
          refute source =~ ":ets.foldl("
        end

        test "ignores promoted dedicated entries when compacting shared log files" do
          {pid, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)

          try do
            assert :ok = GenServer.call(pid, {:put, "shared_live", "shared-value", 0})
            assert :ok = GenServer.call(pid, :flush)
            assert :ok = force_rotate_active_file(pid)

            promoted_key = "promoted_shared_compaction"
            field_key = CompoundKey.hash_field(promoted_key, "field")

            :sys.replace_state(pid, fn state ->
              dedicated_path = Path.join([state.shard_data_path, "promoted", promoted_key])

              :ets.insert(
                state.keydir,
                {field_key, "dedicated-value", 0, LFU.initial(), 0, 0,
                 byte_size("dedicated-value")}
              )

              %{
                state
                | promoted_instances:
                    Map.put(state.promoted_instances, promoted_key, %{
                      path: dedicated_path,
                      type: :hash,
                      total_bytes: 0,
                      dead_bytes: 0,
                      writes_since_compaction: 0,
                      last_compaction_ms: 0
                    })
              }
            end)

            assert {:ok, {copied, 0, _reclaimed}} =
                     GenServer.call(pid, {:run_compaction, [0]})

            assert copied >= 1

            assert [{^field_key, "dedicated-value", 0, _lfu, 0, 0, _vsize}] =
                     :ets.lookup(:sys.get_state(pid).keydir, field_key)
          after
            cleanup_shard(pid, ctx, dir)
          end
        end

        test "removes stale hint when compacting a rewritten shared log file" do
          {pid1, _index, dir, ctx} = start_shard(flush_interval_ms: 5000)
          a = "compact_hint_a_#{:erlang.unique_integer([:positive])}"
          b = "compact_hint_b_#{:erlang.unique_integer([:positive])}"

          on_exit(fn ->
            case Process.whereis(elem(ctx.shard_names, 0)) do
              pid when is_pid(pid) ->
                cleanup_shard(pid, ctx, dir)

              _ ->
                FerricStore.Instance.cleanup(ctx.name)
                File.rm_rf(dir)
            end
          end)

          :ok = GenServer.call(pid1, {:put, a, "old-a", 0})
          :ok = GenServer.call(pid1, {:put, b, "live-b", 0})
          :ok = GenServer.call(pid1, :flush)

          state = :sys.get_state(pid1)
          assert :ok = ShardFlush.write_hint_for_file(state, 0)

          hint_path = Path.join([dir, "data", "shard_0", "00000.hint"])
          assert File.exists?(hint_path)
          alternate_hint_path = Path.join([dir, "data", "shard_0", "00000000000000000000.hint"])
          File.cp!(hint_path, alternate_hint_path)
          File.rm!(hint_path)
          assert File.exists?(alternate_hint_path)

          :ok = GenServer.call(pid1, {:delete, a})
          :ok = GenServer.call(pid1, :flush)
          assert nil == GenServer.call(pid1, {:get, a})
          assert "live-b" == GenServer.call(pid1, {:get, b})

          :ok = force_rotate_active_file(pid1)

          assert {:ok, {copied, 0, _reclaimed}} =
                   GenServer.call(pid1, {:run_compaction, [0]})

          assert copied >= 1

          Process.unlink(pid1)
          ref = Process.monitor(pid1)
          Process.exit(pid1, :kill)
          assert_receive {:DOWN, ^ref, :process, ^pid1, :killed}, 2_000

          pid2 = restart_shard(dir, ctx, 5000)

          assert nil == GenServer.call(pid2, {:get, a})
          assert "live-b" == GenServer.call(pid2, {:get, b})
        end

        test "does not drop tombstone-only files while older values can resurrect" do
          previous_trap_exit = Process.flag(:trap_exit, true)
          dir = Path.join(System.tmp_dir!(), "tombstone_compaction_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)

          name = :"tombstone_compaction_#{:erlang.unique_integer([:positive])}"

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

            assert {:error, {:no_compactable_files, [1]}} =
                     GenServer.call(pid1, {:run_compaction, [1]})

            assert File.exists?(log1)

            :ok = GenServer.stop(pid1, :normal, 5_000)

            pid2 = restart_shard(dir, ctx, 5000)
            assert nil == GenServer.call(pid2, {:get, "a"})
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

        test "drops tombstone-only files after older files are compacted away" do
          previous_trap_exit = Process.flag(:trap_exit, true)
          dir = Path.join(System.tmp_dir!(), "safe_tombstone_drop_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)

          name = :"safe_tombstone_drop_#{:erlang.unique_integer([:positive])}"

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

            assert {:ok, {0, 0, reclaimed0}} = GenServer.call(pid1, {:run_compaction, [0]})
            assert reclaimed0 > 0
            refute File.exists?(log0)

            assert {:ok, {0, 0, reclaimed1}} = GenServer.call(pid1, {:run_compaction, [1]})
            assert reclaimed1 > 0
            refute File.exists?(log1)

            :ok = GenServer.stop(pid1, :normal, 5_000)

            pid2 = restart_shard(dir, ctx, 5000)
            assert nil == GenServer.call(pid2, {:get, "a"})
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

        test "drops tombstone-only files when older files do not contain masked keys" do
          previous_trap_exit = Process.flag(:trap_exit, true)

          dir =
            Path.join(System.tmp_dir!(), "unneeded_tombstone_drop_#{:rand.uniform(9_999_999)}")

          File.mkdir_p!(dir)

          name = :"unneeded_tombstone_drop_#{:erlang.unique_integer([:positive])}"

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

            {:ok, [_]} = NIF.v2_append_batch(log0, [{"b", "live", 0}])
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

            assert {:ok, {0, 0, reclaimed1}} = GenServer.call(pid1, {:run_compaction, [1]})
            assert reclaimed1 > 0
            refute File.exists?(log1)

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

        test "drops tombstone-only files when lower masked values are expired" do
          previous_trap_exit = Process.flag(:trap_exit, true)
          dir = Path.join(System.tmp_dir!(), "expired_tombstone_drop_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)

          name = :"expired_tombstone_drop_#{:erlang.unique_integer([:positive])}"

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
            expired_at = System.os_time(:millisecond) - 1_000

            {:ok, [_]} = NIF.v2_append_batch(log0, [{"a", "expired", expired_at}])
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

            assert {:ok, {0, 0, reclaimed1}} = GenServer.call(pid1, {:run_compaction, [1]})
            assert reclaimed1 > 0
            refute File.exists?(log1)

            :ok = GenServer.stop(pid1, :normal, 5_000)

            pid2 = restart_shard(dir, ctx, 5000)
            assert nil == GenServer.call(pid2, {:get, "a"})
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

        test "drops obsolete tombstones from mixed live files during compaction" do
          previous_trap_exit = Process.flag(:trap_exit, true)
          dir = Path.join(System.tmp_dir!(), "mixed_tombstone_drop_#{:rand.uniform(9_999_999)}")
          File.mkdir_p!(dir)

          name = :"mixed_tombstone_drop_#{:erlang.unique_integer([:positive])}"

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

            {:ok, [_]} = NIF.v2_append_batch(log0, [{"unrelated", "old", 0}])
            {:ok, _} = NIF.v2_append_tombstone(log1, "obsolete-delete")
            {:ok, [_]} = NIF.v2_append_batch(log1, [{"live", "kept", 0}])
            File.touch!(log2)

            assert {:ok, [_]} = NIF.v2_scan_tombstones(log1)

            {:ok, pid1} =
              Shard.start_link(
                index: 0,
                data_dir: dir,
                flush_interval_ms: 5000,
                instance_ctx: ctx
              )

            assert "kept" == GenServer.call(pid1, {:get, "live"})

            assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid1, {:run_compaction, [1]})

            assert {:ok, []} = NIF.v2_scan_tombstones(log1)
            assert "kept" == GenServer.call(pid1, {:get, "live"})
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

        test "drops mixed-file tombstones after newest lower tombstone without scanning older files" do
          previous_trap_exit = Process.flag(:trap_exit, true)

          dir =
            Path.join(System.tmp_dir!(), "mixed_tombstone_desc_scan_#{:rand.uniform(9_999_999)}")

          File.mkdir_p!(dir)

          name = :"mixed_tombstone_desc_scan_#{:erlang.unique_integer([:positive])}"

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
            log3 = Path.join(shard_dir, "00003.log")

            {:ok, [_]} = NIF.v2_append_batch(log0, [{"deleted", "old", 0}])
            {:ok, _} = NIF.v2_append_tombstone(log1, "deleted")
            {:ok, _} = NIF.v2_append_tombstone(log2, "deleted")
            {:ok, [_]} = NIF.v2_append_batch(log2, [{"live", "kept", 0}])
            File.touch!(log3)

            {:ok, pid1} =
              Shard.start_link(
                index: 0,
                data_dir: dir,
                flush_interval_ms: 5000,
                instance_ctx: ctx
              )

            assert nil == GenServer.call(pid1, {:get, "deleted"})
            assert "kept" == GenServer.call(pid1, {:get, "live"})

            parent = self()
            handler_id = {:tombstone_dependency_scan, self(), make_ref()}

            :telemetry.attach(
              handler_id,
              [:ferricstore, :bitcask, :tombstone_dependency_scan],
              fn event, measurements, metadata, _config ->
                send(parent, {:tombstone_dependency_scan, event, measurements, metadata})
              end,
              nil
            )

            on_exit(fn -> :telemetry.detach(handler_id) end)

            File.rm!(log0)
            File.mkdir!(log0)

            assert {:ok, {1, 0, _reclaimed}} = GenServer.call(pid1, {:run_compaction, [2]})
            assert {:ok, []} = NIF.v2_scan_tombstones(log2)
            assert "kept" == GenServer.call(pid1, {:get, "live"})

            assert_receive {:tombstone_dependency_scan,
                            [:ferricstore, :bitcask, :tombstone_dependency_scan],
                            %{
                              candidate_files: 2,
                              files_scanned: 1,
                              masked_keys: 1,
                              resolved_keys: 1
                            }, %{fid: 2, status: :ok}}
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
