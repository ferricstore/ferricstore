defmodule Ferricstore.Raft.WARaftSegmentLogTest.Sections.SegmentLogCapsEtsTailWhileDiskBackedReadsStillSeeOlderEntrie do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      test "segment log caps ETS tail while disk-backed reads still see older entries" do
        with_segment_log_memory_env(
          max_bytes: 1_000,
          max_entries: 1,
          min_entries: 1,
          records_per_segment: 64,
          fun: fn _root, log, log_name ->
            assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

            view0 = {:log_view, log, 0, 0, :undefined}
            payload = :binary.copy("x", 2_048)

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view0,
                       [
                         {1, {:cmd, payload <> "1"}},
                         {1, {:cmd, payload <> "2"}},
                         {1, {:cmd, payload <> "3"}}
                       ],
                       :strict,
                       :low
                     )

            assert :ets.info(log_name, :size) == 1

            assert {:ok, {1, {:cmd, ^payload <> "1"}}} =
                     :ferricstore_waraft_spike_segment_log.get(log, 1)

            assert {:ok, {1, {:cmd, ^payload <> "2"}}} =
                     :ferricstore_waraft_spike_segment_log.get(log, 2)

            assert {:ok, {1, {:cmd, ^payload <> "3"}}} =
                     :ferricstore_waraft_spike_segment_log.get(log, 3)

            assert {:ok, [{1, _}, {2, _}, {3, _}]} =
                     :ferricstore_waraft_spike_segment_log.fold(
                       log,
                       1,
                       3,
                       :infinity,
                       fn index, _size, _entry, acc -> [{index, :seen} | acc] end,
                       []
                     )
                     |> map_fold_seen()

            assert %{ets_entries: 1, disk_first_index: 1, disk_last_index: 3, dir: segment_dir} =
                     :ferricstore_waraft_spike_segment_log.memory_status(log)

            assert {:ok, {1, {:cmd, ^payload <> "2"}}} =
                     :ferricstore_waraft_spike_segment_log.read_disk(
                       segment_dir |> to_string() |> Path.dirname() |> to_charlist(),
                       2
                     )
          end
        )
      end

      test "segment log keeps latest config cached after ETS tail demotion" do
        with_segment_log_memory_env(
          max_bytes: 1_000,
          max_entries: 1,
          min_entries: 1,
          records_per_segment: 64,
          fun: fn _root, log, log_name ->
            assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

            view0 = {:log_view, log, 0, 0, :undefined}
            config = %{version: 1, membership: [node()], participants: [node()], witness: []}
            payload = :binary.copy("x", 2_048)

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view0,
                       [
                         {1, {make_ref(), {:config, config}}},
                         {1, {:cmd, payload <> "2"}},
                         {1, {:cmd, payload <> "3"}}
                       ],
                       :strict,
                       :low
                     )

            assert :ets.info(log_name, :size) == 1
            assert :ets.lookup(log_name, 1) == []
            assert {:ok, 1, ^config} = :ferricstore_waraft_spike_segment_log.config(log)

            :ets.delete_all_objects(log_name)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)
            assert {:ok, 1, ^config} = :ferricstore_waraft_spike_segment_log.config(log)
          end
        )
      end

      test "segment log reopen loads only bounded tail into ETS" do
        parent = self()
        handler_id = {:segment_log_bounded_reopen, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :segment_log, :load],
          &__MODULE__.handle_load_telemetry/4,
          parent
        )

        try do
          clear_segment_offset_registry()

          with_segment_log_memory_env(
            max_bytes: 4_096,
            max_entries: 2,
            min_entries: 1,
            records_per_segment: 64,
            fun: fn _root, log, log_name ->
              assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
              assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

              view0 = {:log_view, log, 0, 0, :undefined}
              payload = :binary.copy("z", 2_048)

              assert :ok =
                       :ferricstore_waraft_spike_segment_log.append(
                         view0,
                         for(i <- 1..6, do: {1, {:cmd, payload <> Integer.to_string(i)}}),
                         :strict,
                         :low
                       )

              assert :ets.info(log_name, :size) == 1

              assert %{dir: segment_dir} =
                       :ferricstore_waraft_spike_segment_log.memory_status(log)

              root_dir = segment_dir |> to_string() |> Path.dirname()

              expected_scan_payload_bytes =
                1..6
                |> Enum.map(fn index ->
                  assert {:ok, {_ordinal, _offset, encoded_size}} =
                           :ferricstore_waraft_spike_segment_log.location_for_index(
                             to_charlist(root_dir),
                             index
                           )

                  encoded_size - 8
                end)
                |> Enum.sum()

              :ets.delete_all_objects(log_name)
              assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

              assert_receive {:segment_log_load, [:ferricstore, :waraft, :segment_log, :load],
                              %{
                                disk_records: 6,
                                decoded_records: decoded_records,
                                ets_entries: ets_entries,
                                scan_payload_bytes: scan_payload_bytes
                              }, %{dir: _dir}},
                             500

              assert ets_entries <= 2
              assert decoded_records <= ets_entries + 1
              # Reopen keeps only a bounded tail in ETS, but it still reads demoted
              # payloads to validate CRCs. Telemetry must expose that real IO cost.
              assert scan_payload_bytes == expected_scan_payload_bytes
              assert :ets.info(log_name, :size) <= 2

              assert :ets.info(:ferricstore_waraft_segment_offset_registry, :size) <= 3

              assert {:ok, {1, {:cmd, ^payload <> "1"}}} =
                       :ferricstore_waraft_spike_segment_log.get(log, 1)

              assert {:ok, {1, {:cmd, ^payload <> "6"}}} =
                       :ferricstore_waraft_spike_segment_log.get(log, 6)

              assert {:ok, {_ordinal, offset, encoded_size}} =
                       :ferricstore_waraft_spike_segment_log.location_for_index(
                         to_charlist(root_dir),
                         1
                       )

              assert {:ok, {1, {:cmd, ^payload <> "1"}}} =
                       :ferricstore_waraft_spike_segment_log.read_disk_at(
                         to_charlist(root_dir),
                         1,
                         offset,
                         encoded_size
                       )
            end
          )
        after
          :telemetry.detach(handler_id)
        end
      end

      test "bounded raft reopen validates CRC for demoted records" do
        with_segment_log_memory_env(
          max_bytes: 1_000,
          max_entries: 1,
          min_entries: 1,
          records_per_segment: 64,
          fun: fn _root, log, log_name ->
            assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

            view0 = {:log_view, log, 0, 0, :undefined}
            payload = :binary.copy("c", 256)

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view0,
                       for(i <- 1..6, do: {1, {:cmd, payload <> Integer.to_string(i)}}),
                       :strict,
                       :low
                     )

            assert %{dir: segment_dir} = :ferricstore_waraft_spike_segment_log.memory_status(log)
            root_dir = segment_dir |> to_string() |> Path.dirname()

            assert {:ok, {_ordinal, offset, _encoded_size}} =
                     :ferricstore_waraft_spike_segment_log.location_for_index(
                       to_charlist(root_dir),
                       2
                     )

            segment_path = Path.join(segment_dir, "0.seg")

            assert {:ok, fd} =
                     :file.open(to_charlist(segment_path), [:read, :write, :raw, :binary])

            assert :ok = :file.pwrite(fd, offset + 8, <<"X">>)
            assert :ok = :file.close(fd)

            :ets.delete_all_objects(log_name)
            clear_segment_offset_registry()

            assert {:error, {:crc_mismatch, ^offset}} =
                     :ferricstore_waraft_spike_segment_log.open(log)
          end
        )
      end

      test "bounded raft reopen fails closed on oversized non-first record length" do
        with_segment_log_memory_env(
          max_bytes: 1_000,
          max_entries: 1,
          min_entries: 1,
          records_per_segment: 64,
          fun: fn _root, log, log_name ->
            assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

            view0 = {:log_view, log, 0, 0, :undefined}
            payload = :binary.copy("o", 256)

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view0,
                       for(i <- 1..3, do: {1, {:cmd, payload <> Integer.to_string(i)}}),
                       :strict,
                       :low
                     )

            assert %{dir: segment_dir} = :ferricstore_waraft_spike_segment_log.memory_status(log)
            root_dir = segment_dir |> to_string() |> Path.dirname()

            assert {:ok, {_ordinal, offset, _encoded_size}} =
                     :ferricstore_waraft_spike_segment_log.location_for_index(
                       to_charlist(root_dir),
                       2
                     )

            segment_path = Path.join(segment_dir, "0.seg")

            assert {:ok, fd} =
                     :file.open(to_charlist(segment_path), [:read, :write, :raw, :binary])

            too_large = 1_073_741_825

            assert :ok =
                     :file.pwrite(fd, offset, <<too_large::32-unsigned-big, 0::32-unsigned-big>>)

            assert :ok = :file.close(fd)

            :ets.delete_all_objects(log_name)
            clear_segment_offset_registry()

            assert {:error, {:record_too_large, ^offset, ^too_large}} =
                     :ferricstore_waraft_spike_segment_log.open(log)
          end
        )
      end

      test "fold_disk streams records instead of loading them into a temp ETS table" do
        parent = self()
        handler_id = {:segment_log_streaming_fold, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :segment_log, :fold_disk],
          &__MODULE__.handle_fold_telemetry/4,
          parent
        )

        try do
          with_segment_log_memory_env(
            max_bytes: 4_096,
            max_entries: 2,
            min_entries: 1,
            records_per_segment: 64,
            fun: fn _root, log, _log_name ->
              assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
              assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

              view0 = {:log_view, log, 0, 0, :undefined}
              payload = :binary.copy("f", 2_048)

              assert :ok =
                       :ferricstore_waraft_spike_segment_log.append(
                         view0,
                         for(i <- 1..6, do: {1, {:cmd, payload <> Integer.to_string(i)}}),
                         :strict,
                         :low
                       )

              assert %{dir: segment_dir} =
                       :ferricstore_waraft_spike_segment_log.memory_status(log)

              log_root = segment_dir |> to_string() |> Path.dirname()

              assert {:ok, [1, 2, 3, 4, 5, 6]} =
                       :ferricstore_waraft_spike_segment_log.fold_disk(
                         to_charlist(log_root),
                         fn index, _entry, acc -> [index | acc] end,
                         []
                       )
                       |> map_fold_seen()

              assert_receive {:segment_log_fold,
                              [:ferricstore, :waraft, :segment_log, :fold_disk],
                              %{disk_records: 6}, %{dir: _dir}},
                             500
            end
          )
        after
          :telemetry.detach(handler_id)
        end
      end

      test "fold_disk decodes trusted Raft log entries with correlation references" do
        with_segment_log_memory_env(
          max_bytes: 4_096,
          max_entries: 2,
          min_entries: 1,
          records_per_segment: 64,
          fun: fn _root, log, _log_name ->
            assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

            view0 = {:log_view, log, 0, 0, :undefined}
            corr = make_ref()

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view0,
                       [{1, {:default, {corr, {:put, "ref-fold:k", "v1", 0}}}}],
                       :strict,
                       :low
                     )

            assert %{dir: segment_dir} = :ferricstore_waraft_spike_segment_log.memory_status(log)
            log_root = segment_dir |> to_string() |> Path.dirname()

            assert {:ok, [{1, {1, {:default, {^corr, {:put, "ref-fold:k", "v1", 0}}}}}]} =
                     :ferricstore_waraft_spike_segment_log.fold_disk(
                       to_charlist(log_root),
                       fn index, entry, acc -> [{index, entry} | acc] end,
                       []
                     )
                     |> map_fold_seen()
          end
        )
      end

      test "segment log uses adaptive memory budget when explicit caps are unset" do
        previous_memory_limit = Application.get_env(:ferricstore, :max_memory_bytes)
        previous_shard_count = Application.get_env(:ferricstore, :shard_count)

        try do
          Application.put_env(:ferricstore, :max_memory_bytes, 2 * 1024 * 1024 * 1024)
          Application.put_env(:ferricstore, :shard_count, 8)
          Ferricstore.MemoryBudget.reset_cache()

          with_segment_log_memory_env(
            max_bytes: nil,
            max_entries: nil,
            min_entries: nil,
            records_per_segment: 64,
            fun: fn _root, log, _log_name ->
              assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
              assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

              limits =
                Ferricstore.MemoryBudget.adaptive_limits(
                  Ferricstore.MemoryBudget.hardware_profile()
                )

              status = :ferricstore_waraft_spike_segment_log.memory_status(log)

              assert status.max_ets_bytes == limits.waraft_segment_log_max_ets_bytes
              assert status.max_ets_entries == limits.waraft_segment_log_max_ets_entries
              assert status.min_ets_entries == limits.waraft_segment_log_min_ets_entries
            end
          )
        after
          restore_env(:ferricstore, :max_memory_bytes, previous_memory_limit)
          restore_env(:ferricstore, :shard_count, previous_shard_count)
          Ferricstore.MemoryBudget.reset_cache()
        end
      end

      test "truncate preserves demoted disk-only records before the truncation point" do
        with_segment_log_memory_env(
          max_bytes: 1_000,
          max_entries: 1,
          min_entries: 1,
          records_per_segment: 64,
          fun: fn _root, log, log_name ->
            assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

            view0 = {:log_view, log, 0, 0, :undefined}
            payload = :binary.copy("y", 2_048)

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view0,
                       for(i <- 1..4, do: {1, {:cmd, payload <> Integer.to_string(i)}}),
                       :strict,
                       :low
                     )

            assert :ets.info(log_name, :size) == 1
            assert {:ok, _} = :ferricstore_waraft_spike_segment_log.get(log, 2)

            assert {:ok, %{}} = :ferricstore_waraft_spike_segment_log.truncate(log, 4, %{})

            segment_dir =
              log
              |> :ferricstore_waraft_spike_segment_log.memory_status()
              |> Map.fetch!(:dir)
              |> to_string()

            assert :ferricstore_waraft_spike_segment_log.read_disk(
                     segment_dir |> Path.dirname() |> to_charlist(),
                     4
                   ) == :not_found

            assert {:ok, {1, {:cmd, ^payload <> "1"}}} =
                     :ferricstore_waraft_spike_segment_log.get(log, 1)

            assert {:ok, {1, {:cmd, ^payload <> "2"}}} =
                     :ferricstore_waraft_spike_segment_log.get(log, 2)

            assert :ferricstore_waraft_spike_segment_log.last_index(log) == 3
          end
        )
      end

      test "trim advances logical floor without physical segment rewrite on hot path" do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_rewrite_hook)

        try do
          with_segment_log_memory_env(
            max_bytes: 1_000_000,
            max_entries: 1_000,
            min_entries: 1,
            records_per_segment: 64,
            fun: fn _root, log, _log_name ->
              assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
              assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

              view0 = {:log_view, log, 0, 0, :undefined}

              assert :ok =
                       :ferricstore_waraft_spike_segment_log.append(
                         view0,
                         for(i <- 1..5, do: {1, {:cmd, i}}),
                         :strict,
                         :low
                       )

              Application.put_env(
                :ferricstore,
                :waraft_segment_log_rewrite_hook,
                {:fail_once_after_live_backup, self()}
              )

              assert {:ok, %{}} = :ferricstore_waraft_spike_segment_log.trim(log, 3, %{})
              refute_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}, 100

              assert :ferricstore_waraft_spike_segment_log.first_index(log) == 3
              assert :ferricstore_waraft_spike_segment_log.get(log, 1) == :not_found
              assert :ferricstore_waraft_spike_segment_log.get(log, 2) == :not_found
              assert {:ok, {1, {:cmd, 3}}} = :ferricstore_waraft_spike_segment_log.get(log, 3)
            end
          )
        after
          restore_env(:ferricstore, :waraft_segment_log_rewrite_hook, previous_hook)
        end
      end

      test "trim floor survives reopen without physically rewriting old segment files" do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_rewrite_hook)

        try do
          with_segment_log_memory_env(
            max_bytes: 1_000_000,
            max_entries: 1_000,
            min_entries: 1,
            records_per_segment: 64,
            fun: fn _root, log, log_name ->
              assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
              assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

              view0 = {:log_view, log, 0, 0, :undefined}

              assert :ok =
                       :ferricstore_waraft_spike_segment_log.append(
                         view0,
                         for(i <- 1..5, do: {1, {:cmd, i}}),
                         :strict,
                         :low
                       )

              %{dir: segment_dir} = :ferricstore_waraft_spike_segment_log.memory_status(log)
              root_dir = segment_dir |> to_string() |> Path.dirname()
              segment_path = Path.join(segment_dir |> to_string(), "0.seg")
              size_before_trim = File.stat!(segment_path).size

              Application.put_env(
                :ferricstore,
                :waraft_segment_log_rewrite_hook,
                {:fail_once_after_live_backup, self()}
              )

              assert {:ok, %{}} = :ferricstore_waraft_spike_segment_log.trim(log, 3, %{})
              refute_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}, 100
              assert File.stat!(segment_path).size == size_before_trim

              assert :ok = :ferricstore_waraft_spike_segment_log.close(log, %{})
              :ets.delete_all_objects(log_name)
              clear_segment_offset_registry()

              assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

              assert :ferricstore_waraft_spike_segment_log.first_index(log) == 3
              assert :ferricstore_waraft_spike_segment_log.get(log, 1) == :not_found
              assert :ferricstore_waraft_spike_segment_log.get(log, 2) == :not_found
              assert {:ok, {1, {:cmd, 3}}} = :ferricstore_waraft_spike_segment_log.get(log, 3)

              assert :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root_dir), 1) ==
                       :not_found
            end
          )
        after
          restore_env(:ferricstore, :waraft_segment_log_rewrite_hook, previous_hook)
        end
      end

      test "bounded raft reopen truncates a torn tail before future appends" do
        with_segment_log_memory_env(
          max_bytes: 1_000,
          max_entries: 1,
          min_entries: 1,
          records_per_segment: 64,
          fun: fn _root, log, log_name ->
            assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

            view0 = {:log_view, log, 0, 0, :undefined}

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view0,
                       for(i <- 1..3, do: {1, {:cmd, "v#{i}"}}),
                       :strict,
                       :low
                     )

            %{dir: segment_dir} = :ferricstore_waraft_spike_segment_log.memory_status(log)
            root_dir = segment_dir |> to_string() |> Path.dirname()
            segment_path = Path.join(segment_dir |> to_string(), "0.seg")
            original_size = File.stat!(segment_path).size

            assert :ok = :ferricstore_waraft_spike_segment_log.close(log, %{})
            File.write!(segment_path, <<0, 0, 0, 4>>, [:append])
            assert File.stat!(segment_path).size == original_size + 4

            :ets.delete_all_objects(log_name)
            clear_segment_offset_registry()

            assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)
            assert File.stat!(segment_path).size == original_size

            view3 = {:log_view, log, 0, 3, :undefined}

            assert :ok =
                     :ferricstore_waraft_spike_segment_log.append(
                       view3,
                       [{1, {:cmd, "v4"}}],
                       :strict,
                       :low
                     )

            assert {:ok, {1, {:cmd, "v4"}}} =
                     :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root_dir), 4)
          end
        )
      end

      test "projection writer persists projected keydir entries as segment-log records" do
        root =
          Path.join(
            System.tmp_dir!(),
            "ferricstore-waraft-segment-log-#{System.unique_integer([:positive])}"
          )

        File.rm_rf!(root)
        on_exit(fn -> File.rm_rf!(root) end)

        position = {:raft_log_pos, 42, 7}
        entries = [{"a", "1", 0}, {"b", "2", 123}]

        assert :ok =
                 :ferricstore_waraft_spike_segment_log.write_projection(
                   to_charlist(root),
                   position,
                   entries
                 )

        assert File.dir?(Path.join([root, "segment_log"]))
        refute File.exists?(Path.join(root, "segment_projected_keydir.term"))

        assert {:ok, records} =
                 :ferricstore_waraft_spike_segment_log.fold_disk(
                   to_charlist(root),
                   fn index, entry, acc -> [{index, entry} | acc] end,
                   []
                 )

        assert Enum.reverse(records) == [
                 {0, {0, {:ferricstore_segment_projection_header, position, 2}}},
                 {1, {0, {:ferricstore_segment_projection_entry, "a", "1", 0}}},
                 {2, {0, {:ferricstore_segment_projection_entry, "b", "2", 123}}}
               ]
      end

      test "projection offset registry survives shutdown-time ETS deletion" do
        root =
          Path.join(
            System.tmp_dir!(),
            "ferricstore-waraft-segment-log-registry-race-#{System.unique_integer([:positive])}"
          )

        previous_hook =
          Application.get_env(:ferricstore, :waraft_segment_log_offset_registry_hook)

        File.rm_rf!(root)

        try do
          Application.put_env(
            :ferricstore,
            :waraft_segment_log_offset_registry_hook,
            {:delete_once, :before_last_lookup, self()}
          )

          assert :ok =
                   :ferricstore_waraft_spike_segment_log.write_projection(
                     to_charlist(root),
                     {:raft_log_pos, 42, 7},
                     [{"a", "1", 0}, {"b", "2", 0}]
                   )

          assert_receive {:waraft_segment_log_offset_registry_hook, :before_last_lookup}, 1_000

          assert {:ok, {_ordinal, _offset, _encoded_size}} =
                   :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), 2)
        after
          restore_env(:ferricstore, :waraft_segment_log_offset_registry_hook, previous_hook)
          File.rm_rf!(root)
        end
      end

      test "projection writer survives shutdown-time writer registry deletion after acquire ensure" do
        root =
          Path.join(
            System.tmp_dir!(),
            "ferricstore-waraft-segment-log-writer-race-#{System.unique_integer([:positive])}"
          )

        previous_hook =
          Application.get_env(:ferricstore, :waraft_segment_log_writer_registry_hook)

        registry = :ferricstore_waraft_segment_writer_registry

        File.rm_rf!(root)

        if :ets.info(registry) != :undefined do
          :ets.delete(registry)
        end

        try do
          Application.put_env(
            :ferricstore,
            :waraft_segment_log_writer_registry_hook,
            {:delete_once, :after_acquire_ensure, self()}
          )

          assert :ok =
                   :ferricstore_waraft_spike_segment_log.write_projection(
                     to_charlist(root),
                     {:raft_log_pos, 42, 7},
                     [{"a", "1", 0}, {"b", "2", 0}]
                   )

          assert_receive {:waraft_segment_log_writer_registry_hook, :after_acquire_ensure}, 1_000

          assert {:ok, {0, {:ferricstore_segment_projection_header, {:raft_log_pos, 42, 7}, 2}}} =
                   :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root), 0)
        after
          restore_env(:ferricstore, :waraft_segment_log_writer_registry_hook, previous_hook)

          if :ets.info(registry) != :undefined do
            :ets.delete(registry)
          end

          File.rm_rf!(root)
        end
      end

      test "segment append telemetry classifies log kind for byte accounting" do
        parent = self()
        handler_id = {__MODULE__, :segment_append_kind, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :segment_log, :append],
          &__MODULE__.handle_append_telemetry/4,
          parent
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        root =
          Path.join(
            System.tmp_dir!(),
            "ferricstore-waraft-segment-kind-#{System.unique_integer([:positive])}"
          )

        File.rm_rf!(root)
        on_exit(fn -> File.rm_rf!(root) end)

        assert :ok =
                 :ferricstore_waraft_spike_segment_log.write_projection(
                   to_charlist(Path.join(root, "segment_projection_log")),
                   {:raft_log_pos, 1, 1},
                   [{"k", "v", 0}]
                 )

        assert_receive {:segment_log_append, [:ferricstore, :waraft, :segment_log, :append],
                        %{bytes: projection_bytes}, %{kind: :segment_projection, result: :ok}},
                       1_000

        assert projection_bytes > 0

        assert :ok =
                 :ferricstore_waraft_spike_segment_log.write_projection_batch(
                   to_charlist(Path.join(root, "apply_projection_log")),
                   {:raft_log_pos, 2, 1},
                   [{"k", "v", 0}]
                 )

        assert_receive {:segment_log_append, [:ferricstore, :waraft, :segment_log, :append],
                        %{bytes: apply_projection_bytes},
                        %{kind: :apply_projection, result: :ok}},
                       1_000

        assert apply_projection_bytes > 0
      end

      test "apply projection batch append does not fsync on the hot apply path" do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        root = Path.join(System.tmp_dir!(), "ferricstore-waraft-apply-projection-nosync")
        File.rm_rf!(root)

        try do
          Application.put_env(
            :ferricstore,
            :waraft_segment_log_file_sync_hook,
            {:fail_once, self()}
          )

          assert :ok =
                   :ferricstore_waraft_spike_segment_log.write_projection_batch(
                     to_charlist(root),
                     {:raft_log_pos, 42, 7},
                     [{"a", "1", 0}]
                   )

          refute_receive {:waraft_segment_log_file_sync, _path}, 100

          assert {:ok, {_ordinal, offset, encoded_size}} =
                   :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), 42)

          assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"a", "1", 0}]}}} =
                   :ferricstore_waraft_spike_segment_log.read_disk_at(
                     to_charlist(root),
                     42,
                     offset,
                     encoded_size
                   )
        after
          restore_env(:ferricstore, :waraft_segment_log_file_sync_hook, previous_hook)
          File.rm_rf!(root)
        end
      end
    end
  end
end
