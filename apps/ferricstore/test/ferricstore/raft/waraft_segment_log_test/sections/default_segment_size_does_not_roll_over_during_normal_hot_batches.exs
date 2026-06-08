defmodule Ferricstore.Raft.WARaftSegmentLogTest.Sections.DefaultSegmentSizeDoesNotRollOverDuringNormalHotBatches do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
  test "default segment size does not roll over during normal hot batches" do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-default-size-#{System.unique_integer([:positive])}"
      )

    try do
      Application.delete_env(:ferricstore, :waraft_segment_log_records_per_segment)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      entries =
        for i <- 1..4097 do
          {"k#{i}", "v#{i}", 0}
        end

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection(
                 to_charlist(root),
                 {:raft_log_pos, 10, 1},
                 entries
               )

      segment_dir = Path.join(root, "segment_log")
      assert File.exists?(Path.join(segment_dir, "0.seg"))
      refute File.exists?(Path.join(segment_dir, "1.seg"))
    after
      if previous == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_records_per_segment)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, previous)
      end
    end
  end

  test "segment appends use the single direct file writer path" do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    registry = :ferricstore_waraft_segment_writer_registry
    partition = System.unique_integer([:positive])
    table = :ferricstore_waraft_segment_log_direct_writer_test
    log_name = :"#{table}_log_#{partition}"

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-direct-writer-#{partition}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    :ets.new(registry, [:named_table, :public, :set])

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(root))
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 65_536)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      :wa_raft_part_sup.prepare_spec(:ferricstore_waraft_backend, %{
        table: table,
        partition: partition
      })

      log =
        {:raft_log, log_name, :ferricstore_waraft_backend, table, partition,
         :ferricstore_waraft_spike_segment_log}

      assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
      assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

      view0 = {:log_view, log, 0, 0, :undefined}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 view0,
                 [{1, {:cmd, 1}}],
                 :strict,
                 :low
               )

      segment_path =
        Path.join([
          to_string(:wa_raft_part_sup.registered_partition_path(table, partition)),
          "segment_log",
          "0.seg"
        ])

      writer_key = to_charlist(Path.expand(segment_path))

      assert [] = :ets.lookup(registry, writer_key)

      view1 = {:log_view, log, 0, 1, :undefined}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 view1,
                 [{1, {:cmd, 2}}],
                 :strict,
                 :low
               )

      assert [] = :ets.lookup(registry, writer_key)
    after
      if :ets.info(registry) != :undefined do
        :ets.delete(registry)
      end

      if previous_db == nil do
        Application.delete_env(:wa_raft, :raft_database)
      else
        Application.put_env(:wa_raft, :raft_database, previous_db)
      end

      if previous_records == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_records_per_segment)
      else
        Application.put_env(
          :ferricstore,
          :waraft_segment_log_records_per_segment,
          previous_records
        )
      end
    end
  end

  test "segment appends close stale writer handles on rollover" do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    registry = :ferricstore_waraft_segment_writer_registry
    partition = System.unique_integer([:positive])
    table = :ferricstore_waraft_segment_log_rollover_writer_test
    log_name = :"#{table}_log_#{partition}"

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-rollover-writer-#{partition}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    :ets.new(registry, [:named_table, :public, :set])

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(root))
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      :wa_raft_part_sup.prepare_spec(:ferricstore_waraft_backend, %{
        table: table,
        partition: partition
      })

      log =
        {:raft_log, log_name, :ferricstore_waraft_backend, table, partition,
         :ferricstore_waraft_spike_segment_log}

      assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
      assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

      view0 = {:log_view, log, 0, 0, :undefined}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 view0,
                 [{1, {:cmd, 1}}],
                 :strict,
                 :low
               )

      assert [first_entry] = writer_entries_for_owner(registry, self())
      assert writer_entry_path(first_entry) |> to_string() |> String.ends_with?("0.seg")

      view1 = {:log_view, log, 0, 1, :undefined}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 view1,
                 [{1, {:cmd, 2}}],
                 :strict,
                 :low
               )

      assert [second_entry] = writer_entries_for_owner(registry, self())
      assert writer_entry_path(second_entry) |> to_string() |> String.ends_with?("1.seg")
    after
      _ =
        :ferricstore_waraft_spike_segment_log.close(
          {:raft_log, log_name, :ferricstore_waraft_backend, table, partition,
           :ferricstore_waraft_spike_segment_log},
          %{}
        )

      if :ets.info(registry) != :undefined do
        :ets.delete(registry)
      end

      restore_env(:wa_raft, :raft_database, previous_db)
      restore_env(:ferricstore, :waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "projection batch appends reuse the direct nosync segment writer" do
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    registry = :ferricstore_waraft_segment_writer_registry

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-projection-nosync-writer-#{System.unique_integer([:positive])}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    :ets.new(registry, [:named_table, :public, :set])

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 65_536)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(root),
                 [
                   {{:raft_log_pos, 1, 0}, [{"a", "1", 0}]},
                   {{:raft_log_pos, 2, 0}, [{"b", "2", 0}]}
                 ]
               )

      assert [entry] = writer_entries_for_owner(registry, self())
      assert writer_entry_path(entry) |> to_string() |> String.ends_with?("0.seg")
    after
      if :ets.info(registry) != :undefined do
        :ets.delete(registry)
      end

      restore_env(:ferricstore, :waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "reset waits for live writer handles owned by async append processes before rewrite" do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    registry = :ferricstore_waraft_segment_writer_registry

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-reset-cross-owner-writer-#{System.unique_integer([:positive])}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    :ets.new(registry, [:named_table, :public, :set])

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 65_536)
      File.rm_rf!(root)
      parent = self()

      writer =
        spawn_link(fn ->
          result =
            :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
              to_charlist(root),
              [{{:raft_log_pos, 1, 1}, [{"a", "1", 0}]}]
            )

          send(parent, {:first_write, self(), result})
        end)

      assert_receive {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref}, 1_000
      assert [_entry] = writer_entries_for_owner(registry, writer)

      reset_task =
        Task.async(fn ->
          :ferricstore_waraft_spike_segment_log.reset_disk_to_position(
            to_charlist(root),
            {:raft_log_pos, 0, 0}
          )
        end)

      refute Task.yield(reset_task, 50),
             "reset must not rewrite the segment directory while an async append writer is still syncing"

      send(waiter, {ref, :continue})
      assert_receive {:first_write, ^writer, :ok}, 1_000
      Application.delete_env(:ferricstore, :waraft_segment_log_file_sync_hook)
      unblock_pending_sync_hooks()
      assert {:ok, :ok} = Task.yield(reset_task, 1_000)

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(root),
                 [{{:raft_log_pos, 1, 1}, [{"b", "2", 0}]}]
               )

      assert {:ok,
              {0,
               {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 1, 1},
                [{"b", "2", 0}]}}} =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root), 1)
    after
      if :ets.info(registry) != :undefined do
        :ets.delete(registry)
      end

      File.rm_rf!(root)
      restore_env(:ferricstore, :waraft_segment_log_file_sync_hook, previous_hook)
      restore_env(:ferricstore, :waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "segment appends prune stale writer entries from dead owners" do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    registry = :ferricstore_waraft_segment_writer_registry
    partition = System.unique_integer([:positive])
    table = :ferricstore_waraft_segment_log_dead_writer_test
    log_name = :"#{table}_log_#{partition}"

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-dead-writer-#{partition}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    :ets.new(registry, [:named_table, :public, :set])

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(root))
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      :wa_raft_part_sup.prepare_spec(:ferricstore_waraft_backend, %{
        table: table,
        partition: partition
      })

      log =
        {:raft_log, log_name, :ferricstore_waraft_backend, table, partition,
         :ferricstore_waraft_spike_segment_log}

      assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
      assert {:ok, _provider_state} = :ferricstore_waraft_spike_segment_log.open(log)

      parent = self()

      owner =
        spawn(fn ->
          view0 = {:log_view, log, 0, 0, :undefined}

          result =
            :ferricstore_waraft_spike_segment_log.append(
              view0,
              [{1, {:cmd, 1}}],
              :strict,
              :low
            )

          send(parent, {:dead_writer_append, self(), result})
        end)

      ref = Process.monitor(owner)
      assert_receive {:dead_writer_append, ^owner, :ok}, 1_000
      assert_receive {:DOWN, ^ref, :process, ^owner, _reason}, 1_000
      assert [_stale] = writer_entries_for_owner(registry, owner)

      view1 = {:log_view, log, 0, 1, :undefined}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 view1,
                 [{1, {:cmd, 2}}],
                 :strict,
                 :low
               )

      assert [] = writer_entries_for_owner(registry, owner)
      assert [current] = writer_entries_for_owner(registry, self())
      assert writer_entry_path(current) |> to_string() |> String.ends_with?("1.seg")
    after
      _ =
        :ferricstore_waraft_spike_segment_log.close(
          {:raft_log, log_name, :ferricstore_waraft_backend, table, partition,
           :ferricstore_waraft_spike_segment_log},
          %{}
        )

      if :ets.info(registry) != :undefined do
        :ets.delete(registry)
      end

      restore_env(:wa_raft, :raft_database, previous_db)
      restore_env(:ferricstore, :waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "point disk reads only the target segment for cold value lookups" do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-point-read-#{System.unique_integer([:positive])}"
      )

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      entries = [
        {"k0", "v0", 0},
        {"k1", "v1", 0},
        {"k2", "v2", 0},
        {"k3", "v3", 0},
        {"k4", "v4", 0}
      ]

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection(
                 to_charlist(root),
                 {:raft_log_pos, 10, 1},
                 entries
               )

      segment_dir = Path.join(root, "segment_log")
      assert File.exists?(Path.join(segment_dir, "0.seg"))
      assert File.exists?(Path.join(segment_dir, "1.seg"))
      assert File.exists?(Path.join(segment_dir, "2.seg"))

      File.write!(Path.join(segment_dir, "0.seg"), "corrupt unrelated segment")

      assert {:ok, {0, {:ferricstore_segment_projection_entry, "k2", "v2", 0}}} =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root), 3)

      assert :not_found =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root), 99)
    after
      if previous == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_records_per_segment)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, previous)
      end
    end
  end

  test "direct disk reads use registered byte offsets inside large segments" do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-direct-read-#{System.unique_integer([:positive])}"
      )

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 65_536)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      entries = for i <- 1..128, do: {"k#{i}", "v#{i}", 0}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection(
                 to_charlist(root),
                 {:raft_log_pos, 10, 1},
                 entries
               )

      assert {:ok, {0, offset, encoded_size}} =
               :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), 128)

      assert offset > 0
      assert encoded_size > 0

      segment_path = Path.join([root, "segment_log", "0.seg"])
      assert {:ok, fd} = :file.open(to_charlist(segment_path), [:read, :write, :raw, :binary])
      assert :ok = :file.pwrite(fd, 0, <<255, 255, 255, 255>>)
      assert :ok = :file.close(fd)

      assert {:ok, {0, {:ferricstore_segment_projection_entry, "k128", "v128", 0}}} =
               :ferricstore_waraft_spike_segment_log.read_disk_at(
                 to_charlist(root),
                 128,
                 offset,
                 encoded_size
               )

      assert {:error, _reason} =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root), 128)
    after
      if previous == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_records_per_segment)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, previous)
      end
    end
  end

  test "point disk reads emit corruption telemetry for target segment corruption" do
    previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    parent = self()
    handler_id = {__MODULE__, :point_corrupt, make_ref()}

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-point-corrupt-#{System.unique_integer([:positive])}"
      )

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log_corrupt],
      &__MODULE__.handle_corrupt_telemetry/4,
      parent
    )

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection(
                 to_charlist(root),
                 {:raft_log_pos, 10, 1},
                 [{"k0", "v0", 0}, {"k1", "v1", 0}, {"k2", "v2", 0}]
               )

      segment_path = Path.join([root, "segment_log", "1.seg"])
      assert File.exists?(segment_path)
      File.write!(segment_path, "corrupt target segment")

      assert {:error, _reason} =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(root), 3)

      assert_receive {:segment_log_corrupt, [:ferricstore, :waraft, :segment_log_corrupt],
                      %{count: 1}, %{path: path, reason: reason}},
                     1_000

      assert path == segment_path
      assert reason != nil
    after
      :telemetry.detach(handler_id)

      if previous == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_records_per_segment)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, previous)
      end
    end
  end

  test "close tolerates writer registry disappearing during shutdown" do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_writer_registry_hook)
    registry = :ferricstore_waraft_segment_writer_registry
    partition = System.unique_integer([:positive])

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-close-race-#{partition}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    try do
      Application.put_env(:wa_raft, :raft_database, root)

      :wa_raft_part_sup.prepare_spec(:ferricstore_waraft_backend, %{
        table: :ferricstore_waraft_segment_log_test,
        partition: partition
      })

      :ets.new(registry, [:named_table, :public, :set])

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_writer_registry_hook,
        {:delete_once, :before_tab2list, self()}
      )

      log =
        {:raft_log, :ferricstore_waraft_segment_log_test_log, :ferricstore_waraft_backend,
         :ferricstore_waraft_segment_log_test, partition, :ferricstore_waraft_spike_segment_log}

      assert :ok = :ferricstore_waraft_spike_segment_log.close(log, %{})
      assert_receive {:waraft_segment_log_writer_registry_hook, :before_tab2list}, 1_000
    after
      File.rm_rf!(root)

      if :ets.info(registry) != :undefined do
        :ets.delete(registry)
      end

      if previous_db == nil do
        Application.delete_env(:wa_raft, :raft_database)
      else
        Application.put_env(:wa_raft, :raft_database, previous_db)
      end

      if previous_hook == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_writer_registry_hook)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_writer_registry_hook, previous_hook)
      end
    end
  end
    end
  end
end
