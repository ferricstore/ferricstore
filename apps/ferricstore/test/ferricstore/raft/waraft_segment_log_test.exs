defmodule Ferricstore.Raft.WARaftSegmentLogTest do
  use ExUnit.Case, async: false

  def handle_corrupt_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_corrupt, event, measurements, metadata})
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

  test "file segment appends keep a writer open across same-segment batches" do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_io = Application.get_env(:ferricstore, :waraft_segment_log_io_mode)
    previous_writer_mode = Application.get_env(:ferricstore, :waraft_segment_log_file_writer_mode)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    registry = :ferricstore_waraft_segment_writer_registry
    partition = System.unique_integer([:positive])
    table = :ferricstore_waraft_segment_log_file_writer_test
    log_name = :"#{table}_log_#{partition}"

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-file-writer-#{partition}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(root))
      Application.put_env(:ferricstore, :waraft_segment_log_io_mode, :file)
      Application.put_env(:ferricstore, :waraft_segment_log_file_writer_mode, :process)
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

      assert [{^writer_key, _writer_dir, :file_writer, writer_pid, first_position}] =
               :ets.lookup(registry, writer_key)

      assert is_pid(writer_pid)
      assert is_integer(first_position)
      assert first_position > 0

      view1 = {:log_view, log, 0, 1, :undefined}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 view1,
                 [{1, {:cmd, 2}}],
                 :strict,
                 :low
               )

      assert [{^writer_key, _writer_dir, :file_writer, ^writer_pid, second_position}] =
               :ets.lookup(registry, writer_key)

      assert second_position > first_position
    after
      if :ets.info(registry) != :undefined do
        for {_key, _dir, :file_writer, pid, _position} <- :ets.tab2list(registry) do
          ref = make_ref()
          send(pid, {:close, self(), ref})

          receive do
            {^ref, _reply} -> :ok
          after
            500 -> :ok
          end
        end

        :ets.delete(registry)
      end

      if previous_db == nil do
        Application.delete_env(:wa_raft, :raft_database)
      else
        Application.put_env(:wa_raft, :raft_database, previous_db)
      end

      if previous_io == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_io_mode)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_io_mode, previous_io)
      end

      if previous_writer_mode == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_file_writer_mode)
      else
        Application.put_env(
          :ferricstore,
          :waraft_segment_log_file_writer_mode,
          previous_writer_mode
        )
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

  test "persistent file segment appends reuse the caller-side fd across same-segment batches" do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_io = Application.get_env(:ferricstore, :waraft_segment_log_io_mode)
    previous_writer_mode = Application.get_env(:ferricstore, :waraft_segment_log_file_writer_mode)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    registry = :ferricstore_waraft_segment_writer_registry
    partition = System.unique_integer([:positive])
    table = :ferricstore_waraft_segment_log_persistent_writer_test
    log_name = :"#{table}_log_#{partition}"

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-persistent-writer-#{partition}"
      )

    if :ets.info(registry) != :undefined do
      :ets.delete(registry)
    end

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(root))
      Application.put_env(:ferricstore, :waraft_segment_log_io_mode, :file)
      Application.put_env(:ferricstore, :waraft_segment_log_file_writer_mode, :persistent)
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

      assert [{^writer_key, _writer_dir, :file_fd, fd, first_position}] =
               :ets.lookup(registry, writer_key)

      assert is_integer(first_position)
      assert first_position > 0

      view1 = {:log_view, log, 0, 1, :undefined}

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 view1,
                 [{1, {:cmd, 2}}],
                 :strict,
                 :low
               )

      assert [{^writer_key, _writer_dir, :file_fd, ^fd, second_position}] =
               :ets.lookup(registry, writer_key)

      assert second_position > first_position
    after
      if :ets.info(registry) != :undefined do
        for {_key, _dir, :file_fd, fd, _position} <- :ets.tab2list(registry) do
          _ = :file.close(fd)
        end

        :ets.delete(registry)
      end

      if previous_db == nil do
        Application.delete_env(:wa_raft, :raft_database)
      else
        Application.put_env(:wa_raft, :raft_database, previous_db)
      end

      if previous_io == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_io_mode)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_io_mode, previous_io)
      end

      if previous_writer_mode == nil do
        Application.delete_env(:ferricstore, :waraft_segment_log_file_writer_mode)
      else
        Application.put_env(
          :ferricstore,
          :waraft_segment_log_file_writer_mode,
          previous_writer_mode
        )
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
