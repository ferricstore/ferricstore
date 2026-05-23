defmodule Ferricstore.Raft.WARaftSegmentLogTest do
  use ExUnit.Case, async: false

  def handle_corrupt_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_corrupt, event, measurements, metadata})
  end

  def handle_append_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_append, event, measurements, metadata})
  end

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
            Ferricstore.MemoryBudget.adaptive_limits(Ferricstore.MemoryBudget.hardware_profile())

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
                    %{bytes: apply_projection_bytes}, %{kind: :apply_projection, result: :ok}},
                   1_000

    assert apply_projection_bytes > 0
  end

  test "apply projection batch append does not fsync on the hot apply path" do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-apply-projection-nosync")
    File.rm_rf!(root)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:fail_once, self()})

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

  defp with_segment_log_memory_env(opts) do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    previous_max_bytes = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_bytes)
    previous_max_entries = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_entries)
    previous_min_entries = Application.get_env(:ferricstore, :waraft_segment_log_min_ets_entries)

    partition = System.unique_integer([:positive])
    table = :"ferricstore_waraft_segment_log_memory_test_#{partition}"
    log_name = :"#{table}_log_#{partition}"

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-memory-#{partition}"
      )

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(root))

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_records_per_segment,
        opts[:records_per_segment]
      )

      Application.put_env(:ferricstore, :waraft_segment_log_max_ets_bytes, opts[:max_bytes])
      Application.put_env(:ferricstore, :waraft_segment_log_max_ets_entries, opts[:max_entries])
      Application.put_env(:ferricstore, :waraft_segment_log_min_ets_entries, opts[:min_entries])

      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      :wa_raft_part_sup.prepare_spec(:ferricstore_waraft_backend, %{
        table: table,
        partition: partition
      })

      log =
        {:raft_log, log_name, :ferricstore_waraft_backend, table, partition,
         :ferricstore_waraft_spike_segment_log}

      opts[:fun].(root, log, log_name)
    after
      restore_env(:wa_raft, :raft_database, previous_db)
      restore_env(:ferricstore, :waraft_segment_log_records_per_segment, previous_records)
      restore_env(:ferricstore, :waraft_segment_log_max_ets_bytes, previous_max_bytes)
      restore_env(:ferricstore, :waraft_segment_log_max_ets_entries, previous_max_entries)
      restore_env(:ferricstore, :waraft_segment_log_min_ets_entries, previous_min_entries)

      if :ets.info(log_name) != :undefined do
        :ets.delete(log_name)
      end
    end
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)

  defp map_fold_seen({:ok, entries}) do
    {:ok, Enum.reverse(entries)}
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
