Code.require_file(
  "waraft_segment_log_test/sections/segment_log_caps_ets_tail_while_disk_backed_reads_still_see_older_entrie.exs",
  __DIR__
)

Code.require_file(
  "waraft_segment_log_test/sections/sync_apply_projection_batch_append_fdatasyncs_before_returning.exs",
  __DIR__
)

Code.require_file(
  "waraft_segment_log_test/sections/default_segment_size_does_not_roll_over_during_normal_hot_batches.exs",
  __DIR__
)

defmodule Ferricstore.Raft.WARaftSegmentLogTest do
  use ExUnit.Case, async: false
  @moduletag :raft
  @moduletag :global_state

  def handle_corrupt_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_corrupt, event, measurements, metadata})
  end

  def handle_append_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_append, event, measurements, metadata})
  end

  def handle_projection_overlap_telemetry(event, measurements, metadata, parent) do
    send(parent, {:projection_overlap, event, measurements, metadata})
  end

  def handle_load_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_load, event, measurements, metadata})
  end

  def handle_fold_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_fold, event, measurements, metadata})
  end

  use Ferricstore.Raft.WARaftSegmentLogTest.Sections.SegmentLogCapsEtsTailWhileDiskBackedReadsStillSeeOlderEntrie

  use Ferricstore.Raft.WARaftSegmentLogTest.Sections.SyncApplyProjectionBatchAppendFdatasyncsBeforeReturning

  defp clear_segment_offset_registry do
    if :ets.info(:ferricstore_waraft_segment_offset_registry) != :undefined do
      :ets.delete_all_objects(:ferricstore_waraft_segment_offset_registry)
    end
  end

  defp with_segment_log_memory_env(opts) do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    previous_max_bytes = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_bytes)
    previous_max_entries = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_entries)
    previous_min_entries = Application.get_env(:ferricstore, :waraft_segment_log_min_ets_entries)

    previous_offset_entries =
      Application.get_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries)

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

      if opts[:offset_entries] do
        Application.put_env(
          :ferricstore,
          :waraft_segment_log_offset_registry_max_entries,
          opts[:offset_entries]
        )
      end

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

      restore_env(
        :ferricstore,
        :waraft_segment_log_offset_registry_max_entries,
        previous_offset_entries
      )

      if :ets.info(log_name) != :undefined do
        :ets.delete(log_name)
      end
    end
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)

  defp unblock_pending_sync_hooks do
    receive do
      {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref} ->
        send(waiter, {ref, :continue})
        unblock_pending_sync_hooks()
    after
      0 -> :ok
    end
  end

  defp writer_entries_for_owner(registry, owner) do
    registry
    |> :ets.tab2list()
    |> Enum.filter(fn
      {{^owner, _path}, _dir, :file_fd, _fd, _position} -> true
      {{^owner, _path}, _dir, _kind, _handle, _position} -> true
      {{^owner, _path}, _dir, _handle, _position} -> true
      _entry -> false
    end)
  end

  defp writer_entry_path({{_owner, path}, _dir, :file_fd, _fd, _position}), do: path
  defp writer_entry_path({{_owner, path}, _dir, _kind, _handle, _position}), do: path
  defp writer_entry_path({{_owner, path}, _dir, _handle, _position}), do: path

  defp map_fold_seen({:ok, entries}) do
    {:ok, Enum.reverse(entries)}
  end

  defp corrupt_segment_crc!(path) do
    <<length::unsigned-big-32, crc::unsigned-big-32, rest::binary>> = File.read!(path)

    File.write!(
      path,
      <<length::unsigned-big-32, Bitwise.bxor(crc, 1)::unsigned-big-32, rest::binary>>
    )
  end

  test "a projection-bound disk fold reads only the Raft tail after its covered index" do
    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      records_per_segment: 4,
      fun: fn _root, log, _log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, provider_state} = provider.open(log)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   for(index <- 1..12, do: {1, {:cmd, index}}),
                   :strict,
                   :low
                 )

        segment_root = log |> provider.memory_status() |> Map.fetch!(:dir) |> Path.dirname()
        fold = fn index, entry, acc -> [{index, entry} | acc] end

        assert {:ok, all} = provider.fold_disk(segment_root, fold, []) |> map_fold_seen()
        assert Enum.map(all, &elem(&1, 0)) == Enum.to_list(1..12)

        assert {:ok, tail} =
                 provider.fold_disk_after(segment_root, 7, fold, []) |> map_fold_seen()

        assert Enum.map(tail, &elem(&1, 0)) == Enum.to_list(8..12)

        assert {:ok, partial} =
                 provider.fold_disk_after(segment_root, 6, fold, []) |> map_fold_seen()

        assert Enum.map(partial, &elem(&1, 0)) == Enum.to_list(7..12)

        assert :ok = provider.close(log, provider_state)

        segment_dir = provider.memory_status(log).dir
        corrupt_segment_crc!(Path.join(segment_dir, "0.seg"))

        assert {:error, {:crc_mismatch, 0}} = provider.fold_disk(segment_root, fold, [])
        assert {:ok, recovered_tail} = provider.fold_disk_after(segment_root, 7, fold, [])
        assert Enum.map(recovered_tail, &elem(&1, 0)) == Enum.to_list(12..8//-1)

        corrupt_segment_crc!(Path.join(segment_dir, "3.seg"))
        assert {:error, {:crc_mismatch, 0}} = provider.fold_disk_after(segment_root, 7, fold, [])

        corrupt_segment_crc!(Path.join(segment_dir, "3.seg"))
        corrupt_segment_crc!(Path.join(segment_dir, "0.seg"))
        File.rm!(Path.join(segment_dir, "2.seg"))

        assert {:error, {:non_contiguous_record_index, 7, 12}} =
                 provider.fold_disk_after(segment_root, 7, fold, [])
      end
    )
  end

  test "disk-backed segment offsets stay bounded across append and truncate" do
    clear_segment_offset_registry()

    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      offset_entries: 4,
      records_per_segment: 64,
      fun: fn _root, log, _log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, provider_state} = provider.open(log)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   for(index <- 1..32, do: {1, {:cmd, "value-#{index}"}}),
                   :strict,
                   :low
                 )

        registry = :ferricstore_waraft_segment_offset_registry
        assert :ets.info(registry, :size) <= 6
        assert {:ok, {1, {:cmd, "value-1"}}} = provider.get(log, 1)
        %{dir: segment_dir} = provider.memory_status(log)
        segment_root = Path.dirname(segment_dir)

        assert {:ok, {_ordinal, _offset, _size}} =
                 provider.location_for_index(to_charlist(segment_root), 24)

        assert {:ok, _} = provider.truncate(log, 24, provider_state)
        assert :ets.info(registry, :size) <= 6
        assert {:ok, {1, {:cmd, "value-23"}}} = provider.get(log, 23)
        assert :not_found = provider.get(log, 24)
        assert :not_found = provider.location_for_index(to_charlist(segment_root), 24)

        assert :ok = provider.close(log, provider_state)
        assert {:ok, _} = provider.open(log)
        assert {:ok, {1, {:cmd, "value-1"}}} = provider.get(log, 1)
        assert :ets.info(registry, :size) <= 6
      end
    )
  end

  test "evicted projection offsets preserve older disk reads and newer appends" do
    clear_segment_offset_registry()

    previous_limit =
      Application.get_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries)

    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 4)

    on_exit(fn ->
      restore_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, previous_limit)
    end)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-projection-bounded-offsets-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    provider = :ferricstore_waraft_spike_segment_log

    assert :ok =
             provider.write_projection_batches_sync(
               to_charlist(root),
               for(
                 index <- 1..32,
                 do: {{:raft_log_pos, index, 7}, [{"key-#{index}", "old", 0}]}
               )
             )

    assert :ets.info(:ferricstore_waraft_segment_offset_registry, :size) <= 6
    assert {:ok, {_ordinal, offset, size}} = provider.location_for_index(to_charlist(root), 1)

    assert {:ok,
            {0,
             {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 1, 7},
              [{"key-1", "old", 0}]}}} =
             provider.read_disk_at(to_charlist(root), 1, offset, size)

    assert :ok =
             provider.write_projection_batches(
               to_charlist(root),
               [{{:raft_log_pos, 33, 7}, [{"key-33", "new", 0}]}]
             )

    assert {:ok,
            {0, {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 33, 7}, entries}}} =
             provider.read_disk(to_charlist(root), 33)

    assert entries == [{"key-33", "new", 0}]
    assert :ets.info(:ferricstore_waraft_segment_offset_registry, :size) <= 6
  end

  test "disk projection fold supplies validated physical locations during one scan" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-projection-location-fold-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    provider = :ferricstore_waraft_spike_segment_log

    assert :ok =
             provider.write_projection_batches_sync(
               to_charlist(root),
               for(
                 index <- 1..8,
                 do: {{:raft_log_pos, index, 7}, [{"key-#{index}", "value-#{index}", 0}]}
               )
             )

    clear_segment_offset_registry()

    assert {:ok, locations} =
             provider.fold_disk_with_locations(
               to_charlist(root),
               fn index, entry, location, acc -> [{index, entry, location} | acc] end,
               []
             )

    assert length(locations) == 8

    for {index, entry, {ordinal, offset, encoded_size}} <- locations do
      assert is_integer(offset) and offset >= 0
      assert encoded_size > 8

      assert {:ok, ^entry} =
               provider.read_disk_at(to_charlist(root), index, offset, encoded_size)

      assert {:ok, {^ordinal, ^offset, ^encoded_size}} =
               provider.location_for_index(to_charlist(root), index)
    end
  end

  test "old WAL offsets use a rebuildable disk index after hot-cache eviction" do
    clear_segment_offset_registry()

    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      offset_entries: 4,
      records_per_segment: 64,
      fun: fn _root, log, _log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, provider_state} = provider.open(log)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   for(index <- 1..32, do: {1, {:cmd, "value-#{index}"}}),
                   :strict,
                   :low
                 )

        %{dir: dir} = provider.memory_status(log)
        index_path = Path.join(dir, "0.idx")
        assert File.regular?(index_path)
        assert :ets.info(:ferricstore_waraft_segment_offset_registry, :size) <= 6

        assert {:ok, {ordinal, offset, size}} =
                 provider.location_for_index(to_charlist(Path.dirname(dir)), 1)

        assert {:ok, {1, {:cmd, "value-1"}}} =
                 provider.read_disk_at(to_charlist(Path.dirname(dir)), 1, offset, size)

        assert ordinal == 0

        # The index is derived. A broken sidecar must not make the WAL unreadable.
        File.write!(index_path, <<0>>, [:write])

        assert {:ok, {^ordinal, ^offset, ^size}} =
                 provider.location_for_index(to_charlist(Path.dirname(dir)), 1)

        assert :ok = provider.close(log, provider_state)
        clear_segment_offset_registry()
        assert {:ok, _} = provider.open(log)
        assert {:ok, %{size: rebuilt_bytes}} = File.stat(index_path)
        assert rebuilt_bytes >= 32 * 28

        assert {:ok, {^ordinal, ^offset, ^size}} =
                 provider.location_for_index(to_charlist(Path.dirname(dir)), 1)
      end
    )
  end

  test "read-ahead startup scans still detect corruption beyond the first buffer" do
    clear_segment_offset_registry()

    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      records_per_segment: 4_096,
      fun: fn _root, log, log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, state} = provider.open(log)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   for(index <- 1..2_000, do: {1, {:cmd, index, :binary.copy("v", 600)}}),
                   :strict,
                   :low
                 )

        %{dir: segment_dir} = provider.memory_status(log)
        log_root = Path.dirname(segment_dir)
        assert :ok = provider.close(log, state)
        :ets.delete_all_objects(log_name)
        clear_segment_offset_registry()

        startup_key = {Ferricstore.Application, :starting}
        previous_starting = :persistent_term.get(startup_key, false)
        :persistent_term.put(startup_key, true)

        try do
          assert {:ok, 2_000} =
                   provider.fold_disk(
                     to_charlist(log_root),
                     fn _index, _entry, count -> count + 1 end,
                     0
                   )

          assert {:ok, {0, offset, _encoded_size}} =
                   provider.location_for_index(to_charlist(log_root), 1_800)

          segment = Path.join(segment_dir, "0.seg")
          assert {:ok, fd} = :file.open(to_charlist(segment), [:read, :write, :binary, :raw])
          assert {:ok, <<old_byte>>} = :file.pread(fd, offset + 16, 1)
          assert :ok = :file.pwrite(fd, offset + 16, <<Bitwise.bxor(old_byte, 255)>>)
          assert :ok = :file.close(fd)

          assert {:error, {:crc_mismatch, ^offset}} = provider.open(log)
        after
          :persistent_term.put(startup_key, previous_starting)
        end
      end
    )
  end

  test "logical trim prunes sidecars without recreating indexes for trimmed records" do
    clear_segment_offset_registry()

    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      offset_entries: 4,
      records_per_segment: 64,
      fun: fn _root, log, log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, state} = provider.open(log)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   for(index <- 1..130, do: {1, {:cmd, index}}),
                   :strict,
                   :low
                 )

        %{dir: segment_dir} = provider.memory_status(log)
        old_index = Path.join(segment_dir, "0.idx")
        live_index = Path.join(segment_dir, "1.idx")
        assert File.regular?(old_index)
        assert File.regular?(live_index)

        assert {:ok, _} = provider.trim(log, 64, state)
        refute File.exists?(old_index)
        assert File.regular?(live_index)
        assert :not_found = provider.get(log, 63)
        assert {:ok, {1, {:cmd, 64}}} = provider.get(log, 64)

        assert :ok = provider.close(log, state)
        :ets.delete_all_objects(log_name)
        clear_segment_offset_registry()

        # The obsolete physical segment is storage debt, not replayable WAL.
        # Its corruption must not block a checkpointed restart.
        old_segment = Path.join(segment_dir, "0.seg")
        assert File.regular?(old_segment)
        assert {:ok, fd} = :file.open(to_charlist(old_segment), [:read, :write, :binary, :raw])
        assert :ok = :file.pwrite(fd, 8, <<0>>)
        assert :ok = :file.close(fd)

        assert {:ok, _} = provider.open(log)
        refute File.exists?(old_index)
        assert {:ok, {1, {:cmd, 64}}} = provider.get(log, 64)

        assert {:ok, folded} =
                 provider.fold_disk(
                   to_charlist(Path.dirname(segment_dir)),
                   fn index, _entry, seen -> [index | seen] end,
                   []
                 )

        assert Enum.reverse(folded) == Enum.to_list(64..130)
      end
    )
  end

  test "offset index rebuild refuses a symlink without touching its target or unrelated files" do
    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      records_per_segment: 64,
      fun: fn _root, log, _log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, state} = provider.open(log)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   [{1, {:cmd, "unchanged"}}],
                   :strict,
                   :low
                 )

        %{dir: dir} = provider.memory_status(log)
        assert :ok = provider.close(log, state)
        index_path = Path.join(dir, "0.idx")
        victim = Path.join(Path.dirname(dir), "victim")
        unrelated = Path.join(dir, "notes.idx")
        File.write!(victim, "protected")
        File.write!(unrelated, "unrelated")
        File.rm!(index_path)
        File.ln_s!(victim, index_path)

        assert {:error, {:unsafe_offset_index_path, :symlink}} = provider.open(log)
        assert File.read!(victim) == "protected"
        assert File.read!(unrelated) == "unrelated"
      end
    )
  end

  test "derived-index write failure does not report a durable WAL append as failed" do
    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      records_per_segment: 64,
      fun: fn _root, log, _log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, state} = provider.open(log)
        %{dir: dir} = provider.memory_status(log)
        index_path = Path.join(dir, "0.idx")
        victim = Path.join(Path.dirname(dir), "victim")
        File.write!(victim, "protected")
        File.ln_s!(victim, index_path)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   [{1, {:cmd, "durable"}}],
                   :strict,
                   :low
                 )

        assert File.read!(victim) == "protected"
        assert {:ok, {1, {:cmd, "durable"}}} = provider.get(log, 1)
        assert :ok = provider.close(log, state)
      end
    )
  end

  test "a stale but checksummed sidecar never redirects an older WAL lookup" do
    clear_segment_offset_registry()

    with_segment_log_memory_env(
      max_bytes: 4_096,
      max_entries: 4,
      min_entries: 2,
      offset_entries: 4,
      records_per_segment: 64,
      fun: fn _root, log, _log_name ->
        provider = :ferricstore_waraft_spike_segment_log
        assert :ok = provider.init(log)
        assert {:ok, _state} = provider.open(log)

        assert :ok =
                 provider.append(
                   {:log_view, log, 0, 0, :undefined},
                   for(index <- 1..32, do: {1, {:cmd, "value-#{index}"}}),
                   :strict,
                   :low
                 )

        %{dir: dir} = provider.memory_status(log)
        root = Path.dirname(dir)

        assert {:ok, {0, actual_offset, encoded_size}} =
                 provider.location_for_index(to_charlist(root), 2)

        assert actual_offset > 0

        index_path = Path.join(dir, "0.idx")

        body =
          <<0xF00D2026::unsigned-big-32, 2::unsigned-big-64, 0::unsigned-big-64,
            encoded_size::unsigned-big-32>>

        forged = <<body::binary, :erlang.crc32(body)::unsigned-big-32>>
        assert {:ok, fd} = :file.open(to_charlist(index_path), [:read, :write, :binary, :raw])
        assert :ok = :file.pwrite(fd, 2 * 28, forged)
        assert :ok = :file.close(fd)
        clear_segment_offset_registry()

        assert {:ok, {0, ^actual_offset, ^encoded_size}} =
                 provider.location_for_index(to_charlist(root), 2)
      end
    )
  end

  test "an unavailable sidecar cannot return an older version after the hot cache evicts it" do
    clear_segment_offset_registry()

    previous_limit =
      Application.get_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries)

    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 4)

    on_exit(fn ->
      restore_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, previous_limit)
    end)

    root =
      Path.join([
        System.tmp_dir!(),
        "ferricstore-offset-sidecar-error-#{System.unique_integer([:positive])}",
        "apply_projection_log"
      ])

    on_exit(fn -> File.rm_rf!(Path.dirname(root)) end)
    provider = :ferricstore_waraft_spike_segment_log

    assert :ok =
             provider.write_projection_batches_sync(
               to_charlist(root),
               [{{:raft_log_pos, 42, 0}, [{"key", "old", 0}]}]
             )

    assert {:ok, {0, old_offset, encoded_size}} =
             provider.location_for_index(to_charlist(root), 42)

    sidecar = Path.join(root, "segment_log/0.idx")
    File.chmod!(sidecar, 0o444)

    assert :ok =
             provider.write_projection_batches(
               to_charlist(root),
               [{{:raft_log_pos, 42, 0}, [{"key", "new", 0}]}]
             )

    assert :ok =
             provider.write_projection_batches(
               to_charlist(root),
               for(
                 index <- 43..50,
                 do: {{:raft_log_pos, index, 0}, [{"key-#{index}", "val", 0}]}
               )
             )

    assert {:ok, {0, newest_offset, _size}} = provider.location_for_index(to_charlist(root), 42)
    assert newest_offset > old_offset

    assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"key", "new", 0}]}}} =
             provider.read_disk_at(to_charlist(root), 42, newest_offset, encoded_size)
  end

  test "apply projection offsets survive alternating writer processes" do
    root =
      Path.join([
        System.tmp_dir!(),
        "ferricstore-waraft-segment-alternating-writers-#{System.unique_integer([:positive])}",
        "apply_projection_log"
      ])

    on_exit(fn -> File.rm_rf!(Path.dirname(root)) end)

    writer = spawn_link(fn -> projection_writer_loop(root) end)
    on_exit(fn -> if Process.alive?(writer), do: Process.exit(writer, :kill) end)

    ref = make_ref()
    send(writer, {:write, self(), ref, 41, "first"})
    assert_receive {^ref, 41, :ok}, 1_000

    assert :ok = write_apply_projection(root, 42, "second")

    send(writer, {:write, self(), ref, 43, "third"})
    assert_receive {^ref, 43, :ok}, 1_000

    assert {:ok, {_ordinal, offset, encoded_size}} =
             :ferricstore_waraft_spike_segment_log.location_for_index(
               to_charlist(root),
               43
             )

    assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"key-43", "third", 0}]}}} =
             :ferricstore_waraft_spike_segment_log.read_disk_at(
               to_charlist(root),
               43,
               offset,
               encoded_size
             )

    send(writer, :stop)
  end

  defp projection_writer_loop(root) do
    receive do
      {:write, reply_to, ref, index, value} ->
        result = write_apply_projection(root, index, value)
        send(reply_to, {ref, index, result})
        projection_writer_loop(root)

      :stop ->
        :ok
    end
  end

  defp write_apply_projection(root, index, value) do
    :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
      to_charlist(root),
      [{{:raft_log_pos, index, 0}, [{"key-#{index}", value, 0}]}]
    )
  end

  test "validated disk reader reuses one segment handle for known locations" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-reader-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
               to_charlist(root),
               [
                 {{:raft_log_pos, 42, 0}, [{"a", "first", 0}]},
                 {{:raft_log_pos, 43, 0}, [{"b", "second", 0}]}
               ]
             )

    assert {:ok, {ordinal, first_offset, first_size}} =
             :ferricstore_waraft_spike_segment_log.location_for_index(
               to_charlist(root),
               42
             )

    assert {:ok, {^ordinal, second_offset, second_size}} =
             :ferricstore_waraft_spike_segment_log.location_for_index(
               to_charlist(root),
               43
             )

    assert {:error, {:segment_ordinal_mismatch, ^ordinal, wrong_ordinal}} =
             :ferricstore_waraft_spike_segment_log.open_disk_reader(
               to_charlist(root),
               42,
               ordinal + 1
             )

    assert wrong_ordinal == ordinal + 1

    assert {:ok, reader} =
             :ferricstore_waraft_spike_segment_log.open_disk_reader(
               to_charlist(root),
               42,
               ordinal
             )

    try do
      assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"a", "first", 0}]}}} =
               :ferricstore_waraft_spike_segment_log.read_disk_reader(
                 reader,
                 42,
                 first_offset,
                 first_size
               )

      assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"b", "second", 0}]}}} =
               :ferricstore_waraft_spike_segment_log.read_disk_reader(
                 reader,
                 43,
                 second_offset,
                 second_size
               )

      assert {:ok,
              [
                {0, {:ferricstore_segment_apply_projection_batch, _, [{"b", "second", 0}]}},
                {0, {:ferricstore_segment_apply_projection_batch, _, [{"a", "first", 0}]}}
              ]} =
               :ferricstore_waraft_spike_segment_log.read_disk_reader_many(reader, [
                 {43, second_offset, second_size},
                 {42, first_offset, first_size}
               ])

      assert {:ok,
              [
                {0, {:ferricstore_segment_apply_projection_batch, _, [{"b", "second", 0}]}},
                {0, {:ferricstore_segment_apply_projection_batch, _, [{"a", "first", 0}]}},
                {0, {:ferricstore_segment_apply_projection_batch, _, [{"b", "second", 0}]}}
              ]} =
               :ferricstore_waraft_spike_segment_log.read_disk_reader_many(reader, [
                 {43, second_offset, second_size},
                 {42, first_offset, first_size},
                 {43, second_offset, second_size}
               ])
    after
      assert :ok = :ferricstore_waraft_spike_segment_log.close_disk_reader(reader)
    end
  end

  test "validated disk reader coalesces adjacent frames before positional IO" do
    source =
      File.read!(
        Path.expand(
          "../../../src/ferricstore_waraft_spike_segment_log/sections/part_01.hrl",
          __DIR__
        )
      )

    assert source =~ "coalesce_adjacent_disk_reader_requests"
    assert source =~ "file:pread(Fd, SpanLocations)"
    refute source =~ "file:pread(Fd, Locations)"
  end

  use Ferricstore.Raft.WARaftSegmentLogTest.Sections.DefaultSegmentSizeDoesNotRollOverDuringNormalHotBatches
end
