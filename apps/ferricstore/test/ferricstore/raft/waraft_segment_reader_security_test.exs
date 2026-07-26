defmodule Ferricstore.Raft.WARaftSegmentReaderSecurityTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftSegmentReader

  @moduletag :raft

  test "missing shard reads do not intern ETS table atoms" do
    shard_index = 1_000_000_000 + System.unique_integer([:positive])
    table_name = "raft_log_ferricstore_waraft_backend_#{shard_index + 1}"

    assert_raise ArgumentError, fn -> String.to_existing_atom(table_name) end

    _result =
      WARaftSegmentReader.read_value(
        %{data_dir: System.tmp_dir!()},
        shard_index,
        1,
        "missing"
      )

    assert_raise ArgumentError, fn -> String.to_existing_atom(table_name) end
  end

  test "batch reads reject malformed segment locations instead of reporting misses" do
    assert {:error, :not_waraft_segment_location} =
             WARaftSegmentReader.read_values_from_location(
               %{data_dir: System.tmp_dir!()},
               0,
               {:corrupt_location, 1},
               ["key"]
             )

    assert {:error, :bad_segment_location} =
             WARaftSegmentReader.read_values_from_location(
               %{data_dir: System.tmp_dir!()},
               0,
               {:waraft_segment, 0},
               ["key"]
             )

    assert {:error, :invalid_location_batch} =
             WARaftSegmentReader.read_values_from_locations(
               %{data_dir: System.tmp_dir!()},
               0,
               [{{:waraft_apply_projection, 1}, []}],
               1_000
             )

    assert {:error, :invalid_location_batch} =
             WARaftSegmentReader.read_values_from_locations(
               %{data_dir: System.tmp_dir!()},
               0,
               List.duplicate({{:waraft_apply_projection, 1}, ["key"]}, 4_097),
               1_000
             )

    assert {:error, :invalid_location_batch} =
             WARaftSegmentReader.read_values_from_locations(
               %{data_dir: System.tmp_dir!()},
               0,
               [{{:waraft_apply_projection, 1}, List.duplicate("key", 4_097)}],
               1_000
             )

    assert {:error, :invalid_location_batch} =
             WARaftSegmentReader.read_values_from_locations(
               %{data_dir: System.tmp_dir!()},
               0,
               [
                 {{:waraft_apply_projection, 1}, ["same-key"]},
                 {{:waraft_apply_projection, 2}, ["same-key"]}
               ],
               1_000
             )
  end

  test "physical reads reject an aggregate unique-frame byte budget overflow before disk access" do
    oversized_requests = [
      %{
        file_id: {:waraft_segment, 1},
        ordinal: 0,
        offset: 0,
        frame_size: 600 * 1_024 * 1_024,
        key: "first"
      },
      %{
        file_id: {:waraft_projection, 2},
        ordinal: 0,
        offset: 0,
        frame_size: 600 * 1_024 * 1_024,
        key: "second"
      }
    ]

    assert {:error, :physical_read_byte_budget_exceeded} =
             WARaftSegmentReader.read_physical_values(
               %{data_dir: "/path-that-must-not-be-read"},
               0,
               oversized_requests,
               1_000,
               :live
             )
  end

  test "multi-location reads fail when a recorded Raft index is missing" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-missing-projection-location-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert {:error, :apply_projection_entry_missing_at_recorded_location} =
             WARaftSegmentReader.read_values_from_locations(
               %{data_dir: data_dir},
               0,
               [{{:waraft_apply_projection, 9_001}, ["missing"]}],
               1_000
             )
  end

  test "spilled apply projections preserve expiry semantics" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-expired-projection-#{System.unique_integer([:positive])}"
      )

    index = 73
    file_id = {:waraft_apply_projection, index}
    expired_at_ms = Ferricstore.HLC.now_ms() - 1

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"expired", "must-stay-hidden", expired_at_ms}
             ])

    assert :not_found =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               "expired"
             )

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    assert :not_found =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               "expired"
             )

    assert {:ok, %{}} =
             WARaftSegmentReader.read_values_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               ["expired"]
             )

    assert {:ok, "must-stay-hidden"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               %{data_dir: data_dir},
               0,
               file_id,
               "expired"
             )

    assert {:ok, %{"expired" => "must-stay-hidden"}} =
             WARaftSegmentReader.read_values_from_location_including_expired(
               %{data_dir: data_dir},
               0,
               file_id,
               ["expired"],
               10_000
             )

    assert {:ok, %{}} =
             WARaftSegmentReader.read_values_from_locations(
               %{data_dir: data_dir},
               0,
               [{file_id, ["expired"]}],
               10_000
             )

    assert {:ok, %{"expired" => "must-stay-hidden"}} =
             WARaftSegmentReader.read_values_from_locations_including_expired(
               %{data_dir: data_dir},
               0,
               [{file_id, ["expired"]}],
               10_000
             )
  end

  test "multi-location reads combine latest and merged projection records" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-multi-location-#{System.unique_integer([:positive])}"
      )

    first_index = 78
    second_index = 79

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, first_index, [
               {"old", "old-value", 0}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, first_index, [
               {"new", "new-value", 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, second_index, [
               {"other", "other-value", 0}
             ])

    assert {:ok, 2} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    assert {:ok,
            %{
              "old" => "old-value",
              "new" => "new-value",
              "other" => "other-value"
            }} =
             WARaftSegmentReader.read_values_from_locations(
               %{data_dir: data_dir},
               0,
               [
                 {{:waraft_apply_projection, first_index}, ["old", "new"]},
                 {{:waraft_apply_projection, second_index}, ["other"]}
               ],
               10_000
             )
  end

  test "durability pin spills complete requested indexes and leaves unrelated indexes hot" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-durability-pin-#{System.unique_integer([:positive])}"
      )

    pinned_index = 75
    unrelated_index = 76

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, pinned_index, [
               {"pinned", "pinned-value", 0},
               {"same-index", "same-index-value", 0}
             ])

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, unrelated_index, [
               {"unrelated", "unrelated-value", 0}
             ])

    assert {:ok, 2} =
             WARaftSegmentReader.ensure_apply_projection_entries_durable(data_dir, 0, [
               {pinned_index, "pinned"}
             ])

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1

    assert {:ok, "pinned-value"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               %{data_dir: data_dir},
               0,
               {:waraft_apply_projection, pinned_index},
               "pinned"
             )

    assert {:ok, "same-index-value"} =
             WARaftSegmentReader.read_value_from_location_including_expired(
               %{data_dir: data_dir},
               0,
               {:waraft_apply_projection, pinned_index},
               "same-index"
             )

    assert {:error, {:apply_projection_entry_not_durable, ^pinned_index, "missing"}} =
             WARaftSegmentReader.ensure_apply_projection_entries_durable(data_dir, 0, [
               {pinned_index, "missing"}
             ])

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1
  end

  test "durability pin verifies every requested key with one disk read per index" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-batched-durability-pin-#{System.unique_integer([:positive])}"
      )

    index = 77
    entries = for key <- 1..128, do: {"key-#{key}", "value-#{key}", 0}
    refs = for {key, _value, _expire_at_ms} <- entries, do: {index, key}
    latest_reads = :atomics.new(1, signed: false)
    merged_reads = :atomics.new(1, signed: false)

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok = WARaftSegmentReader.put_apply_projection(data_dir, 0, index, entries)

    Process.put(
      :ferricstore_waraft_apply_projection_disk_read_hook,
      fn _root, ^index, source ->
        counter = if source == :latest, do: latest_reads, else: merged_reads
        :atomics.add(counter, 1, 1)
      end
    )

    try do
      assert {:ok, 128} =
               WARaftSegmentReader.ensure_apply_projection_entries_durable(data_dir, 0, refs)
    after
      Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook)
    end

    assert :atomics.get(latest_reads, 1) == 1
    assert :atomics.get(merged_reads, 1) == 0
  end

  test "apply projection expiry follows the cluster-adjusted clock" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-hlc-expired-projection-#{System.unique_integer([:positive])}"
      )

    index = 74
    file_id = {:waraft_apply_projection, index}
    wall_ms = System.system_time(:millisecond)
    ref = :persistent_term.get(:ferricstore_hlc_ref)
    previous = :atomics.get(ref, 1)

    on_exit(fn ->
      :atomics.put(:persistent_term.get(:ferricstore_hlc_ref), 1, previous)
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    :atomics.put(ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"hlc-expired", "must-stay-hidden", wall_ms + 30_000}
             ])

    assert :not_found =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               "hlc-expired"
             )

    assert {:ok, %{}} =
             WARaftSegmentReader.read_values_from_location(
               %{data_dir: data_dir},
               0,
               file_id,
               ["hlc-expired"]
             )
  end

  test "apply projection compaction rolls back an interrupted directory swap" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-compact-rollback-#{System.unique_integer([:positive])}"
      )

    projection_root =
      Path.join([
        data_dir,
        "waraft",
        "ferricstore_waraft_backend.1",
        "apply_projection_log"
      ])

    old_index = 41
    trim_index = 42
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_rewrite_hook)

    on_exit(fn ->
      if is_nil(previous_hook) do
        Application.delete_env(:ferricstore, :waraft_segment_log_rewrite_hook)
      else
        Application.put_env(:ferricstore, :waraft_segment_log_rewrite_hook, previous_hook)
      end

      File.rm_rf!(data_dir)
    end)

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
               to_charlist(projection_root),
               [
                 {{:raft_log_pos, old_index, 0}, [{"old", "drop-after-success", 0}]},
                 {{:raft_log_pos, trim_index, 0}, [{"current", "keep", 0}]}
               ]
             )

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_rewrite_hook,
      {:fail_once_after_live_backup, self()}
    )

    assert {:error, {:rewrite_hook, :after_live_backup}} =
             :ferricstore_waraft_spike_segment_log.compact_apply_projection(
               to_charlist(projection_root),
               trim_index,
               []
             )

    assert_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}

    assert {:ok, _old_entry} =
             :ferricstore_waraft_spike_segment_log.read_disk(
               to_charlist(projection_root),
               old_index
             )

    assert {:ok, _current_entry} =
             :ferricstore_waraft_spike_segment_log.read_disk(
               to_charlist(projection_root),
               trim_index
             )

    assert :ok =
             :ferricstore_waraft_spike_segment_log.compact_apply_projection(
               to_charlist(projection_root),
               trim_index,
               []
             )

    assert :not_found =
             :ferricstore_waraft_spike_segment_log.read_disk(
               to_charlist(projection_root),
               old_index
             )

    assert {:ok, _current_entry} =
             :ferricstore_waraft_spike_segment_log.read_disk(
               to_charlist(projection_root),
               trim_index
             )
  end

  test "apply projection disk latch recovers when a spill owner dies" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-disk-lock-recovery-#{System.unique_integer([:positive])}"
      )

    index = 81
    previous_hook = Application.get_env(:ferricstore, :waraft_apply_projection_spill_hook)
    parent = self()

    on_exit(fn ->
      if is_nil(previous_hook) do
        Application.delete_env(:ferricstore, :waraft_apply_projection_spill_hook)
      else
        Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, previous_hook)
      end

      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"recover-lock", "value", 0}
             ])

    Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, fn _batches ->
      send(parent, {:disk_lock_holder_ready, self()})
      Process.sleep(:infinity)
    end)

    assert {:ok, holder} =
             Task.start(fn -> WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0) end)

    monitor = Process.monitor(holder)
    assert_receive {:disk_lock_holder_ready, ^holder}
    Process.exit(holder, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^holder, :killed}

    Application.delete_env(:ferricstore, :waraft_apply_projection_spill_hook)

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    assert {:ok, "value"} =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               {:waraft_apply_projection, index},
               "recover-lock"
             )
  end

  test "apply projection disk latch shares cold reads while keeping rewrites exclusive" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-disk-read-sharing-#{System.unique_integer([:positive])}"
      )

    index = 87
    parent = self()

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"shared-read", "value", 0}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    start_reader = fn ->
      Task.async(fn ->
        Process.put(
          :ferricstore_waraft_apply_projection_disk_read_hook,
          fn _root, ^index, :latest ->
            send(parent, {:apply_projection_disk_reader_entered, self()})

            receive do
              :release_apply_projection_disk_reader -> :ok
            after
              2_000 -> raise "timed out waiting to release apply-projection disk reader"
            end
          end
        )

        WARaftSegmentReader.read_value_from_location(
          %{data_dir: data_dir},
          0,
          {:waraft_apply_projection, index},
          "shared-read"
        )
      end)
    end

    first_reader = start_reader.()
    assert_receive {:apply_projection_disk_reader_entered, first_reader_pid}, 1_000

    second_reader = start_reader.()
    assert_receive {:apply_projection_disk_reader_entered, second_reader_pid}, 1_000

    writer =
      Task.async(fn ->
        WARaftSegmentReader.with_apply_projection_disk_lock(data_dir, 0, fn ->
          send(parent, :apply_projection_disk_writer_entered)
          :ok
        end)
      end)

    refute_receive :apply_projection_disk_writer_entered, 50

    send(first_reader_pid, :release_apply_projection_disk_reader)
    send(second_reader_pid, :release_apply_projection_disk_reader)

    assert {:ok, "value"} = Task.await(first_reader, 2_000)
    assert {:ok, "value"} = Task.await(second_reader, 2_000)
    assert_receive :apply_projection_disk_writer_entered, 1_000
    assert :ok = Task.await(writer, 2_000)
  end

  test "apply projection disk latch reclaims a dead cold reader" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-dead-disk-reader-#{System.unique_integer([:positive])}"
      )

    index = 89
    parent = self()

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"dead-reader", "value", 0}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    reader_pid =
      spawn(fn ->
        Process.put(
          :ferricstore_waraft_apply_projection_disk_read_hook,
          fn _root, ^index, :latest ->
            send(parent, {:dead_apply_projection_reader_entered, self()})
            Process.sleep(:infinity)
          end
        )

        WARaftSegmentReader.read_value_from_location(
          %{data_dir: data_dir},
          0,
          {:waraft_apply_projection, index},
          "dead-reader"
        )
      end)

    assert_receive {:dead_apply_projection_reader_entered, ^reader_pid}, 1_000
    reader_monitor = Process.monitor(reader_pid)
    Process.exit(reader_pid, :kill)
    assert_receive {:DOWN, ^reader_monitor, :process, ^reader_pid, :killed}, 1_000

    assert :ok =
             WARaftSegmentReader.with_apply_projection_disk_lock(data_dir, 0, fn -> :ok end)
  end

  test "apply projection disk latch backs off adaptively under contention" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-disk-lock-backoff-#{System.unique_integer([:positive])}"
      )

    parent = self()

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    holder =
      Task.async(fn ->
        WARaftSegmentReader.with_apply_projection_disk_lock(data_dir, 0, fn ->
          send(parent, {:apply_projection_disk_writer_held, self()})

          receive do
            :release_apply_projection_disk_writer -> :ok
          after
            2_000 -> raise "timed out waiting to release apply-projection disk writer"
          end
        end)
      end)

    assert_receive {:apply_projection_disk_writer_held, holder_pid}, 1_000

    waiter =
      Task.async(fn ->
        Process.put(
          :ferricstore_waraft_apply_projection_lock_backoff_hook,
          fn wait_ms -> send(parent, {:apply_projection_lock_backoff, wait_ms}) end
        )

        WARaftSegmentReader.with_apply_projection_disk_lock(data_dir, 0, fn -> :ok end)
      end)

    assert_receive {:apply_projection_lock_backoff, 1}, 1_000
    assert_receive {:apply_projection_lock_backoff, 2}, 1_000
    assert_receive {:apply_projection_lock_backoff, 4}, 1_000
    assert_receive {:apply_projection_lock_backoff, 8}, 1_000

    send(holder_pid, :release_apply_projection_disk_writer)
    assert :ok = Task.await(holder, 2_000)
    assert :ok = Task.await(waiter, 2_000)
  end

  test "corrupt durable apply projections never satisfy replay dependencies" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-corrupt-projection-#{System.unique_integer([:positive])}"
      )

    index = 91

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"flow:value", "payload", 0}
             ])

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    [segment_path] =
      Path.wildcard(
        Path.join([
          data_dir,
          "waraft",
          "ferricstore_waraft_backend.1",
          "apply_projection_log",
          "segment_log",
          "*.seg"
        ])
      )

    File.write!(segment_path, "corrupt")

    refute WARaftSegmentReader.apply_projection_dependency_ready?(data_dir, 0, index)
  end

  test "apply projection byte accounting tracks inserts replacements deletes spills and clears" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-accounting-#{System.unique_integer([:positive])}"
      )

    index = 103
    large_value = :binary.copy("a", 80_000)
    replacement = :binary.copy("b", 96_000)
    second_value = :binary.copy("c", 72_000)

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"first", large_value, 0},
               {"second", second_value, 0}
             ])

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 2

    assert WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) ==
             byte_size(large_value) + byte_size(second_value)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {"first", replacement, 0}
             ])

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 2

    assert WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) ==
             byte_size(replacement) + byte_size(second_value)

    assert WARaftSegmentReader.delete_apply_projection_entries(data_dir, 0, [
             {index, "second"}
           ]) == 1

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1
    assert WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) == byte_size(replacement)

    assert {:ok, 1} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)
    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0
    assert WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) == 0

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index + 1, [
               {"after-spill", large_value, 0}
             ])

    assert WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0) == 1
    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0
    assert WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) == 0
  end

  test "spill deletion serializes with replacement accounting" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-accounting-race-#{System.unique_integer([:positive])}"
      )

    index = 109
    key = "shared"
    original = :binary.copy("o", 80_000)
    replacement = :binary.copy("r", 96_000)

    previous_hook =
      Application.get_env(:ferricstore, :waraft_apply_projection_cache_mutation_hook)

    on_exit(fn ->
      if is_nil(previous_hook) do
        Application.delete_env(:ferricstore, :waraft_apply_projection_cache_mutation_hook)
      else
        Application.put_env(
          :ferricstore,
          :waraft_apply_projection_cache_mutation_hook,
          previous_hook
        )
      end

      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {key, original, 0}
             ])

    parent = self()

    Application.put_env(:ferricstore, :waraft_apply_projection_cache_mutation_hook, fn
      :after_upsert, %{index: ^index} ->
        send(parent, {:replacement_upserted, self()})

        receive do
          :release_replacement_accounting -> :ok
        after
          5_000 -> :ok
        end

      :before_spill_delete_lock, _metadata ->
        send(parent, {:spill_waiting_for_cache_lock, self()})
        :ok

      _phase, _metadata ->
        :ok
    end)

    replacement_task =
      Task.async(fn ->
        WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
          {key, replacement, 0}
        ])
      end)

    assert_receive {:replacement_upserted, replacement_pid}, 1_000

    spill_task =
      Task.async(fn ->
        WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)
      end)

    assert_receive {:spill_waiting_for_cache_lock, _spill_pid}, 1_000
    assert Task.yield(spill_task, 50) == nil

    send(replacement_pid, :release_replacement_accounting)
    assert Task.await(replacement_task, 1_000) == :ok
    assert Task.await(spill_task, 1_000) == {:ok, 1}

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0
    assert WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) == 0

    assert {:ok, ^replacement} =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               {:waraft_apply_projection, index},
               key
             )
  end

  test "missing counter rebuild serializes with the first cache insert" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-projection-counter-race-#{System.unique_integer([:positive])}"
      )

    index = 113
    value = :binary.copy("v", 80_000)

    previous_hook =
      Application.get_env(:ferricstore, :waraft_apply_projection_cache_mutation_hook)

    on_exit(fn ->
      if is_nil(previous_hook) do
        Application.delete_env(:ferricstore, :waraft_apply_projection_cache_mutation_hook)
      else
        Application.put_env(
          :ferricstore,
          :waraft_apply_projection_cache_mutation_hook,
          previous_hook
        )
      end

      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    rebuild_calls = :atomics.new(1, signed: false)
    parent = self()

    Application.put_env(:ferricstore, :waraft_apply_projection_cache_mutation_hook, fn
      :before_counter_rebuild, %{kind: :count, value: 0} ->
        case :atomics.add_get(rebuild_calls, 1, 1) do
          1 ->
            send(parent, {:counter_rebuild_scanned, self()})

            receive do
              :release_counter_rebuild -> :ok
            after
              5_000 -> :ok
            end

          _later_rebuild ->
            :ok
        end

      _phase, _metadata ->
        :ok
    end)

    count_task =
      Task.async(fn ->
        WARaftSegmentReader.apply_projection_cache_count(data_dir, 0)
      end)

    assert_receive {:counter_rebuild_scanned, counter_pid}, 1_000

    put_task =
      Task.async(fn ->
        WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
          {"first", value, 0}
        ])
      end)

    assert Task.yield(put_task, 50) == nil
    send(counter_pid, :release_counter_rebuild)

    assert Task.await(count_task, 1_000) == 0
    assert Task.await(put_task, 1_000) == :ok
    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1
    assert WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) == byte_size(value)
  end

  test "recorded projection locations fail closed when their record disappears" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "waraft-corrupt-recorded-projection-#{System.unique_integer([:positive])}"
      )

    position_index = 117
    projection_index = 1

    projection_root =
      Path.join([
        data_dir,
        "waraft",
        "ferricstore_waraft_backend.1",
        "segment_projection_log"
      ])

    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection(
               to_charlist(projection_root),
               {:raft_log_pos, position_index, 0},
               [{"flow:key", "payload", 0}]
             )

    assert {:ok, "payload"} =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               {:waraft_projection, projection_index},
               "flow:key"
             )

    [segment_path] = Path.wildcard(Path.join([projection_root, "segment_log", "*.seg"]))
    File.write!(segment_path, "corrupt")

    assert {:error, :projection_entry_missing_at_recorded_location} =
             WARaftSegmentReader.read_value_from_location(
               %{data_dir: data_dir},
               0,
               {:waraft_projection, projection_index},
               "flow:key"
             )
  end
end
