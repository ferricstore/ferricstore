defmodule Ferricstore.Raft.WARaftSpikeTest do
  use ExUnit.Case, async: false

  @moduletag :waraft_spike

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-spike-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    File.mkdir_p!(root)

    on_exit(fn ->
      :ferricstore_waraft_spike.stop()
      File.rm_rf!(root)
    end)

    %{root: String.to_charlist(root)}
  end

  test "one-shard WARaft adapter can bootstrap, SET, and GET", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start(root)
    assert :ok = :ferricstore_waraft_spike.put("k1", "v1")
    assert {:ok, "v1"} = :ferricstore_waraft_spike.get("k1")
  end

  test "volatile mode supports async pipelined SET replies", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    for i <- 1..50 do
      assert :ok = :ferricstore_waraft_spike.put_async({:put, i}, "k#{i}", "v#{i}")
    end

    for i <- 1..50 do
      assert_receive {{:put, ^i}, :ok}, 5_000
    end

    assert {:ok, "v50"} = :ferricstore_waraft_spike.get("k50")
  end

  test "async commit replies are correlated after local apply", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    assert :ok = :ferricstore_waraft_spike.put_async({:apply, 1}, "applied:k1", "v1")
    assert_receive {{:apply, 1}, :ok}, 5_000
    assert {:ok, "v1"} = :ferricstore_waraft_spike.storage_get("applied:k1")
  end

  test "async commit replies carry backpressure errors", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)
    Application.put_env(:ferricstore, :raft_max_pending_low_priority_commits, 0)

    assert :ok = :ferricstore_waraft_spike.put_async(:backpressure, "blocked:k1", "v1")
    assert_receive {:backpressure, {:error, :commit_queue_full}}, 5_000
    assert :not_found = :ferricstore_waraft_spike.storage_get("blocked:k1")
  end

  test "volatile mode supports one async Raft command for a SET batch", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    entries =
      for i <- 1..50 do
        {"batch:k#{i}", "v#{i}"}
      end

    assert :ok = :ferricstore_waraft_spike.put_many_async(:batch, entries)
    assert_receive {:batch, :ok}, 5_000

    assert {:ok, "v1"} = :ferricstore_waraft_spike.get("batch:k1")
    assert {:ok, "v50"} = :ferricstore_waraft_spike.get("batch:k50")
  end

  test "spike config keeps replication and apply batches large enough for pipelined writes", %{
    root: root
  } do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    assert Application.get_env(:ferricstore, :raft_commit_batch_max) >= 1_024
    assert Application.get_env(:ferricstore, :raft_max_pending_reads) >= 100_000
    assert Application.get_env(:ferricstore, :raft_max_log_entries_per_heartbeat) >= 1_024
    assert Application.get_env(:ferricstore, :raft_max_heartbeat_size) >= 16 * 1024 * 1024
    assert Application.get_env(:ferricstore, :raft_apply_log_batch_size) >= 1_024
    assert Application.get_env(:ferricstore, :raft_apply_batch_max_bytes) >= 16 * 1024 * 1024
  end

  test "WARaft exposes the feature hooks required before a migration decision" do
    assert {:module, :wa_raft_log} = Code.ensure_loaded(:wa_raft_log)
    assert {:module, :wa_raft_storage} = Code.ensure_loaded(:wa_raft_storage)
    assert {:module, :wa_raft_server} = Code.ensure_loaded(:wa_raft_server)
    assert {:module, :wa_raft_queue} = Code.ensure_loaded(:wa_raft_queue)
    assert {:module, :wa_raft_part_sup} = Code.ensure_loaded(:wa_raft_part_sup)

    log_callbacks = :wa_raft_log.behaviour_info(:callbacks)
    assert {:append, 4} in log_callbacks
    assert {:fold_binary, 6} in log_callbacks
    assert {:trim, 3} in log_callbacks
    assert {:truncate, 3} in log_callbacks

    storage_callbacks = :wa_raft_storage.behaviour_info(:callbacks)
    assert {:storage_apply, 3} in storage_callbacks
    assert {:storage_apply_config, 3} in storage_callbacks
    assert {:storage_create_snapshot, 2} in storage_callbacks
    assert {:storage_open_snapshot, 3} in storage_callbacks

    assert function_exported?(:wa_raft_server, :adjust_membership, 3)
    assert function_exported?(:wa_raft_server, :adjust_membership, 4)
    assert function_exported?(:wa_raft_server, :snapshot_available, 3)
    assert function_exported?(:wa_raft_acceptor, :commit_async, 4)
    assert function_exported?(:wa_raft_queue, :commit_queue_full, 3)
    assert function_exported?(:wa_raft_queue, :apply_queue_full, 2)
    assert function_exported?(:wa_raft_part_sup, :prepare_spec, 2)
  end

  test "volatile mode can run the local batched load driver", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    assert {:ok, result} = :ferricstore_waraft_spike_load.run(1_000, 10, 10, 16, 100)
    assert %{ops: 1_000, elapsed_us: elapsed_us} = result
    assert elapsed_us > 0

    assert {:ok, value} = :ferricstore_waraft_spike.get(<<"bench:k1000">>)
    assert value == :binary.copy("x", 16)
  end

  test "volatile mode can run the mixed GET and SET load driver", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    assert {:ok, result} = :ferricstore_waraft_spike_load.run_mixed(1_000, 10, 10, 16, 100)
    assert %{ops: 1_000, reads: reads, writes: writes, elapsed_us: elapsed_us} = result
    assert reads > 0
    assert writes > 0
    assert reads + writes == 1_000
    assert elapsed_us > 0

    assert {:ok, value} = :ferricstore_waraft_spike.get(<<"mixed:set:k1000">>)
    assert value == :binary.copy("x", 16)
  end

  test "volatile mode can run multiple WARaft partitions", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_multi_volatile(root, 2)

    assert :ok = :ferricstore_waraft_spike.put_on(1, "partition:k1", "v1")
    assert :ok = :ferricstore_waraft_spike.put_on(2, "partition:k2", "v2")

    assert :ok =
             :ferricstore_waraft_spike.put_many_async_on(:partition_batch, 2, [
               {"partition:k3", "v3"}
             ])

    assert_receive {:partition_batch, :ok}, 5_000

    assert {:ok, "v1"} = :ferricstore_waraft_spike.get_on(1, "partition:k1")
    assert {:ok, "v2"} = :ferricstore_waraft_spike.get_on(2, "partition:k2")
    assert {:ok, "v3"} = :ferricstore_waraft_spike.storage_get_on(2, "partition:k3")
    assert :not_found = :ferricstore_waraft_spike.storage_get_on(1, "partition:k2")
    assert :not_found = :ferricstore_waraft_spike.storage_get_on(2, "partition:k1")
  end

  test "volatile mode can run the multi-partition load driver", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_multi_volatile(root, 2)

    assert {:ok, result} = :ferricstore_waraft_spike_load.run_multi(1_000, 10, 10, 16, 100, 2)
    assert %{ops: 1_000, elapsed_us: elapsed_us} = result
    assert elapsed_us > 0

    assert {:ok, value} = :ferricstore_waraft_spike.storage_get_on(1, <<"bench:p1:k999">>)
    assert value == :binary.copy("x", 16)
    assert {:ok, value} = :ferricstore_waraft_spike.storage_get_on(2, <<"bench:p2:k1000">>)
    assert value == :binary.copy("x", 16)
  end

  test "multi-partition mode can use the custom segment log", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_multi_volatile_segment_log(root, 2)

    assert :ok = :ferricstore_waraft_spike.put_on(1, "segment:p1", "v1")
    assert :ok = :ferricstore_waraft_spike.put_on(2, "segment:p2", "v2")
    assert {:ok, "v1"} = :ferricstore_waraft_spike.get_on(1, "segment:p1")
    assert {:ok, "v2"} = :ferricstore_waraft_spike.get_on(2, "segment:p2")

    segment_dirs =
      for partition <- 1..2 do
        Path.join(List.to_string(root), "ferricstore_waraft_spike.#{partition}/segment_log")
      end

    assert Enum.all?(segment_dirs, fn dir -> [_ | _] = Path.wildcard(Path.join(dir, "*.seg")) end)
  end

  test "acknowledged SET survives adapter restart", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start(root)
    assert :ok = :ferricstore_waraft_spike.put("durable", "value")
    assert {:ok, "value"} = :ferricstore_waraft_spike.get("durable")

    assert :ok = :ferricstore_waraft_spike.stop()
    assert :ok = :ferricstore_waraft_spike.start(root)
    assert {:ok, "value"} = :ferricstore_waraft_spike.get("durable")
  end

  test "custom durable segment log provider persists across restart", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("logged", "value")

    status = :ferricstore_waraft_spike.status()
    assert Keyword.get(status, :log_module) == :ferricstore_waraft_spike_segment_log
    assert Keyword.get(status, :log_last) >= 2

    assert :ok = :ferricstore_waraft_spike.stop()
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)

    restarted = :ferricstore_waraft_spike.status()
    assert Keyword.get(restarted, :log_module) == :ferricstore_waraft_spike_segment_log
    assert Keyword.get(restarted, :log_last) >= Keyword.get(status, :log_last)
    assert {:ok, "value"} = :ferricstore_waraft_spike.get("logged")

    segment_dir = Path.join(List.to_string(root), "ferricstore_waraft_spike.1/segment_log")
    assert [_ | _] = Path.wildcard(Path.join(segment_dir, "*.seg"))
  end

  test "custom durable segment log emits telemetry on crc corruption", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("corrupt:logged", "value")
    assert :ok = :ferricstore_waraft_spike.stop()

    path = corrupt_first_segment_payload!(root)
    parent = self()
    handler = {:waraft_segment_log_corrupt, self(), make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :segment_log_corrupt],
      &__MODULE__.handle_segment_log_corrupt/4,
      parent
    )

    try do
      assert {:error, _reason} = :ferricstore_waraft_spike.start_segment_log(root)

      assert_receive {:segment_log_corrupt, [:ferricstore, :waraft, :segment_log_corrupt],
                      %{count: 1}, %{path: ^path, reason: {:crc_mismatch, _offset}}},
                     1_000
    after
      :telemetry.detach(handler)
    end
  end

  test "custom durable segment log rejects impossible record lengths as corruption", %{
    root: root
  } do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("corrupt:length", "value")
    assert :ok = :ferricstore_waraft_spike.stop()

    path = corrupt_first_segment_length!(root)
    parent = self()
    handler = {:waraft_segment_log_bad_length, self(), make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :segment_log_corrupt],
      &__MODULE__.handle_segment_log_corrupt/4,
      parent
    )

    try do
      assert {:error, _reason} = :ferricstore_waraft_spike.start_segment_log(root)

      assert_receive {:segment_log_corrupt, [:ferricstore, :waraft, :segment_log_corrupt],
                      %{count: 1}, %{path: ^path, reason: {:record_too_large, 0, _len}}},
                     1_000
    after
      :telemetry.detach(handler)
    end
  end

  test "custom durable segment log rejects duplicate recovered indexes as corruption", %{
    root: root
  } do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("corrupt:duplicate", "value")
    assert :ok = :ferricstore_waraft_spike.stop()

    path = overwrite_first_segment_with_duplicate_indexes!(root)
    parent = self()
    handler = {:waraft_segment_log_duplicate, self(), make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :segment_log_corrupt],
      &__MODULE__.handle_segment_log_corrupt/4,
      parent
    )

    try do
      assert {:error, _reason} = :ferricstore_waraft_spike.start_segment_log(root)

      assert_receive {:segment_log_corrupt, [:ferricstore, :waraft, :segment_log_corrupt],
                      %{count: 1}, %{path: ^path, reason: {:duplicate_record_index, 2}}},
                     1_000
    after
      :telemetry.detach(handler)
    end
  end

  test "custom durable segment log rejects gaps in recovered indexes as corruption", %{
    root: root
  } do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("corrupt:gap:1", "value")
    assert :ok = :ferricstore_waraft_spike.put("corrupt:gap:2", "value")
    assert :ok = :ferricstore_waraft_spike.stop()

    path = overwrite_first_segment_with_index_gap!(root)
    rewind_spike_storage!(root, ["corrupt:gap:1", "corrupt:gap:2"], {:raft_log_pos, 1, 1})

    parent = self()
    handler = {:waraft_segment_log_gap, self(), make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :segment_log_corrupt],
      &__MODULE__.handle_segment_log_corrupt/4,
      parent
    )

    try do
      assert {:error, _reason} = :ferricstore_waraft_spike.start_segment_log(root)

      assert_receive {:segment_log_corrupt, [:ferricstore, :waraft, :segment_log_corrupt],
                      %{count: 1}, %{path: ^path, reason: {:non_contiguous_record_index, 1, 3}}},
                     1_000
    after
      :telemetry.detach(handler)
    end
  end

  test "custom durable segment log emits telemetry on bad segment filenames", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("bad-segment-name:k1", "v1")
    assert :ok = :ferricstore_waraft_spike.stop()

    segment_dir = segment_log_dir(root)
    [path | _] = Path.wildcard(Path.join(segment_dir, "*.seg")) |> Enum.sort()
    bad_path = Path.join(segment_dir, "not-a-number.seg")
    File.rename!(path, bad_path)

    handler = {:waraft_segment_log_bad_filename, self(), make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :segment_log_corrupt],
      &__MODULE__.handle_segment_log_corrupt/4,
      self()
    )

    try do
      assert {:error, _reason} = :ferricstore_waraft_spike.start_segment_log(root)

      assert_receive {:segment_log_corrupt, [:ferricstore, :waraft, :segment_log_corrupt],
                      %{count: 1}, %{path: ^segment_dir, reason: reason}},
                     1_000

      assert {:bad_segment_filename, filename} = reason
      assert to_string(filename) == "not-a-number.seg"
    after
      :telemetry.detach(handler)
    end
  end

  test "custom durable segment log recovers double-digit segment files in numeric order", %{
    root: root
  } do
    table = :"ferricstore_waraft_segment_sort_#{System.unique_integer([:positive])}"
    partition = 1
    log_name = :wa_raft_log.registered_name(table, partition)

    log =
      {:raft_log, log_name, :ferricstore, table, partition, :ferricstore_waraft_spike_segment_log}

    raft_root = Path.join(List.to_string(root), "numeric-segment-sort")
    previous_database = Application.get_env(:ferricstore, :raft_database)
    previous_waraft_database = Application.get_env(:wa_raft, :raft_database)

    try do
      Application.put_env(:ferricstore, :raft_database, String.to_charlist(raft_root))
      Application.put_env(:wa_raft, :raft_database, String.to_charlist(raft_root))

      :wa_raft_part_sup.prepare_spec(:ferricstore, %{
        table: table,
        partition: partition,
        log_module: :ferricstore_waraft_spike_segment_log
      })

      assert :ok = :ferricstore_waraft_spike_segment_log.init(log)

      segment_dir = Path.join(raft_root, "#{table}.#{partition}/segment_log")
      write_segment_files_through_double_digits!(segment_dir)

      assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.open(log)
      assert 1 = :ferricstore_waraft_spike_segment_log.first_index(log)
      assert 40_960 = :ferricstore_waraft_spike_segment_log.last_index(log)
    after
      restore_env(:raft_database, previous_database)
      restore_app_env(:wa_raft, :raft_database, previous_waraft_database)

      if :ets.info(log_name) != :undefined do
        :ets.delete(log_name)
      end
    end
  end

  test "custom durable segment log clears partial recovery after later corruption", %{
    root: root
  } do
    table = :"ferricstore_waraft_segment_partial_#{System.unique_integer([:positive])}"
    partition = 1
    log_name = :wa_raft_log.registered_name(table, partition)

    log =
      {:raft_log, log_name, :ferricstore, table, partition, :ferricstore_waraft_spike_segment_log}

    raft_root = Path.join(List.to_string(root), "partial-segment-recovery")
    previous_database = Application.get_env(:ferricstore, :raft_database)
    previous_waraft_database = Application.get_env(:wa_raft, :raft_database)

    try do
      Application.put_env(:ferricstore, :raft_database, String.to_charlist(raft_root))
      Application.put_env(:wa_raft, :raft_database, String.to_charlist(raft_root))

      :wa_raft_part_sup.prepare_spec(:ferricstore, %{
        table: table,
        partition: partition,
        log_module: :ferricstore_waraft_spike_segment_log
      })

      assert :ok = :ferricstore_waraft_spike_segment_log.init(log)

      segment_dir = Path.join(raft_root, "#{table}.#{partition}/segment_log")
      File.mkdir_p!(segment_dir)
      File.write!(Path.join(segment_dir, "0.seg"), encode_segment_record({1, {1, :noop}}))
      File.write!(Path.join(segment_dir, "1.seg"), <<0xFFFF_FFFF::32, 0::32>>)

      assert {:error, _reason} = :ferricstore_waraft_spike_segment_log.open(log)
      assert :undefined = :ferricstore_waraft_spike_segment_log.first_index(log)
      assert :undefined = :ferricstore_waraft_spike_segment_log.last_index(log)
    after
      restore_env(:raft_database, previous_database)
      restore_app_env(:wa_raft, :raft_database, previous_waraft_database)

      if :ets.info(log_name) != :undefined do
        :ets.delete(log_name)
      end
    end
  end

  test "custom durable segment log decodes recovered records safely", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("safe-decode:logged", "value")
    assert :ok = :ferricstore_waraft_spike.stop()

    atom_name = "ferricstore_waraft_corrupt_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    path = overwrite_first_segment_with_unknown_atom_record!(root, atom_name)
    parent = self()
    handler = {:waraft_segment_log_safe_decode, self(), make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :segment_log_corrupt],
      &__MODULE__.handle_segment_log_corrupt/4,
      parent
    )

    try do
      assert {:error, _reason} = :ferricstore_waraft_spike.start_segment_log(root)
      refute existing_atom?(atom_name)

      assert_receive {:segment_log_corrupt, [:ferricstore, :waraft, :segment_log_corrupt],
                      %{count: 1}, %{path: ^path, reason: {:bad_term, _reason}}},
                     1_000
    after
      :telemetry.detach(handler)
    end
  end

  test "custom durable segment log rejects unsafe binary append entries", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)

    atom_name = "ferricstore_waraft_append_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    status = :ferricstore_waraft_spike.status()
    view = segment_log_view(status)

    assert {:error, {:bad_entry_term, _reason}} =
             :wa_raft_log.append(view, [unknown_atom_payload(atom_name)])

    refute existing_atom?(atom_name)
  end

  test "default WARaft ETS log rejects unsafe binary append entries", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    atom_name = "ferricstore_waraft_ets_append_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    status = :ferricstore_waraft_spike.status()
    view = waraft_log_view(status)

    assert {:error, {:bad_entry_term, _reason}} =
             :wa_raft_log.append(view, [unknown_atom_payload(atom_name)])

    refute existing_atom?(atom_name)
  end

  test "WARaft heartbeat comparison decodes binary entries safely", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)

    atom_name = "ferricstore_waraft_heartbeat_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    status = :ferricstore_waraft_spike.status()
    view = segment_log_view(status)

    assert {:error, {:bad_entry_term, _reason}} =
             :wa_raft_log.check_heartbeat(
               view,
               Keyword.fetch!(status, :log_last),
               [unknown_atom_payload(atom_name)]
             )

    refute existing_atom?(atom_name)
  end

  test "custom durable segment log rolls back bytes when append fsync boundary fails", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      last_before = Keyword.fetch!(status, :log_last)
      view = segment_log_view(status)

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_append_hook,
        {:fail_once_after_write, self()}
      )

      entry =
        {Keyword.fetch!(status, :current_term),
         {make_ref(), {:write, :ferricstore_waraft_spike, "unacked:append", "value"}}}

      assert {:error, {:append_hook, :after_write}} = :wa_raft_log.append(view, [entry])
      assert_receive {:waraft_segment_log_append_hook, :after_write}, 1_000

      assert :ok = :ferricstore_waraft_spike.stop()
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert :not_found = :ferricstore_waraft_spike.storage_get("unacked:append")
      assert Keyword.fetch!(:ferricstore_waraft_spike.status(), :log_last) <= last_before + 1
    after
      restore_env(:waraft_segment_log_append_hook, previous_hook)
    end
  end

  test "custom durable segment log rolls back all segment files in a failed split append", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      filler_count = max(0, 4094 - Keyword.fetch!(status, :log_last))

      filler_entries = for _ <- 1..filler_count, do: {term, {make_ref(), :noop}}
      view = append_entries!(view, filler_entries)
      assert log_view_last(view) == 4094

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_append_hook,
        {:fail_after_write_count, 2, self()}
      )

      entries = [
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "split:unacked:1", "v1"}}},
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "split:unacked:2", "v2"}}}
      ]

      assert {:error, {:append_hook, :after_write, 2}} = :wa_raft_log.append(view, entries)
      assert_receive {:waraft_segment_log_append_hook, :after_write, 1}, 1_000
      assert_receive {:waraft_segment_log_append_hook, :after_write, 2}, 1_000

      assert :ok = :ferricstore_waraft_spike.stop()
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert :not_found = :ferricstore_waraft_spike.storage_get("split:unacked:1")
      assert :not_found = :ferricstore_waraft_spike.storage_get("split:unacked:2")
    after
      restore_env(:waraft_segment_log_append_hook, previous_hook)
    end
  end

  test "custom durable segment log fails closed when split append rollback fails", %{
    root: root
  } do
    previous_append_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)
    previous_rollback_hook = Application.get_env(:ferricstore, :waraft_segment_log_rollback_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      filler_count = max(0, 4094 - Keyword.fetch!(status, :log_last))

      filler_entries = for _ <- 1..filler_count, do: {term, {make_ref(), :noop}}
      view = append_entries!(view, filler_entries)
      assert log_view_last(view) == 4094

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_append_hook,
        {:fail_after_write_count, 2, self()}
      )

      Application.put_env(:ferricstore, :waraft_segment_log_rollback_hook, {:fail_once, self()})

      entries = [
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "split:poisoned:1", "v1"}}},
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "split:poisoned:2", "v2"}}}
      ]

      assert {:error,
              {:rollback_failed, {:append_hook, :after_write, 2}, {:rollback_hook, _segment_path}}} =
               :wa_raft_log.append(view, entries)

      assert_receive {:waraft_segment_log_append_hook, :after_write, 1}, 1_000
      assert_receive {:waraft_segment_log_append_hook, :after_write, 2}, 1_000
      assert_receive {:waraft_segment_log_rollback_hook, _segment_path}, 1_000

      poison_marker = Path.join(segment_log_dir(root), "segment_log.append_failed.term")
      assert File.exists?(poison_marker)

      assert :ok = :ferricstore_waraft_spike.stop()

      assert {:error, restart_error} = :ferricstore_waraft_spike.start_segment_log(root)
      assert inspect(restart_error) =~ "segment_log_poisoned"
    after
      restore_env(:waraft_segment_log_append_hook, previous_append_hook)
      restore_env(:waraft_segment_log_rollback_hook, previous_rollback_hook)
    end
  end

  test "custom durable segment log rejects oversized append failure marker before decode", %{
    root: root
  } do
    segment_dir = segment_log_dir(root)
    File.mkdir_p!(segment_dir)

    marker_path = Path.join(segment_dir, "segment_log.append_failed.term")
    File.write!(marker_path, :binary.copy("x", 1_048_577))

    assert {:error, reason} = :ferricstore_waraft_spike.start_segment_log(root)
    assert inspect(reason) =~ "append_failure_marker_file_too_large"
    assert File.exists?(marker_path)
  end

  test "custom durable segment log writes split append groups in numeric segment order", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      {:log_view, log, first, _last, config} = segment_log_view(status)
      view = {:log_view, log, first, 40_958, config}

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_append_hook,
        {:fail_after_write_count, 1, self()}
      )

      entries = [
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment-order:9", "v9"}}},
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment-order:10", "v10"}}}
      ]

      assert {:error, {:append_hook, :after_write, 1}} = :wa_raft_log.append(view, entries)
      assert_receive {:waraft_segment_log_append_hook, :after_write, 1}, 1_000

      segment_dir = segment_log_dir(root)
      assert File.exists?(Path.join(segment_dir, "9.seg"))
      refute File.exists?(Path.join(segment_dir, "10.seg"))
    after
      restore_env(:waraft_segment_log_append_hook, previous_hook)
    end
  end

  test "custom durable segment log fsyncs parent directory when append creates a segment", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_sync_dir_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      filler_count = max(0, 4095 - Keyword.fetch!(status, :log_last))

      filler_entries = for _ <- 1..filler_count, do: {term, {make_ref(), :noop}}
      view = append_entries!(view, filler_entries)
      assert log_view_last(view) == 4095

      Application.put_env(:ferricstore, :waraft_segment_log_sync_dir_hook, {:notify, self()})

      entry = {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:new", "value"}}}
      assert {:ok, _new_view} = :wa_raft_log.append(view, [entry])

      segment_dir = segment_log_dir(root)
      assert_receive {:waraft_segment_log_sync_dir, ^segment_dir}, 1_000
    after
      restore_env(:waraft_segment_log_sync_dir_hook, previous_hook)
    end
  end

  test "custom durable segment log fsyncs appended segment before append returns", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      segment_dir = segment_log_dir(root)

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:notify, self()})

      entry =
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:sync", "value"}}}

      assert {:ok, _new_view} = :wa_raft_log.append(view, [entry])
      assert_receive {:waraft_segment_log_file_sync, path}, 1_000
      assert String.starts_with?(path, segment_dir)
      assert Path.extname(path) == ".seg"
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "custom durable segment log rolls back bytes when append file fsync fails", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      last_before = Keyword.fetch!(status, :log_last)
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      segment_dir = segment_log_dir(root)

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:fail_once, self()})

      entry =
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:fsync-failed", "value"}}}

      assert {:error, {:file_sync_hook, path}} = :wa_raft_log.append(view, [entry])
      assert String.starts_with?(path, segment_dir)
      assert_receive {:waraft_segment_log_file_sync, ^path}, 1_000

      assert :ok = :ferricstore_waraft_spike.stop()
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert :not_found = :ferricstore_waraft_spike.storage_get("segment:fsync-failed")
      assert Keyword.fetch!(:ferricstore_waraft_spike.status(), :log_last) <= last_before + 1
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "custom durable segment log fsyncs one same-segment append group once", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:notify, self()})

      entries = [
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:group:1", "v1"}}},
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:group:2", "v2"}}}
      ]

      assert {:ok, _new_view} = :wa_raft_log.append(view, entries)
      assert_receive {:waraft_segment_log_file_sync, _path}, 1_000
      refute_receive {:waraft_segment_log_file_sync, _path}, 100
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "custom durable segment log rejects new segment when directory fsync fails", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_sync_dir_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      filler_count = max(0, 4095 - Keyword.fetch!(status, :log_last))

      filler_entries = for _ <- 1..filler_count, do: {term, {make_ref(), :noop}}
      view = append_entries!(view, filler_entries)
      assert log_view_last(view) == 4095

      segment_dir = segment_log_dir(root)
      Application.put_env(:ferricstore, :waraft_segment_log_sync_dir_hook, {:fail_once, self()})

      entry =
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:rejected", "value"}}}

      assert {:error, {:sync_new_segment_dir, {:sync_dir_hook, ^segment_dir}}} =
               :wa_raft_log.append(view, [entry])

      assert_receive {:waraft_segment_log_sync_dir, ^segment_dir}, 1_000

      {:log_view, log, _first, _last, _config} = view
      assert :not_found = :ferricstore_waraft_spike_segment_log.get(log, 4096)

      assert :ok = :ferricstore_waraft_spike.stop()
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert :not_found = :ferricstore_waraft_spike.storage_get("segment:rejected")

      recovered_view = segment_log_view(:ferricstore_waraft_spike.status())
      {:log_view, recovered_log, _first, recovered_last, _config} = recovered_view

      if recovered_last >= 4096 do
        case :ferricstore_waraft_spike_segment_log.get(recovered_log, 4096) do
          {:ok, {^term, {_ref, {:write, :ferricstore_waraft_spike, "segment:rejected", "value"}}}} ->
            flunk("recovered the rejected segment append")

          _other ->
            :ok
        end
      end
    after
      restore_env(:waraft_segment_log_sync_dir_hook, previous_hook)
    end
  end

  test "custom durable segment log trims persisted records after apply rotation", %{root: root} do
    previous_interval = Application.get_env(:ferricstore, :raft_max_log_records_per_file)
    previous_keep = Application.get_env(:ferricstore, :raft_max_log_records)

    try do
      Application.put_env(:ferricstore, :raft_max_log_records_per_file, 1)
      Application.put_env(:ferricstore, :raft_max_log_records, 0)

      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)

      for i <- 1..8 do
        assert :ok = :ferricstore_waraft_spike.put("trim:k#{i}", "v#{i}")
      end

      assert eventually(fn ->
               first_persisted_segment_index(root) > 1
             end)

      assert :ok = :ferricstore_waraft_spike.stop()
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert {:ok, "v8"} = :ferricstore_waraft_spike.get("trim:k8")
      assert first_persisted_segment_index(root) > 1
    after
      restore_env(:raft_max_log_records_per_file, previous_interval)
      restore_env(:raft_max_log_records, previous_keep)
    end
  end

  test "custom durable segment log recovers an interrupted trim rewrite", %{root: root} do
    previous_interval = Application.get_env(:ferricstore, :raft_max_log_records_per_file)
    previous_keep = Application.get_env(:ferricstore, :raft_max_log_records)
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_rewrite_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert :ok = :ferricstore_waraft_spike.put("rewrite:seed", "v0")

      Application.put_env(:ferricstore, :raft_max_log_records_per_file, 1)
      Application.put_env(:ferricstore, :raft_max_log_records, 0)

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_rewrite_hook,
        {:fail_once_after_live_backup, self()}
      )

      hook_result =
        Enum.reduce_while(1..20, :pending, fn i, _acc ->
          _ = :ferricstore_waraft_spike.put("rewrite:k#{i}", "v#{i}")

          receive do
            {:waraft_segment_log_rewrite_hook, :after_live_backup} = message ->
              {:halt, message}
          after
            100 ->
              {:cont, :pending}
          end
        end)

      assert {:waraft_segment_log_rewrite_hook, :after_live_backup} = hook_result

      assert :ok = :ferricstore_waraft_spike.stop()
      rewind_spike_storage!(root, ["rewrite:seed"], {:raft_log_pos, 1, 1})

      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert {:ok, "v0"} = :ferricstore_waraft_spike.get("rewrite:seed")
    after
      restore_env(:raft_max_log_records_per_file, previous_interval)
      restore_env(:raft_max_log_records, previous_keep)
      restore_env(:waraft_segment_log_rewrite_hook, previous_hook)
    end
  end

  test "custom durable segment log startup rolls back an interrupted rewrite marker", %{
    root: root
  } do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("rewrite-marker:seed", "v0")
    assert :ok = :ferricstore_waraft_spike.stop()

    segment_dir = segment_log_dir(root)
    parent = Path.dirname(segment_dir)
    staging = Path.join(parent, "segment_log.rewrite.staging.manual")
    backup = Path.join(parent, "segment_log.rewrite.backup.manual")
    marker = Path.join(parent, "segment_log.rewrite.term")

    File.rm_rf!(staging)
    File.rm_rf!(backup)
    File.rename!(segment_dir, backup)
    File.mkdir_p!(staging)

    File.write!(
      marker,
      :erlang.term_to_binary(%{
        version: 1,
        dir: String.to_charlist(segment_dir),
        staging: String.to_charlist(staging),
        backup: String.to_charlist(backup)
      })
    )

    rewind_spike_storage!(root, ["rewrite-marker:seed"], {:raft_log_pos, 1, 1})

    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert {:ok, "v0"} = :ferricstore_waraft_spike.get("rewrite-marker:seed")
    assert File.dir?(segment_dir)
    refute File.exists?(staging)
    refute File.exists?(backup)
    refute File.exists?(marker)
  end

  test "custom durable segment log rejects trim rewrite when backup cleanup fsync fails", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_sync_dir_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert :ok = :ferricstore_waraft_spike.put("rewrite-cleanup:1", "v1")
      assert :ok = :ferricstore_waraft_spike.put("rewrite-cleanup:2", "v2")

      status = :ferricstore_waraft_spike.status()
      {:log_view, log, _first, _last, _config} = segment_log_view(status)
      parent = segment_log_dir(root) |> Path.dirname()

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_sync_dir_hook,
        {:fail_on_count, 7, self()}
      )

      assert {:error, {:sync_dir_hook, ^parent, 7}} =
               :ferricstore_waraft_spike_segment_log.trim(log, 2, %{})

      assert_receive {:waraft_segment_log_sync_dir, ^parent, 7}, 1_000
    after
      restore_env(:waraft_segment_log_sync_dir_hook, previous_hook)
    end
  end

  test "custom durable segment log rejects rewrite markers with paths outside log root", %{
    root: root
  } do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("rewrite-marker-path:seed", "v0")
    assert :ok = :ferricstore_waraft_spike.stop()

    segment_dir = segment_log_dir(root)
    parent = Path.dirname(segment_dir)
    marker = Path.join(parent, "segment_log.rewrite.term")
    outside_staging = Path.join(List.to_string(root), "outside_rewrite_staging")
    outside_backup = Path.join(List.to_string(root), "outside_rewrite_backup")

    File.mkdir_p!(outside_staging)
    File.mkdir_p!(outside_backup)
    File.write!(Path.join(outside_staging, "sentinel"), "keep")

    File.write!(
      marker,
      :erlang.term_to_binary(%{
        version: 1,
        dir: String.to_charlist(segment_dir),
        staging: String.to_charlist(outside_staging),
        backup: String.to_charlist(outside_backup)
      })
    )

    assert {:error, reason} = :ferricstore_waraft_spike.start_segment_log(root)
    assert inspect(reason) =~ "bad_rewrite_marker_path"
    assert File.exists?(Path.join(outside_staging, "sentinel"))
    assert File.dir?(outside_backup)
  end

  test "custom durable segment log replays entries when storage lags the log", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert :ok = :ferricstore_waraft_spike.put("replay:k1", "v1")
    assert :ok = :ferricstore_waraft_spike.put("replay:k2", "v2")
    assert {:ok, "v2"} = :ferricstore_waraft_spike.get("replay:k2")

    status = :ferricstore_waraft_spike.status()
    assert Keyword.fetch!(status, :log_last) >= 3

    assert :ok = :ferricstore_waraft_spike.stop()
    rewind_spike_storage!(root, ["replay:k1", "replay:k2"], {:raft_log_pos, 1, 1})

    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    assert {:ok, "v1"} = :ferricstore_waraft_spike.get("replay:k1")
    assert {:ok, "v2"} = :ferricstore_waraft_spike.get("replay:k2")
  end

  test "stalled member can install a WARaft snapshot", %{root: root} do
    source_root = Path.join(List.to_string(root), "source")
    target_root = Path.join(List.to_string(root), "target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    assert :ok = :ferricstore_waraft_spike.start_segment_log(String.to_charlist(source_root))
    assert :ok = :ferricstore_waraft_spike.put("snap:k1", "v1")
    assert {:ok, "v1"} = :ferricstore_waraft_spike.get("snap:k1")

    assert {:ok, {:raft_log_pos, index, term} = position} =
             :ferricstore_waraft_spike.create_snapshot()

    snapshot_path =
      Path.join([
        source_root,
        "ferricstore_waraft_spike.1",
        "snapshot.#{index}.#{term}"
      ])

    assert File.dir?(snapshot_path)
    assert :ok = :ferricstore_waraft_spike.stop()

    assert :ok =
             :ferricstore_waraft_spike.start_cluster_member_segment_log(
               String.to_charlist(target_root)
             )

    assert :ok =
             :ferricstore_waraft_spike.install_snapshot(
               String.to_charlist(snapshot_path),
               position
             )

    assert {:ok, "v1"} = :ferricstore_waraft_spike.storage_get("snap:k1")
  end

  @tag :cluster
  test "three peer nodes can bootstrap and commit a batched quorum write" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft cluster spike test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- nodes do
      assert :ok =
               :rpc.call(node.name, :ferricstore_waraft_spike, :start_cluster_member, [
                 String.to_charlist(node.data_dir)
               ])
    end

    for node <- names do
      assert :ok = :rpc.call(node, :ferricstore_waraft_spike, :bootstrap_cluster, [names])
    end

    assert :ok = :rpc.call(hd(names), :ferricstore_waraft_spike, :trigger_election, [])

    leader = wait_for_waraft_leader(names)

    assert :ok =
             :rpc.call(leader, :ferricstore_waraft_spike, :put_many, [
               [{"cluster:k1", "v1"}, {"cluster:k2", "v2"}]
             ])

    assert {:ok, "v1"} = :rpc.call(leader, :ferricstore_waraft_spike, :get, ["cluster:k1"])
  end

  @tag :cluster
  test "three peer nodes can commit through the custom segment log" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft cluster spike test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- nodes do
      assert :ok =
               :rpc.call(
                 node.name,
                 :ferricstore_waraft_spike,
                 :start_cluster_member_segment_log,
                 [
                   String.to_charlist(node.data_dir)
                 ]
               )
    end

    for node <- names do
      assert :ok = :rpc.call(node, :ferricstore_waraft_spike, :bootstrap_cluster, [names])
    end

    assert :ok = :rpc.call(hd(names), :ferricstore_waraft_spike, :trigger_election, [])

    leader = wait_for_waraft_leader(names)

    assert :ok =
             :rpc.call(leader, :ferricstore_waraft_spike, :put_many, [
               [{"cluster-segment:k1", "v1"}, {"cluster-segment:k2", "v2"}]
             ])

    assert {:ok, "v1"} =
             :rpc.call(leader, :ferricstore_waraft_spike, :get, ["cluster-segment:k1"])

    assert Enum.any?(nodes, fn node ->
             Path.wildcard(
               Path.join(node.data_dir, "ferricstore_waraft_spike.1/segment_log/*.seg")
             ) != []
           end)
  end

  @tag :cluster
  test "three peer nodes can commit a dynamic membership removal" do
    unless Ferricstore.Test.ClusterHelper.peer_available?() do
      flunk(":peer is required for WARaft cluster spike test")
    end

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_waraft_peers(unique, 3)

    on_exit(fn ->
      Enum.each(nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end)

    names = Enum.map(nodes, & &1.name)

    for left <- names, right <- names, left != right do
      :rpc.call(left, Node, :connect, [right])
    end

    for node <- nodes do
      assert :ok =
               :rpc.call(node.name, :ferricstore_waraft_spike, :start_cluster_member, [
                 String.to_charlist(node.data_dir)
               ])
    end

    for node <- names do
      assert :ok = :rpc.call(node, :ferricstore_waraft_spike, :bootstrap_cluster, [names])
    end

    assert :ok = :rpc.call(hd(names), :ferricstore_waraft_spike, :trigger_election, [])
    leader = wait_for_waraft_leader(names)
    removed = Enum.find(names, &(&1 != leader))
    removed_peer = {:raft_server_ferricstore_waraft_spike_1, removed}

    assert {:ok, {:raft_log_pos, _, _}} =
             :rpc.call(leader, :ferricstore_waraft_spike, :adjust_membership, [
               :remove,
               removed_peer
             ])

    assert eventually(fn ->
             membership = :rpc.call(leader, :ferricstore_waraft_spike, :membership, [])
             is_list(membership) and removed_peer not in membership
           end)
  end

  defp start_waraft_peers(unique, count) do
    code_paths = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    for i <- 1..count do
      name = :"waraft_spike_#{unique}_#{i}"
      data_dir = Path.join(System.tmp_dir!(), "ferricstore-waraft-peer-#{unique}-#{i}")
      File.rm_rf!(data_dir)
      File.mkdir_p!(data_dir)

      {:ok, peer, node_name} =
        :peer.start(%{
          name: name,
          args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
          wait_boot: 120_000
        })

      %{name: node_name, peer: peer, data_dir: data_dir}
    end
  end

  defp wait_for_waraft_leader(names, attempts \\ 100)
  defp wait_for_waraft_leader(_names, 0), do: flunk("WARaft leader was not elected")

  defp wait_for_waraft_leader(names, attempts) do
    case Enum.find(names, fn node ->
           case :rpc.call(node, :ferricstore_waraft_spike, :status, []) do
             status when is_list(status) -> Keyword.get(status, :state) == :leader
             _ -> false
           end
         end) do
      nil ->
        Process.sleep(50)
        wait_for_waraft_leader(names, attempts - 1)

      leader ->
        leader
    end
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(50)
      eventually(fun, attempts - 1)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
  defp restore_app_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_app_env(app, key, value), do: Application.put_env(app, key, value)

  def handle_segment_log_corrupt(event, measurements, metadata, parent) do
    send(parent, {:segment_log_corrupt, event, measurements, metadata})
  end

  defp first_persisted_segment_index(root) do
    root
    |> segment_records()
    |> Enum.map(fn {index, _entry} -> index end)
    |> Enum.min(fn -> 0 end)
  end

  defp segment_records(root) do
    root
    |> segment_log_dir()
    |> Path.join("*.seg")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.flat_map(fn path ->
      case File.read(path) do
        {:ok, binary} ->
          decode_segment_records(binary, [])

        {:error, :enoent} ->
          []
      end
    end)
  end

  defp segment_log_dir(root) do
    Path.join(List.to_string(root), "ferricstore_waraft_spike.1/segment_log")
  end

  defp corrupt_first_segment_payload!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    <<len::32, crc::32, payload::binary-size(len), tail::binary>> = File.read!(path)
    <<first, rest::binary>> = payload
    corrupted_payload = <<Bitwise.bxor(first, 1), rest::binary>>
    File.write!(path, <<len::32, crc::32, corrupted_payload::binary, tail::binary>>)
    path
  end

  defp corrupt_first_segment_length!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    <<_len::32, crc::32, rest::binary>> = File.read!(path)
    File.write!(path, <<0xFFFF_FFFF::32, crc::32, rest::binary>>)
    path
  end

  defp overwrite_first_segment_with_duplicate_indexes!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    records = [
      {1, {1, :noop}},
      {2, {1, :noop}},
      {2, {2, :noop}}
    ]

    File.write!(path, Enum.map(records, &encode_segment_record/1))
    path
  end

  defp overwrite_first_segment_with_index_gap!(root) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    records = [
      {1, {1, :noop}},
      {3, {1, :noop}}
    ]

    File.write!(path, Enum.map(records, &encode_segment_record/1))
    path
  end

  defp write_segment_files_through_double_digits!(segment_dir) do
    File.rm_rf!(segment_dir)
    File.mkdir_p!(segment_dir)
    write_segment_config_fixture!(segment_dir, 4096)

    for segment <- 0..10 do
      start_index = max(1, segment * 4096)
      end_index = min(40_960, (segment + 1) * 4096 - 1)

      if start_index <= end_index do
        path = Path.join(segment_dir, "#{segment}.seg")

        File.open!(path, [:write, :binary], fn file ->
          for index <- start_index..end_index do
            IO.binwrite(file, encode_segment_record({index, {1, :noop}}))
          end
        end)
      end
    end
  end

  defp write_segment_config_fixture!(segment_dir, records_per_segment) do
    payload =
      :erlang.term_to_binary(%{
        version: 1,
        records_per_segment: records_per_segment
      })

    File.write!(Path.join(segment_dir, "segment_config.term"), payload)
  end

  defp overwrite_first_segment_with_unknown_atom_record!(root, atom_name) do
    path =
      root
      |> segment_log_dir()
      |> Path.join("*.seg")
      |> Path.wildcard()
      |> Enum.sort()
      |> hd()

    payload = unknown_atom_record_payload(atom_name)
    crc = :erlang.crc32(payload)
    File.write!(path, <<byte_size(payload)::32, crc::32, payload::binary>>)
    path
  end

  defp encode_segment_record(record) do
    payload = :erlang.term_to_binary(record)
    <<byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
  end

  defp unknown_atom_record_payload(atom_name)
       when is_binary(atom_name) and byte_size(atom_name) < 256 do
    <<131, 104, 2, 97, 1, 104, 2, 97, 1, 119, byte_size(atom_name), atom_name::binary>>
  end

  defp unknown_atom_payload(atom_name) when is_binary(atom_name) and byte_size(atom_name) < 256 do
    <<131, 119, byte_size(atom_name), atom_name::binary>>
  end

  defp segment_log_view(status) do
    waraft_log_view(status)
  end

  defp waraft_log_view(status) do
    name = :wa_raft_log.registered_name(:ferricstore_waraft_spike, 1)
    log_module = Keyword.fetch!(status, :log_module)

    log = {:raft_log, name, :ferricstore, :ferricstore_waraft_spike, 1, log_module}

    {:log_view, log, Keyword.fetch!(status, :log_first), Keyword.fetch!(status, :log_last),
     :undefined}
  end

  defp append_entries!(view, []), do: view

  defp append_entries!(view, entries) do
    assert {:ok, new_view} = :wa_raft_log.append(view, entries)
    new_view
  end

  defp log_view_last({:log_view, _log, _first, last, _config}), do: last

  defp existing_atom?(atom_name) do
    _ = String.to_existing_atom(atom_name)
    true
  rescue
    ArgumentError -> false
  end

  defp decode_segment_records(<<len::32, crc::32, rest::binary>>, acc)
       when byte_size(rest) >= len do
    <<payload::binary-size(len), tail::binary>> = rest
    assert :erlang.crc32(payload) == crc
    decode_segment_records(tail, [:erlang.binary_to_term(payload, [:safe]) | acc])
  end

  defp decode_segment_records(_tail, acc), do: Enum.reverse(acc)

  defp ensure_distribution! do
    case Node.self() do
      :nonode@nohost ->
        node_name = :"waraft_runner_#{:erlang.unique_integer([:positive])}"
        assert {:ok, _} = Node.start(node_name, :shortnames)

      _ ->
        :ok
    end
  end

  defp rewind_spike_storage!(root, keys_to_delete, position) do
    storage_path =
      Path.join([
        List.to_string(root),
        "ferricstore_waraft_spike.1",
        "storage.ets"
      ])

    {:ok, table} = :ets.file2tab(String.to_charlist(storage_path))

    Enum.each(keys_to_delete, fn key ->
      :ets.delete(table, key)
    end)

    true = :ets.insert(table, {:"$position", position})
    tmp_path = storage_path <> ".tmp"
    :ok = :ets.tab2file(table, String.to_charlist(tmp_path))
    true = :ets.delete(table)
    File.rename!(tmp_path, storage_path)
  end
end
