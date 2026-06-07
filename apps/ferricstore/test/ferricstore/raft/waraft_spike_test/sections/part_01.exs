defmodule Ferricstore.Raft.WARaftSpikeTest.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

  test "one-shard WARaft adapter can bootstrap, SET, and GET", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start(root)
    assert :ok = :ferricstore_waraft_spike.put("k1", "v1")
    assert {:ok, "v1"} = :ferricstore_waraft_spike.get("k1")
  end

  test "segment-log toy mode supports async pipelined SET replies", %{root: root} do
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

  test "segment-log toy mode supports one async Raft command for a SET batch", %{root: root} do
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

  test "segment-log toy mode can run the local batched load driver", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_volatile(root)

    assert {:ok, result} = :ferricstore_waraft_spike_load.run(1_000, 10, 10, 16, 100)
    assert %{ops: 1_000, elapsed_us: elapsed_us} = result
    assert elapsed_us > 0

    assert {:ok, value} = :ferricstore_waraft_spike.get(<<"bench:k1000">>)
    assert value == :binary.copy("x", 16)
  end

  test "segment-log toy mode can run the mixed GET and SET load driver", %{root: root} do
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

  test "segment-log toy mode can run multiple WARaft partitions", %{root: root} do
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

  test "segment-log toy mode can run the multi-partition load driver", %{root: root} do
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
    table = :"ferricstore_waraft_safe_decode_#{System.unique_integer([:positive])}"
    partition = 1
    log_name = :wa_raft_log.registered_name(table, partition)

    log =
      {:raft_log, log_name, :ferricstore, table, partition, :ferricstore_waraft_spike_segment_log}

    raft_root = Path.join(List.to_string(root), "safe-decode")
    previous_database = Application.get_env(:ferricstore, :raft_database)
    previous_waraft_database = Application.get_env(:wa_raft, :raft_database)

    atom_name = "ferricstore_waraft_corrupt_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

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
    write_segment_config_fixture!(segment_dir, 65_536)
    payload = unknown_atom_record_payload(atom_name, 1)
    File.write!(
      Path.join(segment_dir, "0.seg"),
      <<byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
    )

    try do
      assert {:error, {:bad_term, _reason}} = :ferricstore_waraft_spike_segment_log.open(log)
      refute existing_atom?(atom_name)
    after
      restore_env(:raft_database, previous_database)
      restore_app_env(:wa_raft, :raft_database, previous_waraft_database)

      if :ets.info(log_name) != :undefined do
        :ets.delete(log_name)
      end
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

  test "public log trim reports provider failure instead of advancing the view", %{root: root} do
    assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
    status = :ferricstore_waraft_spike.status()
    term = Keyword.fetch!(status, :current_term)

    view =
      status
      |> segment_log_view()
      |> append_entries!(
        for i <- 1..5 do
          {term, {make_ref(), {:write, :ferricstore_waraft_spike, "trim-fail:#{i}", "v"}}}
        end
      )

    trim_index = log_view_first(view) + 2
    File.mkdir_p!(Path.join(List.to_string(root), "ferricstore_waraft_spike.1/segment_projection_log"))

    assert {:error, _reason} = :wa_raft_log.trim(view, trim_index)
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
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      view = advance_to_split_pair_boundary!(view, term, 2)

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
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "custom durable segment log fails closed when split append rollback fails", %{
    root: root
  } do
    previous_append_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)
    previous_rollback_hook = Application.get_env(:ferricstore, :waraft_segment_log_rollback_hook)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      view = advance_to_split_pair_boundary!(view, term, 2)

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
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
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
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 4096)
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
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
    end
  end

  test "custom durable segment log fsyncs parent directory when append creates a segment", %{
    root: root
  } do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_sync_dir_hook)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)
      view = advance_to_next_segment_boundary!(view, term, 2)

      Application.put_env(:ferricstore, :waraft_segment_log_sync_dir_hook, {:notify, self()})

      entry = {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:new", "value"}}}
      assert {:ok, _new_view} = :wa_raft_log.append(view, [entry])

      segment_dir = segment_log_dir(root)
      assert_receive {:waraft_segment_log_sync_dir, ^segment_dir}, 1_000
    after
      restore_env(:waraft_segment_log_sync_dir_hook, previous_hook)
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
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

  test "custom durable segment log uses direct datasync for appended data", %{root: root} do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:notify, self()})

      entry =
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:datasync", "value"}}}

      assert {:ok, _new_view} = :wa_raft_log.append(view, [entry])
      assert_receive {:waraft_segment_log_file_sync, _path}, 1_000
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "custom durable segment log reuses direct append fd for same segment", %{root: root} do
    previous_sync_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
    previous_open_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_open_hook)

    try do
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      status = :ferricstore_waraft_spike.status()
      term = Keyword.fetch!(status, :current_term)
      view = segment_log_view(status)

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:notify, self()})
      Application.put_env(:ferricstore, :waraft_segment_log_file_open_hook, {:notify, self()})

      entry1 =
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:reuse:1", "v1"}}}

      assert {:ok, view} = :wa_raft_log.append(view, [entry1])
      assert_receive {:waraft_segment_log_file_open, path}, 1_000
      assert_receive {:waraft_segment_log_file_sync, ^path}, 1_000

      entry2 =
        {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:reuse:2", "v2"}}}

      assert {:ok, _view} = :wa_raft_log.append(view, [entry2])
      assert_receive {:waraft_segment_log_file_sync, ^path}, 1_000
      refute_receive {:waraft_segment_log_file_open, ^path}, 100

      assert :ok = :ferricstore_waraft_spike.stop()
      assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
      assert {:ok, "v1"} = :ferricstore_waraft_spike.storage_get("segment:reuse:1")
      assert {:ok, "v2"} = :ferricstore_waraft_spike.storage_get("segment:reuse:2")
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_sync_hook)
      restore_env(:waraft_segment_log_file_open_hook, previous_open_hook)
    end
  end
    end
  end
end
