defmodule Ferricstore.Raft.WARaftSegmentDiskReaderTest do
  use ExUnit.Case, async: false

  @moduletag :raft
  @moduletag :global_state

  alias Ferricstore.Raft.WARaftBackend.RuntimeSupervisor
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Raft.WARaftSegmentReader.DiskReader

  setup do
    previous_records =
      Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

    previous_handles =
      Application.get_env(:ferricstore, :waraft_segment_reader_cache_max_handles)

    previous_lanes =
      Application.get_env(:ferricstore, :waraft_segment_reader_lanes)

    Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 1)
    Application.put_env(:ferricstore, :waraft_segment_reader_cache_max_handles, 1)
    Application.put_env(:ferricstore, :waraft_segment_reader_lanes, 1)
    :ok = RuntimeSupervisor.ensure_started()

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-disk-reader-#{System.unique_integer([:positive])}"
      )

    root = Path.join([data_dir, "waraft", "ferricstore_waraft_backend.1"])
    projection_root = Path.join(root, "apply_projection_log")

    on_exit(fn ->
      DiskReader.stop(root)
      restore_env(:waraft_segment_log_records_per_segment, previous_records)
      restore_env(:waraft_segment_reader_cache_max_handles, previous_handles)
      restore_env(:waraft_segment_reader_lanes, previous_lanes)
      File.rm_rf!(data_dir)
    end)

    %{data_dir: data_dir, root: root, projection_root: projection_root}
  end

  test "reader reuses handles and evicts within its configured bound", context do
    first_index = 101
    second_index = 102

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [
          {{:raft_log_pos, first_index, 0}, [{"a", "first", 0}]},
          {{:raft_log_pos, second_index, 0}, [{"b", "second", 0}]}
        ]
      )

    first_location = location!(context.projection_root, first_index)
    second_location = location!(context.projection_root, second_index)

    assert {:ok, first_entry} =
             DiskReader.read(context.root, first_index, first_location)

    assert {:ok, ^first_entry} =
             DiskReader.read(context.root, first_index, first_location)

    assert %{handles: 1, opens: 1, evictions: 0, max_handles: 1} =
             DiskReader.status(context.root)

    assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"b", "second", 0}]}}} =
             DiskReader.read(context.root, second_index, second_location)

    assert %{handles: 1, opens: 2, evictions: 1, max_handles: 1} =
             DiskReader.status(context.root)

    assert :ok = DiskReader.invalidate(context.root)
    assert %{handles: 0, opens: 2, evictions: 1} = DiskReader.status(context.root)
  end

  test "batch reader preserves request order across bounded segment handles", context do
    first_index = 151
    second_index = 152

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [
          {{:raft_log_pos, first_index, 0}, [{"a", "first", 0}]},
          {{:raft_log_pos, second_index, 0}, [{"b", "second", 0}]}
        ]
      )

    first_location = location!(context.projection_root, first_index)
    second_location = location!(context.projection_root, second_index)

    assert {:ok,
            [
              {0, {:ferricstore_segment_apply_projection_batch, _, [{"b", "second", 0}]}},
              {0, {:ferricstore_segment_apply_projection_batch, _, [{"a", "first", 0}]}}
            ]} =
             DiskReader.read_many(context.root, [
               {second_index, second_location},
               {first_index, first_location}
             ])

    assert %{handles: 1, opens: 2, evictions: 1, max_handles: 1} =
             DiskReader.status(context.root)
  end

  test "configured lanes share one total descriptor budget and invalidate together", context do
    Application.put_env(:ferricstore, :waraft_segment_reader_cache_max_handles, 4)
    Application.put_env(:ferricstore, :waraft_segment_reader_lanes, 4)
    index = 161

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [{{:raft_log_pos, index, 0}, [{"a", "value", 0}]}]
      )

    location = location!(context.projection_root, index)
    workers = workers_by_lane(4)

    Enum.each(workers, fn {_lane, pid} ->
      send(pid, {:read, context.root, index, location, self()})
    end)

    Enum.each(workers, fn {_lane, pid} ->
      assert_receive {:lane_read, ^pid, {:ok, _entry}}, 1_000
    end)

    assert %{handles: 4, opens: 4, lanes: 4, configured_lanes: 4, max_handles: 4} =
             DiskReader.status(context.root)

    assert :ok =
             WARaftSegmentReader.with_apply_projection_disk_lock(context.data_dir, 0, fn ->
               assert %{handles: 0, lanes: 4, configured_lanes: 4, max_handles: 4} =
                        DiskReader.status(context.root)

               :ok
             end)
  end

  test "batch reader rejects oversized work before storage access", context do
    request = {1, {1, 0, 8}}

    assert {:error, :invalid_segment_reader_batch} =
             DiskReader.read_many(context.root, List.duplicate(request, 4_097))

    assert %{handles: 0, opens: 0} = DiskReader.status(context.root)
  end

  test "batch reader fails the whole read when one frame is corrupt", context do
    first_index = 171
    second_index = 172

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [
          {{:raft_log_pos, first_index, 0}, [{"a", "first", 0}]},
          {{:raft_log_pos, second_index, 0}, [{"b", "second", 0}]}
        ]
      )

    first_location = location!(context.projection_root, first_index)

    {ordinal, offset, encoded_size} =
      second_location = location!(context.projection_root, second_index)

    path = Path.join([context.projection_root, "segment_log", "#{ordinal}.seg"])
    {:ok, fd} = :file.open(to_charlist(path), [:read, :write, :raw, :binary])

    try do
      {:ok, <<last_byte>>} = :file.pread(fd, offset + encoded_size - 1, 1)
      :ok = :file.pwrite(fd, offset + encoded_size - 1, <<Bitwise.bxor(last_byte, 1)>>)
      :ok = :file.sync(fd)
    after
      :ok = :file.close(fd)
    end

    assert {:error, {:crc_mismatch, ^offset}} =
             DiskReader.read_many(context.root, [
               {first_index, first_location},
               {second_index, second_location}
             ])
  end

  test "batch reader honors the caller timeout while its lane is busy", context do
    index = 181

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [{{:raft_log_pos, index, 0}, [{"a", "value", 0}]}]
      )

    location = location!(context.projection_root, index)
    assert {:ok, _entry} = DiskReader.read(context.root, index, location)
    reader = DiskReader.whereis(context.root)
    assert is_pid(reader)
    :ok = :sys.suspend(reader)

    try do
      started_ms = System.monotonic_time(:millisecond)

      assert {:error, {:segment_reader_timeout, {:timeout, _call}}} =
               DiskReader.read_many(context.root, [{index, location}], 20)

      assert System.monotonic_time(:millisecond) - started_ms < 200
    after
      :ok = :sys.resume(reader)
    end
  end

  test "physical reads honor their deadline while compaction owns the disk latch", context do
    index = 191

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [{{:raft_log_pos, index, 0}, [{"a", "value", 0}]}]
      )

    {ordinal, offset, frame_size} = location!(context.projection_root, index)
    parent = self()

    writer =
      Task.async(fn ->
        WARaftSegmentReader.with_apply_projection_disk_lock(context.data_dir, 0, fn ->
          send(parent, :disk_latch_acquired)

          receive do
            :release_disk_latch -> :ok
          end
        end)
      end)

    assert_receive :disk_latch_acquired, 1_000

    reader =
      Task.async(fn ->
        started_ms = System.monotonic_time(:millisecond)

        result =
          WARaftSegmentReader.read_physical_values(
            %{data_dir: context.data_dir},
            0,
            [
              %{
                file_id: {:waraft_apply_projection, index},
                ordinal: ordinal,
                offset: offset,
                frame_size: frame_size,
                key: "a"
              }
            ],
            20,
            :include_expired
          )

        {result, System.monotonic_time(:millisecond) - started_ms}
      end)

    yielded = Task.yield(reader, 250)
    send(writer.pid, :release_disk_latch)
    assert :ok = Task.await(writer, 1_000)

    if is_nil(yielded), do: Task.shutdown(reader, :brutal_kill)

    assert {:ok, {{:error, :deadline_exceeded}, elapsed_ms}} = yielded
    assert elapsed_ms < 200
  end

  test "exclusive apply-projection disk latch invalidates retained readers", context do
    index = 201

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [{{:raft_log_pos, index, 0}, [{"a", "value", 0}]}]
      )

    assert {:ok, _entry} =
             DiskReader.read(context.root, index, location!(context.projection_root, index))

    assert %{handles: 1} = DiskReader.status(context.root)

    assert :ok =
             WARaftSegmentReader.with_apply_projection_disk_lock(context.data_dir, 0, fn ->
               assert %{handles: 0} = DiskReader.status(context.root)

               assert :ok =
                        :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
                          to_charlist(context.projection_root),
                          [{{:raft_log_pos, index, 0}, [{"a", "replacement", 0}]}]
                        )

               assert {:ok,
                       {0,
                        {:ferricstore_segment_apply_projection_batch, _,
                         [{"a", "replacement", 0}]}}} =
                        DiskReader.read(
                          context.root,
                          index,
                          location!(context.projection_root, index)
                        )

               assert %{handles: 1} = DiskReader.status(context.root)

               :ok
             end)

    assert %{handles: 0, opens: 2} = DiskReader.status(context.root)

    assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"a", "replacement", 0}]}}} =
             DiskReader.read(context.root, index, location!(context.projection_root, index))

    assert %{handles: 1, opens: 3} = DiskReader.status(context.root)
  end

  test "production apply-projection reads reuse the per-root disk reader", context do
    index = 301

    :ok =
      :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
        to_charlist(context.projection_root),
        [{{:raft_log_pos, index, 0}, [{"a", "value", 0}]}]
      )

    ctx = %{data_dir: context.data_dir}

    assert {:ok, %{"a" => "value"}} =
             WARaftSegmentReader.read_values_from_location(
               ctx,
               0,
               {:waraft_apply_projection, index},
               ["a"]
             )

    assert %{handles: 1, opens: 1} = DiskReader.status(context.root)

    assert {:ok, %{"a" => "value"}} =
             WARaftSegmentReader.read_values_from_location(
               ctx,
               0,
               {:waraft_apply_projection, index},
               ["a"]
             )

    assert %{handles: 1, opens: 1} = DiskReader.status(context.root)
  end

  defp location!(root, index) do
    {:ok, location} =
      :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), index)

    location
  end

  defp workers_by_lane(lane_count), do: workers_by_lane(lane_count, %{}, 0)

  defp workers_by_lane(lane_count, workers, _attempts) when map_size(workers) == lane_count,
    do: workers

  defp workers_by_lane(_lane_count, workers, attempts) when attempts >= 1_000 do
    Enum.each(workers, fn {_lane, pid} -> Process.exit(pid, :kill) end)
    flunk("could not allocate one worker process per reader lane")
  end

  defp workers_by_lane(lane_count, workers, attempts) do
    pid =
      spawn(fn ->
        receive do
          {:read, root, index, location, parent} ->
            send(parent, {:lane_read, self(), DiskReader.read(root, index, location)})
        end
      end)

    lane = :erlang.phash2(pid, lane_count)

    case Map.has_key?(workers, lane) do
      true ->
        Process.exit(pid, :kill)
        workers_by_lane(lane_count, workers, attempts + 1)

      false ->
        workers_by_lane(lane_count, Map.put(workers, lane, pid), attempts + 1)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
