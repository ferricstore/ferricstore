defmodule Ferricstore.Store.ShardGetManyAdmissionTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.Reads

  test "shard batch reads bound workers and queued requests" do
    parent = self()

    batch_reader = fn [{_path, _offset, key}], _timeout ->
      send(parent, {:batch_read_started, key, self()})

      receive do
        :continue -> {:ok, [key <> ":value"]}
      after
        2_000 -> {:error, :injected_timeout}
      end
    end

    {pid, ctx, data_dir} =
      start_shard(
        shard_get_many_max_concurrency: 1,
        shard_get_many_max_queued: 1,
        get_many_pread_batch: batch_reader
      )

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    state =
      :sys.replace_state(pid, fn state ->
        Map.put(state, :get_many_pread_batch, batch_reader)
      end)

    keys = ["get-many:first", "get-many:second", "get-many:rejected"]

    Enum.with_index(keys)
    |> Enum.each(fn {key, offset} ->
      :ets.insert(state.keydir, {key, nil, 0, 0, 0, offset, 1})
    end)

    first = :gen_server.send_request(pid, {:get_many, [Enum.at(keys, 0)]})
    assert_receive {:batch_read_started, "get-many:first", first_worker}, 500

    second = :gen_server.send_request(pid, {:get_many, [Enum.at(keys, 1)]})
    rejected = :gen_server.send_request(pid, {:get_many, [Enum.at(keys, 2)]})

    assert {:reply, {:error, "BUSY shard batch read queue is full"}} =
             :gen_server.receive_response(rejected, 500)

    refute_receive {:batch_read_started, "get-many:second", _worker}, 50

    send(first_worker, :continue)
    assert_receive {:batch_read_started, "get-many:second", second_worker}, 500
    send(second_worker, :continue)

    assert {:reply, ["get-many:first:value"]} =
             :gen_server.receive_response(first, 1_000)

    assert {:reply, ["get-many:second:value"]} =
             :gen_server.receive_response(second, 1_000)
  end

  test "a crashed batch-read worker releases its slot" do
    parent = self()

    batch_reader = fn [{_path, _offset, key}], _timeout ->
      send(parent, {:batch_read_started, key, self()})

      case key do
        "get-many:crash" ->
          receive do
            :crash -> Process.exit(self(), :kill)
          end

        "get-many:after-crash" ->
          receive do
            :continue -> {:ok, ["recovered"]}
          end
      end
    end

    {pid, ctx, data_dir} =
      start_shard(
        shard_get_many_max_concurrency: 1,
        shard_get_many_max_queued: 1,
        get_many_pread_batch: batch_reader
      )

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    state = :sys.get_state(pid)

    for {key, offset} <- [{"get-many:crash", 0}, {"get-many:after-crash", 1}] do
      :ets.insert(state.keydir, {key, nil, 0, 0, 0, offset, 1})
    end

    crashing = :gen_server.send_request(pid, {:get_many, ["get-many:crash"]})
    assert_receive {:batch_read_started, "get-many:crash", crashing_worker}, 500

    queued = :gen_server.send_request(pid, {:get_many, ["get-many:after-crash"]})
    send(crashing_worker, :crash)

    assert {:reply, {:error, "ERR shard batch read failed"}} =
             :gen_server.receive_response(crashing, 500)

    assert_receive {:batch_read_started, "get-many:after-crash", recovered_worker}, 500
    send(recovered_worker, :continue)

    assert {:reply, ["recovered"]} = :gen_server.receive_response(queued, 500)
  end

  test "an expired queued batch is replied unavailable without starting disk IO" do
    parent = self()

    batch_reader = fn [{_path, _offset, key}], _timeout ->
      send(parent, {:batch_read_started, key, self()})

      receive do
        :continue -> {:ok, [key <> ":value"]}
      end
    end

    {pid, ctx, data_dir} =
      start_shard(
        shard_get_many_max_concurrency: 1,
        shard_get_many_max_queued: 1,
        get_many_pread_batch: batch_reader
      )

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    state =
      :sys.replace_state(pid, fn state ->
        state
        |> Map.put(:get_many_pread_batch, batch_reader)
        |> Map.put(:get_many_deadline_ms, 50)
      end)

    for {key, offset} <- [{"get-many:running", 0}, {"get-many:expired", 1}] do
      :ets.insert(state.keydir, {key, nil, 0, 0, 0, offset, 1})
    end

    running = :gen_server.send_request(pid, {:get_many, ["get-many:running"]})
    assert_receive {:batch_read_started, "get-many:running", worker}, 500

    expired = :gen_server.send_request(pid, {:get_many, ["get-many:expired"]})
    Process.sleep(75)
    send(worker, :continue)

    assert {:reply, [:unavailable]} = :gen_server.receive_response(expired, 500)
    refute_receive {:batch_read_started, "get-many:expired", _worker}, 100
    assert {:reply, [:unavailable]} = :gen_server.receive_response(running, 500)
  end

  test "batch disk IO receives only the remaining request deadline" do
    parent = self()

    batch_reader = fn [_location], timeout ->
      send(parent, {:batch_read_timeout, timeout})
      {:error, :injected_timeout}
    end

    {pid, ctx, data_dir} =
      start_shard(
        shard_get_many_max_concurrency: 1,
        get_many_pread_batch: batch_reader
      )

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    state =
      :sys.replace_state(pid, fn state ->
        state
        |> Map.put(:get_many_pread_batch, batch_reader)
        |> Map.put(:get_many_deadline_ms, 75)
      end)

    :ets.insert(state.keydir, {"get-many:deadline", nil, 0, 0, 0, 0, 1})

    assert [:unavailable] = GenServer.call(pid, {:get_many, ["get-many:deadline"]})
    assert_receive {:batch_read_timeout, timeout}, 500
    assert timeout > 0
    assert timeout <= 75
  end

  test "a running batch-read worker is killed at its absolute deadline" do
    parent = self()

    batch_reader = fn [{_path, _offset, key}], _timeout ->
      send(parent, {:batch_read_blocked, key, self()})

      receive do
        :release -> {:ok, ["late"]}
      end
    end

    {pid, ctx, data_dir} =
      start_shard(
        shard_get_many_max_concurrency: 1,
        get_many_pread_batch: batch_reader
      )

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    state =
      :sys.replace_state(pid, fn state ->
        state
        |> Map.put(:get_many_pread_batch, batch_reader)
        |> Map.put(:get_many_deadline_ms, 50)
      end)

    :ets.insert(state.keydir, {"get-many:blocked", nil, 0, 0, 0, 0, 1})

    request = :gen_server.send_request(pid, {:get_many, ["get-many:blocked"]})
    assert_receive {:batch_read_blocked, "get-many:blocked", worker}, 500
    monitor_ref = Process.monitor(worker)

    assert {:reply, [:unavailable]} = :gen_server.receive_response(request, 500)
    assert_receive {:DOWN, ^monitor_ref, :process, ^worker, :killed}, 500
    assert :sys.get_state(pid).get_many_workers == %{}
  end

  test "a timed-out worker keeps its admission slot until its DOWN arrives" do
    parent = self()

    batch_reader = fn [{_path, _offset, key}], _timeout ->
      send(parent, {:batch_read_started, key, self()})

      receive do
        :continue -> {:ok, [key <> ":value"]}
      end
    end

    {pid, ctx, data_dir} =
      start_shard(
        shard_get_many_max_concurrency: 1,
        shard_get_many_max_queued: 1,
        get_many_pread_batch: batch_reader
      )

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    state = :sys.get_state(pid)
    :ets.insert(state.keydir, {"get-many:timed-out", nil, 0, 0, 0, 0, 1})
    :ets.insert(state.keydir, {"get-many:queued", nil, 0, 0, 0, 1, 1})
    deadline_ms = System.monotonic_time(:millisecond) + 1_000
    first_tag = make_ref()
    second_tag = make_ref()

    assert {:noreply, state} =
             Reads.handle_get_many(
               ["get-many:timed-out"],
               {self(), first_tag},
               deadline_ms,
               state
             )

    assert_receive {:batch_read_started, "get-many:timed-out", first_worker}, 500
    [{job_ref, first_job}] = Map.to_list(state.get_many_workers)

    assert {:noreply, state} =
             Reads.handle_get_many(
               ["get-many:queued"],
               {self(), second_tag},
               deadline_ms,
               state
             )

    state = Reads.handle_get_many_timeout(job_ref, state)

    assert_receive {^first_tag, [:unavailable]}, 500
    refute_receive {:batch_read_started, "get-many:queued", _worker}, 50
    assert %{^job_ref => %{from: nil, timed_out?: true}} = state.get_many_workers
    assert state.get_many_waiting_count == 1

    assert_receive {:DOWN, monitor_ref, :process, ^first_worker, :killed}, 500
    assert monitor_ref == first_job.monitor_ref
    assert {:handled, drained_state} = Reads.handle_get_many_down(monitor_ref, state)
    assert_receive {:batch_read_started, "get-many:queued", second_worker}, 500
    assert map_size(drained_state.get_many_workers) == 1

    Process.exit(second_worker, :kill)
  end

  test "an expired WARaft batch deadline is rejected before storage lookup" do
    assert {:error, :deadline_exceeded} =
             Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
               %{},
               0,
               {:waraft_segment, 1},
               ["get-many:expired-waraft"],
               0
             )
  end

  test "WARaft-backed keys sharing a segment are read in one batch" do
    parent = self()

    waraft_reader = fn _ctx, 0, file_id, keys, timeout_ms ->
      send(parent, {:waraft_batch_read, file_id, keys, timeout_ms})
      {:ok, Map.new(keys, &{&1, &1 <> ":value"})}
    end

    {pid, ctx, data_dir} = start_shard(get_many_waraft_batch: waraft_reader)
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    state =
      :sys.replace_state(pid, fn state ->
        Map.put(state, :get_many_waraft_batch, waraft_reader)
      end)

    file_id = {:waraft_segment, 123}
    keys = ["get-many:waraft-a", "get-many:waraft-b"]

    Enum.each(keys, fn key ->
      :ets.insert(state.keydir, {key, nil, 0, 0, file_id, 0, byte_size(key) + 6})
    end)

    assert Enum.map(keys, &(&1 <> ":value")) == GenServer.call(pid, {:get_many, keys})
    assert_receive {:waraft_batch_read, ^file_id, ^keys, timeout_ms}, 500
    assert timeout_ms > 0
    refute_receive {:waraft_batch_read, ^file_id, _keys, _timeout_ms}, 50
  end

  defp start_shard(opts) do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "shard_get_many_admission_#{System.unique_integer([:positive])}"
      )

    name = :"shard_get_many_admission_#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.build(name, data_dir: data_dir, shard_count: 1)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    {:ok, pid} =
      Shard.start_link(
        [
          index: 0,
          data_dir: data_dir,
          instance_ctx: ctx,
          flow_shared_ref_backfill?: false
        ] ++ opts
      )

    {pid, ctx, data_dir}
  end

  defp cleanup_shard(pid, ctx, data_dir) do
    try do
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
    catch
      :exit, _reason -> :ok
    end

    FerricStore.Instance.cleanup(ctx.name)
    File.rm_rf!(data_dir)
  end
end
