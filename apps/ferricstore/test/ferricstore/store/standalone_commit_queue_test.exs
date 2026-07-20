defmodule Ferricstore.Store.StandaloneCommitQueueTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard
  alias Ferricstore.ServerCatalog

  test "catalog versions advance across separate standalone flushes" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    assert {:ok, first} =
             Router.server_catalog_mutate(ctx, "queue-catalog", "subject", nil, nil, "first", 10)

    assert {:ok, %{version: first_version}} = ServerCatalog.decode_entry(first)

    assert {:ok, second} =
             Router.server_catalog_mutate(
               ctx,
               "queue-catalog",
               "subject",
               first,
               ServerCatalog.encode_revision(first_version),
               "second",
               10
             )

    assert {:ok, %{version: second_version, value: "second"}} =
             ServerCatalog.decode_entry(second)

    assert second_version > first_version
  end

  test "cross-shard barrier release is scoped to the acquiring operation" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    owner = make_ref()
    other = make_ref()

    assert :ok =
             GenServer.call(pid, {:standalone_cross_shard_barrier_acquire, owner})

    pending =
      :gen_server.send_request(
        pid,
        {:standalone_commit, {:put, "queue:barrier-owner", "value", 0}}
      )

    assert %{waiting_count: 1} = GenServer.call(pid, :standalone_commit_debug)

    assert {:error, :standalone_cross_shard_barrier_not_owner} =
             GenServer.call(pid, {:standalone_cross_shard_barrier_release, other})

    assert %{waiting_count: 1} = GenServer.call(pid, :standalone_commit_debug)
    assert :ok = GenServer.call(pid, {:standalone_cross_shard_barrier_release, owner})
    assert {:reply, :ok} = :gen_server.receive_response(pending, 5_000)
    assert "value" == Router.get(ctx, "queue:barrier-owner")
  end

  test "cross-shard participant shuts down for journal recovery when the coordinator dies" do
    {pid, ctx, data_dir} = start_shard([])
    Process.unlink(pid)
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    owner_token = make_ref()
    parent = self()
    shard_monitor = Process.monitor(pid)

    owner =
      spawn(fn ->
        result =
          GenServer.call(pid, {:standalone_cross_shard_barrier_acquire, owner_token})

        send(parent, {:barrier_acquired, result})
        Process.sleep(:infinity)
      end)

    assert_receive {:barrier_acquired, :ok}, 1_000
    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^shard_monitor, :process, ^pid,
                    {:shutdown, {:standalone_cross_shard_owner_down, :killed}}},
                   5_000
  end

  test "waiting commits are bounded without changing FIFO apply order" do
    {pid, ctx, data_dir} = start_shard(standalone_commit_max_queued_ops: 2)
    barrier_owner = make_ref()

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    assert :ok =
             GenServer.call(pid, {:standalone_cross_shard_barrier_acquire, barrier_owner})

    first = :gen_server.send_request(pid, {:standalone_commit, {:put, "queue:key", "first", 0}})

    second =
      :gen_server.send_request(pid, {:standalone_commit, {:put, "queue:key", "second", 0}})

    assert %{waiting_count: 2} = GenServer.call(pid, :standalone_commit_debug)

    rejected =
      :gen_server.send_request(pid, {:standalone_commit, {:put, "queue:rejected", "value", 0}})

    assert {:reply, {:error, "BUSY standalone commit queue is full"}} =
             :gen_server.receive_response(rejected, 500)

    assert %{waiting_count: 2} = GenServer.call(pid, :standalone_commit_debug)

    assert :ok =
             GenServer.call(pid, {:standalone_cross_shard_barrier_release, barrier_owner})

    assert {:reply, :ok} = :gen_server.receive_response(first, 5_000)
    assert {:reply, :ok} = :gen_server.receive_response(second, 5_000)

    assert "second" = Router.get(ctx, "queue:key")
    assert nil == Router.get(ctx, "queue:rejected")
  end

  test "retained commit bytes include the in-flight durability batch" do
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      send(test_pid, {:durability_batch, self(), length(batch)})

      receive do
        :continue -> :passthrough
      after
        1_000 -> :passthrough
      end
    end)

    on_exit(fn -> restore_env(:standalone_durability_hook, previous_hook) end)

    first_command = {:put, "queue:bytes:first", String.duplicate("a", 256), 0}
    first_bytes = :erlang.external_size(first_command)

    {pid, ctx, data_dir} =
      start_shard(
        standalone_fsync_max_delay_ms: 1,
        standalone_commit_max_queued_ops: 10,
        standalone_commit_max_queued_bytes: first_bytes
      )

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    first = :gen_server.send_request(pid, {:standalone_commit, first_command})
    assert_receive {:durability_batch, first_worker, 1}, 1_000

    assert %{
             batch_bytes: 0,
             waiting_bytes: 0,
             inflight_bytes: ^first_bytes,
             retained_bytes: ^first_bytes
           } = GenServer.call(pid, :standalone_commit_debug)

    rejected =
      :gen_server.send_request(
        pid,
        {:standalone_commit, {:put, "queue:bytes:rejected", "b", 0}}
      )

    assert {:reply, {:error, "BUSY standalone commit queue is full"}} =
             :gen_server.receive_response(rejected, 500)

    send(first_worker, :continue)
    assert {:reply, :ok} = :gen_server.receive_response(first, 5_000)
    assert nil == Router.get(ctx, "queue:bytes:rejected")
  end

  test "a drained waiting queue still honors the fsync batch limit" do
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      send(test_pid, {:durability_batch, self(), length(batch)})

      receive do
        :continue -> :passthrough
      after
        1_000 -> :passthrough
      end
    end)

    on_exit(fn -> restore_env(:standalone_durability_hook, previous_hook) end)

    {pid, ctx, data_dir} =
      start_shard(standalone_fsync_max_ops: 2, standalone_commit_max_queued_ops: 5)

    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    barrier_owner = make_ref()

    assert :ok =
             GenServer.call(pid, {:standalone_cross_shard_barrier_acquire, barrier_owner})

    requests =
      for index <- 1..5 do
        :gen_server.send_request(
          pid,
          {:standalone_commit, {:put, "queue:chunk:#{index}", Integer.to_string(index), 0}}
        )
      end

    assert %{waiting_count: 5} = GenServer.call(pid, :standalone_commit_debug)

    assert :ok =
             GenServer.call(pid, {:standalone_cross_shard_barrier_release, barrier_owner})

    assert_receive {:durability_batch, first_worker, 2}, 1_000
    assert %{batch_count: 3, inflight_count: 2} = GenServer.call(pid, :standalone_commit_debug)
    send(first_worker, :continue)

    assert_receive {:durability_batch, second_worker, 2}, 1_000
    send(second_worker, :continue)

    assert_receive {:durability_batch, third_worker, 1}, 1_000
    send(third_worker, :continue)

    Enum.each(requests, fn request ->
      assert {:reply, :ok} = :gen_server.receive_response(request, 5_000)
    end)
  end

  test "a later single-key write cannot overtake an earlier multi-key dependency" do
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      send(test_pid, {:durability_batch, self(), length(batch)})

      receive do
        :continue -> :passthrough
      after
        5_000 -> :passthrough
      end
    end)

    on_exit(fn -> restore_env(:standalone_durability_hook, previous_hook) end)

    {pid, ctx, data_dir} = start_shard(standalone_commit_delay_ms: 0)
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    first = :gen_server.send_request(pid, {:standalone_commit, {:put, "queue:dep:a", "a0", 0}})
    assert_receive {:durability_batch, first_worker, 1}, 1_000

    middle =
      :gen_server.send_request(
        pid,
        {:standalone_commit, {:mset, [{"queue:dep:a", "a1", 0}, {"queue:dep:b", "b1", 0}]}}
      )

    last =
      :gen_server.send_request(pid, {:standalone_commit, {:put, "queue:dep:b", "b2", 0}})

    assert %{batch_count: 0, waiting_count: 2, inflight_count: 1} =
             GenServer.call(pid, :standalone_commit_debug)

    send(first_worker, :continue)
    assert_receive {:durability_batch, second_worker, 2}, 1_000
    send(second_worker, :continue)

    assert_receive {:durability_batch, third_worker, 1}, 1_000
    send(third_worker, :continue)

    assert {:reply, :ok} = :gen_server.receive_response(first, 5_000)
    assert {:reply, :ok} = :gen_server.receive_response(middle, 5_000)
    assert {:reply, :ok} = :gen_server.receive_response(last, 5_000)
    assert "b2" == Router.get(ctx, "queue:dep:b")
  end

  test "same-key conditional writes observe the preceding durability batch" do
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      send(test_pid, {:durability_batch, self(), length(batch)})

      receive do
        :continue -> :passthrough
      after
        5_000 -> :passthrough
      end
    end)

    on_exit(fn -> restore_env(:standalone_durability_hook, previous_hook) end)

    {pid, ctx, data_dir} = start_shard(standalone_commit_delay_ms: 50)
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    opts = %{expire_at_ms: 0, get: false, keepttl: false, nx: true, xx: false}

    first =
      :gen_server.send_request(
        pid,
        {:standalone_commit, {:set, "queue:nx", "first", 0, opts}}
      )

    second =
      :gen_server.send_request(
        pid,
        {:standalone_commit, {:set, "queue:nx", "second", 0, opts}}
      )

    assert_receive {:durability_batch, first_worker, 1}, 1_000

    assert %{batch_count: 0, waiting_count: 1, inflight_count: 1} =
             GenServer.call(pid, :standalone_commit_debug)

    refute_receive {:durability_batch, _worker, _count}, 100
    send(first_worker, :continue)

    assert {:reply, :ok} = :gen_server.receive_response(first, 5_000)
    assert {:reply, nil} = :gen_server.receive_response(second, 5_000)
    refute_receive {:durability_batch, _worker, _count}, 100
    assert "first" == Router.get(ctx, "queue:nx")
  end

  test "a conditional no-op does not advance the standalone WATCH version" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    key = "queue:watch-version:nx"
    opts = %{expire_at_ms: 0, get: false, keepttl: false, nx: true, xx: false}
    before = Router.get_version(ctx, key)

    assert :ok = Router.set(ctx, key, "first", opts)
    committed = Router.get_version(ctx, key)
    assert committed > before

    assert nil == Router.set(ctx, key, "second", opts)
    assert committed == Router.get_version(ctx, key)
  end

  test "nested batches and disjoint writes receive their own exact result slices" do
    {pid, ctx, data_dir} = start_shard(standalone_commit_delay_ms: 100)
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    batch =
      :gen_server.send_request(
        pid,
        {:standalone_commit,
         {:batch,
          [
            {:put, "queue:batch:a", "a", 0},
            {:put, "queue:batch:b", "b", 0}
          ]}}
      )

    single =
      :gen_server.send_request(
        pid,
        {:standalone_commit, {:put, "queue:single:c", "c", 0}}
      )

    assert %{batch_count: 2} = GenServer.call(pid, :standalone_commit_debug)
    assert {:reply, {:ok, [:ok, :ok]}} = :gen_server.receive_response(batch, 5_000)
    assert {:reply, :ok} = :gen_server.receive_response(single, 5_000)

    assert "a" == Router.get(ctx, "queue:batch:a")
    assert "b" == Router.get(ctx, "queue:batch:b")
    assert "c" == Router.get(ctx, "queue:single:c")
  end

  test "pausing writes keeps list reads available while rejecting list mutations" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    assert :ok = GenServer.call(pid, {:pause_writes})
    assert 0 = GenServer.call(pid, {:list_read, "queue:paused:list", :llen})

    assert {:error, "ERR shard writes paused for sync"} =
             GenServer.call(pid, {:list_op, "queue:paused:list", {:rpush, ["value"]}})

    assert {:error, "ERR shard writes paused for sync"} =
             GenServer.call(
               pid,
               {:fetch_or_compute_lock, "queue:paused:key",
                Ferricstore.FetchOrCompute.Outcome.key("queue:paused:key"), "owner",
                Ferricstore.HLC.now_ms() + 5_000}
             )
  end

  test "pause reports a prior flush failure instead of claiming a clean sync boundary" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    :sys.replace_state(pid, &Map.put(&1, :last_flush_error, :synthetic_eio))

    assert {:error, {:flush_failed, :synthetic_eio}} = GenServer.call(pid, {:pause_writes})

    assert %{
             writes_paused: true,
             last_flush_error: :synthetic_eio,
             write_pause_leases: leases
           } = :sys.get_state(pid)

    assert map_size(leases) == 0
  end

  test "failed pause owner death keeps durability failure paused without leaking its lease" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)
    parent = self()

    :sys.replace_state(pid, &Map.put(&1, :last_flush_error, :synthetic_eio))

    owner =
      spawn(fn ->
        lease = {self(), make_ref()}

        send(
          parent,
          {:failed_pause_result, GenServer.call(pid, {:pause_writes, lease})}
        )

        Process.sleep(:infinity)
      end)

    assert_receive {:failed_pause_result, {:error, {:flush_failed, :synthetic_eio}}}, 1_000

    assert %{writes_paused: true, write_pause_leases: leases} = :sys.get_state(pid)
    assert map_size(leases) == 0

    Process.exit(owner, :kill)

    assert eventually(fn ->
             state = :sys.get_state(pid)

             state.writes_paused and state.last_flush_error == :synthetic_eio and
               map_size(state.write_pause_leases) == 0
           end)
  end

  test "pause lease owner death releases only its standalone shard hold" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)

    owner = spawn(fn -> Process.sleep(:infinity) end)
    owner_lease = {owner, make_ref()}
    nested_lease = {self(), make_ref()}

    assert :ok = GenServer.call(pid, {:pause_writes, owner_lease})
    assert :ok = GenServer.call(pid, {:pause_writes, nested_lease})
    assert :ok = GenServer.call(pid, {:pause_writes, nested_lease})

    unrelated_resume = Task.async(fn -> GenServer.call(pid, {:resume_writes}) end)
    assert :ok = Task.await(unrelated_resume, 1_000)
    assert :sys.get_state(pid).writes_paused

    Process.exit(owner, :kill)

    assert eventually(fn ->
             state = :sys.get_state(pid)
             state.writes_paused and map_size(state.write_pause_leases) == 1
           end)

    assert :ok = GenServer.call(pid, {:resume_writes, owner_lease})
    assert :ok = GenServer.call(pid, {:resume_writes, owner_lease})
    assert :sys.get_state(pid).writes_paused

    assert :ok = GenServer.call(pid, {:resume_writes, nested_lease})
    refute :sys.get_state(pid).writes_paused
  end

  test "sole pause lease owner death resumes standalone shard writes" do
    {pid, ctx, data_dir} = start_shard([])
    on_exit(fn -> cleanup_shard(pid, ctx, data_dir) end)
    parent = self()

    owner =
      spawn(fn ->
        lease = {self(), make_ref()}
        send(parent, {:standalone_pause_acquired, GenServer.call(pid, {:pause_writes, lease})})
        Process.sleep(:infinity)
      end)

    assert_receive {:standalone_pause_acquired, :ok}, 1_000
    Process.exit(owner, :kill)

    assert eventually(fn -> not :sys.get_state(pid).writes_paused end)
    assert :ok = GenServer.call(pid, {:put, "pause-owner-down", "value", 0})
    assert "value" == GenServer.call(pid, {:get, "pause-owner-down"})
  end

  defp start_shard(opts) do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "standalone_commit_queue_#{System.unique_integer([:positive])}"
      )

    name = :"standalone_commit_queue_#{System.unique_integer([:positive])}"
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

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
