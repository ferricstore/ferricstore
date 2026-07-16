defmodule Ferricstore.FetchOrComputeWorkerArchitectureTest do
  use ExUnit.Case, async: false

  alias Ferricstore.FetchOrCompute.Worker
  alias Ferricstore.Test.IsolatedInstance

  defmodule Storage do
    @test_pid_key {__MODULE__, :test_pid}

    def set_test_pid(pid), do: :persistent_term.put(@test_pid_key, pid)

    def get(_ctx, "slow") do
      test_pid = :persistent_term.get(@test_pid_key)
      send(test_pid, {:storage_read_started, self()})

      receive do
        :continue_storage_read -> nil
      end
    end

    def get(_ctx, "published-during-lock") do
      case Process.get({__MODULE__, :published_during_lock}, 0) do
        0 ->
          Process.put({__MODULE__, :published_during_lock}, 1)
          nil

        _already_read ->
          "winner"
      end
    end

    def get(_ctx, _key), do: nil

    def fetch_or_compute_lock(_ctx, "slow", _token, _ttl_ms),
      do: {:error, :test_lock_rejected}

    def fetch_or_compute_lock(_ctx, _key, _token, _ttl_ms), do: :ok

    def fetch_or_compute_release(_ctx, "slow-release", _token) do
      block_storage_call(:storage_release_started)
    end

    def fetch_or_compute_release(_ctx, _key, _token), do: :ok

    def fetch_or_compute_publish(_ctx, "slow-publish", _value, _ttl_ms, _token) do
      block_storage_call(:storage_publish_started)
    end

    def fetch_or_compute_publish(_ctx, _key, _value, _ttl_ms, _token), do: :ok

    def fetch_or_compute_fail(_ctx, "slow-fail", _token, _message, _ttl_ms) do
      block_storage_call(:storage_failure_started)
    end

    def fetch_or_compute_fail(_ctx, _key, _token, _message, _ttl_ms), do: :ok
    def fetch_or_compute_outcome(_ctx, _key), do: :pending

    defp block_storage_call(message) do
      test_pid = :persistent_term.get(@test_pid_key)
      send(test_pid, {message, self()})

      receive do
        :continue_storage_call -> :ok
      end
    end
  end

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    Storage.set_test_pid(self())

    on_exit(fn ->
      :persistent_term.erase({Storage, :test_pid})
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "a blocked storage read does not block the partition mailbox", %{ctx: ctx} do
    assert :ok = Ferricstore.Store.Router.put(ctx, "slow", "current")
    {:ok, worker} = Worker.start_link(storage_module: Storage)

    request =
      Task.async(fn ->
        fetch(worker, ctx, "slow")
      end)

    assert_receive {:storage_read_started, storage_reader}, 500
    assert GenServer.call(worker, :coordinator_pid, 100) == worker

    send(storage_reader, :continue_storage_read)
    assert Task.await(request, 1_000) == {:error, :test_lock_rejected}
  end

  test "a partition bounds active distinct keys", %{ctx: ctx} do
    {:ok, worker} =
      Worker.start_link(storage_module: Storage, max_active_entries: 1, compute_timeout_ms: 5_000)

    assert {:compute, "hint", _token} = fetch(worker, ctx, "first")

    assert {:error, "ERR max active fetch_or_compute keys per partition reached"} =
             fetch(worker, ctx, "second")
  end

  test "a lock winner rechecks the cache before asking the caller to compute", %{ctx: ctx} do
    {:ok, worker} = Worker.start_link(storage_module: Storage)

    assert fetch(worker, ctx, "published-during-lock") == {:hit, "winner"}
  end

  test "a blocked publish does not block the partition mailbox", %{ctx: ctx} do
    {:ok, worker} = Worker.start_link(storage_module: Storage)

    publish =
      Task.async(fn ->
        GenServer.call(
          worker,
          {:fetch_or_compute_result, ctx, {ctx.name, "slow-publish"}, "slow-publish", "value",
           "token", 5_000},
          1_000
        )
      end)

    assert_receive {:storage_publish_started, storage_writer}, 500
    assert GenServer.call(worker, :coordinator_pid, 100) == worker

    send(storage_writer, :continue_storage_call)
    assert Task.await(publish, 1_000) == :ok
  end

  test "lease cleanup does not block the partition mailbox", %{ctx: ctx} do
    {:ok, worker} =
      Worker.start_link(storage_module: Storage, compute_timeout_ms: 100)

    assert {:compute, "hint", token} = fetch(worker, ctx, "slow-release")
    send(worker, {:fetch_or_compute_owner_timeout, {ctx.name, "slow-release"}, token})

    assert_receive {:storage_release_started, storage_writer}, 500
    assert GenServer.call(worker, :coordinator_pid, 100) == worker

    send(storage_writer, :continue_storage_call)
  end

  test "a blocked failure write does not block the partition mailbox", %{ctx: ctx} do
    {:ok, worker} = Worker.start_link(storage_module: Storage)

    failure =
      Task.async(fn ->
        GenServer.call(
          worker,
          {:fetch_or_compute_error, ctx, {ctx.name, "slow-fail"}, "slow-fail", "token", "failed"},
          1_000
        )
      end)

    assert_receive {:storage_failure_started, storage_writer}, 500
    assert GenServer.call(worker, :coordinator_pid, 100) == worker

    send(storage_writer, :continue_storage_call)
    assert Task.await(failure, 1_000) == :ok
  end

  defp fetch(worker, ctx, key) do
    GenServer.call(
      worker,
      {:fetch_or_compute, ctx, {ctx.name, key}, key, 5_000, "hint", self()},
      1_000
    )
  end
end
