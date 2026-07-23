defmodule Ferricstore.Flow.Query.AdmissionControllerTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Limits
  alias Ferricstore.Flow.Query.AdmissionController

  test "derives the default query pool from the node memory limit" do
    mib = 1_024 * 1_024
    constrained_pool = 64 * mib
    default_scope_pool = 64 * mib
    maximum_pool = 256 * mib
    constrained = start_controller(memory_limit_bytes: 256 * mib)
    roomy = start_controller(memory_limit_bytes: 2_048 * mib)

    assert %{
             max_node_memory_bytes: ^constrained_pool,
             max_scope_memory_bytes: ^default_scope_pool
           } = :sys.get_state(constrained)

    assert %{
             max_node_memory_bytes: ^maximum_pool,
             max_scope_memory_bytes: ^default_scope_pool
           } = :sys.get_state(roomy)
  end

  test "enforces independent scope and node memory ceilings" do
    server =
      start_controller(
        max_scope: 10,
        max_node: 10,
        max_scope_memory_bytes: 6,
        max_node_memory_bytes: 10
      )

    assert {:ok, first} = AdmissionController.acquire(server, :instance, "tenant-a", 4)

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "tenant-a", 3)

    assert {:ok, second} = AdmissionController.acquire(server, :instance, "tenant-b", 6)

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "tenant-c", 1)

    assert :ok = AdmissionController.release(server, first)
    assert {:ok, third} = AdmissionController.acquire(server, :instance, "tenant-c", 4)

    for lease <- [second, third], do: assert(:ok = AdmissionController.release(server, lease))
  end

  test "resizes lease memory atomically without weakening ownership" do
    server =
      start_controller(
        max_scope: 10,
        max_node: 10,
        max_scope_memory_bytes: 8,
        max_node_memory_bytes: 10
      )

    assert {:ok, first} = AdmissionController.acquire(server, :instance, "tenant-a", 4)
    assert {:ok, second} = AdmissionController.acquire(server, :instance, "tenant-b", 4)

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.resize_memory(server, first, 7)

    assert %{node_memory_bytes: 8} = :sys.get_state(server)

    parent = self()

    spawn(fn ->
      send(parent, {:resize, AdmissionController.resize_memory(server, first, 3)})
    end)

    assert_receive {:resize, {:error, :invalid_query_admission_lease}}
    assert :ok = AdmissionController.release(server, second)
    assert :ok = AdmissionController.resize_memory(server, first, 7)

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "tenant-a", 2)

    assert {:ok, third} = AdmissionController.acquire(server, :instance, "tenant-b", 3)
    assert :ok = AdmissionController.release(server, third)
    assert :ok = AdmissionController.release(server, first)

    assert %{node_memory_bytes: 0, scope_memory_bytes: %{}} = :sys.get_state(server)
  end

  test "owner death releases the lease memory reservation" do
    server =
      start_controller(
        max_scope: 2,
        max_node: 2,
        max_scope_memory_bytes: 10,
        max_node_memory_bytes: 10
      )

    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, _lease} = AdmissionController.acquire(server, :instance, "tenant", 10)
        send(parent, :memory_acquired)

        receive do
          :exit -> :ok
        end
      end)

    assert_receive :memory_acquired

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "other", 1)

    send(pid, :exit)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    eventually(fn ->
      match?({:ok, _lease}, AdmissionController.acquire(server, :instance, "other", 10))
    end)
  end

  test "cancels an acquisition committed after its caller times out" do
    parent = self()

    server =
      start_controller(
        max_scope: 3,
        max_node: 3,
        max_scope_memory_bytes: 30,
        max_node_memory_bytes: 30,
        index_active_fun: fn _instance, _identity ->
          send(parent, {:index_check, self()})

          receive do
            :continue_index_check -> {:ok, true}
          end
        end
      )

    worker =
      spawn(fn ->
        {:ok, lease} = AdmissionController.acquire(server, :instance, "worker", 10)
        send(parent, {:worker_lease, lease})

        receive do
          :pin ->
            result =
              AdmissionController.pin_index(
                server,
                lease,
                :instance,
                {"by-state", 1, "build-state-1"}
              )

            send(parent, {:pin_result, result})
        end

        receive do
          :release -> send(parent, {:worker_release, AdmissionController.release(server, lease)})
        end
      end)

    assert_receive {:worker_lease, _lease}
    send(worker, :pin)
    assert_receive {:index_check, controller}

    assert {:error, :query_engine_failure} =
             AdmissionController.acquire(server, :instance, "timed-out", 10)

    send(controller, :continue_index_check)
    assert_receive {:pin_result, {:error, :query_engine_failure}}

    eventually(fn ->
      match?(%{node_count: 1, node_memory_bytes: 10}, :sys.get_state(server))
    end)

    send(worker, :release)
    assert_receive {:worker_release, :ok}
  end

  test "rejects invalid memory reservations before admission" do
    server = start_controller(max_scope: 1, max_node: 1)

    for bytes <- [0, -1, :invalid] do
      assert {:error, :invalid_query_admission_memory} =
               AdmissionController.acquire(server, :instance, "tenant", bytes)
    end

    assert %{node_count: 0, node_memory_bytes: 0} = :sys.get_state(server)
  end

  test "enforces independent scope and node concurrency ceilings" do
    server = start_controller(max_scope: 2, max_node: 3)

    assert {:ok, first} = AdmissionController.acquire(server, :instance, "tenant-a")
    assert {:ok, second} = AdmissionController.acquire(server, :instance, "tenant-a")

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "tenant-a")

    assert {:ok, third} = AdmissionController.acquire(server, :instance, "tenant-b")

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "tenant-c")

    assert :ok = AdmissionController.release(server, first)
    assert {:ok, fourth} = AdmissionController.acquire(server, :instance, "tenant-c")

    for lease <- [second, third, fourth],
        do: assert(:ok = AdmissionController.release(server, lease))
  end

  test "reports active leases per instance for lifecycle draining" do
    server = start_controller(max_scope: 2, max_node: 4)

    assert {:ok, true} = AdmissionController.drained?(server, :instance_a)
    assert {:ok, lease} = AdmissionController.acquire(server, :instance_a, "tenant")
    assert {:ok, false} = AdmissionController.drained?(server, :instance_a)
    assert {:ok, true} = AdmissionController.drained?(server, :instance_b)

    assert :ok = AdmissionController.release(server, lease)
    assert {:ok, true} = AdmissionController.drained?(server, :instance_a)
  end

  test "fails drain checks closed during the orphan-lease restart grace" do
    clock = :atomics.new(1, signed: false)
    :atomics.put(clock, 1, 10_000)

    server =
      start_controller(
        max_scope: 1,
        max_node: 1,
        orphan_grace_ms: 750,
        clock_ms: fn -> :atomics.get(clock, 1) end
      )

    assert {:ok, false} = AdmissionController.drained?(server, :instance)
    assert {:ok, false} = AdmissionController.drained?(server, :instance, {"index", 1, "build"})

    :atomics.put(clock, 1, 10_750)

    assert {:ok, true} = AdmissionController.drained?(server, :instance)
    assert {:ok, true} = AdmissionController.drained?(server, :instance, {"index", 1, "build"})
  end

  test "fences and drains only leases pinned to the retiring index" do
    server = start_controller(max_scope: 4, max_node: 4)
    first_index = {"by-state", 1, "build-state-1"}
    second_index = {"by-type", 1, "build-type-1"}

    assert {:ok, first_lease} = AdmissionController.acquire(server, :instance, "tenant-a")
    assert :ok = AdmissionController.pin_index(server, first_lease, :instance, first_index)

    assert {:ok, unrelated_lease} =
             AdmissionController.acquire(server, :instance, "tenant-b")

    assert :ok = AdmissionController.fence_index(server, :instance, second_index)
    assert {:ok, true} = AdmissionController.drained?(server, :instance, second_index)
    assert {:ok, false} = AdmissionController.drained?(server, :instance, first_index)

    assert {:error, :query_index_retired} =
             AdmissionController.pin_index(server, unrelated_lease, :instance, second_index)

    assert :ok = AdmissionController.fence_index(server, :instance, first_index)
    assert {:ok, false} = AdmissionController.drained?(server, :instance, first_index)

    assert :ok = AdmissionController.release(server, first_lease)
    assert {:ok, true} = AdmissionController.drained?(server, :instance, first_index)

    assert :ok = AdmissionController.unfence_index(server, :instance, second_index)
    assert :ok = AdmissionController.pin_index(server, unrelated_lease, :instance, second_index)
    assert :ok = AdmissionController.release(server, unrelated_lease)
  end

  test "automatically releases a lease when its owner exits" do
    server = start_controller(max_scope: 1, max_node: 1)
    parent = self()

    pid =
      spawn(fn ->
        {:ok, _lease} = AdmissionController.acquire(server, :instance, "tenant")
        send(parent, :acquired)

        receive do
          :exit -> :ok
        end
      end)

    assert_receive :acquired

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "tenant")

    send(pid, :exit)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, reason} when reason in [:normal, :noproc]

    eventually(fn ->
      match?({:ok, _lease}, AdmissionController.acquire(server, :instance, "tenant"))
    end)
  end

  test "only the owner can release a live lease" do
    server = start_controller(max_scope: 1, max_node: 1)
    assert {:ok, lease} = AdmissionController.acquire(server, :instance, "tenant")
    parent = self()

    spawn(fn -> send(parent, {:release, AdmissionController.release(server, lease)}) end)
    assert_receive {:release, {:error, :invalid_query_admission_lease}}

    assert {:error, :query_concurrency_exceeded} =
             AdmissionController.acquire(server, :instance, "tenant")

    assert :ok = AdmissionController.release(server, lease)
  end

  test "with_permit releases on returned errors and exceptions" do
    server = start_controller(max_scope: 1, max_node: 1)

    assert {:error, :work_failed} =
             AdmissionController.with_permit(server, :instance, "tenant", fn ->
               {:error, :work_failed}
             end)

    assert_raise RuntimeError, "boom", fn ->
      AdmissionController.with_permit(server, :instance, "tenant", fn -> raise "boom" end)
    end

    assert {:ok, :done} =
             AdmissionController.with_permit(server, :instance, "tenant", fn -> {:ok, :done} end)
  end

  test "controller shutdown during admitted work does not replace the work result" do
    name = :"admission_shutdown_#{System.unique_integer([:positive, :monotonic])}"
    {:ok, server} = AdmissionController.start_link(name: name, max_scope: 1, max_node: 1)
    Process.unlink(server)

    assert {:ok, :done} =
             AdmissionController.with_permit(server, :instance, "tenant", fn ->
               :ok = GenServer.stop(server)
               {:ok, :done}
             end)
  end

  test "an unavailable controller fails closed without running admitted work" do
    missing = :"missing_admission_#{System.unique_integer([:positive, :monotonic])}"
    parent = self()

    assert {:error, :query_engine_failure} =
             AdmissionController.with_permit(missing, :instance, "tenant", fn ->
               send(parent, :work_ran)
               :ok
             end)

    refute_receive :work_ran
  end

  test "stores only an internal digest of the scope" do
    server = start_controller(max_scope: 1, max_node: 1)
    assert {:ok, lease} = AdmissionController.acquire(server, :instance, "secret-tenant")

    state = :sys.get_state(server)
    refute inspect(state) =~ "secret-tenant"

    assert Enum.all?(Map.keys(state.scope_counts), fn {:instance, digest} ->
             byte_size(digest) == 32
           end)

    assert :ok = AdmissionController.release(server, lease)
  end

  test "rejects an oversized scope before hashing or admission" do
    server = start_controller(max_scope: 1, max_node: 1)
    oversized = :binary.copy("x", Limits.max_partition_key_bytes() + 1)

    assert {:error, :invalid_query_admission_scope} =
             AdmissionController.acquire(server, :instance, oversized)

    assert %{node_count: 0, scope_counts: %{}} = :sys.get_state(server)
  end

  test "rejects index identities outside the hashed unsigned 64-bit contract" do
    server = start_controller(max_scope: 1, max_node: 1)
    assert {:ok, lease} = AdmissionController.acquire(server, :instance, "tenant")
    invalid = {"by-state", 0x1_0000_0000_0000_0000, "build-state-1"}

    assert {:error, :invalid_query_index_identity} =
             AdmissionController.pin_index(server, lease, :instance, invalid)

    assert {:error, :invalid_query_index_identity} =
             AdmissionController.fence_index(server, :instance, invalid)

    assert %{index_counts: counts, fenced_indexes: fenced} = :sys.get_state(server)
    assert counts == %{}
    assert MapSet.size(fenced) == 0
  end

  test "rejects a new pin when the exact index build is no longer active" do
    identity = {"by-state", 1, "retired-build"}

    server =
      start_controller(
        max_scope: 1,
        max_node: 1,
        index_active_fun: fn :instance, ^identity -> {:ok, false} end
      )

    assert {:ok, lease} = AdmissionController.acquire(server, :instance, "tenant")

    assert {:error, :query_index_retired} =
             AdmissionController.pin_index(server, lease, :instance, identity)

    assert %{index_counts: %{}} = :sys.get_state(server)
    assert :ok = AdmissionController.release(server, lease)
  end

  defp start_controller(opts) do
    name = :"admission_controller_#{System.unique_integer([:positive, :monotonic])}"

    opts =
      opts
      |> Keyword.put(:name, name)
      |> Keyword.put_new(:index_active_fun, fn _instance, _identity -> {:ok, true} end)
      |> Keyword.put_new(:orphan_grace_ms, 0)

    start_supervised!(Supervisor.child_spec({AdmissionController, opts}, id: name))
    name
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      eventually(fun, attempts - 1)
    end
  end
end
