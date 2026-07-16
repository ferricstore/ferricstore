defmodule Ferricstore.Commands.FetchOrComputeTest do
  @moduledoc """
  Tests for the FETCH_OR_COMPUTE, FETCH_OR_COMPUTE_RESULT, and
  FETCH_OR_COMPUTE_ERROR commands.

  These tests exercise cache-aside with stampede protection via the
  FetchOrCompute GenServer. They run against the application-supervised
  shards and the FetchOrCompute process.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Native
  alias Ferricstore.FetchOrCompute
  alias Ferricstore.Store.Router

  setup_all do
    Ferricstore.Test.ShardHelpers.wait_default_quorum_writable(60_000)
    :ok
  end

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "foc_#{base}_#{:rand.uniform(9_999_999)}"

  # Dummy store map -- native commands ignore it.
  defp dummy_store, do: %{}

  describe "performance guards" do
    test "local waiter registration does not append to the waiter list" do
      source =
        File.read!(Path.expand("lib/ferricstore/fetch_or_compute.ex", File.cwd!()))

      refute source =~ "waiters ++",
             "fetch_or_compute waiter enqueue must stay O(1); append makes same-key stampedes O(n^2)"

      refute source =~ "spawn_poller(from",
             "cross-node waiters must share one poller per key instead of spawning per caller"

      refute source =~ ":ets.tab2list",
             "monitor completion must use a monitor-ref index instead of scanning every key"

      assert source =~ "PartitionSupervisor",
             "independent cache keys must not serialize through one global coordinator"
    end
  end

  describe "lease lifecycle" do
    test "same raw key is coordinated and published independently per instance" do
      suffix = System.unique_integer([:positive, :monotonic])
      {ctx_a, stop_a} = start_custom_instance(:"foc-instance-a-#{suffix}")
      {ctx_b, stop_b} = start_custom_instance(:"foc-instance-b-#{suffix}")
      on_exit(fn -> stop_b.() end)
      on_exit(fn -> stop_a.() end)
      key = "same-cache-key"

      assert {:compute, "a", token_a} =
               FetchOrCompute.fetch_or_compute(ctx_a, key, 5_000, "a")

      assert {:compute, "b", token_b} =
               FetchOrCompute.fetch_or_compute(ctx_b, key, 5_000, "b")

      assert :ok =
               FetchOrCompute.fetch_or_compute_result(ctx_a, key, "value-a", token_a, 5_000)

      assert :ok =
               FetchOrCompute.fetch_or_compute_result(ctx_b, key, "value-b", token_b, 5_000)

      assert Router.get(ctx_a, key) == "value-a"
      assert Router.get(ctx_b, key) == "value-b"

      assert ["hit", "value-a"] =
               Native.handle("FETCH_OR_COMPUTE", [key, "5000"], %{__instance_ctx__: ctx_a})

      assert ["hit", "value-b"] =
               Native.handle("FETCH_OR_COMPUTE", [key, "5000"], %{__instance_ctx__: ctx_b})
    end

    @tag :fetch_or_compute_lifecycle
    test "releases a compute lease and wakes waiters when its owner dies" do
      key = ukey("owner_down")
      parent = self()

      owner =
        spawn(fn ->
          result = FetchOrCompute.fetch_or_compute(key, 5_000, "hint")
          send(parent, {:owner_result, self(), result})
          Process.sleep(:infinity)
        end)

      assert_receive {:owner_result, ^owner, {:compute, "hint", _token}}, 1_000

      waiter = Task.async(fn -> FetchOrCompute.fetch_or_compute(key, 5_000, "hint") end)
      Process.sleep(50)
      Process.exit(owner, :kill)

      assert {:ok, {:error, "compute owner terminated"}} = Task.yield(waiter, 1_000)

      assert {:compute, "hint", next_token} =
               FetchOrCompute.fetch_or_compute(key, 5_000, "hint")

      assert :ok = FetchOrCompute.fetch_or_compute_error(key, next_token, "cleanup")
    end

    @tag :fetch_or_compute_lifecycle
    test "uses the requested lease ttl instead of the coordinator default" do
      key = ukey("requested_ttl")
      parent = self()

      owner =
        spawn(fn ->
          result = FetchOrCompute.fetch_or_compute(key, 100, "hint")
          send(parent, {:ttl_owner_result, self(), result})
          Process.sleep(:infinity)
        end)

      on_exit(fn -> if Process.alive?(owner), do: Process.exit(owner, :kill) end)

      assert_receive {:ttl_owner_result, ^owner, {:compute, "hint", _token}}, 1_000
      waiter = Task.async(fn -> FetchOrCompute.fetch_or_compute(key, 100, "hint") end)

      assert {:ok, {:error, :timeout}} = Task.yield(waiter, 1_000)
    end

    @tag :fetch_or_compute_lifecycle
    test "propagates a replicated failure to waiters on another coordinator" do
      key = ukey("remote_failure")
      ctx = FerricStore.Instance.get(:default)
      remote_token = "remote-#{System.unique_integer([:positive, :monotonic])}"

      assert :ok = Router.fetch_or_compute_lock(ctx, key, remote_token, 5_000)

      waiter = Task.async(fn -> FetchOrCompute.fetch_or_compute(key, 5_000, "remote") end)
      Process.sleep(100)

      assert :ok =
               Router.fetch_or_compute_fail(
                 ctx,
                 key,
                 remote_token,
                 "remote compute failed",
                 5_000
               )

      assert {:ok, {:error, "remote compute failed"}} = Task.yield(waiter, 1_000)
    end

    @tag :fetch_or_compute_lifecycle
    test "routes independent keys across multiple coordinator partitions" do
      coordinator_pids =
        1..128
        |> Enum.map(fn index -> FetchOrCompute.coordinator_pid("partition-key-#{index}") end)
        |> Enum.uniq()

      assert length(coordinator_pids) > 1
    end

    @tag :fetch_or_compute_waiter_admission
    test "rejects waiters above the per-key cap without growing the entry" do
      key = ukey("waiter_cap")
      coordinator = FetchOrCompute.coordinator_pid(key)
      old_limit = :sys.get_state(coordinator) |> Map.get(:max_waiters_per_key)

      :sys.replace_state(coordinator, &Map.put(&1, :max_waiters_per_key, 1))

      assert {:compute, "hint", token} =
               FetchOrCompute.fetch_or_compute(key, 5_000, "hint")

      first = Task.async(fn -> FetchOrCompute.fetch_or_compute(key, 5_000, "hint") end)

      assert eventually(fn ->
               case FetchOrCompute.debug_entry(key) do
                 %{waiters: waiters} -> length(waiters) == 1
                 _ -> false
               end
             end)

      second = Task.async(fn -> FetchOrCompute.fetch_or_compute(key, 5_000, "hint") end)

      on_exit(fn ->
        if Process.alive?(first.pid), do: Process.exit(first.pid, :kill)
        if Process.alive?(second.pid), do: Process.exit(second.pid, :kill)
        :sys.replace_state(coordinator, &Map.put(&1, :max_waiters_per_key, old_limit))
        FetchOrCompute.fetch_or_compute_error(key, token, "cleanup")
      end)

      assert {:ok, {:error, "ERR max fetch_or_compute waiters per key reached"}} =
               Task.yield(second, 1_000)

      assert %{waiters: waiters} = FetchOrCompute.debug_entry(key)
      assert length(waiters) == 1
    end

    @tag :fetch_or_compute_waiter_cleanup
    test "removes a waiter as soon as its caller dies" do
      key = ukey("dead_waiter")

      assert {:compute, "hint", token} =
               FetchOrCompute.fetch_or_compute(key, 5_000, "hint")

      waiter = spawn(fn -> FetchOrCompute.fetch_or_compute(key, 5_000, "hint") end)

      on_exit(fn ->
        if Process.alive?(waiter), do: Process.exit(waiter, :kill)
        FetchOrCompute.fetch_or_compute_error(key, token, "cleanup")
      end)

      assert eventually(fn ->
               case FetchOrCompute.debug_entry(key) do
                 %{waiters: [{_from, ^waiter, _monitor_ref}]} -> true
                 _ -> false
               end
             end)

      Process.exit(waiter, :kill)

      assert eventually(fn ->
               case FetchOrCompute.debug_entry(key) do
                 %{waiters: []} -> true
                 _ -> false
               end
             end)
    end
  end

  describe "fenced compute ownership" do
    test "rejects a stale token without publishing or releasing the current owner" do
      key = ukey("stale_token")

      assert {:compute, "hint", first_token} =
               FetchOrCompute.fetch_or_compute(key, 5_000, "hint")

      assert :ok = FetchOrCompute.fetch_or_compute_error(key, first_token, "retry")

      assert {:compute, "hint", current_token} =
               FetchOrCompute.fetch_or_compute(key, 5_000, "hint")

      refute current_token == first_token

      assert {:error, reason} =
               FetchOrCompute.fetch_or_compute_result(key, "stale", first_token, 5_000)

      assert inspect(reason) =~ "owner"
      assert Router.get(FerricStore.Instance.get(:default), key) == nil

      assert :ok =
               FetchOrCompute.fetch_or_compute_result(key, "current", current_token, 5_000)

      assert Router.get(FerricStore.Instance.get(:default), key) == "current"
    end

    test "cross-node waiters share one poller for a missing key" do
      key = ukey("remote_coalescing")
      ctx = FerricStore.Instance.get(:default)
      remote_token = "remote-#{System.unique_integer([:positive, :monotonic])}"

      assert :ok = Router.fetch_or_compute_lock(ctx, key, remote_token, 5_000)

      waiters =
        for _ <- 1..3 do
          Task.async(fn -> FetchOrCompute.fetch_or_compute(key, 5_000, "remote") end)
        end

      assert eventually(fn ->
               case FetchOrCompute.debug_entry(key) do
                 %{kind: :remote, poller_pid: pid, waiters: registered} ->
                   Process.alive?(pid) and length(registered) == 3

                 _other ->
                   false
               end
             end)

      assert :ok =
               Router.fetch_or_compute_publish(ctx, key, "remote-value", 5_000, remote_token)

      assert Enum.map(waiters, &Task.await(&1, 1_000)) ==
               List.duplicate({:ok, "remote-value"}, 3)
    end

    test "raw command returns a protocol error when the shared poller terminates" do
      key = ukey("remote_poller_failure")
      ctx = FerricStore.Instance.get(:default)
      remote_token = "remote-#{System.unique_integer([:positive, :monotonic])}"

      assert :ok = Router.fetch_or_compute_lock(ctx, key, remote_token, 5_000)
      on_exit(fn -> Router.fetch_or_compute_release(ctx, key, remote_token) end)

      waiter =
        Task.async(fn ->
          Native.handle("FETCH_OR_COMPUTE", [key, "5000", "remote"], dummy_store())
        end)

      assert eventually(fn ->
               case FetchOrCompute.debug_entry(key) do
                 %{kind: :remote, poller_pid: pid} ->
                   Process.exit(pid, :kill)
                   true

                 _other ->
                   false
               end
             end)

      assert {:error, message} = Task.await(waiter, 1_000)
      assert message =~ "poller_terminated"
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE on existing key
  # ===========================================================================

  describe "FETCH_OR_COMPUTE on existing key" do
    test "returns hit with the cached value" do
      key = ukey("existing")
      Router.put(FerricStore.Instance.get(:default), key, "cached_value", 0)

      result = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert ["hit", "cached_value"] = result
    end

    test "returns hit with hint when key exists and hint is given" do
      key = ukey("existing_hint")
      Router.put(FerricStore.Instance.get(:default), key, "val", 0)

      result = Native.handle("FETCH_OR_COMPUTE", [key, "5000", "my_hint"], dummy_store())
      assert ["hit", "val"] = result
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE on missing key
  # ===========================================================================

  describe "FETCH_OR_COMPUTE on missing key" do
    test "returns compute with empty hint when no hint given" do
      key = ukey("miss_no_hint")

      result = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert ["compute", "", token] = result

      # Clean up the compute lock.
      FetchOrCompute.fetch_or_compute_error(key, token, "cleanup")
    end

    test "returns compute with the provided hint" do
      key = ukey("miss_with_hint")

      result =
        Native.handle(
          "FETCH_OR_COMPUTE",
          [key, "5000", "https://api.example.com/data"],
          dummy_store()
        )

      assert ["compute", "https://api.example.com/data", token] = result

      # Clean up.
      FetchOrCompute.fetch_or_compute_error(key, token, "cleanup")
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE_RESULT stores value and returns OK
  # ===========================================================================

  describe "FETCH_OR_COMPUTE_RESULT" do
    test "stores value and returns :ok" do
      key = ukey("result_store")

      # Become the computer.
      ["compute", _hint, token] =
        Native.handle("FETCH_OR_COMPUTE", [key, "10000"], dummy_store())

      # Deliver the result.
      assert :ok =
               Native.handle(
                 "FETCH_OR_COMPUTE_RESULT",
                 [key, token, "computed_value", "10000"],
                 dummy_store()
               )

      # Value should now be in the store.
      assert "computed_value" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "stores value with TTL" do
      key = ukey("result_ttl")

      ["compute", _, token] = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())

      assert :ok =
               Native.handle(
                 "FETCH_OR_COMPUTE_RESULT",
                 [key, token, "ttl_val", "5000"],
                 dummy_store()
               )

      assert "ttl_val" == Router.get(FerricStore.Instance.get(:default), key)
      {_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert expire_at_ms > System.os_time(:millisecond)
      assert expire_at_ms <= System.os_time(:millisecond) + 6_000
    end

    test "subsequent FETCH_OR_COMPUTE returns hit after result delivered" do
      key = ukey("result_then_hit")

      ["compute", _, token] = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())

      :ok =
        Native.handle(
          "FETCH_OR_COMPUTE_RESULT",
          [key, token, "done", "5000"],
          dummy_store()
        )

      # Now fetching the same key should return hit.
      result = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert ["hit", "done"] = result
    end
  end

  # ===========================================================================
  # Multiple callers: stampede protection
  # ===========================================================================

  describe "multiple callers stampede protection" do
    test "first gets :compute, others wait and receive the result" do
      key = ukey("stampede")

      assert {:compute, "hint", token} =
               FetchOrCompute.fetch_or_compute(key, 5000, "hint")

      waiter =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5000, "hint")
        end)

      Process.sleep(50)

      FetchOrCompute.fetch_or_compute_result(key, "stampede_val", token, 5000)

      assert {:ok, "stampede_val"} = Task.await(waiter, 1000)

      assert "stampede_val" == Router.get(FerricStore.Instance.get(:default), key)
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE_ERROR wakes waiters with error
  # ===========================================================================

  describe "FETCH_OR_COMPUTE_ERROR" do
    test "public API releases the fenced lease with its token" do
      key = ukey("public_error")

      assert {:ok, {:compute, "", token}} = FerricStore.fetch_or_compute(key, ttl: 5_000)
      assert :ok = FerricStore.fetch_or_compute_error(key, "upstream failed", token: token)

      assert {:ok, {:compute, "", next_token}} =
               FerricStore.fetch_or_compute(key, ttl: 5_000)

      refute next_token == token
      assert :ok = FerricStore.fetch_or_compute_error(key, "cleanup", token: next_token)
    end

    test "wakes waiters with error" do
      key = ukey("error_wake")

      assert {:compute, "hint", token} =
               FetchOrCompute.fetch_or_compute(key, 5000, "hint")

      waiter =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5000, "hint")
        end)

      Process.sleep(50)

      assert :ok =
               Native.handle(
                 "FETCH_OR_COMPUTE_ERROR",
                 [key, token, "db connection failed"],
                 dummy_store()
               )

      assert {:error, "db connection failed"} = Task.await(waiter, 1000)
    end

    test "returns :ok even with no waiters" do
      key = ukey("error_no_waiters")

      ["compute", _, token] = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())

      assert :ok =
               Native.handle("FETCH_OR_COMPUTE_ERROR", [key, token, "some error"], dummy_store())
    end

    test "rejects a token for a non-existent compute lock" do
      key = ukey("error_no_lock")

      assert {:error, reason} =
               Native.handle("FETCH_OR_COMPUTE_ERROR", [key, "missing", "no lock"], dummy_store())

      assert inspect(reason) =~ "owner"
    end
  end

  # ===========================================================================
  # Error cases: wrong number of arguments
  # ===========================================================================

  describe "argument errors" do
    test "FETCH_OR_COMPUTE with no args returns error" do
      assert {:error, msg} = Native.handle("FETCH_OR_COMPUTE", [], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "FETCH_OR_COMPUTE with one arg returns error" do
      assert {:error, msg} = Native.handle("FETCH_OR_COMPUTE", ["key"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "FETCH_OR_COMPUTE with too many args returns error" do
      assert {:error, msg} =
               Native.handle("FETCH_OR_COMPUTE", ["k", "5000", "h", "extra"], dummy_store())

      assert msg =~ "wrong number of arguments"
    end

    test "FETCH_OR_COMPUTE with non-integer ttl returns error" do
      assert {:error, msg} = Native.handle("FETCH_OR_COMPUTE", ["k", "abc"], dummy_store())
      assert msg =~ "not an integer"
    end

    test "FETCH_OR_COMPUTE with zero ttl returns error" do
      assert {:error, msg} = Native.handle("FETCH_OR_COMPUTE", ["k", "0"], dummy_store())
      assert msg =~ "not an integer"
    end

    test "FETCH_OR_COMPUTE_RESULT with wrong args returns error" do
      assert {:error, msg} = Native.handle("FETCH_OR_COMPUTE_RESULT", ["key"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "FETCH_OR_COMPUTE_RESULT with non-integer ttl returns error" do
      assert {:error, msg} =
               Native.handle(
                 "FETCH_OR_COMPUTE_RESULT",
                 ["k", "token", "v", "abc"],
                 dummy_store()
               )

      assert msg =~ "not an integer"
    end

    test "FETCH_OR_COMPUTE_ERROR with wrong args returns error" do
      assert {:error, msg} = Native.handle("FETCH_OR_COMPUTE_ERROR", ["key"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "FETCH_OR_COMPUTE_ERROR with too many args returns error" do
      assert {:error, msg} =
               Native.handle(
                 "FETCH_OR_COMPUTE_ERROR",
                 ["k", "token", "e", "extra"],
                 dummy_store()
               )

      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # Dispatcher integration
  # ===========================================================================

  describe "Dispatcher routing" do
    alias Ferricstore.Commands.Dispatcher

    test "FETCH_OR_COMPUTE is routed through dispatcher" do
      key = ukey("disp_foc")
      Router.put(FerricStore.Instance.get(:default), key, "cached", 0)

      result = Dispatcher.dispatch("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert ["hit", "cached"] = result
    end

    test "FETCH_OR_COMPUTE is case-insensitive" do
      key = ukey("disp_foc_ci")
      Router.put(FerricStore.Instance.get(:default), key, "val", 0)

      result = Dispatcher.dispatch("fetch_or_compute", [key, "5000"], dummy_store())
      assert ["hit", "val"] = result
    end

    test "FETCH_OR_COMPUTE_RESULT is routed through dispatcher" do
      key = ukey("disp_focr")
      # First become the computer.
      ["compute", _, token] =
        Dispatcher.dispatch("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())

      assert :ok =
               Dispatcher.dispatch(
                 "FETCH_OR_COMPUTE_RESULT",
                 [key, token, "v", "5000"],
                 dummy_store()
               )

      assert "v" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "FETCH_OR_COMPUTE_ERROR is routed through dispatcher" do
      key = ukey("disp_foce")

      ["compute", _, token] =
        Dispatcher.dispatch("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())

      assert :ok =
               Dispatcher.dispatch(
                 "FETCH_OR_COMPUTE_ERROR",
                 [key, token, "err"],
                 dummy_store()
               )
    end
  end

  defp eventually(fun, attempts \\ 100)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false

  defp start_custom_instance(name) do
    data_dir =
      Path.join(System.tmp_dir!(), "ferricstore_fetch_or_compute_#{Atom.to_string(name)}")

    {:ok, supervisor} =
      FerricStore.Instance.Supervisor.start_link(name,
        data_dir: data_dir,
        shard_count: 1,
        flow_shared_ref_backfill?: false
      )

    Process.unlink(supervisor)
    ctx = FerricStore.Instance.get(name)

    stop = fn ->
      if Process.alive?(supervisor), do: Supervisor.stop(supervisor)
      FerricStore.Instance.cleanup(name)
      File.rm_rf!(data_dir)
    end

    {ctx, stop}
  end
end
