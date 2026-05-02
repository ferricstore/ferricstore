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
  alias Ferricstore.Store.DiskPressure
  alias Ferricstore.Store.Router

  # Generates a unique key to prevent cross-test interference.
  defp ukey(base), do: "foc_#{base}_#{:rand.uniform(9_999_999)}"

  # Dummy store map -- native commands ignore it.
  defp dummy_store, do: %{}

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
      assert ["compute", ""] = result

      # Clean up the compute lock.
      FetchOrCompute.fetch_or_compute_error(key, "cleanup")
    end

    test "returns compute with the provided hint" do
      key = ukey("miss_with_hint")

      result =
        Native.handle(
          "FETCH_OR_COMPUTE",
          [key, "5000", "https://api.example.com/data"],
          dummy_store()
        )

      assert ["compute", "https://api.example.com/data"] = result

      # Clean up.
      FetchOrCompute.fetch_or_compute_error(key, "cleanup")
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE_RESULT stores value and returns OK
  # ===========================================================================

  describe "FETCH_OR_COMPUTE_RESULT" do
    test "stores value and returns :ok" do
      key = ukey("result_store")

      # Become the computer.
      ["compute", _hint] = Native.handle("FETCH_OR_COMPUTE", [key, "10000"], dummy_store())

      # Deliver the result.
      assert :ok =
               Native.handle(
                 "FETCH_OR_COMPUTE_RESULT",
                 [key, "computed_value", "10000"],
                 dummy_store()
               )

      # Value should now be in the store.
      assert "computed_value" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "stores value with TTL" do
      key = ukey("result_ttl")

      ["compute", _] = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())

      assert :ok =
               Native.handle("FETCH_OR_COMPUTE_RESULT", [key, "ttl_val", "5000"], dummy_store())

      assert "ttl_val" == Router.get(FerricStore.Instance.get(:default), key)
      {_val, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert expire_at_ms > System.os_time(:millisecond)
      assert expire_at_ms <= System.os_time(:millisecond) + 6_000
    end

    test "subsequent FETCH_OR_COMPUTE returns hit after result delivered" do
      key = ukey("result_then_hit")

      ["compute", _] = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      :ok = Native.handle("FETCH_OR_COMPUTE_RESULT", [key, "done", "5000"], dummy_store())

      # Now fetching the same key should return hit.
      result = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert ["hit", "done"] = result
    end

    test "returns write error and wakes waiters with error when storing result fails" do
      key = "foc:result_pressure_#{:erlang.unique_integer([:positive])}"
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)

      assert :ok = Ferricstore.NamespaceConfig.set("foc", "durability", "async")

      try do
        assert {:compute, "hint"} = FetchOrCompute.fetch_or_compute(key, 5_000, "hint")

        waiter =
          Task.async(fn ->
            FetchOrCompute.fetch_or_compute(key, 5_000, "hint")
          end)

        Process.sleep(50)
        DiskPressure.set(ctx, idx)

        assert {:error, "ERR disk pressure" <> _} =
                 FetchOrCompute.fetch_or_compute_result(key, "computed_value", 5_000)

        assert {:error, "ERR disk pressure" <> _} = Task.await(waiter, 1_000)
        assert nil == Router.get(ctx, key)
      after
        DiskPressure.clear(ctx, idx)
        Ferricstore.NamespaceConfig.set("foc", "durability", "quorum")
        FetchOrCompute.fetch_or_compute_error(key, "cleanup")
      end
    end
  end

  # ===========================================================================
  # Multiple callers: stampede protection
  # ===========================================================================

  describe "multiple callers stampede protection" do
    test "first gets :compute, others wait and receive the result" do
      key = ukey("stampede")

      # Spawn two waiter tasks that will block.
      waiter1 =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5000, "hint")
        end)

      # Give waiter1 time to become the computer.
      Process.sleep(50)

      waiter2 =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5000, "hint")
        end)

      # Give waiter2 time to register.
      Process.sleep(50)

      # waiter1 should have gotten :compute. Deliver the result.
      result1 = Task.await(waiter1, 100)
      assert {:compute, "hint"} = result1

      # Deliver the computed value.
      FetchOrCompute.fetch_or_compute_result(key, "stampede_val", 5000)

      # waiter2 should now unblock with the result.
      result2 = Task.await(waiter2, 1000)
      assert {:ok, "stampede_val"} = result2

      # Verify the value is in the store.
      assert "stampede_val" == Router.get(FerricStore.Instance.get(:default), key)
    end
  end

  # ===========================================================================
  # FETCH_OR_COMPUTE_ERROR wakes waiters with error
  # ===========================================================================

  describe "FETCH_OR_COMPUTE_ERROR" do
    test "wakes waiters with error" do
      key = ukey("error_wake")

      # Spawn a waiter that will become the computer.
      computer =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5000, "hint")
        end)

      Process.sleep(50)

      # Spawn a waiter.
      waiter =
        Task.async(fn ->
          FetchOrCompute.fetch_or_compute(key, 5000, "hint")
        end)

      Process.sleep(50)

      # Computer got :compute.
      assert {:compute, "hint"} = Task.await(computer, 100)

      # Report error.
      assert :ok =
               Native.handle(
                 "FETCH_OR_COMPUTE_ERROR",
                 [key, "db connection failed"],
                 dummy_store()
               )

      # Waiter should receive the error.
      result = Task.await(waiter, 1000)
      assert {:error, "db connection failed"} = result
    end

    test "returns :ok even with no waiters" do
      key = ukey("error_no_waiters")

      ["compute", _] = Native.handle("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert :ok = Native.handle("FETCH_OR_COMPUTE_ERROR", [key, "some error"], dummy_store())
    end

    test "returns :ok for non-existent compute lock" do
      key = ukey("error_no_lock")
      assert :ok = Native.handle("FETCH_OR_COMPUTE_ERROR", [key, "no lock"], dummy_store())
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
               Native.handle("FETCH_OR_COMPUTE_RESULT", ["k", "v", "abc"], dummy_store())

      assert msg =~ "not an integer"
    end

    test "FETCH_OR_COMPUTE_ERROR with wrong args returns error" do
      assert {:error, msg} = Native.handle("FETCH_OR_COMPUTE_ERROR", ["key"], dummy_store())
      assert msg =~ "wrong number of arguments"
    end

    test "FETCH_OR_COMPUTE_ERROR with too many args returns error" do
      assert {:error, msg} =
               Native.handle("FETCH_OR_COMPUTE_ERROR", ["k", "e", "extra"], dummy_store())

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
      ["compute", _] = Dispatcher.dispatch("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())

      assert :ok =
               Dispatcher.dispatch("FETCH_OR_COMPUTE_RESULT", [key, "v", "5000"], dummy_store())

      assert "v" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "FETCH_OR_COMPUTE_ERROR is routed through dispatcher" do
      key = ukey("disp_foce")
      ["compute", _] = Dispatcher.dispatch("FETCH_OR_COMPUTE", [key, "5000"], dummy_store())
      assert :ok = Dispatcher.dispatch("FETCH_OR_COMPUTE_ERROR", [key, "err"], dummy_store())
    end
  end
end
