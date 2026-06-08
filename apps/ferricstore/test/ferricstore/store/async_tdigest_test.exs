defmodule Ferricstore.Store.AsyncTDigestTest do
  @moduledoc """
  TDD tests for tdigest concurrency safety.

  tdigest commands (TDIGEST.ADD, TDIGEST.MERGE, TDIGEST.RESET) are
  read-modify-write operations that happen entirely in the caller's
  process:

      read serialized digest from ETS
      deserialize
      merge new values
      reserialize
      write back to ETS via Router.put

  Under concurrent load, two callers can read the same digest, merge
  their own values, and both write — losing one caller's updates.

  The fix is the same per-key latch pattern we use for plain RMW, but
  scoped to the full read-compute-write cycle inside the public
  FerricStore.tdigest_* functions.

  These tests fail before the latch wrapper is added: 50 concurrent
  tdigest_add calls each adding 10 values will produce a digest with
  fewer than 500 observations under the lost-update bug.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ukey(base), do: "td_#{base}_#{:erlang.unique_integer([:positive])}"

  # Extract total weight from tdigest_info result.
  # Info is `{:ok, [label, value, label, value, ...]}` where weights are
  # formatted as strings like "5.0". Sum them as floats.
  defp total_weight(key) do
    case FerricStore.tdigest_info(key) do
      {:ok, info} when is_list(info) -> sum_weights(info)
      info when is_list(info) -> sum_weights(info)
      other -> flunk("tdigest_info returned unexpected shape: #{inspect(other)}")
    end
  end

  defp sum_weights(info) do
    info
    |> Enum.chunk_every(2)
    |> Enum.reduce(0.0, fn
      [label, v], acc when label in ["Merged weight", "Unmerged weight"] ->
        acc + to_float(v)

      _, acc ->
        acc
    end)
  end

  defp to_float(x) when is_number(x), do: x * 1.0
  defp to_float(x) when is_binary(x) do
    case Float.parse(x) do
      {f, _} -> f
      :error -> 0.0
    end
  end

  # ---------------------------------------------------------------------------
  # Uncontended correctness sanity
  # ---------------------------------------------------------------------------

  describe "uncontended tdigest" do
    test "tdigest_add after create accumulates values" do
      key = ukey("create_add")
      :ok = FerricStore.tdigest_create(key)
      assert :ok = FerricStore.tdigest_add(key, [1.0, 2.0, 3.0])
      assert total_weight(key) == 3.0
    end

    test "tdigest_add followed by more adds accumulates" do
      key = ukey("sequential")
      :ok = FerricStore.tdigest_create(key)
      :ok = FerricStore.tdigest_add(key, [1.0, 2.0])
      :ok = FerricStore.tdigest_add(key, [3.0, 4.0, 5.0])
      assert total_weight(key) == 5.0
    end

    test "tdigest_reset zeroes the count" do
      key = ukey("reset")
      :ok = FerricStore.tdigest_create(key)
      :ok = FerricStore.tdigest_add(key, [1.0, 2.0, 3.0])
      assert total_weight(key) == 3.0
      :ok = FerricStore.tdigest_reset(key)
      assert total_weight(key) == 0.0
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrency — the lost-update scenario
  # ---------------------------------------------------------------------------

  describe "concurrent tdigest_add" do
    test "50 concurrent tdigest_add calls of 10 values each sum to 500" do
      key = ukey("concurrent_add")
      :ok = FerricStore.tdigest_create(key)

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            values = for i <- 1..10, do: i * 1.0
            FerricStore.tdigest_add(key, values)
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok))

      # The digest's total count must equal 50 × 10 = 500.
      # Before the latch fix, concurrent read-modify-write loses updates
      # and this would be significantly less than 500.
      assert total_weight(key) == 500.0,
             "expected 500 total observations, got #{total_weight(key)}; " <>
               "indicates concurrent tdigest_add is losing updates"
    end

    test "100 concurrent single-value adds all land" do
      key = ukey("single_value_add")
      :ok = FerricStore.tdigest_create(key)

      tasks =
        for i <- 1..100 do
          Task.async(fn -> FerricStore.tdigest_add(key, [i * 1.0]) end)
        end

      Task.await_many(tasks, 30_000)

      assert total_weight(key) == 100.0
    end
  end

  # ---------------------------------------------------------------------------
  # Mixed operations don't crash and preserve valid state
  # ---------------------------------------------------------------------------

  describe "concurrent mixed ops" do
    test "concurrent add + reset leave the digest in a valid state" do
      key = ukey("mixed")

      # Seed the digest.
      :ok = FerricStore.tdigest_create(key)
      FerricStore.tdigest_add(key, [1.0, 2.0, 3.0])

      adders =
        for _ <- 1..20 do
          Task.async(fn -> FerricStore.tdigest_add(key, [4.0, 5.0]) end)
        end

      resetter = Task.async(fn -> FerricStore.tdigest_reset(key) end)

      _ = Task.await_many(adders ++ [resetter], 30_000)

      # After the reset, some adds may have happened after the reset.
      # We can't predict the exact count — but it must be a non-negative
      # number, must not crash, and must be readable.
      w = total_weight(key)
      assert w >= 0.0 and w <= 3.0 + 20 * 2,
             "expected weight in [0, 43] after mixed add+reset, got #{w}"
    end
  end
end
