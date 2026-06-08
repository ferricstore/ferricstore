defmodule FerricstoreServer.Spec.ConnectionDistributionTest do
  @moduledoc """
  Spec section 2H: Connection Distribution tests.

  Verifies that CLUSTER.HEALTH and CLUSTER.STATS return proper data
  structures with the expected fields and semantics.

  Tests:
    - CLUSTER.HEALTH returns per-shard status, role, keys, memory_bytes
    - CLUSTER.STATS returns per-shard stats plus total_keys and total_memory_bytes
    - Data structures reflect actual shard state (keys written are counted)
    - Error cases: wrong number of arguments
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.MockStore

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  defp ukey(base), do: "conn_dist_#{base}_#{:rand.uniform(999_999)}"

  defp extract_total_keys(result) do
    case Regex.run(~r/total_keys: (\d+)/, result) do
      [_, n] -> String.to_integer(n)
      _ -> 0
    end
  end

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Section 2H: CLUSTER.HEALTH
  # ---------------------------------------------------------------------------

  describe "CLUSTER.HEALTH data structure (spec 2H)" do
    test "returns a string with per-shard role, status, keys, and memory_bytes" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      assert is_binary(result)

      lines = String.split(result, "\r\n")

      # Should have entries for each shard
      for i <- 0..(shard_count() - 1) do
        assert Enum.any?(lines, &String.contains?(&1, "shard_#{i}:")),
               "Missing shard_#{i} in CLUSTER.HEALTH output"
      end

      # Each shard should have role, status, keys, memory_bytes
      assert Enum.any?(lines, &String.contains?(&1, "role: leader"))
      assert Enum.any?(lines, &String.contains?(&1, "status:"))
      assert Enum.any?(lines, &String.contains?(&1, "keys:"))
      assert Enum.any?(lines, &String.contains?(&1, "memory_bytes:"))
    end

    test "status is 'ok' for all live shards" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      lines = String.split(result, "\r\n")
      status_lines = Enum.filter(lines, &String.contains?(&1, "status:"))

      # All shards should be alive in test
      Enum.each(status_lines, fn line ->
        assert String.contains?(line, "ok"),
               "Expected status 'ok' but got: #{inspect(line)}"
      end)
    end

    test "key counts reflect actual stored data" do
      k1 = ukey("health_a")
      k2 = ukey("health_b")

      Router.put(FerricStore.Instance.get(:default), k1, "v1", 0)
      Router.put(FerricStore.Instance.get(:default), k2, "v2", 0)

      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      # Parse key counts from each shard
      lines = String.split(result, "\r\n")

      key_counts =
        lines
        |> Enum.filter(&String.contains?(&1, "keys:"))
        |> Enum.map(fn line ->
          line
          |> String.trim()
          |> String.split(": ")
          |> List.last()
          |> String.to_integer()
        end)

      total_keys = Enum.sum(key_counts)
      assert total_keys >= 2

      # Cleanup
      Router.delete(FerricStore.Instance.get(:default), k1)
      Router.delete(FerricStore.Instance.get(:default), k2)
    end

    test "memory_bytes is non-negative for all shards" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      lines = String.split(result, "\r\n")

      memory_lines =
        lines
        |> Enum.filter(&String.contains?(&1, "memory_bytes:"))

      Enum.each(memory_lines, fn line ->
        bytes_str =
          line
          |> String.trim()
          |> String.split(": ")
          |> List.last()

        {bytes, ""} = Integer.parse(bytes_str)
        assert bytes >= 0
      end)
    end

    test "returns error with extra arguments" do
      store = MockStore.make()
      assert {:error, msg} = Dispatcher.dispatch("CLUSTER.HEALTH", ["extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # Section 2H: CLUSTER.STATS
  # ---------------------------------------------------------------------------

  describe "CLUSTER.STATS data structure (spec 2H)" do
    test "returns a string with per-shard keys, memory_bytes, and totals" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      assert is_binary(result)

      lines = String.split(result, "\r\n")

      # Per-shard entries
      for i <- 0..(shard_count() - 1) do
        assert Enum.any?(lines, &String.contains?(&1, "shard_#{i}:")),
               "Missing shard_#{i} in CLUSTER.STATS output"
      end

      # Total lines
      assert Enum.any?(lines, &String.starts_with?(&1, "total_keys:")),
             "Missing total_keys in CLUSTER.STATS output"

      assert Enum.any?(lines, &String.starts_with?(&1, "total_memory_bytes:")),
             "Missing total_memory_bytes in CLUSTER.STATS output"
    end

    test "total_keys equals sum of per-shard keys" do
      # Get baseline key count before adding our keys
      store_before = MockStore.make()
      result_before = Dispatcher.dispatch("CLUSTER.STATS", [], store_before)
      total_before = extract_total_keys(result_before)

      # Write some known keys
      keys = for i <- 1..5, do: ukey("stats_#{i}")

      Enum.each(keys, fn k ->
        Router.put(FerricStore.Instance.get(:default), k, "v", 0)
      end)

      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      lines = String.split(result, "\r\n")

      # Extract per-shard key counts
      per_shard_keys =
        lines
        |> Enum.filter(fn line ->
          trimmed = String.trim(line)
          String.starts_with?(trimmed, "keys:") and not String.starts_with?(trimmed, "total_keys:")
        end)
        |> Enum.map(fn line ->
          line
          |> String.trim()
          |> String.split(": ")
          |> List.last()
          |> String.to_integer()
        end)

      # Extract total_keys
      total_line =
        Enum.find(lines, &String.starts_with?(&1, "total_keys:"))

      [_, total_str] = String.split(total_line, ": ")
      total = String.to_integer(total_str)

      assert total == Enum.sum(per_shard_keys)
      assert total >= total_before + 5

      # Cleanup
      Enum.each(keys, fn k -> Router.delete(FerricStore.Instance.get(:default), k) end)
    end

    test "total_memory_bytes is non-negative" do
      store = MockStore.make()
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      lines = String.split(result, "\r\n")

      total_mem_line =
        Enum.find(lines, &String.starts_with?(&1, "total_memory_bytes:"))

      [_, bytes_str] = String.split(total_mem_line, ": ")
      {bytes, ""} = Integer.parse(bytes_str)
      assert bytes >= 0
    end

    test "returns error with extra arguments" do
      store = MockStore.make()
      assert {:error, msg} = Dispatcher.dispatch("CLUSTER.STATS", ["extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "adding keys increases total_keys" do
      store = MockStore.make()
      ctx = FerricStore.Instance.get(:default)

      # Add keys with unique names
      new_keys = for i <- 1..3, do: ukey("incr_stats_#{i}")

      # Get baseline AFTER flushing, right before adding
      total_before = extract_total_keys(Dispatcher.dispatch("CLUSTER.STATS", [], store))

      Enum.each(new_keys, fn k -> Router.put(ctx, k, "v", 0) end)

      # Verify each key is readable (proves they were written)
      Enum.each(new_keys, fn k ->
        assert Router.get(ctx, k) == "v"
      end)

      total_after = extract_total_keys(Dispatcher.dispatch("CLUSTER.STATS", [], store))
      assert total_after >= total_before + 3

      Enum.each(new_keys, fn k -> Router.delete(ctx, k) end)
    end
  end
end
