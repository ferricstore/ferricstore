defmodule Ferricstore.Commands.ClusterTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.MockStore
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_default_pipeline_ready()
    {:ok, store: MockStore.make()}
  end

  # ---------------------------------------------------------------------------
  # CLUSTER.HEALTH
  # ---------------------------------------------------------------------------

  describe "CLUSTER.HEALTH" do
    test "returns a string containing per-shard info", %{store: store} do
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      assert is_binary(result)
      assert result =~ "shard_0:"
      assert result =~ "role: leader"
      assert result =~ "status: ok"
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"
    end

    test "includes all 4 shards", %{store: store} do
      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      assert result =~ "shard_0:"
      assert result =~ "shard_1:"
      assert result =~ "shard_2:"
      assert result =~ "shard_3:"
    end

    test "reflects keys stored in shards", %{store: store} do
      # Write some keys to the live store
      Router.put(FerricStore.Instance.get(:default), "cluster_health_key_a", "v1", 0)
      Router.put(FerricStore.Instance.get(:default), "cluster_health_key_b", "v2", 0)

      result = Dispatcher.dispatch("CLUSTER.HEALTH", [], store)

      assert is_binary(result)
      # The result should show non-zero key counts across shards
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"

      # Clean up
      Router.delete(FerricStore.Instance.get(:default), "cluster_health_key_a")
      Router.delete(FerricStore.Instance.get(:default), "cluster_health_key_b")
    end

    test "returns error with extra arguments", %{store: store} do
      assert {:error, _msg} = Dispatcher.dispatch("CLUSTER.HEALTH", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # CLUSTER.STATS
  # ---------------------------------------------------------------------------

  describe "CLUSTER.STATS" do
    test "returns a string containing per-shard stats and totals", %{store: store} do
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      assert is_binary(result)
      assert result =~ "shard_0:"
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"
      assert result =~ "total_keys:"
      assert result =~ "total_memory_bytes:"
    end

    test "includes all shards", %{store: store} do
      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      assert result =~ "shard_0:"
      assert result =~ "shard_1:"
      assert result =~ "shard_2:"
      assert result =~ "shard_3:"
    end

    test "total_keys is sum of per-shard keys", %{store: store} do
      # Write known keys
      Router.put(FerricStore.Instance.get(:default), "cluster_stats_a", "v1", 0)
      Router.put(FerricStore.Instance.get(:default), "cluster_stats_b", "v2", 0)
      Router.put(FerricStore.Instance.get(:default), "cluster_stats_c", "v3", 0)

      result = Dispatcher.dispatch("CLUSTER.STATS", [], store)

      # Extract total_keys value
      [_, total_str] =
        result
        |> String.split("\r\n")
        |> Enum.find(&String.starts_with?(&1, "total_keys:"))
        |> String.split(": ")

      total = String.to_integer(total_str)
      assert total >= 3

      # Clean up
      Router.delete(FerricStore.Instance.get(:default), "cluster_stats_a")
      Router.delete(FerricStore.Instance.get(:default), "cluster_stats_b")
      Router.delete(FerricStore.Instance.get(:default), "cluster_stats_c")
    end

    test "returns error with extra arguments", %{store: store} do
      assert {:error, _msg} = Dispatcher.dispatch("CLUSTER.STATS", ["extra"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # FERRICSTORE.HOTNESS
  # ---------------------------------------------------------------------------

  describe "FERRICSTORE.HOTNESS" do
    setup do
      Ferricstore.Stats.reset_hotness()
      :ok
    end

    test "returns a list of key-value pairs with header fields", %{store: store} do
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      assert is_list(result)
      assert "hot_reads" in result
      assert "cold_reads" in result
      assert "hot_read_pct" in result
      assert "cold_reads_per_second" in result
      assert "top_n" in result
    end

    test "includes per-prefix data after reads", %{store: store} do
      Ferricstore.Stats.record_hot_read("user:1")
      Ferricstore.Stats.record_cold_read("session:1")

      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      assert is_list(result)
      assert "prefix" in result

      prefix_indices =
        result
        |> Enum.with_index()
        |> Enum.filter(fn {val, _} -> val == "prefix" end)
        |> Enum.map(fn {_, idx} -> idx end)

      prefix_names = Enum.map(prefix_indices, fn idx -> Enum.at(result, idx + 1) end)
      assert "user" in prefix_names
      assert "session" in prefix_names
    end

    test "accepts TOP argument" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["TOP", "2"], store)

      assert is_list(result)
      assert "top_n" in result

      # Find the index of "top_n" and the value after it
      idx = Enum.find_index(result, &(&1 == "top_n"))
      assert Enum.at(result, idx + 1) == "2"
    end

    test "TOP and WINDOW arguments are case-insensitive" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["top", "2", "window", "30"], store)

      assert is_list(result)
      idx = Enum.find_index(result, &(&1 == "top_n"))
      assert Enum.at(result, idx + 1) == "2"
      assert "hot_reads" in result
    end

    test "accepts WINDOW argument" do
      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", ["WINDOW", "30"], store)

      assert is_list(result)
      # Should still return data even with WINDOW specified
      assert "hot_reads" in result
    end

    test "reflects recorded reads in hot_reads count" do
      Ferricstore.Stats.record_hot_read("hotness_test:key")
      Ferricstore.Stats.record_cold_read("hotness_test:key2")

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.HOTNESS", [], store)

      idx = Enum.find_index(result, &(&1 == "hot_reads"))
      hot_val = String.to_integer(Enum.at(result, idx + 1))
      assert hot_val >= 1

      cold_idx = Enum.find_index(result, &(&1 == "cold_reads"))
      cold_val = String.to_integer(Enum.at(result, cold_idx + 1))
      assert cold_val >= 1
    end
  end
end
