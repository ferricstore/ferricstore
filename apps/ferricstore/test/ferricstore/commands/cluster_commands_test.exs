defmodule Ferricstore.Commands.ClusterCommandsTest do
  @moduledoc """
  Unit tests for Ferricstore.Commands.Cluster command handlers.

  Tests the CLUSTER.HEALTH, CLUSTER.STATS, CLUSTER.KEYSLOT, and
  CLUSTER.SLOTS commands. Uses a MockStore where the store argument
  is required but not used by cluster commands.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Cluster

  setup do
    store = Ferricstore.Test.MockStore.make()
    {:ok, store: store}
  end

  # ---------------------------------------------------------------------------
  # CLUSTER.HEALTH
  # ---------------------------------------------------------------------------

  describe "CLUSTER.HEALTH" do
    test "returns cluster health info string", %{store: store} do
      result = Cluster.handle("CLUSTER.HEALTH", [], store)
      assert is_binary(result)
      assert result =~ "shard_0:"
      assert result =~ "role:"
      assert result =~ "status:"
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"
    end

    test "includes all shards", %{store: store} do
      result = Cluster.handle("CLUSTER.HEALTH", [], store)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert result =~ "shard_#{i}:",
               "expected shard_#{i} in CLUSTER.HEALTH output"
      end
    end

    test "reports leader or follower role for each shard", %{store: store} do
      result = Cluster.handle("CLUSTER.HEALTH", [], store)
      # In single-node mode, every shard should be "leader"
      assert result =~ "role: leader"
    end

    test "with extra args returns error", %{store: store} do
      result = Cluster.handle("CLUSTER.HEALTH", ["extra"], store)
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLUSTER.STATS
  # ---------------------------------------------------------------------------

  describe "CLUSTER.STATS" do
    test "returns stats string with per-shard and total info", %{store: store} do
      result = Cluster.handle("CLUSTER.STATS", [], store)
      assert is_binary(result)
      assert result =~ "shard_0:"
      assert result =~ "keys:"
      assert result =~ "memory_bytes:"
      assert result =~ "total_keys:"
      assert result =~ "total_memory_bytes:"
    end

    test "includes all shards", %{store: store} do
      result = Cluster.handle("CLUSTER.STATS", [], store)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert result =~ "shard_#{i}:",
               "expected shard_#{i} in CLUSTER.STATS output"
      end
    end

    test "with extra args returns error", %{store: store} do
      result = Cluster.handle("CLUSTER.STATS", ["extra"], store)
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLUSTER.KEYSLOT
  # ---------------------------------------------------------------------------

  describe "CLUSTER.KEYSLOT" do
    test "returns a non-negative integer for a key", %{store: store} do
      result = Cluster.handle("CLUSTER.KEYSLOT", ["mykey"], store)
      assert is_integer(result)
      assert result >= 0
      assert result < 1024
    end

    test "same key always returns same slot", %{store: store} do
      slot1 = Cluster.handle("CLUSTER.KEYSLOT", ["testkey"], store)
      slot2 = Cluster.handle("CLUSTER.KEYSLOT", ["testkey"], store)
      assert slot1 == slot2
    end

    test "different keys may return different slots", %{store: store} do
      # With 1024 slots, two different keys are extremely likely to differ
      slots =
        for i <- 0..99 do
          Cluster.handle("CLUSTER.KEYSLOT", ["key_#{i}"], store)
        end

      unique = Enum.uniq(slots)
      assert length(unique) > 1, "expected different keys to hash to different slots"
    end

    test "with no args returns error", %{store: store} do
      result = Cluster.handle("CLUSTER.KEYSLOT", [], store)
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end

    test "with too many args returns error", %{store: store} do
      result = Cluster.handle("CLUSTER.KEYSLOT", ["a", "b"], store)
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLUSTER.SLOTS
  # ---------------------------------------------------------------------------

  describe "CLUSTER.SLOTS" do
    test "returns a list of slot ranges", %{store: store} do
      result = Cluster.handle("CLUSTER.SLOTS", [], store)
      assert is_list(result)
      assert result != []

      # Each range should be [start, end, shard_index]
      Enum.each(result, fn range ->
        assert is_list(range)
        assert length(range) == 3
        [start_slot, end_slot, shard_index] = range
        assert is_integer(start_slot)
        assert is_integer(end_slot)
        assert is_integer(shard_index)
        assert start_slot >= 0
        assert end_slot >= start_slot
        assert end_slot < 1024
      end)
    end

    test "ranges cover all 1024 slots", %{store: store} do
      result = Cluster.handle("CLUSTER.SLOTS", [], store)

      total_slots =
        Enum.reduce(result, 0, fn [start_slot, end_slot, _shard], acc ->
          acc + (end_slot - start_slot + 1)
        end)

      assert total_slots == 1024
    end

    test "ranges reference valid shard indices", %{store: store} do
      result = Cluster.handle("CLUSTER.SLOTS", [], store)
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      Enum.each(result, fn [_start, _end, shard_index] ->
        assert shard_index >= 0 and shard_index < shard_count,
               "shard_index #{shard_index} out of range [0, #{shard_count})"
      end)
    end

    test "reported slot owners match the instance routing table", %{store: store} do
      result = Cluster.handle("CLUSTER.SLOTS", [], store)
      ctx = FerricStore.Instance.get(:default)

      for [start_slot, end_slot, shard_index] <- result,
          slot <- start_slot..end_slot do
        assert elem(ctx.slot_map, slot) == shard_index,
               "slot #{slot} reports shard #{shard_index}, but Router routes it to shard #{elem(ctx.slot_map, slot)}"
      end
    end

    test "with extra args returns error", %{store: store} do
      result = Cluster.handle("CLUSTER.SLOTS", ["extra"], store)
      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "CLUSTER.JOIN" do
    test "accepts REPLACE option for self-join", %{store: store} do
      node = Atom.to_string(node())
      assert :ok = Cluster.handle("CLUSTER.JOIN", [node, "REPLACE"], store)
    end

    test "rejects unknown CLUSTER.JOIN option", %{store: store} do
      assert {:error, "ERR syntax error"} =
               Cluster.handle("CLUSTER.JOIN", [Atom.to_string(node()), "UNKNOWN"], store)
    end

    test "rejects unknown node names without creating atoms", %{store: store} do
      node_name = "cluster_join_unknown_#{System.unique_integer([:positive])}@127.0.0.1"
      before_count = :erlang.system_info(:atom_count)

      assert {:error, "ERR unknown node" <> _} =
               Cluster.handle("CLUSTER.JOIN", [node_name], store)

      assert :erlang.system_info(:atom_count) == before_count
    end
  end

  describe "CLUSTER.STATUS" do
    test "includes replication marker details", %{store: store} do
      result = Cluster.handle("CLUSTER.STATUS", [], store)

      assert is_binary(result)
      assert result =~ "replication_mode:"
      assert result =~ "cluster_state:"
      assert result =~ "promotion_epoch="
      assert result =~ "barrier_indices="
    end
  end
end
