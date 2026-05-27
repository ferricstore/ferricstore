defmodule Ferricstore.WritePathQuorumTest do
  @moduledoc """
  Tests for quorum write bypass (direct ra.pipeline_command) and
  async WAL fdatasync, extracted from WritePathOptimizationsTest.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.{BitcaskWriter, Router}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper: unique key with a given prefix
  defp ukey(prefix), do: "#{prefix}:#{:rand.uniform(9_999_999)}"

  # =========================================================================
  # Quorum write bypass (direct ra.pipeline_command)
  # =========================================================================

  describe "quorum write bypass" do
    # All write commands should work through the quorum bypass path.
    # The default namespace durability is :quorum, so all writes to keys
    # with the default namespace bypass the Shard GenServer.

    test "SET through bypass" do
      key = ukey("qw")
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "bypass_val", 0)
      assert "bypass_val" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "DEL through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "to_delete", 0)
      assert :ok = Router.delete(FerricStore.Instance.get(:default), key)
      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "INCR through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "10", 0)
      assert {:ok, 11} = Router.incr(FerricStore.Instance.get(:default), key, 1)
    end

    test "INCRBY through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "100", 0)
      assert {:ok, 150} = Router.incr(FerricStore.Instance.get(:default), key, 50)
    end

    test "INCRBYFLOAT through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "10.5", 0)
      assert {:ok, result} = Router.incr_float(FerricStore.Instance.get(:default), key, 1.5)
      assert_in_delta result, 12.0, 0.001
    end

    test "APPEND through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "hello", 0)
      assert {:ok, 10} = Router.append(FerricStore.Instance.get(:default), key, "world")
      assert "helloworld" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "GETSET through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "old", 0)
      assert "old" == Router.getset(FerricStore.Instance.get(:default), key, "new")
      assert "new" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "GETDEL through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "gone", 0)
      assert "gone" == Router.getdel(FerricStore.Instance.get(:default), key)
      assert nil == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "GETEX through bypass" do
      key = ukey("qw")
      expire_at = System.os_time(:millisecond) + 60_000
      Router.put(FerricStore.Instance.get(:default), key, "getex_val", 0)
      assert "getex_val" == Router.getex(FerricStore.Instance.get(:default), key, expire_at)
    end

    test "SETRANGE through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "Hello World", 0)
      assert {:ok, 11} = Router.setrange(FerricStore.Instance.get(:default), key, 6, "Redis")
      assert "Hello Redis" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "CAS through bypass" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "original", 0)
      assert 1 == Router.cas(FerricStore.Instance.get(:default), key, "original", "updated", nil)
      assert "updated" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "CAS fails when expected value does not match" do
      key = ukey("qw")
      Router.put(FerricStore.Instance.get(:default), key, "actual", 0)
      assert 0 == Router.cas(FerricStore.Instance.get(:default), key, "wrong", "updated", nil)
      assert "actual" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "CAS on non-existent key returns nil" do
      key = ukey("qw_nonexist")
      assert nil == Router.cas(FerricStore.Instance.get(:default), key, "any", "new", nil)
    end

    test "LOCK/UNLOCK through bypass" do
      key = ukey("qw")
      assert :ok = Router.lock(FerricStore.Instance.get(:default), key, "owner1", 5_000)
      assert 1 = Router.unlock(FerricStore.Instance.get(:default), key, "owner1")
    end

    test "LOCK fails when already held" do
      key = ukey("qw")
      Router.lock(FerricStore.Instance.get(:default), key, "owner1", 5_000)
      assert {:error, _} = Router.lock(FerricStore.Instance.get(:default), key, "owner2", 5_000)
      Router.unlock(FerricStore.Instance.get(:default), key, "owner1")
    end

    test "EXTEND through bypass" do
      key = ukey("qw")
      Router.lock(FerricStore.Instance.get(:default), key, "owner1", 5_000)
      assert 1 = Router.extend(FerricStore.Instance.get(:default), key, "owner1", 10_000)
      Router.unlock(FerricStore.Instance.get(:default), key, "owner1")
    end

    test "read-your-own-writes after quorum SET" do
      key = ukey("ryow")
      Router.put(FerricStore.Instance.get(:default), key, "immediate", 0)
      assert "immediate" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "rapid SET+GET interleaving from same process" do
      key = ukey("interleave")

      for i <- 1..100 do
        val = "v#{i}"
        Router.put(FerricStore.Instance.get(:default), key, val, 0)
        assert val == Router.get(FerricStore.Instance.get(:default), key)
      end
    end

    test "SET from 100 concurrent processes all succeed" do
      keys =
        for i <- 1..100 do
          k = ukey("conc#{i}")
          {k, "val_#{i}"}
        end

      tasks =
        Enum.map(keys, fn {k, v} ->
          Task.async(fn -> {Router.put(FerricStore.Instance.get(:default), k, v, 0), k, v} end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 15_000))

      for {result, _k, _v} <- results do
        assert result == :ok
      end

      BitcaskWriter.flush_all()

      for {_result, k, v} <- results do
        assert v == Router.get(FerricStore.Instance.get(:default), k)
      end
    end
  end

  describe "WARaft durability" do
    test "SET returns :ok — write is durable after return" do
      key = ukey("wal")
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "durable", 0)
      assert "durable" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "concurrent SETs all return :ok — all durable" do
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            k = ukey("wal#{i}")
            {Router.put(FerricStore.Instance.get(:default), k, "v#{i}", 0), k, "v#{i}"}
          end)
        end

      results = Enum.map(tasks, &Task.await(&1, 15_000))

      for {result, _k, _v} <- results do
        assert result == :ok
      end

      for {_result, k, v} <- results do
        assert v == Router.get(FerricStore.Instance.get(:default), k)
      end
    end

    test "100 concurrent writers — no data loss" do
      keys =
        for i <- 1..100 do
          k = ukey("wal_loss#{i}")
          {k, "val_#{i}"}
        end

      tasks =
        Enum.map(keys, fn {k, v} ->
          Task.async(fn -> Router.put(FerricStore.Instance.get(:default), k, v, 0) end)
        end)

      Enum.each(tasks, &Task.await(&1, 15_000))
      BitcaskWriter.flush_all()

      missing =
        Enum.filter(keys, fn {k, v} ->
          Router.get(FerricStore.Instance.get(:default), k) != v
        end)

      assert missing == [], "Missing keys after concurrent writes: #{inspect(missing)}"
    end
  end
end
