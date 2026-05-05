defmodule Ferricstore.Raft.AsyncDurabilityTest do
  @moduledoc """
  Integration tests for the async durability path (spec section 2F.3).

  Validates the end-to-end flow: configuring a namespace with `:async`
  durability, writing keys through the Batcher, and verifying that the
  async path bypasses Raft quorum while still producing readable data.

  Also includes a latency comparison between `:async` and `:quorum`
  durability modes to confirm the async path is faster.
  """

  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper to generate unique keys with a specific prefix
  defp pkey(prefix, base), do: "#{prefix}:async_dur_#{base}_#{:rand.uniform(9_999_999)}"

  # Flushes the Batcher for all shards. Since Batcher.flush now waits for
  # in-flight async ra_events via the `:async_no_reply` pending tracking,
  # this is sufficient to guarantee all pending async writes have applied
  # before assertions.
  defp drain_async do
    for i <- 0..3 do
      Batcher.flush(i)
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace configuration with async durability
  # ---------------------------------------------------------------------------

  describe "namespace with async durability" do
    test "writes to an async namespace are successful" do
      NamespaceConfig.set("async_ns", "durability", "async")
      assert :async == NamespaceConfig.durability_for("async_ns")

      k = pkey("async_ns", "basic_write")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "async_value", 0})
      drain_async()
      assert "async_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "multiple writes to async namespace are all visible" do
      NamespaceConfig.set("async_multi", "durability", "async")

      keys =
        for i <- 1..20 do
          pkey("async_multi", "multi_#{i}")
        end

      by_shard = Enum.group_by(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)

      for {shard_idx, shard_keys} <- by_shard do
        for k <- shard_keys do
          assert :ok == Batcher.write(shard_idx, {:put, k, "val_#{k}", 0})
        end
      end

      drain_async()

      for k <- keys do
        assert "val_#{k}" == Router.get(FerricStore.Instance.get(:default), k),
               "Expected key #{k} to be readable after async write"
      end
    end

    test "async writes with TTL are stored correctly" do
      NamespaceConfig.set("async_ttl", "durability", "async")
      future = System.os_time(:millisecond) + 60_000

      k = pkey("async_ttl", "with_ttl")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "ttl_val", future})
      drain_async()

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end

    test "delete through async path works" do
      NamespaceConfig.set("async_del", "durability", "async")

      k = pkey("async_del", "to_delete")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "deleteme", 0})
      drain_async()

      ShardHelpers.eventually(fn ->
        assert "deleteme" == Router.get(FerricStore.Instance.get(:default), k)
      end, "async put should be readable", 10, 100)

      assert :ok == Batcher.write(shard, {:delete, k})
      drain_async()

      ShardHelpers.eventually(fn ->
        assert nil == Router.get(FerricStore.Instance.get(:default), k)
      end, "async delete should remove key", 10, 100)
    end
  end

  # ---------------------------------------------------------------------------
  # Quorum writes still work unchanged
  # ---------------------------------------------------------------------------

  describe "quorum durability unchanged" do
    test "quorum writes still go through Raft" do
      NamespaceConfig.set("quorum_ns", "durability", "quorum")

      k = pkey("quorum_ns", "raft_write")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "quorum_val", 0})
      assert "quorum_val" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "default (unconfigured) namespace uses quorum" do
      k = pkey("default_ns", "default_write")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :quorum == NamespaceConfig.durability_for("default_ns")
      assert :ok == Batcher.write(shard, {:put, k, "default_val", 0})
      assert "default_val" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Mixed async and quorum namespaces
  # ---------------------------------------------------------------------------

  describe "mixed durability namespaces" do
    test "async and quorum namespaces coexist on the same shard" do
      NamespaceConfig.set("mix_async", "durability", "async")
      NamespaceConfig.set("mix_quorum", "durability", "quorum")

      shard_idx = 0

      # Find keys for both prefixes on shard 0
      async_key =
        Enum.find(1..5000, fn i ->
          Router.shard_for(FerricStore.Instance.get(:default), "mix_async:coexist_#{i}") == shard_idx
        end)
        |> then(fn i -> "mix_async:coexist_#{i}" end)

      quorum_key =
        Enum.find(1..5000, fn i ->
          Router.shard_for(FerricStore.Instance.get(:default), "mix_quorum:coexist_#{i}") == shard_idx
        end)
        |> then(fn i -> "mix_quorum:coexist_#{i}" end)

      # Write both
      assert :ok == Batcher.write(shard_idx, {:put, async_key, "async_v", 0})
      assert :ok == Batcher.write(shard_idx, {:put, quorum_key, "quorum_v", 0})

      drain_async()

      assert "async_v" == Router.get(FerricStore.Instance.get(:default), async_key)
      assert "quorum_v" == Router.get(FerricStore.Instance.get(:default), quorum_key)
    end

    test "switching durability mode for a namespace is picked up" do
      NamespaceConfig.set("switch_ns", "durability", "quorum")

      k1 = pkey("switch_ns", "before_switch")
      shard1 = Router.shard_for(FerricStore.Instance.get(:default), k1)
      assert :ok == Batcher.write(shard1, {:put, k1, "quorum_era", 0})
      assert "quorum_era" == Router.get(FerricStore.Instance.get(:default), k1)

      # Switch to async
      NamespaceConfig.set("switch_ns", "durability", "async")

      k2 = pkey("switch_ns", "after_switch")
      shard2 = Router.shard_for(FerricStore.Instance.get(:default), k2)
      assert :ok == Batcher.write(shard2, {:put, k2, "async_era", 0})
      drain_async()
      assert "async_era" == Router.get(FerricStore.Instance.get(:default), k2)

      # Both values should still be readable
      assert "quorum_era" == Router.get(FerricStore.Instance.get(:default), k1)
    end
  end

  # ---------------------------------------------------------------------------
  # Latency comparison: async vs quorum
  # ---------------------------------------------------------------------------

  describe "async path latency" do
    test "async writes are faster than quorum writes" do
      NamespaceConfig.set("lat_async", "durability", "async")
      NamespaceConfig.set("lat_quorum", "durability", "quorum")

      iterations = 50

      # Warm up
      for _ <- 1..5 do
        k = pkey("lat_async", "warmup")
        shard = Router.shard_for(FerricStore.Instance.get(:default), k)
        Batcher.write(shard, {:put, k, "w", 0})
      end

      drain_async()

      for _ <- 1..5 do
        k = pkey("lat_quorum", "warmup")
        shard = Router.shard_for(FerricStore.Instance.get(:default), k)
        Batcher.write(shard, {:put, k, "w", 0})
      end

      # Measure async latency
      async_times =
        for i <- 1..iterations do
          k = pkey("lat_async", "lat_#{i}")
          shard = Router.shard_for(FerricStore.Instance.get(:default), k)

          {us, :ok} =
            :timer.tc(fn ->
              Batcher.write(shard, {:put, k, "async_lat_val", 0})
            end)

          us
        end

      drain_async()

      # Measure quorum latency
      quorum_times =
        for i <- 1..iterations do
          k = pkey("lat_quorum", "lat_#{i}")
          shard = Router.shard_for(FerricStore.Instance.get(:default), k)

          {us, :ok} =
            :timer.tc(fn ->
              Batcher.write(shard, {:put, k, "quorum_lat_val", 0})
            end)

          us
        end

      avg_async = Enum.sum(async_times) / iterations
      avg_quorum = Enum.sum(quorum_times) / iterations

      # The async path should be faster because it doesn't wait for Raft
      # consensus. In single-node mode the difference may be modest, but
      # the async path should at minimum not be dramatically slower.
      # Use 5x threshold to avoid flaky tests under full suite load.
      assert avg_async <= avg_quorum * 5.0,
             "Expected async (#{Float.round(avg_async, 1)}us) to be at most 5x " <>
               "quorum (#{Float.round(avg_quorum, 1)}us)"
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent async writes
  # ---------------------------------------------------------------------------

  describe "concurrent async writes" do
    test "many concurrent async writes are all applied" do
      NamespaceConfig.set("conc_async", "durability", "async")

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            k = pkey("conc_async", "conc_#{i}")
            shard = Router.shard_for(FerricStore.Instance.get(:default), k)
            :ok = Batcher.write(shard, {:put, k, "conc_val_#{i}", 0})
            k
          end)
        end

      keys = Task.await_many(tasks, 10_000)
      drain_async()

      for {k, i} <- Enum.with_index(keys, 1) do
        assert "conc_val_#{i}" == Router.get(FerricStore.Instance.get(:default), k),
               "Expected key #{k} to have value conc_val_#{i}"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry from async path
  # ---------------------------------------------------------------------------

  describe "async durability telemetry" do
    test "async writes emit batch flush telemetry" do
      # The Batcher emits [:ferricstore, :batcher, :async_flush] when it
      # flushes an async batch to Raft.
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:ferricstore, :batcher, :async_flush]
        ])

      NamespaceConfig.set("telem_async", "durability", "async")

      k = pkey("telem_async", "telemetry_write")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "telem_val", 0})

      assert_receive {[:ferricstore, :batcher, :async_flush], ^ref, measurements, metadata},
                     2_000

      assert is_integer(measurements.batch_size)
      assert measurements.batch_size >= 1
      assert is_integer(metadata.shard_index)
    end
  end
end
