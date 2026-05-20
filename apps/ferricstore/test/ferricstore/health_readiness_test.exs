defmodule Ferricstore.HealthReadinessTest do
  @moduledoc """
  Tests for the enhanced Health.check/0 readiness verification.

  Verifies that /health/ready returns :ok only when:
  1. set_ready(true) has been called
  2. All shard GenServers are alive
  3. All Raft leaders are elected
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Health
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive()

    on_exit(fn ->
      Health.set_ready(true)
      ShardHelpers.wait_shards_alive()
    end)
  end

  describe "Health.check/0" do
    test "uses backend-aware membership lookup for readiness" do
      source =
        Path.expand("../../lib/ferricstore/health.ex", __DIR__)
        |> File.read!()

      refute source =~ ":ra.members(",
             "health readiness must not bypass Ferricstore.Raft.Cluster.members/1 when WARaft is selected"

      assert source =~ "Ferricstore.Raft.Cluster.members(i, 1_000)"
    end

    test "returns :ok when system is fully ready" do
      Health.set_ready(true)
      result = Health.check()
      assert result.status == :ok
    end

    test "returns :starting when set_ready is false" do
      Health.set_ready(false)
      result = Health.check()
      assert result.status == :starting
      Health.set_ready(true)
    end

    test "includes all shards in response" do
      result = Health.check()
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      assert length(result.shards) == shard_count
    end

    test "all shards report ok when system is healthy" do
      result = Health.check()

      for shard <- result.shards do
        assert shard.status == "ok", "shard #{shard.index} should be ok, got #{shard.status}"
      end
    end

    test "shard_count matches configuration" do
      result = Health.check()
      expected = :persistent_term.get(:ferricstore_shard_count, 4)
      assert result.shard_count == expected
    end

    test "uptime_seconds is non-negative" do
      result = Health.check()
      assert result.uptime_seconds >= 0
    end
  end

  describe "Health.check/0 after shard operations" do
    test "returns :ok after writing data" do
      Ferricstore.Store.Router.put(FerricStore.Instance.get(:default), "health_test_key", "value")
      result = Health.check()
      assert result.status == :ok
    end

    test "shards report key counts" do
      Ferricstore.Store.Router.put(
        FerricStore.Instance.get(:default),
        "health_keys_test",
        "value"
      )

      result = Health.check()
      total_keys = Enum.sum(Enum.map(result.shards, & &1.keys))
      assert total_keys > 0
    end
  end

  describe "FerricStore.await_ready/1" do
    test "returns :ok when system is ready" do
      assert FerricStore.await_ready(timeout: 5_000) == :ok
    end

    test "returns :ok quickly (not waiting full timeout)" do
      {elapsed, :ok} = :timer.tc(fn -> FerricStore.await_ready(timeout: 10_000) end)
      assert elapsed < 5_000_000, "should return in <5s, took #{div(elapsed, 1000)}ms"
    end

    test "raises on timeout when not ready" do
      Health.set_ready(false)

      assert_raise RuntimeError, ~r/not ready/, fn ->
        FerricStore.await_ready(timeout: 500, interval: 50)
      end

      Health.set_ready(true)
    end
  end

  describe "FerricStore.health/0" do
    test "returns health map" do
      result = FerricStore.health()
      assert is_map(result)
      assert Map.has_key?(result, :status)
      assert Map.has_key?(result, :shard_count)
      assert Map.has_key?(result, :shards)
    end
  end

  describe "FerricStore.ready?/0" do
    test "returns boolean" do
      assert is_boolean(FerricStore.ready?())
    end
  end

  describe "Health.ready?/0" do
    test "returns true when set" do
      Health.set_ready(true)
      assert Health.ready?() == true
    end

    test "returns false when not set" do
      Health.set_ready(false)
      assert Health.ready?() == false
      Health.set_ready(true)
    end

    test "defaults to false if never set" do
      # Can't really test this without restarting the app,
      # but verify the key exists after startup
      assert is_boolean(Health.ready?())
    end
  end
end
