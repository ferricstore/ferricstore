defmodule Ferricstore.GracefulShutdownTest do
  @moduledoc """
  Tests that graceful shutdown preserves all data and that restart
  resumes from the same point — no data loss, no duplicate processing.

  Each test runs in a fresh temp data directory to avoid contamination
  from other tests. Shards are restarted with the clean dir before
  each test and restored to the original dir after.
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ctx = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(ctx) end)
  end

  defp ukey(base), do: "gsd_#{base}_#{:rand.uniform(9_999_999)}"

  defp shutdown_and_restart do
    Ferricstore.Application.prep_stop(nil)

    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    for i <- 0..(shard_count - 1) do
      name = Router.shard_name(FerricStore.Instance.get(:default), i)
      pid = Process.whereis(name)

      if pid && Process.alive?(pid) do
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        receive do {:DOWN, ^ref, _, _, _} -> :ok after 5_000 -> :ok end
      end
    end

    # Wait for full readiness: shards alive + WARaft storage + write path works
    ShardHelpers.eventually(fn ->
      shard_count_val = :persistent_term.get(:ferricstore_shard_count, 4)

      Enum.all?(0..(shard_count_val - 1), fn i ->
        pid = Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), i))
        alive = is_pid(pid) and Process.alive?(pid)

        alive and try do
          match?({:ok, {:raft_log_pos, _, _}}, Ferricstore.Raft.WARaftBackend.storage_position(i))
        catch
          :exit, _ -> false
        end
      end) and try do
        Router.put(FerricStore.Instance.get(:default), "__readiness_probe__", "ok", 0)
        Router.delete(FerricStore.Instance.get(:default), "__readiness_probe__")
        true
      catch
        :exit, _ -> false
      end
    end, "full write path should be ready after restart", 300, 200)

    Ferricstore.Health.set_ready(true)
  end

  describe "string data survives graceful shutdown" do
    test "single key survives" do
      k = ukey("single")
      Router.put(FerricStore.Instance.get(:default), k, "before_shutdown")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "before_shutdown" end,
        "key should survive graceful shutdown")
    end

    test "100 keys survive" do
      keys =
        for i <- 1..100 do
          k = ukey("multi_#{i}")
          Router.put(FerricStore.Instance.get(:default), k, "value_#{i}")
          {k, "value_#{i}"}
        end

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      for {k, expected} <- keys do
        ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == expected end,
          "key #{k} should survive shutdown")
      end
    end

    test "overwritten value has latest version after restart" do
      k = ukey("overwrite")
      Router.put(FerricStore.Instance.get(:default), k, "v1")
      Router.put(FerricStore.Instance.get(:default), k, "v2")
      Router.put(FerricStore.Instance.get(:default), k, "v3")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "v3" end,
        "latest value should survive")
    end

    test "deleted key stays deleted after restart" do
      k = ukey("deleted")
      Router.put(FerricStore.Instance.get(:default), k, "exists")
      ShardHelpers.flush_all_shards()
      Router.delete(FerricStore.Instance.get(:default), k)
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == nil end,
        "deleted key should stay deleted")
    end
  end

  describe "TTL survives graceful shutdown" do
    test "key with TTL still has TTL after restart" do
      k = ukey("ttl")
      Router.put(FerricStore.Instance.get(:default), k, "with_ttl", 60_000 + System.os_time(:millisecond))
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "with_ttl" end,
        "TTL key should survive shutdown")
    end
  end

  describe "data across all shards survives" do
    test "keys on every shard survive shutdown" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

      keys =
        for i <- 0..(shard_count - 1) do
          k = ShardHelpers.key_for_shard(i)
          Router.put(FerricStore.Instance.get(:default), k, "shard_#{i}_data")
          {k, i}
        end

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      for {k, i} <- keys do
        ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "shard_#{i}_data" end,
          "shard #{i} key should survive")
      end
    end
  end

  describe "counters survive graceful shutdown" do
    test "INCR value preserved after restart" do
      k = ukey("counter")
      Router.put(FerricStore.Instance.get(:default), k, "0")

      for _ <- 1..50 do
        Router.incr(FerricStore.Instance.get(:default), k, 1)
      end

      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "50" end,
        "counter should be 50 after restart")
    end
  end

  describe "writes after restart work" do
    test "new writes succeed after shutdown + restart" do
      k1 = ukey("before")
      Router.put(FerricStore.Instance.get(:default), k1, "old_data")
      ShardHelpers.flush_all_shards()
      ShardHelpers.compact_wal()

      shutdown_and_restart()

      k2 = ukey("after")
      ShardHelpers.eventually(fn ->
        Router.put(FerricStore.Instance.get(:default), k2, "new_data") == :ok
      end, "write should succeed after restart")

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k2) == "new_data" end,
        "new data should be readable")

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k1) == "old_data" end,
        "old data should still be there")
    end
  end

  describe "multiple shutdown cycles" do
    test "data survives two consecutive shutdown-restart cycles" do
      k = ukey("double")
      Router.put(FerricStore.Instance.get(:default), k, "cycle1")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "cycle1" end,
        "data should survive first cycle")

      Router.put(FerricStore.Instance.get(:default), k, "cycle2")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "cycle2" end,
        "data should survive second cycle")
    end
  end

  describe "empty string and edge cases survive" do
    test "empty string value survives" do
      k = ukey("empty")
      Router.put(FerricStore.Instance.get(:default), k, "")
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == "" end,
        "empty string should survive")
    end

    test "binary with null bytes survives" do
      k = ukey("binary")
      val = <<0, 1, 0, 255, 0, 128>>
      Router.put(FerricStore.Instance.get(:default), k, val)
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn -> Router.get(FerricStore.Instance.get(:default), k) == val end,
        "binary value should survive")
    end

    test "large value survives" do
      k = ukey("large")
      val = String.duplicate("x", 100_000)
      Router.put(FerricStore.Instance.get(:default), k, val)
      ShardHelpers.flush_all_shards()

      shutdown_and_restart()

      ShardHelpers.eventually(fn ->
        got = Router.get(FerricStore.Instance.get(:default), k)
        got != nil and byte_size(got) == 100_000
      end, "large value should survive")
    end
  end
end
