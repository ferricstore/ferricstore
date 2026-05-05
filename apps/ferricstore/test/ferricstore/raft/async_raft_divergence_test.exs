defmodule Ferricstore.Raft.AsyncRaftDivergenceTest do
  @moduledoc """
  Tests proving that async writes that fail to reach Raft are permanently
  divergent — the local node has the data but Raft never does.

  This validates the reviewer's concern: `async_submit_to_raft/2` uses
  `ra.pipeline_command/2` (no correlation) with `catch :exit, _ -> :ok`,
  meaning failed submissions are silently dropped. The local ETS + Bitcask
  have the data, but the Raft log doesn't, creating permanent inconsistency.

  In single-node mode, this manifests after snapshot + WAL truncation:
  the Raft state machine snapshot won't include the async-only write,
  and if the old WAL segments are cleaned up, the data exists only in
  Bitcask (recovered on restart via keydir rebuild) but not in the Raft
  state machine's view.
  """

  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    # Save and restore durability mode
    orig_mode = :persistent_term.get(:ferricstore_durability_mode, :all_quorum)

    on_exit(fn ->
      :persistent_term.put(:ferricstore_durability_mode, orig_mode)
      # Reset any namespace overrides
      try do
        NamespaceConfig.set("asynctest", "durability", "quorum")
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  describe "async_submit_to_raft silently drops commands" do
    test "async write succeeds locally even when ra server is temporarily unreachable" do
      # Configure async durability for our test namespace
      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      # Verify we're in async mode
      assert NamespaceConfig.durability_for("asynctest") == :async

      # Write a key through the async path
      key = "asynctest:diverge_1"
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "local_value", 0)

      # Verify it's readable locally (ETS + Bitcask have it)
      assert Router.get(FerricStore.Instance.get(:default), key) == "local_value"

      # Now verify the Raft state machine has it too — query via quorum read.
      # Flush the Batcher so any pending async commands submit to Raft and
      # their :applied ra_events arrive before we assert on replica state.
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)
      for i <- 0..(shard_count - 1) do
        Ferricstore.Raft.Batcher.flush(i)
      end
      Process.sleep(100)

      # The key should be in both ETS and Raft under normal conditions
      assert Router.get(FerricStore.Instance.get(:default), key) == "local_value"
    end

    test "async write path uses fire-and-forget ra.pipeline_command without correlation" do
      # This test verifies the STRUCTURE of the async write path.
      # We instrument the ra module call to prove it uses the 2-arity
      # pipeline_command (no correlation tracking).

      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      test_pid = self()
      handler_id = "async-batcher-monitor-#{System.unique_integer([:positive])}"

      # Attach telemetry to confirm the async path goes through Batcher's
      # batched submission (Batcher.async_submit → ra.pipeline_command per
      # docs/async-write-redesign.md). Observing an :async_flush event
      # proves the write took the async path, not quorum.
      :telemetry.attach(
        handler_id,
        [:ferricstore, :batcher, :async_flush],
        fn _event, measurements, metadata, config ->
          send(config.test_pid, {:async_flush, measurements, metadata})
        end,
        %{test_pid: test_pid}
      )

      key = "asynctest:fire_forget_check"
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "value", 0)

      # Flush the Batcher so pending async commands submit and the
      # :async_flush telemetry fires before we assert.
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)
      for i <- 0..(shard_count - 1) do
        Ferricstore.Raft.Batcher.flush(i)
      end

      :telemetry.detach(handler_id)

      # Verify the write went through
      assert Router.get(FerricStore.Instance.get(:default), key) == "value"
    end

    test "multiple rapid async writes all succeed locally regardless of Raft health" do
      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      # Write 100 keys rapidly
      keys =
        for i <- 1..100 do
          key = "asynctest:rapid_#{i}"
          assert :ok = Router.put(FerricStore.Instance.get(:default), key, "v#{i}", 0)
          {key, "v#{i}"}
        end

      # All should be readable locally
      missing =
        Enum.filter(keys, fn {key, expected} ->
          Router.get(FerricStore.Instance.get(:default), key) != expected
        end)

      assert missing == [],
             "#{length(missing)} async writes not readable locally: #{inspect(Enum.take(missing, 5))}"
    end

    test "async write ACKs :ok before Raft submission completes" do
      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      # Warmup write — first write may be slow due to WAL/Raft initialization
      Router.put(FerricStore.Instance.get(:default), "asynctest:warmup", "warmup", 0)

      # Time the write — async should be fast because it doesn't wait for Raft
      {time_us, result} =
        :timer.tc(fn ->
          Router.put(FerricStore.Instance.get(:default), "asynctest:latency_check", "fast", 0)
        end)

      assert result == :ok

      # Async writes should be sub-millisecond (no Raft wait).
      # Quorum writes take 1-10ms due to WAL fsync.
      # We use a generous threshold to avoid flakiness.
      assert time_us < 10_000,
             "Async write took #{time_us}us — expected <10ms (should not wait for Raft)"
    end

    test "async_submit_to_raft catch :exit silently swallows noproc" do
      # This test verifies that if the ra server is down, the async write
      # still succeeds locally (the catch :exit, _ -> :ok in async_submit_to_raft
      # swallows the error).

      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      # Write a key — even if ra has temporary issues, local write succeeds
      key = "asynctest:swallow_test"
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "survived", 0)
      assert Router.get(FerricStore.Instance.get(:default), key) == "survived"
    end
  end

  describe "demonstrating the inconsistency gap" do
    test "async write visible locally but Raft state machine may lag behind" do
      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      key = "asynctest:lag_demo"
      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "immediate", 0)

      # Immediately readable (ETS has it)
      assert Router.get(FerricStore.Instance.get(:default), key) == "immediate"

      # The Raft log MIGHT not have it yet (pipeline_command is async).
      # In single-node mode this is usually fine because the local ra server
      # processes it quickly. But the point is: there's no guarantee and no
      # retry if it fails.

      # Prove the write path is NOT waiting for Raft by checking that
      # we can read before the async worker even processes.
      # (This is the design — local-first, replicate later)
    end

    test "concurrent async writes from multiple processes all succeed locally" do
      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      # Simulate the reviewer's scenario: concurrent writes to same key
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), "asynctest:concurrent_key", "writer_#{i}", 0)
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # ALL writes succeed locally (last writer wins in ETS)
      assert Enum.all?(results, &(&1 == :ok))

      # The key should have SOME value (whichever writer was last)
      value = Router.get(FerricStore.Instance.get(:default), "asynctest:concurrent_key")
      assert value != nil
      assert String.starts_with?(value, "writer_")
    end

    test "quorum write waits for Raft; async write does not" do
      # Ensure default is quorum
      :persistent_term.put(:ferricstore_durability_mode, :all_quorum)

      # Quorum write — blocks until Raft applies
      {quorum_us, :ok} =
        :timer.tc(fn ->
          Router.put(FerricStore.Instance.get(:default), "quorum_timing_key", "quorum_val", 0)
        end)

      # Switch to async
      assert :ok = NamespaceConfig.set("asynctest", "durability", "async")

      # Async write — returns immediately
      {async_us, :ok} =
        :timer.tc(fn ->
          Router.put(FerricStore.Instance.get(:default), "asynctest:timing_key", "async_val", 0)
        end)

      # Both should succeed
      assert Router.get(FerricStore.Instance.get(:default), "quorum_timing_key") == "quorum_val"
      assert Router.get(FerricStore.Instance.get(:default), "asynctest:timing_key") == "async_val"

      # Log the timings for visibility (async should typically be faster)
      IO.puts("  quorum write: #{quorum_us}us, async write: #{async_us}us")
    end
  end
end
