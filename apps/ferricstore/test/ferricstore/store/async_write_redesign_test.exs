defmodule Ferricstore.Store.AsyncWriteRedesignTest do
  @moduledoc """
  TDD tests for the async write redesign (docs/async-write-redesign.md).

  Behavior under test:
  - Async writes go through the Batcher and are submitted to Raft as
    batched ra.pipeline_command({:batch, [...]}) calls.
  - Async commands are wrapped as `{:async, inner_cmd}` before reaching the
    state machine so apply/3 can distinguish them.
  - State machine on origin (ETS has entry) skips Bitcask + ETS writes to
    avoid double-writing.
  - State machine on replica (ETS empty) applies inner_cmd normally.
  - Read-your-writes holds for both small and large values on the origin.
  - Concurrent writes to the same key land in correct order via BitcaskWriter.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @ns "rdesign_async"

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns, "durability", "async")
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      Ferricstore.Test.ShardHelpers.flush_all_keys()
    end)

    Process.put(:test_ctx, ctx)
    :ok
  end

  defp ctx, do: Process.get(:test_ctx)

  # ---------------------------------------------------------------------------
  # Pipeline / Batcher routing
  # ---------------------------------------------------------------------------

  describe "async routing" do
    test "suite context exercises the Raft async path" do
      key = "#{@ns}:ctx_probe"

      assert ctx().raft_enabled
      assert Router.durability_for_key_public(ctx(), key) == :async
    end

    test "async writes produce batched ra.pipeline_command submissions" do
      # This test requires the Raft-backed default instance (Batcher submits to Raft).
      default_ctx = FerricStore.Instance.get(:default)
      handler_id = {:redesign_test, :batcher_async}

      _ =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :batcher, :async_flush],
          fn _event, meas, _meta, test_pid ->
            send(test_pid, {:batcher_async_flush, meas})
          end,
          self()
        )

      try do
        tasks =
          for i <- 1..50 do
            Task.async(fn -> Router.put(default_ctx, "#{@ns}:batch_#{i}", "v#{i}", 0) end)
          end

        Task.await_many(tasks, 5_000)

        assert_receive {:batcher_async_flush, %{batch_size: size}}, 2_000
        assert size >= 2, "expected batched submission, got batch_size=#{size}"
      after
        :telemetry.detach(handler_id)
      end
    end

    test "async PUT does not become locally visible when Batcher is overloaded" do
      key = "#{@ns}:overloaded_put_#{:erlang.unique_integer([:positive])}"
      idx = Router.shard_for(ctx(), key)

      on_exit(fn -> Batcher.reset_pending(idx) end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:put, key, "old", 0}}],
          0
        )
      end

      assert {:error, "ERR async replication overloaded"} = Router.put(ctx(), key, "value", 0)
      assert Router.get(ctx(), key) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Read-your-writes (origin)
  # ---------------------------------------------------------------------------

  describe "read-your-writes on origin" do
    test "small value is readable immediately after :ok" do
      key = "#{@ns}:ryw_small_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "hello", 0)
      assert Router.get(ctx(), key) == "hello"
    end

    test "large value (>64KB) is readable immediately after :ok" do
      key = "#{@ns}:ryw_large_#{:erlang.unique_integer([:positive])}"
      big = :binary.copy("x", 100 * 1024)
      :ok = Router.put(ctx(), key, big, 0)
      assert Router.get(ctx(), key) == big
    end

    test "DELETE is observed immediately on origin" do
      key = "#{@ns}:ryw_del_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "present", 0)
      assert Router.get(ctx(), key) == "present"
      Router.delete(ctx(), key)
      assert Router.get(ctx(), key) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Origin-skip property in state machine
  # ---------------------------------------------------------------------------

  describe "state machine origin-skip" do
    @tag :capture_log
    test "origin does not re-insert the ETS entry after Raft apply" do
      # The key test of the redesign: Router writes ETS on origin, state machine
      # applies {:async, inner} and must detect the existing ETS entry and
      # skip. Observe this by writing, waiting for apply, and verifying ETS
      # was not touched a second time (i.e., no ETS races between Router and
      # state machine writing the same key with different LFU counters).
      key = "#{@ns}:skip_origin_#{:erlang.unique_integer([:positive])}"

      :ok = Router.put(ctx(), key, "value1", 0)
      initial = read_ets_entry(ctx(), key)
      assert initial != nil, "Router must populate ETS before returning :ok"

      # Give the state machine time to apply.
      :timer.sleep(100)

      after_apply = read_ets_entry(ctx(), key)

      # Value should be the same — state machine must not have overwritten it
      # (e.g. resetting LFU counter, clearing file_id, etc.)
      assert elem(after_apply, 1) == "value1"
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent ordering
  # ---------------------------------------------------------------------------

  describe "concurrent writes preserve last-write-wins" do
    test "two sequential writes to the same key land in order" do
      key = "#{@ns}:ordered_#{:erlang.unique_integer([:positive])}"
      :ok = Router.put(ctx(), key, "first", 0)
      :ok = Router.put(ctx(), key, "second", 0)
      assert Router.get(ctx(), key) == "second"
    end

    test "bitcask location points at the last repeated small SET" do
      key = "#{@ns}:ordered_disk_#{:erlang.unique_integer([:positive])}"
      c = ctx()

      :ok = Router.put(c, key, "first", 0)
      :ok = Router.put(c, key, "second", 0)

      ShardHelpers.eventually(
        fn ->
          BitcaskWriter.flush_all(c.shard_count)

          case read_ets_entry(c, key) do
            {^key, "second", _exp, _lfu, fid, _off, _vsize} when is_integer(fid) -> true
            _ -> false
          end
        end,
        "bitcask writer did not publish location for repeated SET",
        50,
        20
      )

      {^key, "second", _exp, _lfu, fid, off, _vsize} = read_ets_entry(c, key)
      idx = Router.shard_for(c, key)
      shard_path = Ferricstore.DataDir.shard_data_path(c.data_dir, idx)
      path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")

      assert {:ok, "second"} = NIF.v2_pread_at(path, off)
    end

    test "concurrent INCRs on same key sum correctly (atomicity)" do
      key = "#{@ns}:incr_concurrent_#{:erlang.unique_integer([:positive])}"
      c = ctx()
      Router.put(c, key, "0", 0)

      tasks =
        for _ <- 1..25 do
          Task.async(fn ->
            for _ <- 1..40 do
              Router.incr(c, key, 1)
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      # 25 * 40 = 1000 increments. Latch+worker serializes all same-key
      # RMWs, so no lost updates. Final value must be exactly 1000.
      assert Router.get(ctx(), key) == "1000"
    end
  end

  # ---------------------------------------------------------------------------
  # Latency (async should be genuinely fast, not blocked on Raft)
  # ---------------------------------------------------------------------------

  describe "async latency" do
    test "async SET (small) returns :ok in <5ms on average" do
      # Run many writes and average. Async SET of a small value should be
      # dominated by ETS insert + two casts, not Raft consensus.
      warmup =
        for i <- 1..10 do
          Router.put(ctx(), "#{@ns}:lat_warm_#{i}", "warm", 0)
        end

      _ = warmup

      samples =
        for i <- 1..100 do
          t0 = System.monotonic_time(:microsecond)
          :ok = Router.put(ctx(), "#{@ns}:lat_#{i}", "v", 0)
          System.monotonic_time(:microsecond) - t0
        end

      avg_us = div(Enum.sum(samples), length(samples))

      assert avg_us < 5_000,
             "async small-value SET avg latency #{avg_us}μs exceeded 5000μs; " <>
               "suggests call is blocking on Raft or the Batcher"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp read_ets_entry(ctx, key) do
    idx = Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, idx)

    case :ets.lookup(keydir, key) do
      [entry] -> entry
      [] -> nil
    end
  end
end
