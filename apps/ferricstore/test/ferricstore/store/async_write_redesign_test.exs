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
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
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

  def overload_later_shard_on_first_async_flush(
        _event,
        _measurements,
        %{shard_index: first_idx, origin: true},
        {test_pid, first_idx, second_idx, second_key}
      ) do
    for _ <- 1..64 do
      Batcher.__inject_async_pending__(
        second_idx,
        make_ref(),
        [{:async, node(), {:put, second_key, "pending", 0}}],
        0
      )
    end

    send(test_pid, :later_shard_overloaded)
  end

  def overload_later_shard_on_first_async_flush(_event, _measurements, _metadata, _config),
    do: :ok

  defp key_for_shard(prefix, shard_idx) do
    Enum.find_value(1..100_000, fn i ->
      key = "#{prefix}:#{shard_idx}:#{i}"

      if Router.shard_for(ctx(), key) == shard_idx do
        key
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Pipeline / Batcher routing
  # ---------------------------------------------------------------------------

  describe "async routing" do
    test "suite context exercises the Raft async path" do
      key = "#{@ns}:ctx_probe"

      assert ctx().name == :default
      assert Router.shard_for(ctx(), key) in 0..(ctx().shard_count - 1)
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

      assert {:error, "ERR raft replication overloaded"} = Router.put(ctx(), key, "value", 0)
      assert Router.get(ctx(), key) == nil
    end

    test "large async PUT restores previous value when Batcher is overloaded" do
      key = "#{@ns}:overloaded_large_put_#{:erlang.unique_integer([:positive])}"
      idx = Router.shard_for(ctx(), key)
      large = :binary.copy("x", ctx().hot_cache_max_value_size + 1024)

      :ok = Router.put(ctx(), key, "old", 0)
      assert Router.get(ctx(), key) == "old"

      on_exit(fn -> Batcher.reset_pending(idx) end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:put, key, "pending", 0}}],
          0
        )
      end

      assert {:error, "ERR raft replication overloaded"} = Router.put(ctx(), key, large, 0)
      assert Router.get(ctx(), key) == "old"
      assert recovered_value_from_bitcask(ctx(), key) == "old"
    end

    test "large async PUT does not recover unaccepted value when Batcher is overloaded" do
      key = "#{@ns}:overloaded_large_missing_put_#{:erlang.unique_integer([:positive])}"
      idx = Router.shard_for(ctx(), key)
      large = :binary.copy("x", ctx().hot_cache_max_value_size + 1024)

      on_exit(fn -> Batcher.reset_pending(idx) end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:put, key, "pending", 0}}],
          0
        )
      end

      assert {:error, "ERR raft replication overloaded"} = Router.put(ctx(), key, large, 0)
      assert Router.get(ctx(), key) == nil
      assert recovered_value_from_bitcask(ctx(), key) == nil
    end

    test "large async PUT rollback tombstone marks checkpoint dirty after prewrite flag is cleared" do
      c = ctx()
      key = "#{@ns}:overloaded_large_missing_checkpoint_#{:erlang.unique_integer([:positive])}"
      idx = Router.shard_for(c, key)
      flag_idx = idx + 1
      large = :binary.copy("x", c.hot_cache_max_value_size + 1024)
      test_pid = self()

      on_exit(fn ->
        Batcher.reset_pending(idx)
        Process.delete(:ferricstore_router_after_large_async_prewrite_hook)
      end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:put, key, "pending", 0}}],
          0
        )
      end

      Process.put(:ferricstore_router_after_large_async_prewrite_hook, fn _ctx, ^idx, ^key ->
        :atomics.put(c.checkpoint_flags, flag_idx, 0)
        send(test_pid, :prewrite_dirty_flag_cleared)
      end)

      assert {:error, "ERR raft replication overloaded"} = Router.put(c, key, large, 0)
      assert_receive :prewrite_dirty_flag_cleared

      assert :atomics.get(c.checkpoint_flags, flag_idx) == 1,
             "rollback tombstone must re-mark checkpoint dirty after checkpointer clears prewrite"

      assert recovered_value_from_bitcask(c, key) == nil
    end

    test "batch async PUT does not become locally visible when Batcher is overloaded" do
      key = "#{@ns}:overloaded_batch_put_#{:erlang.unique_integer([:positive])}"
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

      assert {:error, "ERR raft replication overloaded"} =
               Router.batch_put(ctx(), [{key, "value"}])

      assert Router.get(ctx(), key) == nil
    end

    test "cross-shard batch async PUT reports partial unknown after some shard submits were accepted" do
      prefix = "#{@ns}:partial_batch_#{:erlang.unique_integer([:positive])}"
      kv_pairs = [{key_for_shard(prefix, 0), "v0"}, {key_for_shard(prefix, 1), "v1"}]

      ordered_shards =
        kv_pairs
        |> Enum.group_by(fn {key, _value} -> Router.shard_for(ctx(), key) end)
        |> Enum.map(fn {idx, _pairs} -> idx end)

      [first_idx, second_idx] = ordered_shards

      {first_key, _} =
        Enum.find(kv_pairs, fn {key, _value} -> Router.shard_for(ctx(), key) == first_idx end)

      {second_key, _} =
        Enum.find(kv_pairs, fn {key, _value} -> Router.shard_for(ctx(), key) == second_idx end)

      test_pid = self()
      handler_id = {:cross_shard_batch_visibility, test_pid, make_ref()}

      on_exit(fn ->
        Batcher.reset_pending(first_idx)
        Batcher.reset_pending(second_idx)
      end)

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :batcher, :async_flush],
          &__MODULE__.overload_later_shard_on_first_async_flush/4,
          {test_pid, first_idx, second_idx, second_key}
        )

      try do
        default_ctx = ctx()
        task = Task.async(fn -> Router.batch_put(default_ctx, kv_pairs) end)

        assert_receive :later_shard_overloaded, 5_000

        assert {:error, message} = Task.await(task, 5_000)
        assert message =~ "partial"
        assert message =~ "unknown"

        ShardHelpers.eventually(
          fn -> Router.get(ctx(), first_key) == "v#{first_idx}" end,
          "accepted shard key did not become visible",
          20,
          10
        )

        assert Router.get(ctx(), second_key) == nil
      after
        :telemetry.detach(handler_id)
      end
    end

    test "partial large batch failure does not rollback already accepted shards" do
      c = ctx()
      prefix = "#{@ns}:partial_large_#{:erlang.unique_integer([:positive])}"
      large0 = :binary.copy("a", c.hot_cache_max_value_size + 1024)
      large1 = :binary.copy("b", c.hot_cache_max_value_size + 1024)
      kv_pairs = [{key_for_shard(prefix, 0), large0}, {key_for_shard(prefix, 1), large1}]

      ordered_shards =
        kv_pairs
        |> Enum.group_by(fn {key, _value} -> Router.shard_for(c, key) end)
        |> Enum.map(fn {idx, _pairs} -> idx end)

      [first_idx, second_idx] = ordered_shards

      {first_key, first_value} =
        Enum.find(kv_pairs, fn {key, _value} -> Router.shard_for(c, key) == first_idx end)

      {second_key, _} =
        Enum.find(kv_pairs, fn {key, _value} -> Router.shard_for(c, key) == second_idx end)

      test_pid = self()
      handler_id = {:cross_shard_large_batch_visibility, test_pid, make_ref()}

      on_exit(fn ->
        Batcher.reset_pending(first_idx)
        Batcher.reset_pending(second_idx)
      end)

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :batcher, :async_flush],
          &__MODULE__.overload_later_shard_on_first_async_flush/4,
          {test_pid, first_idx, second_idx, second_key}
        )

      try do
        task = Task.async(fn -> Router.batch_put(c, kv_pairs) end)

        assert_receive :later_shard_overloaded, 5_000

        assert {:error, message} = Task.await(task, 5_000)
        assert message =~ "partial"
        assert message =~ "unknown"

        assert tombstone_count(c, first_key) == 0

        ShardHelpers.eventually(
          fn -> Router.get(c, first_key) == first_value end,
          "accepted large shard key did not become visible",
          20,
          10
        )
      after
        :telemetry.detach(handler_id)
      end
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

    test "DELETE persists a single tombstone after async Ra apply" do
      c = ctx()
      key = "#{@ns}:single_tombstone_#{:erlang.unique_integer([:positive])}"
      idx = Router.shard_for(c, key)

      :ok = Router.put(c, key, "present", 0)
      :ok = Batcher.flush(idx)
      :ok = BitcaskWriter.flush_all(c.shard_count)

      assert :ok = Router.delete(c, key)
      assert Router.get(c, key) == nil

      :ok = Batcher.flush(idx)
      :ok = BitcaskWriter.flush_all(c.shard_count)

      assert tombstone_count(c, key) == 1
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

      results = Task.await_many(tasks, 30_000) |> List.flatten()

      assert Enum.all?(results, fn
               {:ok, n} when is_integer(n) -> true
               _ -> false
             end)

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

    test "async SET returns before namespace batch window flush" do
      ns = "#{@ns}_slow_window_#{System.unique_integer([:positive])}"
      on_exit(fn -> Ferricstore.NamespaceConfig.reset(ns) end)

      assert :ok = Ferricstore.NamespaceConfig.set(ns, "durability", "async")
      assert :ok = Ferricstore.NamespaceConfig.set(ns, "window_ms", "100")

      {time_us, result} =
        :timer.tc(fn ->
          Router.put(ctx(), "#{ns}:key", "value", 0)
        end)

      assert result == :ok

      assert time_us < 50_000,
             "async SET took #{time_us}us with a 100ms namespace window; " <>
               "it should return after local enqueue, not wait for Raft flush"
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

  defp recovered_value_from_bitcask(ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    keydir = :ets.new(:async_write_redesign_recovery, [:set, :public])

    try do
      ShardLifecycle.recover_keydir(shard_path, keydir, idx)

      case :ets.lookup(keydir, key) do
        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil ->
          value

        [{^key, nil, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) ->
          path =
            Path.join(shard_path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")

          {:ok, value} = NIF.v2_pread_at(path, off)
          value

        [] ->
          nil
      end
    after
      :ets.delete(keydir)
    end
  end

  defp tombstone_count(ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

    shard_path
    |> Path.join("*.log")
    |> Path.wildcard()
    |> Enum.reduce(0, fn path, acc ->
      case NIF.v2_scan_tombstones(path) do
        {:ok, tombstones} ->
          acc +
            Enum.count(tombstones, fn {tombstone_key, _off, _size, _exp} ->
              tombstone_key == key
            end)

        {:error, _reason} ->
          acc
      end
    end)
  end
end
