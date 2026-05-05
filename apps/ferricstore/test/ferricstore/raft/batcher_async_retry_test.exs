defmodule Ferricstore.Raft.BatcherAsyncRetryTest do
  @moduledoc """
  TDD tests for Option R1 async retry-on-rejection (see design discussion
  in docs/async-rmw-design.md §"Rejected-ra retry" and async-write-redesign).

  The Batcher submits async batches via `ra.pipeline_command`. If Ra
  responds with `:rejected {:not_leader, hint, corr}` (leader change
  during submission), the Batcher must:

  - Retry once to the hinted leader (or same shard if hint is :undefined)
    up to @max_async_retries times.
  - Emit [:ferricstore, :batcher, :async_retry] telemetry on each retry.
  - After max retries, drop the batch and emit
    [:ferricstore, :batcher, :async_dropped] with reason :max_retries.
  - Sweep pending entries older than the TTL; drop with reason :ttl.

  These tests drive the implementation by asserting the observable
  behaviors above. They don't need a live Ra cluster — we inject synthetic
  `:ra_event` messages into a running Batcher and assert telemetry fires
  with the expected metadata.
  """
  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @ns "retry_async"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns, "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ctx, do: FerricStore.Instance.get(:default)

  defp attach_events(handler_id, events, pid) do
    _ =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, meas, meta, test_pid ->
          send(test_pid, {:telemetry, event, meas, meta})
        end,
        pid
      )

    :ok
  end

  # Submit one async write and wait until it's been handed to Ra.
  # Returns the shard index used.
  defp submit_async_write(key, value) do
    :ok = Router.put(ctx(), key, value, 0)
    idx = Router.shard_for(ctx(), key)
    # Force the Batcher to flush pending slots (includes async_origin) so
    # the ra.pipeline_command has been issued and a correlation is in the
    # pending map.
    Batcher.flush(idx)
    idx
  end

  # ---------------------------------------------------------------------------
  # Single retry on :rejected {:not_leader, hint, corr}
  # ---------------------------------------------------------------------------

  describe "async retry on :not_leader" do
    test "one :rejected triggers one retry with the same batch" do
      attach_events({:retry_test, :single}, [
        [:ferricstore, :batcher, :async_retry]
      ], self())

      try do
        # Submit an async write so the Batcher has an async correlation in
        # flight. The ra_event for it will land normally; we inject a fake
        # :rejected event for a synthetic correlation instead.
        _idx = submit_async_write("#{@ns}:retry_one_#{System.unique_integer([:positive])}", "v")

        # Instead of racing with real ra replies, we grab any shard's Batcher
        # and inject a synthetic rejection directly. Because a real async
        # batch is currently tracked there, we can't easily reference its
        # internal corr without instrumentation — so drive this test by
        # sending a fake pending entry in a second setup call.
        idx = 0
        batcher_pid = Process.whereis(Batcher.batcher_name(idx))
        assert is_pid(batcher_pid)

        corr = make_ref()
        batch = [{:put, "#{@ns}:fake_retry_key", "v", 0}]
        # Pre-install a pending entry that looks async.
        Batcher.__inject_async_pending__(idx, corr, batch, 0)

        # Send the rejected event as if Ra redirected us.
        send(batcher_pid, {:ra_event, :leader_unused,
                           {:rejected, {:not_leader, :undefined, corr}}})

        assert_receive {:telemetry, [:ferricstore, :batcher, :async_retry], %{retry_count: 1}, meta},
                       1_000

        assert meta.shard_index == idx
      after
        :telemetry.detach({:retry_test, :single})
      end
    end

    test "max retries exceeded drops the batch with :async_dropped telemetry" do
      attach_events({:retry_test, :max}, [
        [:ferricstore, :batcher, :async_retry],
        [:ferricstore, :batcher, :async_dropped]
      ], self())

      try do
        idx = 0
        batcher_pid = Process.whereis(Batcher.batcher_name(idx))
        assert is_pid(batcher_pid)

        batch = [{:put, "#{@ns}:fake_drop_key", "v", 0}]

        # Inject a pending entry at retry_count = @max_async_retries so the
        # very next rejection triggers the drop (not a retry). The code path
        # is the same as the 3-retry case because retry_count comparison is
        # `< @max_async_retries` — at exactly max, drop.
        corr = make_ref()
        Batcher.__inject_async_pending__(idx, corr, batch, 3)

        send(batcher_pid, {:ra_event, :leader_unused,
                           {:rejected, {:not_leader, :undefined, corr}}})

        assert_receive {:telemetry, [:ferricstore, :batcher, :async_dropped],
                        %{batch_size: 1}, %{reason: :max_retries}},
                       1_000

        refute Batcher.__has_pending__(idx, corr),
               "expected dropped entry to be removed from pending"
      after
        :telemetry.detach({:retry_test, :max})
      end
    end

    test "rejection that succeeds on retry clears pending cleanly" do
      attach_events({:retry_test, :success}, [
        [:ferricstore, :batcher, :async_retry]
      ], self())

      try do
        idx = 0
        batcher_pid = Process.whereis(Batcher.batcher_name(idx))
        corr = make_ref()
        batch = [{:put, "#{@ns}:fake_success_key", "v", 0}]
        Batcher.__inject_async_pending__(idx, corr, batch, 0)

        send(batcher_pid, {:ra_event, :leader_unused,
                           {:rejected, {:not_leader, :undefined, corr}}})

        assert_receive {:telemetry, [:ferricstore, :batcher, :async_retry], _, _}, 1_000

        new_corr = Batcher.__latest_async_corr__(idx)
        assert new_corr != corr

        # Simulate the retry succeeding — :applied removes the entry.
        send(batcher_pid, {:ra_event, :leader_unused,
                           {:applied, [{new_corr, :ok}]}})

        :timer.sleep(50)
        refute Batcher.__has_pending__(idx, new_corr),
               "expected async pending entry to be cleared after :applied"
      after
        :telemetry.detach({:retry_test, :success})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # TTL sweep — pending entry that never gets an ra_event is eventually dropped
  # ---------------------------------------------------------------------------

  describe "async TTL sweep" do
    test "stale pending entry is swept with :ttl reason" do
      attach_events({:retry_test, :ttl}, [
        [:ferricstore, :batcher, :async_dropped]
      ], self())

      try do
        idx = 0
        corr = make_ref()
        batch = [{:put, "#{@ns}:fake_ttl_key", "v", 0}]
        # Inject with an old timestamp — 60s in the past, way beyond TTL.
        Batcher.__inject_async_pending_at__(idx, corr, batch, 0,
          System.monotonic_time() - System.convert_time_unit(60_000, :millisecond, :native))

        # Trigger sweep manually (test hook).
        Batcher.__sweep_pending_now__(idx)

        assert_receive {:telemetry, [:ferricstore, :batcher, :async_dropped],
                        %{batch_size: 1}, %{reason: :ttl}},
                       1_000

        refute Batcher.__has_pending__(idx, corr),
               "expected swept async entry to be gone from pending"
      after
        :telemetry.detach({:retry_test, :ttl})
      end
    end
  end

  describe "quorum TTL sweep" do
    test "stale single pending entry replies unknown outcome and is removed" do
      attach_events({:retry_test, :quorum_single_ttl}, [
        [:ferricstore, :batcher, :quorum_timeout]
      ], self())

      try do
        idx = 0
        corr = make_ref()
        reply_ref = make_ref()
        froms = [{self(), reply_ref}]

        old_mono =
          System.monotonic_time() -
            System.convert_time_unit(60_000, :millisecond, :native)

        Batcher.__inject_quorum_pending_at__(idx, corr, froms, :single, old_mono)
        assert Batcher.__has_pending__(idx, corr)

        Batcher.__sweep_pending_now__(idx)

        assert_receive {^reply_ref, {:error, {:timeout, :unknown_outcome}}}, 1_000

        assert_receive {:telemetry, [:ferricstore, :batcher, :quorum_timeout],
                        %{caller_count: 1, kind: :single}, %{reason: :ttl}},
                       1_000

        refute Batcher.__has_pending__(idx, corr)
      after
        :telemetry.detach({:retry_test, :quorum_single_ttl})
      end
    end

    test "stale batch pending entry replies unknown outcome and is removed" do
      attach_events({:retry_test, :quorum_batch_ttl}, [
        [:ferricstore, :batcher, :quorum_timeout]
      ], self())

      try do
        idx = 0
        corr = make_ref()
        reply_ref = make_ref()
        froms = [{self(), reply_ref}]

        old_mono =
          System.monotonic_time() -
            System.convert_time_unit(60_000, :millisecond, :native)

        Batcher.__inject_quorum_pending_at__(idx, corr, froms, :batch, old_mono)
        assert Batcher.__has_pending__(idx, corr)

        Batcher.__sweep_pending_now__(idx)

        assert_receive {^reply_ref, {:error, {:timeout, :unknown_outcome}}}, 1_000

        assert_receive {:telemetry, [:ferricstore, :batcher, :quorum_timeout],
                        %{caller_count: 1, kind: :batch}, %{reason: :ttl}},
                       1_000

        refute Batcher.__has_pending__(idx, corr)
      after
        :telemetry.detach({:retry_test, :quorum_batch_ttl})
      end
    end
  end
end
