defmodule Ferricstore.Raft.BatcherTest do
  @moduledoc """
  Tests for `Ferricstore.Raft.Batcher`.

  These tests exercise the group commit batcher GenServer using a locally
  managed ra system and shard servers. The batcher accumulates writes for
  up to `batch_window_ms`, then submits them as a single Raft log entry.

  The application starts the ra system, ra servers, and batchers for shards
  0-3 before these tests run.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Raft.ReplyAwaiter
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  defmodule SlowBatcher do
    use GenServer

    def start(name), do: GenServer.start(__MODULE__, nil, name: name)
    @impl true
    def init(nil), do: {:ok, nil}
    @impl true
    def handle_call(:flush, _from, state), do: {:noreply, state}
  end

  setup_all do
    ShardHelpers.wait_shards_alive()

    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Helper to generate unique keys
  defp ukey(base), do: "batcher_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # Batcher process lifecycle
  # ---------------------------------------------------------------------------

  describe "batcher process lifecycle" do
    test "batchers are registered under expected names" do
      for i <- 0..3 do
        name = Batcher.batcher_name(i)
        pid = Process.whereis(name)
        assert is_pid(pid), "Expected batcher #{i} registered as #{name}"
        assert Process.alive?(pid)
      end
    end

    test "batcher_name/1 returns expected atom" do
      assert Batcher.batcher_name(0) == :"Ferricstore.Raft.Batcher.0"
      assert Batcher.batcher_name(3) == :"Ferricstore.Raft.Batcher.3"
    end

    test "flush_all reports stuck batchers" do
      shard_index = 64
      name = Batcher.batcher_name(shard_index)

      if existing = Process.whereis(name) do
        flunk(
          "unexpected Batcher already registered for shard #{shard_index}: #{inspect(existing)}"
        )
      end

      {:ok, pid} = SlowBatcher.start(name)

      on_exit(fn ->
        if Process.alive?(pid), do: Process.exit(pid, :kill)
      end)

      assert {:error, [{^shard_index, {:flush_exit, {:timeout, _call}}}]} =
               Batcher.flush_all(shard_index + 1, 1)
    end
  end

  # ---------------------------------------------------------------------------
  # Single writes through batcher
  # ---------------------------------------------------------------------------

  describe "single writes through batcher" do
    test "write put command succeeds" do
      k = ukey("single_put")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      result = Batcher.write(shard_index, {:put, k, "val", 0})
      assert result == :ok
    end

    test "write delete command succeeds" do
      k = ukey("single_del")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      # First put, then delete
      :ok = Batcher.write(shard_index, {:put, k, "to_delete", 0})
      result = Batcher.write(shard_index, {:delete, k})
      assert result == :ok
    end

    test "data written through batcher is readable from Router" do
      k = ukey("readable")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "batcher_val", 0})
      assert "batcher_val" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "delete through batcher removes key from Router" do
      k = ukey("del_readable")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "temp", 0})
      assert "temp" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:delete, k})
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Batch window accumulation
  # ---------------------------------------------------------------------------

  describe "batch window accumulation" do
    test "multiple concurrent writes are batched" do
      keys =
        for i <- 1..10 do
          ukey("concurrent_#{i}")
        end

      # Group keys by shard
      by_shard =
        Enum.group_by(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)

      # Pick a shard that has multiple keys to test batching
      {shard_idx, shard_keys} = Enum.max_by(by_shard, fn {_, ks} -> length(ks) end)

      # Send all writes concurrently -- they should be batched
      tasks =
        Enum.map(shard_keys, fn k ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, k, "batched", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable
      for k <- shard_keys do
        assert "batched" == Router.get(FerricStore.Instance.get(:default), k)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Flush
  # ---------------------------------------------------------------------------

  describe "flush" do
    test "flush returns :ok when batch is empty" do
      assert :ok == Batcher.flush(0)
    end

    test "flush after writes ensures all data is committed" do
      k = ukey("flush_verify")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "flushed", 0})
      :ok = Batcher.flush(shard_index)

      assert "flushed" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "local apply gating" do
    test "await_local_applied returns timeout instead of exiting" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)

      assert {:error, :timeout} =
               Batcher.await_local_applied(shard_index, last_local_applied + 1_000, 25)
    end

    test "await_local_applied timeout emits telemetry" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      target_index = last_local_applied + 1_000
      handler_id = {:batcher_local_apply_timeout, self(), make_ref()}
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:ferricstore, :batcher, :local_apply_timeout],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:batcher_telemetry, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach(handler_id)
        send(batcher, {:locally_applied, target_index})
        Batcher.reset_pending(shard_index)
      end)

      assert {:error, :timeout} = Batcher.await_local_applied(shard_index, target_index, 25)

      assert_receive {:batcher_telemetry, [:ferricstore, :batcher, :local_apply_timeout],
                      %{count: 1},
                      %{shard_index: ^shard_index, ra_index: ^target_index, timeout_ms: 25}},
                     1_000
    end

    test "local quorum caller replies on raft apply without local-apply waiter" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      reply_ref = make_ref()

      on_exit(fn ->
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{self(), reply_ref}],
          :single,
          System.monotonic_time()
        )

      send(batcher, {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, :ok}}]}})

      assert_receive {^reply_ref, :ok}, 1_000

      state = :sys.get_state(batcher)
      assert state.local_apply_waiters == []
    end

    test "local batch quorum caller replies on raft apply without local-apply waiter" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      reply_ref = make_ref()

      on_exit(fn ->
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{:batch_from, {self(), reply_ref}, 2}],
          :batch,
          System.monotonic_time()
        )

      send(
        batcher,
        {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, {:ok, [:ok, :ok]}}}]}}
      )

      assert_receive {^reply_ref, {:ok, [:ok, :ok]}}, 1_000

      state = :sys.get_state(batcher)
      assert state.local_apply_waiters == []
    end

    test "local alias-backed batch caller replies on raft apply without local-apply waiter" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      {from, token} = ReplyAwaiter.new()

      on_exit(fn ->
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{:batch_from, from, 2}],
          :batch,
          System.monotonic_time()
        )

      send(
        batcher,
        {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, {:ok, [:ok, :ok]}}}]}}
      )

      assert {:ok, [:ok, :ok]} == ReplyAwaiter.await(token, 1_000, {:error, :timeout})

      state = :sys.get_state(batcher)
      assert state.local_apply_waiters == []
    end

    test "remote-origin quorum callers wait for local apply and emit wait telemetry" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      reply_ref = make_ref()
      handler_id = {:batcher_local_apply_waiters, self(), make_ref()}
      test_pid = self()

      :telemetry.attach_many(
        handler_id,
        [
          [:ferricstore, :batcher, :quorum_applied],
          [:ferricstore, :batcher, :local_apply_waiters],
          [:ferricstore, :batcher, :local_apply_gate]
        ],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:batcher_telemetry, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach(handler_id)
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{:remote_origin, :remote@nohost, {self(), reply_ref}}],
          :single,
          System.monotonic_time()
        )

      send(batcher, {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, :ok}}]}})

      assert_receive {:batcher_telemetry, [:ferricstore, :batcher, :quorum_applied],
                      %{duration_us: applied_us, caller_count: 1},
                      %{shard_index: ^shard_index, kind: :single, result: :ok}},
                     1_000

      assert is_integer(applied_us)
      assert applied_us >= 0

      assert_receive {:batcher_telemetry, [:ferricstore, :batcher, :local_apply_waiters],
                      %{depth: 1, oldest_age_ms: oldest_age_ms}, %{shard_index: ^shard_index}},
                     1_000

      assert is_integer(oldest_age_ms)
      assert oldest_age_ms >= 0
      refute_receive {^reply_ref, :ok}, 50

      send(batcher, {:locally_applied, ra_index})
      assert_receive {^reply_ref, {:remote_applied_at, ^ra_index, :ok}}, 1_000

      assert_receive {:batcher_telemetry, [:ferricstore, :batcher, :local_apply_gate],
                      %{duration_us: gate_us, caller_count: 1},
                      %{shard_index: ^shard_index, kind: :single, ra_index: ^ra_index}},
                     1_000

      assert is_integer(gate_us)
      assert gate_us >= 0

      assert_receive {:batcher_telemetry, [:ferricstore, :batcher, :local_apply_waiters],
                      %{depth: 0, oldest_age_ms: 0}, %{shard_index: ^shard_index}},
                     1_000
    end

    test "flush waits for local apply waiters after raft apply drains pending" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      reply_ref = make_ref()

      on_exit(fn ->
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{:remote_origin, :remote@nohost, {self(), reply_ref}}],
          :single,
          System.monotonic_time()
        )

      send(batcher, {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, :ok}}]}})

      flush_task = Task.async(fn -> Batcher.flush(shard_index) end)
      refute Task.yield(flush_task, 50)

      send(batcher, {:locally_applied, ra_index})
      assert :ok = Task.await(flush_task, 1_000)
      assert_receive {^reply_ref, {:remote_applied_at, ^ra_index, :ok}}, 1_000
    end

    test "flush waits for origin replay raft applies to reach the local state machine" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()

      on_exit(fn ->
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_origin_pending__(
          shard_index,
          corr,
          [{:put, "batcher_async_flush_barrier", "v", 0}],
          0
        )

      send(batcher, {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, :ok}}]}})

      flush_task = Task.async(fn -> Batcher.flush(shard_index) end)
      refute Task.yield(flush_task, 50)

      send(batcher, {:locally_applied, ra_index})
      assert :ok = Task.await(flush_task, 1_000)
    end
  end

  # ---------------------------------------------------------------------------
  # Expiry through batcher
  # ---------------------------------------------------------------------------

  describe "expiry through batcher" do
    test "put with TTL stores correct expiry" do
      k = ukey("ttl_batcher")
      future = System.os_time(:millisecond) + 60_000
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "ttl_val", future})

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end
  end
end
