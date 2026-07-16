defmodule Ferricstore.Store.PromotionLatchTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  import ExUnit.CaptureLog

  alias Ferricstore.Store.Promotion

  @promotion_path Path.expand("../../../lib/ferricstore/store/promotion.ex", __DIR__)

  setup do
    original = Application.get_env(:ferricstore, :promotion_compaction_latch_timeout_ms)

    on_exit(fn ->
      case original do
        nil -> Application.delete_env(:ferricstore, :promotion_compaction_latch_timeout_ms)
        value -> Application.put_env(:ferricstore, :promotion_compaction_latch_timeout_ms, value)
      end
    end)

    :ok
  end

  test "await_compaction_latch times out with telemetry when owner stays alive" do
    Application.put_env(:ferricstore, :promotion_compaction_latch_timeout_ms, 5)

    tab = :ets.new(:promotion_latch_timeout, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    redis_key = "promotion_latch_timeout"
    latch_key = {:promoted_compaction, redis_key}
    holder = spawn(fn -> Process.sleep(:infinity) end)
    handler_id = {:promotion_latch_timeout, self(), make_ref()}
    test_pid = self()
    original_trap_exit = Process.flag(:trap_exit, true)

    :ets.insert(tab, {latch_key, holder})

    :telemetry.attach(
      handler_id,
      [:ferricstore, :promotion, :compaction_latch],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:promotion_latch_telemetry, event, measurements, metadata})
      end,
      nil
    )

    try do
      log =
        capture_log(fn ->
          task = Task.async(fn -> Promotion.await_compaction_latch(owner, redis_key) end)

          assert {:exit, {%RuntimeError{message: message}, _stack}} = Task.yield(task, 500)
          assert message =~ "compaction latch timeout"

          assert_receive {:promotion_latch_telemetry,
                          [:ferricstore, :promotion, :compaction_latch], %{wait_ms: wait_ms},
                          %{status: :timeout, shard_index: 0}},
                         1_000

          assert wait_ms >= 5
        end)

      assert log =~ "Promoted compaction latch timeout"
      assert log =~ inspect(latch_key)
    after
      Process.flag(:trap_exit, original_trap_exit)
      :telemetry.detach(handler_id)
      Process.exit(holder, :kill)
      :ets.delete(tab)
    end
  end

  test "the latch owner can resolve promoted routing without waiting on itself" do
    tab = :ets.new(:promotion_latch_reentrant, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    redis_key = "promotion_latch_reentrant"

    token = Promotion.acquire_compaction_latch(owner, redis_key)

    try do
      assert :ok = Promotion.await_compaction_latch(owner, redis_key)
    after
      Promotion.release_compaction_latch(token)
      :ets.delete(tab)
    end
  end

  test "shared-log latch has nonblocking acquisition for the Raft apply path" do
    tab = :ets.new(:promotion_shared_log_latch, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}

    token = Promotion.acquire_shared_log_latch(owner)

    try do
      assert :busy = Promotion.try_acquire_shared_log_latch(owner)
    after
      Promotion.release_compaction_latch(token)
    end

    assert {:ok, next_token} = Promotion.try_acquire_shared_log_latch(owner)
    Promotion.release_compaction_latch(next_token)
    :ets.delete(tab)
  end

  test "dead-owner cleanup cannot delete a replacement latch owner" do
    source = File.read!(@promotion_path)

    refute source =~ ":ets.take(tab, latch_key)",
           "unconditional take can remove a replacement inserted by a concurrent waiter"

    assert source =~ ":ets.delete_object(tab, {latch_key, owner})"
  end

  test "a failed promotion fence wakes waiters fail-closed" do
    tab = :ets.new(:promotion_failure_fence, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    redis_key = "promotion_failure_fence"
    token = Promotion.acquire_compaction_latch(owner, redis_key)
    original_trap_exit = Process.flag(:trap_exit, true)

    try do
      task = Task.async(fn -> Promotion.await_compaction_latch(owner, redis_key) end)
      Process.sleep(5)
      :ok = Promotion.record_compound_promotion_failure(owner, redis_key, :copy_failed)
      Promotion.release_compaction_latch(token)

      assert {:exit, {%RuntimeError{message: message}, _stack}} = Task.yield(task, 1_000)
      assert message =~ "compound promotion failed"
      assert message =~ "copy_failed"
    after
      Process.flag(:trap_exit, original_trap_exit)
      Promotion.release_compaction_latch(token)
    end

    :ok = Promotion.clear_compound_promotion_fence(owner, redis_key)
    :ets.delete(tab)
  end

  test "a successful promotion fence releases waiters only after completion" do
    tab = :ets.new(:promotion_success_fence, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    redis_key = "promotion_success_fence"
    token = Promotion.acquire_compaction_latch(owner, redis_key)

    :ok = Promotion.record_compound_promotion_running(owner, redis_key)
    task = Task.async(fn -> Promotion.await_compaction_latch(owner, redis_key) end)
    Process.sleep(5)
    refute Task.yield(task, 0)

    :ok = Promotion.record_compound_promotion_success(owner, redis_key)
    Promotion.release_compaction_latch(token)
    assert {:ok, :ok} = Task.yield(task, 1_000)

    :ok = Promotion.clear_compound_promotion_fence(owner, redis_key)
    :ets.delete(tab)
  end
end
