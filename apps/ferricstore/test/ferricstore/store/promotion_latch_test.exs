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

  test "promotion marker codec preserves type, lifecycle state, and opaque generation" do
    first_generation = Promotion.new_generation()
    second_generation = Promotion.new_generation()

    refute second_generation == first_generation

    Enum.each([:hash, :set, :zset], fn type ->
      Enum.each([:promoted, :fallback, :cleanup], fn lifecycle_state ->
        marker = Promotion.encode_marker(type, lifecycle_state, first_generation)

        assert {:ok, ^type, ^lifecycle_state, ^first_generation} =
                 Promotion.decode_marker(marker)
      end)
    end)

    assert {:ok, :hash, :promoted, 0} = Promotion.decode_marker("hash")
    assert "hash" == Promotion.encode_marker(:hash, :promoted, 0)
    assert "fallback:set" == Promotion.encode_marker(:set, :fallback, 0)
    assert "cleanup:zset" == Promotion.encode_marker(:zset, :cleanup, 0)
    assert :error = Promotion.decode_marker("not-a-promotion-marker")

    assert {:error, :malformed_versioned_marker} =
             Promotion.decode_marker(<<0xF3, 0x50, 0x4D, 1, 0, 0, 0::unsigned-big-64>>)

    assert {:error, :unsupported_marker_version} =
             Promotion.decode_marker(<<0xF3, 0x50, 0x4D, 2, 0, 0, 1::unsigned-big-64>>)

    encoded = Promotion.encode_marker(:hash, :cleanup, first_generation)

    assert {:error, :malformed_versioned_marker} =
             encoded
             |> binary_part(0, byte_size(encoded) - 1)
             |> Promotion.decode_marker()
  end

  test "promotion generations remain unique without HLC runtime state" do
    atomics_key = :ferricstore_hlc_ref
    previous_ref = :persistent_term.get(atomics_key, :missing)
    :persistent_term.erase(atomics_key)

    try do
      generations = Enum.map(1..1_024, fn _ -> Promotion.new_generation() end)
      assert Enum.all?(generations, &(&1 > 0))
      assert MapSet.size(MapSet.new(generations)) == length(generations)
    after
      case previous_ref do
        :missing -> :persistent_term.erase(atomics_key)
        ref -> :persistent_term.put(atomics_key, ref)
      end
    end
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

  test "apply latch acquisition rejects a recorded promotion failure without leaking ownership" do
    tab = :ets.new(:promotion_failed_apply_acquire, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    redis_key = "promotion_failed_apply_acquire"
    latch_key = {:promoted_compaction, redis_key}

    :ok = Promotion.record_compound_promotion_failure(owner, redis_key, :copy_failed)

    assert_raise RuntimeError, ~r/compound promotion failed.*copy_failed/, fn ->
      Promotion.acquire_compaction_latch_for_apply(owner, redis_key)
    end

    assert :ets.lookup(tab, latch_key) == []

    :ok = Promotion.clear_compound_promotion_fence(owner, redis_key)
    :ets.delete(tab)
  end

  test "multi-key apply acquisition releases earlier latches when a later fence fails" do
    tab = :ets.new(:promotion_failed_multi_apply_acquire, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    first_key = "promotion_multi_apply_a"
    failed_key = "promotion_multi_apply_b"

    :ok = Promotion.record_compound_promotion_failure(owner, failed_key, :copy_failed)

    assert_raise RuntimeError, ~r/compound promotion failed.*copy_failed/, fn ->
      Promotion.with_compaction_latches_for_apply(owner, [failed_key, first_key], fn ->
        flunk("the protected operation must not run")
      end)
    end

    assert :ets.lookup(tab, {:promoted_compaction, first_key}) == []
    assert :ets.lookup(tab, {:promoted_compaction, failed_key}) == []

    :ok = Promotion.clear_compound_promotion_fence(owner, failed_key)
    :ets.delete(tab)
  end

  test "multi-key apply bypasses key normalization when no latch table exists" do
    source = File.read!(@promotion_path)

    assert Regex.match?(
             ~r/with_compaction_latches_for_apply\(owner, redis_keys, fun\).*?case latch_table\(owner\) do.*?nil\s*->\s*fun\.\(\)/s,
             source
           )
  end

  @tag timeout: 500
  test "blocking acquisition remains non-reentrant for the current owner" do
    Application.put_env(:ferricstore, :promotion_compaction_latch_timeout_ms, 5)

    tab = :ets.new(:promotion_latch_non_reentrant_acquire, [:set, :public])
    ctx = %FerricStore.Instance{latch_refs: {tab}}
    owner = %{instance_ctx: ctx, shard_index: 0}
    redis_key = "promotion_latch_non_reentrant_acquire"
    token = Promotion.acquire_compaction_latch(owner, redis_key)

    try do
      assert_raise RuntimeError, ~r/compaction latch timeout/, fn ->
        Promotion.acquire_compaction_latch(owner, redis_key)
      end
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
