defmodule Ferricstore.Store.PromotionInstanceContextTest do
  @moduledoc false

  use ExUnit.Case, async: false
  @moduletag :global_state

  import ExUnit.CaptureLog

  alias Ferricstore.{CommandTime, HLC}
  alias Ferricstore.Commands.Hash
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Store.{CompoundKey, Promotion, Router}
  alias Ferricstore.Store.Shard.CompoundRevisionIndex
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1_000_000,
        promotion_threshold: 1
      )

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "promotion in custom instances does not mutate default instance accounting", %{ctx: ctx} do
    default_ctx = FerricStore.Instance.get(:default)
    default_before = keydir_binary_total(default_ctx)
    custom_before = keydir_binary_total(ctx)

    redis_key =
      "promoted_custom_instance_" <>
        String.duplicate("k", 80) <> "_#{System.unique_integer([:positive])}"

    assert :ok = put_hash_type(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f1"),
               String.duplicate("a", 80),
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f2"),
               String.duplicate("b", 80),
               0
             )

    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    state = :sys.get_state(shard)
    assert Map.has_key?(state.promoted_instances, redis_key)

    assert keydir_binary_total(default_ctx) == default_before
    assert keydir_binary_total(ctx) > custom_before
  end

  test "orphan compound members above the threshold do not crash or promote the shard", %{
    ctx: ctx
  } do
    redis_key = "orphan_promotion_#{System.unique_integer([:positive])}"
    first = CompoundKey.hash_field(redis_key, "f1")
    second = CompoundKey.hash_field(redis_key, "f2")
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    original_pid = Process.whereis(shard)
    monitor_ref = Process.monitor(original_pid)

    assert :ok = Router.compound_put(ctx, redis_key, first, "value1", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "value2", 0)
    refute GenServer.call(shard, {:promoted?, redis_key}, 5_000)
    refute_receive {:DOWN, ^monitor_ref, :process, ^original_pid, _reason}, 1_000
    assert Process.whereis(shard) == original_pid
  end

  test "promotion recheck retries instead of scanning when the member catalog is unavailable", %{
    ctx: ctx
  } do
    redis_key = "unready_promotion_catalog_#{System.unique_integer([:positive])}"
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    unready_index = :ets.new(:unready_promotion_catalog, [:ordered_set, :public])

    assert :ok = put_hash_type(ctx, redis_key)

    state =
      shard
      |> :sys.get_state()
      |> Map.put(:compound_member_index, unready_index)

    assert :retry ==
             ShardCompound.promotion_candidate_status(state, redis_key, :hash, 1)
  end

  test "promotion admission retains a candidate when bounded expiry cleanup needs another pass",
       %{
         ctx: ctx
       } do
    redis_key = "bounded_promotion_admission_#{System.unique_integer([:positive])}"
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    state = :sys.get_state(shard)
    type_key = CompoundKey.type_key(redis_key)
    live_keys = Enum.map(1..2, &CompoundKey.hash_field(redis_key, "live-#{&1}"))
    expired_keys = Enum.map(1..2, &CompoundKey.hash_field(redis_key, "expired-#{&1}"))

    :ets.insert(state.keydir, {type_key, "hash", 0, 0, 0, 0, 4})

    Enum.each(live_keys, fn compound_key ->
      :ets.insert(state.keydir, {compound_key, "value", 0, 0, 0, 0, 5})
      CompoundMemberIndex.put(state.compound_member_index, compound_key, 0)
    end)

    Enum.each(expired_keys, fn compound_key ->
      :ets.insert(state.keydir, {compound_key, "value", 10, 0, 0, 0, 5})
      CompoundMemberIndex.put(state.compound_member_index, compound_key, 10)
    end)

    bounded_state = %{
      state
      | apply_context: %{state.apply_context | compound_member_apply_budget: 1}
    }

    next_state =
      CommandTime.with_now_ms(20, fn ->
        ShardCompound.maybe_promote(bounded_state, redis_key, hd(live_keys), 1)
      end)

    assert next_state.compound_promotion_pending[redis_key] == {:hash, 1}
    assert_received {:start_compound_promotion, ^redis_key, {:hash, 1}}
  end

  test "promotion retries are keyed and deduplicated", %{ctx: ctx} do
    redis_key = "deduplicated_promotion_retry_#{System.unique_integer([:positive])}"
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    candidate = {:hash, 1}
    unready_index = :ets.new(:unready_retry_catalog, [:ordered_set, :public])

    assert :ok = put_hash_type(ctx, redis_key)

    original_index = :sys.get_state(shard).compound_member_index

    :sys.replace_state(shard, fn state ->
      %{
        state
        | compound_member_index: unready_index,
          compound_promotion_pending:
            Map.put(state.compound_promotion_pending, redis_key, candidate)
      }
    end)

    Enum.each(1..8, fn _ -> send(shard, {:start_compound_promotion, redis_key, candidate}) end)

    state = :sys.get_state(shard)
    timers = Map.get(state, :compound_promotion_retry_timers, %{})
    assert map_size(timers) == 1

    assert %{attempt: attempt, tag: tag, candidate: ^candidate, due_at_ms: due_at_ms} =
             timers[redis_key]

    assert attempt >= 1
    assert is_reference(tag)
    assert is_integer(due_at_ms)
    assert %{timer_ref: scheduler_timer_ref} = state.compound_promotion_retry_timer
    assert is_reference(scheduler_timer_ref)

    retry = timers[redis_key]

    send(shard, {:remove_promoted_after_commit, redis_key, :none})

    :sys.replace_state(shard, fn state ->
      %{state | compound_member_index: original_index}
    end)

    send(
      shard,
      {:retry_compound_promotion, redis_key, candidate, retry.tag, retry.attempt}
    )

    refute GenServer.call(shard, {:promoted?, redis_key}, 5_000)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)

      Map.get(state, :compound_promotion_retry_timers, %{}) == %{} and
        not Map.has_key?(state.compound_promotion_pending, redis_key) and
        state.compound_promotion_worker == nil
    end)

    refute GenServer.call(shard, {:promoted?, redis_key}, 5_000)
  end

  test "compound promotion retries coalesce timers and progress fairly", %{ctx: ctx} do
    shard_index = 0
    shard = elem(ctx.shard_names, shard_index)
    shard_pid = Process.whereis(shard)
    owner = %{instance_ctx: ctx, shard_index: shard_index}
    shared_log_latch = Promotion.acquire_shared_log_latch(owner)
    pending = Map.new(1..1_000, fn index -> {"coalesced_retry_#{index}", {:hash, 1}} end)

    try do
      :sys.replace_state(shard, fn state ->
        %{state | compound_promotion_pending: pending}
      end)

      before_reductions = :erlang.process_info(shard_pid, :reductions) |> elem(1)
      send(shard, :start_pending_compound_promotion)

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard)
        map_size(state.compound_promotion_retry_timers) > 0
      end)

      Process.sleep(100)
      after_reductions = :erlang.process_info(shard_pid, :reductions) |> elem(1)
      state = :sys.get_state(shard)

      assert %{timer_ref: timer_ref} = state.compound_promotion_retry_timer
      assert is_reference(timer_ref)
      assert after_reductions - before_reductions < 12_000_000

      assert Enum.all?(Map.values(state.compound_promotion_retry_timers), fn retry ->
               not Map.has_key?(retry, :timer_ref)
             end)
    after
      Promotion.release_compaction_latch(shared_log_latch)
    end

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)

      state.compound_promotion_pending == %{} and
        state.compound_promotion_retry_timers == %{} and
        state.compound_promotion_retry_timer == nil
    end)
  end

  test "post-commit metadata retry warnings are throttled after maximum backoff", %{ctx: ctx} do
    redis_key = "throttled_post_commit_retry_#{System.unique_integer([:positive])}"
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    original_index = :sys.get_state(shard).compound_member_index
    unready_index = :ets.new(:unready_post_commit_retry_catalog, [:ordered_set, :public])

    assert :ok = put_hash_type(ctx, redis_key)

    :sys.replace_state(shard, fn state ->
      %{state | compound_member_index: unready_index}
    end)

    first_max_log = capture_post_commit_retry_log(shard, redis_key, 7)

    assert first_max_log =~ "deferring post-commit promotion cleanup"

    repeated_log = capture_post_commit_retry_log(shard, redis_key, 8)

    refute repeated_log =~ "deferring post-commit promotion cleanup"

    periodic_log = capture_post_commit_retry_log(shard, redis_key, 67)

    assert periodic_log =~ "deferring post-commit promotion cleanup"

    :sys.replace_state(shard, fn state ->
      state = cancel_post_commit_test_timer(state)

      %{state | compound_member_index: original_index, post_commit_promotion_retry_timers: %{}}
    end)
  end

  test "post-commit promotion retries are deduplicated, resolved, and cancelled on flush", %{
    ctx: ctx
  } do
    redis_key = "deduplicated_post_commit_retry_#{System.unique_integer([:positive])}"
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    original_index = :sys.get_state(shard).compound_member_index
    unready_index = :ets.new(:deduplicated_post_commit_retry_catalog, [:ordered_set, :public])

    assert :ok = put_hash_type(ctx, redis_key)

    :sys.replace_state(shard, fn state ->
      %{state | compound_member_index: unready_index}
    end)

    Enum.each(1..8, fn _ ->
      send(shard, {:remove_promoted_after_commit, redis_key, :none})
    end)

    state = :sys.get_state(shard)
    retries = Map.get(state, :post_commit_promotion_retry_timers, %{})
    assert map_size(retries) == 1

    assert %{tag: tag, attempt: attempt, due_at_ms: due_at_ms} =
             retries[{:removal, redis_key, :none}]

    assert is_reference(tag)
    assert attempt >= 1
    assert is_integer(due_at_ms)
    assert %{timer_ref: timer_ref} = :sys.get_state(shard).post_commit_promotion_retry_timer
    assert is_reference(timer_ref)

    send(shard, {:remove_promoted_after_commit, redis_key, :none})

    assert map_size(:sys.get_state(shard).post_commit_promotion_retry_timers) == 1

    state =
      :sys.replace_state(shard, fn state ->
        timer_ref = state.post_commit_promotion_retry_timer.timer_ref

        _ = Process.cancel_timer(timer_ref, async: false, info: false)
        %{state | compound_member_index: original_index}
      end)

    %{tag: tag, attempt: attempt} =
      state.post_commit_promotion_retry_timers[{:removal, redis_key, :none}]

    send(
      shard,
      {:retry_promoted_removal_after_commit, redis_key, :none, tag, attempt}
    )

    assert :sys.get_state(shard).post_commit_promotion_retry_timers == %{}

    :sys.replace_state(shard, fn state ->
      %{state | compound_member_index: unready_index}
    end)

    send(shard, {:remove_promoted_after_commit, redis_key, :none})
    state = :sys.get_state(shard)

    assert %{timer_ref: flush_timer_ref} = state.post_commit_promotion_retry_timer

    :sys.replace_state(shard, &%{&1 | writes_paused: true})
    assert :ok = GenServer.call(shard, {:prepare_promoted_flush, {1, 0}})
    assert :sys.get_state(shard).post_commit_promotion_retry_timers == %{}
    assert Process.read_timer(flush_timer_ref) == false
  end

  test "post-commit promotion retries coalesce timers across many keys", %{ctx: ctx} do
    shard = elem(ctx.shard_names, 0)
    state = :sys.get_state(shard)
    original_index = state.compound_member_index
    unready_index = :ets.new(:many_post_commit_retry_catalog, [:ordered_set, :public])
    keys = Enum.map(1..1_000, &"many_post_commit_retry_#{&1}")

    Enum.each(keys, fn redis_key ->
      :ets.insert(
        state.keydir,
        {CompoundKey.type_key(redis_key), "hash", 0, 0, 0, 0, byte_size("hash")}
      )
    end)

    :sys.replace_state(shard, fn state ->
      %{state | compound_member_index: unready_index}
    end)

    capture_log(fn ->
      Enum.each(keys, fn redis_key ->
        send(shard, {:remove_promoted_after_commit, redis_key, :none})
      end)

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        map_size(:sys.get_state(shard).post_commit_promotion_retry_timers) == length(keys)
      end)
    end)

    state = :sys.get_state(shard)
    assert %{timer_ref: timer_ref} = state.post_commit_promotion_retry_timer
    assert is_reference(timer_ref)

    assert Enum.all?(Map.values(state.post_commit_promotion_retry_timers), fn retry ->
             not Map.has_key?(retry, :timer_ref)
           end)

    :sys.replace_state(shard, fn state ->
      %{state | compound_member_index: original_index}
    end)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)

      state.post_commit_promotion_retry_timers == %{} and
        state.post_commit_promotion_retry_timer == nil
    end)
  end

  test "post-commit promoted cleanup cancels queued promotion state", %{ctx: ctx} do
    redis_key = "cleanup_pending_promotion_#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    dedicated_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)
    File.mkdir_p!(dedicated_path)

    retry_tag = make_ref()

    retry_timer =
      Process.send_after(
        shard,
        {:retry_compound_promotion_batch, retry_tag},
        30_000
      )

    :sys.replace_state(shard, fn state ->
      %{
        state
        | compound_promotion_pending:
            Map.put(state.compound_promotion_pending, redis_key, {:hash, 1}),
          compound_promotion_retry_timers:
            Map.put(state.compound_promotion_retry_timers, redis_key, %{
              tag: retry_tag,
              attempt: 1,
              candidate: {:hash, 1},
              due_at_ms: System.monotonic_time(:millisecond) + 30_000
            }),
          compound_promotion_retry_timer: %{
            tag: retry_tag,
            timer_ref: retry_timer,
            due_at_ms: System.monotonic_time(:millisecond) + 30_000
          },
          promoted_instances:
            Map.put(state.promoted_instances, redis_key, %{
              path: dedicated_path,
              writes: 0,
              total_bytes: 0,
              dead_bytes: 0,
              last_compacted_at: nil
            })
      }
    end)

    cleanup_latch =
      Promotion.acquire_compaction_latch(
        %{instance_ctx: ctx, shard_index: shard_index},
        redis_key
      )

    try do
      send(
        shard,
        {:cleanup_promoted_after_commit, redis_key, :hash, dedicated_path,
         Promotion.new_generation()}
      )

      Process.sleep(50)
      assert File.dir?(dedicated_path)
    after
      Promotion.release_compaction_latch(cleanup_latch)
    end

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)

      not Map.has_key?(state.compound_promotion_pending, redis_key) and
        not Map.has_key?(state.compound_promotion_retry_timers, redis_key) and
        not Map.has_key?(state.promoted_instances, redis_key) and
        not File.dir?(dedicated_path)
    end)
  end

  test "stale post-commit cleanup stays live when a promotion transfers its latch to the shard",
       %{
         ctx: ctx
       } do
    redis_key = "cleanup_after_latch_transfer_#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    shard_pid = Process.whereis(shard)
    dedicated_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)
    test_pid = self()
    old_hook = Application.get_env(:ferricstore, :compound_promotion_worker_test_hook)
    old_timeout = Application.get_env(:ferricstore, :promotion_compaction_latch_timeout_ms)

    Application.put_env(:ferricstore, :promotion_compaction_latch_timeout_ms, 200)

    Application.put_env(:ferricstore, :compound_promotion_worker_test_hook, fn
      ^redis_key ->
        send(test_pid, {:promotion_worker_paused, self()})

        receive do
          :continue_promotion -> :ok
        end

      _other_key ->
        :ok
    end)

    on_exit(fn ->
      restore_env(:compound_promotion_worker_test_hook, old_hook)
      restore_env(:promotion_compaction_latch_timeout_ms, old_timeout)
    end)

    assert :ok = put_hash_type(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "first"),
               "one",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "second"),
               "two",
               0
             )

    assert_receive {:promotion_worker_paused, worker_pid}, 5_000

    send(
      shard,
      {:cleanup_promoted_after_commit, redis_key, :hash, dedicated_path,
       Promotion.new_generation()}
    )

    Process.sleep(25)
    send(worker_pid, :continue_promotion)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      Process.whereis(shard) == shard_pid and
        match?(
          %{compound_promotion_worker: nil},
          :sys.get_state(shard, 1_000)
        ) and File.dir?(dedicated_path)
    end)

    assert GenServer.call(shard, {:promoted?, redis_key}, 1_000)
  end

  test "stale removal drops an old promoted route without cancelling a new pending incarnation",
       %{
         ctx: ctx
       } do
    redis_key = "stale_removal_pending_incarnation_#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    dedicated_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)
    owner = %{instance_ctx: ctx, shard_index: shard_index}
    shared_log_latch = Promotion.acquire_shared_log_latch(owner)

    try do
      assert :ok = put_hash_type(ctx, redis_key)

      assert :ok =
               Router.compound_put(
                 ctx,
                 redis_key,
                 CompoundKey.hash_field(redis_key, "first"),
                 "one",
                 0
               )

      assert :ok =
               Router.compound_put(
                 ctx,
                 redis_key,
                 CompoundKey.hash_field(redis_key, "second"),
                 "two",
                 0
               )

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard)
        state.compound_promotion_pending[redis_key] == {:hash, 1}
      end)

      :sys.replace_state(shard, fn state ->
        %{
          state
          | promoted_instances:
              Map.put(state.promoted_instances, redis_key, %{
                path: dedicated_path,
                writes: 0,
                total_bytes: 0,
                dead_bytes: 0,
                last_compacted_at: nil
              })
        }
      end)

      send(shard, {:remove_promoted_after_commit, redis_key, Promotion.new_generation()})

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard)

        not Map.has_key?(state.promoted_instances, redis_key) and
          state.compound_promotion_pending[redis_key] == {:hash, 1}
      end)
    after
      Promotion.release_compaction_latch(shared_log_latch)
    end
  end

  test "promotion succeeds after its member catalog becomes available", %{ctx: ctx} do
    redis_key = "promotion_catalog_recovers_#{System.unique_integer([:positive])}"
    first = CompoundKey.hash_field(redis_key, "first")
    second = CompoundKey.hash_field(redis_key, "second")
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    owner = %{instance_ctx: ctx, shard_index: shard_index}
    shared_log_latch = Promotion.acquire_shared_log_latch(owner)
    unready_index = :ets.new(:promotion_catalog_recovers, [:ordered_set, :public])

    try do
      assert :ok = put_hash_type(ctx, redis_key)
      assert :ok = Router.compound_put(ctx, redis_key, first, "one", 0)
      assert :ok = Router.compound_put(ctx, redis_key, second, "two", 0)

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard)
        state.compound_promotion_pending[redis_key] == {:hash, 1}
      end)

      :sys.replace_state(shard, fn state ->
        %{state | compound_member_index: unready_index}
      end)
    after
      Promotion.release_compaction_latch(shared_log_latch)
    end

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)
      Map.has_key?(state.compound_promotion_retry_timers, redis_key)
    end)

    state = :sys.get_state(shard)
    :ok = CompoundMemberIndex.rebuild(unready_index, state.keydir)

    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)

    state = :sys.get_state(shard)
    assert state.compound_promotion_retry_timers == %{}
    refute Map.has_key?(state.compound_promotion_pending, redis_key)
    assert state.compound_promotion_worker == nil
    assert "one" == Router.compound_get(ctx, redis_key, first)
    assert "two" == Router.compound_get(ctx, redis_key, second)
  end

  defp capture_post_commit_retry_log(shard, redis_key, attempt) do
    retry = {:removal, redis_key, :none}
    tag = make_ref()

    :sys.replace_state(shard, fn state ->
      state = cancel_post_commit_test_timer(state)

      timer = %{tag: tag, attempt: attempt, due_at_ms: System.monotonic_time(:millisecond)}
      %{state | post_commit_promotion_retry_timers: %{retry => timer}}
    end)

    capture_log(fn ->
      send(
        shard,
        {:retry_promoted_removal_after_commit, redis_key, :none, tag, attempt}
      )

      _ = :sys.get_state(shard)
      Logger.flush()
    end)
  end

  defp cancel_post_commit_test_timer(state) do
    case state.post_commit_promotion_retry_timer do
      %{timer_ref: timer_ref} ->
        _ = Process.cancel_timer(timer_ref, async: false, info: false)

      nil ->
        :ok
    end

    %{state | post_commit_promotion_retry_timer: nil}
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  test "permanently invalid indexed members do not enter the promotion retry loop", %{ctx: ctx} do
    redis_key = "invalid_promotion_member_#{System.unique_integer([:positive])}"
    compound_key = CompoundKey.hash_field(redis_key, "broken")
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    state = :sys.get_state(shard)
    type_key = CompoundKey.type_key(redis_key)

    :ets.insert(state.keydir, {type_key, "hash", 0, 0, 0, 0, 4})
    :ets.insert(state.keydir, {compound_key, :malformed})
    CompoundMemberIndex.put(state.compound_member_index, compound_key, 10)

    assert {:invalid,
            {:invalid_indexed_member, ^compound_key,
             {:invalid_keydir_entry, ^compound_key, [{^compound_key, :malformed}]}}} =
             CommandTime.with_now_ms(20, fn ->
               ShardCompound.promotion_candidate_status(state, redis_key, :hash, 1)
             end)
  end

  test "direct exact delete cancels a promotion deferred by shared-log maintenance", %{ctx: ctx} do
    redis_key = "direct_pending_delete_#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    shard_pid = Process.whereis(shard)
    monitor_ref = Process.monitor(shard_pid)

    shared_log_latch =
      Promotion.acquire_shared_log_latch(%{instance_ctx: ctx, shard_index: shard_index})

    try do
      assert :ok = put_hash_type(ctx, redis_key)

      assert :ok =
               Router.compound_put(
                 ctx,
                 redis_key,
                 CompoundKey.hash_field(redis_key, "f1"),
                 "value1",
                 0
               )

      assert :ok =
               Router.compound_put(
                 ctx,
                 redis_key,
                 CompoundKey.hash_field(redis_key, "f2"),
                 "value2",
                 0
               )

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard)
        state.compound_promotion_pending[redis_key] == {:hash, 1}
      end)

      waiter = Task.async(fn -> GenServer.call(shard, {:promoted?, redis_key}, 5_000) end)

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard)
        Map.has_key?(state.compound_promotion_waiters, redis_key)
      end)

      assert :ok =
               Router.compound_delete_prefix(ctx, redis_key, CompoundKey.hash_prefix(redis_key))

      Ferricstore.Test.ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard)
        not Map.has_key?(state.compound_promotion_pending, redis_key)
      end)

      refute Task.await(waiter, 5_000)
    after
      Promotion.release_compaction_latch(shared_log_latch)
    end

    refute_receive {:DOWN, ^monitor_ref, :process, ^shard_pid, _reason}, 500
    assert Process.whereis(shard) == shard_pid
  end

  test "direct exact delete rejects an unreadable promotion generation before mutation", %{
    ctx: ctx
  } do
    redis_key = "direct_delete_unreadable_generation_#{System.unique_integer([:positive])}"
    field = CompoundKey.hash_field(redis_key, "field")
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    shard_pid = Process.whereis(shard)
    marker_key = Promotion.marker_key(redis_key)
    marker = Promotion.encode_marker(:hash, :promoted, Promotion.new_generation())
    malformed = binary_part(marker, 0, byte_size(marker) - 1)

    assert :ok = put_hash_type(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, field, "value", 0)

    :sys.replace_state(shard, fn state ->
      true =
        :ets.insert(
          state.keydir,
          {marker_key, malformed, 0, 0, state.active_file_id, 0, byte_size(malformed)}
        )

      state
    end)

    assert {:error, :promotion_marker_unavailable} =
             Router.compound_delete_prefix(ctx, redis_key, CompoundKey.hash_prefix(redis_key))

    assert "value" == Router.compound_get(ctx, redis_key, field)
    assert Process.whereis(shard) == shard_pid
  end

  @tag :final_promoted_collection_delete
  test "direct final HDEL retires its promoted marker and directory", %{ctx: ctx} do
    redis_key = "direct_final_hdel_#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    marker_key = Promotion.marker_key(redis_key)
    dedicated_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)
    store = Ferricstore.Test.ShardHelpers.router_store(ctx)

    assert 2 == Hash.handle("HSET", [redis_key, "first", "one", "second", "two"], store)
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    assert File.dir?(dedicated_path)

    assert 2 == Hash.handle("HDEL", [redis_key, "first", "second"], store)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)

      Hash.handle("HLEN", [redis_key], store) == 0 and
        :ets.lookup(state.keydir, marker_key) == [] and
        not Map.has_key?(state.promoted_instances, redis_key) and
        not File.dir?(dedicated_path)
    end)
  end

  @tag :stale_cross_type_cleanup
  test "delayed hash cleanup removes only its old directory after set promotion", %{ctx: ctx} do
    redis_key = "stale_cross_type_cleanup_#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    old_hash_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)
    set_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :set, redis_key)
    type_key = CompoundKey.type_key(redis_key)
    first = CompoundKey.set_member(redis_key, "first")
    second = CompoundKey.set_member(redis_key, "second")

    assert {:ok, ^old_hash_path} =
             Promotion.open_dedicated(ctx.data_dir, shard_index, :hash, redis_key)

    assert :ok = Router.compound_put(ctx, redis_key, type_key, "set", 0)
    assert :ok = Router.compound_put(ctx, redis_key, first, "1", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "1", 0)
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    assert File.dir?(set_path)

    current_generation =
      case Router.compound_get(ctx, redis_key, Promotion.marker_key(redis_key)) do
        marker when is_binary(marker) ->
          assert {:ok, :set, :promoted, generation} = Promotion.decode_marker(marker)
          generation
      end

    send(
      shard,
      {:cleanup_promoted_after_commit, redis_key, :hash, old_hash_path, current_generation}
    )

    Ferricstore.Test.ShardHelpers.eventually(fn -> not File.dir?(old_hash_path) end)

    assert File.dir?(set_path)
    assert "1" == Router.compound_get(ctx, redis_key, first)
    assert "1" == Router.compound_get(ctx, redis_key, second)
    assert GenServer.call(shard, {:promoted?, redis_key}, 5_000)
  end

  test "replicated compound mutation holds its promotion latch from routing through commit", %{
    ctx: ctx
  } do
    redis_key = "replicated_route_reservation_#{System.unique_integer([:positive])}"
    first = CompoundKey.hash_field(redis_key, "first")
    second = CompoundKey.hash_field(redis_key, "second")
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    shard_pid = Process.whereis(shard)
    shard_monitor = Process.monitor(shard_pid)
    latch_table = elem(ctx.latch_refs, shard_index)
    owner = %{instance_ctx: ctx, shard_index: shard_index}
    shared_log_latch = Promotion.acquire_shared_log_latch(owner)
    latch_owner = self()

    on_exit(fn ->
      try do
        :ets.delete_object(latch_table, {:compound_promotion_shared_log, latch_owner})
      rescue
        ArgumentError -> :ok
      end
    end)

    assert :ok = put_hash_type(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, first, "one", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "two", 0)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)
      state.compound_promotion_pending[redis_key] == {:hash, 1}
    end)

    shard_state = :sys.get_state(shard)

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: shard_state.shard_data_path,
        active_file_id: shard_state.active_file_id,
        active_file_path: shard_state.active_file_path,
        active_file_size: shard_state.active_file_size,
        ets: shard_state.ets,
        data_dir: shard_state.data_dir,
        instance_ctx: ctx,
        instance_name: ctx.name,
        apply_context: shard_state.apply_context,
        promoted_instances: shard_state.promoted_instances
      })

    test_pid = self()

    apply_task =
      Task.async(fn ->
        Process.put(:ferricstore_promoted_route_hook, fn
          ^redis_key, ^first, nil ->
            send(test_pid, {:shared_route_selected, self()})

            receive do
              :continue_replicated_delete -> :ok
            end

          _other_key, _other_compound_key, _path ->
            :ok
        end)

        try do
          StateMachine.apply(%{}, {:compound_delete, first}, state)
        after
          Process.delete(:ferricstore_promoted_route_hook)
        end
      end)

    assert_receive {:shared_route_selected, apply_pid}, 5_000

    assert [{{:promoted_compaction, ^redis_key}, ^apply_pid}] =
             :ets.lookup(latch_table, {:promoted_compaction, redis_key})

    Promotion.release_compaction_latch(shared_log_latch)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)
      state.compound_promotion_worker == nil
    end)

    send(apply_pid, :continue_replicated_delete)
    assert {_next_state, :ok} = Task.await(apply_task, 5_000)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)

      state.compound_promotion_worker == nil and
        not Map.has_key?(state.compound_promotion_pending, redis_key)
    end)

    assert nil == Router.compound_get(ctx, redis_key, first)
    assert "two" == Router.compound_get(ctx, redis_key, second)
    refute_receive {:DOWN, ^shard_monitor, :process, ^shard_pid, _reason}, 500
  end

  test "stale successful completion does not reinstall a deleted promoted instance", %{ctx: ctx} do
    redis_key = "stale_promotion_completion_#{System.unique_integer([:positive])}"
    shard_index = Router.shard_for(ctx, redis_key)
    shard = elem(ctx.shard_names, shard_index)
    state = :sys.get_state(shard)
    job_ref = make_ref()
    worker_pid = self()
    monitor_ref = Process.monitor(worker_pid)

    dedicated_path = Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, redis_key)

    worker = %{
      job_ref: job_ref,
      monitor_ref: monitor_ref,
      pid: worker_pid,
      redis_key: redis_key,
      type: :hash,
      latch_token: :none,
      shared_log_latch_token: :none,
      active_file_id: state.active_file_id,
      active_file_path: state.active_file_path
    }

    :sys.replace_state(shard, &%{&1 | compound_promotion_worker: worker})

    waiter = Task.async(fn -> GenServer.call(shard, {:promoted?, redis_key}, 5_000) end)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)
      Map.has_key?(state.compound_promotion_waiters, redis_key)
    end)

    send(
      shard,
      {:compound_promotion_complete, job_ref, worker_pid, {:ok, dedicated_path}}
    )

    refute Task.await(waiter, 5_000)

    Ferricstore.Test.ShardHelpers.eventually(fn ->
      state = :sys.get_state(shard)

      state.compound_promotion_worker == nil and
        not Map.has_key?(state.promoted_instances, redis_key)
    end)
  end

  test "new promoted instance records its initial dedicated byte size", %{ctx: ctx} do
    redis_key = "promoted_initial_size_#{System.unique_integer([:positive])}"

    assert :ok = put_hash_type(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f1"),
               "value1",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f2"),
               "value2",
               0
             )

    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    state = :sys.get_state(shard)
    info = Map.fetch!(state.promoted_instances, redis_key)

    assert info.total_bytes == ShardCompound.promoted_dir_size(info.path)
    assert info.total_bytes > 0
    assert info.dead_bytes == 0
  end

  test "expiry sweep tracks dead bytes for promoted compound entries", %{ctx: ctx} do
    redis_key = "promoted_expiry_accounting_#{System.unique_integer([:positive])}"

    assert :ok = put_hash_type(ctx, redis_key)

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f1"),
               "value1",
               0
             )

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               CompoundKey.hash_field(redis_key, "f2"),
               "value2",
               0
             )

    expired_key = CompoundKey.hash_field(redis_key, "expired")
    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))

    assert :ok =
             Router.compound_put(
               ctx,
               redis_key,
               expired_key,
               "gone",
               HLC.now_ms() - 1
             )

    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    assert :ets.lookup(elem(ctx.latch_refs, 0), {:promoted_compaction, redis_key}) == []
    assert :ets.lookup(elem(ctx.latch_refs, 0), :compound_promotion_shared_log) == []
    state = :sys.get_state(shard)
    before_info = Map.fetch!(state.promoted_instances, redis_key)

    after_state = ShardLifecycle.do_expiry_sweep(state)
    after_info = Map.fetch!(after_state.promoted_instances, redis_key)

    assert after_info.dead_bytes > before_info.dead_bytes
    assert ShardCompound.promoted_dir_size(after_info.path) == before_info.total_bytes
    assert :ets.lookup(state.keydir, expired_key) == []
  end

  test "all-dead promoted compaction reclaims dedicated log bytes", %{ctx: ctx} do
    redis_key = "promoted_all_dead_compaction_#{System.unique_integer([:positive])}"
    value = String.duplicate("x", 600_000)
    field1 = CompoundKey.hash_field(redis_key, "f1")
    field2 = CompoundKey.hash_field(redis_key, "f2")

    assert :ok = put_hash_type(ctx, redis_key)
    assert :ok = Router.compound_put(ctx, redis_key, field1, value, 0)
    assert :ok = Router.compound_put(ctx, redis_key, field2, value, 0)

    shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
    assert GenServer.call(shard, {:promoted?, redis_key}, 30_000)
    promoted_before = :sys.get_state(shard).promoted_instances |> Map.fetch!(redis_key)
    size_before = ShardCompound.promoted_dir_size(promoted_before.path)

    assert size_before > 1_000_000

    assert :ok = Router.compound_delete(ctx, redis_key, field1)
    assert :ok = Router.compound_delete(ctx, redis_key, field2)

    promoted_after = await_compacted_promoted_info(shard, redis_key)
    size_after = ShardCompound.promoted_dir_size(promoted_after.path)

    assert promoted_after.dead_bytes == 0
    assert size_after < div(size_before, 10)
  end

  @tag :promotion_routed_revision
  test "routed promoted mutations publish revisions to the target shard table" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 2,
        hot_cache_max_value_size: 1_000_000,
        promotion_threshold: 1
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    promoted_key = key_for_shard(ctx, 1, "remote_promoted_revision")
    anchor_key = key_for_shard(ctx, 0, "remote_promoted_anchor")
    field = CompoundKey.hash_field(promoted_key, "field")

    assert :ok = put_hash_type(ctx, promoted_key)
    assert :ok = Router.compound_put(ctx, promoted_key, field, "before", 0)

    assert :ok =
             Router.compound_put(
               ctx,
               promoted_key,
               CompoundKey.hash_field(promoted_key, "trigger"),
               "value",
               0
             )

    promoted_shard = elem(ctx.shard_names, 1)
    assert GenServer.call(promoted_shard, {:promoted?, promoted_key}, 30_000)

    anchor_revision = CompoundRevisionIndex.table_name(ctx.name, 0)
    target_revision = CompoundRevisionIndex.table_name(ctx.name, 1)
    assert :missing = CompoundRevisionIndex.revision_token(anchor_revision, field)
    assert :missing = CompoundRevisionIndex.revision_token(target_revision, field)

    assert :ok =
             CrossShardOp.execute(
               [{anchor_key, :write}, {promoted_key, :write}],
               fn store ->
                 :ok = store.compound_put.(promoted_key, field, "after", 0)
                 :ok = store.put.(anchor_key, "touch", 0)
               end,
               instance: ctx
             )

    assert :missing = CompoundRevisionIndex.revision_token(anchor_revision, field)

    assert {:ok, {_epoch, revision}} =
             CompoundRevisionIndex.revision_token(target_revision, field)

    assert revision > 0
    assert "after" = Router.compound_get(ctx, promoted_key, field)
  end

  defp put_hash_type(ctx, redis_key) do
    Router.compound_put(
      ctx,
      redis_key,
      CompoundKey.type_key(redis_key),
      CompoundKey.encode_type(:hash),
      0
    )
  end

  defp key_for_shard(ctx, shard_index, prefix) do
    Enum.find_value(0..100_000, fn suffix ->
      key = "#{prefix}:#{suffix}"
      if Router.shard_for(ctx, key) == shard_index, do: key
    end)
  end

  defp await_compacted_promoted_info(shard, redis_key, attempts \\ 100)

  defp await_compacted_promoted_info(shard, redis_key, attempts) when attempts > 0 do
    info = :sys.get_state(shard).promoted_instances |> Map.fetch!(redis_key)

    if info.dead_bytes == 0 do
      info
    else
      Process.sleep(20)
      await_compacted_promoted_info(shard, redis_key, attempts - 1)
    end
  end

  defp await_compacted_promoted_info(shard, redis_key, 0) do
    :sys.get_state(shard).promoted_instances |> Map.fetch!(redis_key)
  end

  defp keydir_binary_total(ctx) do
    1..ctx.shard_count
    |> Enum.reduce(0, fn idx, acc -> acc + :atomics.get(ctx.keydir_binary_bytes, idx) end)
  end
end
