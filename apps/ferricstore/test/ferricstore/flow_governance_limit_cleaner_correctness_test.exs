defmodule Ferricstore.FlowGovernanceLimitCleanerCorrectnessTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.Governance.LimitStorageCleaner
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.Router

  setup do
    ctx = FerricStore.Instance.get(:default)
    cleaner = LimitStorageCleaner.process_name(ctx)
    :ok = :sys.suspend(cleaner)

    on_exit(fn ->
      if is_pid(Process.whereis(cleaner)), do: :sys.resume(cleaner)
    end)

    {:ok, ctx: ctx}
  end

  test "a pending scope consumes a bounded page burst before traversal advances", %{ctx: ctx} do
    [busy_scope, later_scope] =
      [unique_flow_id("cleaner-busy"), unique_flow_id("cleaner-later")]
      |> Enum.sort_by(&Keys.governance_limit_key/1)

    busy_ids = create_expired_limit(ctx, busy_scope, 300)
    [later_id] = create_expired_limit(ctx, later_scope, 1)

    assert %{commands: 2, deleted: 300, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 2)

    assert Enum.all?(busy_ids, fn id -> reservation(ctx, busy_scope, id) == nil end)
    assert reservation(ctx, later_scope, later_id) != nil

    # Advancing the catalog cursor would exceed the first tick's command budget.
    assert Router.get(ctx, Keys.governance_limit_cleanup_progress_key()) == nil

    assert %{commands: 2, deleted: 1, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 2)

    assert reservation(ctx, later_scope, later_id) == nil
  end

  test "an empty catalog does not issue an absent cursor delete", %{ctx: ctx} do
    progress_key = Keys.governance_limit_cleanup_progress_key()
    shard_index = Router.shard_for(ctx, progress_key)

    assert Router.get(ctx, progress_key) == nil
    before_index = applied_index(shard_index)

    assert %{commands: 0, deleted: 0, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 1)

    assert applied_index(shard_index) == before_index
    assert Router.get(ctx, progress_key) == nil
  end

  test "a non-canonical progress cursor is reset within the tick write budget", %{ctx: ctx} do
    scope = unique_flow_id("cleaner-corrupt-progress")
    [reservation_id] = create_expired_limit(ctx, scope, 1)
    progress_key = Keys.governance_limit_cleanup_progress_key()

    encoded =
      :erlang.term_to_binary({:flow_governance_limit_cleanup_progress, nil}) <> <<0>>

    assert :ok = Router.put(ctx, progress_key, encoded, 0)

    assert %{commands: 1, deleted: 0, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 1)

    assert Router.get(ctx, progress_key) == nil
    assert reservation(ctx, scope, reservation_id) != nil

    assert %{commands: 1, deleted: 1, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 1)
  end

  defp create_expired_limit(ctx, scope, amount) do
    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: amount,
               limit: amount,
               ttl_ms: 5,
               now_ms: 1_000
             )

    assert {:ok, %{reservation_ids: reservation_ids}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: amount, now_ms: 1_001)

    reservation_ids
  end

  defp reservation(ctx, scope, reservation_id) do
    Router.get(ctx, Keys.governance_limit_reservation_key(scope, 0, 1, reservation_id))
  end

  defp applied_index(shard_index) do
    assert {:ok, {:raft_log_pos, index, _term}} = WARaftBackend.storage_position(shard_index)
    index
  end
end
