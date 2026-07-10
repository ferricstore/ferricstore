defmodule Ferricstore.FlowGovernanceCatalogTest do
  use Ferricstore.Test.FlowCase

  @stores ~w(approval_store budget_store circuit_store limit_store ledger)

  test "governance list paths never enumerate the full application keyspace" do
    Enum.each(@stores, fn store ->
      source =
        __DIR__
        |> Path.join("../../lib/ferricstore/flow/governance/#{store}.ex")
        |> Path.expand()
        |> File.read!()

      refute source =~ "Router.keys()", "#{store} must use its governance-owned catalog"
    end)
  end

  test "governance catalog traversal is unbounded in count but bounded in memory" do
    catalog_source =
      __DIR__
      |> Path.join("../../lib/ferricstore/flow/governance/catalog.ex")
      |> Path.expand()
      |> File.read!()

    refute catalog_source =~ "@max_scan"
    assert catalog_source =~ "@page_size"
    assert catalog_source =~ "def collect("

    Enum.each(~w(approval_store budget_store circuit_store limit_store), fn store ->
      source =
        __DIR__
        |> Path.join("../../lib/ferricstore/flow/governance/#{store}.ex")
        |> Path.expand()
        |> File.read!()

      assert source =~ "Catalog.collect(", "#{store} must consume catalog pages incrementally"
      refute source =~ "Catalog.keys(", "#{store} must not materialize every catalog key"
    end)
  end

  test "catalog collection keeps global top results across pages" do
    ctx = FerricStore.Instance.get(:default)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    prefix = "catalog-page-test-#{suffix}"
    catalog_key = Ferricstore.Flow.Keys.governance_catalog_key(:budget)

    members =
      Enum.map(0..299, fn index ->
        prefix <> ":" <> String.pad_leading(Integer.to_string(index), 3, "0")
      end)

    assert {:ok, _added} =
             FerricStore.Impl.zadd(ctx, catalog_key, Enum.map(members, &{0, &1}))

    on_exit(fn -> FerricStore.Impl.zrem(ctx, catalog_key, members) end)

    assert {:ok, top_five} =
             Ferricstore.Flow.Governance.Catalog.collect(
               ctx,
               :budget,
               5,
               fn key -> if String.starts_with?(key, prefix), do: {:ok, key}, else: :skip end,
               & &1,
               :desc
             )

    assert top_five == members |> Enum.reverse() |> Enum.take(5)
  end

  test "exact-scope lists derive record keys without scanning catalog membership" do
    ctx = FerricStore.Instance.get(:default)
    suffix = Integer.to_string(System.unique_integer([:positive]))

    records = [
      {:limit, "direct-limit-#{suffix}", &Ferricstore.Flow.Keys.governance_limit_key/1},
      {:budget, "direct-budget-#{suffix}", &Ferricstore.Flow.Keys.governance_budget_key/1},
      {:circuit, "direct-circuit-#{suffix}", &Ferricstore.Flow.Keys.governance_circuit_key/1}
    ]

    approval_id = "direct-approval-#{suffix}"
    approval_scope = "direct-approval-scope-#{suffix}"

    [{:limit, limit_scope, _}, {:budget, budget_scope, _}, {:circuit, circuit_scope, _}] = records

    assert {:ok, _} =
             FerricStore.flow_limit_lease(limit_scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_budget_reserve(budget_scope, 1,
               limit: 10,
               window_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_circuit_open(circuit_scope,
               failure_threshold: 1,
               open_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "direct-approval-flow-#{suffix}",
               scope: approval_scope,
               now_ms: 1_000
             )

    Enum.each(records, fn {kind, scope, key_fun} ->
      catalog_key = Ferricstore.Flow.Keys.governance_catalog_key(kind)
      assert {:ok, 1} = FerricStore.Impl.zrem(ctx, catalog_key, [key_fun.(scope)])
    end)

    assert {:ok, 1} =
             FerricStore.Impl.zrem(
               ctx,
               Ferricstore.Flow.Keys.governance_catalog_key(:approval),
               [Ferricstore.Flow.Keys.governance_approval_key(approval_id)]
             )

    assert {:ok, [%{scope: ^limit_scope}]} =
             FerricStore.flow_limit_list(scope: limit_scope, now_ms: 1_001)

    assert {:ok, [%{scope: ^budget_scope}]} = FerricStore.flow_budget_list(scope: budget_scope)
    assert {:ok, [%{scope: ^circuit_scope}]} = FerricStore.flow_circuit_list(scope: circuit_scope)

    assert {:ok, [%{id: ^approval_id}]} =
             FerricStore.flow_approval_list(scope: approval_scope)
  end

  test "catalog paging anchors on the last member during concurrent insertion" do
    ctx = FerricStore.Instance.get(:default)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    prefix = "catalog-anchor-test-#{suffix}"
    catalog_key = Ferricstore.Flow.Keys.governance_catalog_key(:approval)
    inserted = prefix <> ":000-before"

    members =
      Enum.map(1..300, fn index ->
        prefix <> ":" <> String.pad_leading(Integer.to_string(index), 3, "0")
      end)

    assert {:ok, _added} =
             FerricStore.Impl.zadd(ctx, catalog_key, Enum.map(members, &{0, &1}))

    on_exit(fn -> FerricStore.Impl.zrem(ctx, catalog_key, [inserted | members]) end)

    Process.put({__MODULE__, :inserted}, false)

    assert {:ok, seen} =
             Ferricstore.Flow.Governance.Catalog.reduce_pages(ctx, :approval, [], fn keys, acc ->
               matching = Enum.filter(keys, &String.starts_with?(&1, prefix))

               unless Process.get({__MODULE__, :inserted}) do
                 assert {:ok, 1} = FerricStore.Impl.zadd(ctx, catalog_key, [{0, inserted}])
                 Process.put({__MODULE__, :inserted}, true)
               end

               acc ++ matching
             end)

    assert seen == members
  end

  test "governance catalogs preserve limit, budget, circuit, and approval listing" do
    suffix = Integer.to_string(System.unique_integer([:positive]))
    limit_scope = "catalog-limit-#{suffix}"
    budget_scope = "catalog-budget-#{suffix}"
    circuit_scope = "catalog-circuit-#{suffix}"
    approval_id = "catalog-approval-#{suffix}"

    assert {:ok, _} =
             FerricStore.flow_limit_lease(limit_scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_budget_reserve(budget_scope, 1,
               limit: 10,
               window_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_circuit_open(circuit_scope,
               failure_threshold: 1,
               open_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, _} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "catalog-flow-#{suffix}",
               scope: "catalog-scope-#{suffix}",
               now_ms: 1_000
             )

    assert {:ok, limits} = FerricStore.flow_limit_list(limit: 100, now_ms: 1_001)
    assert Enum.any?(limits, &(&1.scope == limit_scope))

    assert {:ok, budgets} = FerricStore.flow_budget_list(limit: 100)
    assert Enum.any?(budgets, &(&1.scope == budget_scope))

    assert {:ok, circuits} = FerricStore.flow_circuit_list(limit: 100)
    assert Enum.any?(circuits, &(&1.scope == circuit_scope))

    assert {:ok, approvals} = FerricStore.flow_approval_list(limit: 100)
    assert Enum.any?(approvals, &(&1.id == approval_id))
  end

  @tag :approval_catalog_repair
  test "approval NX conflicts cannot poison attacker-supplied exact catalogs" do
    ctx = FerricStore.Instance.get(:default)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    approval_id = "approval-nx-catalog-#{suffix}"
    original_scope = "approval-original-scope-#{suffix}"
    original_flow = "approval-original-flow-#{suffix}"
    attacker_scope = "approval-attacker-scope-#{suffix}"
    attacker_flow = "approval-attacker-flow-#{suffix}"
    approval_key = Ferricstore.Flow.Keys.governance_approval_key(approval_id)

    assert {:ok, _approval} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: original_flow,
               scope: original_scope,
               now_ms: 1_000
             )

    assert {:error, %{code: "GOVERNANCE_CONFLICT"}} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: attacker_flow,
               scope: attacker_scope,
               now_ms: 1_001
             )

    assert {:ok, attacker_scope_members} =
             FerricStore.Impl.zrange(
               ctx,
               Ferricstore.Flow.Keys.governance_approval_scope_catalog_key(attacker_scope),
               0,
               -1,
               []
             )

    assert {:ok, attacker_flow_members} =
             FerricStore.Impl.zrange(
               ctx,
               Ferricstore.Flow.Keys.governance_approval_flow_catalog_key(attacker_flow),
               0,
               -1,
               []
             )

    refute approval_key in attacker_scope_members
    refute approval_key in attacker_flow_members

    assert {:ok, [%{id: ^approval_id}]} =
             FerricStore.flow_approval_list(scope: original_scope)

    assert {:ok, [%{id: ^approval_id}]} =
             FerricStore.flow_approval_list(flow_id: original_flow)
  end

  @tag :approval_catalog_repair
  test "filtered approval lists repair missing and stale exact membership" do
    ctx = FerricStore.Instance.get(:default)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    approval_id = "approval-exact-repair-#{suffix}"
    scope = "approval-exact-scope-#{suffix}"
    flow_id = "approval-exact-flow-#{suffix}"
    stale_scope = "approval-stale-scope-#{suffix}"
    stale_flow = "approval-stale-flow-#{suffix}"
    approval_key = Ferricstore.Flow.Keys.governance_approval_key(approval_id)
    scope_catalog = Ferricstore.Flow.Keys.governance_approval_scope_catalog_key(scope)
    flow_catalog = Ferricstore.Flow.Keys.governance_approval_flow_catalog_key(flow_id)

    stale_scope_catalog =
      Ferricstore.Flow.Keys.governance_approval_scope_catalog_key(stale_scope)

    stale_flow_catalog = Ferricstore.Flow.Keys.governance_approval_flow_catalog_key(stale_flow)

    assert {:ok, _approval} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: flow_id,
               scope: scope,
               now_ms: 1_000
             )

    assert {:ok, 1} = FerricStore.Impl.zrem(ctx, scope_catalog, [approval_key])
    assert {:ok, 1} = FerricStore.Impl.zrem(ctx, flow_catalog, [approval_key])
    assert {:ok, 1} = FerricStore.Impl.zadd(ctx, stale_scope_catalog, [{0, approval_key}])
    assert {:ok, 1} = FerricStore.Impl.zadd(ctx, stale_flow_catalog, [{0, approval_key}])

    assert {:ok, [%{id: ^approval_id}]} = FerricStore.flow_approval_list(scope: scope)
    assert {:ok, [%{id: ^approval_id}]} = FerricStore.flow_approval_list(flow_id: flow_id)
    assert {:ok, []} = FerricStore.flow_approval_list(scope: stale_scope)
    assert {:ok, []} = FerricStore.flow_approval_list(flow_id: stale_flow)

    assert {:ok, "0.0"} = FerricStore.Impl.zscore(ctx, scope_catalog, approval_key)
    assert {:ok, "0.0"} = FerricStore.Impl.zscore(ctx, flow_catalog, approval_key)
    assert {:ok, nil} = FerricStore.Impl.zscore(ctx, stale_scope_catalog, approval_key)
    assert {:ok, nil} = FerricStore.Impl.zscore(ctx, stale_flow_catalog, approval_key)
  end

  @tag :approval_catalog_repair
  test "approval exact repair is bounded and resumes from durable progress" do
    ctx = FerricStore.Instance.get(:default)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    approval_id = "approval-repair-resume-#{suffix}"
    scope = "approval-repair-resume-scope-#{suffix}"
    flow_id = "approval-repair-resume-flow-#{suffix}"
    approval_key = Ferricstore.Flow.Keys.governance_approval_key(approval_id)
    target_catalog = Ferricstore.Flow.Keys.governance_approval_scope_catalog_key(scope)
    source_catalog = Ferricstore.Flow.Keys.governance_catalog_key(:approval)

    progress_key = Ferricstore.Flow.Governance.ApprovalCatalogRepair.source_progress_key()

    assert {:ok, _approval} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: flow_id,
               scope: scope,
               now_ms: 1_000
             )

    assert {:ok, 1} = FerricStore.Impl.zrem(ctx, target_catalog, [approval_key])

    orphan_keys =
      Enum.map(
        1..(Ferricstore.Flow.Governance.ApprovalCatalogRepair.page_size() + 1),
        &<<1, "approval-repair-orphan-", suffix::binary, "-",
          String.pad_leading(Integer.to_string(&1), 3, "0")::binary>>
      )

    assert {:ok, orphan_count} =
             FerricStore.Impl.zadd(ctx, target_catalog, Enum.map(orphan_keys, &{0, &1}))

    assert orphan_count == length(orphan_keys)

    dummy_keys =
      Enum.map(
        1..Ferricstore.Flow.Governance.ApprovalCatalogRepair.page_size(),
        &<<1, "approval-repair-dummy-",
          String.pad_leading(Integer.to_string(&1), 3, "0")::binary>>
      )

    assert {:ok, added} =
             FerricStore.Impl.zadd(ctx, source_catalog, Enum.map(dummy_keys, &{0, &1}))

    assert added == length(dummy_keys)

    first_caller = Task.async(fn -> FerricStore.flow_approval_list(scope: scope) end)
    assert Task.await(first_caller) == {:ok, []}
    assert is_binary(Ferricstore.Store.Router.get(ctx, progress_key))
    assert {:ok, 1} = FerricStore.Impl.zcard(ctx, target_catalog)

    second_caller = Task.async(fn -> FerricStore.flow_approval_list(scope: scope) end)
    assert {:ok, [%{id: ^approval_id}]} = Task.await(second_caller)
    assert {:ok, "0.0"} = FerricStore.Impl.zscore(ctx, target_catalog, approval_key)
    assert {:ok, 1} = FerricStore.Impl.zcard(ctx, target_catalog)

    repair_source =
      __DIR__
      |> Path.join("../../lib/ferricstore/flow/governance/approval_catalog_repair.ex")
      |> Path.expand()
      |> File.read!()

    refute repair_source =~ "Router.keys"
    assert repair_source =~ "Catalog.page("
    assert repair_source =~ "Catalog.page_key("

    empty_scope = "approval-empty-scope-#{suffix}"
    empty_catalog = Ferricstore.Flow.Keys.governance_approval_scope_catalog_key(empty_scope)

    empty_progress =
      Ferricstore.Flow.Governance.ApprovalCatalogRepair.target_progress_key(empty_catalog)

    assert Ferricstore.Store.Router.get(ctx, empty_progress) == nil
    assert FerricStore.flow_approval_list(scope: empty_scope) == {:ok, []}
    assert Ferricstore.Store.Router.get(ctx, empty_progress) == nil
  end
end
