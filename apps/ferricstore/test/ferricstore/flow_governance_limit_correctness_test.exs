defmodule Ferricstore.FlowGovernanceLimitCorrectnessTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Flow.Governance.CreditLease

  @partition "tenant-governance-limit-correctness"

  setup do
    old_enabled = Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, false)

    on_exit(fn -> restore_env(:flow_governance_limit_cache_enabled, old_enabled) end)
    :ok
  end

  test "terminal completion releases the reservation captured by claim" do
    type = unique_flow_id("bound-limit-type")
    scope = unique_flow_id("bound-limit")
    [first_id, second_id] = create_due_flows(type, 2)

    lease_limit!(scope, 1)
    first = claim_one!(type, scope, 0, 1_001)
    assert first.id == first_id
    assert %{scope: ^scope, shard_id: 0} = raw_record(first.id).governance_limit

    assert :ok =
             FerricStore.flow_complete(first.id, first.lease_token,
               partition_key: @partition,
               fencing_token: first.fencing_token,
               now_ms: 1_002
             )

    assert {:ok, [second]} = claim(type, scope, 0, 1_003)
    assert second.id == second_id
  end

  test "terminal caller cannot redirect release to another governance scope" do
    type = unique_flow_id("bound-scope-type")
    claimed_scope = unique_flow_id("claimed-scope")
    unrelated_scope = unique_flow_id("unrelated-scope")
    create_due_flows(type, 1)

    lease_limit!(claimed_scope, 1)
    lease_limit!(unrelated_scope, 1)

    claimed = claim_one!(type, claimed_scope, 0, 1_001)

    assert {:ok, _spent} =
             FerricStore.flow_limit_spend(unrelated_scope,
               shard_id: 0,
               amount: 1,
               now_ms: 1_001
             )

    assert :ok =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002,
               governance_limit_scope: unrelated_scope,
               governance_shard_id: 0
             )

    assert {:ok, claimed_owner} = FerricStore.flow_limit_get(claimed_scope, now_ms: 1_003)
    assert claimed_owner.leases[0].in_use == 0

    assert {:ok, unrelated_owner} =
             FerricStore.flow_limit_get(unrelated_scope, now_ms: 1_003)

    assert unrelated_owner.leases[0].in_use == 1
  end

  test "duplicate terminal completion releases a reservation exactly once" do
    type = unique_flow_id("idempotent-limit-type")
    scope = unique_flow_id("idempotent-limit")
    _ids = create_due_flows(type, 4)

    lease_limit!(scope, 2)
    first = claim_one!(type, scope, 0, 1_001)
    _second = claim_one!(type, scope, 0, 1_002)

    complete_opts = [
      partition_key: @partition,
      fencing_token: first.fencing_token,
      now_ms: 1_003
    ]

    assert :ok = FerricStore.flow_complete(first.id, first.lease_token, complete_opts)
    assert :ok = FerricStore.flow_complete(first.id, first.lease_token, complete_opts)

    assert {:ok, [_third]} = claim(type, scope, 0, 1_004)

    assert {:error, denial} = claim(type, scope, 0, 1_005)
    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"
  end

  test "retry, fail, and cancel release their captured reservation" do
    Enum.each([:retry, :fail, :cancel], fn operation ->
      Ferricstore.Test.ShardHelpers.flush_all_keys()

      type = unique_flow_id("#{operation}-limit-type")
      scope = unique_flow_id("#{operation}-limit")
      create_due_flows(type, 1)
      lease_limit!(scope, 1)
      claimed = claim_one!(type, scope, 0, 1_001)

      assert :ok = apply_terminal(operation, claimed)
      assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
      assert owner.leases[0].in_use == 0
    end)
  end

  test "Flow lease extension renews the bound governance credit lease" do
    type = unique_flow_id("renew-limit-type")
    scope = unique_flow_id("renew-limit")
    create_due_flows(type, 1)

    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 100,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "worker-renew",
               lease_ms: 100,
               limit: 1,
               now_ms: 1_001,
               governance_limit_scope: scope,
               governance_shard_id: 0
             )

    assert {:ok, extended} =
             FerricStore.flow_extend_lease(claimed.id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               lease_ms: 1_000,
               now_ms: 1_050
             )

    assert extended.lease_deadline_ms == 2_050
    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_101)
    assert owner.leases[0].in_use == 1
    assert owner.leases[0].expires_at_ms >= extended.lease_deadline_ms
  end

  test "limit cache table is owned by its supervised process" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    scope = unique_flow_id("cache-owner-limit")
    lease_limit!(scope, 4)
    ctx = FerricStore.Instance.get(:default)
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        result =
          Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope,
            shard_id: 0,
            amount: 1,
            ttl_ms: 1_000,
            now_ms: 1_001
          )

        send(parent, {:cache_spend, result})
      end)

    assert_receive {:cache_spend, {:ok, _spent}}
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    table = :ferricstore_flow_governance_limit_cache
    assert :ets.whereis(table) != :undefined
    assert :ets.info(table, :owner) == Process.whereis(Ferricstore.Flow.Governance.LimitCache)
  end

  test "limit cache rejects invalid entry shapes instead of granting credits" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("invalid-cache-entry")
    key = {ctx.name, scope, 0}
    table = :ferricstore_flow_governance_limit_cache

    true = :ets.insert(table, {key, 1, 2_000, 1, ["unbacked-reservation"]})

    assert {:error, "ERR flow limit not found"} =
             Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_000
             )

    assert [] == :ets.lookup(table, key)
  end

  test "strict global running policy enforces claims without caller limit options" do
    type = unique_flow_id("policy-limit-type")
    _ids = create_due_flows(type, 2)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 limits: %{
                   "running" => %{limit: 1, enforcement: :strict_global, lease_size: 1}
                 }
               }
             )

    claim_opts = [
      states: ["queued"],
      partition_key: @partition,
      worker: "policy-worker",
      limit: 1,
      lease_ms: 1_000,
      now_ms: 1_001
    ]

    assert {:ok, [first]} = FerricStore.flow_claim_due(type, claim_opts)

    assert {:error, denial} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_002))

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"

    assert :ok =
             FerricStore.flow_complete(first.id, first.lease_token,
               partition_key: @partition,
               fencing_token: first.fencing_token,
               now_ms: 1_003
             )

    assert {:ok, [_second]} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_004))
  end

  test "approximate global running policy bounds prefetched claims and recycles completion" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    type = unique_flow_id("approx-policy-limit-type")
    _ids = create_due_flows(type, 5)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 limits: %{
                   "running" => %{
                     limit: 4,
                     enforcement: :approximate_global,
                     lease_size: 4
                   }
                 }
               }
             )

    claim_opts = [
      states: ["queued"],
      partition_key: @partition,
      worker: "approx-policy-worker",
      limit: 1,
      lease_ms: 1_000,
      now_ms: 1_001
    ]

    claimed =
      for offset <- 0..3 do
        assert {:ok, [record]} =
                 FerricStore.flow_claim_due(
                   type,
                   Keyword.put(claim_opts, :now_ms, 1_001 + offset)
                 )

        record
      end

    assert {:error, denial} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_005))

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"

    [first | _rest] = claimed

    assert :ok =
             FerricStore.flow_complete(first.id, first.lease_token,
               partition_key: @partition,
               fencing_token: first.fencing_token,
               now_ms: 1_006
             )

    assert {:ok, [_replacement]} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_007))
  end

  test "multi-state claims enforce the strictest resolved running policy" do
    type = unique_flow_id("multi-state-policy-limit-type")

    for {state, suffix} <- [{"queued", "queued"}, {"retry", "retry"}] do
      assert :ok =
               FerricStore.flow_create(unique_flow_id("multi-state-#{suffix}"),
                 type: type,
                 state: state,
                 partition_key: @partition,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{
                 limits: %{
                   "running" => %{limit: 4, enforcement: :approximate_global, lease_size: 4}
                 }
               },
               states: %{
                 "queued" => [
                   governance: [
                     limits: %{
                       "running" => %{limit: 1, enforcement: :strict_global, lease_size: 1}
                     }
                   ]
                 ],
                 "retry" => [
                   governance: [
                     limits: %{
                       "running" => %{limit: 2, enforcement: :approximate_global, lease_size: 2}
                     }
                   ]
                 ]
               }
             )

    assert {:error, denial} =
             FerricStore.flow_claim_due(type,
               states: ["queued", "retry"],
               partition_key: @partition,
               worker: "multi-state-policy-worker",
               limit: 2,
               lease_ms: 1_000,
               now_ms: 1_001
             )

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"
  end

  test "batched and independent completion release only committed reservations" do
    Enum.each([false, true], fn independent? ->
      Ferricstore.Test.ShardHelpers.flush_all_keys()

      type = unique_flow_id("many-limit-type")
      scope = unique_flow_id("many-limit")
      create_due_flows(type, 3)
      lease_limit!(scope, 3)

      claimed = for now_ms <- 1_001..1_003, do: claim_one!(type, scope, 0, now_ms)
      [first, second, _third] = claimed

      items =
        Enum.map([first, second], fn record ->
          %{
            id: record.id,
            lease_token: record.lease_token,
            fencing_token: record.fencing_token
          }
        end)

      opts = [now_ms: 1_004, independent: independent?]
      assert terminal_many_success?(FerricStore.flow_complete_many(@partition, items, opts))

      assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_005)

      assert owner.leases[0].in_use == 1,
             "independent=#{independent?} must release exactly two reservations"

      assert terminal_many_success?(FerricStore.flow_complete_many(@partition, items, opts))
      assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_006)

      assert owner.leases[0].in_use == 1,
             "independent=#{independent?} replay must not release again"
    end)
  end

  test "bound governance reservation is not exposed in public Flow records" do
    type = unique_flow_id("private-limit-type")
    scope = unique_flow_id("private-limit")
    create_due_flows(type, 1)
    lease_limit!(scope, 1)
    claimed = claim_one!(type, scope, 0, 1_001)

    assert {:ok, record} = FerricStore.flow_get(claimed.id, partition_key: @partition)
    refute Map.has_key?(record, :governance_limit)
  end

  test "durable reservation ids make replayed release unable to free another flow" do
    type = unique_flow_id("reservation-id-limit-type")
    scope = unique_flow_id("reservation-id-limit")
    create_due_flows(type, 2)
    lease_limit!(scope, 2)

    first = claim_one!(type, scope, 0, 1_001)
    second = claim_one!(type, scope, 0, 1_002)
    first_id = raw_record(first.id).governance_limit.reservation_id
    second_id = raw_record(second.id).governance_limit.reservation_id

    assert is_binary(first_id)
    assert is_binary(second_id)
    assert first_id != second_id

    ctx = FerricStore.Instance.get(:default)
    release_opts = [shard_id: 0, amount: 1, reservation_ids: [first_id], now_ms: 1_003]
    assert {:ok, _} = Ferricstore.Flow.Governance.LimitStore.release(ctx, scope, release_opts)
    assert {:ok, _} = Ferricstore.Flow.Governance.LimitStore.release(ctx, scope, release_opts)

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_004)
    assert owner.leases[0].in_use == 1
  end

  test "public limit release requires exact reservation ids" do
    scope = unique_flow_id("exact-release-limit")
    lease_limit!(scope, 2)

    assert {:ok, %{reservation_ids: [first_id, second_id]}} =
             FerricStore.flow_limit_spend(scope,
               shard_id: 0,
               amount: 2,
               now_ms: 1_001
             )

    assert {:error, "ERR flow limit reservation_ids must contain one unique id per credit"} =
             FerricStore.flow_limit_release(scope, shard_id: 0, amount: 1)

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_002)
    assert owner.leases[0].in_use == 2

    assert {:ok, owner} =
             FerricStore.flow_limit_release(scope,
               shard_id: 0,
               reservation_ids: [first_id]
             )

    assert owner.leases[0].in_use == 1

    assert {:ok, replayed} =
             FerricStore.flow_limit_release(scope,
               shard_id: 0,
               reservation_ids: [first_id]
             )

    assert replayed.leases[0].in_use == 1
    assert second_id != first_id
  end

  test "terminal transition rejects governance metadata without a reservation id" do
    type = unique_flow_id("invalid-terminal-limit-type")
    scope = unique_flow_id("invalid-terminal-limit")
    create_due_flows(type, 1)
    lease_limit!(scope, 1)
    claimed = claim_one!(type, scope, 0, 1_001)
    record = raw_record(claimed.id)
    invalid_limit = Map.delete(record.governance_limit, :reservation_id)
    invalid_record = Map.put(record, :governance_limit, invalid_limit)
    ctx = FerricStore.Instance.get(:default)
    state_key = Ferricstore.Flow.Keys.state_key(claimed.id, @partition)
    shard = Ferricstore.Store.Router.shard_for(ctx, state_key)

    assert :ok =
             Ferricstore.Raft.WARaftBackend.write(
               shard,
               {:put, state_key, Ferricstore.Flow.encode_record(invalid_record), 0}
             )

    assert {:error, "ERR invalid flow governance limit reservation"} =
             FerricStore.flow_complete(claimed.id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.leases[0].in_use == 1
    assert raw_record(claimed.id).state == "running"
  end

  test "existing limit owner applies only monotonic capacity configurations" do
    scope = unique_flow_id("limit-reconfiguration")

    assert {:ok, %{owner: owner}} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 4,
               limit: 4,
               config_version: 1,
               policy_version: "policy-v1",
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert owner.config_version == 1

    assert owner.policy_version_hash ==
             "policy-v1"
             |> CreditLease.policy_version_fingerprint()
             |> CreditLease.policy_version_hash()

    refute Map.has_key?(owner, :policy_version)

    assert {:ok, %{reservation_ids: [first_id, _second_id, _third_id]}} =
             FerricStore.flow_limit_spend(scope,
               shard_id: 0,
               amount: 3,
               now_ms: 1_001
             )

    assert {:error, denial} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 1,
               limit: 2,
               config_version: 2,
               policy_version: "policy-v2",
               ttl_ms: 30_000,
               now_ms: 1_002
             )

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"
    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.limit == 2
    assert owner.config_version == 2

    assert owner.policy_version_hash ==
             "policy-v2"
             |> CreditLease.policy_version_fingerprint()
             |> CreditLease.policy_version_hash()

    assert owner.free == 0
    assert owner.leases[0].available == 0
    assert owner.leases[0].in_use == 3

    assert {:error, stale_denial} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 1,
               limit: 10,
               config_version: 1,
               policy_version: "stale-policy",
               ttl_ms: 30_000,
               now_ms: 1_004
             )

    assert stale_denial.code == "GOVERNANCE_LIMIT_EXCEEDED"

    assert {:error, "ERR flow limit config_version conflict"} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 1,
               limit: 3,
               config_version: 2,
               policy_version: "conflict",
               ttl_ms: 30_000,
               now_ms: 1_005
             )

    assert {:ok, _owner} =
             FerricStore.flow_limit_release(scope,
               shard_id: 0,
               reservation_ids: [first_id]
             )

    assert {:ok, %{owner: owner, lease: lease}} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: 1,
               limit: 5,
               config_version: 3,
               policy_version: "policy-v3",
               ttl_ms: 30_000,
               now_ms: 1_006
             )

    assert owner.limit == 5
    assert owner.config_version == 3

    assert owner.policy_version_hash ==
             "policy-v3"
             |> CreditLease.policy_version_fingerprint()
             |> CreditLease.policy_version_hash()

    assert owner.free == 2
    assert lease.available == 1
    assert lease.in_use == 2
  end

  test "running policy generations reconfigure an existing limit owner" do
    type = unique_flow_id("policy-reconfigured-limit-type")
    [first_flow_id, second_flow_id] = create_due_flows(type, 2)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               version: "policy-v1",
               governance: %{
                 mode: "full",
                 limits: %{
                   "running" => %{
                     limit: 2,
                     lease_size: 2,
                     enforcement: "strict_global"
                   }
                 }
               }
             )

    claim_opts = [
      states: ["queued"],
      partition_key: @partition,
      worker: "policy-reconfiguration-worker",
      lease_ms: 30_000,
      limit: 1,
      now_ms: 1_001
    ]

    assert {:ok, [first]} = FerricStore.flow_claim_due(type, claim_opts)
    assert first.id == first_flow_id

    scope = running_limit_scope(type)
    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_002)
    assert owner.limit == 2
    assert owner.config_version == 1

    assert owner.policy_version_hash ==
             "policy-v1"
             |> CreditLease.policy_version_fingerprint()
             |> CreditLease.policy_version_hash()

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               version: "policy-v2",
               governance: %{
                 mode: "full",
                 limits: %{
                   "running" => %{
                     limit: 1,
                     lease_size: 1,
                     enforcement: "strict_global"
                   }
                 }
               }
             )

    assert {:error, denial} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_003))

    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"
    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_004)
    assert owner.limit == 1
    assert owner.config_version == 2

    assert owner.policy_version_hash ==
             "policy-v2"
             |> CreditLease.policy_version_fingerprint()
             |> CreditLease.policy_version_hash()

    assert owner.leases |> Map.values() |> Enum.map(& &1.in_use) |> Enum.sum() == 1

    assert :ok =
             FerricStore.flow_complete(first.id, first.lease_token,
               partition_key: @partition,
               fencing_token: first.fencing_token,
               now_ms: 1_005
             )

    assert {:ok, [second]} =
             FerricStore.flow_claim_due(type, Keyword.put(claim_opts, :now_ms, 1_006))

    assert second.id == second_flow_id
  end

  test "large valid policy versions are fingerprinted across governed claims" do
    type = unique_flow_id("large-policy-version-limit-type")
    version = String.duplicate("policy-version-", 2_000)
    create_due_flows(type, 1)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               version: version,
               governance: %{
                 mode: "full",
                 limits: %{
                   "running" => %{
                     limit: 1,
                     lease_size: 1,
                     enforcement: "strict_global"
                   }
                 }
               }
             )

    assert {:ok, [_claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "large-policy-version-worker",
               lease_ms: 1_000,
               limit: 1,
               now_ms: 1_001
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(running_limit_scope(type), now_ms: 1_002)

    assert owner.policy_version_hash ==
             version
             |> CreditLease.policy_version_fingerprint()
             |> CreditLease.policy_version_hash()

    refute Map.has_key?(owner, :policy_version)
  end

  test "reconciler releases a terminal reservation after post-commit release is skipped" do
    type = unique_flow_id("reconcile-terminal-limit-type")
    scope = unique_flow_id("reconcile-terminal-limit")
    create_due_flows(type, 1)
    lease_limit!(scope, 1)
    claimed = claim_one!(type, scope, 0, 1_001)
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, attrs} =
             Ferricstore.Flow.MutationAttrs.complete_attrs(claimed.id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002
             )

    _committed_without_api_release = Ferricstore.Store.Router.flow_complete(ctx, attrs)
    assert raw_record(claimed.id).state == "completed"
    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.leases[0].in_use == 1

    assert {:ok, %{released: 1}} =
             Ferricstore.Flow.Governance.LimitReconciler.run_once(ctx,
               now_ms: 1_003,
               scope_limit: 256
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_004)
    assert owner.leases[0].in_use == 0
  end

  test "reconciler releases governance credit after automatic max-active timeout" do
    type = unique_flow_id("reconcile-timeout-limit-type")
    scope = unique_flow_id("reconcile-timeout-limit")
    id = unique_flow_id("reconcile-timeout-flow")

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, max_active_ms: 100)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    lease_limit!(scope, 1)
    _claimed = claim_one!(type, scope, 0, 1_001)

    assert {:ok, %{active_timeouts: 1}} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: 1_100)

    assert raw_record(id).state == "failed"
    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_101)
    assert owner.leases[0].in_use == 1

    ctx = FerricStore.Instance.get(:default)

    assert {:ok, %{released: 1}} =
             Ferricstore.Flow.Governance.LimitReconciler.run_once(ctx,
               now_ms: 1_101,
               scope_limit: 256
             )

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_102)
    assert owner.leases[0].in_use == 0
  end

  test "duplicate cache release cannot mint credits beyond the durable chunk" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    scope = unique_flow_id("cache-cap-limit")
    lease_limit!(scope, 4)
    ctx = FerricStore.Instance.get(:default)
    opts = [shard_id: 0, amount: 1, ttl_ms: 1_000, now_ms: 1_001]

    assert {:ok, %{reservation_ids: [reservation_id]}} =
             Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope, opts)

    release_opts = Keyword.put(opts, :reservation_ids, [reservation_id])
    assert {:ok, _} = Ferricstore.Flow.Governance.LimitCache.release(ctx, scope, release_opts)
    assert {:ok, _} = Ferricstore.Flow.Governance.LimitCache.release(ctx, scope, release_opts)

    Enum.each(1..4, fn _index ->
      assert {:ok, _} = Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope, opts)
    end)

    assert {:error, denial} = Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope, opts)
    assert denial.code == "GOVERNANCE_LIMIT_EXCEEDED"
  end

  test "cache shutdown flush returns only unused prefetched reservations" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    scope = unique_flow_id("cache-flush-limit")
    lease_limit!(scope, 4)
    ctx = FerricStore.Instance.get(:default)
    opts = [shard_id: 0, amount: 1, ttl_ms: 1_000, now_ms: 1_001]

    assert {:ok, %{reservation_ids: [_active_id]}} =
             Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope, opts)

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_002)
    assert owner.leases[0].in_use == 4

    assert {:ok, %{released: 3, errors: 0}} =
             Ferricstore.Flow.Governance.LimitCache.flush(ctx, now_ms: 1_003)

    Ferricstore.Flow.Governance.LimitCache.clear()

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_004)
    assert owner.leases[0].in_use == 1
    assert owner.leases[0].available == 3
  end

  test "flush never releases a cached reservation taken before atomic detach" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    scope = unique_flow_id("cache-flush-take-race")
    lease_limit!(scope, 4)
    ctx = FerricStore.Instance.get(:default)
    opts = [shard_id: 0, amount: 1, ttl_ms: 1_000, now_ms: 1_001]

    assert {:ok, %{reservation_ids: [_active_id]}} =
             Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope, opts)

    parent = self()
    hook_calls = :atomics.new(1, signed: false)

    flush_task =
      Task.async(fn ->
        Ferricstore.Flow.Governance.LimitCache.flush(ctx,
          now_ms: 1_002,
          before_detach_fun: fn entry ->
            if :atomics.add_get(hook_calls, 1, 1) == 1 do
              send(parent, {:limit_cache_before_detach, self(), entry})
              receive do: (:continue_limit_cache_detach -> :ok)
            end
          end,
          release_fun: fn release_ctx, release_scope, release_opts ->
            send(parent, {:limit_cache_released_ids, release_opts[:reservation_ids]})

            Ferricstore.Flow.Governance.LimitStore.release(
              release_ctx,
              release_scope,
              release_opts
            )
          end
        )
      end)

    assert_receive {:limit_cache_before_detach, flush_pid, _entry}, 1_000

    assert {:ok, %{cache: :hit, reservation_ids: [taken_id]}} =
             Ferricstore.Flow.Governance.LimitCache.spend(
               ctx,
               scope,
               Keyword.put(opts, :now_ms, 1_002)
             )

    send(flush_pid, :continue_limit_cache_detach)
    assert {:ok, %{released: 2, errors: 0}} = Task.await(flush_task, 2_000)
    assert_receive {:limit_cache_released_ids, released_ids}, 1_000
    refute taken_id in released_ids

    assert {:ok, owner} = FerricStore.flow_limit_get(scope, now_ms: 1_003)
    assert owner.leases[0].in_use == 2
  end

  test "failed flush release merges detached reservations with a concurrent refill" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    scope = unique_flow_id("cache-flush-restore-race")
    lease_limit!(scope, 4)
    ctx = FerricStore.Instance.get(:default)
    opts = [shard_id: 0, amount: 1, ttl_ms: 1_000, now_ms: 1_001]

    assert {:ok, %{reservation_ids: [_active_id]}} =
             Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope, opts)

    parent = self()

    flush_task =
      Task.async(fn ->
        Ferricstore.Flow.Governance.LimitCache.flush(ctx,
          now_ms: 1_002,
          release_fun: fn _release_ctx, _release_scope, release_opts ->
            send(parent, {:limit_cache_release_detached, self(), release_opts[:reservation_ids]})
            receive do: (:fail_limit_cache_release -> {:error, :injected_release_failure})
          end
        )
      end)

    assert_receive {:limit_cache_release_detached, release_pid, detached_ids}, 1_000
    table = :ferricstore_flow_governance_limit_cache
    key = {ctx.name, scope, 0}
    concurrent_id = "concurrent-refill"
    assert :ets.insert_new(table, {key, 1, 2_001, 1, [concurrent_id]})
    send(release_pid, :fail_limit_cache_release)

    assert {:ok, %{released: 0, errors: 1}} = Task.await(flush_task, 2_000)

    assert [{^key, 4, _expiry, 5, restored_ids, 0, 4}] = :ets.lookup(table, key)
    assert Enum.sort(restored_ids) == Enum.sort([concurrent_id | detached_ids])
  end

  test "clear refuses to erase live cached reservations without a durable flush" do
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)

    scope = unique_flow_id("cache-clear-live")
    lease_limit!(scope, 4)
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, _spent} =
             Ferricstore.Flow.Governance.LimitCache.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_001
             )

    assert {:error, :cached_reservations_present} =
             Ferricstore.Flow.Governance.LimitCache.clear()

    assert {:error, {:cached_reservation_release_failed, %{released: 0, errors: 1}}} =
             Ferricstore.Flow.Governance.LimitCache.clear(ctx,
               now_ms: 1_002,
               release_fun: fn _ctx, _scope, _opts ->
                 {:error, :injected_release_failure}
               end
             )

    assert {:error, :cached_reservations_present} =
             Ferricstore.Flow.Governance.LimitCache.clear()

    assert {:ok, %{released: 3, errors: 0}} =
             Ferricstore.Flow.Governance.LimitCache.clear(ctx, now_ms: 1_002)

    assert :ok = Ferricstore.Flow.Governance.LimitCache.clear()
  end

  test "governed retry still wakes claim waiters after releasing its reservation" do
    type = unique_flow_id("retry-waiter-limit-type")
    scope = unique_flow_id("retry-waiter-limit")
    create_due_flows(type, 1)
    lease_limit!(scope, 1)
    claimed = claim_one!(type, scope, 0, 1_001)

    keys = ClaimWaiters.wait_keys(type, "queued", 0, @partition)
    deadline_ms = System.monotonic_time(:millisecond) + 1_000
    assert :ok = ClaimWaiters.register(keys, self(), deadline_ms)
    on_exit(fn -> ClaimWaiters.unregister(keys, self()) end)

    assert :ok =
             FerricStore.flow_retry(claimed.id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002,
               retry: [
                 max_retries: 3,
                 backoff: [kind: :fixed, base_ms: 1, max_ms: 1, jitter_pct: 0]
               ]
             )

    assert_receive {:flow_claim_due_wake, _key}, 200
  end

  defp create_due_flows(type, count) do
    for index <- 1..count do
      id = unique_flow_id("governed-flow-#{index}")

      assert :ok =
               FerricStore.flow_create(id,
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      id
    end
  end

  defp lease_limit!(scope, limit) do
    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: limit,
               limit: limit,
               ttl_ms: 30_000,
               now_ms: 1_000
             )
  end

  defp claim_one!(type, scope, shard_id, now_ms) do
    assert {:ok, [claimed]} = claim(type, scope, shard_id, now_ms)
    claimed
  end

  defp claim(type, scope, shard_id, now_ms) do
    FerricStore.flow_claim_due(type,
      states: ["queued"],
      partition_key: @partition,
      worker: "worker-#{shard_id}",
      limit: 1,
      now_ms: now_ms,
      governance_limit_scope: scope,
      governance_shard_id: shard_id
    )
  end

  defp raw_record(id) do
    ctx = FerricStore.Instance.get(:default)
    value = Ferricstore.Store.Router.flow_get(ctx, id, @partition)
    Ferricstore.Flow.decode_record(value)
  end

  defp apply_terminal(:retry, claimed) do
    FerricStore.flow_retry(claimed.id, claimed.lease_token,
      partition_key: @partition,
      fencing_token: claimed.fencing_token,
      now_ms: 1_002
    )
  end

  defp apply_terminal(:fail, claimed) do
    FerricStore.flow_fail(claimed.id, claimed.lease_token,
      partition_key: @partition,
      fencing_token: claimed.fencing_token,
      now_ms: 1_002
    )
  end

  defp apply_terminal(:cancel, claimed) do
    FerricStore.flow_cancel(claimed.id,
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      now_ms: 1_002
    )
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp running_limit_scope(type) do
    digest = :sha256 |> :crypto.hash(type) |> Base.url_encode64(padding: false)
    "flow-running:" <> digest
  end

  defp terminal_many_success?(:ok), do: true

  defp terminal_many_success?({:ok, results}) when is_list(results),
    do: Enum.all?(results, &(&1 == :ok))

  defp terminal_many_success?(_result), do: false
end
