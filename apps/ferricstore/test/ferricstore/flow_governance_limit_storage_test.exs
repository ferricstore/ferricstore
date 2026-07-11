defmodule Ferricstore.FlowGovernanceLimitStorageTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.Governance.CreditLease
  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.LimitRecord
  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Flow.Governance.LimitCatalogOutbox
  alias Ferricstore.Flow.Governance.LimitReconciler
  alias Ferricstore.Flow.Governance.LimitStorageCleaner
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.Router

  test "reservation records require the current explicit status field" do
    reservation_id = "reservation-1"

    assert {:ok, :active} =
             reservation_id
             |> LimitRecord.encode_reservation(:active)
             |> LimitRecord.decode_reservation(reservation_id)

    statusless =
      :erlang.term_to_binary({:flow_governance_limit_reservation, reservation_id})

    assert {:error, "ERR flow limit reservation record is corrupt"} =
             LimitRecord.decode_reservation(statusless, reservation_id)
  end

  test "limit owner bytes stay bounded as detached reservations grow" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-storage")
    key = Keys.governance_limit_key(scope)

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1_000,
               limit: 1_001,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1_001,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    assert {:ok, %{reservation_ids: [first_id]}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 1, now_ms: 1_001)

    one_raw = Router.get(ctx, key)
    assert {:ok, one_owner} = LimitRecord.decode_owner(one_raw)
    assert one_owner.leases[0].reservations == %{}

    assert {:ok, %{reservation_ids: many_ids}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 1_000, now_ms: 1_002)

    assert length(many_ids) == 1_000
    many_raw = Router.get(ctx, key)
    assert {:ok, many_owner} = LimitRecord.decode_owner(many_raw)
    assert many_owner.leases[0].reservations == %{}
    assert byte_size(many_raw) <= byte_size(one_raw) + 64

    prefix = Keys.governance_limit_reservation_prefix(scope, 0, many_owner.leases[0].epoch)

    reservation_keys =
      ctx
      |> Router.keys()
      |> Enum.filter(&String.starts_with?(&1, prefix))

    assert length(reservation_keys) == 1_001
    assert Router.get(ctx, Keys.governance_limit_reservation_key(scope, 0, 1, first_id)) != nil
  end

  test "identical replicated spend and release commands replay idempotently" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-replay")
    key = Keys.governance_limit_key(scope)

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    spend = %{
      op: :spend,
      scope: scope,
      shard_id: 0,
      shard_count: ctx.shard_count,
      lease_epoch: 1,
      amount: 1,
      now_ms: 1_001,
      ttl_ms: nil,
      reservation_ids: ["replayed-reservation"],
      configuration: %{limit: nil, config_version: nil, policy_version: nil}
    }

    assert {:ok, %{reservation_ids: ["replayed-reservation"]}} =
             Router.flow_governance_limit_mutate(ctx, key, spend)

    Ferricstore.Test.ShardHelpers.flush_all_shards()

    assert {:ok, %{reservation_ids: ["replayed-reservation"]}} =
             Router.flow_governance_limit_mutate(ctx, key, spend)

    assert {:ok, owner} = LimitStore.get(ctx, scope, now_ms: 1_002)
    assert owner.leases[0].in_use == 1

    release = %{
      op: :release,
      scope: scope,
      shard_id: 0,
      shard_count: ctx.shard_count,
      amount: 1,
      reservation_ids: ["replayed-reservation"],
      now_ms: 1_003
    }

    assert {:ok, _owner} = Router.flow_governance_limit_mutate(ctx, key, release)
    Ferricstore.Test.ShardHelpers.flush_all_shards()
    assert {:ok, _owner} = Router.flow_governance_limit_mutate(ctx, key, release)

    assert {:ok, owner} = LimitStore.get(ctx, scope, now_ms: 1_004)
    assert owner.leases[0].in_use == 0

    Ferricstore.Test.ShardHelpers.flush_all_shards()

    assert {:error, "ERR flow limit lease generation changed"} =
             Router.flow_governance_limit_mutate(ctx, key, spend)

    assert {:ok, owner} = LimitStore.get(ctx, scope, now_ms: 1_005)
    assert owner.leases[0].in_use == 0
  end

  test "released reservation history rotates despite lease renewal" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-renewed-history")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 30_000,
               now_ms: 1_000
             )

    Enum.each(1..50, fn offset ->
      now_ms = 1_000 + offset * 2

      assert {:ok, %{reservation_ids: [reservation_id]}} =
               LimitStore.spend(ctx, scope, shard_id: 0, amount: 1, now_ms: now_ms)

      assert {:ok, _owner} =
               LimitStore.release(ctx, scope,
                 shard_id: 0,
                 reservation_ids: [reservation_id],
                 now_ms: now_ms + 1
               )

      assert {:ok, _lease} =
               LimitStore.renew(ctx, scope,
                 shard_id: 0,
                 ttl_ms: 30_000,
                 now_ms: now_ms + 1
               )
    end)

    assert {:ok, owner} = LimitStore.get(ctx, scope, now_ms: 2_000)
    assert owner.leases[0].in_use == 0

    assert {:ok, raw_owner} =
             ctx
             |> Router.get(Keys.governance_limit_key(scope))
             |> LimitRecord.decode_owner()

    assert raw_owner.leases[0].reservation_page == 0
    assert raw_owner.cleanup_tail - raw_owner.cleanup_head + 1 == 50

    Enum.each(1..50, fn _index ->
      assert {:ok, %{deleted: 1}} = LimitStore.cleanup(ctx, scope, now_ms: 2_000)
    end)

    reservation_pages =
      ctx
      |> Router.keys()
      |> Enum.filter(&String.contains?(&1, ":gov:limit-storage:page:"))
      |> Enum.filter(
        &String.contains?(&1, Base.url_encode64(:crypto.hash(:sha256, scope), padding: false))
      )

    assert reservation_pages == []
  end

  test "public spend rejects caller ids and delayed old releases cannot affect a new epoch" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-epoch")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 5,
               now_ms: 1_000
             )

    assert {:error, "ERR flow limit reservation_ids cannot be supplied for spend"} =
             LimitStore.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               now_ms: 1_001,
               reservation_ids: ["caller-controlled"]
             )

    assert {:ok, %{reservation_ids: [old_id]}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 1, now_ms: 1_001)

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 1_000,
               now_ms: 1_006
             )

    assert {:ok, %{reservation_ids: [new_id]}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 1, now_ms: 1_007)

    refute old_id == new_id

    assert {:ok, owner} =
             LimitStore.release(ctx, scope,
               shard_id: 0,
               amount: 1,
               reservation_ids: [old_id],
               now_ms: 1_008
             )

    assert owner.leases[0].in_use == 1

    assert {:ok, owner} =
             LimitStore.release(ctx, scope,
               shard_id: 0,
               amount: 1,
               reservation_ids: [new_id],
               now_ms: 1_009
             )

    assert owner.leases[0].in_use == 0
  end

  test "expired reservation cleanup is resumable and bounded to 256 entries" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-cleanup")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 300,
               limit: 300,
               ttl_ms: 5,
               now_ms: 1_000
             )

    assert {:ok, %{reservation_ids: ids}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 300, now_ms: 1_001)

    assert length(ids) == 300

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 300,
               ttl_ms: 1_000,
               now_ms: 1_006
             )

    assert {:ok, %{deleted: 256, pending?: true}} = LimitStore.cleanup(ctx, scope)
    assert {:ok, %{deleted: 44, pending?: false}} = LimitStore.cleanup(ctx, scope)
    assert {:ok, %{deleted: 0, pending?: false}} = LimitStore.cleanup(ctx, scope)
  end

  test "production cleaner drains expired pages without a hot-path scan" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-worker-cleanup")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 300,
               limit: 300,
               ttl_ms: 5,
               now_ms: 1_000
             )

    assert {:ok, %{reservation_ids: ids}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 300, now_ms: 1_001)

    assert length(ids) == 300
    cleaner = LimitStorageCleaner.process_name(ctx)
    send(cleaner, :cleanup)

    assert_eventually(
      fn ->
        owner =
          ctx
          |> Router.get(Keys.governance_limit_key(scope))
          |> LimitRecord.decode_owner()

        assert match?(
                 {:ok, %{cleanup_head: head, cleanup_tail: tail}} when head > tail,
                 owner
               ) and
                 Enum.all?(ids, fn id ->
                   Router.get(ctx, Keys.governance_limit_reservation_key(scope, 0, 1, id)) == nil
                 end)
      end,
      timeout: 3_000
    )
  end

  test "a corrupt catalog entry does not starve a later expired limit" do
    ctx = FerricStore.Instance.get(:default)

    limits =
      Enum.map(1..2, fn index ->
        scope = unique_flow_id("cleanup-fairness-#{index}")

        assert {:ok, _lease} =
                 LimitStore.lease(ctx, scope,
                   shard_id: 0,
                   amount: 1,
                   limit: 1,
                   ttl_ms: 5,
                   now_ms: 1_000
                 )

        assert {:ok, %{reservation_ids: [reservation_id]}} =
                 LimitStore.spend(ctx, scope, shard_id: 0, amount: 1, now_ms: 1_001)

        key = Keys.governance_limit_key(scope)
        {key, scope, reservation_id}
      end)

    [{bad_key, bad_scope, bad_id}, {_good_key, good_scope, good_id}] = Enum.sort(limits)
    assert {:ok, bad_owner} = ctx |> Router.get(bad_key) |> LimitRecord.decode_owner()
    bad_lease = %{bad_owner.leases[0] | reservations: %{bad_id => 1}}
    bad_owner = %{bad_owner | leases: %{0 => bad_lease}}
    bad_shard = Router.shard_for(ctx, bad_key)
    malformed = :erlang.term_to_binary({:flow_governance_limit_v1, bad_owner})
    assert :ok = WARaftBackend.write(bad_shard, {:put, bad_key, malformed, 0})
    assert {:ok, true} = Catalog.member?(ctx, Keys.governance_catalog_key(:limit), bad_key)

    assert {:error, _reason} = LimitStorageCleaner.run_once(ctx, now_ms: 1_006)
    assert {:ok, %{deleted: 1}} = LimitStorageCleaner.run_once(ctx, now_ms: 1_006)

    assert Router.get(ctx, Keys.governance_limit_reservation_key(good_scope, 0, 1, good_id)) ==
             nil

    assert Router.get(ctx, Keys.governance_limit_reservation_key(bad_scope, 0, 1, bad_id)) != nil
  end

  test "cleaner tick enforces its Raft page-command budget" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cleanup-command-budget")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 600,
               limit: 600,
               ttl_ms: 5,
               now_ms: 1_000
             )

    assert {:ok, %{reservation_ids: ids}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 600, now_ms: 1_001)

    assert length(ids) == 600

    assert %{commands: 2, deleted: 512, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 2)

    assert %{commands: 1, deleted: 88, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 2)
  end

  test "shard, version, and time bounds keep owner and command terms finite" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-bounds")
    max_exact = 9_007_199_254_740_991

    assert {:error, shard_error} =
             LimitStore.lease(ctx, scope,
               shard_id: ctx.shard_count,
               amount: 1,
               limit: 1,
               ttl_ms: 1,
               now_ms: 0
             )

    assert shard_error =~ "shard_id must be between 0 and"

    assert {:error, _reason} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: max_exact + 1,
               ttl_ms: 1,
               now_ms: 0
             )

    assert {:error, _reason} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               config_version: max_exact + 1,
               ttl_ms: 1,
               now_ms: 0
             )

    assert {:error, _reason} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               policy_version: max_exact + 1,
               ttl_ms: 1,
               now_ms: 0
             )

    assert {:error, "ERR flow limit lease deadline exceeds the supported integer range"} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               ttl_ms: 1,
               now_ms: max_exact
             )
  end

  test "all valid shards are bounded and large policy versions are fingerprinted" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-shard-bound")
    policy_version = String.duplicate("policy-version-", 1_000)

    Enum.each(0..(ctx.shard_count - 1), fn shard_id ->
      assert {:ok, _lease} =
               LimitStore.lease(ctx, scope,
                 shard_id: shard_id,
                 amount: 1,
                 limit: ctx.shard_count,
                 config_version: 1,
                 policy_version: policy_version,
                 ttl_ms: 1_000,
                 now_ms: 1_000
               )
    end)

    raw = Router.get(ctx, Keys.governance_limit_key(scope))
    assert {:ok, owner} = LimitRecord.decode_owner(raw)
    assert map_size(owner.leases) == ctx.shard_count
    assert owner.policy_version == CreditLease.policy_version_fingerprint(policy_version)
    assert byte_size(raw) < 4_096

    assert {:ok, public} = LimitStore.get(ctx, scope, now_ms: 1_001)
    refute Map.has_key?(public, :policy_version)
    refute Map.has_key?(public, :cleanup_head)
    refute Map.has_key?(public, :cleanup_tail)
    refute Map.has_key?(public.leases[0], :reservations)
    refute Map.has_key?(public.leases[0], :reservation_page)
    refute Map.has_key?(public.leases[0], :reservation_page_fill)
  end

  test "internal preallocated spends validate ids and replay exactly" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-preallocated")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 2,
               limit: 2,
               ttl_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, ids} = LimitStore.generate_reservation_ids(ctx, scope, 0, 2)
    assert Enum.all?(ids, &String.starts_with?(&1, "flr1:1:"))
    opts = [shard_id: 0, amount: 2, now_ms: 1_001]
    assert {:ok, %{reservation_ids: ^ids}} = LimitStore.spend_reserved(ctx, scope, opts, ids)
    assert {:ok, %{reservation_ids: ^ids}} = LimitStore.spend_reserved(ctx, scope, opts, ids)

    assert {:error, _reason} =
             LimitStore.spend_reserved(ctx, scope, opts, [List.first(ids), List.first(ids)])

    assert {:ok, owner} = LimitStore.get(ctx, scope, now_ms: 1_002)
    assert owner.leases[0].in_use == 2

    assert {:ok, owner} =
             LimitStore.release(ctx, scope,
               shard_id: 0,
               reservation_ids: ids,
               now_ms: 1_003
             )

    assert owner.leases[0].epoch == 2
    assert owner.leases[0].in_use == 0

    assert {:ok, %{deleted: 2, pending?: false}} =
             LimitStore.cleanup(ctx, scope, now_ms: 1_004)

    assert {:error, "ERR flow limit lease generation changed"} =
             LimitStore.spend_reserved(ctx, scope, opts, ids)

    assert {:ok, owner} = LimitStore.get(ctx, scope, now_ms: 1_004)
    assert owner.leases[0].in_use == 0
  end

  test "reservation history cap rejects before detached writes" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-history-cap")
    key = Keys.governance_limit_key(scope)
    shard = Router.shard_for(ctx, key)

    lease = %CreditLease.Lease{
      shard_id: 0,
      epoch: 1,
      expires_at_ms: 2_000,
      available: 1,
      in_use: 1,
      reservation_page: 256,
      reservation_page_fill: 256
    }

    owner = %CreditLease.Owner{
      scope: scope,
      limit: 2,
      free: 0,
      epoch: 1,
      leases: %{0 => lease}
    }

    assert {:ok, raw} = LimitRecord.encode_owner(owner)
    assert :ok = WARaftBackend.write(shard, {:put, key, raw, 0})

    assert {:error, "ERR flow limit reservation history is full"} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 1, now_ms: 1_001)

    assert Router.get(ctx, key) == raw
  end

  test "unknown public spend outcomes preserve exact ids for compensation" do
    ids = ["generated:1", "generated:2"]

    assert {:error,
            %{
              code: "FLOW_LIMIT_SPEND_UNKNOWN_OUTCOME",
              scope: "scope",
              shard_id: 2,
              reservation_ids: ^ids
            }} =
             LimitStore.normalize_spend_result(
               {:error, {:timeout, :unknown_outcome}},
               "scope",
               2,
               ids
             )

    assert {:error, %{reservation_ids: ^ids}} =
             LimitCache.normalize_reserved_spend_result(
               {:error, {:timeout, :unknown_outcome}},
               "scope",
               [shard_id: 2],
               ids
             )
  end

  test "binary policy changes require an explicit monotonic config version" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-policy-version")

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               config_version: 1,
               policy_version: "policy-a",
               ttl_ms: 1_000,
               now_ms: 1_000
             )

    assert {:error, "ERR flow limit config_version is required with binary policy_version"} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 1,
               limit: 1,
               policy_version: "policy-b",
               ttl_ms: 1_000,
               now_ms: 1_001
             )
  end

  test "current owner records reject embedded reservations as corruption" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("embedded-limit-record")
    key = Keys.governance_limit_key(scope)
    shard = Router.shard_for(ctx, key)

    lease = %CreditLease.Lease{
      shard_id: 0,
      epoch: 1,
      expires_at_ms: 2_000,
      in_use: 1,
      reservations: %{"embedded-id" => 1}
    }

    owner = %CreditLease.Owner{
      scope: scope,
      limit: 1,
      free: 0,
      epoch: 1,
      leases: %{0 => lease}
    }

    invalid_value = :erlang.term_to_binary({:flow_governance_limit_v1, owner})
    assert :ok = WARaftBackend.write(shard, {:put, key, invalid_value, 0})

    assert {:error, "ERR flow limit record is corrupt"} =
             LimitStore.get(ctx, scope, now_ms: 1_001)
  end

  test "an uncataloged current owner is repaired before it creates detached storage" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-catalog-repair")
    key = Keys.governance_limit_key(scope)
    shard = Router.shard_for(ctx, key)
    catalog_key = Keys.governance_catalog_key(:limit)

    lease = %CreditLease.Lease{
      shard_id: 0,
      epoch: 1,
      expires_at_ms: 1_005,
      available: 1
    }

    owner = %CreditLease.Owner{
      scope: scope,
      limit: 1,
      free: 0,
      epoch: 1,
      leases: %{0 => lease}
    }

    assert {:ok, value} = LimitRecord.encode_owner(owner)
    assert :ok = WARaftBackend.write(shard, {:put, key, value, 0})
    assert {:ok, _removed} = FerricStore.Impl.zrem(ctx, catalog_key, [key])
    assert {:ok, false} = Catalog.member?(ctx, catalog_key, key)

    assert {:ok, %{reservation_ids: [reservation_id]}} =
             LimitStore.spend(ctx, scope, shard_id: 0, amount: 1, now_ms: 1_001)

    assert {:ok, true} = Catalog.member?(ctx, catalog_key, key)

    assert {:ok, limits} = LimitStore.list(ctx, limit: 1_000, now_ms: 1_002)
    assert Enum.any?(limits, &(&1.scope == scope))

    assert %{errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_006, page_budget: 64)

    assert_eventually(fn ->
      Router.get(ctx, Keys.governance_limit_reservation_key(scope, 0, 1, reservation_id)) ==
        nil
    end)
  end

  test "missing spends do not register ghosts and the cleaner prunes stale members" do
    ctx = FerricStore.Instance.get(:default)
    missing_scope = unique_flow_id("detached-limit-missing-spend")
    missing_key = Keys.governance_limit_key(missing_scope)
    stale_scope = unique_flow_id("detached-limit-stale-catalog")
    stale_key = Keys.governance_limit_key(stale_scope)
    catalog_key = Keys.governance_catalog_key(:limit)

    assert {:error, "ERR flow limit not found"} =
             LimitStore.spend(ctx, missing_scope, shard_id: 0, amount: 1, now_ms: 1_000)

    assert {:ok, false} = Catalog.member?(ctx, catalog_key, missing_key)

    assert :ok = Catalog.register(ctx, :limit, stale_key)
    assert {:ok, true} = Catalog.member?(ctx, catalog_key, stale_key)

    assert %{commands: 1, errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_001, page_budget: 1)

    assert {:ok, false} = Catalog.member?(ctx, catalog_key, stale_key)
  end

  test "new owners commit before catalog publication so pruning cannot race lease creation" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-catalog-barrier")
    key = Keys.governance_limit_key(scope)
    catalog_key = Keys.governance_catalog_key(:limit)
    parent = self()

    task =
      Task.async(fn ->
        LimitStore.lease(ctx, scope,
          shard_id: 0,
          amount: 1,
          limit: 1,
          ttl_ms: 1_000,
          now_ms: 1_000,
          after_catalog_registration_fun: fn ^key ->
            send(parent, :catalog_registered)

            receive do
              :continue -> :ok
            end
          end
        )
      end)

    assert_receive :catalog_registered, 3_000
    assert is_binary(Router.get(ctx, key))
    assert {:ok, true} = Catalog.member?(ctx, catalog_key, key)

    assert %{errors: 0} =
             LimitStorageCleaner.run_tick(ctx, now_ms: 1_001, page_budget: 2)

    assert {:ok, true} = Catalog.member?(ctx, catalog_key, key)
    send(task.pid, :continue)
    assert {:ok, _lease} = Task.await(task, 3_000)
  end

  test "durable owner publication repairs a catalog member after caller-side loss" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("detached-limit-durable-catalog")
    key = Keys.governance_limit_key(scope)
    catalog_key = Keys.governance_catalog_key(:limit)
    reconciler = Process.whereis(LimitReconciler)
    assert is_pid(reconciler)
    :ok = :sys.suspend(reconciler)

    try do
      assert {:ok, _lease} =
               LimitStore.lease(ctx, scope,
                 shard_id: 0,
                 amount: 1,
                 limit: 2,
                 ttl_ms: 1_000,
                 now_ms: 1_000
               )

      shard_index = Router.shard_for(ctx, key)

      assert {:ok, %{entries: [{_sequence, ^key}]}} =
               LimitCatalogOutbox.read_page(ctx, shard_index, 256)

      assert {:ok, 1} = FerricStore.Impl.zrem(ctx, catalog_key, [key])
      assert {:ok, false} = Catalog.member?(ctx, catalog_key, key)

      assert {:ok, %{errors: 0}} =
               LimitReconciler.run_once(ctx,
                 now_ms: 1_001,
                 reservation_limit: 256,
                 catalog_shard: shard_index
               )

      assert {:ok, true} = Catalog.member?(ctx, catalog_key, key)

      assert {:ok, %{entries: []}} =
               LimitCatalogOutbox.read_page(ctx, shard_index, 256)

      assert {:ok, _lease} =
               LimitStore.lease(ctx, scope,
                 shard_id: 0,
                 amount: 1,
                 limit: 2,
                 ttl_ms: 1_000,
                 now_ms: 1_002
               )

      assert {:ok, %{entries: []}} =
               LimitCatalogOutbox.read_page(ctx, shard_index, 256)
    after
      if Process.alive?(reconciler), do: :sys.resume(reconciler)
    end
  end

  test "catalog publication batches owner reads and the idempotent catalog write" do
    ctx = FerricStore.Instance.get(:default)
    catalog_key = Keys.governance_catalog_key(:limit)
    assert ctx.shard_count > 1
    target_shard = 1
    reconciler = Process.whereis(Ferricstore.Flow.Governance.LimitReconciler)
    :ok = :sys.suspend(reconciler)

    on_exit(fn ->
      if Process.alive?(reconciler), do: :sys.resume(reconciler)
    end)

    owner_keys =
      Enum.map(1..3, fn index ->
        scope = governance_scope_for_shard(ctx, "batched-limit-catalog-#{index}", target_shard)
        owner_key = Keys.governance_limit_key(scope)

        assert {:ok, _lease} =
                 LimitStore.lease(ctx, scope,
                   shard_id: 0,
                   amount: 1,
                   limit: 1,
                   ttl_ms: 1_000,
                   now_ms: 1_000
                 )

        owner_key
      end)

    assert {:ok, 3} = FerricStore.Impl.zrem(ctx, catalog_key, owner_keys)

    assert {:ok, %{entries: entries}} =
             LimitCatalogOutbox.read_page(ctx, target_shard, 256)

    assert length(entries) == 3

    assert {:ok,
            %{
              catalog_published: 0,
              catalog_shards_scanned: 1,
              next_catalog_shard: ^target_shard
            }} =
             Ferricstore.Flow.Governance.LimitReconciler.run_once(ctx,
               now_ms: 1_001,
               reservation_limit: 256,
               catalog_shard: 0
             )

    Enum.each(owner_keys, fn owner_key ->
      assert {:ok, false} = Catalog.member?(ctx, catalog_key, owner_key)
    end)

    assert {:ok,
            %{
              errors: 0,
              catalog_published: 3,
              catalog_read_batches: 2,
              catalog_write_batches: 1,
              catalog_shards_scanned: 1
            }} =
             Ferricstore.Flow.Governance.LimitReconciler.run_once(ctx,
               now_ms: 1_001,
               reservation_limit: 256,
               catalog_shard: target_shard
             )

    Enum.each(owner_keys, fn owner_key ->
      assert {:ok, true} = Catalog.member?(ctx, catalog_key, owner_key)
    end)
  end

  test "catalog publication discards a torn intent whose owner never committed" do
    ctx = FerricStore.Instance.get(:default)
    shard_index = 0
    owner_key = Keys.governance_limit_key(unique_flow_id("missing-catalog-owner"))
    meta_key = Keys.governance_limit_catalog_outbox_meta_key(shard_index)
    intent_key = Keys.governance_limit_catalog_outbox_intent_key(shard_index, 1)

    assert {:ok, meta, 1} = LimitCatalogOutbox.append(LimitCatalogOutbox.empty_meta(), owner_key)

    assert [:ok, :ok] =
             WARaftBackend.write_many([
               {shard_index, {:put, meta_key, LimitCatalogOutbox.encode_meta(meta), 0}},
               {shard_index, {:put, intent_key, LimitCatalogOutbox.encode_intent(owner_key), 0}}
             ])

    assert {:ok, _result} =
             Ferricstore.Flow.Governance.LimitReconciler.run_once(ctx,
               now_ms: 1_001,
               reservation_limit: 256
             )

    assert {:ok, %{entries: []}} =
             LimitCatalogOutbox.read_page(ctx, shard_index, 256)
  end

  test "catalog publication acknowledges only the valid prefix before a corrupt owner" do
    ctx = FerricStore.Instance.get(:default)
    catalog_key = Keys.governance_catalog_key(:limit)
    shard_index = 0
    reconciler = Process.whereis(Ferricstore.Flow.Governance.LimitReconciler)
    :ok = :sys.suspend(reconciler)

    on_exit(fn ->
      if Process.alive?(reconciler), do: :sys.resume(reconciler)
    end)

    owner_keys =
      Enum.map(1..3, fn index ->
        scope = governance_scope_for_shard(ctx, "catalog-prefix-#{index}", shard_index)
        owner_key = Keys.governance_limit_key(scope)

        assert {:ok, _lease} =
                 LimitStore.lease(ctx, scope,
                   shard_id: 0,
                   amount: 1,
                   limit: 1,
                   ttl_ms: 1_000,
                   now_ms: 1_000
                 )

        owner_key
      end)

    [first_key, corrupt_key, last_key] = owner_keys
    assert {:ok, 3} = FerricStore.Impl.zrem(ctx, catalog_key, owner_keys)
    assert :ok = WARaftBackend.write(shard_index, {:put, corrupt_key, "corrupt", 0})

    assert {:ok, %{catalog_published: 1, catalog_errors: 1}} =
             Ferricstore.Flow.Governance.LimitReconciler.run_once(ctx,
               now_ms: 1_001,
               reservation_limit: 256,
               catalog_shard: shard_index
             )

    assert {:ok, true} = Catalog.member?(ctx, catalog_key, first_key)
    assert {:ok, false} = Catalog.member?(ctx, catalog_key, corrupt_key)
    assert {:ok, false} = Catalog.member?(ctx, catalog_key, last_key)

    assert {:ok,
            %{
              entries: [
                {_corrupt_sequence, ^corrupt_key},
                {_last_sequence, ^last_key}
              ]
            }} =
             LimitCatalogOutbox.read_page(ctx, shard_index, 256)

    assert :ok = WARaftBackend.write(shard_index, {:delete, corrupt_key})

    assert {:ok, %{catalog_published: 2, catalog_errors: 0}} =
             Ferricstore.Flow.Governance.LimitReconciler.run_once(ctx,
               now_ms: 1_002,
               reservation_limit: 256,
               catalog_shard: shard_index
             )

    assert {:ok, true} = Catalog.member?(ctx, catalog_key, last_key)
    assert {:ok, %{entries: []}} = LimitCatalogOutbox.read_page(ctx, shard_index, 256)
  end

  test "catalog publication backlog is strictly bounded" do
    full = %{head: 1, tail: 65_536}

    assert {:error, "ERR flow limit catalog publication backlog is full"} =
             LimitCatalogOutbox.append(full, "owner")

    assert {:ok, %{head: 2, tail: 65_537}, 65_537} =
             LimitCatalogOutbox.append(%{head: 2, tail: 65_536}, "owner")
  end

  test "removed metadata binding and reservation scan APIs stay unavailable" do
    refute function_exported?(LimitStore, :bind, 3)
    refute function_exported?(LimitStore, :bind_many, 4)
    refute function_exported?(LimitStore, :reservation_page, 4)
    refute function_exported?(LimitStore, :force_release_amount, 3)
    refute function_exported?(CreditLease, :bind, 4)
    refute function_exported?(CreditLease, :reservations, 1)
  end

  defp governance_scope_for_shard(ctx, prefix, shard_index) do
    Stream.repeatedly(fn -> unique_flow_id(prefix) end)
    |> Enum.find(fn scope ->
      Router.shard_for(ctx, Keys.governance_limit_key(scope)) == shard_index
    end)
  end
end
