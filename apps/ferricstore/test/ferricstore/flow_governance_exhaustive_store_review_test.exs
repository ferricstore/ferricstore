defmodule Ferricstore.FlowGovernanceExhaustiveStoreReviewTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.Governance.ApprovalStore
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.CacheSessionStore
  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.CircuitStore
  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Flow.Governance.LimitReconciler
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Governance.Ledger
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @partition "governance-exhaustive-review"

  test "limit-cache flush releases each bounded batch before detaching the next one" do
    instance_name = unique_flow_id("cache-streaming-flush")
    ctx = %{name: instance_name}
    table = :ferricstore_flow_governance_limit_cache
    detached_count = :atomics.new(1, signed: false)
    first_release_at = :atomics.new(1, signed: false)

    for index <- 1..129 do
      scope = "#{instance_name}:#{index}"
      reservation_id = "reservation-#{index}"

      assert :ets.insert_new(
               table,
               {{instance_name, scope, 0}, 1, 10_000, 1, [reservation_id], 0, 129,
                :flow_governance_limit_cache_entry}
             )
    end

    assert {:ok, %{released: 129, errors: 0}} =
             LimitCache.flush(ctx,
               now_ms: 1_000,
               before_detach_fun: fn _entry ->
                 :atomics.add(detached_count, 1, 1)
               end,
               release_fun: fn _ctx, _scope, _opts ->
                 detached = :atomics.get(detached_count, 1)
                 :atomics.compare_exchange(first_release_at, 1, 0, detached)
                 {:ok, %{}}
               end
             )

    assert :atomics.get(first_release_at, 1) <= 128
  end

  test "atomic governance records reject oversized encoded values before registration" do
    ctx = FerricStore.Instance.get(:default)
    key = "atomic-record:" <> unique_flow_id("byte-budget")

    assert {:error, "ERR governance record exceeds 900000-byte durable limit"} =
             AtomicRecord.mutate(
               ctx,
               key,
               fn _value -> {:ok, %{}} end,
               fn _record -> String.duplicate("v", 900_001) end,
               fn -> {:ok, %{}} end,
               fn record -> {:ok, record} end
             )

    assert is_nil(Router.get(ctx, key))
  end

  test "approval requests reject oversized aggregate records before catalog registration" do
    ctx = FerricStore.Instance.get(:default)
    id = unique_flow_id("approval-byte-budget")
    assignees = Enum.map(1..16, &(String.duplicate("a", 60_000) <> Integer.to_string(&1)))
    record_key = Keys.governance_approval_key(id)

    assert {:error, "ERR flow approval record exceeds 900000-byte durable limit"} =
             ApprovalStore.request(ctx, id,
               flow_id: "flow",
               scope: "scope",
               assignees: assignees,
               now_ms: 100
             )

    assert {:ok, nil} = ApprovalStore.get(ctx, id)

    assert {:ok, false} =
             Catalog.member?(ctx, Keys.governance_catalog_key(:approval), record_key)
  end

  test "expired pending cache pages use the configured exact-release contract" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-expired-release-contract")
    old_enabled = Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled)
    old_multiplier = Application.get_env(:ferricstore, :flow_governance_limit_cache_multiplier)
    old_max_chunk = Application.get_env(:ferricstore, :flow_governance_limit_cache_max_chunk)
    old_page_size = Application.get_env(:ferricstore, :flow_governance_cache_session_page_size)

    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 4)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 4)
    Application.put_env(:ferricstore, :flow_governance_cache_session_page_size, 1)

    on_exit(fn ->
      _ = LimitCache.clear(ctx, now_ms: 3_000)
      restore_env(:flow_governance_limit_cache_enabled, old_enabled)
      restore_env(:flow_governance_limit_cache_multiplier, old_multiplier)
      restore_env(:flow_governance_limit_cache_max_chunk, old_max_chunk)
      restore_env(:flow_governance_cache_session_page_size, old_page_size)
    end)

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 4,
               limit: 4,
               ttl_ms: 100,
               now_ms: 1_000
             )

    spend_opts = [shard_id: 0, amount: 1, ttl_ms: 100]
    assert {:ok, _first} = LimitCache.spend(ctx, scope, spend_opts ++ [now_ms: 1_001])
    assert {:ok, %{cache: :hit}} = LimitCache.spend(ctx, scope, spend_opts ++ [now_ms: 1_002])

    parent = self()

    release_fun = fn release_ctx, release_scope, opts ->
      send(parent, {:expired_page_released, opts[:reservation_ids]})
      LimitStore.release(release_ctx, release_scope, opts)
    end

    _result =
      LimitCache.spend(ctx, scope,
        shard_id: 0,
        amount: 1,
        ttl_ms: 100,
        now_ms: 1_102,
        cache_release_fun: release_fun
      )

    assert_receive {:expired_page_released, [_first_pending_id]}
    assert_receive {:expired_page_released, [_second_pending_id]}
  end

  test "one cache spend bounds expired pending-page cleanup work" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("cache-expired-cleanup-bound")
    old_enabled = Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled)
    old_multiplier = Application.get_env(:ferricstore, :flow_governance_limit_cache_multiplier)
    old_max_chunk = Application.get_env(:ferricstore, :flow_governance_limit_cache_max_chunk)
    old_page_size = Application.get_env(:ferricstore, :flow_governance_cache_session_page_size)

    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, true)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_multiplier, 19)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_max_chunk, 19)
    Application.put_env(:ferricstore, :flow_governance_cache_session_page_size, 1)

    on_exit(fn ->
      _ = LimitCache.clear(ctx, now_ms: 3_000)
      restore_env(:flow_governance_limit_cache_enabled, old_enabled)
      restore_env(:flow_governance_limit_cache_multiplier, old_multiplier)
      restore_env(:flow_governance_limit_cache_max_chunk, old_max_chunk)
      restore_env(:flow_governance_cache_session_page_size, old_page_size)
    end)

    assert {:ok, _lease} =
             LimitStore.lease(ctx, scope,
               shard_id: 0,
               amount: 19,
               limit: 19,
               ttl_ms: 100,
               now_ms: 1_000
             )

    spend_opts = [shard_id: 0, amount: 1, ttl_ms: 100]
    assert {:ok, _first} = LimitCache.spend(ctx, scope, spend_opts ++ [now_ms: 1_001])
    assert {:ok, %{cache: :hit}} = LimitCache.spend(ctx, scope, spend_opts ++ [now_ms: 1_002])

    releases = :atomics.new(1, signed: false)

    _result =
      LimitCache.spend(ctx, scope,
        shard_id: 0,
        amount: 1,
        ttl_ms: 100,
        now_ms: 1_102,
        cache_release_fun: fn _ctx, _scope, _opts ->
          :atomics.add(releases, 1, 1)
          {:ok, %{}}
        end
      )

    assert :atomics.get(releases, 1) == 16
  end

  test "cache recovery derives progress from durable metadata instead of trusting a forged cursor" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-review-node")
    instance_name = unique_flow_id("cache-review-instance")

    assert {:ok, first} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    assert {:ok, [_first_page, _second_page]} =
             CacheSessionStore.persist_prefetch(
               ctx,
               first,
               "scope",
               0,
               ["reservation-1", "reservation-2"],
               page_size: 1,
               expires_at_ms: 10_000,
               config_version: 1
             )

    assert {:ok, second} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    parent = self()

    release_fun = fn _ctx, _scope, opts ->
      send(parent, {:released, opts[:reservation_ids]})
      {:ok, %{}}
    end

    forged_cursor = %{session_id: first.session_id, sequence: 3}

    assert {:ok, %{processed: 1, released: 1, errors: 0, next_cursor: next_cursor}} =
             CacheSessionStore.recover(ctx, second,
               cursor: forged_cursor,
               limit: 1,
               now_ms: 1_000,
               release_fun: release_fun
             )

    assert next_cursor == %{session_id: first.session_id, sequence: 2}
    assert_receive {:released, ["reservation-1"]}
    refute_receive {:released, ["reservation-2"]}
  end

  test "circuit mutations preserve configured values when options omit them" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("circuit-review")

    assert {:ok, configured} =
             CircuitStore.record_failure(ctx, scope,
               now_ms: 100,
               failure_threshold: 9,
               open_ms: 4_321
             )

    assert configured.failure_threshold == 9
    assert configured.open_ms == 4_321

    assert {:ok, opened} = CircuitStore.open(ctx, scope, now_ms: 101)
    assert opened.failure_threshold == 9
    assert opened.open_ms == 4_321
    assert opened.failure_count == 1

    assert {:ok, recorded} = CircuitStore.record_failure(ctx, scope, now_ms: 102)
    assert recorded.failure_threshold == 9
    assert recorded.open_ms == 4_321
  end

  test "circuit mutations reject oversized error-class collections before storage" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("circuit-error-class-bound")
    error_classes = Enum.map(1..1_001, &"Error#{&1}")

    assert {:error, "ERR flow circuit error_classes must contain at most 1000 values"} =
             CircuitStore.record_failure(ctx, scope,
               now_ms: 100,
               error_classes: error_classes
             )

    assert {:ok, nil} = CircuitStore.get(ctx, scope)

    assert {:error,
            "ERR flow circuit error_classes must contain non-empty strings of at most 256 bytes"} =
             CircuitStore.record_failure(ctx, scope,
               now_ms: 100,
               error_classes: [String.duplicate("e", 257)]
             )

    assert {:ok, nil} = CircuitStore.get(ctx, scope)
  end

  test "circuit mutations fingerprint oversized observed error classes" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("circuit-error-class-width")
    oversized = String.duplicate("e", 257)

    assert {:ok, circuit} =
             CircuitStore.record_failure(ctx, scope,
               now_ms: 100,
               failure_threshold: 1,
               open_ms: 1_000,
               error_class: oversized
             )

    assert circuit.status == :open

    assert %{error_class: "sha256:" <> digest} =
             Enum.find(circuit.events, &(&1.kind == :failure))

    assert byte_size(digest) == 43
  end

  test "effect reservation retries consult the existing decision before changed policy" do
    type = unique_flow_id("effect-idempotency-type")
    id = unique_flow_id("effect-idempotency-flow")

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{effects: %{allowed: ["email.send"]}}
             )

    claimed = create_and_claim!(id, type)

    opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      operation_digest: "digest-1",
      idempotency_key: "idempotency-1",
      now_ms: 1_002
    ]

    assert {:ok, reserved} =
             FerricStore.flow_effect_reserve(id, "send", "email.send", opts)

    refute Map.has_key?(reserved, :circuit_failure_threshold)
    refute Map.has_key?(reserved, :circuit_open_ms)

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{effects: %{denied: ["email.send"]}}
             )

    assert {:ok, replayed} =
             FerricStore.flow_effect_reserve(id, "send", "email.send", opts)

    assert replayed.decision == :already_reserved
    assert replayed.operation_digest == "digest-1"
  end

  test "effect decoding rejects aggregate records above the durable byte budget" do
    type = unique_flow_id("effect-byte-budget-type")
    id = unique_flow_id("effect-byte-budget-flow")
    claimed = create_and_claim!(id, type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "send", "email.send",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "digest",
               idempotency_key: "idempotency",
               now_ms: 1_002
             )

    ctx = FerricStore.Instance.get(:default)
    key = Keys.governance_effect_key(id, "send", @partition)
    encoded = Router.get(ctx, key)
    assert {:ok, {:flow_governance_effect_v1, effect}} = TermCodec.decode(encoded)
    field = String.duplicate("f", 230_000)

    oversized =
      effect
      |> Map.put(:idempotency_key, field)
      |> Map.put(:policy_hash, field)
      |> Map.put(:external_id, field)
      |> Map.put(:error, field)

    assert :ok =
             Router.put(ctx, key, TermCodec.encode({:flow_governance_effect_v1, oversized}), 0)

    assert {:error, "ERR flow governance effect record is corrupt"} =
             FerricStore.flow_effect_get(id, "send", partition_key: @partition)
  end

  test "an effect reservation that loses the NX race releases its half-open probe" do
    type = unique_flow_id("effect-probe-race-type")
    id = unique_flow_id("effect-probe-race-flow")
    effect_type = "email.send"
    circuit_scope = "effect:" <> effect_type

    circuit_rule = %{
      failure_threshold: 1,
      open_ms: 1,
      half_open_max_probes: 2,
      half_open_success_threshold: 2
    }

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{circuits: %{circuit_scope => circuit_rule}}
             )

    claimed = create_and_claim!(id, type)
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, %{status: :open}} =
             CircuitStore.open(
               ctx,
               circuit_scope,
               Map.to_list(circuit_rule) ++ [now_ms: 1_000]
             )

    parent = self()

    reserve = fn ->
      Task.async(fn ->
        Process.put(:ferricstore_governance_effect_before_set_hook, fn _effect ->
          send(parent, {:effect_ready_to_set, self()})

          receive do
            :continue_effect_set -> :ok
          end
        end)

        FerricStore.flow_effect_reserve(id, "send", effect_type,
          partition_key: @partition,
          lease_token: claimed.lease_token,
          fencing_token: claimed.fencing_token,
          operation_digest: "probe-race-digest",
          idempotency_key: "probe-race-idempotency",
          now_ms: 1_002
        )
      end)
    end

    first = reserve.()
    second = reserve.()

    assert_receive {:effect_ready_to_set, first_pid}, 2_000
    assert_receive {:effect_ready_to_set, second_pid}, 2_000
    send(first_pid, :continue_effect_set)
    send(second_pid, :continue_effect_set)

    results = [Task.await(first, 2_000), Task.await(second, 2_000)]
    assert Enum.all?(results, &match?({:ok, _effect}, &1))

    decisions = results |> Enum.map(fn {:ok, effect} -> effect.decision end) |> MapSet.new()
    assert decisions == MapSet.new([:reserved, :already_reserved])

    assert {:ok, circuit} = CircuitStore.get(ctx, circuit_scope)
    assert circuit.status == :half_open
    assert circuit.half_open_in_flight == 1
  end

  test "invalid terminal effect metadata is rejected without changing status" do
    type = unique_flow_id("effect-terminal-type")
    id = unique_flow_id("effect-terminal-flow")
    claimed = create_and_claim!(id, type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "charge", "stripe.charge",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "digest-2",
               idempotency_key: "idempotency-2",
               now_ms: 1_002
             )

    assert {:error, _reason} =
             FerricStore.flow_effect_confirm(id, "charge",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               external_id: 42,
               now_ms: 1_003
             )

    assert {:ok, effect} =
             FerricStore.flow_effect_get(id, "charge", partition_key: @partition)

    assert effect.status == :reserved
    refute Map.has_key?(effect, :external_id)
  end

  test "corrupt circuit state returns an error instead of crashing effect reservation" do
    type = unique_flow_id("effect-corrupt-circuit-type")
    id = unique_flow_id("effect-corrupt-circuit-flow")
    scope = "effect:email.send"

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{circuits: %{scope => %{failure_threshold: 1, open_ms: 1_000}}}
             )

    claimed = create_and_claim!(id, type)
    ctx = FerricStore.Instance.get(:default)
    assert :ok = Router.put(ctx, Keys.governance_circuit_key(scope), <<0, 1, 2>>, 0)

    assert {:error, "ERR flow circuit record is corrupt"} =
             FerricStore.flow_effect_reserve(id, "send", "email.send",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "digest-3",
               idempotency_key: "idempotency-3",
               now_ms: 1_002
             )
  end

  test "repeated terminal effect updates do not duplicate ledger events" do
    type = unique_flow_id("effect-ledger-retry-type")
    id = unique_flow_id("effect-ledger-retry-flow")
    claimed = create_and_claim!(id, type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "send", "email.send",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "digest-4",
               idempotency_key: "idempotency-4",
               now_ms: 1_002
             )

    terminal_opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      external_id: "external-1",
      now_ms: 1_003
    ]

    assert {:ok, first} = FerricStore.flow_effect_confirm(id, "send", terminal_opts)
    assert first.decision == :confirmed

    assert {:ok, replayed} = FerricStore.flow_effect_confirm(id, "send", terminal_opts)
    assert replayed.decision == :already_applied

    assert {:ok, events} =
             FerricStore.flow_governance_ledger(id, partition_key: @partition)

    assert Enum.map(events, & &1.kind) == [:effect_reserved, :effect_confirmed]
  end

  test "ledger append uses the CAS index as its only authoritative record" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("ledger-authority-type")
    id = unique_flow_id("ledger-authority-flow")
    _claimed = create_and_claim!(id, type)
    record = ctx |> Router.get(Keys.state_key(id, @partition)) |> Ferricstore.Flow.decode_record()
    event_id = "effect:send:reserved"

    fields = %{
      event_id: event_id,
      effect_key: "send",
      effect_type: "email.send",
      status: :reserved
    }

    assert {:ok, :ok} = Ledger.append(ctx, record, :effect_reserved, fields, 1_002)
    assert {:ok, :ok} = Ledger.append(ctx, record, :effect_reserved, fields, 1_002)

    assert {:ok, [event]} = Ledger.list(ctx, id, partition_key: @partition)
    assert event.id == event_id

    event_key = Keys.governance_ledger_key(id, event_id, @partition)
    assert is_nil(Router.get(ctx, event_key))
  end

  test "effect reservation is not acknowledged until its ledger event is durable" do
    type = unique_flow_id("effect-reserve-ledger-recovery-type")
    id = unique_flow_id("effect-reserve-ledger-recovery-flow")
    claimed = create_and_claim!(id, type)

    opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      operation_digest: "reserve-ledger-recovery-digest",
      idempotency_key: "reserve-ledger-recovery-idempotency",
      now_ms: 1_002
    ]

    Process.put(:ferricstore_governance_ledger_before_append_hook, fn event ->
      if event.kind == :effect_reserved,
        do: {:error, "ERR injected governance ledger failure"},
        else: :ok
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_governance_ledger_before_append_hook)
    end)

    assert {:error, "ERR injected governance ledger failure"} =
             FerricStore.flow_effect_reserve(id, "send", "email.send", opts)

    assert {:ok, persisted} =
             FerricStore.flow_effect_get(id, "send", partition_key: @partition)

    assert persisted.status == :reserved
    assert {:ok, []} = FerricStore.flow_governance_ledger(id, partition_key: @partition)

    Process.delete(:ferricstore_governance_ledger_before_append_hook)

    assert {:ok, replayed} =
             FerricStore.flow_effect_reserve(id, "send", "email.send", opts)

    assert replayed.decision == :already_reserved
    assert {:ok, [event]} = FerricStore.flow_governance_ledger(id, partition_key: @partition)
    assert event.kind == :effect_reserved
  end

  test "terminal effect retry completes a ledger side effect that previously failed" do
    type = unique_flow_id("effect-terminal-ledger-recovery-type")
    id = unique_flow_id("effect-terminal-ledger-recovery-flow")
    claimed = create_and_claim!(id, type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "send", "email.send",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "terminal-ledger-recovery-digest",
               idempotency_key: "terminal-ledger-recovery-idempotency",
               now_ms: 1_002
             )

    Process.put(:ferricstore_governance_ledger_before_append_hook, fn event ->
      if event.kind == :effect_confirmed,
        do: {:error, "ERR injected governance ledger failure"},
        else: :ok
    end)

    on_exit(fn ->
      Process.delete(:ferricstore_governance_ledger_before_append_hook)
    end)

    terminal_opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      external_id: "message-1",
      now_ms: 1_003
    ]

    assert {:error, "ERR injected governance ledger failure"} =
             FerricStore.flow_effect_confirm(id, "send", terminal_opts)

    assert {:ok, persisted} =
             FerricStore.flow_effect_get(id, "send", partition_key: @partition)

    assert persisted.status == :confirmed

    Process.delete(:ferricstore_governance_ledger_before_append_hook)

    assert {:ok, replayed} =
             FerricStore.flow_effect_confirm(
               id,
               "send",
               Keyword.put(terminal_opts, :now_ms, 1_004)
             )

    assert replayed.decision == :already_applied

    assert {:ok, events} =
             FerricStore.flow_governance_ledger(id, partition_key: @partition)

    assert Enum.map(events, & &1.kind) == [:effect_reserved, :effect_confirmed]
  end

  test "terminal effect retry completes a circuit side effect that previously failed" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("effect-terminal-circuit-recovery-type")
    id = unique_flow_id("effect-terminal-circuit-recovery-flow")
    effect_type = unique_flow_id("effect-terminal-circuit-recovery")
    scope = "effect:" <> effect_type

    assert {:ok, _policy} =
             FerricStore.flow_policy_set(type,
               governance: %{circuits: %{scope => %{failure_threshold: 1, open_ms: 1_000}}}
             )

    claimed = create_and_claim!(id, type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "send", effect_type,
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "terminal-circuit-recovery-digest",
               idempotency_key: "terminal-circuit-recovery-idempotency",
               now_ms: 1_002
             )

    circuit_key = Keys.governance_circuit_key(scope)
    assert :ok = Router.put(ctx, circuit_key, <<0, 1, 2>>, 0)

    terminal_opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token,
      error: "smtp unavailable",
      now_ms: 1_003
    ]

    assert {:error, "ERR flow circuit record is corrupt"} =
             FerricStore.flow_effect_fail(id, "send", terminal_opts)

    assert {:ok, persisted} =
             FerricStore.flow_effect_get(id, "send", partition_key: @partition)

    assert persisted.status == :failed
    assert :ok = Router.delete(ctx, circuit_key)

    assert {:ok, replayed} =
             FerricStore.flow_effect_fail(
               id,
               "send",
               Keyword.put(terminal_opts, :now_ms, 1_004)
             )

    assert replayed.decision == :already_applied
    assert {:ok, circuit} = CircuitStore.get(ctx, scope)
    assert circuit.status == :open
    assert circuit.failure_count == 1
  end

  test "ledger codec rejects a status that does not match its event kind" do
    type = unique_flow_id("ledger-status-type")
    id = unique_flow_id("ledger-status-flow")
    claimed = create_and_claim!(id, type)

    assert {:ok, _reserved} =
             FerricStore.flow_effect_reserve(id, "send", "email.send",
               partition_key: @partition,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               operation_digest: "digest-5",
               idempotency_key: "idempotency-5",
               now_ms: 1_002
             )

    ctx = FerricStore.Instance.get(:default)
    key = Keys.governance_ledger_index_key(id, @partition)
    encoded = Router.get(ctx, key)
    assert {:ok, {:flow_governance_ledger_index_v1, [event]}} = TermCodec.decode(encoded)
    corrupt = TermCodec.encode({:flow_governance_ledger_index_v1, [%{event | status: :bogus}]})
    assert :ok = Router.put(ctx, key, corrupt, 0)

    assert {:error, "ERR flow governance ledger index is corrupt"} =
             FerricStore.flow_governance_ledger(id, partition_key: @partition)
  end

  test "ledger indexes retain newest events within a fixed byte budget" do
    ctx = FerricStore.Instance.get(:default)
    type = unique_flow_id("ledger-byte-budget-type")
    id = unique_flow_id("ledger-byte-budget-flow")
    _claimed = create_and_claim!(id, type)
    record = ctx |> Router.get(Keys.state_key(id, @partition)) |> Ferricstore.Flow.decode_record()
    message = String.duplicate("m", 60_000)

    for sequence <- 1..18 do
      assert {:ok, :ok} =
               Ledger.append(
                 ctx,
                 record,
                 :effect_reserved,
                 %{
                   effect_key: "effect-#{sequence}",
                   effect_type: "email.send",
                   status: :reserved,
                   message: message
                 },
                 2_000 + sequence
               )
    end

    index_key = Keys.governance_ledger_index_key(id, @partition)
    encoded = Router.get(ctx, index_key)
    assert is_binary(encoded)
    assert byte_size(encoded) <= 900_000

    assert {:ok, events} = Ledger.list(ctx, id, partition_key: @partition, limit: 1_000)
    assert List.last(events).at_ms == 2_018
    assert length(events) < 18
  end

  test "governance lists surface corrupt cataloged records" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("budget-corrupt-list")

    assert {:ok, _budget} =
             FerricStore.flow_budget_reserve(scope, 1,
               limit: 10,
               window_ms: 1_000,
               now_ms: 100
             )

    assert :ok = Router.put(ctx, Keys.governance_budget_key(scope), <<0, 1, 2>>, 0)

    assert {:error, "ERR flow budget record is corrupt"} =
             FerricStore.flow_budget_list(scope: scope)
  end

  test "exact approval lists preserve and surface corrupt cataloged records" do
    ctx = FerricStore.Instance.get(:default)
    approval_id = unique_flow_id("approval-corrupt-exact-list")
    scope = unique_flow_id("approval-corrupt-exact-scope")
    approval_key = Keys.governance_approval_key(approval_id)
    scope_catalog = Keys.governance_approval_scope_catalog_key(scope)

    assert {:ok, _approval} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "flow-corrupt-exact-list",
               scope: scope,
               now_ms: 1_000
             )

    assert :ok = Router.put(ctx, approval_key, <<0, 1, 2>>, 0)

    assert {:error, "ERR flow approval record is corrupt"} =
             FerricStore.flow_approval_list(scope: scope)

    assert {:ok, true} = Catalog.member?(ctx, scope_catalog, approval_key)
  end

  test "approval decisions atomically expire requests at their deadline" do
    approval_id = unique_flow_id("approval-deadline")
    scope = unique_flow_id("approval-deadline-scope")

    assert {:ok, %{status: :pending}} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "flow-approval-deadline",
               scope: scope,
               expires_at_ms: 2_000,
               now_ms: 1_000
             )

    assert {:error, %{code: "GOVERNANCE_CONFLICT", status: :expired}} =
             FerricStore.flow_approval_approve(approval_id,
               approver: "operator",
               now_ms: 2_000
             )

    assert {:ok, expired} = FerricStore.flow_approval_get(approval_id)
    assert expired.status == :expired
    assert expired.decided_at_ms == 2_000
    assert is_nil(expired.decided_by)

    assert {:ok, [listed]} = FerricStore.flow_approval_list(status: :expired, scope: scope)
    assert listed.id == approval_id
  end

  test "approval requests reject an absolute deadline that is not in the future" do
    approval_id = unique_flow_id("approval-stale-deadline")

    assert {:error, "ERR flow approval expires_at_ms must be greater than now_ms"} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "flow-approval-stale-deadline",
               scope: "approval-stale-deadline-scope",
               expires_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_approval_get(approval_id)
  end

  test "approval decisions reject time regression without corrupting the record" do
    approval_id = unique_flow_id("approval-time-regression")

    assert {:ok, requested} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "flow-approval-time-regression",
               scope: "approval-time-regression-scope",
               now_ms: 2_000
             )

    assert {:error, "ERR flow approval now_ms cannot precede requested_at_ms"} =
             FerricStore.flow_approval_approve(approval_id,
               approver: "operator",
               now_ms: 1_999
             )

    assert {:ok, ^requested} = FerricStore.flow_approval_get(approval_id)
  end

  test "budget commits reject unsafe usage terms without corrupting the record" do
    scope = unique_flow_id("budget-unsafe-usage")

    assert {:ok, reserved} =
             FerricStore.flow_budget_reserve(scope, 1,
               limit: 10,
               window_ms: 1_000,
               reservation_id: "reservation",
               now_ms: 1_000
             )

    assert {:error, "ERR flow budget usage must be a bounded portable term"} =
             FerricStore.flow_budget_commit(scope, "reservation", 1,
               usage: fn -> :unsafe end,
               now_ms: 1_001
             )

    assert {:ok, fetched} = FerricStore.flow_budget_get(scope)

    assert fetched ==
             Map.drop(reserved, [
               :reservation_id,
               :reserved_amount,
               :actual_amount,
               :status,
               :usage,
               :overage_amount,
               :reserved_at_ms,
               :settled_at_ms
             ])
  end

  test "oversized governance storage keys are rejected before catalog publication" do
    ctx = FerricStore.Instance.get(:default)
    oversized = String.duplicate("k", Router.max_key_size())

    cases = [
      {:approval, Keys.governance_approval_key(oversized),
       fn ->
         FerricStore.flow_approval_request(oversized,
           flow_id: "flow",
           scope: "scope",
           now_ms: 1_000
         )
       end},
      {:budget, Keys.governance_budget_key(oversized),
       fn ->
         FerricStore.flow_budget_reserve(oversized, 1,
           limit: 1,
           window_ms: 1_000,
           now_ms: 1_000
         )
       end},
      {:circuit, Keys.governance_circuit_key(oversized),
       fn -> FerricStore.flow_circuit_open(oversized, now_ms: 1_000) end},
      {:limit, Keys.governance_limit_key(oversized),
       fn ->
         FerricStore.flow_limit_lease(oversized,
           shard_id: 0,
           amount: 1,
           limit: 1,
           ttl_ms: 1_000,
           now_ms: 1_000
         )
       end}
    ]

    on_exit(fn ->
      Enum.each(cases, fn {kind, key, _call} ->
        _ = Catalog.unregister_key(ctx, Keys.governance_catalog_key(kind), key)
      end)
    end)

    for {kind, key, call} <- cases do
      assert {:error, "ERR key too large (max 65535 bytes)"} = call.()
      assert {:ok, false} = Catalog.member?(ctx, Keys.governance_catalog_key(kind), key)
    end
  end

  test "governance stores reject integers outside the exact durable range" do
    max_exact = 9_007_199_254_740_991
    approval_id = unique_flow_id("approval-exact-bound")
    budget_scope = unique_flow_id("budget-exact-bound")
    circuit_scope = unique_flow_id("circuit-exact-bound")

    assert {:error, _reason} =
             FerricStore.flow_approval_request(approval_id,
               flow_id: "flow",
               scope: "scope",
               now_ms: max_exact + 1
             )

    assert {:error, _reason} =
             FerricStore.flow_budget_reserve(budget_scope, max_exact + 1,
               limit: max_exact + 1,
               window_ms: 1_000,
               now_ms: 1_000
             )

    assert {:error, _reason} =
             FerricStore.flow_circuit_open(circuit_scope,
               now_ms: max_exact + 1,
               open_ms: max_exact + 1
             )

    assert {:ok, nil} = FerricStore.flow_approval_get(approval_id)
    assert {:ok, nil} = FerricStore.flow_budget_get(budget_scope)
    assert {:ok, nil} = FerricStore.flow_circuit_get(circuit_scope)
  end

  test "budget commits reject aggregate usage outside the exact durable range" do
    max_exact = 9_007_199_254_740_991
    scope = unique_flow_id("budget-aggregate-exact-bound")

    assert {:ok, _reserved} =
             FerricStore.flow_budget_reserve(scope, max_exact - 1,
               limit: max_exact,
               window_ms: 1_000,
               reservation_id: "first",
               now_ms: 1_000
             )

    assert {:ok, first} =
             FerricStore.flow_budget_commit(scope, "first", max_exact - 1, now_ms: 1_001)

    assert {:ok, _reserved} =
             FerricStore.flow_budget_reserve(scope, 1,
               reservation_id: "second",
               now_ms: 1_002
             )

    assert {:error, "ERR flow budget aggregate usage exceeds durable integer range"} =
             FerricStore.flow_budget_commit(scope, "second", max_exact, now_ms: 1_003)

    assert {:ok, fetched} = FerricStore.flow_budget_get(scope)
    assert fetched.used == max_exact
    assert fetched.scope == first.scope
  end

  test "limit reconciliation rejects time outside the exact durable range" do
    ctx = FerricStore.Instance.get(:default)

    assert {:error, "ERR invalid flow limit reconciliation options"} =
             LimitReconciler.run_once(ctx,
               now_ms: 9_007_199_254_740_992,
               reservation_limit: 1
             )
  end

  test "circuit mutations reject timestamps older than durable state" do
    scope = unique_flow_id("circuit-monotonic-time")

    assert {:ok, opened} =
             FerricStore.flow_circuit_open(scope,
               failure_threshold: 1,
               open_ms: 1_000,
               now_ms: 1_000
             )

    assert opened.status == :open

    assert {:error, "ERR flow circuit now_ms cannot precede updated_at_ms"} =
             FerricStore.flow_circuit_close(scope, now_ms: 999)

    assert {:ok, persisted} = FerricStore.flow_circuit_get(scope)
    assert persisted.status == :open
    assert persisted.updated_at_ms == 1_000
  end

  test "circuit list preserves option validation errors" do
    assert {:error, "ERR flow circuit limit must be a positive integer"} =
             FerricStore.flow_circuit_list(limit: 0)

    assert {:error, "ERR flow circuit limit must be a positive integer"} =
             CircuitStore.list(nil, limit: 0)
  end

  test "approval rejects oversized secondary catalog dimensions before storage" do
    ctx = FerricStore.Instance.get(:default)
    oversized = String.duplicate("x", Router.max_key_size() + 1)
    flow_id_case = unique_flow_id("approval-oversized-flow-id")
    scope_case = unique_flow_id("approval-oversized-scope")

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             FerricStore.flow_approval_request(flow_id_case,
               flow_id: oversized,
               scope: "scope",
               now_ms: 1_000
             )

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             FerricStore.flow_approval_request(scope_case,
               flow_id: "flow",
               scope: oversized,
               now_ms: 1_000
             )

    for id <- [flow_id_case, scope_case] do
      key = Keys.governance_approval_key(id)
      assert {:ok, nil} = FerricStore.flow_approval_get(id)
      assert {:ok, false} = Catalog.member?(ctx, Keys.governance_catalog_key(:approval), key)
    end
  end

  test "exact governance lists reject oversized derived keys" do
    oversized = String.duplicate("x", Router.max_key_size() + 1)

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             FerricStore.flow_approval_list(flow_id: oversized)

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             FerricStore.flow_budget_list(scope: oversized)

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             FerricStore.flow_circuit_list(scope: oversized)
  end

  test "budget rejects oversized reservation identifiers before catalog publication" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("budget-oversized-reservation-id")
    key = Keys.governance_budget_key(scope)

    assert {:error, "ERR flow budget reservation_id must be at most 256 bytes"} =
             FerricStore.flow_budget_reserve(scope, 1,
               limit: 10,
               window_ms: 1_000,
               reservation_id: String.duplicate("r", 257),
               now_ms: 1_000
             )

    assert {:ok, nil} = FerricStore.flow_budget_get(scope)
    assert {:ok, false} = Catalog.member?(ctx, Keys.governance_catalog_key(:budget), key)
  end

  test "approval metadata is bounded before record publication" do
    ctx = FerricStore.Instance.get(:default)
    request_id = unique_flow_id("approval-oversized-metadata")
    decision_id = unique_flow_id("approval-oversized-approver")

    assert {:error, "ERR flow approval reason must be at most 262144 bytes"} =
             FerricStore.flow_approval_request(request_id,
               flow_id: "flow",
               scope: "scope",
               reason: String.duplicate("r", 262_145),
               now_ms: 1_000
             )

    request_key = Keys.governance_approval_key(request_id)
    assert {:ok, nil} = FerricStore.flow_approval_get(request_id)

    assert {:ok, false} =
             Catalog.member?(ctx, Keys.governance_catalog_key(:approval), request_key)

    assert {:ok, pending} =
             FerricStore.flow_approval_request(decision_id,
               flow_id: "flow",
               scope: "scope",
               now_ms: 1_000
             )

    assert {:error, "ERR flow approval approver must be at most 262144 bytes"} =
             FerricStore.flow_approval_approve(decision_id,
               approver: String.duplicate("a", 262_145),
               now_ms: 1_001
             )

    assert {:ok, persisted} = FerricStore.flow_approval_get(decision_id)
    assert persisted.status == :pending
    assert persisted == pending
  end

  test "cache session heads reject oversized durable identities" do
    ctx = FerricStore.Instance.get(:default)
    node_id = unique_flow_id("cache-head-identity-node")
    instance_name = unique_flow_id("cache-head-identity-instance")

    assert {:ok, _session} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)

    head_key = Keys.governance_limit_cache_session_head_key(node_id, instance_name)

    corrupt =
      TermCodec.encode({
        :flow_governance_cache_session_head_v1,
        1,
        String.duplicate("s", Router.max_key_size() + 1)
      })

    assert :ok = Router.put(ctx, head_key, corrupt, 0)

    assert {:error, :cache_session_head_corrupt} =
             CacheSessionStore.open(ctx, node_id: node_id, instance_name: instance_name)
  end

  test "effect transitions reject timestamps older than the durable effect" do
    type = unique_flow_id("effect-monotonic-time-type")
    id = unique_flow_id("effect-monotonic-time-flow")
    claimed = create_and_claim!(id, type)

    opts = [
      partition_key: @partition,
      lease_token: claimed.lease_token,
      fencing_token: claimed.fencing_token
    ]

    assert {:ok, reserved} =
             FerricStore.flow_effect_reserve(
               id,
               "charge",
               "payment.charge",
               opts ++
                 [
                   operation_digest: "digest",
                   idempotency_key: "idempotency",
                   now_ms: 1_002
                 ]
             )

    assert {:error, "ERR flow effect now_ms cannot precede updated_at_ms"} =
             FerricStore.flow_effect_confirm(id, "charge", opts ++ [now_ms: 1_001])

    assert {:ok, persisted} =
             FerricStore.flow_effect_get(id, "charge", partition_key: @partition)

    assert persisted.status == :reserved
    assert persisted.updated_at_ms == reserved.updated_at_ms
  end

  defp create_and_claim!(id, type) do
    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "governance-review-worker",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_001
             )

    claimed
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
