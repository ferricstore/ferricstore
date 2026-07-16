defmodule Ferricstore.FlowGovernanceExhaustiveReviewTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Governance.Budget
  alias Ferricstore.Flow.Governance.Admin
  alias Ferricstore.Flow.Governance.Approval
  alias Ferricstore.Flow.Governance.ApprovalStore
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.BudgetStore
  alias Ferricstore.Flow.Governance.CacheSessionStore
  alias Ferricstore.Flow.Governance.Circuit
  alias Ferricstore.Flow.Governance.CircuitStore
  alias Ferricstore.Flow.Governance.CreditLease
  alias Ferricstore.Flow.Governance.Effect
  alias Ferricstore.Flow.Governance.Ledger
  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Flow.Governance.LimitCatalogOutbox
  alias Ferricstore.Flow.Governance.LimitRecord
  alias Ferricstore.Flow.Governance.LimitReconciler
  alias Ferricstore.Flow.Governance.LimitStorageCleaner
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Governance.Policy
  alias Ferricstore.Flow.Governance.ReleaseOutbox
  alias Ferricstore.Flow.Governance.Scope
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.TermCodec

  @max_exact_version 9_007_199_254_740_991

  test "budget reservation retries are idempotent and cannot reuse a reservation id" do
    budget = Budget.fixed_window("scope", 10, 1_000, now_ms: 100)

    assert {:ok, reserved, reservation} =
             Budget.reserve(budget, 3, now_ms: 100, reservation_id: "reservation-1")

    assert {:ok, retried, ^reservation} =
             Budget.reserve(reserved, 3, now_ms: 101, reservation_id: "reservation-1")

    assert retried == reserved
    assert retried.used == 3

    assert {:error, "ERR flow budget reservation id already exists", unchanged} =
             Budget.reserve(retried, 4, now_ms: 102, reservation_id: "reservation-1")

    assert unchanged == retried

    assert {:ok, settled, _reservation} =
             Budget.release(retried, "reservation-1", now_ms: 103)

    assert {:error, "ERR flow budget reservation id already exists", unchanged} =
             Budget.reserve(settled, 3, now_ms: 104, reservation_id: "reservation-1")

    assert unchanged == settled
  end

  test "budget active reservations are bounded without breaking idempotent retries" do
    reservations =
      Map.new(1..4_096, fn index ->
        id = "reservation-#{index}"

        {id,
         %{
           id: id,
           amount: 1,
           actual_amount: nil,
           status: :reserved,
           usage: nil,
           reserved_at_ms: 0,
           settled_at_ms: nil,
           window_start_ms: 0
         }}
      end)

    budget = %Budget{
      scope: "scope",
      limit: 4_097,
      window_ms: 1_000,
      window_start_ms: 0,
      used: 4_096,
      reservations: reservations
    }

    assert Budget.valid?(budget)

    assert {:ok, ^budget, %{id: "reservation-1"}} =
             Budget.reserve(budget, 1, now_ms: 1, reservation_id: "reservation-1")

    assert {:error, "ERR flow budget has too many active reservations", ^budget} =
             Budget.reserve(budget, 1, now_ms: 1, reservation_id: "overflow")

    refute Budget.valid?(%{
             budget
             | reservations:
                 Map.put(reservations, "overflow", %{
                   id: "overflow",
                   amount: 1,
                   actual_amount: nil,
                   status: :reserved,
                   usage: nil,
                   reserved_at_ms: 1,
                   settled_at_ms: nil,
                   window_start_ms: 0
                 })
           })
  end

  test "budget validation rejects undercounted current-window reservations" do
    budget = Budget.fixed_window("scope", 10, 1_000, now_ms: 100)

    assert {:ok, reserved, _reservation} =
             Budget.reserve(budget, 3, now_ms: 101, reservation_id: "reservation")

    refute Budget.valid?(%{reserved | used: 2})
  end

  test "budget validation rejects impossible released reservation payloads" do
    budget = Budget.fixed_window("scope", 10, 1_000, now_ms: 100)

    assert {:ok, reserved, _reservation} =
             Budget.reserve(budget, 3, now_ms: 101, reservation_id: "reservation")

    assert {:ok, released, _reservation} =
             Budget.release(reserved, "reservation", now_ms: 102)

    corrupted =
      update_in(released.settled_reservations["reservation"], fn reservation ->
        %{reservation | actual_amount: 1}
      end)

    refute Budget.valid?(corrupted)
  end

  test "budget validation rejects oversized scopes and aggregate records" do
    refute Budget.valid?(Budget.fixed_window(String.duplicate("s", 65_536), 1, 1_000, now_ms: 0))

    reservations =
      Map.new(1..2_500, fn index ->
        id = Integer.to_string(index) <> String.duplicate("r", 250)

        {id,
         %{
           id: id,
           amount: 1,
           actual_amount: nil,
           status: :reserved,
           usage: nil,
           reserved_at_ms: 1,
           settled_at_ms: nil,
           window_start_ms: 0
         }}
      end)

    refute Budget.valid?(%Budget{
             scope: "scope",
             limit: 2_500,
             used: 2_500,
             window_ms: 1_000,
             window_start_ms: 0,
             reservations: reservations
           })
  end

  test "policy string-list deduplication preserves first-seen order" do
    assert {:ok, policy} =
             Policy.normalize(%{
               effects: %{allowed: ["email", "webhook", "email", "sms", "webhook"]}
             })

    assert policy.effects.allowed == ["email", "webhook", "sms"]
  end

  test "policy resolution ignores unsupported effect-specific policy maps" do
    policy = %{
      governance: %{
        effects_by_type: %{
          "email" => %{effects: %{denied: ["email"]}}
        }
      }
    }

    resolved = Policy.resolve(policy, "queued", "email")

    assert resolved.effects.denied == []
  end

  test "policy string lists stop at the governance collection bound" do
    values = Enum.map(1..1_001, &"effect-#{&1}")

    assert {:error, "ERR flow governance list must contain at most 1000 values"} =
             Policy.normalize(%{effects: %{allowed: values}})
  end

  test "policy circuit error classes have a bounded persisted width" do
    error_class = String.duplicate("e", 257)

    assert {:error, "ERR flow governance error_classes values must be at most 256 bytes"} =
             Policy.normalize(%{
               circuits: %{
                 "effect:email" => %{
                   failure_threshold: 1,
                   open_ms: 1_000,
                   error_classes: [error_class]
                 }
               }
             })
  end

  test "policy rule maps stop at the governance collection bound" do
    rules = Map.new(1..1_001, &{"scope-#{&1}", %{limit: 1}})

    assert {:error, "ERR flow governance limits policy must contain at most 1000 rules"} =
             Policy.normalize(%{limits: rules})
  end

  test "policy rule names stop at the durable dimension bound" do
    name = String.duplicate("s", 65_536)

    assert {:error, "ERR flow governance limits names must be at most 65535 bytes"} =
             Policy.normalize(%{limits: %{name => %{limit: 1}}})
  end

  test "normalized governance policies have a fixed aggregate byte budget" do
    values = Enum.map(1..600, &(String.duplicate("e", 2_000) <> Integer.to_string(&1)))

    assert {:error, "ERR flow governance policy exceeds 1048576-byte durable limit"} =
             Policy.normalize(%{effects: %{allowed: values}})
  end

  test "policy rules reject integers outside the exact durable range" do
    max_exact = 9_007_199_254_740_991

    assert {:error, _reason} =
             Policy.normalize(%{
               budgets: %{
                 "scope" => %{limit: max_exact + 1, window_ms: 1_000}
               }
             })

    assert {:error, _reason} =
             Policy.normalize(%{
               approvals: %{timeout_ms: max_exact + 1}
             })
  end

  test "ignored half-open failures release their probe without closing the circuit" do
    circuit =
      "scope"
      |> Circuit.new(
        failure_threshold: 1,
        open_ms: 1,
        error_classes: ["IgnoredError"],
        half_open_max_probes: 1,
        half_open_success_threshold: 1
      )
      |> Circuit.record_manual_open(100)
      |> Circuit.claim_probe(101)

    assert circuit.status == :half_open
    assert circuit.half_open_in_flight == 1

    updated = Circuit.record_failure(circuit, 102, error_class: "NotCountedError")

    assert updated.status == :half_open
    assert updated.half_open_in_flight == 0
    assert updated.half_open_successes == 0
    assert Circuit.probe_available?(updated, 102)
  end

  test "circuit records reject oversized error-class collections" do
    error_classes = Enum.map(1..1_001, &"Error#{&1}")

    refute Circuit.valid?(Circuit.new("scope", error_classes: error_classes))
  end

  test "circuit records reject oversized scope dimensions" do
    refute Circuit.valid?(Circuit.new(String.duplicate("s", 65_536), []))
  end

  test "circuit records bound configured and observed error-class widths" do
    oversized = String.duplicate("e", 257)

    refute Circuit.valid?(Circuit.new("scope", error_classes: [oversized]))

    circuit =
      "scope"
      |> Circuit.new(failure_threshold: 1, open_ms: 1_000)
      |> Circuit.record_failure(100, error_class: oversized)

    assert Circuit.valid?(circuit)

    assert %{error_class: "sha256:" <> digest} =
             Enum.find(circuit.events, &(&1.kind == :failure))

    assert byte_size(digest) == 43
  end

  test "circuit records reject impossible half-open probe counters" do
    circuit =
      "scope"
      |> Circuit.new(half_open_max_probes: 2, half_open_success_threshold: 3)
      |> Circuit.record_manual_open(100)
      |> Map.merge(%{
        status: :half_open,
        half_open_started_at_ms: 101,
        half_open_in_flight: 2,
        half_open_successes: 3,
        opened_at_ms: 100,
        updated_at_ms: 101
      })

    refute Circuit.valid?(circuit)
  end

  test "release outbox can persist its terminal acknowledged metadata" do
    meta = %{head: @max_exact_version, tail: @max_exact_version}

    assert {:ok, terminal, [@max_exact_version]} =
             ReleaseOutbox.acknowledge(meta, @max_exact_version, @max_exact_version)

    assert terminal == %{head: @max_exact_version + 1, tail: @max_exact_version}

    assert terminal |> ReleaseOutbox.encode_meta() |> ReleaseOutbox.decode_meta() ==
             {:ok, terminal}

    assert {:error, _reason} = ReleaseOutbox.append(terminal, 1)
  end

  test "release outbox rejects structurally invalid metadata before acknowledgement" do
    assert {:error, "ERR flow governance release outbox is corrupt"} =
             ReleaseOutbox.acknowledge(%{head: 3, tail: 1}, 3, 3)

    assert {:error, "ERR flow governance release outbox is corrupt"} =
             ReleaseOutbox.acknowledge(
               %{head: @max_exact_version + 2, tail: @max_exact_version},
               @max_exact_version + 2,
               @max_exact_version + 2
             )
  end

  test "release outbox bounds unreconciled durable intents" do
    full = %{head: 1, tail: 65_536}

    assert {:error, "ERR flow governance release outbox backlog is full"} =
             ReleaseOutbox.append(full, 1)

    assert {:ok, %{head: 2, tail: 65_537}, 65_537..65_537} =
             ReleaseOutbox.append(%{head: 2, tail: 65_536}, 1)
  end

  test "limit catalog outbox rejects structurally invalid metadata before acknowledgement" do
    assert {:error, "ERR flow limit catalog publication outbox is corrupt"} =
             LimitCatalogOutbox.acknowledge(%{head: 3, tail: 1}, 3, 1)
  end

  test "limit catalog outbox terminal acknowledgement retries remain idempotent" do
    terminal = %{head: @max_exact_version + 1, tail: @max_exact_version}

    assert {:ok, ^terminal, []} =
             LimitCatalogOutbox.acknowledge(
               terminal,
               @max_exact_version + 1,
               @max_exact_version
             )
  end

  test "release outbox cursors reject sequences outside the durable integer range" do
    cursor = "v1:0|0=#{@max_exact_version + 1}"

    assert {:error, "ERR invalid flow governance release outbox cursor"} =
             ReleaseOutbox.decode_reconcile_cursor(cursor, 1)
  end

  test "approval records reject terminal decisions at or after their expiry" do
    approval =
      Approval.request("approval",
        flow_id: "flow",
        scope: "scope",
        now_ms: 1_000,
        expires_at_ms: 2_000
      )

    invalid = %{
      approval
      | status: :approved,
        decided_by: "operator",
        decided_at_ms: 2_000
    }

    refute Approval.valid?(invalid)
  end

  test "governance models reject integers outside the exact durable range" do
    max_exact = 9_007_199_254_740_991

    approval =
      Approval.request("approval",
        flow_id: "flow",
        scope: "scope",
        now_ms: max_exact + 1
      )

    budget = Budget.fixed_window("scope", max_exact + 1, 1_000, now_ms: 0)
    circuit = Circuit.new("scope", open_ms: max_exact + 1)

    refute Approval.valid?(approval)
    refute Budget.valid?(budget)
    refute Circuit.valid?(circuit)
  end

  test "limit owner decoding rejects malformed detached reservations before normalization" do
    owner =
      valid_limit_owner()
      |> put_in([Access.key!(:leases), 0, Access.key!(:reservations)], :corrupt)

    assert {:error, "ERR flow limit record is corrupt"} = decode_limit_owner(owner)
  end

  test "limit owner decoding enforces capacity, epoch, and reclaim invariants" do
    owner = valid_limit_owner()

    invalid_owners = [
      %{owner | free: owner.free + 1},
      %{owner | epoch: 0},
      put_in(owner, [Access.key!(:leases), 0, Access.key!(:pending_reclaim)], 2),
      put_in(owner, [Access.key!(:leases), 0, Access.key!(:drain_rate)], :invalid)
    ]

    for invalid <- invalid_owners do
      assert {:error, "ERR flow limit record is corrupt"} = decode_limit_owner(invalid)
    end
  end

  test "limit owner records reject oversized scope dimensions" do
    owner = CreditLease.owner(String.duplicate("s", 65_536), 0)
    assert {:error, "ERR flow limit record is corrupt"} = LimitRecord.encode_owner(owner)
  end

  test "credit reclaim selection is deterministic across large shard maps" do
    leases =
      Map.new(0..100, fn shard_id ->
        {shard_id,
         %CreditLease.Lease{
           shard_id: shard_id,
           epoch: shard_id + 1,
           expires_at_ms: 10_000,
           available: 1
         }}
      end)

    owner = %CreditLease.Owner{
      scope: "scope",
      limit: 101,
      free: 0,
      epoch: 101,
      leases: leases
    }

    assert {:error, _denial, updated} =
             CreditLease.grant(owner, 100, 3, now_ms: 1_000, ttl_ms: 1_000)

    reclaiming =
      updated.leases
      |> Enum.filter(fn {_shard_id, lease} -> lease.pending_reclaim > 0 end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    assert reclaiming == [0, 1, 2]
  end

  test "limit cleanup checkpoints reject page numbers outside the durable page bound" do
    oversized =
      TermCodec.encode({
        :flow_governance_limit_cleanup,
        0,
        1,
        LimitRecord.max_reservation_pages() + 1,
        LimitRecord.max_reservation_pages() + 1
      })

    assert {:error, "ERR flow limit cleanup record is corrupt"} =
             LimitRecord.decode_cleanup(oversized)

    assert_raise ArgumentError, fn ->
      LimitRecord.encode_cleanup(
        0,
        1,
        LimitRecord.max_reservation_pages() + 1,
        LimitRecord.max_reservation_pages() + 1
      )
    end
  end

  test "cache-session public helpers reject malformed sessions and options without raising" do
    session = %{
      node_id: "node",
      instance_name: "instance",
      session_id: "session",
      generation: 1,
      previous_session_id: nil
    }

    assert {:error, "ERR invalid flow governance cache update options"} =
             CacheSessionStore.update_pages(nil, session, [], [])

    assert {:error, _reason} = CacheSessionStore.recover(nil, %{}, [])

    refute CacheSessionStore.page_present?(nil, %{})
    refute CacheSessionStore.head_present?(nil, [:not_a_keyword])
  end

  test "governance entrypoints reject non-keyword option lists without touching storage" do
    invalid_opts = [:not_a_keyword]

    calls = [
      {Admin, :overview, [nil, invalid_opts]},
      {ApprovalStore, :request, [nil, "id", invalid_opts]},
      {ApprovalStore, :get, [nil, "id", invalid_opts]},
      {ApprovalStore, :list, [nil, invalid_opts]},
      {ApprovalStore, :approve, [nil, "id", invalid_opts]},
      {BudgetStore, :reserve, [nil, "scope", 1, invalid_opts]},
      {BudgetStore, :commit, [nil, "scope", "reservation", 1, invalid_opts]},
      {BudgetStore, :release, [nil, "scope", "reservation", invalid_opts]},
      {BudgetStore, :get, [nil, "scope", invalid_opts]},
      {BudgetStore, :list, [nil, invalid_opts]},
      {CircuitStore, :open, [nil, "scope", invalid_opts]},
      {CircuitStore, :close, [nil, "scope", invalid_opts]},
      {CircuitStore, :record_failure, [nil, "scope", invalid_opts]},
      {CircuitStore, :record_success, [nil, "scope", invalid_opts]},
      {CircuitStore, :get, [nil, "scope", invalid_opts]},
      {CircuitStore, :list, [nil, invalid_opts]},
      {Effect, :reserve, [nil, "id", "key", "type", invalid_opts]},
      {Effect, :confirm, [nil, "id", "key", invalid_opts]},
      {Effect, :get, [nil, "id", "key", invalid_opts]},
      {Ledger, :list, [nil, "id", invalid_opts]},
      {LimitStore, :lease, [nil, "scope", invalid_opts]},
      {LimitStore, :spend, [nil, "scope", invalid_opts]},
      {LimitStore, :spend_reserved, [nil, "scope", invalid_opts, ["reservation"]]},
      {LimitStore, :renew, [nil, "scope", invalid_opts]},
      {LimitStore, :release, [nil, "scope", invalid_opts]},
      {LimitStore, :get, [nil, "scope", invalid_opts]},
      {LimitStore, :cleanup, [nil, "scope", invalid_opts]},
      {LimitStore, :list, [nil, invalid_opts]},
      {LimitReconciler, :run_once, [nil, invalid_opts]},
      {LimitStorageCleaner, :run_once, [nil, invalid_opts]},
      {LimitCache, :clear, [nil, invalid_opts]},
      {LimitCache, :with_drained_cache, [nil, fn -> :ok end, invalid_opts]},
      {LimitCache, :flush, [nil, invalid_opts]},
      {LimitCache, :recover, [nil, invalid_opts]}
    ]

    for {module, function, arguments} <- calls do
      assert {:error, _reason} = apply(module, function, arguments),
             "#{inspect(module)}.#{function}/#{length(arguments)} accepted a non-keyword list"
    end

    decoder = fn _value -> {:ok, %{}} end
    encoder = fn value -> TermCodec.encode(value) end
    initializer = fn -> {:ok, %{}} end
    mutation = fn value -> {:ok, value} end

    assert {:error, _reason} =
             AtomicRecord.mutate(
               nil,
               "key",
               decoder,
               encoder,
               initializer,
               mutation,
               max_retries: :invalid
             )
  end

  test "scope parsing rejects malformed exported arguments without raising" do
    assert {:error, "ERR governance scope metadata must be a keyword list"} =
             Scope.normalize("partition:tenant", [:not_a_keyword])

    assert {:error, "ERR governance scope must be a string"} = Scope.normalize(42)

    assert {:error, "ERR governance scope record must be a map"} =
             Scope.resolve(%{partition_key: "tenant"}, :not_a_record)
  end

  test "maintenance and ledger boundaries reject invalid finite inputs" do
    assert {:error, "ERR invalid flow limit cleanup page budget"} =
             LimitStorageCleaner.run_tick(nil, page_budget: 0)

    oversized_id = String.duplicate("x", Ferricstore.Store.Router.max_key_size())

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             Ledger.resolve_index_key(oversized_id, [])
  end

  test "telemetry uses a bounded stable code for arbitrary error terms" do
    handler_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :governance, :exhaustive_review_error],
        fn _event, _measurements, metadata, _config -> send(test_pid, {:metadata, metadata}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    secret = "secret-#{System.unique_integer([:positive])}"

    assert {:error, {:credential, ^secret}} =
             Telemetry.emit(:exhaustive_review_error, {:error, {:credential, secret}})

    assert_receive {:metadata, %{status: :error, code: "non_binary_error"}}
  end

  test "credit leases reject generation overflow before creating an invalid owner" do
    owner = %CreditLease.Owner{
      scope: "scope",
      limit: 1,
      free: 1,
      epoch: @max_exact_version
    }

    assert {:error, "ERR flow limit lease generation exhausted", ^owner} =
             CreditLease.grant(owner, 0, 1, now_ms: 1, ttl_ms: 1)
  end

  test "credit lease top-ups never shorten an existing deadline" do
    lease = %CreditLease.Lease{
      shard_id: 0,
      epoch: 1,
      expires_at_ms: 1_000,
      available: 1
    }

    owner = %CreditLease.Owner{
      scope: "scope",
      limit: 2,
      free: 1,
      epoch: 1,
      leases: %{0 => lease}
    }

    assert {:ok, _owner, topped_up} =
             CreditLease.grant(owner, 0, 1, now_ms: 500, ttl_ms: 100)

    assert topped_up.expires_at_ms == 1_000
  end

  test "credit lease initial grants set an exact deadline" do
    owner = %CreditLease.Owner{scope: "scope", limit: 1, free: 1}

    assert {:ok, _owner, lease} =
             CreditLease.grant(owner, 0, 1, now_ms: 500, ttl_ms: 100)

    assert lease.expires_at_ms == 600
  end

  test "credit lease spends reject timestamps older than the previous spend" do
    lease = %CreditLease.Lease{
      shard_id: 0,
      epoch: 1,
      expires_at_ms: 1_000,
      available: 2,
      last_spend_at_ms: 500
    }

    owner = %CreditLease.Owner{
      scope: "scope",
      limit: 2,
      free: 0,
      epoch: 1,
      leases: %{0 => lease}
    }

    assert {:error, "ERR flow limit now_ms cannot precede last_spend_at_ms", ^owner} =
             CreditLease.spend(owner, 0, 1, now_ms: 499)
  end

  test "budget transitions reject timestamps that move durable state backward" do
    budget = Budget.fixed_window("scope", 10, 1_000, now_ms: 100)

    assert {:error, "ERR flow budget now_ms cannot precede window_start_ms", ^budget} =
             Budget.reserve(budget, 1, now_ms: 99, reservation_id: "stale-reserve")

    assert {:ok, reserved, _reservation} =
             Budget.reserve(budget, 1, now_ms: 200, reservation_id: "reservation")

    assert {:error, "ERR flow budget now_ms cannot precede reservation", ^reserved} =
             Budget.commit(reserved, "reservation", 1, now_ms: 199)

    assert {:error, "ERR flow budget now_ms cannot precede reservation", ^reserved} =
             Budget.release(reserved, "reservation", now_ms: 199)
  end

  test "budget validation rejects impossible reservation timestamps" do
    budget = Budget.fixed_window("scope", 10, 1_000, now_ms: 100)

    invalid = %{
      budget
      | used: 1,
        settled_reservations: %{
          "reservation" => %{
            id: "reservation",
            amount: 1,
            actual_amount: 1,
            status: :committed,
            usage: nil,
            reserved_at_ms: 200,
            settled_at_ms: 199,
            window_start_ms: 100,
            overage_amount: 0
          }
        },
        settled_reservation_order: ["reservation"]
    }

    refute Budget.valid?(invalid)
  end

  test "circuit validation rejects backward event history" do
    circuit =
      "scope"
      |> Circuit.new(failure_threshold: 1, open_ms: 1_000)
      |> Circuit.record_manual_open(1_000)
      |> Circuit.record_manual_close(999)

    refute Circuit.valid?(circuit)
  end

  defp valid_limit_owner do
    lease = %CreditLease.Lease{
      shard_id: 0,
      epoch: 1,
      expires_at_ms: 1_000,
      available: 1,
      in_use: 1,
      pending_reclaim: 1,
      drain_rate: 0.0,
      reservations: %{}
    }

    %CreditLease.Owner{
      scope: "scope",
      limit: 5,
      free: 3,
      epoch: 1,
      config_version: 1,
      leases: %{0 => lease}
    }
  end

  defp decode_limit_owner(owner) do
    {:flow_governance_limit_v1, owner}
    |> TermCodec.encode()
    |> LimitRecord.decode_owner()
  end
end
