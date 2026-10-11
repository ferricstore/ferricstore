defmodule Ferricstore.FlowUserValueAtomsTest do
  use Ferricstore.Test.FlowCase

  # Flow user values are replicated inside Raft commands. A restarted or fresh
  # replica decodes the log with safe binary_to_term, so an atom that only the
  # writer's VM had created would make the log unrecoverable. User values must
  # not contain atoms other than true, false and nil.

  @partition "tenant-user-value-atoms"

  test "create rejects payloads with atom keys or atom values" do
    for payload <- [
          %{order_id: 1},
          %{"status" => :pending},
          ["ok", [%{"nested" => {:tuple, 1}}]]
        ] do
      error =
        assert_raise ArgumentError, fn ->
          FerricStore.flow_create(unique_flow_id("atom-payload"),
            type: "atoms",
            state: "queued",
            partition_key: @partition,
            payload: payload,
            run_at_ms: 1_000,
            now_ms: 1_000
          )
        end

      assert error.message =~ "string keys"
    end
  end

  test "create accepts payloads whose only atoms are true, false and nil" do
    id = unique_flow_id("atom-free-payload")
    payload = %{"order_id" => 1, "paid" => true, "refunded" => false, "note" => nil}

    assert :ok =
             FerricStore.flow_create(id,
               type: "atoms",
               state: "queued",
               partition_key: @partition,
               payload: payload,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("atoms",
               states: ["queued"],
               partition_key: @partition,
               worker: "w",
               limit: 1,
               now_ms: 1_001
             )

    assert claimed.payload == payload
  end

  test "create_many rejects an item payload with atoms before writing any item" do
    good = unique_flow_id("many-good")
    bad = unique_flow_id("many-bad")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_create_many(
        @partition,
        [
          {good, type: "atoms", state: "queued", payload: %{"ok" => 1}, run_at_ms: 1_000},
          {bad, type: "atoms", state: "queued", payload: %{bad: 1}, run_at_ms: 1_000}
        ],
        now_ms: 1_000
      )
    end

    assert {:ok, nil} = FerricStore.flow_get(good, partition_key: @partition)
  end

  test "complete and fail reject atoms in result and error values" do
    id = unique_flow_id("atom-result")

    assert :ok =
             FerricStore.flow_create(id,
               type: "atoms-result",
               state: "queued",
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("atoms-result",
               states: ["queued"],
               partition_key: @partition,
               worker: "w",
               limit: 1,
               now_ms: 1_001
             )

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_complete(id, claimed.lease_token,
        partition_key: @partition,
        result: %{total: 1},
        now_ms: 1_002
      )
    end

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_fail(id, claimed.lease_token,
        partition_key: @partition,
        error: %{reason: :boom},
        now_ms: 1_002
      )
    end
  end

  test "value_put rejects values containing atoms" do
    for value <- [%{doc: "x"}, %{"status" => :pending}, ["ok", {:tuple, 1}], :bare] do
      assert_raise ArgumentError, ~r/string keys/, fn ->
        FerricStore.flow_value_put(value, partition_key: @partition)
      end
    end
  end

  test "signal rejects atoms in named values before recording the signal" do
    id = create_flow!("signal-values", "atoms-signal", "waiting")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_signal(id,
        partition_key: @partition,
        signal: "ready",
        if_state: "waiting",
        transition_to: "queued",
        values: %{"event" => %{"status" => :paid}},
        run_at_ms: 2_000,
        now_ms: 1_100
      )
    end

    assert {:ok, %{state: "waiting"}} = FerricStore.flow_get(id, partition_key: @partition)
  end

  test "complete_many rejects atoms in an atom-keyed map item result" do
    claimed = create_and_claim!("complete-many-atom-key", "atoms-complete-many-a")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_complete_many(
        @partition,
        [
          %{
            id: claimed.id,
            lease_token: claimed.lease_token,
            fencing_token: claimed.fencing_token,
            result: %{order: :x}
          }
        ],
        now_ms: 1_002
      )
    end

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(claimed.id, partition_key: @partition)
  end

  test "complete_many rejects atoms in a string-keyed map item result" do
    claimed = create_and_claim!("complete-many-string-key", "atoms-complete-many-s")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_complete_many(
        @partition,
        [
          %{
            "id" => claimed.id,
            "lease_token" => claimed.lease_token,
            "fencing_token" => claimed.fencing_token,
            "result" => %{"k" => :x}
          }
        ],
        now_ms: 1_002
      )
    end

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(claimed.id, partition_key: @partition)
  end

  test "create_many rejects atoms in a tagged tuple item payload" do
    id = unique_flow_id("many-tagged-tuple")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_create_many(
        @partition,
        [{:id, id, :payload, %{"k" => :x}}],
        type: "atoms",
        state: "queued",
        run_at_ms: 1_000,
        now_ms: 1_000
      )
    end

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_create_many(
        nil,
        [{:id, id, :partition_key, @partition, :payload, %{"k" => [:x]}}],
        type: "atoms",
        state: "queued",
        run_at_ms: 1_000,
        now_ms: 1_000
      )
    end

    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: @partition)
  end

  test "transition_many rejects atoms in item values" do
    id = create_flow!("transition-many", "atoms-transition-many", "queued")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_transition_many(
        @partition,
        "queued",
        "next",
        [%{id: id, fencing_token: 0, values: %{"doc" => :x}}],
        now_ms: 1_100
      )
    end

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id, partition_key: @partition)
  end

  test "retry_many rejects atoms in an item error" do
    claimed = create_and_claim!("retry-many", "atoms-retry-many")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_retry_many(
        @partition,
        [
          %{
            id: claimed.id,
            lease_token: claimed.lease_token,
            fencing_token: claimed.fencing_token,
            error: %{"code" => :timeout}
          }
        ],
        run_at_ms: 3_000,
        now_ms: 1_002
      )
    end

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(claimed.id, partition_key: @partition)
  end

  test "fail_many rejects atoms in the batch error" do
    claimed = create_and_claim!("fail-many", "atoms-fail-many")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_fail_many(
        @partition,
        [
          %{
            id: claimed.id,
            lease_token: claimed.lease_token,
            fencing_token: claimed.fencing_token
          }
        ],
        error: %{"code" => :boom},
        now_ms: 1_002
      )
    end

    assert {:ok, %{state: "running"}} =
             FerricStore.flow_get(claimed.id, partition_key: @partition)
  end

  test "cancel rejects atoms in the reason" do
    id = create_flow!("cancel-reason", "atoms-cancel", "queued")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_cancel(id,
        partition_key: @partition,
        fencing_token: 0,
        reason: %{"by" => :operator},
        now_ms: 1_100
      )
    end

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id, partition_key: @partition)
  end

  test "instance cancel rejects atoms in the reason" do
    id = create_flow!("impl-cancel-reason", "atoms-impl-cancel", "queued")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.Impl.flow_cancel(FerricStore.Instance.get(:default), id,
        partition_key: @partition,
        fencing_token: 0,
        reason: :operator,
        now_ms: 1_100
      )
    end

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id, partition_key: @partition)
  end

  test "cancel_many rejects atoms in an item reason" do
    id = create_flow!("cancel-many-reason", "atoms-cancel-many", "queued")

    assert_raise ArgumentError, ~r/string keys/, fn ->
      FerricStore.flow_cancel_many(
        @partition,
        [%{id: id, fencing_token: 0, reason: %{"by" => :operator}}],
        now_ms: 1_100
      )
    end

    assert {:ok, %{state: "queued"}} = FerricStore.flow_get(id, partition_key: @partition)
  end

  test "batch, signal, value and cancel paths accept string keys with true, false and nil" do
    assert {:ok, %{ref: ref}} =
             FerricStore.flow_value_put(%{"ok" => true, "no" => false, "none" => nil},
               partition_key: @partition
             )

    assert is_binary(ref)

    map_id = unique_flow_id("accept-map")
    tuple_id = unique_flow_id("accept-tuple")

    assert :ok =
             FerricStore.flow_create_many(
               @partition,
               [
                 %{"id" => map_id, "payload" => %{"paid" => true, "note" => nil}},
                 {:id, tuple_id, :payload, %{"refunded" => false}}
               ],
               type: "atoms-accept",
               state: "waiting",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert :ok =
             FerricStore.flow_signal(map_id,
               partition_key: @partition,
               signal: "ready",
               if_state: "waiting",
               transition_to: "queued",
               values: %{"event" => %{"paid" => true, "note" => nil}},
               run_at_ms: 1_000,
               now_ms: 1_100
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("atoms-accept",
               states: ["queued"],
               partition_key: @partition,
               worker: "w",
               limit: 1,
               now_ms: 1_101
             )

    assert :ok =
             FerricStore.flow_complete_many(
               @partition,
               [
                 %{
                   "id" => claimed.id,
                   "lease_token" => claimed.lease_token,
                   "fencing_token" => claimed.fencing_token,
                   "result" => %{"ok" => true, "note" => nil}
                 }
               ],
               now_ms: 1_102
             )

    assert {:ok, %{state: "completed"}} = FerricStore.flow_get(map_id, partition_key: @partition)

    assert :ok =
             FerricStore.flow_cancel(tuple_id,
               partition_key: @partition,
               fencing_token: 0,
               reason: %{"by" => "operator", "forced" => false, "note" => nil},
               now_ms: 1_103
             )

    assert {:ok, %{state: "cancelled"}} =
             FerricStore.flow_get(tuple_id, partition_key: @partition)
  end

  defp create_flow!(prefix, type, state) do
    id = unique_flow_id(prefix)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: state,
               partition_key: @partition,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    id
  end

  defp create_and_claim!(prefix, type) do
    create_flow!(prefix, type, "queued")

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "w",
               limit: 1,
               now_ms: 1_001
             )

    claimed
  end
end
