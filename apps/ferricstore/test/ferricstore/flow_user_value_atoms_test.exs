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
end
