defmodule Ferricstore.Flow.CreateIdLimitTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.MutationAttrs

  @too_long {:error, "ERR flow id is longer than 256 bytes"}

  test "public create takes a 256-byte id and rejects a longer one without writing it" do
    partition = "id-limit"
    longest = id_of(256)
    too_long = id_of(257)

    assert :ok = FerricStore.flow_create(longest, type: "id-limit", partition_key: partition)
    assert {:ok, %{id: ^longest}} = FerricStore.flow_get(longest, partition_key: partition)

    assert FerricStore.flow_create(too_long, type: "id-limit", partition_key: partition) ==
             @too_long

    assert {:ok, nil} = FerricStore.flow_get(too_long, partition_key: partition)
  end

  test "a batch with one long id writes none of its items" do
    partition = "id-limit-batch"
    short = unique_flow_id("id-limit-short")
    too_long = id_of(257)

    assert FerricStore.flow_create_many(
             partition,
             [%{id: short}, %{id: too_long}],
             type: "id-limit"
           ) == @too_long

    assert {:ok, nil} = FerricStore.flow_get(short, partition_key: partition)
    assert {:ok, nil} = FerricStore.flow_get(too_long, partition_key: partition)
  end

  test "start and claim rejects a long id" do
    assert FerricStore.flow_start_and_claim(id_of(257), "id-limit", "queued",
             partition_key: "id-limit-claim",
             worker: "w"
           ) == @too_long
  end

  test "spawned children are new Flows and take the same limit" do
    assert MutationAttrs.spawn_children_attrs(
             unique_flow_id("id-limit-parent"),
             [
               %{id: id_of(257), type: "id-limit"}
             ],
             partition_key: "id-limit-children",
             group_id: "g",
             exhaust_to: %{success: "completed", failure: "failed"},
             fencing_token: 1
           ) == @too_long
  end

  test "a Flow created with a longer id before the limit keeps working" do
    partition = "id-limit-existing"
    legacy = id_of(1_024)
    now = System.system_time(:millisecond)

    # Internal creates are not limited; this stands in for a Flow stored
    # before the limit existed.
    assert :ok =
             Ferricstore.Flow.create_internal(
               FerricStore.Instance.get(:default),
               legacy,
               type: "id-limit-existing",
               partition_key: partition,
               run_at_ms: now
             )

    assert {:ok, [%{id: ^legacy} = claimed]} =
             FerricStore.flow_claim_due("id-limit-existing",
               partition_key: partition,
               worker: "w",
               limit: 1,
               now_ms: now
             )

    assert :ok =
             FerricStore.flow_complete(legacy, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: now + 1
             )

    assert {:ok, %{state: "completed"}} = FerricStore.flow_get(legacy, partition_key: partition)
  end

  defp id_of(bytes) do
    prefix = unique_flow_id("id-limit") <> "-"
    prefix <> String.duplicate("x", bytes - byte_size(prefix))
  end
end
