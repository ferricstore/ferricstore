defmodule Ferricstore.Flow.FlowInputLimitsTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.MutationAttrs

  @too_long {:error, "ERR flow id is longer than 256 bytes"}
  @partition_too_long {:error, "ERR flow partition_key is longer than 256 bytes"}

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

  test "a new Flow takes a 256-byte partition key and rejects a longer one" do
    id = unique_flow_id("partition-limit")
    longest = id_of(256)

    assert :ok = FerricStore.flow_create(id, type: "id-limit", partition_key: longest)
    assert {:ok, %{id: ^id}} = FerricStore.flow_get(id, partition_key: longest)

    too_long = id_of(257)

    assert FerricStore.flow_create(unique_flow_id("partition-limit"),
             type: "id-limit",
             partition_key: too_long
           ) == @partition_too_long

    assert FerricStore.flow_create_many(too_long, [%{id: unique_flow_id("partition-limit")}],
             type: "id-limit"
           ) == @partition_too_long

    assert FerricStore.flow_start_and_claim(
             unique_flow_id("partition-limit"),
             "id-limit",
             "queued",
             partition_key: too_long,
             worker: "w"
           ) == @partition_too_long
  end

  test "children inherit a parent's longer partition key but cannot open a new one" do
    legacy_partition = id_of(1_024)

    opts = [
      partition_key: legacy_partition,
      group_id: "g",
      exhaust_to: %{success: "completed", failure: "failed"},
      fencing_token: 1
    ]

    assert {:ok, %{children: [%{partition_key: ^legacy_partition}]}} =
             MutationAttrs.spawn_children_attrs(
               unique_flow_id("partition-parent"),
               [%{id: unique_flow_id("partition-child"), type: "id-limit"}],
               opts
             )

    assert MutationAttrs.spawn_children_attrs(
             unique_flow_id("partition-parent"),
             [
               %{
                 id: unique_flow_id("partition-child"),
                 type: "id-limit",
                 partition_key: id_of(300)
               }
             ],
             opts
           ) == @partition_too_long
  end

  test "a Flow stored under a longer partition key keeps working" do
    partition = id_of(1_024)
    id = unique_flow_id("partition-existing")
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create_internal(
               FerricStore.Instance.get(:default),
               id,
               type: "partition-existing",
               partition_key: partition,
               run_at_ms: now
             )

    assert {:ok, [%{id: ^id} = claimed]} =
             FerricStore.flow_claim_due("partition-existing",
               partition_key: partition,
               worker: "w",
               limit: 1,
               now_ms: now
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: now + 1
             )
  end

  test "new Flows take a type up to 128 bytes, a state up to 64 and a correlation id up to 256" do
    partition = "input-limits"
    create = &FerricStore.flow_create(unique_flow_id("input-limits"), &1)

    assert :ok =
             create.(
               type: bytes(128),
               state: bytes(64),
               correlation_id: bytes(256),
               partition_key: partition
             )

    assert create.(type: bytes(129), partition_key: partition) ==
             {:error, "ERR flow type is longer than 128 bytes"}

    assert create.(type: "t", state: bytes(65), partition_key: partition) ==
             {:error, "ERR flow state is longer than 64 bytes"}

    assert create.(type: "t", correlation_id: bytes(257), partition_key: partition) ==
             {:error, "ERR flow correlation_id is longer than 256 bytes"}

    assert FerricStore.flow_start_and_claim(unique_flow_id("input-limits"), bytes(129), "queued",
             partition_key: partition,
             worker: "w"
           ) == {:error, "ERR flow type is longer than 128 bytes"}

    assert FerricStore.flow_start_and_claim(unique_flow_id("input-limits"), "t", bytes(65),
             partition_key: partition,
             worker: "w"
           ) == {:error, "ERR flow state is longer than 64 bytes"}
  end

  test "a claim or start takes a worker name up to 256 bytes" do
    assert FerricStore.flow_claim_due("input-limits-claim",
             partition_key: "input-limits-claim",
             worker: bytes(257)
           ) == {:error, "ERR flow worker is longer than 256 bytes"}

    assert FerricStore.flow_start_and_claim(unique_flow_id("input-limits"), "t", "queued",
             partition_key: "input-limits-claim",
             worker: bytes(257)
           ) == {:error, "ERR flow worker is longer than 256 bytes"}

    assert {:ok, []} =
             FerricStore.flow_claim_due("input-limits-claim",
               partition_key: "input-limits-claim",
               worker: bytes(256)
             )
  end

  test "a transition, step or reschedule moves only into a state up to 64 bytes" do
    too_long = {:error, "ERR flow state is longer than 64 bytes"}
    opts = [fencing_token: 1, partition_key: "input-limits"]

    assert MutationAttrs.transition_attrs("flow", "queued", bytes(65), opts) == too_long
    assert {:ok, _attrs} = MutationAttrs.transition_attrs("flow", "queued", bytes(64), opts)

    assert MutationAttrs.step_continue_attrs("flow", "token", "queued", bytes(65), opts) ==
             too_long

    assert MutationAttrs.reschedule_attrs("flow", "token", [state: bytes(65)] ++ opts) ==
             too_long
  end

  test "a child group takes a group id up to 128 bytes and states up to 64" do
    spawn = fn opts ->
      MutationAttrs.spawn_children_attrs(
        unique_flow_id("input-limits-parent"),
        [%{id: unique_flow_id("input-limits-child"), type: "t"}],
        Keyword.merge(
          [
            partition_key: "input-limits",
            group_id: "g",
            exhaust_to: %{success: "completed", failure: "failed"},
            fencing_token: 1
          ],
          opts
        )
      )
    end

    assert {:ok, _attrs} = spawn.(group_id: bytes(128), wait_state: bytes(64))
    assert spawn.(group_id: bytes(129)) == {:error, "ERR flow group_id is longer than 128 bytes"}
    assert spawn.(wait_state: bytes(65)) == {:error, "ERR flow state is longer than 64 bytes"}

    assert spawn.(exhaust_to: %{success: bytes(65), failure: "failed"}) ==
             {:error, "ERR flow state is longer than 64 bytes"}
  end

  test "a Flow stored with a longer type and state can still be claimed and completed" do
    partition = "input-limits-existing"
    type = bytes(200)
    id = unique_flow_id("input-limits-existing")
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create_internal(
               FerricStore.Instance.get(:default),
               id,
               type: type,
               state: bytes(100),
               partition_key: partition,
               run_at_ms: now
             )

    assert {:ok, [%{id: ^id} = claimed]} =
             FerricStore.flow_claim_due(type,
               state: bytes(100),
               partition_key: partition,
               worker: "w",
               limit: 1,
               now_ms: now
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: now + 1
             )
  end

  defp bytes(count), do: String.duplicate("s", count)

  defp id_of(bytes) do
    prefix = unique_flow_id("id-limit") <> "-"
    prefix <> String.duplicate("x", bytes - byte_size(prefix))
  end
end
