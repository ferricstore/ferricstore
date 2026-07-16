defmodule Ferricstore.Flow.ClaimDueAPITest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.ClaimDueAPI

  test "wait_keys supports explicit state" do
    assert {:ok, [_ | _]} =
             ClaimDueAPI.wait_keys("type", state: "queued", partition_key: "p")
  end

  test "wait_keys rejects a state filter whose expanded footprint is unbounded" do
    states = Enum.map(1..65, &"state-#{&1}")

    assert {:error, "ERR flow claim filter footprint exceeds maximum 64"} =
             ClaimDueAPI.wait_keys("type", states: states, partition_key: "p")
  end

  test "wait_keys rejects a partition filter whose expanded footprint is unbounded" do
    partition_keys = Enum.map(1..65, &"partition-#{&1}")

    assert {:error, "ERR flow claim filter footprint exceeds maximum 64"} =
             ClaimDueAPI.wait_keys("type", state: "queued", partition_keys: partition_keys)
  end

  test "wait_keys bounds the combined state and partition footprint" do
    states = Enum.map(1..9, &"state-#{&1}")
    partition_keys = Enum.map(1..8, &"partition-#{&1}")

    assert {:error, "ERR flow claim filter footprint exceeds maximum 64"} =
             ClaimDueAPI.wait_keys("type", states: states, partition_keys: partition_keys)

    assert {:ok, keys} =
             ClaimDueAPI.wait_keys("type",
               states: Enum.take(states, 8),
               partition_keys: partition_keys
             )

    assert length(keys) == 64
  end

  test "wait_keys accounts for encoded type and state bytes before generating internal keys" do
    type = String.duplicate("t", 30_000)
    state = String.duplicate("s", 25_000)

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             ClaimDueAPI.wait_keys(type, state: state, partition_key: "partition")
  end

  test "return_records supports compact jobs" do
    assert [["id", "p", "lease", 7]] =
             ClaimDueAPI.return_records(
               :ctx,
               [%{id: "id", partition_key: "p", lease_token: "lease", fencing_token: 7}],
               %{enabled?: false, max_bytes: 0},
               :jobs_compact,
               nil
             )
  end

  test "claim_due rejects inexact lease timestamps before routing" do
    opts = [
      worker: "worker",
      governance_limit_scope: "test",
      governance_shard_id: 0
    ]

    assert {:error, "ERR flow lease_ms exceeds maximum 9007199254740991"} =
             ClaimDueAPI.result(
               %{shard_count: 1},
               "type",
               Keyword.put(opts, :lease_ms, 9_007_199_254_740_992)
             )

    assert {:error, "ERR flow now_ms exceeds maximum 9007199254740991"} =
             ClaimDueAPI.result(
               %{shard_count: 1},
               "type",
               Keyword.put(opts, :now_ms, 9_007_199_254_740_992)
             )

    assert {:error, "ERR flow lease_ms deadline exceeds maximum 9007199254740991"} =
             ClaimDueAPI.result(
               %{shard_count: 1},
               "type",
               Keyword.merge(opts,
                 now_ms: 9_007_199_254_740_991,
                 lease_ms: 1
               )
             )
  end

  test "claim_due rejects block timeouts larger than the VM timer limit before routing" do
    assert {:error, "ERR flow block_ms exceeds maximum 4294967295"} =
             ClaimDueAPI.claim_due(%{}, "type",
               worker: "worker",
               block_ms: 4_294_967_296
             )
  end

  test "claim_due rejects payload hydration above the configured ceiling before routing" do
    assert {:error, "ERR flow payload_max_bytes exceeds maximum 65536"} =
             ClaimDueAPI.result(%{}, "type",
               worker: "worker",
               payload_max_bytes: 65_537
             )
  end
end
