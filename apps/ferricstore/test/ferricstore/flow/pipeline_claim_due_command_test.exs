defmodule Ferricstore.Flow.PipelineClaimDueCommandTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineClaimDueCommand

  defp callbacks do
    %{
      optional_now_ms: fn opts -> {:ok, Keyword.get(opts, :now_ms)} end,
      payload_return_opts: fn _opts, default_enabled? ->
        {:ok, %{enabled?: default_enabled?, max_bytes: 64 * 1024}}
      end,
      named_value_return_opts: fn opts -> {:ok, Keyword.get(opts, :values)} end
    }
  end

  test "normalizes grouped claim_due command options" do
    assert {:ok, command} =
             PipelineClaimDueCommand.command(
               {:claim_due, "email",
                [
                  worker: "worker-a",
                  states: ["queued", "ready", "queued"],
                  lease_ms: 50,
                  limit: 10,
                  priority: 1,
                  now_ms: 123,
                  return: :jobs_compact,
                  partition_keys: ["p1", "p2", "p1"],
                  reclaim_expired: false,
                  reclaim_ratio: 0,
                  values: ["payload"]
                ]},
               callbacks()
             )

    assert command.type == "email"
    assert command.limit == 10
    assert command.return_mode == :jobs_compact
    assert command.attrs.state == ["ready", "queued"]
    assert command.attrs.partition_key == nil
    assert command.attrs.partition_keys == ["p1", "p2"]
    assert command.opts[:partition_keys] == ["p1", "p2"]
    assert command.opts[:values] == ["payload"]
    assert command.groupable?
  end

  test "rejects mixed ANY and explicit states" do
    assert {:error, "ERR flow STATE ANY cannot be mixed with explicit states"} =
             PipelineClaimDueCommand.command(
               {:claim_due, "email", [worker: "worker-a", states: ["queued", "ANY"]]},
               callbacks()
             )
  end

  test "rejects claim filters whose state and partition product exceeds the bound" do
    states = Enum.map(1..9, &"state-#{&1}")
    partition_keys = Enum.map(1..8, &"partition-#{&1}")

    assert {:error, "ERR flow claim filter footprint exceeds maximum 64"} =
             PipelineClaimDueCommand.command(
               {:claim_due, "email",
                [worker: "worker-a", states: states, partition_keys: partition_keys]},
               callbacks()
             )
  end

  test "rejects internal keys that exceed the limit only after component encoding" do
    type = String.duplicate("t", 30_000)
    state = String.duplicate("s", 25_000)

    assert {:error, "ERR key too large (max 65535 bytes)"} =
             PipelineClaimDueCommand.command(
               {:claim_due, type, [worker: "worker-a", state: state, partition_key: "partition"]},
               callbacks()
             )
  end

  test "rejects lease durations that cannot produce exact Flow index deadlines" do
    assert {:error, "ERR flow lease_ms exceeds maximum 9007199254740991"} =
             PipelineClaimDueCommand.command(
               {:claim_due, "email", [worker: "worker-a", lease_ms: 9_007_199_254_740_992]},
               callbacks()
             )
  end

  test "rejects claim deadlines that leave the exact Flow timestamp range" do
    assert {:error, "ERR flow lease_ms deadline exceeds maximum 9007199254740991"} =
             PipelineClaimDueCommand.command(
               {:claim_due, "email",
                [worker: "worker-a", now_ms: 9_007_199_254_740_991, lease_ms: 1]},
               callbacks()
             )
  end

  test "rejects blocking claims because pipelines cannot wait" do
    assert {:error, "ERR flow block_ms is not supported in pipelines"} =
             PipelineClaimDueCommand.command(
               {:claim_due, "email", [worker: "worker-a", block_ms: 1]},
               callbacks()
             )
  end
end
