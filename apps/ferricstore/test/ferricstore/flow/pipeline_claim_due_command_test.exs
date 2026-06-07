defmodule Ferricstore.Flow.PipelineClaimDueCommandTest do
  use ExUnit.Case, async: true

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
end
