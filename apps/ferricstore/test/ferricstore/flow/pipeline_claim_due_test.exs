defmodule Ferricstore.Flow.PipelineClaimDueTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineClaimDue

  defp stats, do: %{groups: 0, coalesced_calls: 0, batched_calls: 0}

  defp callbacks do
    %{
      claim_due_result: fn _ctx, _type, opts ->
        limit = Keyword.fetch!(opts, :limit)
        {:ok, Enum.map(1..limit, &%{id: "job-#{&1}"})}
      end,
      return_records: fn _ctx, records, _payload_return, _return_mode, _named_values ->
        records
      end,
      normal_attrs: fn attrs, state, limit ->
        if state == nil, do: nil, else: %{attrs | state: state, limit: limit}
      end,
      normal_state_filter: fn
        "running" -> nil
        state -> state
      end
    }
  end

  test "results preserves errors and coalesces globally compatible claims" do
    claim_a = %{
      type: "email",
      opts: [limit: 1],
      limit: 1,
      key: :same,
      queue_key: :queue,
      groupable?: true,
      attrs: %{state: "queued", limit: 1, partition_key: "p", type: "email", priority: 0},
      payload_return: %{enabled?: false},
      return_mode: :records,
      named_values: nil,
      reclaim_expired?: false,
      reclaim_ratio: 0
    }

    claim_b = %{claim_a | opts: [limit: 2], limit: 2}
    commands = [{:ok, claim_a}, {:error, "ERR bad"}, {:ok, claim_b}]

    assert {results, next_stats} =
             PipelineClaimDue.results(commands, :ctx, [], stats(), callbacks())

    assert results == [
             {:ok, [%{id: "job-1"}]},
             {:error, "ERR bad"},
             {:ok, [%{id: "job-2"}, %{id: "job-3"}]}
           ]

    assert next_stats.coalesced_calls == 1
  end

  test "results falls back to adjacent grouping when queue keys conflict" do
    claim_a = %{
      type: "email",
      opts: [limit: 1],
      limit: 1,
      key: :a,
      queue_key: :queue,
      groupable?: true,
      attrs: %{state: "queued", limit: 1, partition_key: "p", type: "email", priority: 0},
      payload_return: %{enabled?: false},
      return_mode: :records,
      named_values: nil,
      reclaim_expired?: false,
      reclaim_ratio: 0
    }

    claim_b = %{claim_a | key: :b}

    assert {results, next_stats} =
             PipelineClaimDue.results(
               [{:ok, claim_a}, {:ok, claim_b}],
               :ctx,
               [],
               stats(),
               callbacks()
             )

    assert results == [{:ok, [%{id: "job-1"}]}, {:ok, [%{id: "job-1"}]}]
    assert next_stats.groups == 2
  end
end
