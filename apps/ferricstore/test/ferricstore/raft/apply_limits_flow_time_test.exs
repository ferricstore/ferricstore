defmodule Ferricstore.Raft.ApplyLimitsFlowTimeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.{ApplyContext, ApplyLimits}

  @max_exact_ms 9_007_199_254_740_991

  test "rejects inexact Flow timestamps in top-level and batched command attributes" do
    assert {:error, "ERR flow now_ms exceeds maximum 9007199254740991"} =
             ApplyLimits.validate_flow_time(%{now_ms: @max_exact_ms + 1}, 1)

    assert {:error, "ERR flow run_at_ms exceeds maximum 9007199254740991"} =
             ApplyLimits.validate_flow_time(
               %{records: [%{id: "one", run_at_ms: @max_exact_ms + 1}]},
               1
             )

    assert {:error, "ERR flow step_now_ms exceeds maximum 9007199254740991"} =
             ApplyLimits.validate_flow_time(
               %{records: [%{step_now_ms: @max_exact_ms + 1, step_count: 1}]},
               1
             )
  end

  test "rejects derived deadlines that leave the exact Flow timestamp range" do
    assert {:error, "ERR flow lease_ms deadline exceeds maximum 9007199254740991"} =
             ApplyLimits.validate_flow_time(%{now_ms: @max_exact_ms, lease_ms: 1}, 1)

    assert {:error, "ERR flow ttl_ms deadline exceeds maximum 9007199254740991"} =
             ApplyLimits.validate_flow_time(%{ttl_ms: @max_exact_ms}, 1)

    assert {:error, "ERR flow step_count deadline exceeds maximum 9007199254740991"} =
             ApplyLimits.validate_flow_time(
               %{step_now_ms: @max_exact_ms, step_count: 1},
               1
             )
  end

  test "validates structural shared attributes but leaves user data opaque" do
    assert {:error, "ERR flow now_ms exceeds maximum 9007199254740991"} =
             ApplyLimits.validate_flow_time(
               %{records: [%{id: "one"}], shared: %{now_ms: @max_exact_ms + 1}},
               1
             )

    assert :ok =
             ApplyLimits.validate_flow_time(
               %{
                 now_ms: 1,
                 payload: %{now_ms: @max_exact_ms + 1},
                 attributes: %{"run_at_ms" => @max_exact_ms + 1}
               },
               1
             )
  end

  test "both Flow apply wrappers enforce the replicated time invariant" do
    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore/raft/state_machine/sections/cross_shard_dispatch.ex",
          __DIR__
        )
      )

    assert source
           |> String.split("ApplyLimits.validate_flow_time(attrs, apply_now_ms())")
           |> length() == 3
  end


  test "replicated apply context bounds the total structural batch footprint" do
    state = %{apply_context: ApplyContext.new(flow_max_batch_items: 2)}

    assert :ok = ApplyLimits.validate_flow_batch(state, %{records: [%{}, %{}]})

    assert {:error, "ERR flow batch item count exceeds maximum 2"} =
             ApplyLimits.validate_flow_batch(state, %{records: [%{}, %{}, %{}]})

    assert {:error, "ERR flow batch item count exceeds maximum 2"} =
             ApplyLimits.validate_flow_batch(state, %{records: [%{}, %{}], children: [%{}]})
  end

  test "both Flow apply wrappers enforce the replicated batch invariant" do
    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore/raft/state_machine/sections/cross_shard_dispatch.ex",
          __DIR__
        )
      )

    assert source
           |> String.split("with :ok <- ApplyLimits.validate_flow_batch(state, attrs)")
           |> length() == 3
  end
end
