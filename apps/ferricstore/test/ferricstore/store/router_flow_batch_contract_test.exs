defmodule Ferricstore.Store.RouterFlowBatchContractTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Store.Router

  test "flow create batch dispatches create commands without transition-only matching" do
    ctx = FerricStore.Instance.get(:default)
    id = unique_flow_id("router-create-batch")
    partition_key = unique_flow_id("router-create-partition")

    attrs = %{
      id: id,
      type: "router-create-batch",
      state: "queued",
      partition_key: partition_key,
      run_at_ms: 1_000,
      now_ms: 1_000
    }

    assert [:ok] = Router.flow_create_batch(ctx, [attrs])
    assert {:ok, %{id: ^id}} = FerricStore.flow_get(id, partition_key: partition_key)
  end

  test "flow batch result merge fails every valid slot closed on cardinality mismatch" do
    invalid = {:error, :invalid_input}
    unknown = ErrorReasons.write_timeout_unknown()

    valid = [
      {0, "key-a", {:command, :a}},
      {2, "key-b", {:command, :b}}
    ]

    assert %{0 => ^unknown, 1 => ^invalid, 2 => ^unknown} =
             Router.__merge_flow_batch_results_for_test__(valid, %{1 => invalid}, [:ok])

    assert %{0 => ^unknown, 1 => ^invalid, 2 => ^unknown} =
             Router.__merge_flow_batch_results_for_test__(valid, %{1 => invalid}, [:ok, :ok, :ok])

    assert %{0 => ^unknown, 1 => ^invalid, 2 => ^unknown} =
             Router.__merge_flow_batch_results_for_test__(valid, %{1 => invalid}, :invalid)
  end

  test "flow many result expansion never treats a short group response as success" do
    unknown = ErrorReasons.write_timeout_unknown()

    groups = [
      {0, "key-a", [0, 2], {:command, :a}},
      {1, "key-b", [1], {:command, :b}}
    ]

    assert :ok = Router.__expand_flow_many_results_for_test__(3, groups, [:ok, :ok])

    assert {:ok, [^unknown, ^unknown, ^unknown]} =
             Router.__expand_flow_many_results_for_test__(3, groups, [:ok])

    assert {:ok, [^unknown, ^unknown, ^unknown]} =
             Router.__expand_flow_many_results_for_test__(3, groups, :invalid)
  end

  test "flow pipeline result expansion requires exact outer and inner cardinality" do
    unknown = ErrorReasons.write_timeout_unknown()

    groups = [
      {0, "key-a", [0, 2], {:command, :a}},
      {1, "key-b", [1], {:command, :b}}
    ]

    assert [:ok, :ok, :ok] =
             Router.__expand_flow_pipeline_results_for_test__(
               3,
               %{},
               groups,
               [[:ok, :ok], [:ok]]
             )

    assert [^unknown, ^unknown, ^unknown] =
             Router.__expand_flow_pipeline_results_for_test__(3, %{}, groups, [[:ok, :ok]])

    assert [^unknown, :ok, ^unknown] =
             Router.__expand_flow_pipeline_results_for_test__(
               3,
               %{},
               groups,
               [[:ok], [:ok]]
             )
  end

  test "flow transition result expansion fails mismatched groups closed" do
    unknown = ErrorReasons.write_timeout_unknown()

    groups = [
      {"key-a", [0, 2], {:command, :a}},
      {"key-b", [1], {:command, :b}}
    ]

    assert [:ok, :ok, :ok] =
             Router.__expand_flow_transition_results_for_test__(
               3,
               groups,
               [[:ok, :ok], [:ok]]
             )

    assert [^unknown, ^unknown, ^unknown] =
             Router.__expand_flow_transition_results_for_test__(3, groups, [[:ok, :ok]])

    assert [^unknown, :ok, ^unknown] =
             Router.__expand_flow_transition_results_for_test__(
               3,
               groups,
               [[:ok], [:ok]]
             )
  end
end
