defmodule Ferricstore.Raft.ApplyWorkTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.{ApplyContext, ApplyWork}

  test "admit_items enforces the replicated budget without accepting improper lists" do
    context = ApplyContext.new(batch_command_apply_budget: 2)

    assert {:ok, 2} = ApplyWork.admit_items(context, [:first, :second])

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_items(context, [:first, :second, :third])

    assert {:error, :invalid_batch_command_list} =
             ApplyWork.admit_items(context, [:first | :invalid_tail])
  end

  test "normalize_batch expands nested batch commands under one cumulative budget" do
    context =
      ApplyContext.new(
        batch_command_apply_budget: 3,
        compound_member_apply_budget: 3
      )

    commands = [
      {:put_batch, [{"first", "1", nil}, {"second", "2", nil}]},
      {:batch, [{:delete_batch, ["third"]}]}
    ]

    assert {:ok,
            [
              {:put, "first", "1", nil},
              {:put, "second", "2", nil},
              {:delete, "third"}
            ], 3} = ApplyWork.normalize_batch(context, commands)

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.normalize_batch(context, commands ++ [{:delete, "fourth"}])
  end

  test "admit_command follows wrappers and accounts for compound members" do
    context =
      ApplyContext.new(
        batch_command_apply_budget: 2,
        compound_member_apply_budget: 1
      )

    wrapped =
      {:ferricstore_latency_trace,
       {:async, self(), {:compound_batch_put, "hash", [{"field", "value"}]}}}

    assert :ok = ApplyWork.admit_command(context, wrapped)

    assert {:error, :compound_member_apply_budget_exceeded} =
             ApplyWork.admit_command(
               context,
               {:compound_batch_put, "hash", [{"first", "1"}, {"second", "2"}]}
             )
  end

  test "paired commands reject mismatched cardinality before apply" do
    context = ApplyContext.new(batch_command_apply_budget: 4)

    assert {:error, :batch_pair_cardinality_mismatch} =
             ApplyWork.admit_command(
               context,
               {:pfmerge, "dest", ["source"], [:first_sketch, :extra_sketch]}
             )

    assert {:error, :batch_pair_cardinality_mismatch} =
             ApplyWork.admit_command(
               context,
               {:cms_merge, "dest", ["source"], [1, 2], nil}
             )
  end
end
