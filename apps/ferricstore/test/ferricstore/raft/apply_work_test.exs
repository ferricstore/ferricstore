defmodule Ferricstore.Raft.ApplyWorkTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.{ApplyContext, ApplyWork, CommandStamp}
  alias Ferricstore.Store.CompoundKey

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

  test "batch_usage separates reply width from replicated apply work" do
    context =
      ApplyContext.new(
        batch_command_apply_budget: 4,
        compound_member_apply_budget: 4
      )

    assert {:ok,
            %{
              command_items: 3,
              compound_members: 0,
              visits: 1,
              replies: 1
            }} =
             ApplyWork.batch_usage(
               context,
               [{:mset, [{"first", "1", nil}, {"second", "2", nil}, {"third", "3", nil}]}]
             )

    assert {:ok,
            %{
              command_items: 2,
              compound_members: 2,
              visits: 2,
              replies: 2
            }} =
             ApplyWork.batch_usage(
               context,
               [
                 {:zadd_single, "zset", 1.0, "first"},
                 {:zadd_single, "zset", 2.0, "second"}
               ]
             )
  end

  test "wrapped expanded batches share the outer command budget" do
    limited = ApplyContext.new(batch_command_apply_budget: 3)

    commands = [
      {:ferricstore_latency_trace, {:put_batch, [{"first", "1", nil}, {"second", "2", nil}]}},
      {:ferricstore_latency_trace, {:put_batch, [{"third", "3", nil}, {"fourth", "4", nil}]}}
    ]

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.batch_usage(limited, commands)

    exact = ApplyContext.new(batch_command_apply_budget: 4)

    assert {:ok,
            %{
              command_items: 4,
              compound_members: 0,
              replies: 2,
              visits: 6
            }} = ApplyWork.batch_usage(exact, commands)

    assert :ok = ApplyWork.admit_command(exact, hd(commands))

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_command(
               ApplyContext.new(batch_command_apply_budget: 1),
               hd(commands)
             )
  end

  test "wrapped empty expanded commands still consume one command item" do
    context = ApplyContext.new(batch_command_apply_budget: 1)

    for inner <- [
          {:batch, []},
          {:put_batch, []},
          {:put_blob_batch, []},
          {:delete_batch, []}
        ] do
      wrapped = {:ferricstore_latency_trace, inner}

      assert {:ok, %{command_items: 1, replies: 1}} =
               ApplyWork.batch_usage(context, [wrapped])

      assert :ok = ApplyWork.admit_command(context, wrapped)
    end

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.batch_usage(
               context,
               [
                 {:ferricstore_latency_trace, {:batch, []}},
                 {:ferricstore_latency_trace, {:batch, []}}
               ]
             )
  end

  test "expiry batches share compound work across one replicated batch" do
    context =
      ApplyContext.new(
        batch_command_apply_budget: 4,
        compound_member_apply_budget: 1
      )

    commands = [
      {:expire_if_batch, [{CompoundKey.hash_field("first", "field"), 1}]},
      {:expire_if_batch, [{CompoundKey.hash_field("second", "field"), 1}]}
    ]

    assert {:error, :compound_member_apply_budget_exceeded} =
             ApplyWork.batch_usage(context, commands)

    assert {:error, :invalid_expire_if_batch_entry} =
             ApplyWork.batch_usage(context, [{:expire_if_batch, [:malformed]}])
  end

  test "final sanitized TTB commands are admitted by decoded work" do
    context = ApplyContext.new(batch_command_apply_budget: 1)

    preencoded =
      CommandStamp.to_ttb({:delete_batch, ["first", "second"]})

    sanitized = ApplyContext.wrap_command(preencoded, context)

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_command(context, sanitized)
  end
end
