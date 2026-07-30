defmodule Ferricstore.Raft.ApplyWorkTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Stream.AppendBatch
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

  test "compact stream append batches retain per-entry work and reply budgets" do
    command =
      {:stream_append_many_auto, "stream",
       [
         ["field", "first"],
         ["field", "second"]
       ]}

    exact =
      ApplyContext.new(
        batch_command_apply_budget: 4,
        compound_member_apply_budget: 2
      )

    assert {:ok,
            %{
              command_items: 4,
              compound_members: 2,
              visits: 1,
              replies: 2
            }} = ApplyWork.batch_usage(exact, [command])

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_command(
               ApplyContext.new(
                 batch_command_apply_budget: 3,
                 compound_member_apply_budget: 2
               ),
               command
             )

    assert {:error, :compound_member_apply_budget_exceeded} =
             ApplyWork.admit_command(
               ApplyContext.new(
                 batch_command_apply_budget: 4,
                 compound_member_apply_budget: 1
               ),
               command
             )

    assert {:ok, 2} = ApplyWork.admit_stream_many(exact, elem(command, 2))

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_stream_many(
               ApplyContext.new(
                 batch_command_apply_budget: 3,
                 compound_member_apply_budget: 2
               ),
               elem(command, 2)
             )

    assert {:error, :compound_member_apply_budget_exceeded} =
             ApplyWork.admit_stream_many(
               ApplyContext.new(
                 batch_command_apply_budget: 4,
                 compound_member_apply_budget: 1
               ),
               elem(command, 2)
             )
  end

  test "fused compact stream admission retains malformed-command precedence" do
    context =
      ApplyContext.new(
        batch_command_apply_budget: 2,
        compound_member_apply_budget: 1
      )

    assert {:ok, 0} = ApplyWork.admit_stream_many(context, [])

    assert {:error, :invalid_stream_append_many_auto} =
             ApplyWork.admit_stream_many(context, [[]])

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_stream_many(context, [[], ["one", "two"]])

    assert {:error, :invalid_batch_command_list} =
             ApplyWork.admit_stream_many(context, [["field" | :improper]])
  end

  test "compact stream admission rejects invalid field-pair schemas" do
    context =
      ApplyContext.new(
        batch_command_apply_budget: 16,
        compound_member_apply_budget: 4
      )

    for fields_lists <- [
          [["orphan"]],
          [["field", "value", "orphan"]],
          [[123, "value"]],
          [["field", :value]]
        ] do
      assert {:error, :invalid_stream_append_many_auto} =
               ApplyWork.admit_stream_many(context, fields_lists)

      assert {:error, :invalid_stream_append_many_auto} =
               ApplyWork.admit_command(
                 context,
                 {:stream_append_many_auto, "stream", fields_lists}
               )
    end
  end

  test "fused compact stream admission honors exact common field-shape budgets" do
    one_pair = [["field", "value"]]
    two_pairs = [["field-1", "value-1", "field-2", "value-2"]]

    assert {:ok, 1} =
             ApplyWork.admit_stream_many(
               ApplyContext.new(batch_command_apply_budget: 2),
               one_pair
             )

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_stream_many(
               ApplyContext.new(batch_command_apply_budget: 1),
               one_pair
             )

    assert {:ok, 1} =
             ApplyWork.admit_stream_many(
               ApplyContext.new(batch_command_apply_budget: 4),
               two_pairs
             )

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_stream_many(
               ApplyContext.new(batch_command_apply_budget: 3),
               two_pairs
             )
  end

  test "compact stream append admits exactly the configured member boundary" do
    entry = ["field", "value"]

    exact_context =
      ApplyContext.new(
        batch_command_apply_budget: 8_192,
        compound_member_apply_budget: 4_096
      )

    assert :ok =
             ApplyWork.admit_command(
               exact_context,
               {:stream_append_many_auto, "stream", List.duplicate(entry, 4_096)}
             )

    over_context =
      ApplyContext.new(
        batch_command_apply_budget: 8_194,
        compound_member_apply_budget: 4_096
      )

    assert {:error, :compound_member_apply_budget_exceeded} =
             ApplyWork.admit_command(
               over_context,
               {:stream_append_many_auto, "stream", List.duplicate(entry, 4_097)}
             )
  end

  test "grouped stream append charges fields, positions, members, and replies" do
    command =
      {:stream_append_grouped_auto,
       [
         {"stream:a", [["field", "first"]], [0]},
         {"stream:b", [["field", "second"]], [1]}
       ], 2}

    exact =
      ApplyContext.new(
        batch_command_apply_budget: 6,
        compound_member_apply_budget: 2
      )

    assert {:ok,
            %{
              command_items: 6,
              compound_members: 2,
              visits: 1,
              replies: 2
            }} = ApplyWork.batch_usage(exact, [command])

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_command(
               ApplyContext.new(
                 batch_command_apply_budget: 5,
                 compound_member_apply_budget: 2
               ),
               command
             )

    work = %{command_items: 6, compound_members: 2, replies: 2}
    assert :ok = ApplyWork.admit_grouped_stream_work(exact, work)

    assert {:error, :batch_command_apply_budget_exceeded} =
             ApplyWork.admit_grouped_stream_work(
               ApplyContext.new(
                 batch_command_apply_budget: 5,
                 compound_member_apply_budget: 2
               ),
               work
             )

    assert {:error, :compound_member_apply_budget_exceeded} =
             ApplyWork.admit_grouped_stream_work(
               ApplyContext.new(
                 batch_command_apply_budget: 6,
                 compound_member_apply_budget: 1
               ),
               work
             )
  end

  test "fused grouped stream validation rejects invalid field-pair schemas" do
    groups = [
      {"stream:a", [[], ["one"], ["one", "two", "three"]], [2, 0, 1]}
    ]

    command = {:stream_append_grouped_auto, groups, 3}
    context = ApplyContext.new(batch_command_apply_budget: 8, compound_member_apply_budget: 3)

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups_with_work(groups, 3)

    assert {:error, :invalid_stream_append_grouped_auto} =
             ApplyWork.batch_usage(context, [command])
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
