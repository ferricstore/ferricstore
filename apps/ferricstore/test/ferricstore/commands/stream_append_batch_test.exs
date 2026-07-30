defmodule Ferricstore.Commands.StreamAppendBatchTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Stream.AppendBatch

  test "groups interleaved auto-ID appends by topic and retains global positions" do
    commands = [
      {:stream_append, "stream:a", :auto, ["field", "a1"], nil, false},
      {:stream_append, "stream:b", :auto, ["field", "b1"], nil, false},
      {:stream_append, "stream:a", :auto, ["field", "a2"], nil, false}
    ]

    assert {:ok,
            [
              {"stream:a", [["field", "a1"], ["field", "a2"]], [0, 2]},
              {"stream:b", [["field", "b1"]], [1]}
            ], 3} = AppendBatch.group_commands(commands)

    assert :generic = AppendBatch.group_commands([])
    assert :generic = AppendBatch.group_commands([{:put, "key", "value", 0}])
  end

  test "validates an exact position permutation" do
    groups = [
      {"stream:a", [["field", "a1"], ["field", "a2"]], [0, 2]},
      {"stream:b", [["field", "b1"]], [1]}
    ]

    assert :ok = AppendBatch.validate_groups(groups, 3)

    assert {:ok, %{command_items: 9, compound_members: 3, replies: 3}} =
             AppendBatch.validate_groups_with_work(groups, 3)

    assert {:ok, %{command_items: 5, compound_members: 1, replies: 1}} =
             AppendBatch.validate_groups_with_work(
               [{"stream:a", [["field-1", "value-1", "field-2", "value-2"]], [0]}],
               1
             )

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups(
               [{"stream:a", [["field", "a1"], ["field", "a2"]], [0, 0]}],
               2
             )

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups([{"stream:a", [["field", "a1"]], [1]}], 1)

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups([{"stream:a", [["field", "a1"]], [0, 1]}], 2)

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups(
               [
                 {"stream:a", [["field", "a1"]], [0]},
                 {"stream:a", [["field", "a2"]], [1]}
               ],
               2
             )

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups([], 0)

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups_with_work(
               [{"stream:a", [["field" | :improper]], [0]}],
               1
             )

    for fields <- [
          [],
          ["orphan"],
          ["field", "value", "orphan"],
          [123, "value"],
          ["field", :value]
        ] do
      assert {:error, :invalid_stream_append_grouped_auto} =
               AppendBatch.validate_groups_with_work(
                 [{"stream:a", [fields], [0]}],
                 1
               )
    end
  end

  test "expands groups back into original command order" do
    groups = [
      {"stream:a", [["field", "a1"], ["field", "a2"]], [0, 2]},
      {"stream:b", [["field", "b1"]], [1]}
    ]

    assert {:ok,
            [
              {:stream_append, "stream:a", :auto, ["field", "a1"], nil, false},
              {:stream_append, "stream:b", :auto, ["field", "b1"], nil, false},
              {:stream_append, "stream:a", :auto, ["field", "a2"], nil, false}
            ]} = AppendBatch.expand_groups(groups, 3)
  end

  test "validates position permutations above the bitset bound" do
    count = 4097
    fields = List.duplicate(["field", "value"], count)
    positions = Enum.to_list(0..(count - 1))

    assert {:ok, %{compound_members: ^count, replies: ^count}} =
             AppendBatch.validate_groups_with_work(
               [{"stream:large", fields, positions}],
               count
             )

    assert {:error, :invalid_stream_append_grouped_auto} =
             AppendBatch.validate_groups_with_work(
               [{"stream:large", fields, [0 | Enum.to_list(0..(count - 2))]}],
               count
             )
  end
end
