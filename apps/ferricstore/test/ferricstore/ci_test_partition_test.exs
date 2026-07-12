Code.require_file(Path.expand("../../../../.github/scripts/core_test_partition.ex", __DIR__))

defmodule Ferricstore.CI.TestPartitionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CI.TestPartition

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-ci-partition-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "longest-processing-time allocation is complete balanced and deterministic", %{root: root} do
    files =
      for {name, weight} <- Enum.zip(~w(a b c d e f), [9_000, 8_000, 7_000, 6_000, 5_000, 4_000]) do
        file = write_test_file(root, name, 1)
        {file, weight}
      end

    timings = Map.new(files)
    paths = Enum.map(files, &elem(&1, 0))
    plan = TestPartition.plan(Enum.reverse(paths), timings, 3)

    assert Enum.map(plan, & &1.weight_ms) == [13_000, 13_000, 13_000]
    assert plan == TestPartition.plan(paths, timings, 3)

    assert plan
           |> Enum.flat_map(& &1.files)
           |> Enum.sort() == Enum.sort(paths)

    for {heavy, _weight} <- Enum.take(files, 3) do
      assert Enum.count(plan, &(heavy in &1.files)) == 1
    end
  end

  test "selection excludes dedicated files and weights new files by source size", %{root: root} do
    timed = write_test_file(root, "timed", 1)
    fallback = write_test_file(root, "fallback", 300)
    excluded = write_test_file(root, "excluded", 100)
    timings_path = Path.join(root, "timings.tsv")
    File.write!(timings_path, "5000\t#{timed}\n")

    selected =
      for partition <- 1..2,
          file <- TestPartition.selected_files(root, timings_path, 2, partition, [excluded]),
          do: file

    assert Enum.sort(selected) == Enum.sort([timed, fallback])
    refute excluded in selected

    assert [5_000, 1_500] ==
             root
             |> Path.join("*_test.exs")
             |> Path.wildcard()
             |> Enum.reject(&(&1 == excluded))
             |> TestPartition.plan(TestPartition.load_timings!(timings_path), 2)
             |> Enum.map(& &1.weight_ms)
             |> Enum.sort(:desc)

    assert_raise ArgumentError, fn ->
      TestPartition.selected_files(root, timings_path, 2, 0, [])
    end
  end

  defp write_test_file(root, name, line_count) do
    path = Path.join(root, "#{name}_test.exs")
    File.write!(path, String.duplicate("# test weight\n", line_count))
    path
  end
end
