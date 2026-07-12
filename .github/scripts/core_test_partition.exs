Code.require_file("core_test_partition.ex", __DIR__)

case System.argv() do
  [root, timings_path, partition_count_text, partition_index_text | excluded_files] ->
    with {partition_count, ""} <- Integer.parse(partition_count_text),
         {partition_index, ""} <- Integer.parse(partition_index_text) do
      root
      |> Ferricstore.CI.TestPartition.selected_files(
        timings_path,
        partition_count,
        partition_index,
        excluded_files
      )
      |> Enum.each(&IO.puts/1)
    else
      _other -> raise ArgumentError, "partition count and index must be integers"
    end

  _other ->
    IO.puts(
      :stderr,
      "usage: core_test_partition.exs ROOT TIMINGS PARTITION_COUNT PARTITION_INDEX [EXCLUDED_FILE ...]"
    )

    System.halt(64)
end
