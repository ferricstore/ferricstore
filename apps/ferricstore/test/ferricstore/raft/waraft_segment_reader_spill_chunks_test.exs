defmodule Ferricstore.Raft.WARaftSegmentReaderSpillChunksTest do
  @moduledoc """
  A large apply-projection spill is written a bounded number of entries at a
  time, whole Raft indexes per write, so it never holds copies of every
  spilled row at once. Every entry still reaches disk and leaves the cache.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftSegmentReader

  @indexes 40
  @entries_per_index 10
  @chunk_entries 50

  setup do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "segment_reader_spill_chunks_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(data_dir)
    test_pid = self()

    Application.put_env(
      :ferricstore,
      :waraft_apply_projection_spill_chunk_entries,
      @chunk_entries
    )

    Application.put_env(:ferricstore, :waraft_apply_projection_spill_hook, fn batches ->
      send(test_pid, {:spill_write, Enum.map(batches, fn {_pos, entries} -> length(entries) end)})
      :ok
    end)

    on_exit(fn ->
      Application.delete_env(:ferricstore, :waraft_apply_projection_spill_chunk_entries)
      Application.delete_env(:ferricstore, :waraft_apply_projection_spill_hook)
      File.rm_rf(data_dir)
    end)

    for index <- 1..@indexes do
      entries =
        for i <- 1..@entries_per_index, do: {"key-#{index}-#{i}", "value-#{index}-#{i}", 0}

      assert :ok = WARaftSegmentReader.put_apply_projection(data_dir, 0, index, entries)
    end

    {:ok, data_dir: data_dir}
  end

  test "a compaction spill writes whole indexes in bounded chunks", %{data_dir: data_dir} do
    total = @indexes * @entries_per_index

    assert {:ok, ^total} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0, total, 0)
    assert_spilled_in_chunks(data_dir, total)
  end

  test "spilling the whole cache writes in bounded chunks", %{data_dir: data_dir} do
    total = @indexes * @entries_per_index

    assert {:ok, ^total} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)
    assert_spilled_in_chunks(data_dir, total)
  end

  defp assert_spilled_in_chunks(data_dir, total) do
    writes = spill_writes()

    # Whole indexes per write: 50 entries is five indexes of ten.
    assert length(writes) == div(total, @chunk_entries)
    assert Enum.all?(writes, fn sizes -> Enum.sum(sizes) <= @chunk_entries end)
    assert Enum.all?(List.flatten(writes), &(&1 == @entries_per_index))

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0

    ctx = %{data_dir: data_dir}

    for index <- [1, div(@indexes, 2), @indexes], i <- [1, @entries_per_index] do
      assert {:ok, "value-#{index}-#{i}"} ==
               WARaftSegmentReader.read_value_from_location(
                 ctx,
                 0,
                 {:waraft_apply_projection, index},
                 "key-#{index}-#{i}"
               )
    end
  end

  defp spill_writes(acc \\ []) do
    receive do
      {:spill_write, sizes} -> spill_writes([sizes | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
