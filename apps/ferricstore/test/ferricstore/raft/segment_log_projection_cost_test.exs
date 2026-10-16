defmodule Ferricstore.Raft.SegmentLogProjectionCostTest do
  # Uses call-count tracing, which is global per function.
  use ExUnit.Case, async: false

  @log :ferricstore_waraft_spike_segment_log

  test "rewriting a segment projection does not redo path work for every record" do
    # A log trim rewrites the shard's whole segment projection inside the WARaft
    # commit path. Splitting and re-deriving the same directory path for every
    # record made that rewrite dominate trim stalls.
    root =
      Path.join(
        System.tmp_dir!(),
        "segment_projection_cost_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)

    records = 5_000
    entries = for i <- 1..records, do: {"key-#{i}", "value", 0}

    path_calls =
      count_calls([{:filename, :split, 1}, {:filename, :dirname, 1}], fn ->
        assert :ok =
                 @log.write_projection(
                   String.to_charlist(root),
                   {:raft_log_pos, records, 1},
                   entries
                 )
      end)

    assert path_calls < div(records, 10),
           "#{path_calls} filename split/dirname calls for #{records} records"
  end

  test "rewriting a segment projection reads its files back in buffered chunks" do
    # The rewrite re-scans what it wrote to rebuild offsets. Unbuffered, each
    # record cost a header read and a payload read, each a system call.
    root =
      Path.join(
        System.tmp_dir!(),
        "segment_projection_reads_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)

    records = 5_000
    entries = for i <- 1..records, do: {"key-#{i}", "value", 0}

    file_reads =
      count_calls([{:prim_file, :read_nif, 2}], fn ->
        assert :ok =
                 @log.write_projection(
                   String.to_charlist(root),
                   {:raft_log_pos, records, 1},
                   entries
                 )
      end)

    assert file_reads < div(records, 10), "#{file_reads} file reads for #{records} records"
  end

  test "scans outside an offset rebuild keep unbuffered reads" do
    # A reader can scan a log while its writer appends. macOS can report a new
    # file size before the appended bytes are readable, and a read-ahead buffer
    # filled early would hold those unwritten bytes and fail the record CRC.
    # Only offset rebuilds, where nothing appends, may buffer.
    root =
      Path.join(
        System.tmp_dir!(),
        "segment_projection_live_reads_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)

    records = 500
    entries = for i <- 1..records, do: {"key-#{i}", "value", 0}
    dir = String.to_charlist(root)
    assert :ok = @log.write_projection(dir, {:raft_log_pos, records, 1}, entries)

    file_reads =
      count_calls([{:prim_file, :read_nif, 2}], fn ->
        assert {:ok, folded} = @log.fold_disk(dir, fn _index, _entry, acc -> acc + 1 end, 0)
        assert folded > 0
      end)

    assert file_reads >= records, "#{file_reads} file reads for #{records} records"
  end

  # Count only this process and what it spawns: the application's own
  # background work in the test VM would otherwise add noise.
  defp count_calls(mfas, fun) do
    :erlang.trace(self(), true, [:call, :set_on_spawn])
    Enum.each(mfas, &:erlang.trace_pattern(&1, true, [:call_count]))

    try do
      fun.()

      Enum.reduce(mfas, 0, fn mfa, acc ->
        {:call_count, count} = :erlang.trace_info(mfa, :call_count)
        acc + (count || 0)
      end)
    after
      Enum.each(mfas, &:erlang.trace_pattern(&1, false, [:call_count]))
      :erlang.trace(self(), false, [:call, :set_on_spawn])
    end
  end
end
