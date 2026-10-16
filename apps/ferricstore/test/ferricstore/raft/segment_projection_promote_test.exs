defmodule Ferricstore.Raft.SegmentProjectionPromoteTest do
  # The rewrite hook is node-global application env.
  use ExUnit.Case, async: false

  @log :ferricstore_waraft_spike_segment_log

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "segment_projection_promote_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)

    %{
      checkpoint: String.to_charlist(Path.join(root, "segment_projection_checkpoint_log")),
      projection: String.to_charlist(Path.join(root, "segment_projection_log"))
    }
  end

  test "the checkpoint becomes the projection without a rewrite", context do
    %{checkpoint: checkpoint, projection: projection} = context
    checkpoint_entries = entries("new", 50)
    assert :ok = @log.write_projection(projection, {:raft_log_pos, 10, 1}, entries("old", 20))
    assert :ok = @log.write_projection(checkpoint, {:raft_log_pos, 90, 1}, checkpoint_entries)

    checkpoint_locations = locations(checkpoint, length(checkpoint_entries))
    segment_files = files(checkpoint)

    assert :ok = @log.promote_projection(checkpoint, projection)

    refute File.exists?(Path.join(checkpoint, "segment_log"))
    assert files(projection) == segment_files
    assert locations(projection, length(checkpoint_entries)) == checkpoint_locations
    assert projection_contents(projection) == {{:raft_log_pos, 90, 1}, checkpoint_entries}
  end

  test "promotion creates a projection that did not exist yet", context do
    %{checkpoint: checkpoint, projection: projection} = context
    assert :ok = @log.write_projection(checkpoint, {:raft_log_pos, 7, 1}, entries("only", 5))

    assert :ok = @log.promote_projection(checkpoint, projection)
    assert projection_contents(projection) == {{:raft_log_pos, 7, 1}, entries("only", 5)}
  end

  test "an interrupted promotion keeps the old projection", context do
    %{checkpoint: checkpoint, projection: projection} = context
    assert :ok = @log.write_projection(projection, {:raft_log_pos, 10, 1}, entries("old", 20))
    assert :ok = @log.write_projection(checkpoint, {:raft_log_pos, 90, 1}, entries("new", 50))

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_rewrite_hook,
      {:fail_once_after_live_backup, self()}
    )

    on_exit(fn -> Application.delete_env(:ferricstore, :waraft_segment_log_rewrite_hook) end)

    assert {:error, {:rewrite_hook, :after_live_backup}} =
             @log.promote_projection(checkpoint, projection)

    assert_received {:waraft_segment_log_rewrite_hook, :after_live_backup}
    assert projection_contents(projection) == {{:raft_log_pos, 10, 1}, entries("old", 20)}
    assert locations(projection, 20) |> Enum.all?(&match?({:ok, _}, &1))
  end

  test "a source without trusted offsets is not adopted", context do
    %{checkpoint: checkpoint, projection: projection} = context
    File.mkdir_p!(Path.join(checkpoint, "segment_log"))

    assert {:error, {:projection_source_offsets_missing, _source}} =
             @log.promote_projection(checkpoint, projection)
  end

  defp entries(prefix, count),
    do: for(i <- 1..count, do: {"#{prefix}-#{String.pad_leading("#{i}", 4, "0")}", "v#{i}", 0})

  defp locations(dir, count),
    do: for(index <- 1..count, do: @log.location_for_index(dir, index))

  defp files(dir) do
    segment_dir = Path.join(dir, "segment_log")

    segment_dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".seg"))
    |> Map.new(&{&1, File.stat!(Path.join(segment_dir, &1)).inode})
  end

  defp projection_contents(dir) do
    {:ok, {position, entries}} =
      @log.fold_disk(
        dir,
        fn
          0, {0, {:ferricstore_segment_projection_header, position, _count}}, {_, entries} ->
            {position, entries}

          _index,
          {0, {:ferricstore_segment_projection_entry, key, value, expire}},
          {position, entries} ->
            {position, [{key, value, expire} | entries]}
        end,
        {nil, []}
      )

    {position, Enum.reverse(entries)}
  end
end
