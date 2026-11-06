defmodule Ferricstore.Raft.WARaftSegmentReaderDiskBatchCacheTest do
  @moduledoc """
  One Raft command's apply projection is one batch on disk. Reading many of
  its keys inside `with_disk_batch_cache/1` decodes that batch once instead of
  once per key; outside it every read still goes to disk.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftSegmentReader

  @index 7
  @keys 200

  setup do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "segment_reader_batch_cache_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(data_dir)
    on_exit(fn -> File.rm_rf(data_dir) end)

    now_ms = System.os_time(:millisecond)

    entries =
      for i <- 1..@keys do
        # Every tenth key has already expired, so live and including-expired
        # reads must still differ through the cache.
        expire_at_ms = if rem(i, 10) == 0, do: now_ms - 1_000, else: 0
        {"key-#{i}", "value-#{i}", expire_at_ms}
      end

    assert :ok = WARaftSegmentReader.put_apply_projection(data_dir, 0, @index, entries)
    assert {:ok, @keys} = WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

    Process.put(:ferricstore_waraft_apply_projection_disk_read_hook, fn _root, index, source ->
      send(self(), {:disk_read, index, source})
      :ok
    end)

    on_exit(fn -> Process.delete(:ferricstore_waraft_apply_projection_disk_read_hook) end)

    {:ok, ctx: %{data_dir: data_dir}, entries: entries}
  end

  test "reads of one spilled batch decode it once inside the scope", %{ctx: ctx, entries: entries} do
    location = {:waraft_apply_projection, @index}

    uncached = read_all(ctx, location, entries)
    # Every read decodes the batch again: two reads per key, plus a merged-log
    # read for each expired key's live read.
    assert %{latest: latest} = disk_reads()
    assert latest == 2 * @keys

    cached = WARaftSegmentReader.with_disk_batch_cache(fn -> read_all(ctx, location, entries) end)
    assert disk_reads() == %{latest: 1, merged: 1}
    assert cached == uncached

    for {key, value, expire_at_ms} <- entries do
      expected = if expire_at_ms == 0, do: {:ok, value}, else: :not_found
      assert {^key, ^expected, {:ok, ^value}} = List.keyfind(cached, key, 0)
    end

    # The cache ends with the scope.
    assert {:ok, "value-1"} =
             WARaftSegmentReader.read_value_from_location(ctx, 0, location, "key-1")

    assert disk_reads() == %{latest: 1}
  end

  test "a key the batch does not hold is still not found inside the scope", %{ctx: ctx} do
    location = {:waraft_apply_projection, @index}

    WARaftSegmentReader.with_disk_batch_cache(fn ->
      assert {:ok, "value-1"} =
               WARaftSegmentReader.read_value_from_location(ctx, 0, location, "key-1")

      assert :not_found =
               WARaftSegmentReader.read_value_from_location(ctx, 0, location, "missing")
    end)
  end

  defp read_all(ctx, location, entries) do
    Enum.map(entries, fn {key, _value, _expire_at_ms} ->
      {key, WARaftSegmentReader.read_value_from_location(ctx, 0, location, key),
       WARaftSegmentReader.read_value_from_location_including_expired(ctx, 0, location, key)}
    end)
  end

  defp disk_reads(counts \\ %{}) do
    receive do
      {:disk_read, @index, source} -> disk_reads(Map.update(counts, source, 1, &(&1 + 1)))
    after
      0 -> counts
    end
  end
end
