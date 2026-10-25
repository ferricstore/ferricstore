defmodule Ferricstore.Raft.ApplyProjectionCacheScalingTest do
  @moduledoc """
  The apply-projection cache holds every shard's not-yet-spilled values in one
  table keyed `{root, index, key}`, and readers select by root or by
  `{root, index}`: the Flow LMDB writer looks up an index's entries for each
  value it mirrors. In a hash set each such select scanned the whole table, so
  the mirror slowed down as the cache grew (10k Flow creates took the writers
  over 40 s to absorb). In an ordered set the select walks only the matching
  range.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftSegmentReader

  @table :ferricstore_waraft_apply_projection_cache

  setup do
    dirs =
      for name <- ["big", "probe"] do
        dir =
          Path.join(
            System.tmp_dir!(),
            "apc-scaling-#{name}-#{System.unique_integer([:positive])}"
          )

        File.mkdir_p!(dir)
        dir
      end

    on_exit(fn ->
      for dir <- dirs do
        WARaftSegmentReader.clear_apply_projection_cache(dir, 0)
        File.rm_rf!(dir)
      end
    end)

    %{dirs: dirs}
  end

  test "the cache is ordered by {root, index, key}" do
    assert :ok == WARaftSegmentReader.put_apply_projection(System.tmp_dir!(), 0, 1, [])
    assert :ets.info(@table, :type) == :ordered_set
  end

  test "a per-index lookup does not scan other roots' entries", %{dirs: [big, probe]} do
    small_us =
      median_us(fn -> WARaftSegmentReader.apply_projection_dependency_ready?(probe, 0, 7) end)

    for index <- 1..1_000 do
      entries = for k <- 1..100, do: {"key-#{index}-#{k}", "value", 0}
      assert :ok == WARaftSegmentReader.put_apply_projection(big, 0, index, entries)
    end

    assert WARaftSegmentReader.apply_projection_cache_count(big, 0) == 100_000

    large_us =
      median_us(fn -> WARaftSegmentReader.apply_projection_dependency_ready?(probe, 0, 7) end)

    # A scan of 100,000 rows costs milliseconds against microseconds for a
    # range walk; allow wide noise.
    assert large_us < max(small_us, 50) * 20,
           "lookup took #{large_us} us with 100k other entries vs #{small_us} us without"
  end

  defp median_us(fun) do
    for(_ <- 1..51, do: elem(:timer.tc(fun), 0))
    |> Enum.sort()
    |> Enum.at(25)
  end
end
