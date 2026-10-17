defmodule Ferricstore.Raft.SegmentProjectionRelocationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.WARaftStorage

  # Trims and checkpoints repoint keydir rows while storage keeps applying
  # writes to the same table. A write that lands between reading a row and
  # repointing it must survive.
  test "relocation never overwrites a row that a concurrent write replaced" do
    keydir = :ets.new(:relocation_race_keydir, [:set, :public, {:write_concurrency, true}])
    keys = for i <- 1..2_000, do: "relocation-race:#{i}"

    lost =
      for round <- 1..200, reduce: 0 do
        lost ->
          old_location = {:waraft_segment, round}
          originals = for key <- keys, do: {key, nil, 0, 0, old_location, round, 8}
          true = :ets.insert(keydir, originals)
          parent = self()

          writer =
            spawn_link(fn ->
              receive do
                :go -> :ok
              end

              # Same per-key work as the relocation, so the two stay in step.
              for key <- keys do
                [_row] = :ets.lookup(keydir, key)
                true = :ets.insert(keydir, {key, "new", 0, 0, {:waraft_segment, -round}, 0, 3})
              end

              send(parent, {:written, self()})
            end)

          send(writer, :go)

          for row <- originals do
            WARaftStorage.__relocate_segment_projection_row_for_test__(keydir, row, round, 0, 8)
          end

          assert_receive {:written, ^writer}, 5_000

          lost +
            :ets.select_count(keydir, [
              {{:_, :_, :_, :_, {:waraft_projection, :_}, :_, :_}, [], [true]}
            ])
      end

    assert lost == 0
  end

  test "relocation repoints an unchanged row and keeps its cached value" do
    keydir = :ets.new(:relocation_keydir, [:set, :public])
    row = {"cached", nil, 0, 3, {:waraft_segment, 4}, 11, 8}
    true = :ets.insert(keydir, row)

    # A cold read warmed the row after the relocation captured it.
    true = :ets.insert(keydir, {"cached", "value", 0, 7, {:waraft_segment, 4}, 11, 8})

    WARaftStorage.__relocate_segment_projection_row_for_test__(keydir, row, 9, 120, 5)

    assert [{"cached", "value", 0, 7, {:waraft_projection, 9}, 120, 5}] =
             :ets.lookup(keydir, "cached")
  end

  test "relocation leaves a row alone once its location or expiry changed" do
    keydir = :ets.new(:relocation_changed_keydir, [:set, :public])
    moved = {"moved", nil, 0, 0, {:waraft_segment, 4}, 11, 8}
    expired = {"expired", nil, 100, 0, {:waraft_segment, 4}, 12, 8}
    hot = {"hot", "old", 0, 0, {:waraft_segment, 4}, 13, 8}
    true = :ets.insert(keydir, [moved, expired, hot])

    true =
      :ets.insert(keydir, [
        {"moved", nil, 0, 0, {:waraft_segment, 6}, 0, 8},
        {"expired", nil, 200, 0, {:waraft_segment, 4}, 12, 8},
        {"hot", "newer", 0, 0, {:waraft_segment, 4}, 13, 8}
      ])

    for row <- [moved, expired, hot] do
      WARaftStorage.__relocate_segment_projection_row_for_test__(keydir, row, 9, 120, 5)
    end

    assert [{"moved", nil, 0, 0, {:waraft_segment, 6}, 0, 8}] = :ets.lookup(keydir, "moved")

    assert [{"expired", nil, 200, 0, {:waraft_segment, 4}, 12, 8}] =
             :ets.lookup(keydir, "expired")

    assert [{"hot", "newer", 0, 0, {:waraft_segment, 4}, 13, 8}] = :ets.lookup(keydir, "hot")
  end
end
