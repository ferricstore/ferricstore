defmodule Ferricstore.Store.ZSetTreeNifTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  @moduletag :tmp_dir

  @nx 1
  @xx 2
  @gt 4

  test "writes, reads and replays through the NIFs", %{tmp_dir: dir} do
    path = Path.join(dir, "tree")

    assert {:ok, 0} = NIF.zset_tree_card(path)
    assert {:ok, []} = NIF.zset_tree_range_by_index(path, 0, -1, false)

    assert {:ok, [{true, true}, {true, true}, {true, true}, {false, false}]} =
             NIF.zset_tree_apply(path, 1, 1, [
               {:add, "b", 2.0, 0},
               {:add, "a", 1.0, 0},
               {:add, "c", 3.0, 0},
               {:add, "a", 9.0, @nx}
             ])

    # A replayed entry gives the outcome it had the first time.
    assert {:ok, [{true, true}, {true, true}, {true, true}, {false, false}]} =
             NIF.zset_tree_apply(path, 1, 1, [
               {:add, "b", 2.0, 0},
               {:add, "a", 1.0, 0},
               {:add, "c", 3.0, 0},
               {:add, "a", 9.0, @nx}
             ])

    assert {:ok, [{false, false}, 5.5, nil, true, [{"c", 3.0}]]} =
             NIF.zset_tree_apply(path, 2, 1, [
               {:add, "missing", 1.0, @xx},
               {:incr, "a", 4.5, 0},
               {:incr, "b", -1.0, @gt},
               {:remove, "b"},
               {:pop_min, 1}
             ])

    assert {:ok, 1} = NIF.zset_tree_card(path)
    assert {:ok, [5.5, nil]} = NIF.zset_tree_scores(path, ["a", "nope"])
    assert {:ok, 0} = NIF.zset_tree_rank(path, "a", false)
    assert {:ok, nil} = NIF.zset_tree_rank(path, "nope", true)

    members = for i <- 1..500, do: {:add, "m#{String.pad_leading("#{i}", 3, "0")}", 0.0, 0}
    assert {:ok, _} = NIF.zset_tree_apply(path, 3, 1, members)

    assert {:ok, [{"m498", +0.0}, {"m499", +0.0}, {"m500", +0.0}]} =
             NIF.zset_tree_range_by_index(path, -4, -2, false)

    assert {:ok, 500} = NIF.zset_tree_count_by_score(path, {:incl, 0.0}, {:excl, 1.0})
    assert {:ok, 2} = NIF.zset_tree_count_by_lex(path, {:excl, "m001"}, {:incl, "m003"})

    assert {:ok, [{"a", 5.5}, {"m500", +0.0}]} =
             NIF.zset_tree_range_by_score(path, :neg_inf, :inf, true, 0, 2)

    assert {:ok, [{"m002", +0.0}, {"m003", +0.0}]} =
             NIF.zset_tree_range_by_lex(path, {:excl, "m001"}, :max, false, 0, 2)

    assert {:ok, [{"m001", +0.0}]} = NIF.zset_tree_at_positions(path, [0])
    assert {:ok, {[{"a", 5.5}, {"m001", +0.0}], "m001"}} = NIF.zset_tree_scan(path, nil, 2)
    assert {:ok, {[], nil}} = NIF.zset_tree_scan(path, "zzz", 2)

    assert :ok = NIF.zset_tree_sync(path)
    assert {:ok, :uncovered} = NIF.zset_tree_compact(path, 2)
    assert {:ok, {before, after_bytes}} = NIF.zset_tree_compact(path, 3)
    assert after_bytes < before
    assert {:ok, {_frames, 0, 501}} = NIF.zset_tree_recover(path)
    assert {:ok, 501} = NIF.zset_tree_card(path)
  end

  test "an overflowing increment is refused and changes nothing", %{tmp_dir: dir} do
    path = Path.join(dir, "tree")
    max = 1.7976931348623157e308
    assert {:ok, _} = NIF.zset_tree_apply(path, 1, 1, [{:add, "x", max, 0}])
    assert {:error, _} = NIF.zset_tree_apply(path, 2, 1, [{:incr, "x", max, 0}])
    assert {:ok, [^max]} = NIF.zset_tree_scores(path, ["x"])
  end

  test "bad terms raise badarg", %{tmp_dir: dir} do
    path = Path.join(dir, "tree")
    assert_raise ArgumentError, fn -> NIF.zset_tree_apply(path, 1, 1, [{:add, "x"}]) end
    assert_raise ArgumentError, fn -> NIF.zset_tree_apply(path, 1, 1, [{:add, "x", 1.0, 99}]) end
    assert_raise ArgumentError, fn -> NIF.zset_tree_count_by_score(path, {:incl, "x"}, :inf) end
  end
end
