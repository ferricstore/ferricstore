defmodule Ferricstore.Store.ZSetTreeIntegrationTest do
  @moduledoc """
  From feature level 9 a sorted set past the promotion threshold lives in its
  own counted tree file instead of the keydir. Every command must answer as it
  did for a set promoted before level 9.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 900_000

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.{CompoundKey, Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  @threshold 16

  setup do
    isolated = ShardHelpers.setup_isolated_data_dir()
    snapshot = ShardHelpers.replace_default_apply_context(promotion_threshold: @threshold)

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(snapshot)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    {:ok, ctx: FerricStore.Instance.get(:default)}
  end

  defp cmd(ctx, name, args), do: Dispatcher.dispatch(name, args, ctx)

  defp raise_to_9!(ctx) do
    assert {:ok, %{active: 9}} = FeatureLevel.raise(ctx, 9)
  end

  defp tree_files(ctx, key) do
    shard = Router.shard_for(ctx, key)

    ctx.data_dir
    |> Promotion.dedicated_path(shard, :zset_tree, key)
    |> Path.join("*.tree")
    |> Path.wildcard()
  end

  defp member_rows(ctx, key) do
    shard = Router.shard_for(ctx, key)
    prefix = CompoundKey.zset_prefix(key)

    elem(ctx.keydir_refs, shard)
    |> :ets.tab2list()
    |> Enum.filter(fn row -> String.starts_with?(elem(row, 0), prefix) end)
  end

  test "a set past the threshold moves into its tree and leaves the keydir", %{ctx: ctx} do
    raise_to_9!(ctx)
    pairs = Enum.flat_map(1..40, fn i -> ["#{i}", "m#{String.pad_leading("#{i}", 3, "0")}"] end)
    assert 40 = cmd(ctx, "ZADD", ["z" | pairs])

    assert [_file] = tree_files(ctx, "z")
    assert [] = member_rows(ctx, "z")
    assert 40 = cmd(ctx, "ZCARD", ["z"])
    assert 39 = cmd(ctx, "ZRANK", ["z", "m040"])
    assert ["m039", "m040"] = cmd(ctx, "ZRANGE", ["z", "-2", "-1"])
    assert {:simple, "zset"} = cmd(ctx, "TYPE", ["z"])

    assert 1 = cmd(ctx, "DEL", ["z"])
    assert 0 = cmd(ctx, "EXISTS", ["z"])
    assert 0 = cmd(ctx, "ZCARD", ["z"])
  end

  test "replies match a set promoted before level 9", %{ctx: ctx} do
    seeds = [11, 23, 47]
    legacy = for seed <- seeds, do: run_random(ctx, "legacy:#{seed}", seed)
    raise_to_9!(ctx)
    tree = for seed <- seeds, do: run_random(ctx, "tree:#{seed}", seed)

    for {{legacy_run, tree_run}, seed} <- Enum.zip(Enum.zip(legacy, tree), seeds) do
      Enum.zip(legacy_run, tree_run)
      |> Enum.with_index()
      |> Enum.each(fn {{{command, expected}, {_command, got}}, step} ->
        assert got == expected,
               "seed #{seed} step #{step}: #{inspect(command)}\nlegacy: #{inspect(expected)}\ntree:   #{inspect(got)}"
      end)
    end

    assert Enum.any?(seeds, &(tree_files(ctx, "tree:#{&1}") != []))
  end

  test "a tree-encoded set survives a restart", %{ctx: ctx} do
    raise_to_9!(ctx)
    pairs = Enum.flat_map(1..200, fn i -> ["#{rem(i * 7, 50)}", "m#{i}"] end)
    assert 200 = cmd(ctx, "ZADD", ["r" | pairs])
    assert 3 = cmd(ctx, "ZREM", ["r", "m1", "m2", "m3"])
    before = cmd(ctx, "ZRANGE", ["r", "0", "-1", "WITHSCORES"])

    assert :ok = WARaftBackend.stop()
    assert :ok = WARaftBackend.start(ctx)
    assert :ok = ShardHelpers.wait_default_pipeline_ready()

    assert ^before = cmd(ctx, "ZRANGE", ["r", "0", "-1", "WITHSCORES"])
    assert 197 = cmd(ctx, "ZCARD", ["r"])
    assert 1 = cmd(ctx, "ZADD", ["r", "1", "after-restart"])
  end

  test "rank and range by index stay flat as the set grows", %{ctx: ctx} do
    raise_to_9!(ctx)

    timing = fn size ->
      key = "flat:#{size}"

      for chunk <- Enum.chunk_every(1..size, 2_000) do
        args = Enum.flat_map(chunk, fn i -> ["0", "e#{String.pad_leading("#{i}", 7, "0")}"] end)
        cmd(ctx, "ZADD", [key | args])
      end

      last = "e#{String.pad_leading("#{size}", 7, "0")}"
      runs = for _ <- 1..30, do: elem(:timer.tc(fn -> cmd(ctx, "ZRANK", [key, last]) end), 0)
      Enum.at(Enum.sort(runs), 15)
    end

    small = timing.(2_000)
    large = timing.(60_000)
    assert large < small * 3, "ZRANK median #{large} us at 60k vs #{small} us at 2k"
  end

  # Random commands over a small member space, so sets cross the threshold,
  # shrink and grow again.
  # The commands are drawn before any runs: the store may use :rand itself.
  defp run_random(ctx, key, seed) do
    :rand.seed(:exsss, {seed, seed, seed})
    commands = for _ <- 1..500, do: random_command(key)
    for command <- commands, do: {command, run(ctx, command)}
  end

  defp run(ctx, [name | args]) when name == "ZSCAN" do
    scan_all(ctx, args, "0", [])
  end

  defp run(ctx, [name | args]), do: cmd(ctx, name, args)

  defp scan_all(ctx, [key | opts], cursor, acc) do
    case cmd(ctx, "ZSCAN", [key, cursor | opts]) do
      [next, items] when next == "0" -> pairs_sorted(acc ++ items)
      [next, items] -> scan_all(ctx, [key | opts], next, acc ++ items)
      other -> other
    end
  end

  defp pairs_sorted(flat), do: flat |> Enum.chunk_every(2) |> Enum.uniq() |> Enum.sort()

  defp member, do: "m#{String.pad_leading("#{:rand.uniform(60)}", 2, "0")}"

  defp score,
    do: Enum.random(["0", "1", "-3", "2.5", "7", "-0.5", "12", "#{:rand.uniform(40) - 20}"])

  defp score_bound,
    do: Enum.random(["-inf", "+inf", "(1", "2.5", "(-3", "0", "#{:rand.uniform(30) - 15}"])

  defp lex_bound, do: Enum.random(["-", "+", "[#{member()}", "(#{member()}"])

  defp index, do: "#{:rand.uniform(80) - 40}"

  defp maybe(list), do: if(:rand.uniform(2) == 1, do: list, else: [])

  defp limit, do: maybe(["LIMIT", "#{:rand.uniform(4) - 1}", "#{:rand.uniform(6) - 2}"])

  defp zadd_flags do
    Enum.random([
      [],
      [],
      ["NX"],
      ["XX"],
      ["GT"],
      ["LT"],
      ["CH"],
      ["XX", "GT", "CH"],
      ["NX", "XX"]
    ])
  end

  defp random_command(key) do
    case :rand.uniform(22) do
      n when n <= 6 ->
        pairs = Enum.flat_map(1..:rand.uniform(6), fn _ -> [score(), member()] end)
        ["ZADD", key | zadd_flags() ++ pairs]

      7 ->
        ["ZADD", key, "INCR" | maybe(["XX"]) ++ [score(), member()]]

      8 ->
        ["ZINCRBY", key, score(), member()]

      9 ->
        ["ZREM", key | Enum.map(1..:rand.uniform(4), fn _ -> member() end)]

      10 ->
        [Enum.random(["ZPOPMIN", "ZPOPMAX"]), key | maybe(["#{:rand.uniform(4)}"])]

      11 ->
        ["ZRANGE", key, index(), index() | maybe(["REV"]) ++ maybe(["WITHSCORES"])]

      12 ->
        [
          "ZRANGE",
          key,
          score_bound(),
          score_bound(),
          "BYSCORE" | maybe(["REV"]) ++ limit() ++ maybe(["WITHSCORES"])
        ]

      13 ->
        ["ZRANGE", key, lex_bound(), lex_bound(), "BYLEX" | maybe(["REV"]) ++ limit()]

      14 ->
        [
          Enum.random(["ZRANGEBYSCORE", "ZREVRANGEBYSCORE"]),
          key,
          score_bound(),
          score_bound() | maybe(["WITHSCORES"]) ++ limit()
        ]

      15 ->
        ["ZREVRANGE", key, index(), index() | maybe(["WITHSCORES"])]

      16 ->
        [Enum.random(["ZRANK", "ZREVRANK"]), key, member() | maybe(["WITHSCORE"])]

      17 ->
        ["ZSCORE", key, member()]

      18 ->
        ["ZMSCORE", key, member(), member()]

      19 ->
        ["ZCOUNT", key, score_bound(), score_bound()]

      20 ->
        ["ZCARD", key]

      21 ->
        ["ZSCAN", key | maybe(["COUNT", "#{:rand.uniform(10)}"])]

      22 ->
        Enum.random([["TYPE", key], ["EXISTS", key]])
    end
  end
end
