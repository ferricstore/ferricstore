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
  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.{CompoundKey, Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  import Ferricstore.Test.Eventually

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

  test "MULTI replies match a set promoted before level 9", %{ctx: ctx} do
    seeds = [5, 61]

    seed_set = fn key ->
      pairs =
        Enum.flat_map(1..30, fn i -> ["#{i - 15}", "m#{String.pad_leading("#{i}", 2, "0")}"] end)

      assert 30 = cmd(ctx, "ZADD", [key | pairs])
    end

    legacy =
      for seed <- seeds do
        seed_set.("{multi-legacy:#{seed}}")
        run_random_multi("{multi-legacy:#{seed}}", seed)
      end

    raise_to_9!(ctx)

    tree =
      for seed <- seeds do
        seed_set.("{multi-tree:#{seed}}")
        assert [_file] = tree_files(ctx, "{multi-tree:#{seed}}")
        run_random_multi("{multi-tree:#{seed}}", seed)
      end

    for seed <- seeds do
      assert cmd(ctx, "ZRANGE", ["{multi-tree:#{seed}}", "0", "-1", "WITHSCORES"]) ==
               cmd(ctx, "ZRANGE", ["{multi-legacy:#{seed}}", "0", "-1", "WITHSCORES"])

      if tree_files(ctx, "{multi-tree:#{seed}}") != [],
        do: assert([] = member_rows(ctx, "{multi-tree:#{seed}}"))
    end

    assert Enum.any?(seeds, &(tree_files(ctx, "{multi-tree:#{&1}}") != []))

    for {{legacy_run, tree_run}, seed} <- Enum.zip(Enum.zip(legacy, tree), seeds) do
      Enum.zip(legacy_run, tree_run)
      |> Enum.with_index()
      |> Enum.each(fn {{{commands, expected}, {_commands, got}}, step} ->
        assert got == expected,
               "seed #{seed} exec #{step}: #{inspect(commands)}\nlegacy: #{inspect(expected)}\ntree:   #{inspect(got)}"
      end)
    end
  end

  test "a set that grows past the threshold inside MULTI moves into its tree", %{ctx: ctx} do
    raise_to_9!(ctx)
    pairs = Enum.flat_map(1..40, fn i -> ["#{i}", "m#{String.pad_leading("#{i}", 3, "0")}"] end)

    assert %{results: [40, 40, ["m040"]], prepare_errors: []} =
             exec([["ZADD", "{mz}" | pairs], ["ZCARD", "{mz}"], ["ZRANGE", "{mz}", "-1", "-1"]])

    assert [_file] = tree_files(ctx, "{mz}")
    assert [] = member_rows(ctx, "{mz}")
    assert 39 = cmd(ctx, "ZRANK", ["{mz}", "m040"])
  end

  # Key commands on a tree-encoded set answer as they do for a set promoted
  # before level 9.
  test "key commands match a set promoted before level 9", %{ctx: ctx} do
    scenario = fn key ->
      pairs = Enum.flat_map(1..30, fn i -> ["#{i}", "m#{String.pad_leading("#{i}", 2, "0")}"] end)
      other = key <> ":copy"
      renamed = key <> ":renamed"

      [
        cmd(ctx, "ZADD", [key | pairs]),
        cmd(ctx, "TYPE", [key]),
        cmd(ctx, "EXISTS", [key]),
        cmd(ctx, "OBJECT", ["ENCODING", key]),
        cmd(ctx, "COPY", [key, other]),
        cmd(ctx, "ZRANGE", [other, "0", "4", "WITHSCORES"]),
        cmd(ctx, "ZCARD", [other]),
        cmd(ctx, "RENAME", [other, renamed]),
        cmd(ctx, "ZCARD", [renamed]),
        cmd(ctx, "ZCARD", [other]),
        cmd(ctx, "ZRANK", [renamed, "m30"]),
        cmd(ctx, "ZADD", [renamed, "100", "new"]),
        cmd(ctx, "ZCARD", [key]),
        cmd(ctx, "PEXPIRE", [key, "150"]),
        Process.sleep(300),
        cmd(ctx, "ZCARD", [key]),
        cmd(ctx, "EXISTS", [key]),
        cmd(ctx, "ZADD", [key, "1", "fresh"]),
        cmd(ctx, "ZRANGE", [key, "0", "-1"]),
        cmd(ctx, "ZRANDMEMBER", [renamed, "-3"]) |> length(),
        cmd(ctx, "DEL", [renamed]),
        cmd(ctx, "ZCARD", [renamed])
      ]
    end

    legacy = scenario.("{keys-legacy}")
    raise_to_9!(ctx)
    tree = scenario.("{keys-tree}")

    assert tree == legacy
  end

  # A tree member has no TTL of its own: EXPIRE and PERSIST change the type key
  # and the marker, and leave the tree file as it is. The set expires with its
  # marker, and a later write starts a new set.
  test "EXPIRE leaves the tree as it is and the set expires with its marker", %{ctx: ctx} do
    key = "{ttl-tree}"
    raise_to_9!(ctx)

    pairs = Enum.flat_map(1..30, fn i -> ["#{i}", "m#{i}"] end)
    assert 30 = cmd(ctx, "ZADD", [key | pairs])
    assert [file] = tree_files(ctx, key)
    size = File.stat!(file).size

    assert 1 = cmd(ctx, "PEXPIRE", [key, "100000"])
    assert cmd(ctx, "PTTL", [key]) > 0
    assert 1 = cmd(ctx, "PERSIST", [key])
    assert -1 = cmd(ctx, "TTL", [key])
    assert File.stat!(file).size == size

    assert %{results: [1], prepare_errors: []} = exec([["PEXPIRE", key, "150"]])
    assert cmd(ctx, "PTTL", [key]) > 0
    Process.sleep(300)

    assert 0 = cmd(ctx, "ZCARD", [key])
    assert 0 = cmd(ctx, "EXISTS", [key])
    assert 1 = cmd(ctx, "ZADD", [key, "1", "a"])
    assert ["a"] = cmd(ctx, "ZRANGE", [key, "0", "-1"])
    assert 1 = cmd(ctx, "ZCARD", [key])
    assert File.stat!(file).size == size
  end

  test "WATCH, SCAN TYPE and FLUSHDB see a tree-encoded set", %{ctx: ctx} do
    raise_to_9!(ctx)
    key = "{server}"
    pairs = Enum.flat_map(1..30, fn i -> ["#{i}", "m#{i}"] end)
    assert 30 = cmd(ctx, "ZADD", [key | pairs])
    assert [_file] = tree_files(ctx, key)

    # SCAN TYPE zset names the set once, not its members.
    scanned =
      Stream.unfold("0", fn
        nil ->
          nil

        cursor ->
          [next, keys] = cmd(ctx, "SCAN", [cursor, "TYPE", "zset", "COUNT", "1000"])
          {keys, if(next == "0", do: nil, else: next)}
      end)
      |> Enum.concat()

    assert [^key] = Enum.filter(scanned, &String.starts_with?(&1, "{server}"))

    # A write after WATCH aborts the transaction.
    watched = %{key => Router.watch_token(ctx, key)}
    assert 1 = cmd(ctx, "ZADD", [key, "99", "intruder"])

    {:ok, command} = PreparedCommand.prepare("ZADD", [key, "1", "from-multi"])
    assert nil == Ferricstore.Transaction.Coordinator.execute([command], watched, nil)
    assert nil == cmd(ctx, "ZSCORE", [key, "from-multi"])

    # Unchanged since WATCH: it runs.
    watched = %{key => Router.watch_token(ctx, key)}
    assert [1] = Ferricstore.Transaction.Coordinator.execute([command], watched, nil)

    assert :ok = cmd(ctx, "FLUSHDB", [])
    assert 0 = cmd(ctx, "EXISTS", [key])
    assert 0 = cmd(ctx, "ZCARD", [key])
    assert 2 = cmd(ctx, "ZADD", [key, "1", "a", "2", "b"])
    assert ["a", "b"] = cmd(ctx, "ZRANGE", [key, "0", "-1"])
  end

  test "GEO commands match a set promoted before level 9", %{ctx: ctx} do
    scenario = fn key ->
      places =
        for i <- 1..25,
            do: ["#{13 + i / 100}", "#{52 + i / 100}", "p#{String.pad_leading("#{i}", 2, "0")}"]

      [
        cmd(ctx, "GEOADD", [key | List.flatten(places)]),
        cmd(ctx, "GEOPOS", [key, "p01", "p25", "missing"]),
        cmd(ctx, "GEODIST", [key, "p01", "p25", "km"]),
        cmd(ctx, "GEOHASH", [key, "p10"]),
        cmd(ctx, "GEOSEARCH", [
          key,
          "FROMMEMBER",
          "p05",
          "BYRADIUS",
          "20",
          "km",
          "ASC",
          "COUNT",
          "5"
        ]),
        cmd(ctx, "GEOSEARCH", [
          key,
          "FROMLONLAT",
          "13.1",
          "52.1",
          "BYBOX",
          "40",
          "40",
          "km",
          "DESC",
          "WITHDIST"
        ]),
        cmd(ctx, "ZCARD", [key])
      ]
    end

    legacy = scenario.("{geo-legacy}")
    raise_to_9!(ctx)
    tree = scenario.("{geo-tree}")

    assert tree == legacy
    assert [_file] = tree_files(ctx, "{geo-tree}")
  end

  # A tree write inside MULTI lands at once; when the transaction then fails,
  # each tree it wrote is cut back to before its first write.
  test "a transaction that fails after writing a tree leaves the tree as it was", %{ctx: ctx} do
    raise_to_9!(ctx)
    key = "{rollback}"
    poison = key <> ":poison"
    pairs = Enum.flat_map(1..30, fn i -> ["#{i}", "m#{i}"] end)
    assert 30 = cmd(ctx, "ZADD", [key | pairs])
    assert [file] = tree_files(ctx, key)
    before = cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
    size = File.stat!(file).size

    Application.put_env(:ferricstore, :cross_shard_transaction_hook, fn
      {:staged_put, _shard, ^poison} -> throw({:transaction_store_failure, :injected_failure})
      _event -> :ok
    end)

    try do
      result =
        exec([
          ["ZADD", key, "100", "new"],
          ["ZINCRBY", key, "5", "m1"],
          ["ZREM", key, "m2"],
          ["SET", poison, "x"]
        ])

      refute match?(%{results: [1, "6", 1, :ok]}, result)
    after
      Application.delete_env(:ferricstore, :cross_shard_transaction_hook)
    end

    assert ^before = cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
    assert nil == cmd(ctx, "ZSCORE", [key, "new"])
    assert File.stat!(file).size == size
    assert nil == cmd(ctx, "GET", [poison])

    assert 1 = cmd(ctx, "ZADD", [key, "1", "after"])
    assert 31 = cmd(ctx, "ZCARD", [key])
  end

  # Sets promoted to a dedicated Bitcask before level 9 rank by walking an
  # in-memory index; the first write after the raise moves them into a tree.
  test "a set promoted before level 9 moves into its tree on its next write", %{ctx: ctx} do
    key = "{legacy-promoted}"
    in_multi = "{legacy-promoted}:multi"
    shard = Router.shard_for(ctx, key)
    dedicated = Promotion.dedicated_path(ctx.data_dir, shard, :zset, key)
    dedicated_multi = Promotion.dedicated_path(ctx.data_dir, shard, :zset, in_multi)
    pairs = Enum.flat_map(1..40, fn i -> ["#{rem(i * 7, 23)}", "m#{i}"] end)
    assert 40 = cmd(ctx, "ZADD", [key | pairs])
    assert 40 = cmd(ctx, "ZADD", [in_multi | pairs])
    assert_eventually(fn -> assert File.dir?(dedicated) and File.dir?(dedicated_multi) end)
    multi_before = cmd(ctx, "ZRANGE", [in_multi, "0", "-1", "WITHSCORES"])
    assert "11" = cmd(ctx, "ZINCRBY", [key, "4", "m1"])
    before = cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])

    raise_to_9!(ctx)
    assert ^before = cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
    assert 1 = cmd(ctx, "ZADD", [key, "100", "after"])

    assert [_file] = tree_files(ctx, key)
    assert_eventually(fn -> refute File.exists?(dedicated) end)
    assert before ++ ["after", "100"] == cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
    assert 41 = cmd(ctx, "ZCARD", [key])
    assert 40 = cmd(ctx, "ZRANK", [key, "after"])
    assert [] = member_rows(ctx, key)

    assert %{results: [1, 41], prepare_errors: []} =
             exec([["ZADD", in_multi, "100", "after"], ["ZCARD", in_multi]])

    assert [_file] = tree_files(ctx, in_multi)
    assert_eventually(fn -> refute File.exists?(dedicated_multi) end)

    assert multi_before ++ ["after", "100"] ==
             cmd(ctx, "ZRANGE", [in_multi, "0", "-1", "WITHSCORES"])

    assert :ok = WARaftBackend.stop()
    assert :ok = WARaftBackend.start(ctx)
    assert :ok = ShardHelpers.wait_default_pipeline_ready()
    assert before ++ ["after", "100"] == cmd(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
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

  defmodule LocalInstance do
    use FerricStore, shard_count: 4
  end

  # The cross-shard store of an instance without Raft refuses tree-encoded
  # sets, so such an instance must never make one.
  test "an instance without Raft keeps large sets out of trees and copies them across shards" do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_zset_local_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      LocalInstance.stop()
      File.rm_rf(root)
    end)

    assert {:ok, _pid} =
             LocalInstance.start_link(
               data_dir: root,
               shard_count: 4,
               promotion_threshold: @threshold
             )

    local = FerricStore.Instance.get(LocalInstance)
    refute Router.durable_context?(local)
    raise_to_9!(local)

    pairs = Enum.flat_map(1..40, fn i -> ["#{i}", "m#{i}"] end)
    assert 40 = cmd(local, "ZADD", ["src" | pairs])
    assert [] = tree_files(local, "src")
    members = cmd(local, "ZRANGE", ["src", "0", "-1", "WITHSCORES"])

    [copy, renamed] =
      Stream.map(1..1_000, &"dst#{&1}")
      |> Stream.filter(&(Router.shard_for(local, &1) != Router.shard_for(local, "src")))
      |> Enum.take(2)

    assert 1 = cmd(local, "COPY", ["src", copy])
    assert :ok = cmd(local, "RENAME", ["src", renamed])
    assert ^members = cmd(local, "ZRANGE", [copy, "0", "-1", "WITHSCORES"])
    assert ^members = cmd(local, "ZRANGE", [renamed, "0", "-1", "WITHSCORES"])
    assert 0 = cmd(local, "EXISTS", ["src"])
  end

  # Random commands over a small member space, so sets cross the threshold,
  # shrink and grow again.
  # The commands are drawn before any runs: the store may use :rand itself.
  defp run_random(ctx, key, seed) do
    :rand.seed(:exsss, {seed, seed, seed})
    commands = for _ <- 1..500, do: random_command(key)
    for command <- commands, do: {command, run(ctx, command)}
  end

  # Random commands in MULTI blocks of one to eight.
  defp run_random_multi(key, seed) do
    :rand.seed(:exsss, {seed, seed, seed})

    commands =
      for _ <- 1..400,
          command = random_command(key),
          hd(command) != "ZSCAN",
          do: command

    commands
    |> chunk_randomly([])
    |> Enum.map(fn block -> {block, exec(block)} end)
  end

  # One MULTI/EXEC: a command that does not prepare is reported in place and
  # left out of the queue.
  defp exec(block) do
    prepared = Enum.map(block, fn [name | args] -> PreparedCommand.prepare(name, args) end)
    queue = for {:ok, command} <- prepared, do: command

    results =
      case queue do
        [] -> []
        _commands -> Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
      end

    %{results: results, prepare_errors: for({:error, reason} <- prepared, do: reason)}
  end

  defp chunk_randomly([], acc), do: Enum.reverse(acc)

  defp chunk_randomly(commands, acc) do
    {block, rest} = Enum.split(commands, :rand.uniform(8))
    chunk_randomly(rest, [block | acc])
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
