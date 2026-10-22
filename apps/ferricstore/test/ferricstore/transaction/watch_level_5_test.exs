defmodule Ferricstore.Transaction.WatchLevel5Test do
  @moduledoc """
  From feature level 5 a WATCH decides from replicated write versions
  (`Ferricstore.Transaction.WatchIndex`) instead of storage locations. Every
  way a client can write a watched key must still abort the EXEC, and an
  untouched key must not.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.PreparedTransactionCoordinator, as: Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 5}} = FeatureLevel.raise(FerricStore.Instance.get(:default), 5)
    {:ok, isolated: isolated}
  end

  setup do
    tag = System.unique_integer([:positive])

    {:ok,
     ctx: FerricStore.Instance.get(:default),
     key: "{watch5:#{tag}}key",
     other: "{watch5:#{tag}}other",
     target: "{watch5:#{tag}}target"}
  end

  test "WATCH answers a write-version token", %{ctx: ctx, key: key} do
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    assert {:watch_index, _version, true} = Router.watch_token(ctx, key)
    assert {:watch_index, _version, false} = Router.watch_token(ctx, key <> ":missing")
  end

  test "an untouched watched key lets the EXEC apply", %{ctx: ctx, key: key, target: target} do
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    watched = watch(ctx, [key])

    assert [:ok] = Coordinator.execute([{"SET", [target, "applied"]}], watched, nil)
    assert Router.get(ctx, target) == "applied"
  end

  test "a write to another key lets the EXEC apply", %{
    ctx: ctx,
    key: key,
    other: other,
    target: target
  } do
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    watched = watch(ctx, [key])
    assert :ok = dispatch(ctx, "SET", [other, "x"])

    assert [:ok] = Coordinator.execute([{"SET", [target, "applied"]}], watched, nil)
  end

  string_writes = [
    {"SET of the same value", [{"SET", ["$key", "v"]}]},
    {"SET", [{"SET", ["$key", "changed"]}]},
    {"DEL", [{"DEL", ["$key"]}]},
    {"EXPIRE", [{"EXPIRE", ["$key", "1000"]}]},
    {"APPEND", [{"APPEND", ["$key", "x"]}]},
    {"SETRANGE", [{"SETRANGE", ["$key", "0", "z"]}]},
    {"GETDEL", [{"GETDEL", ["$key"]}]},
    {"MSET", [{"MSET", ["$other", "o", "$key", "m"]}]},
    {"RENAME onto it", [{"SET", ["$other", "o"]}, {"RENAME", ["$other", "$key"]}]}
  ]

  for {name, writes} <- string_writes do
    test "#{name} aborts the EXEC", %{ctx: ctx, key: key, other: other, target: target} do
      assert :ok = dispatch(ctx, "SET", [key, "v"])
      watched = watch(ctx, [key])

      for {command, args} <- unquote(Macro.escape(writes)) do
        dispatch(ctx, command, Enum.map(args, &bind(&1, key, other)))
      end

      assert_aborted(ctx, watched, target)
    end
  end

  collection_writes = [
    {"HSET", {"HSET", ["$key", "f", "v"]}, {"HSET", ["$key", "f", "w"]}},
    {"LPUSH", {"LPUSH", ["$key", "a"]}, {"LPUSH", ["$key", "b"]}},
    {"SADD", {"SADD", ["$key", "a"]}, {"SADD", ["$key", "b"]}},
    {"ZADD", {"ZADD", ["$key", "1", "a"]}, {"ZADD", ["$key", "2", "a"]}},
    {"XADD", {"XADD", ["$key", "*", "f", "v"]}, {"XADD", ["$key", "*", "f", "w"]}},
    {"BF.ADD", {"BF.ADD", ["$key", "a"]}, {"BF.ADD", ["$key", "b"]}}
  ]

  for {name, {create, create_args}, {write, write_args}} <- collection_writes do
    test "#{name} on a watched collection aborts the EXEC", %{
      ctx: ctx,
      key: key,
      other: other,
      target: target
    } do
      dispatch(ctx, unquote(create), Enum.map(unquote(create_args), &bind(&1, key, other)))
      watched = watch(ctx, [key])
      dispatch(ctx, unquote(write), Enum.map(unquote(write_args), &bind(&1, key, other)))

      assert_aborted(ctx, watched, target)
    end
  end

  test "a field write to a watched promoted hash aborts the EXEC", %{
    ctx: ctx,
    key: key,
    target: target
  } do
    assert 150 == dispatch(ctx, "HSET", [key | Enum.flat_map(1..150, &["f#{&1}", "v#{&1}"])])

    ShardHelpers.eventually(
      fn -> promoted?(ctx, key) end,
      "expected #{inspect(key)} to finish promotion"
    )

    watched = watch(ctx, [key])
    assert 0 == dispatch(ctx, "HSET", [key, "f7", "changed"])

    assert_aborted(ctx, watched, target)
  end

  test "another client's transaction writing the key aborts the EXEC", %{
    ctx: ctx,
    key: key,
    target: target
  } do
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    watched = watch(ctx, [key])
    assert [:ok] = Coordinator.execute([{"SET", [key, "from-multi"]}], %{}, nil)

    assert_aborted(ctx, watched, target)
  end

  test "a batched write aborts the EXEC", %{ctx: ctx, key: key, other: other, target: target} do
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    watched = watch(ctx, [key])
    Router.batch_quorum_put(ctx, [{other, "o"}, {key, "batched"}])

    assert_aborted(ctx, watched, target)
  end

  test "creating a watched missing key aborts the EXEC", %{ctx: ctx, key: key, target: target} do
    watched = watch(ctx, [key])
    assert :ok = dispatch(ctx, "SET", [key, "created"])

    assert_aborted(ctx, watched, target)
  end

  test "a watched key that expires aborts the EXEC", %{ctx: ctx, key: key, target: target} do
    assert :ok = dispatch(ctx, "SET", [key, "v", "PX", "50"])
    watched = watch(ctx, [key])
    Process.sleep(100)

    assert_aborted(ctx, watched, target)
  end

  test "FLUSHDB aborts the EXEC", %{ctx: ctx, key: key, target: target} do
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    watched = watch(ctx, [key])
    assert :ok = dispatch(ctx, "FLUSHDB", [])

    assert_aborted(ctx, watched, target)
  end

  # A replica that restarts alone must read the versions its log wrote, or it
  # would decide a watched EXEC differently from the others.
  test "write versions survive a restart", %{key: key, target: target, isolated: isolated} do
    ctx = FerricStore.Instance.get(:default)
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    clean = watch(ctx, [key])
    changed = watch(ctx, [key])
    assert :ok = dispatch(ctx, "SET", [key, "after-watch"])

    ShardHelpers.restart_current_data_dir(isolated)
    ctx = FerricStore.Instance.get(:default)

    assert Router.watch_tokens(ctx, [key]) != clean
    assert_aborted(ctx, changed, target)

    rewatched = watch(ctx, [key])
    assert [:ok] = Coordinator.execute([{"SET", [target, "applied"]}], rewatched, nil)
  end

  test "an unwritten key's version survives a restart", %{
    key: key,
    target: target,
    isolated: isolated
  } do
    ctx = FerricStore.Instance.get(:default)
    assert :ok = dispatch(ctx, "SET", [key, "v"])
    watched = watch(ctx, [key])

    ShardHelpers.restart_current_data_dir(isolated)
    ctx = FerricStore.Instance.get(:default)

    assert Router.watch_tokens(ctx, [key]) == watched
    assert [:ok] = Coordinator.execute([{"SET", [target, "applied"]}], watched, nil)
  end

  defp watch(ctx, keys) do
    tokens = Router.watch_tokens(ctx, keys)
    assert Enum.all?(Map.values(tokens), &match?({:watch_index, _version, _live?}, &1))
    tokens
  end

  defp assert_aborted(ctx, watched, target) do
    assert Coordinator.execute([{"SET", [target, "must-not-apply"]}], watched, nil) == nil
    assert Router.get(ctx, target) == nil
  end

  defp bind("$key", key, _other), do: key
  defp bind("$other", _key, other), do: other
  defp bind(arg, _key, _other), do: arg

  defp dispatch(ctx, command, args), do: Dispatcher.dispatch(command, args, ctx)

  defp promoted?(ctx, key),
    do: GenServer.call(Router.shard_name(ctx, Router.shard_for(ctx, key)), {:promoted?, key})
end
