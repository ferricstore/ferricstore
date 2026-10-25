defmodule Ferricstore.Commands.DelLevel6Test do
  @moduledoc """
  DEL read whether a key existed and then deleted it, so clients on two nodes
  both counted the key. From cluster feature level 6 the instance deletes
  each key with one replicated entry that decides whether it existed where it
  applies (not a transaction, which costs several more fsyncs). Every type
  must go as completely as the older path deletes it.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  defp cmd(ctx, name, args), do: Dispatcher.dispatch(name, args, ctx)
  defp ukey(tag), do: "{del6:#{System.unique_integer([:positive])}}#{tag}"

  defp prob_files(ctx, key) do
    digest = Ferricstore.ProbFile.digest(key)
    Path.wildcard(Path.join(ctx.data_dir, "**/#{digest}.*"))
  end

  describe "below feature level 6" do
    test "DEL keeps the older path" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, %{active: active}} = FerricStore.cluster_feature_level()
      assert active < 6
      key = ukey("s")

      assert :unsupported ==
               Ferricstore.Store.Ops.compound_command(ctx, key, {:del, [key]}, 6)

      assert :ok == cmd(ctx, "SET", [key, "v"])
      assert 1 == cmd(ctx, "DEL", [key, key])
      assert {:ok, 0} == FerricStore.del(key)
    end
  end

  describe "at feature level 6" do
    setup do
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
      assert {:ok, %{active: 6}} = FerricStore.raise_cluster_feature_level(6)

      parent = self()

      Process.put(:ferricstore_tx_backend_write_hook, fn _shard, _command ->
        send(parent, :transaction_used)
        {:error, :noproc}
      end)

      on_exit(fn -> Process.delete(:ferricstore_tx_backend_write_hook) end)
      %{ctx: FerricStore.Instance.get(:default)}
    end

    test "DEL deletes every type in one entry and counts what existed", %{ctx: ctx} do
      blob = :binary.copy("b", ctx.blob_side_channel_threshold_bytes + 1)

      keys = %{
        string: ukey("s"),
        blob: ukey("b"),
        ttl: ukey("t"),
        hash: ukey("h"),
        set: ukey("set"),
        zset: ukey("z"),
        list: ukey("l"),
        stream: ukey("x"),
        geo: ukey("g"),
        hll: ukey("p"),
        tdigest: ukey("td")
      }

      assert :ok == cmd(ctx, "SET", [keys.string, "v"])
      assert :ok == cmd(ctx, "SET", [keys.blob, blob])
      assert :ok == cmd(ctx, "SET", [keys.ttl, "v", "EX", "100"])
      assert 2 == cmd(ctx, "HSET", [keys.hash, "f", "1", "g", "2"])
      assert 2 == cmd(ctx, "SADD", [keys.set, "a", "b"])
      assert 2 == cmd(ctx, "ZADD", [keys.zset, "1", "a", "2", "b"])
      assert 2 == cmd(ctx, "RPUSH", [keys.list, "a", "b"])
      assert is_binary(cmd(ctx, "XADD", [keys.stream, "*", "f", "1"]))
      assert :ok == cmd(ctx, "XGROUP", ["CREATE", keys.stream, "g", "0"])
      assert 1 == cmd(ctx, "GEOADD", [keys.geo, "13.36", "38.11", "a"])
      assert 1 == cmd(ctx, "PFADD", [keys.hll, "a"])
      assert :ok == cmd(ctx, "TDIGEST.CREATE", [keys.tdigest])

      for {type, key} <- keys do
        assert 1 == cmd(ctx, "DEL", [key, key]), "#{type}"
        assert 0 == cmd(ctx, "EXISTS", [key]), "#{type}"
        assert {:simple, "none"} == cmd(ctx, "TYPE", [key]), "#{type}"
      end

      assert [] == cmd(ctx, "HGETALL", [keys.hash])
      assert [] == cmd(ctx, "SMEMBERS", [keys.set])
      assert [] == cmd(ctx, "ZRANGE", [keys.zset, "0", "-1"])
      assert [] == cmd(ctx, "LRANGE", [keys.list, "0", "-1"])
      assert 0 == cmd(ctx, "XLEN", [keys.stream])

      # A list or stream made again starts empty, with no old metadata.
      assert 1 == cmd(ctx, "RPUSH", [keys.list, "c"])
      assert ["c"] == cmd(ctx, "LRANGE", [keys.list, "0", "-1"])
      assert :ok == cmd(ctx, "SET", [keys.hash, "now a string"])
      assert "now a string" == cmd(ctx, "GET", [keys.hash])
      assert 0 == cmd(ctx, "DEL", [ukey("missing")])
      refute_received :transaction_used
    end

    test "DEL removes a probabilistic key's file", %{ctx: ctx} do
      for {create, args} <- [
            {"BF.RESERVE", ["0.01", "100"]},
            {"CMS.INITBYDIM", ["10", "5"]},
            {"CF.RESERVE", ["100"]},
            {"TOPK.RESERVE", ["3"]}
          ] do
        key = ukey(create)
        assert :ok == cmd(ctx, create, [key | args])
        assert [_file] = prob_files(ctx, key)

        assert 1 == cmd(ctx, "DEL", [key])
        assert 0 == cmd(ctx, "EXISTS", [key])
        assert [] == prob_files(ctx, key), create
        assert :ok == cmd(ctx, create, [key | args])
      end

      refute_received :transaction_used
    end

    test "DEL removes a promoted collection", %{ctx: ctx} do
      key = ukey("big")
      fields = Enum.flat_map(1..300, &["f#{&1}", "v#{&1}"])
      assert 300 == cmd(ctx, "HSET", [key | fields])
      shard = Router.shard_name(ctx, Router.shard_for(ctx, key))
      assert GenServer.call(shard, {:promoted?, key}, 30_000)

      assert 1 == cmd(ctx, "DEL", [key])
      assert 0 == cmd(ctx, "HLEN", [key])
      assert 1 == cmd(ctx, "HSET", [key, "a", "1"])
      assert ["a", "1"] == cmd(ctx, "HGETALL", [key])
      refute GenServer.call(shard, {:promoted?, key}, 30_000)
    end

    test "the embedded API's DEL counts once", %{ctx: ctx} do
      [a, b] = [ukey("a"), ukey("b")]
      assert :ok == cmd(ctx, "SET", [a, "1"])
      assert 1 == cmd(ctx, "SADD", [b, "m"])
      assert {:ok, 2} == FerricStore.del([a, b, a, ukey("c")])
      assert {:ok, 0} == FerricStore.del([a, b])
      refute_received :transaction_used
    end
  end
end
