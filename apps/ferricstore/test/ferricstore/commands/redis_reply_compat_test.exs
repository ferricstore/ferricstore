defmodule Ferricstore.Commands.RedisReplyCompatTest do
  @moduledoc """
  Replies that differed from Redis 8 for the same commands, found by running
  random command sequences against both. The expected values are Redis's.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, NativeAstParser}

  @wrongtype {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{compat:#{System.unique_integer([:positive])}}"
    }
  end

  # Every case runs twice: through the command handlers, and through the
  # parsed form the TCP and HTTP gateways (and MULTI) execute.
  defp run(ctx, command, args) do
    case Process.get(:compat_path) do
      :ast ->
        {:ok, _cmd, _args, ast, _keys} = NativeAstParser.parse(command, args)
        Dispatcher.dispatch_ast(ast, ctx)

      _handlers ->
        Dispatcher.dispatch(command, args, ctx)
    end
  end

  for path <- [:handlers, :ast] do
    @path path

    describe "#{@path}: sorted set scores" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "are answered as Redis formats doubles", %{ctx: ctx, key: key} do
        assert 1 == run(ctx, "ZADD", [key, "1", "a"])
        assert "1" == run(ctx, "ZSCORE", [key, "a"])
        assert "3.5" == run(ctx, "ZINCRBY", [key, "2.5", "a"])
        assert "4" == run(ctx, "ZINCRBY", [key, "0.5", "a"])
        assert ["4", nil] == run(ctx, "ZMSCORE", [key, "a", "missing"])

        assert 3 ==
                 run(ctx, "ZADD", [key, "1.5e-7", "b", "1e20", "c", "100000000000000000", "d"])

        assert ["b", "1.5e-7", "a", "4", "d", "100000000000000000", "c", "1e+20"] ==
                 run(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])

        assert ["c", "1e+20"] == run(ctx, "ZPOPMAX", [key])
        assert ["b", "1.5e-7"] == run(ctx, "ZPOPMIN", [key])

        assert ["a", "4"] ==
                 run(ctx, "ZRANGEBYSCORE", [key, "-inf", "+inf", "WITHSCORES", "LIMIT", "0", "1"])
      end
    end

    describe "#{@path}: ZRANGE with Redis 6.2 options" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      setup %{ctx: ctx, key: key} do
        3 = run(ctx, "ZADD", [key, "1", "a", "2", "b", "3", "c"])
        :ok
      end

      test "BYSCORE takes score bounds, exclusive ones too", %{ctx: ctx, key: key} do
        assert ["a"] == run(ctx, "ZRANGE", [key, "0", "(2", "BYSCORE"])

        assert ["b", "2", "c", "3"] ==
                 run(ctx, "ZRANGE", [key, "(1", "3", "BYSCORE", "WITHSCORES"])

        assert [] == run(ctx, "ZRANGE", [key, "0", "-1", "BYSCORE"])
        assert ["b"] == run(ctx, "ZRANGE", [key, "-inf", "+inf", "BYSCORE", "LIMIT", "1", "1"])
      end

      test "REV reverses a rank range and swaps score bounds", %{ctx: ctx, key: key} do
        assert ["c", "b"] == run(ctx, "ZRANGE", [key, "0", "1", "REV"])

        assert ["c", "3", "b", "2", "a", "1"] ==
                 run(ctx, "ZRANGE", [key, "0", "-1", "REV", "WITHSCORES"])

        assert ["c", "b", "a"] == run(ctx, "ZRANGE", [key, "3", "1", "BYSCORE", "REV"])
        assert [] == run(ctx, "ZRANGE", [key, "1", "3", "BYSCORE", "REV"])

        assert ["c", "b"] ==
                 run(ctx, "ZRANGE", [key, "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "0", "2"])
      end

      test "LIMIT needs BYSCORE", %{ctx: ctx, key: key} do
        assert {:error,
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"} ==
                 run(ctx, "ZRANGE", [key, "0", "-1", "LIMIT", "0", "1"])
      end

      test "a wrong type is reported for every form", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert @wrongtype == run(ctx, "ZRANGE", [key <> ":s", "0", "-1", "REV"])
        assert @wrongtype == run(ctx, "ZRANGE", [key <> ":s", "0", "1", "BYSCORE"])
      end
    end

    describe "#{@path}: list pops and LPOS" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "LPOP and RPOP with a count answer an array, even for one element", %{
        ctx: ctx,
        key: key
      } do
        assert 3 == run(ctx, "RPUSH", [key, "x", "y", "z"])
        assert ["x"] == run(ctx, "LPOP", [key, "1"])
        assert ["z"] == run(ctx, "RPOP", [key, "1"])
        assert "y" == run(ctx, "LPOP", [key])
        assert nil == run(ctx, "LPOP", [key, "1"])
      end

      test "LPOS with COUNT answers an empty array for a missing key", %{ctx: ctx, key: key} do
        assert [] == run(ctx, "LPOS", [key, "x", "COUNT", "1"])
        assert [] == run(ctx, "LPOS", [key, "x", "COUNT", "0"])
        assert nil == run(ctx, "LPOS", [key, "x"])
      end
    end

    describe "#{@path}: string and bitmap ranges" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "GETRANGE clamps a negative end that falls before the start", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key, "hello"])
        assert "h" == run(ctx, "GETRANGE", [key, "0", "-10"])
        assert "h" == run(ctx, "GETRANGE", [key, "-10", "-8"])
        assert "h" == run(ctx, "GETRANGE", [key, "0", "-5"])
        assert "" == run(ctx, "GETRANGE", [key, "3", "1"])
        assert "" == run(ctx, "GETRANGE", [key, "-1", "-5"])
        assert "" == run(ctx, "GETRANGE", [key, "10", "20"])
        assert "hello" == run(ctx, "GETRANGE", [key, "-100", "100"])
      end

      test "BITPOS answers -1 for a start byte past the value", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key, "c"])
        assert -1 == run(ctx, "BITPOS", [key, "0", "2"])
        assert -1 == run(ctx, "BITPOS", [key, "0", "1"])
        assert -1 == run(ctx, "BITPOS", [key, "1", "2"])
        assert 0 == run(ctx, "BITPOS", [key, "0", "0"])
        # A search without an end still counts the value as padded with zeros.
        :ok = run(ctx, "SET", [key <> ":ones", <<255>>])
        assert 8 == run(ctx, "BITPOS", [key <> ":ones", "0"])
        assert 8 == run(ctx, "BITPOS", [key <> ":ones", "0", "0"])
        assert -1 == run(ctx, "BITPOS", [key <> ":ones", "0", "0", "0"])
        assert -1 == run(ctx, "BITPOS", [key, "0", "8", "100", "BIT"])
      end
    end

    describe "#{@path}: key moves" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "COPY onto itself is refused, whether or not the key exists", %{ctx: ctx, key: key} do
        same = {:error, "ERR source and destination objects are the same"}
        assert same == run(ctx, "COPY", [key, key])
        :ok = run(ctx, "SET", [key, "v"])
        assert same == run(ctx, "COPY", [key, key, "REPLACE"])
      end

      test "SMOVE checks the destination's type even when the member is absent", %{
        ctx: ctx,
        key: key
      } do
        source = key <> ":src"
        destination = key <> ":dst"
        assert 1 == run(ctx, "SADD", [source, "a"])
        :ok = run(ctx, "SET", [destination, "x"])
        assert @wrongtype == run(ctx, "SMOVE", [source, destination, "missing"])
        assert @wrongtype == run(ctx, "SMOVE", [source, destination, "a"])
        assert ["a"] == run(ctx, "SMEMBERS", [source])
      end
    end
  end
end
