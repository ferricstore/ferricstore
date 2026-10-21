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

    describe "#{@path}: ZADD INCR" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "increments like ZINCRBY, under NX, XX, GT and LT", %{ctx: ctx, key: key} do
        assert "5" == run(ctx, "ZADD", [key, "INCR", "5", "a"])
        assert "7.5" == run(ctx, "ZADD", [key, "incr", "2.5", "a"])

        assert {:error, "ERR INCR option supports a single increment-element pair"} ==
                 run(ctx, "ZADD", [key, "INCR", "1", "a", "2", "b"])

        assert nil == run(ctx, "ZADD", [key, "NX", "INCR", "1", "a"])
        assert "1" == run(ctx, "ZADD", [key, "NX", "INCR", "1", "c"])
        assert "8.5" == run(ctx, "ZADD", [key, "XX", "INCR", "1", "a"])
        assert nil == run(ctx, "ZADD", [key, "XX", "INCR", "1", "d"])
        assert nil == run(ctx, "ZADD", [key, "GT", "INCR", "-1", "a"])
        assert "9.5" == run(ctx, "ZADD", [key, "GT", "INCR", "1", "a"])
        assert nil == run(ctx, "ZADD", [key, "LT", "INCR", "1", "a"])
        assert "-90.5" == run(ctx, "ZADD", [key, "LT", "INCR", "-100", "a"])
        assert "-89.5" == run(ctx, "ZADD", [key, "CH", "INCR", "1", "a"])
        assert ["a", "-89.5", "c", "1"] == run(ctx, "ZRANGE", [key, "0", "-1", "WITHSCORES"])

        assert {:error, "ERR value is not a valid float"} ==
                 run(ctx, "ZADD", [key, "INCR", "x", "a"])

        fresh = key <> ":fresh"
        assert nil == run(ctx, "ZADD", [fresh, "XX", "INCR", "1", "a"])
        assert 0 == run(ctx, "EXISTS", [fresh])
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert @wrongtype == run(ctx, "ZADD", [key <> ":s", "INCR", "1", "a"])
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

      test "a negative stop before the first member is an empty range", %{ctx: ctx, key: key} do
        assert [] == run(ctx, "ZRANGE", [key, "-5", "-4"])
        assert [] == run(ctx, "ZREVRANGE", [key, "-5", "-4", "WITHSCORES"])
        assert ["a"] == run(ctx, "ZRANGE", [key, "-5", "-3"])
        assert 1 == run(ctx, "ZADD", [key <> ":one", "2.5", "b"])
        assert [] == run(ctx, "ZREVRANGE", [key <> ":one", "-1", "-2", "WITHSCORES"])
      end

      test "ZRANK and ZREVRANK WITHSCORE answer the rank and the score", %{ctx: ctx, key: key} do
        assert [1, "2"] == run(ctx, "ZRANK", [key, "b", "WITHSCORE"])
        assert [0, "3"] == run(ctx, "ZREVRANK", [key, "c", "withscore"])
        assert nil == run(ctx, "ZRANK", [key, "missing", "WITHSCORE"])
        assert nil == run(ctx, "ZRANK", [key <> ":missing", "a", "WITHSCORE"])
        assert {:error, "ERR syntax error"} == run(ctx, "ZRANK", [key, "b", "WITHSCORES"])
        assert 1 == run(ctx, "ZRANK", [key, "b"])
      end

      test "LIMIT needs BYSCORE", %{ctx: ctx, key: key} do
        assert {:error,
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"} ==
                 run(ctx, "ZRANGE", [key, "0", "-1", "LIMIT", "0", "1"])
      end

      test "bad score bounds are reported before a wrong type", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        not_float = {:error, "ERR min or max is not a float"}
        assert not_float == run(ctx, "ZRANGE", [key <> ":s", "abc", "+inf", "BYSCORE"])
        assert not_float == run(ctx, "ZRANGEBYSCORE", [key <> ":s", "abc", "+inf"])
        assert not_float == run(ctx, "ZREVRANGEBYSCORE", [key <> ":s", "+inf", "(x"])
        assert not_float == run(ctx, "ZCOUNT", [key <> ":s", "-inf", "nope"])
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

      test "a list emptied by RPOPLPUSH or LMOVE is gone", %{ctx: ctx, key: key} do
        for {move, args} <- [
              {"RPOPLPUSH", []},
              {"LMOVE", ["LEFT", "RIGHT"]}
            ] do
          source = "#{key}:#{move}:src"
          assert 1 == run(ctx, "RPUSH", [source, "a"])
          assert "a" == run(ctx, move, [source, "#{key}:#{move}:dst" | args])
          # HSET and DEL, unlike TYPE or EXISTS, trip over a type left behind.
          assert 1 == run(ctx, "HSET", [source, "f", "v"])
          assert 1 == run(ctx, "DEL", [source])
        end
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
        # Two negative indexes with the start after the end are empty before
        # any clamping.
        assert "" == run(ctx, "GETRANGE", [key, "-10", "-20"])
        assert "" == run(ctx, "GETRANGE", [key, "-5", "-6"])
        assert "h" == run(ctx, "GETRANGE", [key, "-100", "-100"])
      end

      test "BITOP with an empty result deletes the destination", %{ctx: ctx, key: key} do
        destination = key <> ":dst"
        :ok = run(ctx, "SET", [destination, "x"])
        assert 0 == run(ctx, "BITOP", ["AND", destination, key <> ":m1", key <> ":m2"])
        assert nil == run(ctx, "GET", [destination])
        assert 0 == run(ctx, "DEL", [destination])

        assert 1 == run(ctx, "SADD", [destination, "member"])
        assert 0 == run(ctx, "BITOP", ["NOT", destination, key <> ":m3"])
        assert 0 == run(ctx, "EXISTS", [destination])

        assert 0 == run(ctx, "BITOP", ["OR", destination, key <> ":m4"])
        assert 0 == run(ctx, "DEL", [destination])
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

      test "BITPOS finds no bit in an empty string", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key, ""])

        for range <- [[], ["0"], ["0", "-1"], ["0", "5", "BIT"]], bit <- ["0", "1"] do
          assert -1 == run(ctx, "BITPOS", [key, bit | range]), inspect([bit | range])
        end
      end

      test "BITPOS reads a missing key as clear bits in every form", %{ctx: ctx, key: key} do
        missing = key <> ":missing"

        for range <- [[], ["5"], ["0", "15"], ["0", "15", "BYTE"], ["0", "15", "BIT"]] do
          assert 0 == run(ctx, "BITPOS", [missing, "0" | range]), inspect(range)
          assert -1 == run(ctx, "BITPOS", [missing, "1" | range]), inspect(range)
        end
      end
    end

    # Entries keep FerricStore's documented flat form, [id, field, value, ...].
    describe "#{@path}: stream IDs" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "a bare millisecond ID means sequence 0, and ms-* picks the next", %{
        ctx: ctx,
        key: key
      } do
        smaller =
          {:error,
           "ERR The ID specified in XADD is equal or smaller than the target stream top item"}

        assert "5-0" == run(ctx, "XADD", [key, "5", "f", "a"])
        assert smaller == run(ctx, "XADD", [key, "5", "f", "b"])
        assert "5-1" == run(ctx, "XADD", [key, "5-*", "f", "c"])
        assert "7-0" == run(ctx, "XADD", [key, "007", "f", "d"])
        assert "8-1" == run(ctx, "XADD", [key, "8-01", "f", "e"])
        assert 1 == run(ctx, "XDEL", [key, "5"])
        assert 1 == run(ctx, "XDEL", [key, "05-01"])
        assert 2 == run(ctx, "XLEN", [key])

        assert {:error, "ERR The ID specified in XADD must be greater than 0-0"} ==
                 run(ctx, "XADD", [key <> ":fresh", "0", "f", "z"])
      end

      test "XADD NOMKSTREAM checks the key's type", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key, "v"])
        assert @wrongtype == run(ctx, "XADD", [key, "NOMKSTREAM", "0-1", "f", "b"])
        assert nil == run(ctx, "XADD", [key <> ":missing", "NOMKSTREAM", "5", "f", "b"])
        assert 0 == run(ctx, "EXISTS", [key <> ":missing"])
      end
    end

    describe "#{@path}: stream ranges" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      setup %{ctx: ctx, key: key} do
        for {id, value} <- [{"1-1", "a"}, {"2-0", "b"}, {"3-5", "c"}],
            do: ^id = run(ctx, "XADD", [key, id, "f", value])

        :ok
      end

      test "take - and + in either position and ( for exclusive bounds", %{ctx: ctx, key: key} do
        assert [] == run(ctx, "XRANGE", [key, "3-5", "-"])
        assert [] == run(ctx, "XRANGE", [key, "+", "-"])
        assert [] == run(ctx, "XREVRANGE", [key, "-", "+"])
        assert [["2-0", "f", "b"], ["3-5", "f", "c"]] == run(ctx, "XRANGE", [key, "(1-1", "+"])
        assert [["1-1", "f", "a"], ["2-0", "f", "b"]] == run(ctx, "XRANGE", [key, "-", "(3-5"])
        assert [["2-0", "f", "b"], ["1-1", "f", "a"]] == run(ctx, "XREVRANGE", [key, "(3-5", "-"])
        assert [] == run(ctx, "XRANGE", [key, "(2-0", "(2-0"])
      end

      test "an end without a sequence spans its millisecond", %{ctx: ctx, key: key} do
        assert [["1-1", "f", "a"]] == run(ctx, "XRANGE", [key, "1", "1"])
        assert [["1-1", "f", "a"]] == run(ctx, "XREVRANGE", [key, "1", "1"])
      end

      test "an exclusive bound past the last ID is refused", %{ctx: ctx, key: key} do
        assert {:error, "ERR invalid start ID for the interval"} ==
                 run(ctx, "XRANGE", [key, "(18446744073709551615-18446744073709551615", "+"])

        assert {:error, "ERR invalid end ID for the interval"} ==
                 run(ctx, "XRANGE", [key, "0-0", "(0-0"])
      end

      test "COUNT 0 answers nil for a stream and an empty array for a missing key", %{
        ctx: ctx,
        key: key
      } do
        assert nil == run(ctx, "XRANGE", [key, "-", "+", "COUNT", "0"])
        assert nil == run(ctx, "XREVRANGE", [key, "+", "-", "COUNT", "0"])
        assert [] == run(ctx, "XRANGE", [key <> ":missing", "-", "+", "COUNT", "0"])
      end

      test "XADD refuses the ID 0-0 before looking at the key", %{ctx: ctx, key: key} do
        refused = {:error, "ERR The ID specified in XADD must be greater than 0-0"}
        assert refused == run(ctx, "XADD", [key <> ":missing", "NOMKSTREAM", "0-0", "f", "x"])
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert refused == run(ctx, "XADD", [key <> ":s", "0-0", "f", "x"])
      end
    end

    describe "#{@path}: TTLs in seconds" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "TTL and EXPIRETIME round to the nearest second", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key, "v", "PX", "1400"])
        assert 1 == run(ctx, "TTL", [key])
        assert 1 == run(ctx, "PEXPIRE", [key, "1600"])
        assert 2 == run(ctx, "TTL", [key])
        assert 1 == run(ctx, "PEXPIREAT", [key, "1800000001400"])
        assert 1_800_000_001 == run(ctx, "EXPIRETIME", [key])
        assert 1 == run(ctx, "PEXPIREAT", [key, "1800000001600"])
        assert 1_800_000_002 == run(ctx, "EXPIRETIME", [key])
      end

      test "HTTL and HEXPIRETIME round up", %{ctx: ctx, key: key} do
        assert 2 == run(ctx, "HSET", [key, "a", "1", "b", "2"])
        assert [1] == run(ctx, "HPEXPIRE", [key, "1400", "FIELDS", "1", "a"])
        before_ms = Ferricstore.CommandTime.now_ms()
        assert [1] == run(ctx, "HPEXPIRE", [key, "100400", "FIELDS", "1", "b"])
        after_ms = Ferricstore.CommandTime.now_ms()
        assert [2] == run(ctx, "HTTL", [key, "FIELDS", "1", "a"])
        assert [expire_at_s] = run(ctx, "HEXPIRETIME", [key, "FIELDS", "1", "b"])
        assert expire_at_s * 1_000 >= before_ms + 100_400
        assert expire_at_s * 1_000 < after_ms + 101_400
      end
    end

    describe "#{@path}: hash field expiry" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "a TTL of 0 deletes the field and answers 2", %{ctx: ctx, key: key} do
        assert 2 == run(ctx, "HSET", [key, "a", "1", "b", "2"])
        assert [2, -2] == run(ctx, "HEXPIRE", [key, "0", "FIELDS", "2", "a", "a"])
        assert ["b", "2"] == run(ctx, "HGETALL", [key])
        assert [2] == run(ctx, "HPEXPIRE", [key, "0", "FIELDS", "1", "b"])
        assert 0 == run(ctx, "EXISTS", [key])
        assert [-2, -2] == run(ctx, "HEXPIRE", [key, "0", "NX", "FIELDS", "2", "a", "b"])
      end

      test "NX, XX, GT and LT go anywhere after the TTL", %{ctx: ctx, key: key} do
        assert 2 == run(ctx, "HSET", [key, "a", "1", "b", "2"])
        assert [1, -2] == run(ctx, "HEXPIRE", [key, "100", "NX", "FIELDS", "2", "a", "zz"])
        assert [0] == run(ctx, "HEXPIRE", [key, "100", "NX", "FIELDS", "1", "a"])
        assert [0] == run(ctx, "HEXPIRE", [key, "50", "GT", "FIELDS", "1", "a"])
        assert [1] == run(ctx, "HEXPIRE", [key, "200", "gt", "FIELDS", "1", "a"])
        assert [1] == run(ctx, "HEXPIRE", [key, "10", "FIELDS", "1", "a", "lt"])
        assert [0] == run(ctx, "HEXPIRE", [key, "10", "XX", "FIELDS", "1", "b"])
        assert [0] == run(ctx, "HEXPIRE", [key, "10", "GT", "FIELDS", "1", "b"])
        assert [1] == run(ctx, "HEXPIRE", [key, "20", "LT", "FIELDS", "1", "b"])
        assert [1, 0, 0] == run(ctx, "HEXPIRE", [key, "5", "LT", "FIELDS", "3", "a", "a", "a"])
        assert [a_ms, b_ms] = run(ctx, "HPTTL", [key, "FIELDS", "2", "a", "b"])
        assert a_ms in 4_000..5_000 and b_ms in 19_000..20_000
        assert [-2, 0] == run(ctx, "HPEXPIRE", [key, "8000", "NX", "NX", "FIELDS", "2", "c", "a"])
        assert [0] == run(ctx, "HEXPIRE", [key, "0", "NX", "FIELDS", "1", "a"])
        assert [2] == run(ctx, "HPEXPIRE", [key, "0", "XX", "FIELDS", "1", "a"])
        assert ["b", "2"] == run(ctx, "HGETALL", [key])
      end

      test "argument errors follow the type check", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert @wrongtype == run(ctx, "HEXPIRE", [key <> ":s", "-1", "FIELDS", "1", "a"])
        assert 1 == run(ctx, "HSET", [key, "a", "1"])

        for {args, message} <- [
              {["-1", "FIELDS", "1", "a"], "ERR invalid expire time, must be >= 0"},
              {["x", "FIELDS", "1", "a"], "ERR value is not an integer or out of range"},
              {["9999999999999", "FIELDS", "1", "a"],
               "ERR invalid expire time in 'hexpire' command"},
              {["10", "NX", "GT", "FIELDS", "1", "a"], "ERR Multiple condition flags specified"},
              {["10", "FIELDS", "1", "a", "FIELDS", "1", "b"],
               "ERR FIELDS keyword specified multiple times"},
              {["10", "NX", "NX", "NX"], "ERR missing FIELDS argument"},
              {["10", "FOO", "FIELDS", "1", "a"], "ERR unknown argument: FOO"},
              {["10", "FIELDS", "1", "a", "b"], "ERR unknown argument: b"},
              {["10", "FIELDS", "0", "a"], "ERR Parameter `numFields` should be greater than 0"},
              {["10", "FIELDS", "5", "a", "b"], "ERR wrong number of arguments"},
              {["10", "NX", "FIELDS", "x"],
               "ERR FIELDS requires at least numfields and one field argument"}
            ] do
          assert {:error, message} == run(ctx, "HEXPIRE", [key | args]), inspect(args)
        end

        assert [-1] == run(ctx, "HTTL", [key, "FIELDS", "1", "a"])
      end
    end

    describe "#{@path}: hash field expiry limits" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "the latest expiry is Redis's", %{ctx: ctx, key: key} do
        assert 1 == run(ctx, "HSET", [key, "a", "1"])

        for {command, time} <- [{"HEXPIRE", "69000000000"}, {"HPEXPIRE", "69000000000000"}] do
          assert {:error, "ERR invalid expire time in '#{String.downcase(command)}' command"} ==
                   run(ctx, command, [key, time, "FIELDS", "1", "a"])
        end

        assert [1] == run(ctx, "HEXPIRE", [key, "68000000000", "FIELDS", "1", "a"])
        assert [1] == run(ctx, "HPEXPIRE", [key, "68000000000000", "FIELDS", "1", "a"])
      end
    end

    describe "#{@path}: GETEX and SETRANGE error order" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "GETEX looks the key up before it checks the expiry value", %{ctx: ctx, key: key} do
        missing = key <> ":missing"
        invalid = {:error, "ERR invalid expire time in 'getex' command"}

        for option <- [["EX", "0"], ["EX", "x"], ["PX", "-5"]] do
          assert nil == run(ctx, "GETEX", [missing | option]), inspect(option)
        end

        assert {:error, "ERR syntax error"} == run(ctx, "GETEX", [missing, "FOO"])
        assert 1 == run(ctx, "RPUSH", [key <> ":l", "a"])
        assert @wrongtype == run(ctx, "GETEX", [key <> ":l", "EX", "0"])
        :ok = run(ctx, "SET", [key, "v"])
        assert invalid == run(ctx, "GETEX", [key, "EX", "0"])
        assert invalid == run(ctx, "GETEX", [key, "PXAT", "-1"])
        assert invalid == run(ctx, "GETEX", [key, "EX", "9223372036854775807"])

        assert {:error, "ERR value is not an integer or out of range"} ==
                 run(ctx, "GETEX", [key, "EX", "x"])

        assert "v" == run(ctx, "GETEX", [key, "persist"])
      end

      test "INCRBYFLOAT checks the key's type before the increment", %{ctx: ctx, key: key} do
        assert 1 == run(ctx, "RPUSH", [key <> ":l", "a"])
        assert @wrongtype == run(ctx, "INCRBYFLOAT", [key <> ":l", "abc"])

        assert {:error, "ERR value is not a valid float"} ==
                 run(ctx, "INCRBYFLOAT", [key <> ":missing", "abc"])
      end

      test "SETRANGE checks the key's type before the resulting size", %{ctx: ctx, key: key} do
        assert 1 == run(ctx, "RPUSH", [key <> ":l", "a"])
        assert @wrongtype == run(ctx, "SETRANGE", [key <> ":l", "536870912", "x"])

        assert {:error, "ERR offset is out of range"} ==
                 run(ctx, "SETRANGE", [key <> ":l", "-1", "x"])

        assert {:error, "ERR string exceeds maximum allowed size (proto-max-bulk-len)"} ==
                 run(ctx, "SETRANGE", [key, "536870912", "x"])
      end
    end

    describe "#{@path}: hash field lists" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "HTTL and its family answer Redis's argument errors", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert 1 == run(ctx, "HSET", [key, "a", "1"])

        for command <- ~w(HTTL HPTTL HPERSIST HEXPIRETIME HGETDEL) do
          assert @wrongtype == run(ctx, command, [key <> ":s", "FIELDS", "0", "a"]), command

          for {args, message} <- [
                {["FIELDS", "0", "a"], "ERR Number of fields must be a positive integer"},
                {["FIELDS", "01", "a"], "ERR Number of fields must be a positive integer"},
                {["FIELDS", "2", "a"],
                 "ERR The `numfields` parameter must match the number of arguments"},
                {["FOO", "1", "a"],
                 "ERR Mandatory argument FIELDS is missing or not at the right position"}
              ] do
            assert {:error, message} == run(ctx, command, [key | args]),
                   "#{command} #{inspect(args)}"
          end
        end

        assert [-1] == run(ctx, "HTTL", [key, "fields", "1", "a"])
        assert ["1"] == run(ctx, "HGETDEL", [key, "fields", "1", "a"])
      end
    end

    describe "#{@path}: HGETEX and HSETEX" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "HGETEX with a TTL of 0 or a past time deletes the fields", %{ctx: ctx, key: key} do
        assert 3 == run(ctx, "HSET", [key, "a", "1", "b", "2", "c", "3"])
        assert ["1", nil] == run(ctx, "HGETEX", [key, "EX", "0", "FIELDS", "2", "a", "a"])
        assert ["b", "2", "c", "3"] == run(ctx, "HGETALL", [key])
        assert ["2"] == run(ctx, "HGETEX", [key, "PX", "0", "FIELDS", "1", "b"])
        assert ["3"] == run(ctx, "HGETEX", [key, "EXAT", "1", "FIELDS", "1", "c"])
        assert 0 == run(ctx, "EXISTS", [key])
        assert 1 == run(ctx, "HSET", [key, "a", "1"])
        assert ["1"] == run(ctx, "HGETEX", [key, "FIELDS", "1", "a", "EX", "100"])
        assert [100] == run(ctx, "HTTL", [key, "FIELDS", "1", "a"])
        assert ["1"] == run(ctx, "HGETEX", [key, "fields", "1", "a", "persist"])
        assert [-1] == run(ctx, "HTTL", [key, "FIELDS", "1", "a"])

        assert [nil, nil] ==
                 run(ctx, "HGETEX", [key <> ":missing", "EX", "1", "FIELDS", "2", "a", "b"])
      end

      test "HGETEX argument errors follow the type check", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert @wrongtype == run(ctx, "HGETEX", [key <> ":s", "EX", "-1", "FIELDS", "1", "a"])
        assert 1 == run(ctx, "HSET", [key, "a", "1"])
        only_one = "ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified"

        for {args, message} <- [
              {["EX", "10", "PX", "10", "FIELDS", "1", "a"], only_one},
              {["PERSIST", "PERSIST", "FIELDS", "1", "a"], only_one},
              {["FIELDS", "1", "a", "b"], "ERR unknown argument: b"},
              {["FIELDS", "0", "a"], "ERR invalid number of fields"},
              {["FIELDS", "2", "a"], "ERR wrong number of arguments"},
              {["FIELDS", "1", "a", "FIELDS", "1", "a"],
               "ERR FIELDS keyword specified multiple times"},
              {["EX", "-1", "FIELDS", "1", "a"], "ERR invalid expire time, must be >= 0"},
              {["EX", "x", "FIELDS", "1", "a"], "ERR value is not an integer or out of range"},
              {["PXAT", "70368744177664", "FIELDS", "1", "a"],
               "ERR invalid expire time in 'hgetex' command"}
            ] do
          assert {:error, message} == run(ctx, "HGETEX", [key | args]), inspect(args)
        end
      end

      test "HSETEX takes Redis 8's FNX, FXX, KEEPTTL and expiry options", %{ctx: ctx, key: key} do
        assert 1 == run(ctx, "HSETEX", [key, "EX", "100", "FIELDS", "2", "a", "1", "b", "2"])
        assert [100, 100] == run(ctx, "HTTL", [key, "FIELDS", "2", "a", "b"])
        assert 1 == run(ctx, "HSETEX", [key, "FIELDS", "1", "a", "9"])
        assert [-1, 100] == run(ctx, "HTTL", [key, "FIELDS", "2", "a", "b"])
        assert 1 == run(ctx, "HSETEX", [key, "KEEPTTL", "FIELDS", "1", "b", "9"])
        assert [100] == run(ctx, "HTTL", [key, "FIELDS", "1", "b"])
        assert 0 == run(ctx, "HSETEX", [key, "FNX", "FIELDS", "2", "a", "1", "c", "3"])
        assert 1 == run(ctx, "HSETEX", [key, "FNX", "FIELDS", "1", "c", "3"])
        assert 0 == run(ctx, "HSETEX", [key, "FXX", "FIELDS", "2", "c", "4", "d", "4"])

        assert 1 ==
                 run(ctx, "HSETEX", [key, "fxx", "px", "5000", "fields", "2", "c", "5", "a", "5"])

        assert [c_ms, a_ms] = run(ctx, "HPTTL", [key, "FIELDS", "2", "c", "a"])
        assert c_ms in 4_000..5_000 and a_ms in 4_000..5_000
        assert ["5", "9", "5"] == run(ctx, "HMGET", [key, "a", "b", "c"])
        assert 1 == run(ctx, "HSETEX", [key, "EX", "0", "FIELDS", "2", "a", "1", "zz", "1"])
        assert [nil, nil] == run(ctx, "HMGET", [key, "a", "zz"])
        assert 1 == run(ctx, "HSETEX", [key, "FIELDS", "2", "q", "1", "q", "2"])
        assert "2" == run(ctx, "HGET", [key, "q"])

        fresh = key <> ":fresh"
        assert 0 == run(ctx, "HSETEX", [fresh, "FXX", "FIELDS", "1", "a", "1"])
        assert 0 == run(ctx, "EXISTS", [fresh])
        assert 1 == run(ctx, "HSETEX", [fresh, "EX", "0", "FNX", "FIELDS", "1", "a", "1"])
        assert 0 == run(ctx, "EXISTS", [fresh])
        # FerricStore's own form: HSETEX key seconds field value [...]
        assert 1 == run(ctx, "HSETEX", [fresh, "10", "z", "1"])
        assert [10] == run(ctx, "HTTL", [fresh, "FIELDS", "1", "z"])
      end

      test "HSETEX checks its arguments before the key's type", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert @wrongtype == run(ctx, "HSETEX", [key <> ":s", "FIELDS", "1", "a", "1"])
        expiry_once = "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified"

        for {args, message} <- [
              {["EX", "-1", "FIELDS", "1", "a", "1"], "ERR invalid expire time, must be >= 0"},
              {["FNX", "FXX", "FIELDS", "1", "a", "1"],
               "ERR Only one of FXX or FNX arguments can be specified"},
              {["EX", "10", "KEEPTTL", "FIELDS", "1", "a", "1"], expiry_once},
              {["KEEPTTL", "KEEPTTL", "FIELDS", "1", "a", "1"], expiry_once},
              {["FIELDS", "1", "a", "1", "b", "2"], "ERR unknown argument: b"},
              {["FIELDS", "0", "a", "1"], "ERR invalid number of fields"},
              {["FIELDS", "2", "a", "1", "b"], "ERR wrong number of arguments"},
              {["FIELDS", "1", "a", "1", "FIELDS", "1", "b", "1"],
               "ERR FIELDS keyword specified multiple times"}
            ] do
          assert {:error, message} == run(ctx, "HSETEX", [key <> ":s" | args]), inspect(args)
        end
      end
    end

    describe "#{@path}: argument error texts" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "match Redis's", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key, "1"])

        for command <- ["EXPIRE", "PEXPIRE"] do
          assert {:error, "ERR invalid expire time in '#{String.downcase(command)}' command"} ==
                   run(ctx, command, [key, "9223372036854775807"])
        end

        assert 1 == run(ctx, "SADD", [key <> ":s", "a"])

        assert {:error, "ERR numkeys should be greater than 0"} ==
                 run(ctx, "SINTERCARD", ["0", key <> ":s"])

        assert {:error, "ERR value is out of range, must be positive"} ==
                 run(ctx, "SPOP", [key <> ":s", "-1"])

        assert {:error, "ERR syntax error"} == run(ctx, "SET", [key, "v", "NX", "XX"])

        for expiry <- [["EX", "0"], ["PX", "-1"], ["EXAT", "0"]] do
          assert {:error, "ERR invalid expire time in 'set' command"} ==
                   run(ctx, "SET", [key, "v" | expiry]),
                 inspect(expiry)
        end
      end
    end

    describe "#{@path}: HSCAN NOVALUES" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "answers the fields only", %{ctx: ctx, key: key} do
        assert 2 == run(ctx, "HSET", [key, "a", "1", "b", "2"])
        assert ["0", fields] = run(ctx, "HSCAN", [key, "0", "NOVALUES"])
        assert Enum.sort(fields) == ["a", "b"]

        assert ["0", ["a"]] ==
                 run(ctx, "HSCAN", [key, "0", "MATCH", "a", "novalues", "COUNT", "10"])

        assert ["0", pairs] = run(ctx, "HSCAN", [key, "0"])
        assert length(pairs) == 4
      end
    end

    describe "#{@path}: hyperloglog" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "PFMERGE takes a destination without sources", %{ctx: ctx, key: key} do
        assert :ok == run(ctx, "PFMERGE", [key])
        assert 0 == run(ctx, "PFCOUNT", [key])
        assert 1 == run(ctx, "PFADD", [key, "a"])
        assert :ok == run(ctx, "PFMERGE", [key])
        assert 1 == run(ctx, "PFCOUNT", [key])
        :ok = run(ctx, "SET", [key <> ":s", "not a sketch"])

        assert {:error, "WRONGTYPE Key is not a valid HyperLogLog string value."} ==
                 run(ctx, "PFMERGE", [key <> ":s"])
      end
    end

    describe "#{@path}: geo searches" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "distances use Redis's Earth radius and format", %{ctx: ctx, key: key} do
        assert 2 ==
                 run(ctx, "GEOADD", [
                   key,
                   "13.361389",
                   "38.115556",
                   "Palermo",
                   "15.087269",
                   "37.502669",
                   "Catania"
                 ])

        meters = run(ctx, "GEODIST", [key, "Palermo", "Catania"])
        assert meters =~ ~r/^\d+\.\d{4}$/
        assert_in_delta 166_274.1516, String.to_float(meters), 1.0
        assert "0.0000" == run(ctx, "GEODIST", [key, "Palermo", "Palermo", "km"])

        assert [["Palermo", "0.0000"], ["Catania", km]] =
                 run(ctx, "GEOSEARCH", [
                   key,
                   "FROMMEMBER",
                   "Palermo",
                   "BYRADIUS",
                   "200",
                   "km",
                   "WITHDIST",
                   "ASC"
                 ])

        assert_in_delta 166.2742, String.to_float(km), 0.001
        # Coordinates at full precision, not rounded to 4 decimals.
        assert [[lng, lat]] = run(ctx, "GEOPOS", [key, "Palermo"])
        assert byte_size(lng) > 10 and byte_size(lat) > 10
        assert_in_delta 13.361389, String.to_float(lng), 0.00001
        assert_in_delta 38.115556, String.to_float(lat), 0.00001
      end

      test "a missing key answers [] and empties GEOSEARCHSTORE's destination", %{
        ctx: ctx,
        key: key
      } do
        missing = key <> ":missing"
        destination = key <> ":dst"

        assert [] ==
                 run(ctx, "GEOSEARCH", [missing, "FROMMEMBER", "m", "BYRADIUS", "10", "km"])

        assert 1 == run(ctx, "GEOADD", [destination, "13.36", "38.11", "palermo"])

        assert 0 ==
                 run(ctx, "GEOSEARCHSTORE", [
                   destination,
                   missing,
                   "FROMMEMBER",
                   "m",
                   "BYRADIUS",
                   "10",
                   "km"
                 ])

        assert 0 == run(ctx, "EXISTS", [destination])

        assert 1 == run(ctx, "GEOADD", [key, "13.36", "38.11", "palermo"])

        assert {:error, "ERR could not decode requested zset member"} ==
                 run(ctx, "GEOSEARCH", [key, "FROMMEMBER", "nope", "BYRADIUS", "10", "km"])
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
