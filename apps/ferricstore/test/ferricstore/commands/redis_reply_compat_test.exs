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

      test "BITPOS reads a missing key as clear bits in every form", %{ctx: ctx, key: key} do
        missing = key <> ":missing"

        for range <- [[], ["5"], ["0", "15"], ["0", "15", "BYTE"], ["0", "15", "BIT"]] do
          assert 0 == run(ctx, "BITPOS", [missing, "0" | range]), inspect(range)
          assert -1 == run(ctx, "BITPOS", [missing, "1" | range]), inspect(range)
        end
      end
    end

    # Entries keep FerricStore's documented flat form, [id, field, value, ...].
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

    describe "#{@path}: hash field expiry at a time" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "HEXPIREAT, HPEXPIREAT and HPEXPIRETIME", %{ctx: ctx, key: key} do
        assert 3 == run(ctx, "HSET", [key, "a", "1", "b", "2", "c", "3"])
        assert [2] == run(ctx, "HEXPIREAT", [key, "0", "FIELDS", "1", "a"])
        assert [2] == run(ctx, "HEXPIREAT", [key, "1", "NX", "FIELDS", "1", "b"])
        assert ["c", "3"] == run(ctx, "HGETALL", [key])
        assert [1, -2] == run(ctx, "HPEXPIREAT", [key, "1800000001400", "FIELDS", "2", "c", "zz"])

        assert [1_800_000_001_400, -2] ==
                 run(ctx, "HPEXPIRETIME", [key, "FIELDS", "2", "c", "zz"])

        assert [1_800_000_002] == run(ctx, "HEXPIRETIME", [key, "FIELDS", "1", "c"])
        assert [0] == run(ctx, "HEXPIREAT", [key, "1800000000", "GT", "FIELDS", "1", "c"])
        assert [1] == run(ctx, "HEXPIREAT", [key, "1800000002", "gt", "FIELDS", "1", "c"])
        assert [1_800_000_002_000] == run(ctx, "HPEXPIRETIME", [key, "FIELDS", "1", "c"])
        assert 1 == run(ctx, "HSET", [key, "d", "4"])
        assert [-1] == run(ctx, "HPEXPIRETIME", [key, "FIELDS", "1", "d"])
        assert [-2, -2] == run(ctx, "HPEXPIRETIME", [key <> ":missing", "FIELDS", "2", "a", "b"])
        assert [1] == run(ctx, "HEXPIREAT", [key, "70368744177", "FIELDS", "1", "d"])
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert @wrongtype == run(ctx, "HPEXPIRETIME", [key <> ":s", "FIELDS", "1", "a"])
        assert @wrongtype == run(ctx, "HEXPIREAT", [key <> ":s", "-1", "FIELDS", "1", "a"])
      end

      test "the latest expiry is Redis's", %{ctx: ctx, key: key} do
        assert 1 == run(ctx, "HSET", [key, "a", "1"])

        for {command, time} <- [
              {"HPEXPIREAT", "70368744177664"},
              {"HEXPIREAT", "70368744178"},
              {"HEXPIRE", "69000000000"},
              {"HPEXPIRE", "69000000000000"}
            ] do
          assert {:error, "ERR invalid expire time in '#{String.downcase(command)}' command"} ==
                   run(ctx, command, [key, time, "FIELDS", "1", "a"])
        end

        assert {:error, "ERR invalid expire time, must be >= 0"} ==
                 run(ctx, "HEXPIREAT", [key, "-1", "FIELDS", "1", "a"])

        assert [1] == run(ctx, "HPEXPIREAT", [key, "70368744177663", "FIELDS", "1", "a"])
        assert [1] == run(ctx, "HEXPIRE", [key, "68000000000", "FIELDS", "1", "a"])
      end
    end

    describe "#{@path}: hash field lists" do
      setup do: Process.put(:compat_path, @path) |> then(fn _previous -> :ok end)

      test "HTTL and its family answer Redis's argument errors", %{ctx: ctx, key: key} do
        :ok = run(ctx, "SET", [key <> ":s", "v"])
        assert 1 == run(ctx, "HSET", [key, "a", "1"])

        for command <- ~w(HTTL HPTTL HPERSIST HEXPIRETIME HPEXPIRETIME HGETDEL) do
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
