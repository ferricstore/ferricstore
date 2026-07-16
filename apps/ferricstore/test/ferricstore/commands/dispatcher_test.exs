defmodule Ferricstore.Commands.DispatcherTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # Routing to Strings handler
  # ---------------------------------------------------------------------------

  test "dispatches SET to Strings handler" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("SET", ["k", "v"], store)
    assert "v" == store.get.("k")
  end

  test "dispatches Rust SET AST to Strings AST handler" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch_ast({:set, "k", "v", [:nx]}, store)
    assert "v" == store.get.("k")
    assert nil == Dispatcher.dispatch_ast({:set, "k", "new", [:nx]}, store)
    assert "v" == store.get.("k")
  end

  test "dispatches Rust string command ASTs" do
    store = MockStore.make(%{"k" => {"1", 0}, "gone" => {"x", 0}})

    assert "1" == Dispatcher.dispatch_ast({:get, "k"}, store)
    assert :ok = Dispatcher.dispatch_ast({:mset, ["a", "10", "b", "20"]}, store)
    assert ["10", "20"] == Dispatcher.dispatch_ast({:mget, ["a", "b"]}, store)
    assert {:ok, 2} == Dispatcher.dispatch_ast({:incr, "k"}, store)
    assert 1 == Dispatcher.dispatch_ast({:del, ["gone"]}, store)
    assert 0 == Dispatcher.dispatch_ast({:exists, ["gone"]}, store)
  end

  test "known Rust AST tags with raw arg lists return wrong arity instead of unsupported" do
    store = MockStore.make()

    for {ast, cmd} <- [
          {{:set, ["k"]}, "set"},
          {{:incr, []}, "incr"},
          {{:lrange, ["list", "0"]}, "lrange"},
          {{:hget, ["hash"]}, "hget"},
          {{:sismember, ["set"]}, "sismember"},
          {{:zscore, ["zset"]}, "zscore"},
          {{:setbit, ["bits", "1"]}, "setbit"}
        ] do
      expected = "ERR wrong number of arguments for '#{cmd}' command"
      assert {:error, ^expected} = Dispatcher.dispatch_ast(ast, store)
    end
  end

  test "dispatches Rust server/admin command ASTs without raw command fallback" do
    store = MockStore.make(%{"k" => {"v", 0}})

    assert {:simple, "PONG"} == Dispatcher.dispatch_ast({:ping, []}, store)
    assert "hi" == Dispatcher.dispatch_ast({:echo, ["hi"]}, store)
    assert 1 == Dispatcher.dispatch_ast({:dbsize, []}, store)
    assert ["k"] == Dispatcher.dispatch_ast({:keys, ["*"]}, store)
    assert is_integer(Dispatcher.dispatch_ast({:command, ["COUNT"]}, store))

    assert {:error, "ERR SELECT not supported. Use named caches."} =
             Dispatcher.dispatch_ast({:select, ["0"]}, store)
  end

  test "dispatches Rust native and namespace command ASTs without raw command fallback" do
    store = MockStore.make()

    assert {:error, "ERR wrong number of arguments for 'cas' command"} =
             Dispatcher.dispatch_ast({:cas, []}, store)

    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:lock, "k", "owner", {:error, "ERR value is not an integer or out of range"}},
               store
             )

    assert {:error, "ERR wrong number of arguments for 'ferricstore.config' command"} =
             Dispatcher.dispatch_ast({:ferricstore_config, []}, store)

    assert {:error, "ERR wrong number of arguments for 'memory' command"} =
             Dispatcher.dispatch_ast({:memory, []}, store)
  end

  test "dispatches Rust typed string numeric ASTs" do
    store = MockStore.make(%{"k" => {"10", 0}, "s" => {"abcdef", 0}})

    assert {:ok, 15} == Dispatcher.dispatch_ast({:incrby, "k", 5}, store)
    assert {:ok, 12} == Dispatcher.dispatch_ast({:decrby, "k", 3}, store)
    assert "13.5" == Dispatcher.dispatch_ast({:incrbyfloat, "k", 1.5}, store)
    assert :ok == Dispatcher.dispatch_ast({:setex, "ttl", 10, "v"}, store)
    assert :ok == Dispatcher.dispatch_ast({:psetex, "pttl", 10, "v"}, store)
    assert "cde" == Dispatcher.dispatch_ast({:getrange, "s", 2, 4}, store)
    assert 6 == Dispatcher.dispatch_ast({:setrange, "s", 1, "ZZ"}, store)
  end

  test "dispatches Rust string numeric AST parse errors" do
    store = MockStore.make()

    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:incrby, "k", {:error, "ERR value is not an integer or out of range"}},
               store
             )

    assert {:error, "ERR value is not a valid float"} =
             Dispatcher.dispatch_ast(
               {:incrbyfloat, "k", {:error, "ERR value is not a valid float"}},
               store
             )
  end

  test "dispatches Rust typed GETEX AST" do
    store =
      MockStore.make(%{
        "k" => {"v", 0},
        "persist" => {"x", System.os_time(:millisecond) + 10_000}
      })

    assert "v" == Dispatcher.dispatch_ast({:getex, "k"}, store)
    assert "v" == Dispatcher.dispatch_ast({:getex, "k", {:ex, 10}}, store)
    assert "v" == Dispatcher.dispatch_ast({:getex, "k", {:px, 10}}, store)
    assert "v" == Dispatcher.dispatch_ast({:getex, "k", {:exat, 9_999_999_999}}, store)
    assert "v" == Dispatcher.dispatch_ast({:getex, "k", {:pxat, 9_999_999_999_999}}, store)
    assert "x" == Dispatcher.dispatch_ast({:getex, "persist", :persist}, store)
  end

  test "dispatches Rust GETEX parse errors" do
    assert {:error, "ERR syntax error"} =
             Dispatcher.dispatch_ast(
               {:getex, "k", {:error, "ERR syntax error"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed list ASTs" do
    store = MockStore.make()

    assert 3 == Dispatcher.dispatch_ast({:rpush, ["l", "a", "b", "c"]}, store)
    assert "b" == Dispatcher.dispatch_ast({:lindex, "l", 1}, store)
    assert ["a", "b"] == Dispatcher.dispatch_ast({:lrange, "l", 0, 1}, store)
    assert :ok == Dispatcher.dispatch_ast({:lset, "l", 1, "B"}, store)
    assert ["a", "B", "c"] == Dispatcher.dispatch_ast({:lrange, "l", 0, -1}, store)
    assert 4 == Dispatcher.dispatch_ast({:linsert, "l", :before, "B", "x"}, store)
    assert 1 == Dispatcher.dispatch_ast({:lrem, "l", 1, "x"}, store)
    assert "a" == Dispatcher.dispatch_ast({:lpop, "l", 1}, store)
    assert "c" == Dispatcher.dispatch_ast({:rpop, "l"}, store)
  end

  test "dispatches Rust list parse errors" do
    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:lpop, "l", {:error, "ERR value is not an integer or out of range"}},
               MockStore.make()
             )

    assert {:error, "ERR syntax error"} =
             Dispatcher.dispatch_ast(
               {:linsert, "l", {:error, "ERR syntax error"}, "p", "x"},
               MockStore.make()
             )
  end

  test "dispatches Rust typed hash ASTs" do
    store = MockStore.make()

    assert 2 == Dispatcher.dispatch_ast({:hset, ["h", "f1", "1", "f2", "2"]}, store)
    assert 6 == Dispatcher.dispatch_ast({:hincrby, "h", "f1", 5}, store)
    assert "3.5" == Dispatcher.dispatch_ast({:hincrbyfloat, "h", "f2", 1.5}, store)
    assert [1] == Dispatcher.dispatch_ast({:hexpire, "h", 10, ["f1"]}, store)
    assert [_ttl] = Dispatcher.dispatch_ast({:httl, "h", ["f1"]}, store)
    assert ["6"] == Dispatcher.dispatch_ast({:hgetex, "h", :persist, ["f1"]}, store)
    assert ["6"] == Dispatcher.dispatch_ast({:hgetex, "h", :none, ["f1"]}, store)
    assert 1 == Dispatcher.dispatch_ast({:hsetex, "h", 60, ["f3", "v3"]}, store)
    assert ["6", nil] == Dispatcher.dispatch_ast({:hmget, ["h", "f1", "missing"]}, store)
  end

  test "dispatches Rust hash parse errors" do
    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:hincrby, "h", "f", {:error, "ERR value is not an integer or out of range"}},
               MockStore.make()
             )

    assert {:error, "ERR number of fields does not match the count argument"} =
             Dispatcher.dispatch_ast(
               {:hgetdel, "h",
                {:error, "ERR number of fields does not match the count argument"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed set ASTs" do
    store = MockStore.make()

    assert 3 == Dispatcher.dispatch_ast({:sadd, ["s", "a", "b", "c"]}, store)
    assert 1 == Dispatcher.dispatch_ast({:sismember, "s", "a"}, store)
    assert [1, 0, 1] == Dispatcher.dispatch_ast({:smismember, ["s", "a", "x", "b"]}, store)
    assert 3 == Dispatcher.dispatch_ast({:scard, "s"}, store)

    member = Dispatcher.dispatch_ast({:srandmember, "s"}, store)
    assert member in ["a", "b", "c"]

    assert members = Dispatcher.dispatch_ast({:srandmember, "s", -3}, store)
    assert length(members) == 3

    assert popped = Dispatcher.dispatch_ast({:spop, "s", 1}, store)
    assert length(popped) == 1

    assert ["0", scan] = Dispatcher.dispatch_ast({:sscan, "s", 0, [count: 10]}, store)
    assert Enum.sort(scan) == Enum.sort(Dispatcher.dispatch_ast({:smembers, "s"}, store))

    assert 3 == Dispatcher.dispatch_ast({:sadd, ["s1", "a", "b", "c"]}, store)
    assert 3 == Dispatcher.dispatch_ast({:sadd, ["s2", "b", "c", "d"]}, store)
    assert 2 == Dispatcher.dispatch_ast({:sintercard, ["s1", "s2"], 10}, store)
  end

  test "dispatches Rust set parse errors" do
    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:spop, "s", {:error, "ERR value is not an integer or out of range"}},
               MockStore.make()
             )

    assert {:error, "ERR invalid cursor"} =
             Dispatcher.dispatch_ast(
               {:sscan, "s", {:error, "ERR invalid cursor"}},
               MockStore.make()
             )

    assert {:error, "ERR Number of keys can't be greater than number of args"} =
             Dispatcher.dispatch_ast(
               {:sintercard, {:error, "ERR Number of keys can't be greater than number of args"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed zset ASTs" do
    store = MockStore.make()

    assert 2 == Dispatcher.dispatch_ast({:zadd, "z", [:nx], [{1.0, "a"}, {2.0, "b"}]}, store)
    assert "3.5" == Dispatcher.dispatch_ast({:zincrby, "z", 2.5, "a"}, store)
    assert ["b", "2.0", "a", "3.5"] == Dispatcher.dispatch_ast({:zrange, "z", 0, -1, true}, store)
    assert 2 == Dispatcher.dispatch_ast({:zcount, "z", {:inclusive, 1.0}, :inf}, store)

    assert ["a"] ==
             Dispatcher.dispatch_ast(
               {:zrangebyscore, "z", {:exclusive, 2.0}, :inf, [limit: {0, 1}]},
               store
             )

    assert member = Dispatcher.dispatch_ast({:zrandmember, "z"}, store)
    assert member in ["a", "b"]

    assert random_with_scores = Dispatcher.dispatch_ast({:zrandmember, "z", -2, true}, store)
    assert length(random_with_scores) == 4

    assert ["0", scan] = Dispatcher.dispatch_ast({:zscan, "z", 0, [count: 10]}, store)
    assert Enum.sort(Enum.take_every(scan, 2)) == ["a", "b"]

    assert popped = Dispatcher.dispatch_ast({:zpopmin, "z", 1}, store)
    assert length(popped) == 2
  end

  test "dispatches Rust zset parse errors" do
    assert {:error, "ERR value is not a valid float"} =
             Dispatcher.dispatch_ast(
               {:zadd, "z", {:error, "ERR value is not a valid float"}},
               MockStore.make()
             )

    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:zrange, "z", {:error, "ERR value is not an integer or out of range"}, "bad"},
               MockStore.make()
             )

    assert {:error, "ERR min or max is not a float"} =
             Dispatcher.dispatch_ast(
               {:zrangebyscore, "z", {:error, "ERR min or max is not a float"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed bitmap ASTs" do
    store = MockStore.make()

    assert 0 == Dispatcher.dispatch_ast({:setbit, "bits", 7, 1}, store)
    assert 1 == Dispatcher.dispatch_ast({:getbit, "bits", 7}, store)
    assert 1 == Dispatcher.dispatch_ast({:bitcount, "bits"}, store)
    assert 1 == Dispatcher.dispatch_ast({:bitcount, "bits", {0, 7, :bit}}, store)
    assert 7 == Dispatcher.dispatch_ast({:bitpos, "bits", 1, :all}, store)
    assert 7 == Dispatcher.dispatch_ast({:bitpos, "bits", 1, {0, 0, :byte}}, store)

    assert 1 == Dispatcher.dispatch_ast({:bitop, :bor, "dst", ["bits", "missing"]}, store)
    assert 1 == Dispatcher.dispatch_ast({:getbit, "dst", 7}, store)
  end

  test "dispatches Rust bitmap parse errors" do
    assert {:error, "ERR bit offset is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:setbit, "bits", {:error, "ERR bit offset is not an integer or out of range"},
                "1"},
               MockStore.make()
             )

    assert {:error, "ERR bit is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:bitpos, "bits", {:error, "ERR bit is not an integer or out of range"}},
               MockStore.make()
             )

    assert {:error, "ERR BITOP NOT requires one and only one key"} =
             Dispatcher.dispatch_ast(
               {:bitop, {:error, "ERR BITOP NOT requires one and only one key"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed generic ASTs" do
    store = MockStore.make(%{"src" => {"v", 0}, "user:1" => {"a", 0}, "other" => {"b", 0}})

    assert {:simple, "string"} == Dispatcher.dispatch_ast({:type, "src"}, store)
    assert 1 == Dispatcher.dispatch_ast({:copy, "src", "dst", false}, store)
    assert "v" == store.get.("dst")
    assert :ok == Dispatcher.dispatch_ast({:rename, "dst", "renamed"}, store)
    assert "v" == store.get.("renamed")
    assert 0 == Dispatcher.dispatch_ast({:renamenx, "renamed", "src"}, store)

    assert ["0", ["user:1"]] ==
             Dispatcher.dispatch_ast(
               {:scan, "0", [match: "user:*", count: 10, type: "string"]},
               store
             )

    assert -1 == Dispatcher.dispatch_ast({:expiretime, "src"}, store)
    assert -1 == Dispatcher.dispatch_ast({:pexpiretime, "src"}, store)
    assert "embstr" == Dispatcher.dispatch_ast({:object, :encoding, "src"}, store)
    assert help = Dispatcher.dispatch_ast({:object, :help}, store)
    assert is_list(help)

    assert {:error, "ERR value is not an integer or out of range"} ==
             Dispatcher.dispatch_ast({:wait, "abc", "xyz"}, store)

    assert 1 == Dispatcher.dispatch_ast({:unlink, ["src"]}, store)
  end

  test "dispatches Rust generic parse errors" do
    assert {:error, "ERR syntax error"} =
             Dispatcher.dispatch_ast(
               {:copy, "src", "dst", {:error, "ERR syntax error"}},
               MockStore.make()
             )

    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:scan, "0", {:error, "ERR value is not an integer or out of range"}},
               MockStore.make()
             )

    assert {:error, "ERR unknown subcommand or wrong number of arguments for 'bogus' command"} =
             Dispatcher.dispatch_ast(
               {:object,
                {:error,
                 "ERR unknown subcommand or wrong number of arguments for 'bogus' command"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed stream ASTs" do
    store = MockStore.make()
    stream_key = "dispatcher-stream-#{System.unique_integer([:positive])}"

    assert "1-0" ==
             Dispatcher.dispatch_ast(
               {:xadd, stream_key, {{:explicit, 1, 0}, ["f", "v"], nil, false}},
               store
             )

    assert 1 == Dispatcher.dispatch_ast({:xlen, stream_key}, store)

    assert [["1-0", "f", "v"]] ==
             Dispatcher.dispatch_ast({:xrange, stream_key, :min, :max, :infinity}, store)

    assert [["1-0", "f", "v"]] ==
             Dispatcher.dispatch_ast({:xrevrange, stream_key, :min, :max, 1}, store)

    assert [[stream_key, [["1-0", "f", "v"]]]] ==
             Dispatcher.dispatch_ast({:xread, :infinity, :no_block, [{stream_key, "0"}]}, store)

    assert 0 == Dispatcher.dispatch_ast({:xtrim, stream_key, {:maxlen, false, 10}}, store)
    assert 1 == Dispatcher.dispatch_ast({:xdel, stream_key, ["1-0"]}, store)
  end

  test "dispatches Rust stream parse errors" do
    assert {:error, "ERR Invalid stream ID specified as stream command argument"} =
             Dispatcher.dispatch_ast(
               {:xadd, {:error, "ERR Invalid stream ID specified as stream command argument"}},
               MockStore.make()
             )

    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:xtrim, "s", {:error, "ERR value is not an integer or out of range"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed Geo ASTs" do
    store = MockStore.make()

    assert 2 ==
             Dispatcher.dispatch_ast(
               {:geoadd, "g", [],
                [{13.361389, 38.115556, "Palermo"}, {15.087269, 37.502669, "Catania"}]},
               store
             )

    assert [[_lng, _lat], [_lng2, _lat2]] =
             Dispatcher.dispatch_ast({:geopos, ["g", "Palermo", "Catania"]}, store)

    assert [hash] = Dispatcher.dispatch_ast({:geohash, ["g", "Palermo"]}, store)
    assert is_binary(hash)

    dist = Dispatcher.dispatch_ast({:geodist, "g", "Palermo", "Catania", "KM"}, store)
    assert is_binary(dist)

    assert ["Palermo"] =
             Dispatcher.dispatch_ast(
               {:geosearch, "g",
                [
                  center: {:lonlat, 13.361389, 38.115556},
                  shape: {:radius, 10_000.0},
                  unit: "M"
                ]},
               store
             )
  end

  test "dispatches Rust Geo parse errors" do
    assert {:error, "ERR invalid longitude,latitude pair 200,38"} =
             Dispatcher.dispatch_ast(
               {:geoadd, {:error, "ERR invalid longitude,latitude pair 200,38"}},
               MockStore.make()
             )

    assert {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"} =
             Dispatcher.dispatch_ast(
               {:geodist, "g", "a", "b",
                {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}},
               MockStore.make()
             )
  end

  test "dispatches Rust typed HyperLogLog ASTs" do
    store = MockStore.make()

    assert 1 == Dispatcher.dispatch_ast({:pfadd, ["h", "a", "b"]}, store)
    count = Dispatcher.dispatch_ast({:pfcount, ["h"]}, store)
    assert count >= 2
    assert :ok = Dispatcher.dispatch_ast({:pfmerge, ["dst", "h"]}, store)
  end

  test "dispatches Rust HyperLogLog parse errors" do
    assert {:error, "ERR wrong number of arguments for 'pfcount' command"} =
             Dispatcher.dispatch_ast(
               {:pfcount, {:error, "ERR wrong number of arguments for 'pfcount' command"}},
               MockStore.make()
             )
  end

  test "dispatches GET to Strings handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert "v" == Dispatcher.dispatch("GET", ["k"], store)
  end

  test "dispatches DEL to Strings handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert 1 == Dispatcher.dispatch("DEL", ["k"], store)
  end

  test "dispatches EXISTS to Strings handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert 1 == Dispatcher.dispatch("EXISTS", ["k"], store)
  end

  test "dispatches MGET to Strings handler" do
    store = MockStore.make(%{"a" => {"1", 0}})
    assert ["1", nil] == Dispatcher.dispatch("MGET", ["a", "b"], store)
  end

  test "dispatches MSET to Strings handler" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("MSET", ["a", "1", "b", "2"], store)
    assert "1" == store.get.("a")
  end

  # ---------------------------------------------------------------------------
  # Routing to Expiry handler
  # ---------------------------------------------------------------------------

  test "dispatches EXPIRE to Expiry handler" do
    store = MockStore.make(%{"k" => {"v", 0}})
    assert 1 == Dispatcher.dispatch("EXPIRE", ["k", "10"], store)
  end

  test "dispatches TTL to Expiry handler" do
    future = System.os_time(:millisecond) + 10_000
    store = MockStore.make(%{"k" => {"v", future}})
    ttl = Dispatcher.dispatch("TTL", ["k"], store)
    assert ttl > 0
  end

  test "dispatches PTTL to Expiry handler" do
    future = System.os_time(:millisecond) + 10_000
    store = MockStore.make(%{"k" => {"v", future}})
    pttl = Dispatcher.dispatch("PTTL", ["k"], store)
    assert pttl > 0
  end

  test "dispatches PERSIST to Expiry handler" do
    future = System.os_time(:millisecond) + 60_000
    store = MockStore.make(%{"k" => {"v", future}})
    assert 1 == Dispatcher.dispatch("PERSIST", ["k"], store)
  end

  test "dispatches Rust expiry command ASTs" do
    store = MockStore.make(%{"k" => {"v", 0}})

    assert 1 == Dispatcher.dispatch_ast({:expire, "k", 10}, store)
    assert Dispatcher.dispatch_ast({:ttl, "k"}, store) > 0
    assert 1 == Dispatcher.dispatch_ast({:persist, "k"}, store)
    assert -1 == Dispatcher.dispatch_ast({:pttl, "k"}, store)
    assert 1 == Dispatcher.dispatch_ast({:pexpire, "k", 50, :nx}, store)
    assert 0 == Dispatcher.dispatch_ast({:expireat, "missing", 9_999_999_999}, store)
    assert 1 == Dispatcher.dispatch_ast({:pexpireat, "k", 1}, store)
  end

  test "dispatches Rust expiry AST parse errors without reparsing" do
    store = MockStore.make(%{"k" => {"v", 0}})

    assert {:error, "ERR value is not an integer or out of range"} =
             Dispatcher.dispatch_ast(
               {:expire, "k", {:error, "ERR value is not an integer or out of range"}},
               store
             )

    assert {:error, "ERR Unsupported option bad"} =
             Dispatcher.dispatch_ast(
               {:pexpire, "k", 10, {:error, "ERR Unsupported option bad"}},
               store
             )
  end

  # ---------------------------------------------------------------------------
  # Routing to Server handler
  # ---------------------------------------------------------------------------

  test "dispatches PING to Server handler" do
    assert {:simple, "PONG"} == Dispatcher.dispatch("PING", [], MockStore.make())
  end

  test "dispatches ECHO to Server handler" do
    assert "hi" == Dispatcher.dispatch("ECHO", ["hi"], MockStore.make())
  end

  test "dispatches KEYS to Server handler" do
    store = MockStore.make(%{"a" => {"1", 0}})
    assert ["a"] == Dispatcher.dispatch("KEYS", ["*"], store)
  end

  test "dispatches DBSIZE to Server handler" do
    store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
    assert 2 == Dispatcher.dispatch("DBSIZE", [], store)
  end

  test "dispatches FLUSHDB to Server handler" do
    store = MockStore.make(%{"a" => {"1", 0}})
    assert :ok = Dispatcher.dispatch("FLUSHDB", [], store)
  end

  # ---------------------------------------------------------------------------
  # Unknown command
  # ---------------------------------------------------------------------------

  test "unknown command returns error tuple" do
    assert {:error, msg} = Dispatcher.dispatch("UNKNOWNCMD", [], MockStore.make())
    assert msg =~ "unknown command"
    assert msg =~ "unknowncmd"
  end

  # ---------------------------------------------------------------------------
  # Case insensitivity
  # ---------------------------------------------------------------------------

  test "dispatch is case-insensitive (lowercase)" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("set", ["k", "v"], store)
  end

  test "dispatch is case-insensitive (mixed case)" do
    store = MockStore.make()
    assert :ok = Dispatcher.dispatch("Set", ["k", "v"], store)
  end
end
