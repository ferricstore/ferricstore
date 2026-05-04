defmodule Ferricstore.Commands.DispatcherFuzzTest do
  @moduledoc """
  Deterministic fuzz-style guards for command dispatch.

  Scope is direct dispatcher handling with MockStore. TCP framing is covered by
  RESP parser fuzz tests; this catches command parser exceptions and malformed
  argument handling in the shared command layer.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.MockStore

  @iterations 400

  @known_safe_commands ~w(
    GET SET DEL EXISTS MGET MSET MSETNX INCR DECR INCRBY DECRBY INCRBYFLOAT
    APPEND STRLEN GETSET GETDEL GETEX SETNX SETEX PSETEX GETRANGE SETRANGE
    EXPIRE EXPIREAT PEXPIRE PEXPIREAT TTL PTTL PERSIST TYPE UNLINK RENAME
    RENAMENX COPY RANDOMKEY SCAN EXPIRETIME PEXPIRETIME OBJECT WAIT PING ECHO
    DBSIZE KEYS INFO COMMAND SELECT LOLWUT DEBUG SLOWLOG CONFIG MODULE WAITAOF
    HSET HGET HDEL HMGET HGETALL HEXISTS HKEYS HVALS HLEN HINCRBY HINCRBYFLOAT
    HSETNX HSTRLEN HRANDFIELD HSCAN HEXPIRE HTTL HPERSIST HPEXPIRE HPTTL
    HEXPIRETIME HGETDEL HGETEX HSETEX LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX
    LSET LREM LTRIM LPOS LINSERT LMOVE LPUSHX RPUSHX RPOPLPUSH SADD SREM
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SPOP SDIFF SINTER SUNION
    SDIFFSTORE SINTERSTORE SUNIONSTORE SINTERCARD SMOVE SSCAN ZADD ZREM ZSCORE
    ZRANK ZREVRANK ZRANGE ZREVRANGE ZCARD ZINCRBY ZCOUNT ZPOPMIN ZPOPMAX
    ZRANDMEMBER ZSCAN ZMSCORE ZRANGEBYSCORE ZREVRANGEBYSCORE SETBIT GETBIT
    BITCOUNT BITPOS BITOP JSON.SET JSON.GET JSON.DEL JSON.NUMINCRBY JSON.TYPE
    JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRAPPEND JSON.ARRLEN
    JSON.TOGGLE JSON.CLEAR JSON.MGET XADD XLEN XRANGE XREVRANGE XTRIM XDEL
    XINFO XGROUP XREADGROUP XACK
  )

  setup do
    reset_stream_tables()
    :ok
  end

  test "generated command arrays dispatch to result values, not exceptions" do
    seed_rand()

    for _ <- 1..@iterations do
      store = MockStore.make()
      {cmd, args} = random_command()

      assert_no_crash("Dispatcher crashed for #{inspect([cmd | args], limit: 80)}", fn ->
        result = Dispatcher.dispatch(cmd, args, store)
        assert_dispatch_result(result)
      end)
    end
  end

  test "unknown generated command names return unknown-command errors" do
    seed_rand()
    store = MockStore.make()

    for _ <- 1..@iterations do
      cmd = "FUZZ." <> random_printable(:rand.uniform(24))
      args = random_args(:rand.uniform(5) - 1)

      assert {:error, "ERR unknown command '" <> _} = Dispatcher.dispatch(cmd, args, store)
    end
  end

  defp seed_rand do
    :rand.seed(:exsss, {0xD15A, 0x7C4, 0xF022})
  end

  defp random_command do
    cmd =
      Enum.random([
        Enum.random(@known_safe_commands),
        String.downcase(Enum.random(@known_safe_commands)),
        random_printable(:rand.uniform(18))
      ])

    {cmd, random_args(:rand.uniform(8) - 1)}
  end

  defp random_args(count) when count <= 0, do: []
  defp random_args(count), do: Enum.map(1..count, fn _ -> random_arg() end)

  defp random_arg do
    Enum.random([
      "",
      "0",
      "1",
      "-1",
      "+inf",
      "-inf",
      "nan",
      "$",
      "*",
      random_printable(:rand.uniform(32) - 1),
      Integer.to_string(:rand.uniform(20_000) - 10_000)
    ])
  end

  defp random_printable(count) when count <= 0, do: ""
  defp random_printable(count), do: for(_ <- 1..count, into: "", do: <<Enum.random(32..126)>>)

  defp assert_dispatch_result({:error, msg}) when is_binary(msg), do: :ok
  defp assert_dispatch_result({:ok, value}), do: assert_dispatch_result(value)

  defp assert_dispatch_result(value)
       when is_binary(value) or is_integer(value) or is_float(value), do: :ok

  defp assert_dispatch_result(value) when is_list(value) or is_map(value), do: :ok
  defp assert_dispatch_result(value) when value in [:ok, nil, true, false], do: :ok

  defp assert_dispatch_result(other) do
    flunk("unexpected dispatch result shape: #{inspect(other)}")
  end

  defp assert_no_crash(message, fun) do
    fun.()
  rescue
    exception ->
      flunk("#{message}: #{Exception.format(:error, exception, __STACKTRACE__)}")
  catch
    kind, reason ->
      flunk("#{message}: #{inspect({kind, reason})}")
  end

  defp reset_stream_tables do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end
  end
end
