defmodule Ferricstore.Commands.RedisErrorMessagesTest do
  @moduledoc "Error replies that clients match on read as Redis words them."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  test "too few arguments answer the arity error, not an internal message" do
    ctx = FerricStore.Instance.get(:default)

    for {command, args} <- [
          {"SETEX", ["k", "10"]},
          {"PSETEX", ["k", "10"]},
          {"BITOP", ["AND"]},
          {"UNLINK", []},
          {"HSCAN", ["k"]},
          {"HTTL", ["k", "FIELDS"]},
          {"HPERSIST", ["k"]},
          {"HPTTL", ["k", "FIELDS", "1"]},
          {"HEXPIRETIME", []},
          {"HGETDEL", ["k", "FIELDS"]}
        ] do
      assert {:error, "ERR wrong number of arguments for '#{String.downcase(command)}' command"} ==
               Dispatcher.dispatch(command, args, ctx),
             "#{command} #{inspect(args)}"
    end
  end

  test "writes refuse keys over the key size limit, as SET does" do
    ctx = FerricStore.Instance.get(:default)
    too_large = "{big}" <> String.duplicate("k", Ferricstore.Store.Router.max_key_size())

    for {command, args} <- [
          {"SET", [too_large, "v"]},
          {"HSET", [too_large, "f", "v"]},
          {"SADD", [too_large, "m"]},
          {"ZADD", [too_large, "1", "m"]},
          {"RPUSH", [too_large, "a"]},
          {"XADD", [too_large, "*", "f", "v"]}
        ] do
      assert {:error, "ERR key too large"} == Dispatcher.dispatch(command, args, ctx),
             command
    end

    assert 0 == Dispatcher.dispatch("EXISTS", [too_large], ctx)
  end

  test "LPOS RANK 0 answers Redis's message" do
    ctx = FerricStore.Instance.get(:default)
    key = "{lpos:#{System.unique_integer([:positive])}}"
    assert 1 == Dispatcher.dispatch("RPUSH", [key, "a"], ctx)

    assert {:error,
            "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"} ==
             Dispatcher.dispatch("LPOS", [key, "a", "RANK", "0"], ctx)
  end
end
