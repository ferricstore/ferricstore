defmodule Ferricstore.Commands.HashFieldExpireAtTest do
  @moduledoc "HEXPIREAT, HPEXPIREAT and HPEXPIRETIME, as Redis 8 answers them."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Transaction.ReplicaCompat

  setup do
    ctx = FerricStore.Instance.get(:default)
    key = "hexpat:{#{System.unique_integer([:positive])}}h"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    3 = d.("HSET", [key, "a", "1", "b", "2", "c", "3"])
    %{ctx: ctx, d: d, key: key}
  end

  test "HEXPIREAT and HPEXPIREAT set a field's expiry at a Unix time", %{d: d, key: key} do
    at_s = System.os_time(:second) + 1_000
    at_ms = System.os_time(:millisecond) + 2_000_000

    assert [1, -2] == d.("HEXPIREAT", [key, Integer.to_string(at_s), "FIELDS", "2", "a", "nope"])
    assert [at_s] == d.("HEXPIRETIME", [key, "FIELDS", "1", "a"])
    assert [at_s * 1_000] == d.("HPEXPIRETIME", [key, "FIELDS", "1", "a"])

    assert [1] == d.("HPEXPIREAT", [key, Integer.to_string(at_ms), "FIELDS", "1", "b"])
    assert [at_ms] == d.("HPEXPIRETIME", [key, "FIELDS", "1", "b"])
    assert [-1, -2] == d.("HPEXPIRETIME", [key, "FIELDS", "2", "c", "nope"])

    # Conditions compare with the field's current expiry, as HEXPIRE's do.
    assert [0] == d.("HEXPIREAT", [key, Integer.to_string(at_s + 10), "NX", "FIELDS", "1", "a"])
    assert [1] == d.("HEXPIREAT", [key, Integer.to_string(at_s + 10), "GT", "FIELDS", "1", "a"])
    assert [at_s + 10] == d.("HEXPIRETIME", [key, "FIELDS", "1", "a"])
  end

  test "a time already past deletes the field", %{d: d, key: key} do
    assert [2] == d.("HPEXPIREAT", [key, "1000", "FIELDS", "1", "c"])
    assert 0 == d.("HEXISTS", [key, "c"])
    assert [-2] == d.("HPEXPIRETIME", [key, "FIELDS", "1", "c"])
  end

  test "argument errors and missing keys", %{d: d, key: key} do
    assert {:error, "ERR invalid expire time, must be >= 0"} ==
             d.("HEXPIREAT", [key, "-1", "FIELDS", "1", "a"])

    assert {:error, "ERR wrong number of arguments for 'hexpireat' command"} ==
             d.("HEXPIREAT", [key, "1"])

    assert {:error, "ERR wrong number of arguments for 'hpexpiretime' command"} ==
             d.("HPEXPIRETIME", [key])

    assert [-2] == d.("HPEXPIRETIME", ["hexpat:missing", "FIELDS", "1", "a"])
  end

  test "inside MULTI they need feature level 12", %{ctx: ctx, key: key} do
    for {command, args} <- [
          {"HEXPIREAT", [key, "9999999999", "FIELDS", "1", "a"]},
          {"HPEXPIREAT", [key, "9999999999000", "NX", "FIELDS", "1", "a"]},
          {"HPEXPIRETIME", [key, "FIELDS", "1", "a"]}
        ] do
      assert {:ok, prepared} = PreparedCommand.prepare(command, args)
      assert 12 == ReplicaCompat.required_level(prepared.ast), command
      assert ReplicaCompat.queue_error(command, prepared.ast, prepared.command_keys, ctx)
    end
  end
end
