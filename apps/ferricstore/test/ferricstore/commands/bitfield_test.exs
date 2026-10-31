defmodule Ferricstore.Commands.BitfieldTest do
  @moduledoc "BITFIELD and BITFIELD_RO reads, and what below feature level 12 refuses."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Bitfield, Dispatcher, PreparedCommand}
  alias Ferricstore.Transaction.ReplicaCompat

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "bitfield:{#{System.unique_integer([:positive])}}"

    %{
      ctx: ctx,
      d: fn command, args -> Dispatcher.dispatch(command, args, ctx) end,
      k: &(tag <> &1)
    }
  end

  test "GET reads signed and unsigned fields, zeros past the end", %{d: d, k: k} do
    :ok = d.("SET", [k.("s"), <<100, 200, 0b1010_0000>>])

    assert [100, -56, 200, 5, 0, 0] ==
             d.("BITFIELD", [
               k.("s"),
               "GET",
               "i8",
               "#0",
               "GET",
               "i8",
               "#1",
               "GET",
               "u8",
               "8",
               "GET",
               "u3",
               "16",
               "GET",
               "u8",
               "#9",
               "GET",
               "i64",
               "100"
             ])

    assert [100] == d.("BITFIELD_RO", [k.("s"), "GET", "u8", "0"])
    assert [0] == d.("BITFIELD_RO", [k.("missing"), "GET", "i5", "3"])
    assert [] == d.("BITFIELD", [k.("s")])
    assert 0 == d.("EXISTS", [k.("missing")])
  end

  test "errors", %{d: d, k: k} do
    type_error =
      "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported " <>
        "but i64 is."

    for type <- ["u64", "i65", "i0", "x8", "u"] do
      assert {:error, ^type_error} = d.("BITFIELD", [k.("s"), "GET", type, "0"])
    end

    assert {:error, "ERR bit offset is not an integer or out of range"} ==
             d.("BITFIELD", [k.("s"), "GET", "u8", "-1"])

    assert {:error, "ERR bit offset is not an integer or out of range"} ==
             d.("BITFIELD", [k.("s"), "GET", "u8", "#x"])

    assert {:error, "ERR value is not an integer or out of range"} ==
             d.("BITFIELD", [k.("s"), "SET", "u8", "0", "x"])

    assert {:error, "ERR Invalid OVERFLOW type specified"} ==
             d.("BITFIELD", [k.("s"), "OVERFLOW", "NOPE"])

    assert {:error, "ERR syntax error"} == d.("BITFIELD", [k.("s"), "FOO", "u8", "0"])
    assert {:error, "ERR syntax error"} == d.("BITFIELD", [k.("s"), "GET", "u8"])

    assert {:error, "ERR BITFIELD_RO only supports the GET subcommand"} ==
             d.("BITFIELD_RO", [k.("s"), "SET", "u8", "0", "1"])

    1 = d.("RPUSH", [k.("l"), "x"])
    assert {:error, "WRONGTYPE" <> _} = d.("BITFIELD", [k.("l"), "GET", "u8", "0"])
  end

  test "below feature level 12 a write is refused", %{d: d, k: k} do
    assert {:error, message} = d.("BITFIELD", [k.("s"), "SET", "u8", "0", "1"])
    assert message =~ "needs cluster feature level 12"
    assert 0 == d.("EXISTS", [k.("s")])
  end

  test "keys and MULTI", %{ctx: ctx, k: k} do
    for command <- ["BITFIELD", "BITFIELD_RO"] do
      {:ok, prepared} = PreparedCommand.prepare(command, [k.("s"), "GET", "u8", "0"])
      assert [k.("s")] == prepared.command_keys
      assert 12 == ReplicaCompat.required_level(prepared.ast)
      assert ReplicaCompat.queue_error(command, prepared.ast, prepared.command_keys, ctx)
    end
  end

  test "Redis's overflow rules" do
    run = fn value, args -> Bitfield.run(value, elem(Bitfield.parse(args, :write), 1)) end

    # Redis's documented example: WRAP on the first counter, SAT on the second.
    ops = ["INCRBY", "u2", "100", "1", "OVERFLOW", "SAT", "INCRBY", "u2", "102", "1"]

    {value, replies} =
      Enum.reduce(1..4, {<<>>, []}, fn _, {value, replies} ->
        {value, reply, _changed?} = run.(value, ops)
        {value, replies ++ [reply]}
      end)

    assert [[1, 1], [2, 2], [3, 3], [0, 3]] == replies
    assert byte_size(value) == 13

    replies = fn value, args -> run.(value, args) |> elem(1) end

    assert [0, -128] == replies.(<<>>, ["SET", "i8", "0", "128", "GET", "i8", "0"])

    assert [0, 127] ==
             replies.(<<>>, ["OVERFLOW", "SAT", "SET", "i8", "0", "300", "GET", "i8", "0"])

    assert [-128, -128] ==
             replies.(<<>>, ["OVERFLOW", "SAT", "INCRBY", "i8", "0", "-500", "GET", "i8", "0"])

    assert {<<>>, [nil], false} == run.(<<>>, ["OVERFLOW", "FAIL", "INCRBY", "u4", "0", "16"])
    assert [255] == replies.(<<>>, ["OVERFLOW", "SAT", "INCRBY", "u8", "0", "1000"])
    assert [0, 255] == replies.(<<>>, ["SET", "u8", "0", "-1", "GET", "u8", "0"])

    assert [0, 9_223_372_036_854_775_807] ==
             replies.(<<>>, ["SET", "i64", "0", "9223372036854775807", "GET", "i64", "0"])

    max_i64 = <<0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
    assert [-9_223_372_036_854_775_808] == replies.(max_i64, ["INCRBY", "i64", "0", "1"])

    assert {^max_i64, [9_223_372_036_854_775_807], false} =
             run.(max_i64, ["OVERFLOW", "SAT", "INCRBY", "i64", "0", "1"])
  end
end

defmodule Ferricstore.Commands.BitfieldLevel12Test do
  @moduledoc "From feature level 12 BITFIELD writes run in one compound entry."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 12}} = FerricStore.raise_cluster_feature_level(12)
    :ok
  end

  setup do
    ctx = FerricStore.Instance.get(:default)
    key = "bitfield12:{#{System.unique_integer([:positive])}}"
    %{d: fn command, args -> Dispatcher.dispatch(command, args, ctx) end, key: key}
  end

  test "SET and INCRBY write, keep the TTL, and concurrent INCRBYs all count", %{d: d, key: key} do
    assert [1, 0] == d.("BITFIELD", [key, "INCRBY", "i5", "100", "1", "GET", "u4", "0"])
    assert [0, 0] == d.("BITFIELD", [key, "SET", "i8", "#0", "100", "SET", "i8", "#1", "200"])
    assert [100, -56] == d.("BITFIELD", [key, "GET", "i8", "#0", "GET", "i8", "#1"])

    assert 1 == d.("EXPIRE", [key, "100"])
    assert [100] == d.("BITFIELD", [key, "SET", "u8", "0", "7"])
    assert d.("TTL", [key]) in 99..100

    # A FAIL overflow writes nothing; a missing key stays missing.
    other = key <> "x"
    assert [nil] == d.("BITFIELD", [other, "OVERFLOW", "FAIL", "INCRBY", "u2", "0", "8"])
    assert 0 == d.("EXISTS", [other])

    counter = key <> "c"

    1..20
    |> Task.async_stream(fn _ -> d.("BITFIELD", [counter, "INCRBY", "u16", "0", "1"]) end,
      max_concurrency: 10
    )
    |> Stream.run()

    assert [20] == d.("BITFIELD_RO", [counter, "GET", "u16", "0"])
  end

  test "a transaction runs BITFIELD", %{d: d, key: key} do
    {:ok, prepared} = PreparedCommand.prepare("BITFIELD", [key, "INCRBY", "u8", "0", "5"])
    assert [[5]] == Ferricstore.Transaction.Coordinator.execute([prepared], %{}, nil)
    assert [5] == d.("BITFIELD_RO", [key, "GET", "u8", "0"])
  end
end
