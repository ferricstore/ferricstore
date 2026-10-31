defmodule Ferricstore.Commands.ProbInsertTest do
  @moduledoc "BF.INSERT, CF.INSERT and CF.INSERTNX, as RedisBloom answers them."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "insert:{#{System.unique_integer([:positive])}}"

    %{
      d: fn command, args -> Dispatcher.dispatch(command, args, ctx) end,
      k: &(tag <> &1)
    }
  end

  test "BF.INSERT creates the filter with its options and adds the items", %{d: d, k: k} do
    assert [1, 1, 0] ==
             d.("BF.INSERT", [
               k.("bf"),
               "CAPACITY",
               "1000",
               "ERROR",
               "0.001",
               "ITEMS",
               "a",
               "b",
               "a"
             ])

    assert [1, 1, 0] == d.("BF.MEXISTS", [k.("bf"), "a", "b", "c"])
    # The filter BF.RESERVE would make with the same options.
    :ok = d.("BF.RESERVE", [k.("reserved"), "0.001", "1000"])

    assert info_value(d.("BF.INFO", [k.("reserved")]), "Capacity") ==
             info_value(d.("BF.INFO", [k.("bf")]), "Capacity")

    # EXPANSION and NONSCALING are taken (FerricStore's filters do not grow).
    assert [1] ==
             d.("BF.INSERT", [k.("bf"), "EXPANSION", "2", "NONSCALING", "ITEMS", "c"])

    assert [1] == d.("BF.INSERT", [k.("default"), "ITEMS", "x"])
  end

  test "BF.INSERT NOCREATE needs an existing filter", %{d: d, k: k} do
    assert {:error, "ERR not found"} == d.("BF.INSERT", [k.("bf"), "NOCREATE", "ITEMS", "a"])
    assert 0 == d.("EXISTS", [k.("bf")])
    :ok = d.("BF.RESERVE", [k.("bf"), "0.01", "100"])
    assert [1] == d.("BF.INSERT", [k.("bf"), "NOCREATE", "ITEMS", "a"])
  end

  test "BF.INSERT errors", %{d: d, k: k} do
    assert {:error, "ERR wrong number of arguments for 'bf.insert' command"} ==
             d.("BF.INSERT", [k.("bf"), "CAPACITY", "10"])

    assert {:error, "ERR wrong number of arguments for 'bf.insert' command"} ==
             d.("BF.INSERT", [k.("bf"), "ITEMS"])

    assert {:error, "ERR bad capacity value"} ==
             d.("BF.INSERT", [k.("bf"), "CAPACITY", "0", "ITEMS", "a"])

    assert {:error, "ERR (0 < error rate range < 1)"} ==
             d.("BF.INSERT", [k.("bf"), "ERROR", "2", "ITEMS", "a"])

    assert {:error, "ERR bad expansion value"} ==
             d.("BF.INSERT", [k.("bf"), "EXPANSION", "0", "ITEMS", "a"])

    assert {:error, "ERR syntax error"} == d.("BF.INSERT", [k.("bf"), "FOO", "ITEMS", "a"])

    :ok = d.("SET", [k.("s"), "v"])
    assert {:error, "WRONGTYPE" <> _} = d.("BF.INSERT", [k.("s"), "ITEMS", "a"])
  end

  test "CF.INSERT and CF.INSERTNX", %{d: d, k: k} do
    assert [1, 1, 1] == d.("CF.INSERT", [k.("cf"), "CAPACITY", "2048", "ITEMS", "a", "b", "a"])
    assert 2 == d.("CF.COUNT", [k.("cf"), "a"])
    assert [0, 1] == d.("CF.INSERTNX", [k.("cf"), "ITEMS", "a", "c"])

    assert {:error, "ERR not found"} == d.("CF.INSERT", [k.("none"), "NOCREATE", "ITEMS", "a"])

    assert {:error, "ERR not found"} ==
             d.("CF.INSERTNX", [k.("none"), "NOCREATE", "ITEMS", "a"])

    assert 0 == d.("EXISTS", [k.("none")])

    assert {:error, "ERR wrong number of arguments for 'cf.insertnx' command"} ==
             d.("CF.INSERTNX", [k.("cf"), "ITEMS"])

    assert {:error, "ERR bad capacity value"} ==
             d.("CF.INSERT", [k.("cf"), "CAPACITY", "x", "ITEMS", "a"])

    assert {:error, "ERR syntax error"} ==
             d.("CF.INSERT", [k.("cf"), "ERROR", "0.1", "ITEMS", "a"])
  end

  test "a full cuckoo filter answers -1 for the items it could not take", %{d: d, k: k} do
    :ok = d.("CF.RESERVE", [k.("full"), "1"])
    replies = d.("CF.INSERT", [k.("full"), "ITEMS" | Enum.map(1..64, &"item#{&1}")])
    assert 64 == length(replies)
    assert -1 in replies
    assert Enum.all?(replies, &(&1 in [1, -1]))
  end

  defp info_value(info, name) when is_list(info) do
    info |> Enum.chunk_every(2) |> Enum.find_value(fn [k, v] -> if k == name, do: v end)
  end

  defp info_value(info, name) when is_map(info), do: Map.get(info, name)
end
