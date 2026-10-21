defmodule Ferricstore.Commands.FloatIncrementFormatTest do
  @moduledoc """
  INCRBYFLOAT and HINCRBYFLOAT answer plain decimals, as Redis does ("3",
  "5200", "0.00000015"). Below cluster feature level 4 the stored value keeps
  the form every release writes, so replicas on different releases agree.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Transaction.Coordinator

  setup do
    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{float:#{System.unique_integer([:positive])}}"
    }
  end

  test "replies are plain decimals and stored values keep their form", %{ctx: ctx, key: key} do
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end

    assert "2.5" == d.("INCRBYFLOAT", [key <> ":f", "2.5"])
    assert "3" == d.("INCRBYFLOAT", [key <> ":f", "0.5"])
    assert "3.0" == d.("GET", [key <> ":f"])
    assert "5200" == d.("INCRBYFLOAT", [key <> ":big", "5200"])
    assert "5.2e3" == d.("GET", [key <> ":big"])
    assert "0.00000015" == d.("INCRBYFLOAT", [key <> ":tiny", "0.00000015"])

    assert "2.5" == d.("HINCRBYFLOAT", [key <> ":h", "f", "2.5"])
    assert "3" == d.("HINCRBYFLOAT", [key <> ":h", "f", "0.5"])
    assert "3.0" == d.("HGET", [key <> ":h", "f"])
    assert "5200" == d.("HINCRBYFLOAT", [key <> ":h", "g", "5200"])

    {:ok, prepared} = PreparedCommand.prepare("HINCRBYFLOAT", [key <> ":mh", "f", "5200"])
    assert ["5200"] == Coordinator.execute([prepared], %{}, nil)
    assert "5200.0" == d.("HGET", [key <> ":mh", "f"])
  end
end
