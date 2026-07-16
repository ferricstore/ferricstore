defmodule Ferricstore.Commands.KeyExtractionLinearTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Catalog, Extension}

  defmodule PositionalExtension do
    @behaviour Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.POSITIONAL",
          first_key: 1,
          last_key: -1,
          step: 2,
          access: :read
        }
      ]
    end

    @impl true
    def handle(_command, _args, _store), do: :ok
  end

  setup do
    previous = Application.get_env(:ferricstore, :command_extensions)
    Application.put_env(:ferricstore, :command_extensions, [PositionalExtension])

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :command_extensions)
        value -> Application.put_env(:ferricstore, :command_extensions, value)
      end
    end)
  end

  test "catalog and extension positional extraction preserve order and step" do
    assert {:ok, ["k1", "k2", "k3"]} == Catalog.get_keys("MGET", ["k1", "k2", "k3"])

    assert {:ok, ["k1", "k2", "k3"]} ==
             Extension.keys("EXT.POSITIONAL", ["k1", "value1", "k2", "value2", "k3"])
  end

  test "positional extraction does not perform indexed list walks" do
    for relative <- [
          "../../../lib/ferricstore/commands/catalog.ex",
          "../../../lib/ferricstore/commands/extension.ex",
          "../../../lib/ferricstore/commands/native_ast_parser.ex"
        ] do
      source = relative |> Path.expand(__DIR__) |> File.read!()
      refute source =~ "Enum.at(args"
    end
  end
end
