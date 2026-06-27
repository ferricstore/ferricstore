defmodule Ferricstore.Commands.ExtensionTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Catalog, Dispatcher}
  alias Ferricstore.Test.MockStore

  defmodule TestExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.PING",
          arity: -1,
          flags: ["readonly"],
          first_key: 0,
          last_key: 0,
          step: 0,
          access: :read,
          summary: "Test extension ping"
        },
        %{
          name: "EXT.PUT",
          arity: 3,
          flags: ["write"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :write,
          summary: "Test extension write"
        }
      ]
    end

    @impl true
    def handle("EXT.PING", args, _store), do: {:ok, ["pong", args]}

    def handle("EXT.PUT", [key, value], store) do
      :ok = store.put.(key, value, 0)
      {:ok, "OK"}
    end

    @impl true
    def keys("EXT.PUT", [_key, value]), do: {:ok, ["dynamic:" <> value]}
    def keys(_command, _args), do: :error
  end

  defmodule ShadowingExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "GET",
          arity: -1,
          flags: ["readonly"],
          first_key: 0,
          last_key: 0,
          step: 0,
          access: :read,
          summary: "Invalid shadow command"
        }
      ]
    end

    @impl true
    def handle("GET", _args, _store), do: {:ok, "shadowed"}
  end

  setup do
    previous = Application.get_env(:ferricstore, :command_extensions)
    Application.delete_env(:ferricstore, :command_extensions)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :command_extensions)
        value -> Application.put_env(:ferricstore, :command_extensions, value)
      end
    end)
  end

  test "unknown commands keep the normal unknown-command response without extensions" do
    assert {:error, "ERR unknown command 'ext.ping', with args beginning with: "} =
             Dispatcher.dispatch("EXT.PING", [], MockStore.make())
  end

  test "configured extension commands route through the dispatcher" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    assert {:ok, ["pong", ["a", "b"]]} =
             Dispatcher.dispatch("ext.ping", ["a", "b"], MockStore.make())
  end

  test "extension metadata supplies keys for parsed raw commands" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    assert {:ok, "EXT.PUT", ["k", "v"], {:extension_command, "EXT.PUT", ["k", "v"]},
            ["dynamic:v"]} =
             Dispatcher.parse_raw("ext.put", ["k", "v"])
  end

  test "extension command handlers receive the provided store" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])
    store = MockStore.make()

    assert {:ok, "OK"} = Dispatcher.dispatch("EXT.PUT", ["k", "v"], store)
    assert "v" == store.get.("k")
  end

  test "configured extensions cannot shadow built-in command routing or key metadata" do
    Application.put_env(:ferricstore, :command_extensions, [ShadowingExtension])
    store = MockStore.make(%{"k" => {"v", 0}})

    assert "v" == Dispatcher.dispatch("GET", ["k"], store)
    assert {:ok, "GET", ["k"], {:get, "k"}, ["k"]} = Dispatcher.parse_raw("GET", ["k"])
    assert Enum.count(Catalog.names(), &(&1 == "get")) == 1
  end
end
