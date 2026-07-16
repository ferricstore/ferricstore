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

  defmodule InvalidKeysExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.INVALID_KEYS",
          arity: 2,
          flags: ["write"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :write
        }
      ]
    end

    @impl true
    def handle("EXT.INVALID_KEYS", _args, _store), do: {:ok, "must not execute"}

    @impl true
    def keys("EXT.INVALID_KEYS", [_key]), do: {:ok, [:not_a_binary_key]}
  end

  defmodule SnapshotExtensionA do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.SNAPSHOT",
          arity: 2,
          flags: ["write"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :write
        }
      ]
    end

    @impl true
    def handle("EXT.SNAPSHOT", [_key], _store), do: {:ok, :provider_a}

    @impl true
    def keys("EXT.SNAPSHOT", [key]), do: {:ok, ["a:" <> key]}
  end

  defmodule SnapshotExtensionB do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      [
        %{
          name: "EXT.SNAPSHOT",
          arity: 2,
          flags: ["readonly"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :read
        }
      ]
    end

    @impl true
    def handle("EXT.SNAPSHOT", [_key], _store), do: {:ok, :provider_b}

    @impl true
    def keys("EXT.SNAPSHOT", [key]), do: {:ok, ["b:" <> key]}
  end

  defmodule CountingExtension do
    @behaviour Ferricstore.Commands.Extension

    @impl true
    def commands do
      if pid = Application.get_env(:ferricstore, :extension_test_pid) do
        send(pid, :extension_commands_called)
      end

      [
        %{
          name: "EXT.COUNTED",
          arity: 2,
          flags: ["write"],
          first_key: 1,
          last_key: 1,
          step: 1,
          access: :write
        }
      ]
    end

    @impl true
    def handle("EXT.COUNTED", [_key], _store), do: {:ok, "OK"}
  end

  setup do
    previous = Application.get_env(:ferricstore, :command_extensions)
    previous_test_pid = Application.get_env(:ferricstore, :extension_test_pid)
    Application.delete_env(:ferricstore, :command_extensions)
    Application.delete_env(:ferricstore, :extension_test_pid)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :command_extensions)
        value -> Application.put_env(:ferricstore, :command_extensions, value)
      end

      case previous_test_pid do
        nil -> Application.delete_env(:ferricstore, :extension_test_pid)
        value -> Application.put_env(:ferricstore, :extension_test_pid, value)
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

  test "extension metadata supplies keys for prepared raw commands" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    assert {:ok,
            %Ferricstore.Commands.PreparedCommand{
              command: "EXT.PUT",
              args: ["k", "v"],
              ast: {:extension_command, TestExtension, "EXT.PUT", ["k", "v"], :write},
              acl_keys: ["dynamic:v"]
            }} = Dispatcher.prepare_raw("ext.put", ["k", "v"])
  end

  test "catalog key discovery uses an extension's dynamic key callback" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])

    assert {:ok, ["dynamic:v"]} = Catalog.get_keys("EXT.PUT", ["k", "v"])
  end

  test "prepared extension commands retain the authorized provider snapshot" do
    Application.put_env(:ferricstore, :command_extensions, [SnapshotExtensionA])

    assert {:ok, prepared} = Dispatcher.prepare_raw("EXT.SNAPSHOT", ["tenant-key"])
    assert prepared.acl_keys == ["a:tenant-key"]
    assert prepared.read_keys == []
    assert prepared.write_keys == ["a:tenant-key"]

    Application.put_env(:ferricstore, :command_extensions, [SnapshotExtensionB])

    assert {:ok, :provider_a} = Dispatcher.dispatch_prepared(prepared, MockStore.make())
  end

  test "preparing an extension command reads provider metadata once" do
    Application.put_env(:ferricstore, :extension_test_pid, self())
    Application.put_env(:ferricstore, :command_extensions, [CountingExtension])

    assert {:ok, prepared} = Dispatcher.prepare_raw("EXT.COUNTED", ["key"])
    assert prepared.acl_keys == ["key"]

    assert_receive :extension_commands_called
    refute_receive :extension_commands_called
  end

  test "invalid extension key metadata fails closed before dispatch" do
    Application.put_env(:ferricstore, :command_extensions, [InvalidKeysExtension])

    assert {:error, message} = Dispatcher.prepare_raw("EXT.INVALID_KEYS", ["tenant:key"])
    assert message =~ "invalid key metadata"

    assert {:error, ^message} =
             Dispatcher.dispatch_raw("EXT.INVALID_KEYS", ["tenant:key"], MockStore.make())
  end

  test "extension command handlers receive the provided store" do
    Application.put_env(:ferricstore, :command_extensions, [TestExtension])
    store = MockStore.make()

    assert {:ok, "OK"} = Dispatcher.dispatch("EXT.PUT", ["k", "v"], store)
    assert "v" == store.get.("k")
  end

  test "extension request context helper returns attached trusted context" do
    assert %{} == Ferricstore.Commands.Extension.request_context(MockStore.make())

    assert %{"subject" => "client-1"} ==
             Ferricstore.Commands.Extension.request_context(%{
               store: MockStore.make(),
               request_context: %{"subject" => "client-1"}
             })
  end

  test "configured extensions cannot shadow built-in command routing or key metadata" do
    Application.put_env(:ferricstore, :command_extensions, [ShadowingExtension])
    store = MockStore.make(%{"k" => {"v", 0}})

    assert "v" == Dispatcher.dispatch("GET", ["k"], store)

    assert {:ok,
            %Ferricstore.Commands.PreparedCommand{
              command: "GET",
              args: ["k"],
              ast: {:get, "k"},
              acl_keys: ["k"]
            }} = Dispatcher.prepare_raw("GET", ["k"])

    assert Enum.count(Catalog.names(), &(&1 == "get")) == 1
  end
end
