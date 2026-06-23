defmodule Ferricstore.Commands.ServerInfoNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Server
  alias Ferricstore.Test.MockStore

  @default_key {FerricStore.Instance, :default}

  setup do
    original = :persistent_term.get(@default_key, :missing)
    :persistent_term.erase(@default_key)

    on_exit(fn ->
      case original do
        :missing -> :persistent_term.erase(@default_key)
        ctx -> :persistent_term.put(@default_key, ctx)
      end
    end)

    :ok
  end

  test "INFO stats is available before a default instance is registered" do
    result = Server.handle("INFO", ["stats"], MockStore.make())

    assert result =~ "# Stats"
    assert result =~ "total_commands_processed:0"
    assert result =~ "keyspace_hits:0"
    assert result =~ "read_sample_rate:1:100"
  end

  test "INFO server is available before a default instance is registered" do
    result = Server.handle("INFO", ["server"], MockStore.make())

    assert result =~ "# Server"
    assert result =~ "protocol:embedded"
    assert result =~ "native_port:0"
  end

  test "INFO clients is available before a default instance is registered" do
    result = Server.handle("INFO", ["clients"], MockStore.make())

    assert result =~ "# Clients"
    assert result =~ "connected_clients:0"
    assert result =~ "blocked_clients:0"
  end

  test "INFO all includes early-start server and clients sections" do
    result = Server.handle("INFO", ["all"], MockStore.make())

    assert result =~ "# Server"
    assert result =~ "# Clients"
    assert result =~ "# Stats"
  end
end
