defmodule FerricstoreServer.Connection.AuthNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias FerricstoreServer.Connection.Auth

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

  test "ACL SETUSER returns LOADING instead of crashing before default instance init" do
    state = %{username: "default", acl_cache: :full_access}

    assert {:continue, response, ^state} =
             Auth.dispatch_acl("SETUSER", ["alice", "on", ">secret"], state)

    assert IO.iodata_to_binary(response) =~ "LOADING"
  end

  test "ACL DELUSER returns LOADING instead of crashing before default instance init" do
    state = %{username: "default", acl_cache: :full_access}

    assert {:continue, response, ^state} = Auth.dispatch_acl("DELUSER", ["alice"], state)
    assert IO.iodata_to_binary(response) =~ "LOADING"
  end
end
