defmodule Ferricstore.Commands.InternalKeyDynamicAccessTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.MockStore

  @error "ERR access to internal keys is not allowed"
  @state_key "f:{f}:s:flow-1"

  test "dynamic multi-key commands reject a reserved source before store access" do
    store = guarded_store()

    for {command, args} <- [
          {"CMS.MERGE", ["cms:dest", "1", @state_key]},
          {"TDIGEST.MERGE", ["tdigest:dest", "1", @state_key]},
          {"SINTERCARD", ["2", "ordinary", @state_key]},
          {"PFCOUNT", ["ordinary", @state_key]}
        ] do
      assert {:error, @error} = Dispatcher.dispatch(command, args, store)
    end

    refute_received {:unexpected_store_access, _operation, _key}
  end

  test "subcommand metadata operations authorize the data key" do
    store = guarded_store()

    for {command, args} <- [
          {"OBJECT", ["ENCODING", @state_key]},
          {"MEMORY", ["USAGE", @state_key]},
          {"XINFO", ["STREAM", @state_key]},
          {"XGROUP", ["CREATE", @state_key, "group-1", "$", "MKSTREAM"]}
        ] do
      assert {:error, @error} = Dispatcher.dispatch(command, args, store)
    end

    refute_received {:unexpected_store_access, _operation, _key}
  end

  test "extra native operations authorize their first key even on invalid input" do
    store = guarded_store()

    for {command, args} <- [
          {"CAS", [@state_key, "expected", "replacement"]},
          {"LOCK", [@state_key, "owner", "invalid-ttl"]},
          {"UNLOCK", [@state_key, "owner", "unexpected-extra-arg"]},
          {"EXTEND", [@state_key, "owner", "invalid-ttl"]},
          {"RATELIMIT.ADD", [@state_key, "invalid-window", "10"]},
          {"KEY_INFO", [@state_key]},
          {"FETCH_OR_COMPUTE", [@state_key, "invalid-ttl"]},
          {"FETCH_OR_COMPUTE_RESULT", [@state_key, "token", "value", "invalid-ttl"]},
          {"FETCH_OR_COMPUTE_ERROR", [@state_key, "token", "error"]}
        ] do
      assert {:error, @error} = Dispatcher.dispatch(command, args, store)
    end

    refute_received {:unexpected_store_access, _operation, _key}
  end

  defp guarded_store do
    owner = self()

    MockStore.make()
    |> Map.put(:get, fn key ->
      send(owner, {:unexpected_store_access, :read, key})
      nil
    end)
    |> Map.put(:put, fn key, _value, _expire_at_ms ->
      send(owner, {:unexpected_store_access, :write, key})
      :ok
    end)
  end
end
