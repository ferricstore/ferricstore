defmodule FerricstoreServer.Native.ReadErrorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.LFU
  alias Ferricstore.Test.IsolatedInstance
  alias FerricstoreServer.Native.Commands

  @op_get 0x0101
  @op_mget 0x0104

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    keydir = elem(ctx.keydir_refs, 0)
    key = "invalid-native-read"
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :invalid_offset, 5})

    state = %{
      acl_cache: :full_access,
      require_auth: false,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      compression: :none,
      compact_flow_responses: false
    }

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    %{key: key, state: state}
  end

  test "GET sanitizes storage read failures", %{key: key, state: state} do
    assert {:error, "ERR storage read failed", _state} =
             Commands.execute(@op_get, %{"key" => key}, state)
  end

  test "MGET sanitizes storage read failures", %{key: key, state: state} do
    assert {:error, "ERR storage read failed", _state} =
             Commands.execute(@op_mget, %{"keys" => [key]}, state)
  end
end
