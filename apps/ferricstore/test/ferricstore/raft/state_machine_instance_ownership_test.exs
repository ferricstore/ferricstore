defmodule Ferricstore.Raft.StateMachineInstanceOwnershipTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Store.ExpiryTracker

  test "state from another data directory cannot mutate a reused instance's runtime refs" do
    suffix = System.unique_integer([:positive])
    name = :"state_machine_instance_owner_#{suffix}"
    root = Path.join(System.tmp_dir!(), "state_machine_instance_owner_#{suffix}")
    registered_data_dir = Path.join(root, "registered")
    isolated_data_dir = Path.join(root, "isolated")
    shard_data_path = Ferricstore.DataDir.shard_data_path(isolated_data_dir, 0)
    active_file_path = Path.join(shard_data_path, "00000.log")
    keydir = :ets.new(:state_machine_instance_owner, [:set, :public])

    File.mkdir_p!(shard_data_path)
    File.touch!(active_file_path)

    registered_ctx =
      FerricStore.Instance.build(name, data_dir: registered_data_dir, shard_count: 1)

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_data_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        instance_name: name
      })

    expiry_count = ExpiryTracker.count(registered_ctx, 0)
    expiry_due = ExpiryTracker.next_due(registered_ctx, 0)
    keydir_bytes = :atomics.get(registered_ctx.keydir_binary_bytes, 1)
    checkpoint_dirty = :atomics.get(registered_ctx.checkpoint_flags, 1)

    on_exit(fn ->
      ExpiryTracker.restore(registered_ctx, 0, expiry_count, expiry_due)
      :atomics.put(registered_ctx.keydir_binary_bytes, 1, keydir_bytes)
      :atomics.put(registered_ctx.checkpoint_flags, 1, checkpoint_dirty)
      FerricStore.Instance.cleanup(name)
      File.rm_rf!(root)
    end)

    assert {_state, :ok} =
             StateMachine.apply(%{}, {:put, "isolated", "value", 10_000}, state)

    assert ExpiryTracker.count(registered_ctx, 0) == expiry_count
    assert ExpiryTracker.next_due(registered_ctx, 0) == expiry_due
    assert :atomics.get(registered_ctx.keydir_binary_bytes, 1) == keydir_bytes
    assert :atomics.get(registered_ctx.checkpoint_flags, 1) == checkpoint_dirty
  end
end
