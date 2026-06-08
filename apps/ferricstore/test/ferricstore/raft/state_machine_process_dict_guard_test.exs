defmodule Ferricstore.Raft.StateMachineProcessDictGuardTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  test "apply entry clears consolidated apply process state" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    body =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "clear_stale_pending_state")

    assert body =~ "Process.delete(@sm_apply_state_key)",
           "stale consolidated apply state can release a Ra cursor after a crashed apply"
  end

  test "release cursor apply state uses one process dictionary key" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    refute source =~ ":sm_pending_state"
    refute source =~ ":sm_checkpoint_dependencies_clean_before_write"
    refute source =~ ":sm_checkpoint_clean_before_write"
    refute source =~ ":sm_checkpoint_dirty_indices"
    refute source =~ ":sm_release_cursor_blocked"

    assert source =~ "@sm_apply_state_key :sm_apply_state"
  end

  test "pending write apply state declares and clears one key set" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    with_pending_body =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "with_pending_writes")

    assert source =~ "@sm_pending_write_keys"
    assert with_pending_body =~ "init_pending_write_process_state(state)"
    assert with_pending_body =~ "clear_pending_write_process_state()"

    for key <- [
          ":sm_pending_writes",
          ":sm_pending_originals",
          ":sm_pending_values",
          ":sm_pending_lmdb_values",
          ":sm_pending_lmdb_mirror_ops",
          ":sm_pending_lmdb_mirror_after_flush",
          ":sm_pending_fast_put_batch",
          ":sm_pending_fast_delete_batch"
        ] do
      assert source =~ key
    end
  end

  test "delete_batch fast path stages tombstones and publishes ETS after append" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "defp apply_delete_batch_keys_fast",
           "delete_batch should have a dedicated pure-delete fast path"

    fast_body =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "apply_delete_batch_keys_fast"
      )

    assert fast_body =~ "queue_pending_delete_fast"
    refute fast_body =~ "do_delete("

    publish_body =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "apply_fast_delete_pending_locations",
        ":ets.delete(state.ets, key)"
      )

    assert publish_body =~ ":ets.delete(state.ets, key)"
    assert publish_body =~ "maybe_queue_lmdb_state_delete_after_publish(state, key)"
  end

  test "no-meta apply path clears consolidated apply process state" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    body =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "maybe_release_cursor",
        "Process.delete(@sm_apply_state_key)"
      )

    assert body =~ "Process.delete(@sm_apply_state_key)",
           "cross-shard sub-apply without Ra meta must not leak stale apply-state into the next apply"
  end

  test "checkpoint clean does not fail open for unresolved instance context" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    refute source =~ "defp checkpoint_clean?(%{instance_ctx: nil}), do: true",
           "unresolved instance checkpoint context must not be treated as clean; only :default may use the legacy carve-out"
  end
end
