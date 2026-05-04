defmodule Ferricstore.Raft.StateMachineProcessDictGuardTest do
  use ExUnit.Case, async: true

  @state_machine_path Path.expand(
                        "../../../lib/ferricstore/raft/state_machine.ex",
                        __DIR__
                      )

  test "apply entry clears consolidated apply process state" do
    source = File.read!(@state_machine_path)

    [_match, body] =
      Regex.run(
        ~r/(defp clear_stale_pending_state do.*?)(?=\n  defp apply_now_ms)/s,
        source
      )

    assert body =~ "Process.delete(@sm_apply_state_key)",
           "stale consolidated apply state can release a Ra cursor after a crashed apply"
  end

  test "release cursor apply state uses one process dictionary key" do
    source = File.read!(@state_machine_path)

    refute source =~ ":sm_pending_state"
    refute source =~ ":sm_checkpoint_dependencies_clean_before_write"
    refute source =~ ":sm_checkpoint_clean_before_write"
    refute source =~ ":sm_checkpoint_dirty_indices"
    refute source =~ ":sm_release_cursor_blocked"

    assert source =~ "@sm_apply_state_key :sm_apply_state"
  end

  test "no-meta apply path clears consolidated apply process state" do
    source = File.read!(@state_machine_path)

    [_match, body] =
      Regex.run(
        ~r/(defp maybe_release_cursor\(_meta, _old_count, state, result\) do.*?)(?=\n  defp release_cursor_effects)/s,
        source
      )

    assert body =~ "Process.delete(@sm_apply_state_key)",
           "cross-shard sub-apply without Ra meta must not leak stale apply-state into the next apply"
  end

  test "checkpoint clean does not fail open for unresolved instance context" do
    source = File.read!(@state_machine_path)

    refute source =~ "defp checkpoint_clean?(%{instance_ctx: nil}), do: true",
           "unresolved instance checkpoint context must not be treated as clean; only :default may use the legacy carve-out"
  end
end
