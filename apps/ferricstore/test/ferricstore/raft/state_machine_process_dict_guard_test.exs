defmodule Ferricstore.Raft.StateMachineProcessDictGuardTest do
  use ExUnit.Case, async: true

  @state_machine_path Path.expand(
                        "../../../lib/ferricstore/raft/state_machine.ex",
                        __DIR__
                      )

  test "apply entry clears checkpoint dependency process state" do
    source = File.read!(@state_machine_path)

    [_match, body] =
      Regex.run(
        ~r/(defp clear_stale_pending_state do.*?)(?=\n  defp apply_now_ms)/s,
        source
      )

    assert body =~ "Process.delete(:sm_checkpoint_dependencies_clean_before_write)",
           "stale dependency-clean state can release a Ra cursor after a crashed apply"

    assert body =~ "Process.delete(:sm_checkpoint_clean_before_write)",
           "stale shard-clean state can release a Ra cursor after a crashed apply"

    assert body =~ "Process.delete(:sm_checkpoint_dirty_indices)",
           "stale dirty checkpoint indices can block or misdirect the next release decision"
  end
end
