defmodule Ferricstore.Raft.MembershipGateTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.MembershipGate

  test "stable membership work excludes concurrent membership mutation" do
    parent = self()

    stable =
      Task.async(fn ->
        MembershipGate.with_stable_membership(fn ->
          send(parent, {:stable_membership_entered, self()})

          receive do
            :release_stable_membership -> :stable_done
          end
        end)
      end)

    assert_receive {:stable_membership_entered, stable_pid}, 1_000

    change =
      Task.async(fn ->
        MembershipGate.with_membership_change(fn ->
          send(parent, {:membership_change_entered, self()})
          :change_done
        end)
      end)

    refute_receive {:membership_change_entered, _pid}, 100
    send(stable_pid, :release_stable_membership)
    assert :stable_done = Task.await(stable, 1_000)
    assert_receive {:membership_change_entered, change_pid}, 1_000
    assert change_pid == change.pid
    assert :change_done = Task.await(change, 1_000)
  end

  test "durable flush and public membership workflows use the shared gate" do
    store_source = Ferricstore.Test.SourceFiles.store_ops_source()
    backend_source = Ferricstore.Test.SourceFiles.waraft_backend_source()

    assert store_source =~ "MembershipGate.with_stable_membership"
    assert backend_source =~ "MembershipGate.with_membership_change"
  end
end
