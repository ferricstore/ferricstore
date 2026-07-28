defmodule Ferricstore.Raft.CommandAtomsClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :raft

  alias Ferricstore.Raft.{CommandAtoms, CommandStamp}
  alias Ferricstore.Test.{ClusterHelper, ShardHelpers}

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  test "a fresh replica safely decodes commands containing atoms from unloaded modules" do
    ShardHelpers.ensure_distribution_started!(:command_atoms_runner)
    code_paths = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    {:ok, peer, replica} =
      :peer.start(%{
        name: :"command_atoms_#{System.unique_integer([:positive])}",
        args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
        wait_boot: 120_000
      })

    on_exit(fn -> :peer.stop(peer) end)

    command =
      {:flow_schedule_replace, "schedule-id", %{next_run_at_ms: 2, overlap_queued_due_at_ms: 1}}

    {:ttb, encoded} = CommandStamp.to_ttb(command)

    assert {:error, :invalid_preencoded_command} =
             :erpc.call(replica, CommandStamp, :decode_ttb, [encoded])

    assert :ok = :erpc.call(replica, CommandAtoms, :preload!, [])

    assert {:ok, {^command, %{hlc_ts: {_physical, _logical}, wall_time_ms: _wall}}} =
             :erpc.call(replica, CommandStamp, :decode_ttb, [encoded])
  end
end
