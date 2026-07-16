defmodule Ferricstore.Raft.StateMachineTTBGuardTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  alias Ferricstore.Raft.CommandStamp
  alias Ferricstore.Raft.StateMachine

  test "malformed preencoded commands fail without crashing or mutating state" do
    state = %{sentinel: make_ref()}

    assert {^state, {:error, :invalid_preencoded_command}} =
             StateMachine.apply(%{index: 1}, {:ttb, <<0, 1, 2>>}, state)
  end

  test "the stamped-command codec rejects non-current external-term forms" do
    {:ttb, current} = CommandStamp.to_ttb({:put, "key", "value", 0})

    assert {:ok, {{:put, "key", "value", 0}, %{hlc_ts: {_physical, _logical}}}} =
             CommandStamp.decode_ttb(current)

    compressed =
      :erlang.term_to_binary(
        {{:put, "key", String.duplicate("value", 1_024), 0}, %{hlc_ts: {1, 0}}},
        compressed: 9
      )

    assert <<131, 80, _::binary>> = compressed

    assert {:error, :invalid_preencoded_command} = CommandStamp.decode_ttb(compressed)

    assert {:error, :invalid_preencoded_command} =
             CommandStamp.decode_ttb(current <> <<0>>)

    assert {:error, :invalid_preencoded_command} =
             CommandStamp.decode_ttb(:erlang.term_to_binary({:put, "key", "value", 0}))
  end

  test "WARaft blocks replay-position advancement for invalid or unknown commands" do
    source = Ferricstore.Test.SourceFiles.waraft_storage_source()

    assert source =~ "storage_apply_failure_reason?(:invalid_preencoded_command)"
    assert source =~ "storage_apply_failure_reason?({:unknown_command, _command})"
  end
end
