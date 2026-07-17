defmodule Ferricstore.Raft.StateMachineTTBGuardTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  alias Ferricstore.Raft.{ApplyFailure, CommandStamp}
  alias Ferricstore.Raft.StateMachine

  test "malformed preencoded commands fail without crashing or mutating state" do
    state = %{sentinel: make_ref()}

    assert {^state, {:error, :invalid_preencoded_command}} =
             StateMachine.apply(%{index: 1}, {:ttb, <<0, 1, 2>>}, state)
  end

  test "the stamped-command codec rejects non-current external-term forms" do
    {:ttb, current} = CommandStamp.to_ttb({:put, "key", "value", 0})

    assert {:ok,
            {{:put, "key", "value", 0},
             %{hlc_ts: {_physical, _logical}, wall_time_ms: wall_time_ms}}} =
             CommandStamp.decode_ttb(current)

    assert is_integer(wall_time_ms)

    compressed =
      :erlang.term_to_binary(
        {{:put, "key", String.duplicate("value", 1_024), 0}, %{hlc_ts: {1, 0}, wall_time_ms: 1}},
        compressed: 9
      )

    assert <<131, 80, _::binary>> = compressed

    assert {:error, :invalid_preencoded_command} = CommandStamp.decode_ttb(compressed)

    assert {:error, :invalid_preencoded_command} =
             CommandStamp.decode_ttb(current <> <<0>>)

    assert {:error, :invalid_preencoded_command} =
             CommandStamp.decode_ttb(:erlang.term_to_binary({:put, "key", "value", 0}))

    assert {:error, :invalid_preencoded_command} =
             CommandStamp.decode_ttb(
               :erlang.term_to_binary(
                 {{:put, "key", "value", 0}, %{hlc_ts: {1, 0}}},
                 minor_version: 2
               )
             )

    assert {:error, :invalid_preencoded_command} =
             CommandStamp.decode_ttb(
               :erlang.term_to_binary(
                 {{:put, "key", "value", 0}, %{hlc_ts: {1, 0}, wall_time_ms: 2}},
                 minor_version: 2
               )
             )

    assert {:error, :invalid_preencoded_command} =
             CommandStamp.decode_ttb(
               :erlang.term_to_binary(
                 {"not-a-command", %{hlc_ts: {1, 0}, wall_time_ms: 1}},
                 minor_version: 2
               )
             )
  end

  test "WARaft blocks malformed encodings but advances deterministic command errors" do
    assert ApplyFailure.storage_reason?(:invalid_preencoded_command)
    refute ApplyFailure.storage_reason?({:unknown_command, :unsupported})
  end

  test "WARaft blocks projection and internal rollback failures" do
    for reason <- [
          {:waraft_projection_failed, :forced_projection_failure},
          {:hash_type_marker_rollback_failed, {:bitcask_append_failed, :enospc}, :ok},
          {:compound_batch_mutate_rollback_failed, :write_failed, :ok, :restore_failed},
          {:stream_metadata_rollback_failed, :write_failed, :entry_failed, :type_failed}
        ] do
      assert ApplyFailure.storage_reason?(reason)
      assert ApplyFailure.storage_result?({:error, reason})
    end

    refute ApplyFailure.storage_reason?({:user_command_failed, :ordinary_error})
  end
end
