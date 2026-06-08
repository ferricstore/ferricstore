defmodule Ferricstore.Raft.StateMachineTest.Sections.HandleAux5 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

  describe "handle_aux/5" do
    test "key_written increments hot key counter" do
      aux = %{hot_keys: %{}}
      int_state = %{some: :internal_state}

      {:no_reply, new_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 1
      assert returned_state == int_state
    end

    test "key_written accumulates counts" do
      aux = %{hot_keys: %{"hot_key" => 5}}
      int_state = %{}

      {:no_reply, new_aux, _} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 6
    end

    test "unknown command returns aux unchanged" do
      aux = %{hot_keys: %{}}
      int_state = %{}

      {:no_reply, returned_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, :unknown, aux, int_state)

      assert returned_aux == aux
      assert returned_state == int_state
    end
  end

  # ---------------------------------------------------------------------------
  # overview/1
  # ---------------------------------------------------------------------------

  describe "overview/1" do
    test "returns shard_index, keydir_size, and applied_count", %{
      state: state,
      shard_index: shard_index
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "ov_k", "ov_v", 0}, state)

      overview = StateMachine.overview(state2)
      assert overview.shard_index == shard_index
      assert overview.keydir_size == 1
      assert overview.applied_count == 1
    end

    test "keydir_size reflects ETS size", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
      {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

      assert StateMachine.overview(s3).keydir_size == 3
    end
  end

  # ---------------------------------------------------------------------------
  # release_cursor for Raft log compaction (spec 2E.5)
  # ---------------------------------------------------------------------------
    end
  end
end
