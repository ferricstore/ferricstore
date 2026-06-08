defmodule Ferricstore.Raft.StateMachineTest.Sections.StateMachineCompoundReads do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "state-machine compound reads" do
        test "HINCRBY reads cold promoted hash values from dedicated Bitcask", %{
          state: state,
          ets: ets
        } do
          key = "hash_promoted_cold_hincrby_#{System.unique_integer([:positive])}"
          field_key = CompoundKey.hash_field(key, "counter")

          dedicated_path =
            Ferricstore.Store.Promotion.dedicated_path(
              state.data_dir,
              state.shard_index,
              :hash,
              key
            )

          File.mkdir_p!(dedicated_path)
          dedicated_file = Path.join(dedicated_path, "00000.log")
          File.touch!(dedicated_file)

          {:ok, [{cold_offset, cold_size}]} =
            NIF.v2_append_batch(dedicated_file, [{field_key, "41", 0}])

          :ets.insert(ets, {field_key, nil, 0, 1, 0, cold_offset, cold_size})

          {_state2, result} =
            StateMachine.apply(%{}, {:hincrby, key, "counter", 1}, state)

          assert 42 == result
          assert [{^field_key, "42", 0, _lfu, 0, _off, 2}] = :ets.lookup(ets, field_key)
        end

        test "HINCRBYFLOAT reads cold promoted hash values from dedicated Bitcask", %{
          state: state,
          ets: ets
        } do
          key = "hash_promoted_cold_hincrbyfloat_#{System.unique_integer([:positive])}"
          field_key = CompoundKey.hash_field(key, "ratio")

          dedicated_path =
            Ferricstore.Store.Promotion.dedicated_path(
              state.data_dir,
              state.shard_index,
              :hash,
              key
            )

          File.mkdir_p!(dedicated_path)
          dedicated_file = Path.join(dedicated_path, "00000.log")
          File.touch!(dedicated_file)

          {:ok, [{cold_offset, cold_size}]} =
            NIF.v2_append_batch(dedicated_file, [{field_key, "41.5", 0}])

          :ets.insert(ets, {field_key, nil, 0, 1, 0, cold_offset, cold_size})

          {_state2, result} =
            StateMachine.apply(%{}, {:hincrbyfloat, key, "ratio", 1.0}, state)

          assert "42.5" == result
          assert [{^field_key, "42.5", 0, _lfu, 0, _off, 4}] = :ets.lookup(ets, field_key)
        end

        test "ZINCRBY reads cold promoted zset values from dedicated Bitcask", %{
          state: state,
          ets: ets
        } do
          key = "zset_promoted_cold_zincrby_#{System.unique_integer([:positive])}"
          member_key = CompoundKey.zset_member(key, "Palermo")

          dedicated_path =
            Ferricstore.Store.Promotion.dedicated_path(
              state.data_dir,
              state.shard_index,
              :zset,
              key
            )

          File.mkdir_p!(dedicated_path)
          dedicated_file = Path.join(dedicated_path, "00000.log")
          File.touch!(dedicated_file)

          {:ok, [{cold_offset, cold_size}]} =
            NIF.v2_append_batch(dedicated_file, [{member_key, "41.5", 0}])

          :ets.insert(ets, {member_key, nil, 0, 1, 0, cold_offset, cold_size})

          {_state2, result} =
            StateMachine.apply(%{}, {:zincrby, key, 1.0, "Palermo"}, state)

          assert "42.5" == result
          assert [{^member_key, "42.5", 0, _lfu, 0, _off, 4}] = :ets.lookup(ets, member_key)
        end
      end

      # ---------------------------------------------------------------------------
      # apply/3 with :put
      # ---------------------------------------------------------------------------
    end
  end
end
