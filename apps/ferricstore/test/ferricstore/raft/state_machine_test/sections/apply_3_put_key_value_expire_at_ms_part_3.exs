defmodule Ferricstore.Raft.StateMachineTest.Sections.Apply3PutKeyValueExpireAtMsPart3 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "apply/3 with {:put, key, value, expire_at_ms} part 3" do
        test "compound_batch_delete Bitcask append errors keep existing fields visible", %{
          state: state,
          ets: ets
        } do
          redis_key = "compound_delete_batch_failure_hash"
          existing_a = CompoundKey.hash_field(redis_key, "a")
          existing_b = CompoundKey.hash_field(redis_key, "b")
          missing = CompoundKey.hash_field(redis_key, "missing")

          {state2, {:ok, [:ok, :ok]}} =
            StateMachine.apply(
              %{},
              {:compound_batch_put, redis_key,
               [{existing_a, "old_a", 0}, {existing_b, "old_b", 0}]},
              state
            )

          old_a = :ets.lookup(ets, existing_a)
          old_b = :ets.lookup(ets, existing_b)

          file_id = 9_500_000 + :erlang.unique_integer([:positive])
          bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
          File.mkdir_p!(bad_active_path)
          bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

          {_new_state, result} =
            StateMachine.apply(
              %{},
              {:compound_batch_delete, redis_key, [existing_a, existing_b, missing]},
              bad_state
            )

          assert {:error, {:bitcask_append_failed, _reason}} = result
          assert old_a == :ets.lookup(ets, existing_a)
          assert old_b == :ets.lookup(ets, existing_b)
          assert [] == :ets.lookup(ets, missing)
        end

        test "compound_put appends one Bitcask record", %{
          state: state,
          shard_index: shard_index
        } do
          handler_id = {:compound_put_single_append, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :bitcask, :append],
              &__MODULE__.relay_compound_put_append_telemetry/4,
              self()
            )

          redis_key = "compound_single_append_hash"
          field_key = CompoundKey.hash_field(redis_key, "field")

          try do
            {_new_state, :ok} =
              StateMachine.apply(%{}, {:compound_put, field_key, "value", 0}, state)

            assert_receive {:compound_put_append, measurements,
                            %{shard_index: ^shard_index, status: :ok}},
                           500

            assert measurements.batch_size == 1
            assert measurements.delete_count == 0
            refute_receive {:compound_put_append, _measurements, _metadata}, 100
          after
            :telemetry.detach(handler_id)
          end
        end

        test "emits bounded apply and Bitcask append telemetry", %{
          state: state,
          shard_index: shard_index
        } do
          handler_id = {:state_machine_quorum_telemetry, self(), make_ref()}

          :ok =
            :telemetry.attach_many(
              handler_id,
              [
                [:ferricstore, :raft, :apply],
                [:ferricstore, :bitcask, :append]
              ],
              fn event, measurements, metadata, test_pid ->
                send(test_pid, {:quorum_telemetry, event, measurements, metadata})
              end,
              self()
            )

          try do
            {_new_state, :ok} =
              StateMachine.apply(%{}, {:put, "telemetry_key", "value", 0}, state)

            assert_receive {:quorum_telemetry, [:ferricstore, :bitcask, :append], append_meas,
                            %{shard_index: ^shard_index, status: :ok}},
                           500

            assert append_meas.batch_size == 1
            assert append_meas.batch_bytes > 0
            assert is_integer(append_meas.duration_us)

            assert_receive {:quorum_telemetry, [:ferricstore, :raft, :apply], apply_meas,
                            %{shard_index: ^shard_index, result: :ok, disk: :ok}},
                           500

            assert is_integer(apply_meas.duration_us)
          after
            :telemetry.detach(handler_id)
          end
        end

        test "writes value to disk and ETS", %{state: state, ets: ets, shard_index: shard_index} do
          {new_state, result} =
            StateMachine.apply(%{}, {:put, "key1", "value1", 0}, state)

          assert result == :ok
          assert new_state.applied_count == 1

          # Verify ETS (v2 7-tuple format) — value is available immediately
          assert [{"key1", "value1", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "key1")

          # Flush background writer so disk location is materialized
          BitcaskWriter.flush(shard_index)

          # Verify disk via pread
          [{_, _, _, _, fid, off, _}] = :ets.lookup(ets, "key1")
          assert is_integer(fid)

          log_path =
            Path.join(
              state.shard_data_path,
              "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
            )

          assert {:ok, "value1"} = NIF.v2_pread_at(log_path, off)
        end

        test "put with expiry stores expire_at_ms", %{state: state, ets: ets} do
          future = System.os_time(:millisecond) + 60_000

          {_new_state, result} =
            StateMachine.apply(%{}, {:put, "expiring", "val", future}, state)

          assert result == :ok

          assert [{"expiring", "val", ^future, _lfu, _fid, _off, _vsize}] =
                   :ets.lookup(ets, "expiring")
        end

        test "put overwrites previous value", %{state: state, ets: ets} do
          {state2, :ok} = StateMachine.apply(%{}, {:put, "k", "v1", 0}, state)
          {state3, :ok} = StateMachine.apply(%{}, {:put, "k", "v2", 0}, state2)

          assert state3.applied_count == 2
          assert [{"k", "v2", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "k")
        end

        test "increments applied_count on each put", %{state: state} do
          {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
          {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
          {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

          assert s1.applied_count == 1
          assert s2.applied_count == 2
          assert s3.applied_count == 3
        end
      end
    end
  end
end
