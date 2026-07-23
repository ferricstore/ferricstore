defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowBlobSideChannelApply do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

      describe "Flow blob side-channel apply" do
        test "prepared Flow create payload stores a ref in the Raft/Bitcask value record", %{
          state: state
        } do
          setup_flow_indexes(state)

          id = "flow-blob-create"
          type = "blob-flow"
          partition_key = "tenant-blob-flow"
          payload = :binary.copy("flow-payload", 1024)
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          command =
            {:flow_create, state_key,
             %{
               id: id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               payload: payload
             }}

          assert {:ok, prepared} =
                   BlobCommand.prepare(
                     %{data_dir: state.data_dir, blob_side_channel_threshold_bytes: 128},
                     state.shard_index,
                     command,
                     single_member?: true
                   )

          refute prepared == command

          {state, {:applied_at, 1, :ok}, _effects} =
            StateMachine.apply(%{index: 1, system_time: 1_000}, prepared, state)

          record = flow_record!(state, state_key)

          assert is_binary(record.payload_ref)
          assert_flow_blob_value!(state, record.payload_ref, payload)
        end

        test "prepared Flow named value put stores a ref in the Raft/Bitcask value record", %{
          state: state
        } do
          setup_flow_indexes(state)

          id = "flow-blob-named-value"
          type = "blob-flow"
          partition_key = "tenant-blob-flow-named"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
          payload = :binary.copy("flow-named-value", 1024)

          create_command =
            {:flow_create, state_key,
             %{id: id, type: type, state: "queued", partition_key: partition_key}}

          {state, {:applied_at, 1, :ok}, _effects} =
            StateMachine.apply(%{index: 1, system_time: 1_000}, create_command, state)

          command =
            {:flow_named_value_put, state_key,
             %{id: id, name: "doc", value: payload, partition_key: partition_key}}

          assert {:ok, prepared} =
                   BlobCommand.prepare(
                     %{data_dir: state.data_dir, blob_side_channel_threshold_bytes: 128},
                     state.shard_index,
                     command,
                     single_member?: true
                   )

          refute prepared == command

          {state, {:applied_at, 2, {:ok, %{ref: value_ref}}}, _effects} =
            StateMachine.apply(%{index: 2, system_time: 1_010}, prepared, state)

          record = flow_record!(state, state_key)

          assert get_in(record.value_refs, ["doc", :ref]) == value_ref
          assert_flow_blob_value!(state, value_ref, payload)
        end

        test "prepared Flow create named values store refs in the Raft/Bitcask value records", %{
          state: state
        } do
          setup_flow_indexes(state)

          id = "flow-blob-create-named-values"
          type = "blob-flow"
          partition_key = "tenant-blob-flow-create-named"
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
          payload = :binary.copy("flow-create-named-value", 1024)

          command =
            {:flow_create, state_key,
             %{
               id: id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               values: %{"doc" => payload}
             }}

          assert {:ok, prepared} =
                   BlobCommand.prepare(
                     %{data_dir: state.data_dir, blob_side_channel_threshold_bytes: 128},
                     state.shard_index,
                     command,
                     single_member?: true
                   )

          refute prepared == command

          {state, {:applied_at, 1, :ok}, _effects} =
            StateMachine.apply(%{index: 1, system_time: 1_000}, prepared, state)

          record = flow_record!(state, state_key)
          value_ref = get_in(record.value_refs, ["doc", :ref])

          assert is_binary(value_ref)
          assert_flow_blob_value!(state, value_ref, payload)
        end

        test "prepared Flow transition_many shared payload stores one shared blob ref", %{
          state: state
        } do
          setup_flow_indexes(state)

          partition_key = "tenant-blob-flow-shared-transition"
          type = "blob-flow"
          id_a = "flow-blob-transition-a"
          id_b = "flow-blob-transition-b"
          batch_key = Ferricstore.Flow.Keys.state_key("__transition_batch__", partition_key)
          payload = :binary.copy("flow-shared-payload", 1024)

          create_command =
            {:flow_create_many, batch_key,
             %{
               records: [
                 %{id: id_a, type: type, state: "queued", partition_key: partition_key},
                 %{id: id_b, type: type, state: "queued", partition_key: partition_key}
               ]
             }}

          {state, {:applied_at, 1, :ok}, _effects} =
            StateMachine.apply(%{index: 1, system_time: 1_000}, create_command, state)

          command =
            {:flow_transition_many, batch_key,
             %{
               shared: %{
                 from_state: "queued",
                 to_state: "ready",
                 payload: payload,
                 now_ms: 1_100,
                 run_at_ms: 1_200
               },
               records: [
                 %{id: id_a, partition_key: partition_key, fencing_token: 0},
                 %{id: id_b, partition_key: partition_key, fencing_token: 0}
               ]
             }}

          assert {:ok, prepared} =
                   BlobCommand.prepare(
                     %{data_dir: state.data_dir, blob_side_channel_threshold_bytes: 128},
                     state.shard_index,
                     command,
                     single_member?: true
                   )

          assert {:flow_transition_many, ^batch_key, prepared_attrs} = prepared
          assert {:ferricstore_flow_blob_value_ref, encoded_ref} = prepared_attrs.shared.payload
          refute Enum.any?(prepared_attrs.records, &Map.has_key?(&1, :payload))

          {state, {:applied_at, 2, :ok}, _effects} =
            StateMachine.apply(%{index: 2, system_time: 1_100}, prepared, state)

          record_a = flow_record!(state, Ferricstore.Flow.Keys.state_key(id_a, partition_key))
          record_b = flow_record!(state, Ferricstore.Flow.Keys.state_key(id_b, partition_key))

          assert record_a.payload_ref != record_b.payload_ref
          assert flow_value!(state, record_a.payload_ref) == encoded_ref
          assert flow_value!(state, record_b.payload_ref) == encoded_ref
          assert_flow_blob_value!(state, record_a.payload_ref, payload)
          assert_flow_blob_value!(state, record_b.payload_ref, payload)
        end

        test "prepared Flow create payload does not enqueue direct LMDB value projection", %{
          state: state,
          shard_index: shard_index
        } do
          old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
          old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
          old_max_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

          Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
          Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 10_000)
          state = %{state | flow_lmdb_mirror?: true}

          setup_flow_indexes(state)

          {:ok, writer_pid} =
            Ferricstore.Flow.LMDBWriter.start_link(
              instance_name: state.instance_name,
              shard_index: shard_index,
              data_dir: state.data_dir
            )

          handler_id = {:flow_blob_value_lmdb_enqueue, self(), make_ref()}

          :ok =
            :telemetry.attach(
              handler_id,
              [:ferricstore, :flow, :lmdb_writer, :backlog],
              fn _event, measurements, metadata, test_pid ->
                send(test_pid, {:flow_lmdb_backlog, measurements, metadata})
              end,
              self()
            )

          on_exit(fn ->
            :telemetry.detach(handler_id)

            try do
              if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5_000)
            catch
              :exit, _ -> :ok
            end

            restore_env(:flow_lmdb_mode, old_mode)
            restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
            restore_env(:flow_lmdb_max_batch_ops, old_max_ops)
          end)

          id = "flow-blob-create-no-lmdb"
          type = "blob-flow"
          partition_key = "tenant-blob-flow"
          payload = :binary.copy("flow-payload", 1024)
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          command =
            {:flow_create, state_key,
             %{
               id: id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               payload: payload
             }}

          assert {:ok, prepared} =
                   BlobCommand.prepare(
                     %{data_dir: state.data_dir, blob_side_channel_threshold_bytes: 128},
                     state.shard_index,
                     command,
                     single_member?: true
                   )

          {state, {:applied_at, 1, :ok}, _effects} =
            StateMachine.apply(%{index: 1, system_time: 1_000}, prepared, state)

          assert_receive {:flow_lmdb_backlog, %{pending_ops: 4}, %{shard_index: ^shard_index}},
                         500

          assert :ok = Ferricstore.Flow.LMDBWriter.flush(state.instance_name, shard_index)
          record = flow_record!(state, state_key)
          assert :not_found = Ferricstore.Flow.LMDB.get(state.flow_lmdb_path, state_key)
          assert :not_found = Ferricstore.Flow.LMDB.get(state.flow_lmdb_path, record.payload_ref)
        end

        test "prepared Flow create payload does not reopen the blob during apply", %{state: state} do
          setup_flow_indexes(state)

          id = "flow-blob-create-no-read"
          type = "blob-flow"
          partition_key = "tenant-blob-flow"
          payload = :binary.copy("flow-payload", 1024)
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          command =
            {:flow_create, state_key,
             %{
               id: id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               payload: payload
             }}

          assert {:ok, prepared} =
                   BlobCommand.prepare(
                     %{data_dir: state.data_dir, blob_side_channel_threshold_bytes: 128},
                     state.shard_index,
                     command,
                     single_member?: true
                   )

          test_pid = self()

          Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
            send(test_pid, {:blob_opened_during_apply, path})
            File.open(path, modes)
          end)

          on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

          {state, {:applied_at, 1, :ok}, _effects} =
            StateMachine.apply(%{index: 1, system_time: 1_000}, prepared, state)

          record = flow_record!(state, state_key)

          assert is_binary(record.payload_ref)
          refute_received {:blob_opened_during_apply, _path}
        end

        test "prepared Flow create payload tolerates keydir table disappearing during shutdown",
             %{
               state: state,
               ets: ets
             } do
          setup_flow_indexes(state)

          id = "flow-blob-create-shutdown"
          type = "blob-flow"
          partition_key = "tenant-blob-flow"
          payload = :binary.copy("flow-payload", 1024)
          state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

          command =
            {:flow_create, state_key,
             %{
               id: id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               payload: payload
             }}

          assert {:ok, prepared} =
                   BlobCommand.prepare(
                     %{data_dir: state.data_dir, blob_side_channel_threshold_bytes: 128},
                     state.shard_index,
                     command,
                     single_member?: true
                   )

          :ets.delete(ets)

          assert {_state, {:applied_at, 2, :ok}, _effects} =
                   StateMachine.apply(%{index: 2, system_time: 1_000}, prepared, state)

          assert :undefined == :ets.whereis(ets)
        end
      end
    end
  end
end
