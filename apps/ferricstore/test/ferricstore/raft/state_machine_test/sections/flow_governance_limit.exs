defmodule Ferricstore.Raft.StateMachineTest.Sections.FlowGovernanceLimit do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      @tag :flow_governance_limit
      test "malformed limit mutation is rejected as an applied command", %{state: state} do
        command = {:flow_governance_limit_mutate, "owner-key"}

        assert {next_state, {:error, "ERR invalid flow limit mutation"}} =
                 Ferricstore.Raft.StateMachine.apply(%{}, command, state)

        assert next_state.applied_count == state.applied_count + 1
      end

      @tag :flow_governance_limit
      test "new limit replay resumes a durable catalog intent without cursor metadata", %{
        state: state,
        ets: ets,
        dir: dir
      } do
        assert_flow_governance_catalog_prefix_replay(state, ets, dir, 1, :same_owner)
      end

      @tag :flow_governance_limit
      test "new limit replay resumes durable catalog intent and cursor metadata", %{
        state: state,
        ets: ets,
        dir: dir
      } do
        assert_flow_governance_catalog_prefix_replay(state, ets, dir, 2, :same_owner)
      end

      @tag :flow_governance_limit
      test "new limit replaces an intent-only prefix whose owner never committed", %{
        state: state,
        ets: ets,
        dir: dir
      } do
        assert_flow_governance_catalog_prefix_replay(state, ets, dir, 1, :next_owner)
      end

      defp assert_flow_governance_catalog_prefix_replay(
             state,
             ets,
             dir,
             prefix_length,
             replay_mode
           ) do
        alias Ferricstore.Bitcask.NIF
        alias Ferricstore.Flow.Governance.LimitCatalogOutbox
        alias Ferricstore.Flow.Governance.LimitRecord
        alias Ferricstore.Flow.Keys
        alias Ferricstore.Raft.StateMachine, as: RawStateMachine
        alias Ferricstore.Store.Shard.Lifecycle

        scope = "torn-limit-#{System.unique_integer([:positive])}"
        owner_key = Keys.governance_limit_key(scope)
        command = flow_governance_limit_lease_command(scope)

        instance_name = :"torn_limit_#{System.unique_integer([:positive])}"

        instance_ctx =
          FerricStore.Instance.build(instance_name, shard_count: 1, data_dir: state.data_dir)

        state = %{
          state
          | shard_index: 0,
            instance_name: instance_name,
            instance_ctx: instance_ctx
        }

        meta_key = Keys.governance_limit_catalog_outbox_meta_key(state.shard_index)
        intent_key = Keys.governance_limit_catalog_outbox_intent_key(state.shard_index, 1)

        duplicate_intent_key =
          Keys.governance_limit_catalog_outbox_intent_key(state.shard_index, 2)

        old_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

        Application.put_env(:ferricstore, :standalone_durability_hook, fn path, batch ->
          assert [
                   {:put, ^intent_key, _intent, 0},
                   {:put, ^meta_key, _meta, 0},
                   {:put, ^owner_key, _owner, 0}
                 ] = batch

          prefix =
            batch
            |> Enum.take(prefix_length)
            |> Enum.map(fn {:put, key, value, expire_at_ms} ->
              {key, value, expire_at_ms}
            end)

          assert {:ok, locations} = NIF.v2_append_batch(path, prefix)
          assert length(locations) == prefix_length
          {:error, :simulated_crash_after_catalog_prefix}
        end)

        on_exit(fn ->
          restore_env(:standalone_durability_hook, old_hook)
          FerricStore.Instance.cleanup(instance_name)
        end)

        assert {_failed_state,
                {:error, {:bitcask_append_failed, :simulated_crash_after_catalog_prefix}}} =
                 RawStateMachine.apply_standalone_command(command, state)

        assert [] == :ets.lookup(ets, owner_key)
        assert [] == :ets.lookup(ets, meta_key)
        assert [] == :ets.lookup(ets, intent_key)

        :ok = Lifecycle.recover_keydir(dir, ets, state.shard_index)
        assert [] == :ets.lookup(ets, owner_key)
        assert [{^intent_key, nil, 0, _lfu, 0, _offset, _size}] = :ets.lookup(ets, intent_key)

        if prefix_length == 1 do
          assert [] == :ets.lookup(ets, meta_key)
        else
          assert [{^meta_key, nil, 0, _lfu, 0, _offset, _size}] = :ets.lookup(ets, meta_key)
        end

        Application.delete_env(:ferricstore, :standalone_durability_hook)

        {replayed_scope, replayed_owner_key, replayed_command} =
          case replay_mode do
            :same_owner ->
              {scope, owner_key, command}

            :next_owner ->
              next_scope = "next-limit-#{System.unique_integer([:positive])}"

              {next_scope, Keys.governance_limit_key(next_scope),
               flow_governance_limit_lease_command(next_scope)}
          end

        assert {_replayed_state, {:flow_limit_reply, {:ok, %{lease: %{epoch: 1}}}}} =
                 RawStateMachine.apply_standalone_command(replayed_command, state)

        if replay_mode == :next_owner, do: assert(:ets.lookup(ets, owner_key) == [])

        assert [{^replayed_owner_key, owner_raw, 0, _lfu, 0, _offset, _size}] =
                 :ets.lookup(ets, replayed_owner_key)

        assert {:ok, %{scope: ^replayed_scope}} = LimitRecord.decode_owner(owner_raw)

        assert [{^intent_key, intent_raw, 0, _lfu, 0, _offset, _size}] =
                 :ets.lookup(ets, intent_key)

        assert {:ok, ^replayed_owner_key} = LimitCatalogOutbox.decode_intent(intent_raw)

        assert [{^meta_key, meta_raw, 0, _lfu, 0, _offset, _size}] =
                 :ets.lookup(ets, meta_key)

        assert {:ok, %{head: 1, tail: 1}} = LimitCatalogOutbox.decode_meta(meta_raw)
        assert [] == :ets.lookup(ets, duplicate_intent_key)
      end

      defp flow_governance_limit_lease_command(scope) do
        owner_key = Ferricstore.Flow.Keys.governance_limit_key(scope)

        {:flow_governance_limit_mutate, owner_key,
         %{
           op: :lease,
           scope: scope,
           shard_id: 0,
           shard_count: 1,
           amount: 1,
           ttl_ms: 1_000,
           now_ms: 1_000,
           configuration: %{limit: 1, config_version: nil, policy_version: nil}
         }}
      end
    end
  end
end
