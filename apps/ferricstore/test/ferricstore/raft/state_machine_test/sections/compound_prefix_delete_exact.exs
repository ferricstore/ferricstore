defmodule Ferricstore.Raft.StateMachineTest.Sections.CompoundPrefixDeleteExact do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.ApplyContext
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.{CompoundKey, LFU, Promotion}
      alias Ferricstore.Store.Shard.CompoundMemberIndex

      @tag :compound_prefix_delete_exact
      test "compound prefix deletion rejects work above the replicated member budget atomically",
           %{
             state: state,
             ets: ets
           } do
        redis_key = "prefix-delete-budget"
        prefix = CompoundKey.hash_prefix(redis_key)
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_delete_member_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        Enum.each([field_a, field_b], fn key ->
          :ets.insert(ets, {key, "value", 0, LFU.initial(), 0, 0, 5})
          CompoundMemberIndex.put(index, key)
        end)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :compound_delete_budget_exceeded}} =
                 StateMachine.apply(%{}, {:compound_delete_prefix, prefix}, limited_state)

        assert [_row] = :ets.lookup(ets, field_a)
        assert [_row] = :ets.lookup(ets, field_b)
      end

      @tag :compound_prefix_delete_exact
      test "string overwrite preserves the collection when its bounded cleanup is rejected", %{
        state: state,
        ets: ets
      } do
        redis_key = "string-overwrite-budget"
        type_key = CompoundKey.type_key(redis_key)
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_delete_member_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})

        Enum.each([field_a, field_b], fn key ->
          :ets.insert(ets, {key, "value", 0, LFU.initial(), 0, 0, 5})
          CompoundMemberIndex.put(index, key)
        end)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :compound_delete_budget_exceeded}} =
                 StateMachine.apply(%{}, {:put, redis_key, "string", 0}, limited_state)

        assert [] = :ets.lookup(ets, redis_key)
        assert [_row] = :ets.lookup(ets, type_key)
        assert [_row] = :ets.lookup(ets, field_a)
        assert [_row] = :ets.lookup(ets, field_b)
      end

      @tag :compound_prefix_delete_exact
      test "compound prefix deletion propagates storage failure without dropping members", %{
        state: state,
        ets: ets
      } do
        redis_key = "prefix-delete-failure"
        prefix = CompoundKey.hash_prefix(redis_key)
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        Enum.each([field_a, field_b], fn key ->
          :ets.insert(ets, {key, "value", 0, LFU.initial(), 0, 0, 5})
          CompoundMemberIndex.put(index, key)
        end)

        bad_state = state_with_missing_active_file(state)

        assert {_state, {:error, :active_file_unavailable}} =
                 StateMachine.apply(%{}, {:compound_delete_prefix, prefix}, bad_state)

        assert [{^field_a, "value", _, _, _, _, _}] = :ets.lookup(ets, field_a)
        assert [{^field_b, "value", _, _, _, _, _}] = :ets.lookup(ets, field_b)
      end

      @tag :compound_prefix_delete_exact
      test "compound prefix deletion refuses an unready partial catalog", %{
        state: state,
        ets: ets
      } do
        redis_key = "prefix-delete-partial"
        prefix = CompoundKey.set_prefix(redis_key)
        indexed_key = CompoundKey.set_member(redis_key, "indexed")
        missing_key = CompoundKey.set_member(redis_key, "missing")
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        :ets.delete_all_objects(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {indexed_key, "1", 0, LFU.initial(), 0, 0, 1})
        :ets.insert(ets, {missing_key, "1", 0, LFU.initial(), 0, 0, 1})
        CompoundMemberIndex.put(index, indexed_key)

        assert {_state, {:error, :compound_member_index_unavailable}} =
                 StateMachine.apply(%{}, {:compound_delete_prefix, prefix}, state)

        assert [_row] = :ets.lookup(ets, indexed_key)
        assert [_row] = :ets.lookup(ets, missing_key)
      end

      @tag :compound_prefix_delete_exact
      test "WATCH refuses an unready partial compound catalog", %{state: state, ets: ets} do
        redis_key = "watch-partial-catalog"
        type_key = CompoundKey.type_key(redis_key)
        indexed_key = CompoundKey.set_member(redis_key, "indexed")
        missing_key = CompoundKey.set_member(redis_key, "missing")
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        :ets.delete_all_objects(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {
          type_key,
          CompoundKey.encode_type(:set),
          0,
          LFU.initial(),
          0,
          0,
          3
        })

        :ets.insert(ets, {indexed_key, "1", 0, LFU.initial(), 0, 0, 1})
        :ets.insert(ets, {missing_key, "1", 0, LFU.initial(), 0, 0, 1})
        CompoundMemberIndex.put(index, indexed_key)

        result = apply_result_value(StateMachine.apply(%{}, {:watch_tokens, [redis_key]}, state))

        assert {:error, :watch_compound_index_unavailable} = result
        assert [_row] = :ets.lookup(ets, indexed_key)
        assert [_row] = :ets.lookup(ets, missing_key)
      end

      @tag :compound_prefix_delete_exact
      test "promoted prefix deletion durably removes its marker and directory", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-prefix-delete"
        member_a = CompoundKey.set_member(redis_key, "a")
        member_b = CompoundKey.set_member(redis_key, "b")

        {state, dedicated_path, _log_path} =
          promoted_delete_fixture(state, ets, shard_index, redis_key, [
            {member_a, "1", 0},
            {member_b, "1", 0}
          ])

        assert {state_after_prefix, :ok} =
                 StateMachine.apply(
                   %{},
                   {:compound_delete_prefix, CompoundKey.set_prefix(redis_key)},
                   state
                 )

        refute Map.has_key?(state_after_prefix.promoted_instances, redis_key)
        refute File.dir?(dedicated_path)
        assert [] = :ets.lookup(ets, Promotion.marker_key(redis_key))
        assert [] = :ets.lookup(ets, member_a)
        assert [] = :ets.lookup(ets, member_b)

        type_key = CompoundKey.type_key(redis_key)

        assert {_state_after_type, :ok} =
                 StateMachine.apply(%{}, {:compound_delete, type_key}, state_after_prefix)

        assert [] = :ets.lookup(ets, type_key)
      end

      @tag :compound_prefix_delete_exact
      test "state-machine prefix deletion does not scan the shard keydir" do
        source =
          File.read!(
            Path.expand(
              "../../../lib/ferricstore/raft/state_machine/sections/read_warm.ex",
              __DIR__
            )
          )

        [_before, body] =
          String.split(source, "defp do_compound_member_prefix_delete", parts: 2)

        [body | _after] = String.split(body, "defp do_compound_put", parts: 2)

        refute body =~ ":ets.select"
        assert body =~ "CompoundMemberIndex"
      end

      @tag :compound_prefix_delete_exact
      test "WARaft compound prefix projection uses the bounded exact catalog" do
        source =
          File.read!(
            Path.expand(
              "../../../lib/ferricstore/raft/waraft_storage/sections/segment_projection.ex",
              __DIR__
            )
          )

        [_before, body] = String.split(source, "defp segment_project_delete_prefix", parts: 2)

        [body | _after] = String.split(body, "defp segment_project_zset_put", parts: 2)

        refute body =~ ":ets.select"
        refute body =~ "segment_project_prefix_keys"
        assert body =~ "CompoundMemberIndex.keys_for_prefix"
        assert body =~ ":limit_exceeded"
        refute source =~ "segment_project_put_compound_member_index"
        refute source =~ "segment_project_delete_compound_member_index"
      end

      defp promoted_delete_fixture(state, ets, shard_index, redis_key, entries) do
        dedicated_path = Promotion.dedicated_path(state.data_dir, shard_index, :set, redis_key)
        log_path = Path.join(dedicated_path, "00000.log")
        File.mkdir_p!(dedicated_path)
        File.touch!(log_path)

        type_key = CompoundKey.type_key(redis_key)
        durable_entries = [{type_key, "set", 0} | entries]
        {:ok, locations} = NIF.v2_append_batch(log_path, durable_entries)

        Enum.zip(durable_entries, locations)
        |> Enum.each(fn {{key, value, expire_at_ms}, {offset, value_size}} ->
          :ets.insert(ets, {key, value, expire_at_ms, LFU.initial(), 0, offset, value_size})
        end)

        marker = Promotion.marker_key(redis_key)
        :ets.insert(ets, {marker, "set", 0, LFU.initial(), 0, 0, 3})

        index = state.compound_member_index_name
        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        Enum.each(entries, fn {key, _value, _expire_at_ms} ->
          CompoundMemberIndex.put(index, key)
        end)

        promoted_instances =
          Map.put(state.promoted_instances, redis_key, %{path: dedicated_path, type: :set})

        {%{state | promoted_instances: promoted_instances}, dedicated_path, log_path}
      end
    end
  end
end
