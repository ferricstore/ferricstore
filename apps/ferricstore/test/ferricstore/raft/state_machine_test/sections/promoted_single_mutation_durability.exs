defmodule Ferricstore.Raft.StateMachineTest.Sections.PromotedSingleMutationDurability do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.{CompoundKey, LFU, Promotion}
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Store.Shard.CompoundMemberIndex
      alias Ferricstore.Store.Shard.CompoundRevisionIndex

      @tag :promoted_single_mutation_durability
      test "direct promoted writes use the replicated index as their logical revision", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-replicated-revision"
        field_key = CompoundKey.hash_field(redis_key, "field")

        {state, _log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
            {field_key, "old", 0}
          ])

        ra_index = 9_000_001

        {state, {:applied_at, ^ra_index, :ok}, _effects} =
          StateMachine.apply(
            %{index: ra_index, system_time: 1_000},
            {:compound_put, field_key, "new", 0},
            state
          )

        assert {:ok, {_epoch, ^ra_index}} =
                 CompoundRevisionIndex.revision_token(
                   state.compound_revision_index_name,
                   field_key
                 )
      end

      @tag :promoted_single_mutation_durability
      test "same-value promoted writes change the logical WATCH revision", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-watch-same-value"
        field_key = CompoundKey.hash_field(redis_key, "field")

        {state, _log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
            {field_key, "same", 0}
          ])

        {state, token_before} = StateMachine.apply(%{}, {:watch_token, redis_key}, state)
        {state, 0} = StateMachine.apply(%{}, {:hset_single, redis_key, "field", "same"}, state)
        {_state, token_after} = StateMachine.apply(%{}, {:watch_token, redis_key}, state)

        refute token_after == token_before
      end

      @tag :promoted_single_mutation_durability
      test "promoted compaction does not change a logical WATCH token", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-watch-compaction"
        field_key = CompoundKey.hash_field(redis_key, "field")

        {state, _log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        {state, token_before} = StateMachine.apply(%{}, {:watch_token, redis_key}, state)

        compaction_state =
          state
          |> Map.put(:index, shard_index)
          |> Map.put(:keydir, ets)
          |> Map.put(:compound_member_index, state.compound_member_index_name)

        dedicated_path = state.promoted_instances[redis_key].path

        assert {:ok, _state} =
                 ShardCompound.compact_dedicated_result(
                   compaction_state,
                   redis_key,
                   dedicated_path
                 )

        {_state, token_after} =
          StateMachine.apply(%{}, {:watch_token, redis_key}, state)

        assert token_after == token_before
      end

      @tag :promoted_single_mutation_durability
      test "committed promoted writes report exact maintenance deltas", %{
        state: state,
        ets: ets
      } do
        redis_key = "promoted-maintenance-hash"
        field_key = CompoundKey.hash_field(redis_key, "field")
        shard_name = :"promoted_maintenance_#{System.unique_integer([:positive])}"
        parent = self()
        collector = spawn_link(fn -> promoted_maintenance_forward(parent) end)
        true = Process.register(collector, shard_name)

        on_exit(fn ->
          if Process.alive?(collector), do: Process.exit(collector, :normal)
        end)

        latch = :ets.new(:promoted_maintenance_latch, [:set, :public])
        instance_ctx = %FerricStore.Instance{shard_names: {shard_name}, latch_refs: {latch}}
        state = %{state | shard_index: 0, instance_ctx: instance_ctx}

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "old", 0}
          ])

        {_state, 0} = StateMachine.apply(%{}, {:hset_single, redis_key, "field", "new"}, state)

        record_size = 26 + byte_size(field_key) + byte_size("new")
        old_record_size = 26 + byte_size(field_key) + byte_size("old")

        assert_receive {:promoted_maintenance_after_commit, ^redis_key,
                        %{
                          appended_bytes: ^record_size,
                          reclaimable_bytes: ^old_record_size,
                          writes: 1
                        }}
      end

      @tag :promoted_single_mutation_durability
      test "promoted hash single writes stay in the dedicated log", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-single-hash"
        field_key = CompoundKey.hash_field(redis_key, "counter")

        {state, log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
            {field_key, "1", 0}
          ])

        {state, 0} = StateMachine.apply(%{}, {:hset_single, redis_key, "counter", "2"}, state)
        assert_promoted_value(log_path, field_key, "2")

        {_state, 3} = StateMachine.apply(%{}, {:hincrby, redis_key, "counter", 1}, state)
        assert_promoted_value(log_path, field_key, "3")
      end

      @tag :promoted_single_mutation_durability
      test "promoted set single writes and removals stay in the dedicated log", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-single-set"
        old_member = CompoundKey.set_member(redis_key, "old")
        new_member = CompoundKey.set_member(redis_key, "new")

        {state, log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :set, [
            {old_member, "1", 0}
          ])

        {state, 1} = StateMachine.apply(%{}, {:sadd_single, redis_key, "new"}, state)
        assert_promoted_value(log_path, new_member, "1")

        {_state, 1} = StateMachine.apply(%{}, {:srem_single, redis_key, "old"}, state)
        assert_promoted_tombstone(log_path, old_member)
      end

      @tag :promoted_single_mutation_durability
      test "promoted sorted-set single writes and removals stay in the dedicated log", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-single-zset"
        old_member = CompoundKey.zset_member(redis_key, "old")
        new_member = CompoundKey.zset_member(redis_key, "new")

        {state, log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :zset, [
            {old_member, "1.0", 0}
          ])

        {state, 1} = StateMachine.apply(%{}, {:zadd_single, redis_key, 2.0, "new"}, state)
        assert_promoted_value(log_path, new_member, "2.0")

        {state, "3.0"} = StateMachine.apply(%{}, {:zincrby, redis_key, 1.0, "new"}, state)
        assert_promoted_value(log_path, new_member, "3.0")

        {_state, 1} = StateMachine.apply(%{}, {:zrem_single, redis_key, "old"}, state)
        assert_promoted_tombstone(log_path, old_member)
      end

      @tag :promoted_single_mutation_durability
      @tag :append_result_validation
      test "malformed promoted put locations do not publish keydir state", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-malformed-single-put"
        field_key = CompoundKey.hash_field(redis_key, "field")

        {state, _log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
            {field_key, "old", 0}
          ])

        original = :ets.lookup(ets, field_key)

        Process.put(:ferricstore_promoted_append_hook, fn
          :record, _path, _payload -> {:ok, {-1, :bad_record_size}}
        end)

        try do
          assert {_state,
                  {:error,
                   {:bitcask_append_result_mismatch,
                    {:invalid_location, 0, {-1, :bad_record_size}}}}} =
                   StateMachine.apply(%{}, {:compound_put, field_key, "new", 0}, state)

          assert original == :ets.lookup(ets, field_key)
        after
          Process.delete(:ferricstore_promoted_append_hook)
        end
      end

      @tag :promoted_single_mutation_durability
      @tag :append_result_validation
      test "malformed promoted tombstone locations do not delete keydir state", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-malformed-single-delete"
        field_key = CompoundKey.hash_field(redis_key, "field")

        {state, _log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
            {field_key, "old", 0}
          ])

        original = :ets.lookup(ets, field_key)

        Process.put(:ferricstore_promoted_append_hook, fn
          :tombstone, _path, _payload -> {:ok, :bad_location}
        end)

        try do
          assert {_state,
                  {:error,
                   {:bitcask_append_result_mismatch, {:invalid_location, 0, :bad_location}}}} =
                   StateMachine.apply(%{}, {:compound_delete, field_key}, state)

          assert original == :ets.lookup(ets, field_key)
        after
          Process.delete(:ferricstore_promoted_append_hook)
        end
      end

      @tag :promoted_single_mutation_durability
      @tag :append_result_validation
      test "malformed promoted batch locations cannot publish a valid prefix", %{
        state: state,
        ets: ets,
        shard_index: shard_index
      } do
        redis_key = "promoted-malformed-batch"
        existing = CompoundKey.hash_field(redis_key, "existing")
        new_field = CompoundKey.hash_field(redis_key, "new")

        {state, _log_path} =
          promoted_single_fixture(state, ets, shard_index, redis_key, :hash, [
            {existing, "old", 0}
          ])

        original = :ets.lookup(ets, existing)

        Process.put(:ferricstore_promoted_append_hook, fn
          :batch, _path, _payload -> {:ok, [{0, 3}, :bad_location]}
        end)

        try do
          assert {_state,
                  {:error,
                   {:bitcask_append_result_mismatch, {:invalid_location, 1, :bad_location}}}} =
                   StateMachine.apply(
                     %{},
                     {:compound_batch_put, redis_key,
                      [{existing, "new", 0}, {new_field, "new", 0}]},
                     state
                   )

          assert original == :ets.lookup(ets, existing)
          assert [] == :ets.lookup(ets, new_field)
        after
          Process.delete(:ferricstore_promoted_append_hook)
        end
      end

      @tag :promoted_single_mutation_durability
      test "transaction exact promoted prefix deletion retires its dedicated storage", %{
        state: state,
        ets: ets
      } do
        redis_key = "promoted-transaction-prefix-delete"
        field_key = CompoundKey.hash_field(redis_key, "field")
        marker_key = Promotion.marker_key(redis_key)
        state = promoted_cleanup_test_state(state)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        dedicated_path = state.promoted_instances[redis_key].path

        assert {:ok, flushed_state} =
                 StateMachine.__transaction_compound_delete_prefix_for_test__(
                   state,
                   redis_key,
                   CompoundKey.hash_prefix(redis_key)
                 )

        refute Map.has_key?(flushed_state.promoted_instances, redis_key)
        assert [] == :ets.lookup(ets, field_key)
        assert_cleanup_marker(ets, marker_key, :hash, 0)

        assert_receive {:cleanup_promoted_after_commit, ^redis_key, :hash, ^dedicated_path,
                        _incarnation_token}
      end

      @tag :promoted_single_mutation_durability
      test "transaction prefix deletion rejects an unreadable generation before publish", %{
        state: state,
        ets: ets
      } do
        redis_key = "promoted-transaction-unreadable-generation"
        field_key = CompoundKey.hash_field(redis_key, "field")
        marker_key = Promotion.marker_key(redis_key)
        state = promoted_cleanup_test_state(state)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        marker = Promotion.encode_marker(:hash, :promoted, Promotion.new_generation())
        malformed = binary_part(marker, 0, byte_size(marker) - 1)

        :ets.insert(
          ets,
          {marker_key, malformed, 0, LFU.initial(), 0, 0, byte_size(malformed)}
        )

        original_field = :ets.lookup(ets, field_key)

        assert {:error, :promotion_marker_unavailable} =
                 StateMachine.__transaction_compound_delete_prefix_for_test__(
                   state,
                   redis_key,
                   CompoundKey.hash_prefix(redis_key)
                 )

        assert original_field == :ets.lookup(ets, field_key)

        assert [{^marker_key, ^malformed, 0, _lfu, 0, 0, _size}] =
                 :ets.lookup(ets, marker_key)
      end

      @tag :promoted_single_mutation_durability
      test "exact promoted prefix deletion rejects a missing generation before mutation", %{
        state: state,
        ets: ets
      } do
        redis_key = "promoted-prefix-missing-generation"
        field_key = CompoundKey.hash_field(redis_key, "field")
        marker_key = Promotion.marker_key(redis_key)
        state = promoted_cleanup_test_state(state)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        :ets.delete(ets, marker_key)
        original_field = :ets.lookup(ets, field_key)

        assert {_state, {:error, :promotion_marker_unavailable}} =
                 StateMachine.apply(
                   %{},
                   {:compound_delete_prefix, CompoundKey.hash_prefix(redis_key)},
                   state
                 )

        assert original_field == :ets.lookup(ets, field_key)
        assert File.dir?(state.promoted_instances[redis_key].path)
      end

      @tag :promoted_single_mutation_durability
      test "transaction cleanup remains recoverable when no shard worker is registered", %{
        state: state,
        ets: ets
      } do
        redis_key = "promoted-transaction-durable-cleanup"
        field_key = CompoundKey.hash_field(redis_key, "field")
        marker_key = Promotion.marker_key(redis_key)
        state = promoted_cleanup_test_state(state, worker?: false)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        dedicated_path = state.promoted_instances[redis_key].path

        assert {:ok, flushed_state} =
                 StateMachine.__transaction_compound_delete_prefix_for_test__(
                   state,
                   redis_key,
                   CompoundKey.hash_prefix(redis_key)
                 )

        refute Map.has_key?(flushed_state.promoted_instances, redis_key)

        assert [{^marker_key, marker, 0, _lfu, _fid, _offset, _size}] =
                 :ets.lookup(ets, marker_key)

        assert {:ok, :hash, :cleanup, 0} = Promotion.decode_marker(marker)
        assert File.dir?(dedicated_path)

        assert %{} =
                 Promotion.recover_promoted(
                   state.shard_data_path,
                   ets,
                   state.data_dir,
                   0,
                   state.instance_ctx
                 )

        assert [] == :ets.lookup(ets, marker_key)
        refute File.dir?(dedicated_path)
      end

      @tag :promoted_single_mutation_durability
      test "transaction promoted batch deletion retires storage when it includes the type key", %{
        state: state,
        ets: ets
      } do
        redis_key = "promoted-transaction-batch-delete"
        field_key = CompoundKey.hash_field(redis_key, "field")
        type_key = CompoundKey.type_key(redis_key)
        marker_key = Promotion.marker_key(redis_key)
        state = promoted_cleanup_test_state(state)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        dedicated_path = state.promoted_instances[redis_key].path

        assert {:ok, flushed_state} =
                 StateMachine.__transaction_compound_batch_delete_for_test__(
                   state,
                   redis_key,
                   [field_key, type_key]
                 )

        refute Map.has_key?(flushed_state.promoted_instances, redis_key)
        assert [] == :ets.lookup(ets, field_key)
        assert [] == :ets.lookup(ets, type_key)
        assert_cleanup_marker(ets, marker_key, :hash, 0)

        assert_receive {:cleanup_promoted_after_commit, ^redis_key, :hash, ^dedicated_path,
                        _incarnation_token}
      end

      @tag :promoted_single_mutation_durability
      test "exact prefix deletion rejects an unreadable promotion generation before apply", %{
        state: state,
        ets: ets
      } do
        redis_key = "promoted-prefix-unreadable-generation"
        field_key = CompoundKey.hash_field(redis_key, "field")
        marker_key = Promotion.marker_key(redis_key)
        state = promoted_cleanup_test_state(state)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        marker = Promotion.encode_marker(:hash, :promoted, Promotion.new_generation())
        malformed = binary_part(marker, 0, byte_size(marker) - 1)

        :ets.insert(
          ets,
          {marker_key, malformed, 0, LFU.initial(), 0, 0, byte_size(malformed)}
        )

        original_field = :ets.lookup(ets, field_key)

        assert {_state, {:error, :promotion_marker_unavailable}} =
                 StateMachine.apply(
                   %{},
                   {:compound_delete_prefix, CompoundKey.hash_prefix(redis_key)},
                   state
                 )

        assert original_field == :ets.lookup(ets, field_key)

        assert [{^marker_key, ^malformed, 0, _lfu, 0, 0, _size}] =
                 :ets.lookup(ets, marker_key)
      end

      @tag :promoted_single_mutation_durability
      test "ordinary promotion marker deletion waits then fails closed", %{
        state: state,
        ets: ets
      } do
        redis_key = "ordinary-marker-delete-latch"
        field_key = CompoundKey.hash_field(redis_key, "field")
        marker_key = Promotion.marker_key(redis_key)
        state = promoted_cleanup_test_state(state)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        owner = %{instance_ctx: state.instance_ctx, shard_index: 0}
        latch = Promotion.acquire_compaction_latch(owner, redis_key)

        deletion =
          Task.async(fn ->
            StateMachine.apply(%{}, {:delete, marker_key}, state)
          end)

        try do
          assert nil == Task.yield(deletion, 50)
        after
          Promotion.release_compaction_latch(latch)
        end

        assert {_next_state, {:error, :promotion_marker_delete_requires_cleanup}} =
                 Task.await(deletion, 5_000)

        assert [_marker] = :ets.lookup(ets, marker_key)
        assert [_field] = :ets.lookup(ets, field_key)
        assert File.dir?(state.promoted_instances[redis_key].path)
      end

      @tag :promoted_single_mutation_durability
      test "delete batches reject promotion marker deletion atomically", %{
        state: state,
        ets: ets
      } do
        redis_key = "ordinary-marker-delete-batch"
        field_key = CompoundKey.hash_field(redis_key, "field")
        marker_key = Promotion.marker_key(redis_key)
        plain_key = "ordinary-marker-delete-batch-plain"
        state = promoted_cleanup_test_state(state)

        {state, _log_path} =
          promoted_single_fixture(state, ets, 0, redis_key, :hash, [
            {field_key, "value", 0}
          ])

        assert {state, :ok} = StateMachine.apply(%{}, {:put, plain_key, "plain", 0}, state)

        assert {_next_state, {:error, :promotion_marker_delete_requires_cleanup}} =
                 StateMachine.apply(%{}, {:delete_batch, [plain_key, marker_key]}, state)

        assert [{^plain_key, "plain", 0, _lfu, _fid, _offset, 5}] =
                 :ets.lookup(ets, plain_key)

        assert [_marker] = :ets.lookup(ets, marker_key)
        assert [_field] = :ets.lookup(ets, field_key)
        assert File.dir?(state.promoted_instances[redis_key].path)
      end

      defp promoted_cleanup_test_state(state, opts \\ []) do
        suffix = System.unique_integer([:positive])
        shard_name = :"promoted_cleanup_#{suffix}"

        if Keyword.get(opts, :worker?, true) do
          parent = self()
          collector = spawn_link(fn -> promoted_maintenance_forward(parent) end)
          true = Process.register(collector, shard_name)

          on_exit(fn ->
            if Process.alive?(collector), do: Process.exit(collector, :normal)
          end)
        end

        latch = :ets.new(:promoted_cleanup_latch, [:set, :public])

        instance_ctx = %{
          FerricStore.Instance.get(:default)
          | name: :"promoted_cleanup_#{suffix}",
            data_dir: state.data_dir,
            data_dir_expanded: Path.expand(state.data_dir),
            shard_count: 1,
            shard_names: {shard_name},
            latch_refs: {latch}
        }

        %{state | shard_index: 0, instance_ctx: instance_ctx}
      end

      defp promoted_single_fixture(state, ets, shard_index, redis_key, type, entries) do
        dedicated_path = Promotion.dedicated_path(state.data_dir, shard_index, type, redis_key)
        log_path = Path.join(dedicated_path, "00000.log")
        File.mkdir_p!(dedicated_path)
        File.touch!(log_path)

        type_key = CompoundKey.type_key(redis_key)
        type_value = Atom.to_string(type)
        durable_entries = [{type_key, type_value, 0} | entries]
        {:ok, locations} = NIF.v2_append_batch(log_path, durable_entries)

        Enum.zip(durable_entries, locations)
        |> Enum.each(fn {{key, value, expire_at_ms}, {offset, value_size}} ->
          :ets.insert(
            ets,
            {key, value, expire_at_ms, LFU.initial(), 0, offset, value_size}
          )
        end)

        marker_key = Promotion.marker_key(redis_key)
        :ets.insert(ets, {marker_key, type_value, 0, LFU.initial(), 0, 0, byte_size(type_value)})

        promoted_single_ensure_table(state.compound_member_index_name, :ordered_set)
        promoted_single_ensure_table(state.zset_score_index_name, :ordered_set)
        promoted_single_ensure_table(state.zset_score_lookup_name, :set)

        CompoundMemberIndex.reset(state.compound_member_index_name)

        Enum.each(durable_entries, fn {key, _value, _expire_at_ms} ->
          CompoundMemberIndex.put(state.compound_member_index_name, key)
        end)

        promoted_instances =
          Map.put(state.promoted_instances, redis_key, %{path: dedicated_path, type: type})

        {%{state | promoted_instances: promoted_instances}, log_path}
      end

      defp promoted_single_ensure_table(table, type) do
        if :ets.info(table) == :undefined do
          :ets.new(table, [type, :public, :named_table])
          on_exit(fn -> safe_delete_ets(table) end)
        end
      end

      defp assert_cleanup_marker(ets, marker_key, type, generation) do
        assert [{^marker_key, marker, 0, _lfu, _fid, _offset, _size}] =
                 :ets.lookup(ets, marker_key)

        assert {:ok, ^type, :cleanup, ^generation} = Promotion.decode_marker(marker)
      end

      defp assert_promoted_value(log_path, key, expected) do
        assert {:ok, records} = NIF.v2_scan_file(log_path)
        assert {^key, offset, _value_size, _expire_at_ms, false} = promoted_latest(records, key)
        assert {:ok, ^expected} = NIF.v2_pread_at(log_path, offset)
      end

      defp assert_promoted_tombstone(log_path, key) do
        assert {:ok, records} = NIF.v2_scan_file(log_path)
        assert {^key, _offset, 0, _expire_at_ms, true} = promoted_latest(records, key)
      end

      defp promoted_latest(records, key) do
        records
        |> Enum.filter(&(elem(&1, 0) == key))
        |> List.last()
      end

      defp promoted_maintenance_forward(parent) do
        receive do
          message ->
            send(parent, message)
            promoted_maintenance_forward(parent)
        end
      end
    end
  end
end
