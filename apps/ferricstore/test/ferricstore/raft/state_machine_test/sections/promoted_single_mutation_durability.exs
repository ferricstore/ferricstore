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

      @tag :promoted_single_mutation_durability
      test "actual promoted apply batches keep their epoch across separate append phases", %{
        state: state,
        ets: ets
      } do
        key = "promoted-apply-publication-phases"
        fields = Enum.map(["a", "b"], &CompoundKey.hash_field(key, &1))
        state = promoted_publication_test_state(state)

        {state, _path} =
          promoted_single_fixture(state, ets, 0, key, :hash, Enum.map(fields, &{&1, "old", 0}))

        parent = self()

        writer =
          Task.async(fn ->
            Process.put(:ferricstore_promoted_publication_hook, fn ->
              count = Process.get(:publication_phase, 0) + 1
              Process.put(:publication_phase, count)

              if count == 2 do
                send(parent, {:between_publications, self()})

                receive do
                  :continue -> :ok
                after
                  5_000 -> raise "publication timeout"
                end
              end
            end)

            StateMachine.apply(
              %{},
              {:batch, Enum.map(fields, &{:compound_put, &1, "new", 0})},
              state
            )
          end)

        assert_receive {:between_publications, publisher}, 2_000
        assert :ets.lookup_element(ets, hd(fields), 2) == "new"
        assert :ets.lookup_element(ets, List.last(fields), 2) == "old"

        reader =
          Task.async(fn ->
            Ferricstore.Store.PromotedPublication.read(state.instance_ctx, 0, fn ->
              Enum.map(fields, &:ets.lookup_element(ets, &1, 2))
            end)
          end)

        try do
          assert Task.yield(reader, 20) == nil
          send(publisher, :continue)
          assert {_state, {:ok, [:ok, :ok]}} = Task.await(writer)
          assert Task.await(reader) == ["new", "new"]
        after
          send(publisher, :continue)
          Task.shutdown(writer, :brutal_kill)
          Task.shutdown(reader, :brutal_kill)
        end
      end

      @tag :promoted_single_mutation_durability
      test "actual promoted apply errors after publication keep the shortcut fenced", %{
        state: state,
        ets: ets
      } do
        key = "promoted-apply-later-error"
        field = CompoundKey.hash_field(key, "a")
        state = promoted_publication_test_state(state)
        {state, _path} = promoted_single_fixture(state, ets, 0, key, :hash, [{field, "old", 0}])

        Process.put(:ferricstore_promoted_append_hook, fn :record, _path, _payload ->
          if Process.get(:published_once, false) do
            {:error, :forced_later_append_failure}
          else
            Process.put(:published_once, true)
            :passthrough
          end
        end)

        try do
          {_state, result} =
            StateMachine.apply(
              %{},
              {:batch, [{:compound_put, field, "new", 0}, {:compound_put, field, "later", 0}]},
              state
            )

          refute result == {:ok, [:ok, :ok]}

          assert Ferricstore.Store.PromotedPublication.read(state.instance_ctx, 0, fn ->
                   :ets.lookup_element(ets, field, 2)
                 end) == :fallback
        after
          Process.delete(:ferricstore_promoted_append_hook)
          Process.delete(:published_once)
        end
      end

      @tag :promoted_single_mutation_durability
      test "failed actual promoted transaction publication retains its failure fence", %{
        state: state,
        ets: ets
      } do
        key = "promoted-transaction-publication-failure"
        field = CompoundKey.hash_field(key, "a")
        state = promoted_publication_test_state(state)
        {state, _path} = promoted_single_fixture(state, ets, 0, key, :hash, [{field, "old", 0}])
        previous = Application.get_env(:ferricstore, :cross_shard_transaction_hook)

        Application.put_env(:ferricstore, :cross_shard_transaction_hook, fn
          {:published_group, _idx} -> raise "forced transaction publication failure"
          _event -> :ok
        end)

        try do
          assert_raise RuntimeError, "forced transaction publication failure", fn ->
            StateMachine.apply(%{}, {:tx_execute, [{"HSET", [key, "a", "new"]}], nil}, state)
          end

          assert Ferricstore.Store.PromotedPublication.read(state.instance_ctx, 0, fn ->
                   :ets.lookup_element(ets, field, 2)
                 end) == :fallback
        after
          if previous,
            do: Application.put_env(:ferricstore, :cross_shard_transaction_hook, previous),
            else: Application.delete_env(:ferricstore, :cross_shard_transaction_hook)
        end
      end

      defp promoted_publication_test_state(state) do
        state = promoted_cleanup_test_state(state)

        ctx = %{
          state.instance_ctx
          | publication_epoch: :atomics.new(1, signed: false),
            keydir_refs: {state.ets}
        }

        %{state | instance_ctx: ctx}
      end

      @tag :promoted_single_mutation_durability
      test "single HSET rejects an invalid cold field before durable publication", %{
        state: state,
        ets: ets
      } do
        key = "promoted-single-invalid-cold-field"
        field = CompoundKey.hash_field(key, "field")
        state = promoted_publication_test_state(state)
        {state, path} = promoted_single_fixture(state, ets, 0, key, :hash, [{field, "old", 0}])
        [row] = :ets.lookup(ets, field)
        invalid = row |> put_elem(1, nil) |> put_elem(5, :invalid_offset)
        :ets.insert(ets, invalid)
        before_size = File.stat!(path).size

        assert {_state, {:error, _reason}} =
                 StateMachine.apply(%{}, {:hset_single, key, "field", "new"}, state)

        assert :ets.lookup(ets, field) == [invalid]
        assert File.stat!(path).size == before_size
      end

      @tag :promoted_single_mutation_durability
      test "consecutive promoted HSETs share one validated durable append and ordered counts", %{
        state: state,
        ets: ets
      } do
        key = "promoted-hset-group-commit"
        field = CompoundKey.hash_field(key, "existing")
        state = promoted_publication_test_state(state)
        {state, _path} = promoted_single_fixture(state, ets, 0, key, :hash, [{field, "old", 0}])
        counter = make_ref()
        Process.put(counter, [])

        Process.put(:ferricstore_promoted_append_hook, fn operation, _path, _payload ->
          Process.put(counter, [operation | Process.get(counter)])
          :passthrough
        end)

        try do
          assert {_state, {:ok, [0, 1, 0]}} =
                   StateMachine.apply_waraft_segment_command(
                     {:batch,
                      [
                        {:hset_single, key, "existing", "new"},
                        {:hset_single, key, "missing", "first"},
                        {:hset_single, key, "missing", "last"}
                      ]},
                     %{},
                     state,
                     fn _batch -> flunk("promoted group unexpectedly used shared projection") end
                   )

          assert Process.get(counter) == [:batch]
          assert :ets.lookup_element(ets, field, 2) == "new"
          assert :ets.lookup_element(ets, CompoundKey.hash_field(key, "missing"), 2) == "last"
        after
          Process.delete(counter)
          Process.delete(:ferricstore_promoted_append_hook)
        end
      end

      @tag :promoted_single_mutation_durability
      test "promoted HSET groups reject malformed append results before publishing", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-invalid-append"
        state = promoted_publication_test_state(state)
        fields = Enum.map(["a", "b"], &CompoundKey.hash_field(key, &1))

        {state, path} =
          promoted_single_fixture(state, ets, 0, key, :hash, Enum.map(fields, &{&1, "old", 0}))

        before = Enum.map(fields, &:ets.lookup(ets, &1))
        size = File.stat!(path).size

        Process.put(:ferricstore_promoted_append_hook, fn :batch, _, _ ->
          {:ok, [{-1, 3}, {0, 3}]}
        end)

        try do
          assert {_state, {:error, _}} =
                   StateMachine.apply_waraft_segment_command(
                     {:batch, Enum.map(["a", "b"], &{:hset_single, key, &1, "new"})},
                     %{},
                     state,
                     fn _ -> flunk("unexpected shared projection") end
                   )

          assert Enum.map(fields, &:ets.lookup(ets, &1)) == before
          assert File.stat!(path).size == size
        after
          Process.delete(:ferricstore_promoted_append_hook)
        end
      end

      @tag :promoted_single_mutation_durability
      test "cold-read failure in a HSET run remains fail-closed on the sequential fallback", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-cold-fallback"
        state = promoted_publication_test_state(state)
        fields = Enum.map(["a", "b"], &CompoundKey.hash_field(key, &1))

        {state, path} =
          promoted_single_fixture(state, ets, 0, key, :hash, Enum.map(fields, &{&1, "old", 0}))

        [row] = :ets.lookup(ets, hd(fields))
        :ets.insert(ets, row |> put_elem(1, nil) |> put_elem(5, :invalid_offset))
        before = Enum.map(fields, &:ets.lookup(ets, &1))
        size = File.stat!(path).size

        assert {_state, {:error, _}} =
                 StateMachine.apply_waraft_segment_command(
                   {:batch, Enum.map(["a", "b"], &{:hset_single, key, &1, "new"})},
                   %{},
                   state,
                   fn _ -> flunk("unexpected shared projection") end
                 )

        assert Enum.map(fields, &:ets.lookup(ets, &1)) == before
        assert File.stat!(path).size == size
      end

      @tag :promoted_single_mutation_durability
      test "a non-HSET command remains an ordering barrier between promoted writes", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-ordering"
        state = promoted_publication_test_state(state)
        field = CompoundKey.hash_field(key, "counter")
        {state, _path} = promoted_single_fixture(state, ets, 0, key, :hash, [{field, "0", 0}])

        assert {_state, {:ok, [0, 2, 0]}} =
                 StateMachine.apply_waraft_segment_command(
                   {:batch,
                    [
                      {:hset_single, key, "counter", "1"},
                      {:hincrby, key, "counter", 1},
                      {:hset_single, key, "counter", "3"}
                    ]},
                   %{},
                   state,
                   fn _ -> flunk("unexpected shared projection") end
                 )

        assert :ets.lookup_element(ets, field, 2) == "3"
      end

      @tag :promoted_single_mutation_durability
      test "a large HSET run is split at the bounded group size with exact reply ordering", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-bounds"
        state = promoted_publication_test_state(state)
        {state, _path} = promoted_single_fixture(state, ets, 0, key, :hash, [])
        counter = make_ref()
        Process.put(counter, [])

        Process.put(:ferricstore_promoted_append_hook, fn operation, _, payload ->
          width = if operation == :batch, do: length(payload), else: 1
          Process.put(counter, [{operation, width} | Process.get(counter)])
          :passthrough
        end)

        try do
          assert {_state, {:ok, replies}} =
                   StateMachine.apply_waraft_segment_command(
                     {:batch, Enum.map(1..129, &{:hset_single, key, "field-#{&1}", "value"})},
                     %{},
                     state,
                     fn _ -> flunk("unexpected shared projection") end
                   )

          assert replies == List.duplicate(1, 129)
          assert Process.get(counter) == [{:record, 1}, {:batch, 128}]
        after
          Process.delete(counter)
          Process.delete(:ferricstore_promoted_append_hook)
        end
      end

      @tag :promoted_single_mutation_durability
      test "grouped HSET values recover from the dedicated log into a fresh keydir", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-recovery"
        state = promoted_publication_test_state(state)
        a = CompoundKey.hash_field(key, "a")
        b = CompoundKey.hash_field(key, "b")
        {state, path} = promoted_single_fixture(state, ets, 0, key, :hash, [{a, "old", 0}])

        assert {_state, {:ok, [0, 1, 0]}} =
                 StateMachine.apply_waraft_segment_command(
                   {:batch,
                    [
                      {:hset_single, key, "a", "new"},
                      {:hset_single, key, "b", "first"},
                      {:hset_single, key, "b", "last"}
                    ]},
                   %{},
                   state,
                   fn _ -> flunk("unexpected shared projection") end
                 )

        fresh = :ets.new(:group_recovered_keydir, [:ordered_set, :public])
        :ets.insert(fresh, :ets.lookup(ets, Promotion.marker_key(key)))
        recovered = Promotion.recover_promoted(state.shard_data_path, fresh, state.data_dir, 0)
        assert Map.has_key?(recovered, key)

        for {field, value} <- [{a, "new"}, {b, "last"}] do
          assert [{^field, _cached, 0, _lfu, 0, offset, _size}] = :ets.lookup(fresh, field)
          assert {:ok, ^value} = NIF.v2_pread_at(path, offset)
        end
      end

      @tag :promoted_single_mutation_durability
      test "readers wait for grouped HSET publication after its durable append", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-publication"
        state = promoted_publication_test_state(state)
        fields = Enum.map(["a", "b"], &CompoundKey.hash_field(key, &1))

        {state, path} =
          promoted_single_fixture(state, ets, 0, key, :hash, Enum.map(fields, &{&1, "old", 0}))

        parent = self()

        writer =
          Task.async(fn ->
            Process.put(:ferricstore_promoted_publication_hook, fn ->
              send(parent, {:group_before_publish, self()})

              receive do
                :continue -> :ok
              after
                5_000 -> raise "group publication timeout"
              end
            end)

            StateMachine.apply_waraft_segment_command(
              {:batch, Enum.map(["a", "b"], &{:hset_single, key, &1, "new"})},
              %{},
              state,
              fn _ -> flunk("unexpected shared projection") end
            )
          end)

        assert_receive {:group_before_publish, publisher}, 2_000
        for field <- fields, do: assert_promoted_value(path, field, "new")
        assert Enum.map(fields, &:ets.lookup_element(ets, &1, 2)) == ["old", "old"]

        reader =
          Task.async(fn ->
            Ferricstore.Store.PromotedPublication.read(state.instance_ctx, 0, fn ->
              Enum.map(fields, &:ets.lookup_element(ets, &1, 2))
            end)
          end)

        try do
          assert Task.yield(reader, 20) == nil
          send(publisher, :continue)
          assert {_state, {:ok, [0, 0]}} = Task.await(writer)
          assert Task.await(reader) == ["new", "new"]
        after
          send(publisher, :continue)
          Task.shutdown(writer, :brutal_kill)
          Task.shutdown(reader, :brutal_kill)
        end
      end

      @tag :promoted_single_mutation_durability
      test "an expired field in a HSET run uses sequential TTL-aware insertion counts", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-expiry"
        state = promoted_publication_test_state(state)
        field = CompoundKey.hash_field(key, "a")
        {state, path} = promoted_single_fixture(state, ets, 0, key, :hash, [{field, "old", 1}])

        assert {_state, {:ok, [1, 0]}} =
                 StateMachine.apply_waraft_segment_command(
                   {:batch,
                    [{:hset_single, key, "a", "first"}, {:hset_single, key, "a", "last"}]},
                   %{system_time: 1_000},
                   state,
                   fn _ -> flunk("unexpected shared projection") end
                 )

        assert [{^field, "last", 0, _, _, _, _}] = :ets.lookup(ets, field)
        assert_promoted_value(path, field, "last")
      end

      @tag :promoted_single_mutation_durability
      test "a HSET run splits at the byte bound before the command-count bound", %{
        state: state,
        ets: ets
      } do
        key = "hset-group-byte-bound"
        state = promoted_publication_test_state(state)

        state = %{
          state
          | instance_ctx: %{state.instance_ctx | blob_side_channel_threshold_bytes: 0}
        }

        {state, _path} = promoted_single_fixture(state, ets, 0, key, :hash, [])
        value = :binary.copy("x", 400_000)
        Process.put(:group_append_widths, [])

        Process.put(:ferricstore_promoted_append_hook, fn operation, _, payload ->
          width = if operation == :batch, do: length(payload), else: 1

          Process.put(:group_append_widths, [
            {operation, width} | Process.get(:group_append_widths)
          ])

          :passthrough
        end)

        try do
          assert {_state, {:ok, [1, 1, 1]}} =
                   StateMachine.apply_waraft_segment_command(
                     {:batch, Enum.map(["a", "b", "c"], &{:hset_single, key, &1, value})},
                     %{},
                     state,
                     fn _ -> flunk("unexpected shared projection") end
                   )

          assert Process.get(:group_append_widths) == [{:record, 1}, {:batch, 2}]
        after
          Process.delete(:group_append_widths)
          Process.delete(:ferricstore_promoted_append_hook)
        end
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
