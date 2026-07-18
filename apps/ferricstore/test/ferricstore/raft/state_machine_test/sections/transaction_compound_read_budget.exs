defmodule Ferricstore.Raft.StateMachineTest.Sections.TransactionCompoundReadBudget do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.ApplyContext
      alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
      alias Ferricstore.Store.{BlobRef, CompoundKey, LFU}
      alias Ferricstore.Store.Shard.CompoundMemberIndex

      @tag :transaction_compound_scan_budget
      test "transaction compound scan rejects work above the replicated member budget atomically",
           %{
             state: state,
             ets: ets
           } do
        redis_key = "tx-compound-scan-budget"
        type_key = CompoundKey.type_key(redis_key)
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        staged_key = "tx-compound-scan-must-roll-back"
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_member_apply_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})

        Enum.each([{field_a, "one"}, {field_b, "two"}], fn {key, value} ->
          :ets.insert(ets, {key, value, 0, LFU.initial(), 0, 0, byte_size(value)})
          CompoundMemberIndex.put(index, key)
        end)

        entries =
          Enum.map(
            [
              {"SET", [staged_key, "staged"]},
              {"HGETALL", [redis_key]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_compound_read_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, limited_state)

        assert [] = :ets.lookup(ets, staged_key)
        assert [_row] = :ets.lookup(ets, type_key)
        assert [_row] = :ets.lookup(ets, field_a)
        assert [_row] = :ets.lookup(ets, field_b)
      end

      @tag :transaction_compound_count_index
      test "transaction compound count stays exact without scanning the raw shard prefix", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-count-budget"
        type_key = CompoundKey.type_key(redis_key)
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_member_apply_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})

        Enum.each([field_a, field_b], fn key ->
          :ets.insert(ets, {key, "value", 0, LFU.initial(), 0, 0, 5})
          CompoundMemberIndex.put(index, key)
        end)

        {:ok, prepared_hlen} =
          Ferricstore.Commands.PreparedCommand.prepare("HLEN", [redis_key])

        {:ok, hlen_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hlen)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, [2]} =
                 StateMachine.apply(%{}, {:tx_execute, [hlen_entry], nil}, limited_state)

        assert [_row] = :ets.lookup(ets, type_key)
        assert [_row] = :ets.lookup(ets, field_a)
        assert [_row] = :ets.lookup(ets, field_b)
      end

      @tag :transaction_compound_scan_budget
      test "transaction compound scan enforces the replicated payload byte boundary", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-scan-byte-budget"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "f")
        index = state.compound_member_index_name
        context = ApplyContext.new(transaction_result_byte_budget: 5)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, "1234", 0, LFU.initial(), 0, 0, 4})
        CompoundMemberIndex.put(index, field, 0)

        {:ok, prepared_hgetall} =
          Ferricstore.Commands.PreparedCommand.prepare("HGETALL", [redis_key])

        {:ok, hgetall_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hgetall)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, [["f", "1234"]]} =
                 StateMachine.apply(%{}, {:tx_execute, [hgetall_entry], nil}, limited_state)

        :ets.insert(ets, {field, "12345", 0, LFU.initial(), 0, 0, 5})

        assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, [hgetall_entry], nil}, limited_state)
      end

      @tag :transaction_compound_scan_budget
      test "transaction compound scans consume one cumulative replicated member budget", %{
        state: state,
        ets: ets
      } do
        first_key = "tx-compound-scan-cumulative-a"
        second_key = "tx-compound-scan-cumulative-b"
        staged_key = "tx-compound-scan-cumulative-must-roll-back"
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_member_apply_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        Enum.each([first_key, second_key], fn redis_key ->
          type_key = CompoundKey.type_key(redis_key)
          field = CompoundKey.hash_field(redis_key, "field")
          :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
          :ets.insert(ets, {field, "value", 0, LFU.initial(), 0, 0, 5})
          CompoundMemberIndex.put(index, field, 0)
        end)

        entries =
          Enum.map(
            [
              {"SET", [staged_key, "staged"]},
              {"HGETALL", [first_key]},
              {"HGETALL", [second_key]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_compound_read_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, limited_state)

        assert [] = :ets.lookup(ets, staged_key)
      end

      @tag :transaction_compound_scan_resume
      test "transaction scan cleanup makes bounded progress through expired catalog rows", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-scan-expired-progress"
        type_key = CompoundKey.type_key(redis_key)
        index = state.compound_member_index_name
        expired_at_ms = Ferricstore.HLC.now_ms() - 1
        context = ApplyContext.new(compound_member_apply_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})

        Enum.each(["a", "b"], fn field ->
          compound_key = CompoundKey.hash_field(redis_key, field)

          :ets.insert(
            ets,
            {compound_key, field, expired_at_ms, LFU.initial(), 0, 0, 1}
          )

          CompoundMemberIndex.put(index, compound_key, expired_at_ms)
        end)

        {:ok, prepared_hgetall} =
          Ferricstore.Commands.PreparedCommand.prepare("HGETALL", [redis_key])

        {:ok, hgetall_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hgetall)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_compound_read_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, [hgetall_entry], nil}, limited_state)

        assert {_state, [[]]} =
                 StateMachine.apply(%{}, {:tx_execute, [hgetall_entry], nil}, limited_state)

        assert {:ok, []} =
                 CompoundMemberIndex.keys_for_prefix(
                   index,
                   CompoundKey.hash_prefix(redis_key)
                 )
      end

      @tag :transaction_compound_catalog_unavailable
      test "transaction compound scans and counts fail closed with an unready exact catalog", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-unready-catalog"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "field")
        staged_key = "tx-compound-unready-must-roll-back"
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        :ets.delete_all_objects(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, "value", 0, LFU.initial(), 0, 0, 5})

        entries =
          Enum.map(
            [
              {"SET", [staged_key, "staged"]},
              {"HGETALL", [redis_key]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        assert {_state, {:error, {:state_read_failed, :compound_member_index_unavailable}}} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, state)

        assert [] = :ets.lookup(ets, staged_key)

        {:ok, prepared_hlen} =
          Ferricstore.Commands.PreparedCommand.prepare("HLEN", [redis_key])

        {:ok, hlen_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hlen)

        assert {_state, {:error, {:state_read_failed, :compound_member_index_unavailable}}} =
                 StateMachine.apply(%{}, {:tx_execute, [hlen_entry], nil}, state)

        assert [_row] = :ets.lookup(ets, type_key)
        assert [_row] = :ets.lookup(ets, field)
      end

      @tag :transaction_command_budget
      test "transaction command budget rejects the whole queue before mutation", %{
        state: state,
        ets: ets
      } do
        first_key = "tx-command-budget-first"
        second_key = "tx-command-budget-second"

        entries =
          Enum.map(
            [
              {"SET", [first_key, "one"]},
              {"SET", [second_key, "two"]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        context = ApplyContext.new(transaction_command_budget: 1)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_command_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, limited_state)

        assert [] = :ets.lookup(ets, first_key)
        assert [] = :ets.lookup(ets, second_key)

        assert {_state, [:ok]} =
                 StateMachine.apply(%{}, {:tx_execute, [hd(entries)], nil}, limited_state)

        assert [{^first_key, "one", _, _, _, _, _}] = :ets.lookup(ets, first_key)
      end

      @tag :transaction_key_apply_budget
      test "transaction key budget rejects oversized MSET before staging", %{
        state: state,
        ets: ets
      } do
        first_key = "tx-key-budget-mset-first"
        second_key = "tx-key-budget-mset-second"
        context = ApplyContext.new(transaction_key_apply_budget: 1)

        {:ok, oversized} =
          Ferricstore.Commands.PreparedCommand.prepare(
            "MSET",
            [first_key, "one", second_key, "two"]
          )

        {:ok, oversized_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(oversized)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_key_apply_budget_exceeded}} =
                 StateMachine.apply(
                   %{},
                   {:tx_execute, [oversized_entry], nil},
                   limited_state
                 )

        assert [] = :ets.lookup(ets, first_key)
        assert [] = :ets.lookup(ets, second_key)

        {:ok, exact} =
          Ferricstore.Commands.PreparedCommand.prepare("MSET", [first_key, "one"])

        {:ok, exact_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(exact)

        assert {_state, [:ok]} =
                 StateMachine.apply(%{}, {:tx_execute, [exact_entry], nil}, limited_state)

        assert [{^first_key, "one", _, _, _, _, _}] = :ets.lookup(ets, first_key)
      end

      @tag :transaction_key_apply_budget
      test "transaction key budget rejects oversized MGET before dispatch", %{
        state: state,
        ets: ets
      } do
        first_key = "tx-key-budget-mget-first"
        second_key = "tx-key-budget-mget-second"
        context = ApplyContext.new(transaction_key_apply_budget: 1)

        :ets.insert(ets, {first_key, "one", 0, LFU.initial(), 0, 0, 3})
        :ets.insert(ets, {second_key, "two", 0, LFU.initial(), 0, 0, 3})

        {:ok, oversized} =
          Ferricstore.Commands.PreparedCommand.prepare("MGET", [first_key, second_key])

        {:ok, oversized_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(oversized)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_key_apply_budget_exceeded}} =
                 StateMachine.apply(
                   %{},
                   {:tx_execute, [oversized_entry], nil},
                   limited_state
                 )

        {:ok, exact} =
          Ferricstore.Commands.PreparedCommand.prepare("MGET", [first_key])

        {:ok, exact_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(exact)

        assert {_state, [["one"]]} =
                 StateMachine.apply(%{}, {:tx_execute, [exact_entry], nil}, limited_state)
      end

      @tag :transaction_compound_mutation_budget
      test "transaction compound batch puts are admitted before staging", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-put-budget"
        type_key = CompoundKey.type_key(redis_key)
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_member_apply_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        {:ok, oversized} =
          Ferricstore.Commands.PreparedCommand.prepare(
            "HSET",
            [redis_key, "a", "one", "b", "two"]
          )

        {:ok, oversized_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(oversized)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_compound_read_budget_exceeded}} =
                 StateMachine.apply(
                   %{},
                   {:tx_execute, [oversized_entry], nil},
                   limited_state
                 )

        assert [] = :ets.lookup(ets, type_key)
        assert [] = :ets.lookup(ets, field_a)
        assert [] = :ets.lookup(ets, field_b)

        {:ok, exact} =
          Ferricstore.Commands.PreparedCommand.prepare("HSET", [redis_key, "a", "one"])

        {:ok, exact_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(exact)

        assert {_state, [1]} =
                 StateMachine.apply(%{}, {:tx_execute, [exact_entry], nil}, limited_state)

        assert [{^type_key, "hash", _, _, _, _, _}] = :ets.lookup(ets, type_key)
        assert [{^field_a, "one", _, _, _, _, _}] = :ets.lookup(ets, field_a)
      end

      @tag :transaction_compound_mutation_budget
      test "transaction compound batch deletes are admitted before staging", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-delete-budget"
        type_key = CompoundKey.type_key(redis_key)
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_member_apply_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})

        Enum.each([{field_a, "one"}, {field_b, "two"}], fn {compound_key, value} ->
          :ets.insert(
            ets,
            {compound_key, value, 0, LFU.initial(), 0, 0, byte_size(value)}
          )

          CompoundMemberIndex.put(index, compound_key, 0)
        end)

        {:ok, oversized} =
          Ferricstore.Commands.PreparedCommand.prepare("HDEL", [redis_key, "a", "b"])

        {:ok, oversized_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(oversized)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_compound_read_budget_exceeded}} =
                 StateMachine.apply(
                   %{},
                   {:tx_execute, [oversized_entry], nil},
                   limited_state
                 )

        assert [{^type_key, "hash", _, _, _, _, _}] = :ets.lookup(ets, type_key)
        assert [{^field_a, "one", _, _, _, _, _}] = :ets.lookup(ets, field_a)
        assert [{^field_b, "two", _, _, _, _, _}] = :ets.lookup(ets, field_b)
      end

      @tag :transaction_compound_batch_read_budget
      test "transaction compound batch reads are admitted before storage access", %{
        state: state,
        ets: ets
      } do
        context = ApplyContext.new(compound_member_apply_budget: 1)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        fixtures = [
          {
            "HMGET",
            "tx-compound-batch-read-hash",
            "hash",
            &CompoundKey.hash_field/2,
            ["a", "b"],
            ["one", "two"],
            ["one"]
          },
          {
            "SMISMEMBER",
            "tx-compound-batch-read-set",
            "set",
            &CompoundKey.set_member/2,
            ["a", "b"],
            [<<1>>, <<1>>],
            [1]
          },
          {
            "ZMSCORE",
            "tx-compound-batch-read-zset",
            "zset",
            &CompoundKey.zset_member/2,
            ["a", "b"],
            ["1", "2"],
            ["1.0"]
          }
        ]

        Enum.each(
          fixtures,
          fn {command, redis_key, type, compound_key_fun, members, values, exact_result} ->
            type_key = CompoundKey.type_key(redis_key)
            :ets.insert(ets, {type_key, type, 0, LFU.initial(), 0, 0, byte_size(type)})

            Enum.zip(members, values)
            |> Enum.each(fn {member, value} ->
              compound_key = compound_key_fun.(redis_key, member)

              :ets.insert(
                ets,
                {compound_key, value, 0, LFU.initial(), 0, 0, byte_size(value)}
              )
            end)

            {:ok, oversized} =
              Ferricstore.Commands.PreparedCommand.prepare(command, [redis_key | members])

            {:ok, oversized_entry} =
              Ferricstore.Transaction.ExecutionEntry.from_prepared(oversized)

            assert {_state, {:error, :transaction_compound_read_budget_exceeded}} =
                     StateMachine.apply(
                       %{},
                       {:tx_execute, [oversized_entry], nil},
                       limited_state
                     )

            {:ok, exact} =
              Ferricstore.Commands.PreparedCommand.prepare(command, [redis_key, hd(members)])

            {:ok, exact_entry} =
              Ferricstore.Transaction.ExecutionEntry.from_prepared(exact)

            {_state, exact_apply_result} =
              StateMachine.apply(
                %{},
                {:tx_execute, [exact_entry], nil},
                limited_state
              )

            assert [^exact_result] = exact_apply_result
          end
        )
      end

      @tag :transaction_expired_overlay
      test "an expired staged overwrite stays a tombstone for later transaction reads", %{
        state: state,
        ets: ets
      } do
        key = "tx-expired-overlay"
        now_ms = Ferricstore.HLC.now_ms()
        expired_at_ms = now_ms - 1

        :ets.insert(ets, {key, "old", 0, LFU.initial(), 0, 0, 3})

        entries =
          Enum.map(
            [
              {"SET", [key, "new", "PXAT", Integer.to_string(expired_at_ms)]},
              {"GET", [key]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        assert {_state, [:ok, nil]} =
                 StateMachine.apply(
                   %{system_time: now_ms},
                   {:tx_execute, entries, nil},
                   state
                 )
      end

      @tag :transaction_result_byte_budget
      test "transaction result bytes are cumulative across point reads and roll back staged writes",
           %{
             state: state,
             ets: ets
           } do
        first_key = "tx-result-budget-first"
        second_key = "tx-result-budget-second"
        staged_key = "tx-result-budget-must-roll-back"

        :ets.insert(ets, {first_key, "1234", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {second_key, "5678", 0, LFU.initial(), 0, 0, 4})

        entries =
          Enum.map(
            [
              {"SET", [staged_key, "staged"]},
              {"GET", [first_key]},
              {"GET", [second_key]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        exact_context = ApplyContext.new(transaction_result_byte_budget: 8)

        exact_state = %{
          state
          | apply_context: exact_context,
            apply_context_encoded: ApplyContext.encode(exact_context)
        }

        assert {_state, ["1234", "5678"]} =
                 StateMachine.apply(%{}, {:tx_execute, tl(entries), nil}, exact_state)

        context = ApplyContext.new(transaction_result_byte_budget: 9)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, limited_state)

        assert [] = :ets.lookup(ets, staged_key)
        assert [{^first_key, "1234", _, _, _, _, _}] = :ets.lookup(ets, first_key)
        assert [{^second_key, "5678", _, _, _, _, _}] = :ets.lookup(ets, second_key)
      end

      @tag :transaction_result_byte_budget
      test "transaction byte admission rejects a blob ref before opening its payload", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        redis_key = "tx-result-budget-blob"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "f")
        payload = "0123456789"
        encoded_ref = payload |> BlobRef.from_segment(0, 0) |> BlobRef.encode!()
        index = state.compound_member_index_name

        {:ok, {offset, _record_size}} =
          NIF.v2_append_record(active_file_path, field, encoded_ref, 0)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, nil, 0, LFU.initial(), 0, offset, byte_size(encoded_ref)})
        CompoundMemberIndex.put(index, field, 0)

        instance_ctx = %{
          name: :"tx_result_blob_#{System.unique_integer([:positive])}",
          data_dir: state.data_dir,
          shard_count: 1,
          keydir_refs: {ets},
          blob_side_channel_threshold_bytes: 1
        }

        context = ApplyContext.new(transaction_result_byte_budget: 5)

        limited_state = %{
          state
          | instance_ctx: instance_ctx,
            apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_hgetall} =
          Ferricstore.Commands.PreparedCommand.prepare("HGETALL", [redis_key])

        {:ok, hgetall_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hgetall)

        test_pid = self()

        Process.put(:ferricstore_blob_store_open_read_hook, fn path, _modes ->
          send(test_pid, {:blob_payload_opened, path})
          {:error, :must_not_open}
        end)

        on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

        assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, [hgetall_entry], nil}, limited_state)

        refute_received {:blob_payload_opened, _path}
      end

      @tag :transaction_result_byte_budget
      test "transaction GET reads blob metadata but rejects before opening an oversized payload",
           %{
             state: state,
             ets: ets,
             active_file_path: active_file_path
           } do
        key = "tx-result-budget-point-blob"
        payload = "0123456789"
        encoded_ref = payload |> BlobRef.from_segment(0, 0) |> BlobRef.encode!()

        {:ok, {offset, _record_size}} =
          NIF.v2_append_record(active_file_path, key, encoded_ref, 0)

        :ets.insert(ets, {key, nil, 0, LFU.initial(), 0, offset, byte_size(encoded_ref)})

        instance_ctx = %{
          name: :"tx_result_point_blob_#{System.unique_integer([:positive])}",
          data_dir: state.data_dir,
          shard_count: 1,
          keydir_refs: {ets},
          blob_side_channel_threshold_bytes: 1
        }

        context = ApplyContext.new(transaction_result_byte_budget: 5)

        limited_state = %{
          state
          | instance_ctx: instance_ctx,
            apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_get} = Ferricstore.Commands.PreparedCommand.prepare("GET", [key])
        {:ok, get_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_get)
        test_pid = self()

        Process.put(:ferricstore_blob_store_open_read_hook, fn path, _modes ->
          send(test_pid, {:point_blob_payload_opened, path})
          {:error, :must_not_open}
        end)

        on_exit(fn -> Process.delete(:ferricstore_blob_store_open_read_hook) end)

        assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, [get_entry], nil}, limited_state)

        refute_received {:point_blob_payload_opened, _path}
      end

      @tag :transaction_result_byte_budget
      test "transaction byte admission rejects oversized cold inline results before disk read", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        redis_key = "tx-result-budget-cold-inline"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "f")
        index = state.compound_member_index_name

        {:ok, {offset, value_size}} =
          NIF.v2_append_record(active_file_path, field, "12345", 0)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, nil, 0, LFU.initial(), 0, offset, value_size})
        CompoundMemberIndex.put(index, field, 0)

        context = ApplyContext.new(transaction_result_byte_budget: 5)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_hgetall} =
          Ferricstore.Commands.PreparedCommand.prepare("HGETALL", [redis_key])

        {:ok, hgetall_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hgetall)

        test_pid = self()

        Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, ^field ->
          send(test_pid, :cold_inline_read)
        end)

        on_exit(fn ->
          Process.delete(:ferricstore_state_machine_cold_read_success_hook)
        end)

        assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, [hgetall_entry], nil}, limited_state)

        refute_received :cold_inline_read
      end

      @tag :transaction_result_byte_budget
      test "transaction GET rejects a known oversized cold value before disk read", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        key = "tx-result-budget-cold-get"
        {:ok, {offset, value_size}} = NIF.v2_append_record(active_file_path, key, "12345", 0)
        :ets.insert(ets, {key, nil, 0, LFU.initial(), 0, offset, value_size})

        context = ApplyContext.new(transaction_result_byte_budget: 4)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_get} = Ferricstore.Commands.PreparedCommand.prepare("GET", [key])
        {:ok, get_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_get)
        test_pid = self()

        Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, ^key ->
          send(test_pid, {:cold_point_read, key})
        end)

        on_exit(fn ->
          Process.delete(:ferricstore_state_machine_cold_read_success_hook)
        end)

        assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, [get_entry], nil}, limited_state)

        refute_received {:cold_point_read, ^key}
      end

      @tag :transaction_result_byte_budget
      test "transaction MGET rejects cumulative known cold bytes before batch disk read", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        first_key = "tx-result-budget-cold-mget-a"
        second_key = "tx-result-budget-cold-mget-b"

        Enum.each([first_key, second_key], fn key ->
          {:ok, {offset, value_size}} = NIF.v2_append_record(active_file_path, key, "123", 0)
          :ets.insert(ets, {key, nil, 0, LFU.initial(), 0, offset, value_size})
        end)

        context = ApplyContext.new(transaction_result_byte_budget: 5)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_mget} =
          Ferricstore.Commands.PreparedCommand.prepare("MGET", [first_key, second_key])

        {:ok, mget_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_mget)

        test_pid = self()

        Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, key ->
          send(test_pid, {:cold_batch_read, key})
        end)

        on_exit(fn ->
          Process.delete(:ferricstore_state_machine_cold_read_success_hook)
        end)

        assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                 StateMachine.apply(%{}, {:tx_execute, [mget_entry], nil}, limited_state)

        refute_received {:cold_batch_read, _key}
      end

      @tag :transaction_batch_result_projection
      test "multi-field HGETDEL and HGETEX preflight cold result bytes as batches", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        redis_key = "tx-result-budget-cold-hash-batch"
        type_key = CompoundKey.type_key(redis_key)
        fields = ["a", "b"]
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})

        compound_keys =
          Enum.map(fields, fn field ->
            compound_key = CompoundKey.hash_field(redis_key, field)

            {:ok, {offset, value_size}} =
              NIF.v2_append_record(active_file_path, compound_key, "123", 0)

            :ets.insert(
              ets,
              {compound_key, nil, 0, LFU.initial(), 0, offset, value_size}
            )

            CompoundMemberIndex.put(index, compound_key, 0)
            compound_key
          end)

        original_rows =
          Map.new(compound_keys, fn compound_key ->
            {compound_key, :ets.lookup(ets, compound_key)}
          end)

        context = ApplyContext.new(transaction_result_byte_budget: 5)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        test_pid = self()

        Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, key ->
          send(test_pid, {:cold_hash_batch_read, key})
        end)

        on_exit(fn ->
          Process.delete(:ferricstore_state_machine_cold_read_success_hook)
        end)

        Enum.each(["HGETDEL", "HGETEX"], fn command ->
          {:ok, prepared} =
            Ferricstore.Commands.PreparedCommand.prepare(
              command,
              [redis_key, "FIELDS", "2" | fields]
            )

          {:ok, entry} =
            Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

          assert {_state, {:error, :transaction_result_byte_budget_exceeded}} =
                   StateMachine.apply(%{}, {:tx_execute, [entry], nil}, limited_state)

          refute_received {:cold_hash_batch_read, _key}

          Enum.each(compound_keys, fn compound_key ->
            assert Map.fetch!(original_rows, compound_key) == :ets.lookup(ets, compound_key)
          end)
        end)
      end

      @tag :transaction_internal_read_budget
      test "small APPEND result may read a larger cold source value", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        key = "tx-internal-cold-append"
        {:ok, {offset, value_size}} = NIF.v2_append_record(active_file_path, key, "12345", 0)
        :ets.insert(ets, {key, nil, 0, LFU.initial(), 0, offset, value_size})

        context = ApplyContext.new(transaction_result_byte_budget: 1)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_append} =
          Ferricstore.Commands.PreparedCommand.prepare("APPEND", [key, "x"])

        {:ok, append_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_append)

        assert {_state, [6]} =
                 StateMachine.apply(%{}, {:tx_execute, [append_entry], nil}, limited_state)

        assert [{^key, "12345x", 0, _lfu, _file_id, _offset, 6}] = :ets.lookup(ets, key)
      end

      @tag :transaction_internal_read_budget
      test "small HSET result may replace a larger cold field value", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        redis_key = "tx-internal-cold-hset"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "field")
        index = state.compound_member_index_name
        {:ok, {offset, value_size}} = NIF.v2_append_record(active_file_path, field, "12345", 0)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, nil, 0, LFU.initial(), 0, offset, value_size})
        CompoundMemberIndex.put(index, field, 0)

        context = ApplyContext.new(transaction_result_byte_budget: 1)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_hset} =
          Ferricstore.Commands.PreparedCommand.prepare("HSET", [redis_key, "field", "x"])

        {:ok, hset_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hset)

        assert {_state, [0]} =
                 StateMachine.apply(%{}, {:tx_execute, [hset_entry], nil}, limited_state)

        assert [{^field, "x", 0, _lfu, _file_id, _offset, 1}] = :ets.lookup(ets, field)
      end

      @tag :transaction_compound_projection
      test "HKEYS admits only returned field bytes and does not read cold values", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        redis_key = "tx-projection-hkeys"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "f")
        index = state.compound_member_index_name
        {:ok, {offset, value_size}} = NIF.v2_append_record(active_file_path, field, "12345", 0)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, nil, 0, LFU.initial(), 0, offset, value_size})
        CompoundMemberIndex.put(index, field, 0)

        context = ApplyContext.new(transaction_result_byte_budget: 1)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_hkeys} =
          Ferricstore.Commands.PreparedCommand.prepare("HKEYS", [redis_key])

        {:ok, hkeys_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hkeys)

        test_pid = self()

        Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, ^field ->
          send(test_pid, :hkeys_read_hidden_value)
        end)

        on_exit(fn ->
          Process.delete(:ferricstore_state_machine_cold_read_success_hook)
        end)

        assert {_state, [["f"]]} =
                 StateMachine.apply(%{}, {:tx_execute, [hkeys_entry], nil}, limited_state)

        refute_received :hkeys_read_hidden_value
      end

      @tag :transaction_compound_projection
      test "HVALS does not charge hidden field bytes", %{state: state, ets: ets} do
        redis_key = "tx-projection-hvals"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "long-field")
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, "x", 0, LFU.initial(), 0, 0, 1})
        CompoundMemberIndex.put(index, field, 0)

        context = ApplyContext.new(transaction_result_byte_budget: 1)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_hvals} =
          Ferricstore.Commands.PreparedCommand.prepare("HVALS", [redis_key])

        {:ok, hvals_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_hvals)

        assert {_state, [["x"]]} =
                 StateMachine.apply(%{}, {:tx_execute, [hvals_entry], nil}, limited_state)
      end

      @tag :transaction_compound_projection
      test "SMEMBERS admits member bytes without reading stored sentinel values", %{
        state: state,
        ets: ets,
        active_file_path: active_file_path
      } do
        redis_key = "tx-projection-smembers"
        type_key = CompoundKey.type_key(redis_key)
        member_key = CompoundKey.set_member(redis_key, "m")
        index = state.compound_member_index_name

        {:ok, {offset, value_size}} =
          NIF.v2_append_record(active_file_path, member_key, "12345", 0)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "set", 0, LFU.initial(), 0, 0, 3})
        :ets.insert(ets, {member_key, nil, 0, LFU.initial(), 0, offset, value_size})
        CompoundMemberIndex.put(index, member_key, 0)

        context = ApplyContext.new(transaction_result_byte_budget: 1)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        {:ok, prepared_smembers} =
          Ferricstore.Commands.PreparedCommand.prepare("SMEMBERS", [redis_key])

        {:ok, smembers_entry} =
          Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_smembers)

        test_pid = self()

        Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _ctx, ^member_key ->
          send(test_pid, :smembers_read_hidden_value)
        end)

        on_exit(fn ->
          Process.delete(:ferricstore_state_machine_cold_read_success_hook)
        end)

        assert {_state, [["m"]]} =
                 StateMachine.apply(%{}, {:tx_execute, [smembers_entry], nil}, limited_state)

        refute_received :smembers_read_hidden_value
      end

      @tag :transaction_compound_count_index
      test "transaction compound count overlays staged additions and deletions", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-count-overlay"
        type_key = CompoundKey.type_key(redis_key)
        existing_field = CompoundKey.hash_field(redis_key, "existing")
        staged_field = CompoundKey.hash_field(redis_key, "staged")
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {existing_field, "original", 0, LFU.initial(), 0, 0, 8})
        CompoundMemberIndex.put(index, existing_field, 0)

        entries =
          Enum.map(
            [
              {"HSET", [redis_key, "staged", "pending"]},
              {"HLEN", [redis_key]},
              {"HDEL", [redis_key, "existing"]},
              {"HLEN", [redis_key]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        assert {_state, [1, 2, 1, 1]} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, state)

        assert [_row] = :ets.lookup(ets, type_key)
        assert [] = :ets.lookup(ets, existing_field)
        assert [{^staged_field, "pending", _, _, _, _, _}] = :ets.lookup(ets, staged_field)
      end

      @tag :transaction_compound_count_index
      test "last staged hash deletion removes the type marker before later transaction reads", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-count-last-delete"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "field")
        index = state.compound_member_index_name

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, "value", 0, LFU.initial(), 0, 0, 5})
        CompoundMemberIndex.put(index, field, 0)

        entries =
          Enum.map(
            [
              {"HDEL", [redis_key, "field"]},
              {"HLEN", [redis_key]},
              {"TYPE", [redis_key]}
            ],
            fn {command, args} ->
              {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
              {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
              entry
            end
          )

        assert {_state, [1, 0, {:simple, "none"}]} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, state)

        assert [] = :ets.lookup(ets, type_key)
        assert [] = :ets.lookup(ets, field)
      end

      @tag :transaction_compound_read_budget_boundary
      test "transaction compound scan and count admit the exact replicated member budget", %{
        state: state,
        ets: ets
      } do
        redis_key = "tx-compound-read-budget-boundary"
        type_key = CompoundKey.type_key(redis_key)
        field = CompoundKey.hash_field(redis_key, "field")
        index = state.compound_member_index_name
        context = ApplyContext.new(compound_member_apply_budget: 1)

        CompoundMemberIndex.ensure_table!(index)
        CompoundMemberIndex.reset(index)
        on_exit(fn -> safe_delete_ets(index) end)

        :ets.insert(ets, {type_key, "hash", 0, LFU.initial(), 0, 0, 4})
        :ets.insert(ets, {field, "value", 0, LFU.initial(), 0, 0, 5})
        CompoundMemberIndex.put(index, field)

        entries =
          Enum.map([{"HGETALL", [redis_key]}, {"HLEN", [redis_key]}], fn {command, args} ->
            {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
            {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
            entry
          end)

        limited_state = %{
          state
          | apply_context: context,
            apply_context_encoded: ApplyContext.encode(context)
        }

        assert {_state, [["field", "value"], 1]} =
                 StateMachine.apply(%{}, {:tx_execute, entries, nil}, limited_state)
      end
    end
  end
end
