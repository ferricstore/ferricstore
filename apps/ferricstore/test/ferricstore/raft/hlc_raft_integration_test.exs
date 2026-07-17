defmodule Ferricstore.Raft.HlcRaftIntegrationTest do
  @moduledoc """
  Tests for HLC integration with Raft heartbeats (spec 2G.6).

  Verifies that:

    * The state machine correctly unwraps HLC-stamped commands and merges
      the piggybacked timestamp into the local HLC.
    * Unwrapped (legacy) commands continue to work without HLC merging.
    * The `state_enter(:leader, ...)` callback advances the HLC.
    * The Batcher stamps commands with HLC timestamps before submitting
      to ra.
    * Drift stays near zero in single-node mode (leader == follower).
  """

  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.HLC
  alias Ferricstore.Raft.StateMachine

  # ---------------------------------------------------------------------------
  # Setup: create a temporary Bitcask store, ETS tables, and reset HLC
  # ---------------------------------------------------------------------------

  setup do
    dir = Path.join(System.tmp_dir!(), "hlc_raft_test_#{:rand.uniform(9_999_999)}")
    shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
    File.mkdir_p!(shard_path)

    active_file_path = Path.join(shard_path, "00000.log")
    File.touch!(active_file_path)

    suffix = :rand.uniform(9_999_999)
    keydir_name = :"hlc_raft_keydir_#{suffix}"
    :ets.new(keydir_name, [:set, :public, :named_table])

    state =
      StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        data_dir: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir_name
      })

    # Reset the HLC atomics to a clean slate so tests don't interfere.
    ref = :persistent_term.get(:ferricstore_hlc_ref)
    :atomics.put(ref, 1, 0)

    on_exit(fn ->
      try do
        :ets.delete(keydir_name)
      rescue
        ArgumentError -> :ok
      end

      File.rm_rf(dir)
    end)

    %{
      state: state,
      ets: keydir_name,
      store: nil,
      dir: dir
    }
  end

  # ---------------------------------------------------------------------------
  # HLC-wrapped command processing
  # ---------------------------------------------------------------------------

  describe "apply/3 with HLC-wrapped commands" do
    test "unwraps and processes a put command with hlc_ts metadata", %{
      state: state,
      ets: ets
    } do
      hlc_ts = HLC.now()
      wrapped = {{:put, "hlc_key", "hlc_val", 0}, stamp_metadata(hlc_ts)}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == :ok
      assert new_state.applied_count == 1

      # Verify the inner command was processed correctly (v2 7-tuple)
      assert [{"hlc_key", "hlc_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "hlc_key")
    end

    test "unwraps and processes a delete command with hlc_ts metadata", %{
      state: state,
      ets: ets
    } do
      # First, put a key
      {state2, :ok} = StateMachine.apply(%{}, {:put, "del_hlc", "v", 0}, state)

      # Now delete it with an HLC-wrapped command
      hlc_ts = HLC.now()
      wrapped = {{:delete, "del_hlc"}, stamp_metadata(hlc_ts)}
      {state3, result} = StateMachine.apply(%{}, wrapped, state2)

      assert result == :ok
      assert state3.applied_count == 2
      assert [] == :ets.lookup(ets, "del_hlc")
    end

    test "unwraps and processes a batch command with hlc_ts metadata", %{
      state: state,
      ets: ets
    } do
      hlc_ts = HLC.now()
      batch = [{:put, "b1", "v1", 0}, {:put, "b2", "v2", 0}]
      wrapped = {{:batch, batch}, stamp_metadata(hlc_ts)}

      {new_state, {:ok, results}} = StateMachine.apply(%{}, wrapped, state)

      assert results == [:ok, :ok]
      assert new_state.applied_count == 2
      assert [{"b1", "v1", 0, _, _, _, _}] = :ets.lookup(ets, "b1")
      assert [{"b2", "v2", 0, _, _, _, _}] = :ets.lookup(ets, "b2")
    end

    test "transaction writes stay unpublished until their append succeeds", %{
      state: state,
      ets: ets
    } do
      first = "unpublished_tx_first"
      second = "unpublished_tx_second"
      first_entry = {first, "old-1", 0, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      second_entry = {second, "old-2", 0, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, [first_entry, second_entry])

      execution_entries =
        Enum.map([{first, "new-1"}, {second, "new-2"}], fn {key, value} ->
          {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("SET", [key, value])
          {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
          entry
        end)

      previous_hook = Application.get_env(:ferricstore, :cross_shard_transaction_hook)
      parent = self()
      release_ref = make_ref()

      Application.put_env(:ferricstore, :cross_shard_transaction_hook, fn
        {:staged_put, _shard_index, ^first} ->
          send(
            parent,
            {:transaction_write_staged, self(), :ets.lookup(ets, first)}
          )

          receive do
            {:continue_transaction, ^release_ref} -> :ok
          after
            2_000 -> :ok
          end

        _event ->
          :ok
      end)

      on_exit(fn ->
        if previous_hook do
          Application.put_env(:ferricstore, :cross_shard_transaction_hook, previous_hook)
        else
          Application.delete_env(:ferricstore, :cross_shard_transaction_hook)
        end
      end)

      wrapped = {{:tx_execute, execution_entries, nil}, stamp_metadata(HLC.now())}
      transaction = Task.async(fn -> StateMachine.apply(%{}, wrapped, state) end)

      assert_receive {:transaction_write_staged, apply_pid, [^first_entry]}, 1_000

      try do
        assert :ets.lookup(ets, first) == [first_entry]
        assert :ets.lookup(ets, second) == [second_entry]
      after
        send(apply_pid, {:continue_transaction, release_ref})
      end

      {_new_state, result} = Task.await(transaction, 2_000)
      assert result == [:ok, :ok]
      assert [{^first, "new-1", 0, _, _, _, _}] = :ets.lookup(ets, first)
      assert [{^second, "new-2", 0, _, _, _, _}] = :ets.lookup(ets, second)
    end

    test "unwraps and processes an incr_float command with hlc_ts metadata", %{
      state: state
    } do
      hlc_ts = HLC.now()
      wrapped = {{:incr_float, "counter", 10.0}, stamp_metadata(hlc_ts)}

      {new_state, {:ok, result}} = StateMachine.apply(%{}, wrapped, state)

      assert new_state.applied_count == 1
      assert_in_delta result, 10.0, 0.001
    end

    test "rejects an apply-side expiry decision caused only by leader HLC drift", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_getdel"
      entry = {key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      wrapped =
        {{:getdel, key}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, key) == [entry]
    end

    test "rechecks drift safety when a cold location changes during apply", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_cold_retry"
      initial_entry = {key, nil, 91_000, Ferricstore.Store.LFU.initial(), 9_999, 0, 5}
      unsafe_entry = {key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, initial_entry)

      Process.put(:ferricstore_state_machine_cold_location_miss_hook, fn ->
        :ets.insert(ets, unsafe_entry)
      end)

      try do
        wrapped =
          {{:getdel, key}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

        {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

        assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
        assert :ets.lookup(ets, key) == [unsafe_entry]
      after
        Process.delete(:ferricstore_state_machine_cold_location_miss_hook)
      end
    end

    test "resolves the current row when a successful cold read races replacement", %{
      state: state,
      ets: ets
    } do
      key = "cold_success_replacement"

      {:ok, {offset, value_size}} =
        Ferricstore.Bitcask.NIF.v2_append_record(
          state.active_file_path,
          key,
          "old-value",
          0
        )

      :ets.insert(
        ets,
        {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, offset, value_size}
      )

      Process.put(:ferricstore_state_machine_cold_read_success_hook, fn _state, ^key ->
        :ets.insert(
          ets,
          {key, "new-value", 0, Ferricstore.Store.LFU.initial(), 0, 0, 9}
        )
      end)

      try do
        wrapped = {{:getdel, key}, stamp_metadata(HLC.now())}
        {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

        assert result == "new-value"
        assert :ets.lookup(ets, key) == []
      after
        Process.delete(:ferricstore_state_machine_cold_read_success_hook)
      end
    end

    test "bounds cold-read retries across continuously changing locations", %{
      state: state,
      ets: ets
    } do
      key = "cold_retry_global_budget"
      :ets.insert(ets, {key, nil, 0, Ferricstore.Store.LFU.initial(), 9_000, 0, 5})
      Process.put(:cold_retry_hook_count, 0)

      Process.put(:ferricstore_state_machine_cold_location_miss_hook, fn ->
        count = Process.get(:cold_retry_hook_count, 0) + 1
        Process.put(:cold_retry_hook_count, count)

        if count <= 12 do
          :ets.insert(
            ets,
            {key, nil, 0, Ferricstore.Store.LFU.initial(), 9_000 + count, 0, 5}
          )
        else
          :ets.insert(ets, {key, "eventual-value", 0, Ferricstore.Store.LFU.initial(), 0, 0, 14})
        end
      end)

      try do
        wrapped = {{:getdel, key}, stamp_metadata(HLC.now())}
        {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

        assert {:error,
                {:state_read_failed, {:cold_value_unavailable, {_file_id, 0, 5}, _reason}}} =
                 result

        assert Process.get(:cold_retry_hook_count) <= 8
      after
        Process.delete(:ferricstore_state_machine_cold_location_miss_hook)
        Process.delete(:cold_retry_hook_count)
      end
    end

    test "rejects transaction reads whose expiry depends on leader HLC drift", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_transaction"
      entry = {key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("PTTL", [key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

      wrapped =
        {{:tx_execute, [execution_entry], nil}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, key) == [entry]
    end

    test "rejects transaction EXISTS when leader HLC drift makes expiry unsafe", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_exists"
      entry = {key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("EXISTS", [key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

      wrapped =
        {{:tx_execute, [execution_entry], nil}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, key) == [entry]
    end

    test "rejects transaction MGET when leader HLC drift makes expiry unsafe", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_mget"
      entry = {key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("MGET", [key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

      wrapped =
        {{:tx_execute, [execution_entry], nil}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, key) == [entry]
    end

    test "rejects transaction HGETALL instead of returning a partial drifted scan", %{
      state: state,
      ets: ets
    } do
      redis_key = "drift_guarded_hgetall"
      type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      field_entry = {field_key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, {type_key, "hash", 0, Ferricstore.Store.LFU.initial(), 0, 0, 4})
      :ets.insert(ets, field_entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("HGETALL", [redis_key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

      wrapped =
        {{:tx_execute, [execution_entry], nil}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, field_key) == [field_entry]
    end

    test "rejects transaction HLEN instead of undercounting a drifted hash", %{
      state: state,
      ets: ets
    } do
      redis_key = "drift_guarded_hlen"
      type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      field_entry = {field_key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, {type_key, "hash", 0, Ferricstore.Store.LFU.initial(), 0, 0, 4})
      :ets.insert(ets, field_entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("HLEN", [redis_key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

      wrapped =
        {{:tx_execute, [execution_entry], nil}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, field_key) == [field_entry]
    end

    test "rejects promoted hash RMW when leader HLC drift makes the field ambiguous", %{
      state: state,
      ets: ets
    } do
      redis_key = "drift_guarded_promoted_hash"
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      field_entry = {field_key, "10", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 2}
      :ets.insert(ets, field_entry)

      dedicated_path =
        Ferricstore.Store.Promotion.dedicated_path(
          state.data_dir,
          state.shard_index,
          :hash,
          redis_key
        )

      File.mkdir_p!(dedicated_path)
      File.touch!(Path.join(dedicated_path, "00000.log"))

      state =
        Map.put(state, :promoted_instances, %{
          redis_key => %{path: dedicated_path, type: :hash}
        })

      wrapped =
        {{:hincrby, redis_key, "field", 1}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, field_key) == [field_entry]
    end

    test "retains an unsafe same-apply pending value and marks the read failure" do
      key = "drift_guarded_pending"
      pending = %{key => {"value", 31_000}}

      Ferricstore.CommandTime.with_expiry_context(61_000, 1_000, fn ->
        Process.put(:sm_pending_values, pending)
        Process.put(:sm_state_read_failure, nil)

        try do
          assert StateMachine.__sm_pending_value_meta_for_test__(key) == :miss
          assert Process.get(:sm_state_read_failure) == :hlc_drift_exceeded
          assert Process.get(:sm_pending_values) == pending
        after
          Process.delete(:sm_pending_values)
          Process.delete(:sm_state_read_failure)
        end
      end)
    end

    test "does not let another owner steal a wall-live fetch-or-compute lease", %{
      state: state
    } do
      key = "drift_guarded_fetch_lease"
      owner = "original-owner"
      expire_at_ms = 31_000

      state =
        state
        |> Map.put(:fetch_or_compute_locks, %{key => {owner, expire_at_ms}})
        |> Map.put(
          :fetch_or_compute_lock_expiries,
          :gb_trees.enter({expire_at_ms, key}, owner, :gb_trees.empty())
        )

      outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)

      wrapped =
        {{:fetch_or_compute_lock, key, outcome_key, "other-owner", 91_000},
         %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, :hlc_drift_exceeded}
      assert new_state.fetch_or_compute_locks == state.fetch_or_compute_locks
      assert new_state.fetch_or_compute_lock_expiries == state.fetch_or_compute_lock_expiries
    end

    test "an unrelated ambiguous lease does not block a disjoint acquisition", %{
      state: state
    } do
      existing_key = "drift_guarded_existing_lease"
      new_key = "drift_guarded_disjoint_lease"
      existing_owner = "existing-owner"
      new_owner = "new-owner"
      existing_expire_at_ms = 31_000
      new_expire_at_ms = 91_000

      state =
        state
        |> Map.put(:fetch_or_compute_locks, %{
          existing_key => {existing_owner, existing_expire_at_ms}
        })
        |> Map.put(
          :fetch_or_compute_lock_expiries,
          :gb_trees.enter(
            {existing_expire_at_ms, existing_key},
            existing_owner,
            :gb_trees.empty()
          )
        )

      outcome_key = Ferricstore.FetchOrCompute.Outcome.key(new_key)

      wrapped =
        {{:fetch_or_compute_lock, new_key, outcome_key, new_owner, new_expire_at_ms},
         %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == :ok

      assert new_state.fetch_or_compute_locks == %{
               existing_key => {existing_owner, existing_expire_at_ms},
               new_key => {new_owner, new_expire_at_ms}
             }
    end

    test "does not bypass a wall-live fetch-or-compute lease for ordinary writes", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_fetch_write"
      owner = "lease-owner"
      expire_at_ms = 31_000

      state =
        state
        |> Map.put(:fetch_or_compute_locks, %{key => {owner, expire_at_ms}})
        |> Map.put(
          :fetch_or_compute_lock_expiries,
          :gb_trees.enter({expire_at_ms, key}, owner, :gb_trees.empty())
        )

      wrapped =
        {{:put, key, "new-value", 0}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, :hlc_drift_exceeded}
      assert :ets.lookup(ets, key) == []
      assert new_state.fetch_or_compute_locks == state.fetch_or_compute_locks
      assert new_state.fetch_or_compute_lock_expiries == state.fetch_or_compute_lock_expiries
    end

    test "does not acquire a lease when the prior outcome has unsafe expiry", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_fetch_outcome"
      owner = "new-owner"
      outcome_key = Ferricstore.FetchOrCompute.Outcome.key(key)
      outcome_entry = {outcome_key, "failure", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 7}
      :ets.insert(ets, outcome_entry)

      wrapped =
        {{:fetch_or_compute_lock, key, outcome_key, owner, 91_000},
         %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert new_state.fetch_or_compute_locks == state.fetch_or_compute_locks
      assert new_state.fetch_or_compute_lock_expiries == state.fetch_or_compute_lock_expiries
      assert :ets.lookup(ets, outcome_key) == [outcome_entry]
    end

    test "rejects lifecycle expiry candidates caused only by leader HLC drift", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_expiry_sweep"
      entry = {key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      wrapped =
        {{:expire_if_batch, [{key, 31_000}]}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, key) == [entry]
    end

    test "WATCH token creation rejects drift-ambiguous expiry", %{
      state: state,
      ets: ets
    } do
      key = "drift_guarded_watch_token"
      entry = {key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      wrapped =
        {{:watch_token, key}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, :hlc_drift_exceeded}
      assert :ets.lookup(ets, key) == [entry]
    end

    test "WATCH validation returns a storage error and aborts on ambiguous expiry", %{
      state: state,
      ets: ets
    } do
      watched_key = "drift_guarded_watch_fence"
      target_key = "drift_guarded_watch_target"
      entry = {watched_key, "value", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      {:ok, prepared} =
        Ferricstore.Commands.PreparedCommand.prepare("SET", [target_key, "new-value"])

      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)

      wrapped =
        {{:tx_execute, [execution_entry], nil, %{watched_key => :missing}},
         %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, watched_key) == [entry]
      assert :ets.lookup(ets, target_key) == []
    end

    test "rejects negative expiry in compound batch entries before mutation", %{
      state: state,
      ets: ets
    } do
      redis_key = "invalid_compound_batch_expiry"
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      initial_size = File.stat!(state.active_file_path).size

      wrapped =
        {{:compound_batch_put, redis_key, [{field_key, "value", -1}]}, stamp_metadata(HLC.now())}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, :invalid_compound_batch_entry}
      assert :ets.lookup(ets, field_key) == []
      assert File.stat!(state.active_file_path).size == initial_size
    end

    test "rejects negative expiry in raw compound blob batches before mutation", %{
      state: state,
      ets: ets
    } do
      redis_key = "invalid_compound_blob_batch_expiry"
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      initial_size = File.stat!(state.active_file_path).size

      wrapped =
        {{:compound_blob_batch_put, redis_key, [{field_key, "value", -1, :value}]},
         stamp_metadata(HLC.now())}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, :invalid_compound_blob_batch_entry}
      assert :ets.lookup(ets, field_key) == []
      assert File.stat!(state.active_file_path).size == initial_size
    end

    test "ZINCRBY rejects an invalid stored score without overwriting it", %{
      state: state,
      ets: ets
    } do
      redis_key = "invalid_zset_score"
      type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
      member_key = Ferricstore.Store.CompoundKey.zset_member(redis_key, "member")
      member_entry = {member_key, "not-a-score", 0, Ferricstore.Store.LFU.initial(), 0, 0, 11}

      :ets.insert(ets, {type_key, "zset", 0, Ferricstore.Store.LFU.initial(), 0, 0, 4})
      :ets.insert(ets, member_entry)

      wrapped =
        {{:zincrby, redis_key, 1.0, "member"}, stamp_metadata(HLC.now())}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, "ERR value is not a valid float"}
      assert :ets.lookup(ets, member_key) == [member_entry]
    end

    test "ZINCRBY can replace a plain string that is definitely expired", %{
      state: state,
      ets: ets
    } do
      redis_key = "expired_string_to_zset"
      type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
      member_key = Ferricstore.Store.CompoundKey.zset_member(redis_key, "member")
      :ets.insert(ets, {redis_key, "old", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 3})

      wrapped =
        {{:zincrby, redis_key, 1.0, "member"}, %{hlc_ts: {61_000, 0}, wall_time_ms: 61_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == "1.0"
      assert :ets.lookup(ets, redis_key) == []
      assert [{^type_key, "zset", 0, _, _, _, _}] = :ets.lookup(ets, type_key)
      assert [{^member_key, "1.0", 0, _, _, _, _}] = :ets.lookup(ets, member_key)
    end

    test "ZINCRBY fails closed when a plain string expiry is ambiguous", %{
      state: state,
      ets: ets
    } do
      redis_key = "ambiguous_string_to_zset"
      type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
      member_key = Ferricstore.Store.CompoundKey.zset_member(redis_key, "member")
      entry = {redis_key, "old", 31_000, Ferricstore.Store.LFU.initial(), 0, 0, 3}
      :ets.insert(ets, entry)

      wrapped =
        {{:zincrby, redis_key, 1.0, "member"}, %{hlc_ts: {61_000, 0}, wall_time_ms: 1_000}}

      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == {:error, {:state_read_failed, :hlc_drift_exceeded}}
      assert :ets.lookup(ets, redis_key) == [entry]
      assert :ets.lookup(ets, type_key) == []
      assert :ets.lookup(ets, member_key) == []
    end

    test "direct RMW returns a storage error for malformed expiry metadata", %{
      state: state,
      ets: ets
    } do
      key = "malformed_direct_expiry"
      entry = {key, "value", :invalid, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      wrapped = {{:getdel, key}, stamp_metadata(HLC.now())}
      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result ==
               {:error, {:state_read_failed, {:invalid_keydir_entry, key, entry}}}

      assert :ets.lookup(ets, key) == [entry]
    end

    test "transaction reads return a storage error for malformed expiry metadata", %{
      state: state,
      ets: ets
    } do
      key = "malformed_transaction_expiry"
      entry = {key, "value", :invalid, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("PTTL", [key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
      wrapped = {{:tx_execute, [execution_entry], nil}, stamp_metadata(HLC.now())}
      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result ==
               {:error, {:state_read_failed, {:invalid_keydir_entry, key, entry}}}

      assert :ets.lookup(ets, key) == [entry]
    end

    test "transaction scans return a storage error for malformed expiry metadata", %{
      state: state,
      ets: ets
    } do
      redis_key = "malformed_scan_expiry"
      type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      field_entry = {field_key, "value", :invalid, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, {type_key, "hash", 0, Ferricstore.Store.LFU.initial(), 0, 0, 4})
      :ets.insert(ets, field_entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("HGETALL", [redis_key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
      wrapped = {{:tx_execute, [execution_entry], nil}, stamp_metadata(HLC.now())}
      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result ==
               {:error, {:state_read_failed, {:invalid_keydir_entry, field_key, field_entry}}}

      assert :ets.lookup(ets, field_key) == [field_entry]
    end

    test "transaction counts return a storage error for malformed expiry metadata", %{
      state: state,
      ets: ets
    } do
      redis_key = "malformed_count_expiry"
      type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
      field_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      field_entry = {field_key, "value", :invalid, Ferricstore.Store.LFU.initial(), 0, 0, 5}
      :ets.insert(ets, {type_key, "hash", 0, Ferricstore.Store.LFU.initial(), 0, 0, 4})
      :ets.insert(ets, field_entry)

      {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare("HLEN", [redis_key])
      {:ok, execution_entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
      wrapped = {{:tx_execute, [execution_entry], nil}, stamp_metadata(HLC.now())}
      {_new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result ==
               {:error, {:state_read_failed, {:invalid_keydir_entry, field_key, field_entry}}}

      assert :ets.lookup(ets, field_key) == [field_entry]
    end

    test "unwraps and processes an append command with hlc_ts metadata", %{
      state: state
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "app_key", "hello", 0}, state)

      hlc_ts = HLC.now()
      wrapped = {{:append, "app_key", " world"}, stamp_metadata(hlc_ts)}

      {_state3, {:ok, new_len}} = StateMachine.apply(%{}, wrapped, state2)

      assert new_len == 11
    end

    test "legacy unwrapped commands still work (backward compatibility)", %{
      state: state,
      ets: ets
    } do
      # Ensure unwrapped commands (no HLC metadata) continue to function
      {new_state, result} = StateMachine.apply(%{}, {:put, "legacy", "val", 0}, state)

      assert result == :ok
      assert new_state.applied_count == 1
      assert [{"legacy", "val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "legacy")
    end
  end

  # ---------------------------------------------------------------------------
  # HLC merging during apply
  # ---------------------------------------------------------------------------

  describe "HLC merging during apply" do
    test "merges a remote HLC timestamp piggybacked on a command", %{state: state} do
      # Record the HLC before the command
      {_before_phys, _before_log} = HLC.now()

      # Create a "remote" timestamp that is 50ms ahead of current wall clock.
      # In multi-node mode, this simulates a leader that is slightly ahead.
      remote_phys = System.os_time(:millisecond) + 50
      remote_ts = {remote_phys, 0}

      wrapped = {{:put, "merge_test", "val", 0}, stamp_metadata(remote_ts)}
      {_new_state, :ok} = StateMachine.apply(%{}, wrapped, state)

      # After apply, the local HLC should have merged the remote timestamp.
      # The physical component should be at least as large as the remote.
      {after_phys, _after_log} = HLC.now()
      assert after_phys >= remote_phys
    end

    test "HLC merge with a past timestamp does not regress the clock", %{state: state} do
      # Advance the HLC to current time
      {current_phys, _} = HLC.now()

      # Send a command with a timestamp from the past
      remote_ts = {current_phys - 1000, 0}
      wrapped = {{:put, "past_test", "val", 0}, stamp_metadata(remote_ts)}
      {_new_state, :ok} = StateMachine.apply(%{}, wrapped, state)

      # HLC should not have regressed
      {after_phys, _} = HLC.now()
      assert after_phys >= current_phys
    end

    test "drift stays near zero in single-node mode after multiple applies", %{state: state} do
      # In single-node mode, the HLC timestamp on the command comes from the
      # same node that applies it. Drift should remain near zero.
      state_acc =
        Enum.reduce(1..100, state, fn i, acc ->
          hlc_ts = HLC.now()
          wrapped = {{:put, "drift_key_#{i}", "v#{i}", 0}, stamp_metadata(hlc_ts)}
          {new_state, :ok} = StateMachine.apply(%{}, wrapped, acc)
          new_state
        end)

      assert state_acc.applied_count == 100

      # Drift should be very small since we are single-node.
      # Allow up to 50ms to avoid CI flakes under load.
      drift = HLC.drift_ms()
      assert drift < 50, "Expected drift < 50ms in single-node mode, got #{drift}ms"
    end

    test "HLC update is called even when the command result is an error", %{state: state} do
      # Write a non-float value
      {state2, :ok} = StateMachine.apply(%{}, {:put, "not_a_float", "abc", 0}, state)

      # Try to incr_float it with an HLC-wrapped command -- should fail but still merge HLC
      remote_phys = System.os_time(:millisecond) + 25
      remote_ts = {remote_phys, 0}
      wrapped = {{:incr_float, "not_a_float", 1.0}, stamp_metadata(remote_ts)}

      {_state3, {:error, _reason}} = StateMachine.apply(%{}, wrapped, state2)

      # The HLC should still have been merged despite the command error
      {after_phys, _} = HLC.now()
      assert after_phys >= remote_phys
    end
  end

  # ---------------------------------------------------------------------------
  # Release cursor with HLC-wrapped commands
  # ---------------------------------------------------------------------------

  describe "release_cursor with HLC-wrapped commands" do
    test "release_cursor is emitted at interval boundary for wrapped commands", %{
      ets: ets,
      dir: dir
    } do
      interval = 3
      shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
      instance_name = :"hlc_release_cursor_#{System.unique_integer([:positive])}"
      instance_ctx = FerricStore.Instance.build(instance_name, shard_count: 1, data_dir: dir)

      :atomics.put(instance_ctx.replay_safe_index, 1, 3)
      :atomics.put(instance_ctx.flow_lmdb_replay_safe_index, 1, 3)
      :atomics.put(instance_ctx.flow_history_projected_index, 1, 3)

      on_exit({:hlc_release_cursor_instance, instance_name}, fn ->
        FerricStore.Instance.cleanup(instance_name)
      end)

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: shard_path,
          data_dir: dir,
          active_file_id: 0,
          active_file_path: Path.join(shard_path, "00000.log"),
          ets: ets,
          release_cursor_interval: interval,
          instance_ctx: instance_ctx
        })

      # Apply 2 wrapped commands (below interval)
      state_before =
        Enum.reduce(1..2, state, fn i, acc ->
          hlc_ts = HLC.now()
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}
          wrapped = {{:getdel, "rc_hlc_#{i}"}, stamp_metadata(hlc_ts)}
          {new_state, {:applied_at, _, nil}, _effects} = StateMachine.apply(meta, wrapped, acc)
          new_state
        end)

      assert state_before.applied_count == 2

      # The 3rd apply should emit release_cursor
      hlc_ts = HLC.now()
      meta = %{index: 3, term: 1, system_time: System.os_time(:millisecond)}
      wrapped = {{:getdel, "rc_hlc_3"}, stamp_metadata(hlc_ts)}

      {new_state, {:applied_at, _, nil}, effects} =
        StateMachine.apply(meta, wrapped, state_before)

      assert new_state.applied_count == 3
      cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
      assert {:release_cursor, 3} = cursor_effect
    end
  end

  # ---------------------------------------------------------------------------
  # state_enter/2 -- leader HLC advancement
  # ---------------------------------------------------------------------------

  describe "state_enter(:leader, ...) HLC advancement" do
    test "state_enter(:leader) advances the HLC clock", %{state: state} do
      # Reset HLC to zero
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      :atomics.put(ref, 1, 0)

      # Becoming leader should advance the HLC
      effects = StateMachine.state_enter(:leader, state)

      assert effects == []

      # After state_enter(:leader), HLC should be at wall-clock time
      {phys, _log} = HLC.now()
      wall = System.os_time(:millisecond)

      assert phys > 0
      # Physical component should be very close to wall clock
      assert abs(wall - phys) < 10
    end

    test "state_enter(:follower) still returns empty effects", %{state: state} do
      assert StateMachine.state_enter(:follower, state) == []
    end

    test "state_enter(:candidate) still returns empty effects", %{state: state} do
      assert StateMachine.state_enter(:candidate, state) == []
    end
  end

  # ---------------------------------------------------------------------------
  # Batcher HLC stamping (structural test via state machine)
  # ---------------------------------------------------------------------------

  describe "Batcher HLC stamp format" do
    test "wrapped command format is correctly handled by apply/3", %{state: state} do
      # Simulate the exact format that Batcher.stamp_hlc/1 produces
      inner_cmd = {:put, "stamped_key", "stamped_val", 0}
      hlc_ts = {System.os_time(:millisecond), 42}
      stamped = {inner_cmd, stamp_metadata(hlc_ts)}

      {new_state, result} = StateMachine.apply(%{}, stamped, state)

      assert result == :ok
      assert new_state.applied_count == 1
    end

    test "wrapped batch command format is correctly handled", %{state: state} do
      batch = [{:put, "bk1", "bv1", 0}, {:put, "bk2", "bv2", 0}]
      hlc_ts = {System.os_time(:millisecond), 0}
      stamped = {{:batch, batch}, stamp_metadata(hlc_ts)}

      {new_state, {:ok, results}} = StateMachine.apply(%{}, stamped, state)

      assert results == [:ok, :ok]
      assert new_state.applied_count == 2
    end

    test "wrapped command with high logical counter merges correctly", %{state: state} do
      # Simulate a remote leader with a very high logical counter.
      # This tests that the HLC merge handles high logical values.
      remote_physical = System.os_time(:millisecond) + 50
      hlc_ts = {remote_physical, 65_535}
      stamped = {{:put, "high_log_key", "val", 0}, stamp_metadata(hlc_ts)}

      {_new_state, :ok} = StateMachine.apply(%{}, stamped, state)

      after_timestamp = HLC.now()
      assert HLC.compare(after_timestamp, hlc_ts) == :gt
      assert elem(after_timestamp, 0) > remote_physical
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "wrapped command with zero timestamp is handled gracefully", %{state: state} do
      hlc_ts = {0, 0}
      wrapped = {{:put, "zero_ts", "val", 0}, stamp_metadata(hlc_ts)}

      {new_state, result} = StateMachine.apply(%{}, wrapped, state)

      assert result == :ok
      assert new_state.applied_count == 1
    end

    test "multiple wrapped commands maintain monotonic HLC", %{state: state} do
      timestamps =
        Enum.reduce(1..50, {state, []}, fn i, {acc, ts_list} ->
          hlc_ts = HLC.now()
          wrapped = {{:put, "mono_#{i}", "v#{i}", 0}, stamp_metadata(hlc_ts)}
          {new_state, :ok} = StateMachine.apply(%{}, wrapped, acc)

          # Record the HLC after each apply
          after_ts = HLC.now()
          {new_state, ts_list ++ [after_ts]}
        end)
        |> elem(1)

      # All timestamps should be monotonically increasing
      timestamps
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [a, b] ->
        assert HLC.compare(b, a) == :gt,
               "Expected #{inspect(b)} > #{inspect(a)}"
      end)
    end
  end

  defp stamp_metadata({physical_ms, _logical} = hlc_ts),
    do: %{hlc_ts: hlc_ts, wall_time_ms: physical_ms}
end
