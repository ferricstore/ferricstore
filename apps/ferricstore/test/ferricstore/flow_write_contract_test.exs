defmodule Ferricstore.FlowWriteContractTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "flow create has a no-values fast path for named value refs" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    assert source =~ "flow_attrs_named_value_refs_empty?(attrs)"
    assert source =~ "defp flow_empty_named_ref_input?(nil), do: true"
  end

  test "router flow many batches use fixed shard buckets" do
    source = File.read!("lib/ferricstore/store/router.ex")

    assert source =~ "flow_fixed_shard_buckets(ctx.shard_count)"
    assert source =~ "put_elem(buckets, shard_idx"
  end

  test "router flow pipeline results use ordered tuples instead of index maps" do
    source = File.read!("lib/ferricstore/store/router.ex")

    assert source =~ "flow_result_tuple(count)"
    assert source =~ "put_elem(results, index, result)"
  end

  test "flow create fast apply inserts due lifecycle indexes once" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    refute source =~ "flow_create_put_fast_due(state, plans)",
           "fast create already includes due/due-any rows in flow_create_put_fast_indexes/2; a separate due pass duplicates lifecycle index work"
  end

  test "flow claim partition-key aggregation is not quadratic" do
    source = File.read!("lib/ferricstore/store/router.ex")

    refute source =~ "acc ++ records",
           "multi-shard claim_due must append records with reverse accumulation, not repeated list concatenation"
  end

  test "flow pipeline write result assembly is tuple based" do
    source = File.read!("lib/ferricstore/flow.ex")

    assert source =~ "pipeline_write_ordered_results(ctx, [])"

    refute source =~ "pipeline_write_indexed_results(ctx, %{})",
           "pipeline writes are ordered; result assembly should not hash every command index through a map"
  end

  test "flow claim_due adjacent pipeline prepends run results without list concatenation" do
    source = File.read!("lib/ferricstore/flow.ex")

    [function_source] =
      Regex.run(~r/defp pipeline_claim_due_adjacent_results\(\[{:ok, claim}.*?^  end/ms, source)

    assert function_source =~ "prepend_claim_due_results"

    refute function_source =~ "++ acc",
           "claim_due pipeline result assembly should not copy each coalesced run with list concatenation"
  end

  test "flow query aggregators accumulate chunks without repeated list concatenation" do
    source = File.read!("lib/ferricstore/flow.ex")

    assert source =~ "flatten_flow_chunks"
    refute source =~ "records ++ acc"
    refute source =~ "ids ++ acc"
    refute source =~ "flow_decode_terminal_index_entries(entries, path, now_ms) ++ acc"
    refute source =~ "flow_decode_query_index_entries(entries, path, now_ms) ++ acc"
  end

  test "flow history projection avoids grouping when pending entries stay on the apply shard" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    assert source =~ "flow_history_projection_same_shard?"

    assert source =~
             "publish_pending_flow_history_projection_entries(state, ctx, entries, ra_index)"
  end

  test "flow history hot path skips after-history pass when records need no trim or terminal mirror" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    assert source =~ "flow_after_history_put_records_batch(state, records)"
    assert source =~ "defp flow_after_history_fast_record?"
    assert source =~ "flow_many_history_entries_and_records("
  end

  test "flow retry many history builds entries and next records in one traversal" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    assert source =~ "flow_retry_history_entries_and_records(state, plans, [], [])"
    assert source =~ "defp flow_retry_history_entries_and_records("
  end

  test "flow fast create history builds entries and records in one traversal" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    assert source =~ "flow_create_fast_history_entries_and_records(state, plans, [], [])"
    assert source =~ "defp flow_create_fast_history_entries_and_records("
  end

  test "flow put-new indexes record rollback originals as known missing without member lookups" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    assert source =~ "track_flow_index_known_missing_originals("
    assert source =~ "FlowIndex.put_new_members("
    assert source =~ "FlowIndex.put_new_entries("
  end

  test "flow fast create state records use known-new pending originals without ETS lookups" do
    source = File.read!("lib/ferricstore/raft/state_machine.ex")

    assert source =~ "flow_put_new_state_records_batch(state, key_records)"
    assert source =~ "track_keydir_binary_delta_from_missing("
  end

  test "singular write commands return success only while claim/get return flow data" do
    now_ms = System.system_time(:millisecond)
    suffix = System.unique_integer([:positive, :monotonic])
    type_for = fn name -> "contract-#{name}-#{suffix}" end

    queued_id = "contract-queued-#{suffix}"
    cancel_id = "contract-cancel-#{suffix}"
    complete_id = "contract-complete-#{suffix}"
    fail_id = "contract-fail-#{suffix}"
    retry_id = "contract-retry-#{suffix}"
    transition_id = "contract-transition-#{suffix}"
    rewind_id = "contract-rewind-#{suffix}"

    assert :ok =
             FerricStore.flow_create(queued_id,
               type: type_for.("queued"),
               payload: %{step: "queued"},
               run_at_ms: now_ms + 60_000
             )

    assert {:ok, queued} = FerricStore.flow_get(queued_id)
    assert queued.id == queued_id
    assert queued.payload_ref

    assert :ok =
             FerricStore.flow_create(cancel_id,
               type: type_for.("cancel"),
               payload: %{step: "cancel"},
               run_at_ms: now_ms
             )

    assert {:ok, [cancel_claim]} =
             FerricStore.flow_claim_due(type_for.("cancel"),
               limit: 1,
               now_ms: now_ms,
               worker: "contract"
             )

    assert cancel_claim.id == cancel_id

    assert :ok =
             FerricStore.flow_cancel(cancel_claim.id,
               lease_token: cancel_claim.lease_token,
               fencing_token: cancel_claim.fencing_token,
               reason: "contract"
             )

    assert {:ok, cancelled} = FerricStore.flow_get(cancel_id)
    assert cancelled.state == "cancelled"

    assert :ok =
             FerricStore.flow_create(complete_id,
               type: type_for.("complete"),
               payload: %{step: "complete"},
               run_at_ms: now_ms
             )

    assert {:ok, [complete_claim]} =
             FerricStore.flow_claim_due(type_for.("complete"),
               limit: 1,
               now_ms: now_ms,
               worker: "contract"
             )

    assert complete_claim.id == complete_id
    assert complete_claim.lease_token

    assert :ok =
             FerricStore.flow_complete(complete_claim.id, complete_claim.lease_token,
               fencing_token: complete_claim.fencing_token,
               result: %{ok: true}
             )

    assert {:ok, completed} = FerricStore.flow_get(complete_id)
    assert completed.state == "completed"

    assert :ok =
             FerricStore.flow_create(fail_id,
               type: type_for.("fail"),
               payload: %{step: "fail"},
               run_at_ms: now_ms
             )

    assert {:ok, [fail_claim]} =
             FerricStore.flow_claim_due(type_for.("fail"),
               limit: 1,
               now_ms: now_ms,
               worker: "contract"
             )

    assert fail_claim.id == fail_id

    assert :ok =
             FerricStore.flow_fail(fail_claim.id, fail_claim.lease_token,
               fencing_token: fail_claim.fencing_token,
               error: %{reason: "contract"}
             )

    assert {:ok, failed} = FerricStore.flow_get(fail_id)
    assert failed.state == "failed"

    assert :ok =
             FerricStore.flow_create(retry_id,
               type: type_for.("retry"),
               payload: %{step: "retry"},
               run_at_ms: now_ms
             )

    assert {:ok, [retry_claim]} =
             FerricStore.flow_claim_due(type_for.("retry"),
               limit: 1,
               now_ms: now_ms,
               worker: "contract"
             )

    assert retry_claim.id == retry_id

    assert :ok =
             FerricStore.flow_retry(retry_claim.id, retry_claim.lease_token,
               fencing_token: retry_claim.fencing_token,
               run_at_ms: now_ms + 1_000
             )

    assert {:ok, retried} = FerricStore.flow_get(retry_id)
    assert retried.state == "queued"

    assert :ok =
             FerricStore.flow_create(transition_id,
               type: type_for.("transition"),
               payload: %{step: "transition"},
               run_at_ms: now_ms
             )

    assert {:ok, [transition_claim]} =
             FerricStore.flow_claim_due(type_for.("transition"),
               limit: 1,
               now_ms: now_ms,
               worker: "contract"
             )

    assert transition_claim.id == transition_id

    assert :ok =
             FerricStore.flow_transition(transition_claim.id, "running", "waiting",
               lease_token: transition_claim.lease_token,
               fencing_token: transition_claim.fencing_token,
               run_at_ms: now_ms + 2_000
             )

    assert {:ok, transitioned} = FerricStore.flow_get(transition_id)
    assert transitioned.state == "waiting"

    assert :ok =
             FerricStore.flow_create(rewind_id,
               type: type_for.("rewind"),
               payload: %{step: "rewind"},
               run_at_ms: now_ms
             )

    assert {:ok, [{created_event_id, _fields}]} = FerricStore.flow_history(rewind_id, count: 10)

    assert {:ok, [rewind_claim]} =
             FerricStore.flow_claim_due(type_for.("rewind"),
               limit: 1,
               now_ms: now_ms,
               worker: "contract"
             )

    assert rewind_claim.id == rewind_id

    assert :ok =
             FerricStore.flow_complete(rewind_claim.id, rewind_claim.lease_token,
               fencing_token: rewind_claim.fencing_token,
               result: %{ok: true}
             )

    assert :ok = FerricStore.flow_rewind(rewind_id, to_event: created_event_id)
    assert {:ok, rewound} = FerricStore.flow_get(rewind_id)
    assert rewound.state == "queued"
  end

  test "batch write commands return success only while claim/get return flow data" do
    now_ms = System.system_time(:millisecond)
    suffix = System.unique_integer([:positive, :monotonic])
    partition = "contract-many-partition-#{suffix}"
    type_for = fn name -> "contract-many-#{name}-#{suffix}" end

    create_ids = ["many-create-a-#{suffix}", "many-create-b-#{suffix}"]

    assert :ok =
             FerricStore.flow_create_many(partition, create_ids,
               type: type_for.("create"),
               payload: %{step: "create"},
               run_at_ms: now_ms + 60_000
             )

    Enum.each(create_ids, fn id ->
      assert {:ok, created} = FerricStore.flow_get(id, partition_key: partition)
      assert created.id == id
      assert created.state == "queued"
    end)

    complete_ids = ["many-complete-a-#{suffix}", "many-complete-b-#{suffix}"]

    complete_claims =
      create_many_and_claim(partition, complete_ids, type_for.("complete"), now_ms)

    assert :ok =
             FerricStore.flow_complete_many(
               partition,
               claim_items(complete_claims),
               result: %{ok: true}
             )

    assert_all_states(complete_ids, partition, "completed")

    retry_ids = ["many-retry-a-#{suffix}", "many-retry-b-#{suffix}"]
    retry_claims = create_many_and_claim(partition, retry_ids, type_for.("retry"), now_ms)

    assert :ok =
             FerricStore.flow_retry_many(
               partition,
               claim_items(retry_claims),
               run_at_ms: now_ms + 1_000
             )

    assert_all_states(retry_ids, partition, "queued")

    fail_ids = ["many-fail-a-#{suffix}", "many-fail-b-#{suffix}"]
    fail_claims = create_many_and_claim(partition, fail_ids, type_for.("fail"), now_ms)

    assert :ok =
             FerricStore.flow_fail_many(
               partition,
               claim_items(fail_claims),
               error: %{reason: "contract"}
             )

    assert_all_states(fail_ids, partition, "failed")

    transition_ids = ["many-transition-a-#{suffix}", "many-transition-b-#{suffix}"]

    transition_claims =
      create_many_and_claim(partition, transition_ids, type_for.("transition"), now_ms)

    assert :ok =
             FerricStore.flow_transition_many(
               partition,
               "running",
               "waiting",
               claim_items(transition_claims),
               run_at_ms: now_ms + 2_000
             )

    assert_all_states(transition_ids, partition, "waiting")

    cancel_ids = ["many-cancel-a-#{suffix}", "many-cancel-b-#{suffix}"]
    cancel_claims = create_many_and_claim(partition, cancel_ids, type_for.("cancel"), now_ms)

    assert :ok =
             FerricStore.flow_cancel_many(
               partition,
               claim_items(cancel_claims),
               reason: "contract"
             )

    assert_all_states(cancel_ids, partition, "cancelled")
  end

  test "independent complete_many returns per-item success and completes valid jobs" do
    now_ms = System.system_time(:millisecond)
    suffix = System.unique_integer([:positive, :monotonic])
    partition = "contract-independent-complete-#{suffix}"
    type = "contract-independent-complete-type-#{suffix}"
    ids = ["independent-complete-a-#{suffix}", "independent-complete-b-#{suffix}"]
    claims = create_many_and_claim(partition, ids, type, now_ms)

    assert {:ok, [:ok, :ok]} =
             FerricStore.flow_complete_many(
               partition,
               claim_items(claims),
               result: %{ok: true},
               independent: true
             )

    assert_all_states(ids, partition, "completed")
  end

  defp create_many_and_claim(partition, ids, type, now_ms) do
    assert :ok =
             FerricStore.flow_create_many(partition, ids,
               type: type,
               payload: %{step: type},
               run_at_ms: now_ms
             )

    assert {:ok, claims} =
             FerricStore.flow_claim_due(type,
               limit: length(ids),
               now_ms: now_ms,
               worker: "contract-many",
               partition_key: partition
             )

    assert Enum.sort(Enum.map(claims, & &1.id)) == Enum.sort(ids)
    claims
  end

  defp claim_items(claims) do
    Enum.map(claims, fn claim ->
      %{
        id: claim.id,
        lease_token: claim.lease_token,
        fencing_token: claim.fencing_token
      }
    end)
  end

  defp assert_all_states(ids, partition, state) do
    Enum.each(ids, fn id ->
      assert {:ok, record} = FerricStore.flow_get(id, partition_key: partition)
      assert record.state == state
    end)
  end
end
