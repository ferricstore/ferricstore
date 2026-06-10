defmodule Ferricstore.FlowWriteContractTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

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
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_attrs_named_value_refs_empty?(attrs)"
    assert source =~ "defp flow_empty_named_ref_input?(nil), do: true"
  end

  test "flow named value refs do not scan whole flow records on the no-value hot path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_named_value_refs_empty_fast_path"

    refute source =~ "Map.get(:value_refs, record_or_refs)",
           "records without :value_refs must not be treated as a value-ref map; that scans every record field"
  end

  test "flow no-value transition fast path avoids value_ref map lookup for normal records" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert [_, fast_path_source] =
             String.split(source, "  defp flow_named_value_refs_empty_fast_path", parts: 2)

    assert [fast_path_source, _] =
             String.split(fast_path_source, "  defp flow_put_record_value_refs", parts: 2)

    refute fast_path_source =~ "Map.get",
           "normal Flow transitions do not carry named values; the no-value path must avoid a per-record Map.get/3"
  end

  test "router flow many batches use fixed shard buckets" do
    source = Ferricstore.Test.SourceFiles.router_source()

    assert source =~ "flow_fixed_shard_buckets(ctx.shard_count)"
    assert source =~ "put_elem(buckets, shard_idx"
  end

  test "router flow pipeline results use ordered tuples instead of index maps" do
    source = Ferricstore.Test.SourceFiles.router_source()

    assert source =~ "flow_result_tuple(count)"
    assert source =~ "put_elem(results, index, result)"
  end

  test "flow create fast apply inserts due lifecycle indexes once" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    refute source =~ "flow_create_put_fast_due(state, plans)",
           "fast create already includes due/due-any rows in flow_create_put_fast_indexes/2; a separate due pass duplicates lifecycle index work"
  end

  test "flow claim partition-key aggregation is not quadratic" do
    source = Ferricstore.Test.SourceFiles.router_source()

    refute source =~ "acc ++ records",
           "multi-shard claim_due must append records with reverse accumulation, not repeated list concatenation"
  end

  test "flow pipeline write result assembly is tuple based" do
    source = Ferricstore.Test.SourceFiles.flow_source()

    assert source =~ "def ordered_results([], _ctx, _callbacks, results_rev)"
    assert source =~ "Enum.reverse(results_rev)"

    refute source =~ "pipeline_write_indexed_results(ctx, %{})",
           "pipeline writes are ordered; result assembly should not hash every command index through a map"
  end

  test "flow pipeline transitions avoid oversized transition batch apply" do
    source = Ferricstore.Test.SourceFiles.flow_source()
    router_source = Ferricstore.Test.SourceFiles.router_source()
    state_machine_source = Ferricstore.Test.SourceFiles.state_machine_source()

    [function_source] =
      Regex.run(
        ~r/defp state_run_results\(ctx, keyed_commands, callbacks\).*?^  end/ms,
        source
      )

    assert function_source =~ "transition_run_results(ctx, keyed_commands, callbacks)"
    assert source =~ "defp transition_run_results(ctx, keyed_commands, callbacks)"
    assert source =~ "Router.flow_transition_batch(attrs_list)"

    [router_function_source] =
      Regex.run(~r/def flow_transition_batch\(ctx, attrs_list\).*?^  end/ms, router_source)

    assert router_function_source =~ "flow_transition_batch_valid_results(ctx, valid)"

    refute router_source =~ "flow_transition_pipeline_batch",
           "coalescing many transitions into one apply batch serializes large blob writes and explodes p99"

    refute state_machine_source =~ "flow_transition_pipeline_batch",
           "transition batching needs a blob-safe design before reintroducing a specialized apply command"
  end

  test "flow claim_due adjacent pipeline prepends run results without list concatenation" do
    source = Ferricstore.Test.SourceFiles.flow_source()

    [function_source] =
      Regex.run(~r/defp adjacent_results\(\[{:ok, claim}.*?^  end/ms, source)

    assert function_source =~ "prepend_results(results, acc)"

    refute function_source =~ "++ acc",
           "claim_due pipeline result assembly should not copy each coalesced run with list concatenation"
  end

  test "flow claim_due state normalization avoids generic Enum.uniq hot path" do
    source = Ferricstore.Test.SourceFiles.flow_source()

    [function_source] =
      Regex.run(
        ~r/defp normalize_claim_state_values\(values\) when is_list\(values\).*?^  end/ms,
        source
      )

    refute function_source =~ "Enum.uniq",
           "large multi-state claim_due should dedupe while validating instead of allocating through Enum.uniq"

    assert source =~ "dedupe_claim_states_keep_last"
  end

  test "Flow LMDB async enqueue guard avoids Process.info on apply hot path" do
    source = File.read!("lib/ferricstore/flow/lmdb_writer.ex")

    assert [_, async_source] =
             String.split(
               source,
               "\n  def enqueue_async(instance_name, shard_index, ops, after_flush)",
               parts: 2
             )

    assert [async_source, _] = String.split(async_source, "\n  def durable?", parts: 2)

    refute async_source =~ "enqueue_guard(pid, op_count)",
           "async apply projection enqueue should use per-writer atomics, not Process.info/2"

    refute async_source =~ "Process.info",
           "Process.info/2 showed up in the Flow apply hot-path profile"

    assert async_source =~ "enqueue_async_guard(instance_name, shard_index, op_count)"
  end

  test "flow soak latency metrics are sampled without extra hot event counters" do
    source = Ferricstore.Test.SourceFiles.flow_state_lmdb_soak_source()

    assert source =~ "int_env(\"FLOW_LATENCY_SAMPLE_RATE\", 10)"

    assert source =~
             "record_flow_latency(table, key, duration_us, item_count, event_count, latency_sample_rate)"

    assert source =~ "rem(event_count, latency_sample_rate) == 0"
    assert source =~ "flow_event_key?(key)"
    assert source =~ "unless flow_event_key?(key), do: update_max"
  end

  test "flow query aggregators accumulate chunks without repeated list concatenation" do
    source = Ferricstore.Test.SourceFiles.flow_source()

    refute source =~ "records ++ acc"
    refute source =~ "ids ++ acc"
    refute source =~ "flow_decode_terminal_index_entries(entries, path, now_ms) ++ acc"
    refute source =~ "flow_decode_query_index_entries(entries, path, now_ms) ++ acc"
  end

  test "flow history projection avoids grouping when pending entries stay on the apply shard" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_history_projection_same_shard?"

    assert source =~
             "publish_pending_flow_history_projection_entries(state, ctx, entries, ra_index)"
  end

  test "flow LMDB mirror enqueue stays async on the state-machine hot path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    [function_source] =
      Regex.run(~r/defp enqueue_lmdb_mirror_group\(state, shard_index.*?^  end/ms, source)

    assert function_source =~ "LMDBWriter.enqueue_async("

    refute function_source =~ "LMDBWriter.enqueue(",
           "Flow apply must not block on LMDB projection enqueue; flush/request are the sync boundary"
  end

  test "flow history projection never sync-writes from state-machine apply" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    refute source =~ "HistoryProjector.write_entries_sync",
           "Flow apply must enqueue history projection asynchronously and gate replay/release instead of sync-writing history"

    refute source =~ "with_sync_flow_history",
           "WARaft Flow apply must not force sync history projection"

    refute source =~ "raw_put_cold(state, entry.key, flow_history_entry_value",
           "Flow history writes must not have an inline cold-write branch controlled by config"
  end

  test "flow history projection entries stay lazy on the state-machine hot path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    [projection_entry_source] =
      Regex.run(~r/defp flow_history_projection_entry\(.*?^      end/ms, source)

    refute projection_entry_source =~ "Flow.encode_history_fields",
           "Flow apply should not encode history values; HistoryProjector encodes lazy descriptors async"

    assert projection_entry_source =~ "value: {:flow_history_fields, record, event, now_ms, meta}"

    refute projection_entry_source =~ "record: record",
           "hot-path projection entries should use one compact value descriptor instead of extra map fields"

    refute projection_entry_source =~ "event: event",
           "hot-path projection entries should use one compact value descriptor instead of extra map fields"

    refute projection_entry_source =~ "now_ms: now_ms",
           "hot-path projection entries should use one compact value descriptor instead of extra map fields"

    refute source =~ "Flow.encode_history_fields(record, event, now_ms, Map.get(entry, :meta, %{}))",
           "single-command history planning must also enqueue lazy descriptors"

    assert source =~ "value: {:flow_history_fields, record, event, now_ms, meta}"
  end

  test "flow history projector pressure path stores overflow instead of sync-failing apply" do
    source = File.read!("lib/ferricstore/flow/history_projector.ex")

    assert [_, async_source] =
             String.split(source, "\n  def enqueue_async(instance_ctx, shard_index, entries, ra_index)",
               parts: 2
             )

    assert [async_source, _] = String.split(async_source, "\n  @spec flush", parts: 2)

    assert async_source =~ "Pending.append_overflow(projector, entries)"

    refute async_source =~ "GenServer.cast(pid, :drain_overflow)",
           "queue-full overflow must not cast one drain message per apply batch; flush/retry drains it coalesced"

    refute async_source =~ "{:error, :queue_full}",
           "queue-full history projection must move to retry overflow, not fail the Ra apply path"
  end

  test "flow claim_due native planner owns history-ready planning without LMDB sync" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "NativeFlowIndex.plan_claims_with_history(",
           "claim_due should use the native planner for state/index/history-ready mutation plans"

    [function_source] =
      Regex.run(~r/defp flow_plan_claim_candidates_native\(.*?^  end/ms, source)

    refute function_source =~ "HistoryProjector.write_entries_sync",
           "claim_due native planning must not move LMDB/history projection into the apply hot path"

    refute function_source =~ "LMDBWriter.enqueue(",
           "claim_due native planning must leave LMDB as async cold projection"
  end

  test "flow history hot path skips after-history pass when records need no trim or terminal mirror" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_after_history_put_records_batch(state, records)"
    assert source =~ "defp flow_after_history_fast_record?"
    assert source =~ "flow_many_projection_entries_and_records("
  end

  test "flow transition due-index moves cache repeated batch keys" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_transition_move_due_indexes_nonempty",
        "from_due_cache"
      )

    assert function_source =~ "from_due_cache"
    assert function_source =~ "flow_claim_cached_due_index_key(from_due_cache, record)"

    refute function_source =~ "flow_due_state_index_plan(record, next",
           "transition batches usually share type/state/priority/partition; rebuilding due keys per item is visible in the profile"
  end

  test "flow claim_due native multi-key path does not sort due keys before the NIF scan" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_claim_due_scan_keys_native_multi_loop",
        "NativeFlowIndex.claim_due_candidates"
      )

    refute function_source =~ "Enum.sort",
           "native Flow claim_due scans keys in caller order; sorting every partition batch was visible in the profile and does not improve correctness"
  end

  test "flow apply key-size validation does not call Router on the hot path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "@flow_max_key_size 65_535"

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "flow_validate_key_size")

    assert function_source =~ "@flow_max_key_size"

    refute function_source =~ "Router.max_key_size()",
           "Flow claim/create validation runs in replicated apply; max key size is a constant and should not call Router per key"
  end

  test "flow small values skip blob externalization dispatch on apply hot path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "maybe_externalize_apply_value"
      )

    assert function_source =~ "flow_inline_blob_value?(threshold, value)"

    assert source =~ "BlobRef.encoded_size?(size)",
           "ref-sized user bytes must still go through BlobValue so they are not confused with internal refs"

    assert function_source =~ "BlobValue.maybe_externalize",
           "large values and ref-shaped values still need the blob externalization path"
  end

  test "flow many router stamps per-shard batches so apply does not rehash each item" do
    router_source = Ferricstore.Test.SourceFiles.router_source()
    state_machine_source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert router_source =~ "@flow_shard_marker :__flow_shard_index__"
    assert router_source =~ "flow_stamp_shard(command_attrs, shard_idx)"
    assert router_source =~ "flow_stamp_shard(%{records: attrs}, shard_idx)"

    assert state_machine_source =~ "@flow_shard_marker :__flow_shard_index__"
    assert state_machine_source =~ "flow_attrs_same_stamped_shard?"
    assert state_machine_source =~ "flow_key_infos_same_stamped_shard?"

    attrs_check_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        state_machine_source,
        "flow_many_same_state_machine_shard?",
        "Router.shard_for(ctx, key)"
      )

    assert attrs_check_source =~ "flow_attrs_same_stamped_shard?"
    assert attrs_check_source =~ "Router.shard_for(ctx, key)"

    key_info_check_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        state_machine_source,
        "flow_many_same_state_machine_shard_by_keys?",
        "Router.shard_for(ctx, key)"
      )

    assert key_info_check_source =~ "flow_key_infos_same_stamped_shard?"
    assert key_info_check_source =~ "Router.shard_for(ctx, key)"
  end

  test "flow many apply uses the command-level shard stamp before per-record rehash fallback" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~
             "flow_many_partitions_valid?(state, attrs_list, Map.get(attrs, @flow_shard_marker))"

    valid_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_many_partitions_valid?",
        "flow_many_same_state_machine_shard?"
      )

    assert valid_source =~ "flow_many_same_state_machine_shard?(state, attrs_list, stamped_shard)"

    same_shard_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_many_same_state_machine_shard?",
        "stamped_shard == shard_index"
      )

    assert same_shard_source =~ "stamped_shard == shard_index"
    assert same_shard_source =~ "flow_attrs_same_stamped_shard?"
    assert same_shard_source =~ "Router.shard_for(ctx, key)"
  end

  test "flow claim_due validates due-index key size without building every due key" do
    source = Ferricstore.Test.SourceFiles.flow_source()

    [function_source] =
      Regex.run(
        ~r/defp validate_claim_due_keys\(type, state, nil, partition_key\).*?^  end/ms,
        source
      )

    assert function_source =~ "validate_claim_due_key_lengths"

    refute function_source =~ "__MODULE__.Keys.due_key",
           "claim_due can probe many partitions per poll; validation must use length math instead of allocating every generated due-index key"
  end

  test "flow claim_due fast index path avoids generic per-plan tuple dispatch" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_claim_fast_index_entries",
        "flow_claim_fast_index_entries_loop(plans,"
      )

    assert function_source =~ "flow_claim_fast_index_entries_loop(plans,"

    refute function_source =~ "flow_claim_plan_pair(plan)",
           "claim_due fast index apply is on the WARaft apply hot path; it should dispatch plan tuple shapes directly instead of calling the generic plan-pair helper for every item"
  end

  test "flow terminal transition skips empty due and metadata index passes" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_transition_plans_due_index_empty?(plans)"
    assert source =~ "flow_transition_move_due_indexes_nonempty(state, plans)"
    assert source =~ "flow_transition_plans_metadata_index_empty?(plans)"
    assert source =~ "flow_transition_move_metadata_indexes_nonempty(state, plans)"
  end

  test "flow terminal transition caches repeated lifecycle index keys" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    state_index_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_transition_move_state_indexes",
        "flow_claim_cached_state_index_key"
      )

    assert state_index_source =~ "flow_claim_cached_state_index_key"

    delete_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_transition_delete_old_secondary_indexes",
        "flow_claim_cached_worker_index_key"
      )

    assert delete_source =~ "flow_claim_cached_inflight_index_key"
    assert delete_source =~ "flow_claim_cached_worker_index_key"
  end

  test "flow non-idempotent create fast path does not synchronously query LMDB for existence" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    [function_source] =
      Regex.run(
        ~r/defp flow_create_non_idempotent_many_prepare\(state, attrs_list, key_infos\).*?^  end/ms,
        source
      )

    assert function_source =~ "flow_registry_keys_present_hot_only"
    assert function_source =~ "flow_state_keys_present_hot_only"

    refute function_source =~ "flow_state_keys_present(state, keys)",
           "current Flow state is the hot authoritative row; LMDB is an async cold projection and must not be a create hot-path dependency"

    [hot_only_source] =
      Regex.run(
        ~r/defp flow_state_keys_present_hot_only\(state, keys\).*?^  end/ms,
        source
      )

    assert hot_only_source =~ "flow_state_key_present_hot?"

    refute hot_only_source =~ "flow_state_keys_present(state, keys)",
           "hot-only helper must not delegate to the lagged LMDB presence path"
  end

  test "flow registry markers use segment-keydir storage instead of apply projection" do
    storage_source = Ferricstore.Test.SourceFiles.waraft_storage_source()

    [storage_function_source] =
      Regex.run(
        ~r/defp segment_project_cold_flow_key\?\(key\).*?^  end/ms,
        storage_source
      )

    assert storage_function_source =~ "FlowKeys.value_key?(key)"
    assert storage_function_source =~ "FlowKeys.history_key?(key)"
    assert storage_function_source =~ "FlowKeys.registry_key?(key)"

    state_machine_source = Ferricstore.Test.SourceFiles.state_machine_source()

    [registry_marker_source] =
      Regex.run(
        ~r/defp flow_put_registry_marker\(state, key, record\).*?^  end/ms,
        state_machine_source
      )

    assert registry_marker_source =~ "flow_put_hot(state, key"
    refute registry_marker_source =~ "raw_put_cold"
  end

  test "flow retry many history builds entries and next records in one traversal" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_retry_projection_entries_and_records(state, plans, [], [])"
    assert source =~ "defp flow_retry_projection_entries_and_records("
  end

  test "flow multi-row history queues async projection once per apply batch" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_many_projection_entries_and_records("
    assert source =~ "flow_create_projection_entries_and_records("
    assert source =~ "queue_pending_flow_history_projections_batch(projection_entries)"

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "flow_many_put_history")

    refute function_source =~ "flow_history_put_ready_entry(",
           "transition/complete/fail/cancel batches should not Process.get/put the pending history queue once per event"

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "flow_create_put_history")

    assert [async_create_source, _sync_fallback_source] =
             Regex.split(~r/\n\s+else\n/, function_source, parts: 2)

    refute async_create_source =~ "flow_history_put_ready_entry(",
           "create_many should queue async history projections in one batch instead of one Process.get/put per event"
  end

  test "flow fast create history builds entries and records in one traversal" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "flow_create_fast_projection_entries_and_records(state, plans, [], [])"
    assert source =~ "defp flow_create_fast_projection_entries_and_records("
    assert source =~ "defp flow_create_fast_history_entries_and_records("
  end

  test "flow secondary indexes are native-only on the replicated apply hot path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    refute source =~ ~r/(?<!Native)FlowIndex\.put_/
    refute source =~ ~r/(?<!Native)FlowIndex\.move_/
    refute source =~ ~r/(?<!Native)FlowIndex\.delete_/
    refute source =~ ~r/(?<!Native)FlowIndex\.score_of\(/
    refute source =~ ~r/(?<!Native)FlowIndex\.count_all\(/
    refute source =~ ":ets.insert(state.flow_index_name"
    refute source =~ ":ets.insert(state.flow_lookup_name"
    refute source =~ ":ets.delete(state.flow_index_name"
    refute source =~ ":ets.delete(state.flow_lookup_name"
  end

  test "flow secondary indexes are not backed by ETS tables at boot or rebuild" do
    sources =
      [
        {"lib/ferricstore/store/shard*", Ferricstore.Test.SourceFiles.shard_source()},
        {"lib/ferricstore/raft/waraft_storage.ex",
         Ferricstore.Test.SourceFiles.waraft_storage_source()},
        {"lib/ferricstore/flow/lmdb_rebuilder.ex",
         File.read!("lib/ferricstore/flow/lmdb_rebuilder.ex")},
        {"lib/ferricstore/flow/lmdb_writer.ex",
         File.read!("lib/ferricstore/flow/lmdb_writer.ex")},
        {"lib/ferricstore/flow/history_projector.ex",
         File.read!("lib/ferricstore/flow/history_projector.ex")},
        {"lib/ferricstore/store/router*", Ferricstore.Test.SourceFiles.router_source()}
      ]

    Enum.each(sources, fn {path, source} ->
      refute source =~ ~r/(?<!Native)FlowIndex\./,
             "#{path} must not call the ETS Flow index; native index is the primary Flow index"

      refute source =~ "Ferricstore.Flow.OrderedIndex",
             "#{path} must not depend on the ETS Flow index module"

      refute source =~ "Ferricstore.Flow.Index",
             "#{path} must not depend on the ETS Flow index module"

      refute source =~ "merge_from_ets",
             "#{path} must not rebuild native Flow indexes from ETS"

      refute source =~ "rebuild_from_ets",
             "#{path} must not rebuild native Flow indexes from ETS"
    end)
  end

  test "native Flow index apply_batch does not flatten grouped ops on the hot path" do
    source = File.read!("lib/ferricstore/flow/native_ordered_index.ex")

    [function_source] =
      Regex.run(~r/def apply_batch\(resource, ops\).*?^  end/ms, source)

    refute function_source =~ "List.flatten",
           "Flow native index apply_batch receives grouped op batches; flattening copies the whole batch before the NIF"

    refute function_source =~ "reverse_flatten",
           "Flow native index apply_batch should parse grouped batches directly"
  end

  test "native Flow claim planning passes float candidates directly to the NIF" do
    source = File.read!("lib/ferricstore/flow/native_ordered_index.ex")

    for function_name <- ["plan_claims", "plan_claims_with_history"] do
      [function_source] =
        Regex.run(~r/def #{function_name}\(.*?^  end/ms, source)

      refute function_source =~ "parse_claim_candidates",
             "#{function_name}/... is fed by native claim_due candidates; reparsing and reversing the list copies every candidate before the planner NIF"

      assert function_source =~ "NIF.flow_record_"
    end
  end

  test "flow native claim key construction validates generated keys in one fast path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "flow_native_claim_keys")

    assert function_source =~ "flow_validate_native_claim_key_sizes("

    refute function_source =~ "flow_validate_key_size(",
           "claim_due builds six generated keys per native claim batch; repeated generic validator calls show up in the apply profile"
  end

  test "flow native claim hydration reuses precomputed state-key prefix" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "flow_plan_claim_candidates_native",
        "state_key_prefix"
      )

    assert function_source =~ "flow_read_claim_candidate_hot_values("
    assert function_source =~ "state_key_prefix"

    refute function_source =~ "flow_read_claim_candidate_values(",
           "native claim planning already parsed the due key and built state_key_prefix; parsing the same due key again during hydration is wasted apply CPU"
  end

  test "flow history projector computes initial LFU once per published batch" do
    source = File.read!("lib/ferricstore/flow/history_projector/storage.ex")

    [function_source] =
      Regex.run(~r/def publish_keydir_entries\(instance_ctx.*?^  end/ms, source)

    assert function_source =~ "initial_lfu = LFU.initial()"

    refute function_source =~ "LFU.initial(),",
           "history projection publishes many rows; LFU.initial/0 reads atomics and should not run per entry"
  end

  test "flow blob-ref writes compute initial LFU once per value" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    function_source =
      Ferricstore.Test.SourceFiles.private_function_source!(source, "raw_put_flow_blob_ref")

    assert function_source =~ "lfu = LFU.initial()"

    assert length(Regex.scan(~r/LFU\.initial\(\)/, function_source)) == 1,
           "large Flow payloads store blob refs; the apply path must not reread LFU atomics per ETS/write operation"
  end

  test "flow state-record blob staging reuses LFU for the ETS row and cold write" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    [function_source] =
      Regex.run(~r/defp flow_stage_state_record_batch_entry\(.*?^  end/ms, source)

    refute function_source =~
             "{nil, LFU.initial(), {:put_cold, key, disk_val, expire_at_ms, LFU.initial()}}",
           "large Flow transition records should compute LFU once and reuse it for both the keydir row and pending cold write"
  end

  test "flow fast create state records use known-new pending originals without ETS lookups" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

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
