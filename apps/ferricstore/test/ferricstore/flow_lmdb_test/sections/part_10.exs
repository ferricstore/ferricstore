defmodule Ferricstore.Flow.LMDBTest.Sections.Part10 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

  test "partial retention cleanup keeps values still referenced by terminal state" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)
    old_scan_limit = Application.get_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.put_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit, 1)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_lmdb_history_cleanup_scan_limit, old_scan_limit)
    end)

    id = "retention-partial-keeps-state-value"
    partition_key = "tenant-retention-partial-keeps-state-value"
    flow_type = "retention-partial-keeps-state-value"
    payload = "payload-retention-partial-keeps-state-value"
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               payload: payload,
               history_hot_max_events: 0,
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, created} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-partial-keeps-state-value",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: now + 2
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)
    Process.sleep(5)

    assert {:ok, first} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)

    assert first.flows == 0
    assert first.history == 1
    assert first.values == 0
    assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [created.payload_ref])
  end

  test "retention cleanup keeps shared value when LMDB reference scan is unavailable" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)
    old_reference_hook = Application.get_env(:ferricstore, :flow_retention_reference_scan_hook)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

    {:ok, projector} =
      Ferricstore.Flow.HistoryProjector.start_link(
        shard_index: 0,
        shard_data_path: shard_data_path,
        instance_ctx: ctx
      )

    lmdb_path = Ferricstore.Flow.LMDB.path(shard_data_path)
    held_lmdb_path = lmdb_path <> ".held"

    on_exit(fn ->
      try do
        if Process.alive?(projector), do: GenServer.stop(projector, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end

      if File.exists?(held_lmdb_path) do
        File.rm_rf!(lmdb_path)
        File.rename(held_lmdb_path, lmdb_path)
      end

      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
      restore_env(:flow_retention_reference_scan_hook, old_reference_hook)
    end)

    now = System.system_time(:millisecond)
    partition_key = "tenant-retention-lmdb-reference-unavailable"
    owner_id = "retention-lmdb-reference-owner"
    consumer_id = "retention-lmdb-reference-consumer"
    owner_type = "retention-lmdb-reference-owner"
    consumer_type = "retention-lmdb-reference-consumer"
    doc = "shared-doc"
    parent = self()

    assert :ok =
             Ferricstore.Flow.create(ctx, owner_id,
               type: owner_type,
               partition_key: partition_key,
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, %{ref: shared_ref}} =
             Ferricstore.Flow.value_put(ctx, doc,
               partition_key: partition_key,
               owner_flow_id: owner_id,
               name: "doc",
               now_ms: now + 1
             )

    assert {:ok, owner_created} =
             Ferricstore.Flow.get(ctx, owner_id, partition_key: partition_key)

    assert owner_created.value_refs["doc"].ref == shared_ref

    assert :ok =
             Ferricstore.Flow.create(ctx, consumer_id,
               type: consumer_type,
               partition_key: partition_key,
               value_refs: %{"doc" => shared_ref},
               retention_ttl_ms: 60_000,
               run_at_ms: now,
               now_ms: now + 2
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, owner_type,
               worker: "worker-retention-lmdb-reference-owner",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 3
             )

    assert claimed.id == owner_id

    assert :ok =
             Ferricstore.Flow.complete(ctx, owner_id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: now + 4
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    delete_keydir_entries_containing!(ctx, 0, consumer_id)

    Application.put_env(:ferricstore, :flow_retention_reference_scan_hook, fn record, refs ->
      if Map.get(record, :id) == owner_id and shared_ref in refs and File.exists?(lmdb_path) do
        send(parent, :retention_reference_scan_started)
        File.rm_rf!(held_lmdb_path)
        File.rename!(lmdb_path, held_lmdb_path)
        File.mkdir_p!(lmdb_path)
      end

      :ok
    end)

    totals =
      Enum.reduce_while(1..10, %{flows: 0, history: 0, values: 0}, fn _, acc ->
        {:ok, cleaned} = Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

        next = %{
          flows: acc.flows + cleaned.flows,
          history: acc.history + cleaned.history,
          values: acc.values + cleaned.values
        }

        if next.flows == 1, do: {:halt, next}, else: {:cont, next}
      end)

    assert totals.flows == 1
    assert_received :retention_reference_scan_started
    assert totals.values == 0
    assert {:ok, [^doc]} = Ferricstore.Flow.value_mget(ctx, [shared_ref])
  end

  test "retention cleanup preserves owned values when another shard has unprojected Flow rows" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 2, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    now = System.system_time(:millisecond)
    owner_id = "retention-unprojected-owner"
    owner_partition = "tenant-retention-unprojected-owner"
    owner_type = "retention-unprojected-owner"
    doc = "retention-unprojected-doc"

    assert :ok =
             Ferricstore.Flow.create(ctx, owner_id,
               type: owner_type,
               partition_key: owner_partition,
               values: %{"doc" => doc},
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, owner_created} =
             Ferricstore.Flow.get(ctx, owner_id, partition_key: owner_partition)

    doc_ref = owner_created.value_refs["doc"].ref

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, owner_type,
               worker: "worker-retention-unprojected-owner",
               partition_key: owner_partition,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, owner_id, claimed.lease_token,
               partition_key: owner_partition,
               fencing_token: claimed.fencing_token,
               now_ms: now + 2
             )

    owner_shard =
      Ferricstore.Store.Router.shard_for(
        ctx,
        Ferricstore.Flow.Keys.state_key(owner_id, owner_partition)
      )

    unprojected_shard = if owner_shard == 0, do: 1, else: 0

    unprojected_lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(unprojected_shard)
      |> Ferricstore.Flow.LMDB.path()

    File.rm_rf!(unprojected_lmdb_path)
    File.mkdir_p!(unprojected_lmdb_path)
    refute Ferricstore.Flow.LMDB.env_present?(unprojected_lmdb_path)

    unprojected =
      active_lmdb_record("retention-unprojected-other", "retention-unprojected-other", "queued",
        partition_key: "tenant-retention-unprojected-other",
        updated_at_ms: now + 3,
        next_run_at_ms: now + 3
      )

    unprojected_state_key =
      Ferricstore.Flow.Keys.state_key(unprojected.id, unprojected.partition_key)

    encoded_unprojected = Ferricstore.Flow.encode_record(unprojected)

    :ets.insert(
      elem(ctx.keydir_refs, unprojected_shard),
      {unprojected_state_key, encoded_unprojected, 0, 0, :hot, 0, byte_size(encoded_unprojected)}
    )

    assert {:ok, cleaned} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)

    assert cleaned.flows == 1
    assert cleaned.values == 0
    assert {:ok, [^doc]} = Ferricstore.Flow.value_mget(ctx, [doc_ref])
  end

  test "retention cleanup caps owned value cleanup per command" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    old_history_scan_limit =
      Application.get_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit)

    old_value_scan_limit = Application.get_env(:ferricstore, :flow_lmdb_value_cleanup_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_history_cleanup_scan_limit, 100)
    Application.put_env(:ferricstore, :flow_lmdb_value_cleanup_scan_limit, 1)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_history_cleanup_scan_limit, old_history_scan_limit)
      restore_env(:flow_lmdb_value_cleanup_scan_limit, old_value_scan_limit)
    end)

    id = "retention-owned-values-capped"
    partition_key = "tenant-retention-owned-values-capped"
    flow_type = "retention-owned-values-capped"
    now = System.system_time(:millisecond)

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               history_hot_max_events: 0,
               retention_ttl_ms: 1,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-retention-owned-values-capped",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: now + 1
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: now + 2
             )

    extra_refs =
      for version <- 10..12 do
        ref = Ferricstore.Flow.Keys.value_key(id, :shared, version, partition_key)
        encoded = Ferricstore.Flow.encode_value("extra-#{version}")
        assert :ok = Ferricstore.Store.Router.put(ctx, ref, encoded, 0)
        ref
      end

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    Process.sleep(5)

    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    lmdb_path = Ferricstore.Flow.LMDB.path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0))

    assert {:ok, first} =
             Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)

    assert first.flows == 0
    assert first.values == 1
    assert {:ok, _state_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)

    assert {:ok, values_after_first} = Ferricstore.Flow.value_mget(ctx, extra_refs)
    assert Enum.count(values_after_first, &is_nil/1) == 1

    totals =
      Enum.reduce_while(1..10, %{flows: first.flows, values: first.values}, fn _, acc ->
        {:ok, cleaned} = Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: now + 10_000)
        next = %{flows: acc.flows + cleaned.flows, values: acc.values + cleaned.values}

        if next.flows == 1 do
          {:halt, next}
        else
          {:cont, next}
        end
      end)

    assert totals.flows == 1
    assert totals.values >= length(extra_refs)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert {:ok, [nil, nil, nil]} = Ferricstore.Flow.value_mget(ctx, extra_refs)
  end

  test "async history hard cap removes trimmed events from LMDB projection" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
    end)

    id = "history-async-hard-cap"
    partition_key = "tenant-history-async-hard-cap"
    flow_type = "history-async-hard-cap"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               history_hot_max_events: 1,
               history_max_events: 2,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-history-async-hard-cap",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_001
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002
             )

    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    {_flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert {:ok, 2} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    assert {:ok, history} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert Enum.map(history, fn {_event_id, fields} -> fields["event"] end) == [
             "claimed",
             "completed"
           ]
  end

  test "history cold projection survives restart while include_cold reads projected events" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_async_history = Application.get_env(:ferricstore, :flow_async_history)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_async_history, true)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_async_history, old_async_history)
    end)

    id = "history-cold-projection-restart"
    partition_key = "tenant-history-cold-projection-restart"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "history-cold-projection-restart",
               partition_key: partition_key,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "history-cold-projection-restart",
               worker: "worker-history-cold-projection-restart",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 1_000
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 2_000
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)
    restart_isolated_shard!(ctx, 0)

    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    {_flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)

    assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> Ferricstore.Flow.LMDB.path()

    assert {:ok, 3} =
             Ferricstore.Flow.LMDB.prefix_count(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key)
             )

    assert {:ok, lmdb_entries} =
             Ferricstore.Flow.LMDB.prefix_entries(
               lmdb_path,
               Ferricstore.Flow.LMDB.history_index_prefix(history_key),
               10
             )

    assert Enum.all?(lmdb_entries, fn {_key, value} ->
             match?(
               {:ok,
                {_event_id, _event_ms, _expire_at_ms, _compound_key, {:flow_history, 0}, offset,
                 value_size}}
               when is_integer(offset) and offset >= 0 and is_integer(value_size) and
                      value_size > 0,
               Ferricstore.Flow.LMDB.decode_history_index_location(value)
             )
           end)

    assert {:ok, hot_events} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               include_cold: false
             )

    assert hot_events == []

    assert {:ok, cold_events} =
             Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

    assert Enum.map(cold_events, fn {_event_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  test "history include_cold returns latest count when cold history exceeds query scan window" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
    old_scan_limit = Application.get_env(:ferricstore, :flow_lmdb_history_query_scan_limit)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
    Application.put_env(:ferricstore, :flow_lmdb_history_query_scan_limit, 10)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
      restore_env(:flow_lmdb_history_query_scan_limit, old_scan_limit)
    end)

    id = "history-cold-latest-window"
    partition_key = "tenant-history-cold-latest-window"
    flow_type = "history-cold-latest-window"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: flow_type,
               partition_key: partition_key,
               history_hot_max_events: 2,
               history_max_events: 200,
               run_at_ms: 1,
               now_ms: 1
             )

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, flow_type,
               worker: "worker-history-cold-latest-window",
               partition_key: partition_key,
               lease_ms: 30_000,
               limit: 1,
               now_ms: 2
             )

    Enum.each(3..92, fn now_ms ->
      assert {:ok, _record} =
               Ferricstore.Flow.extend_lease(ctx, id, claimed.lease_token,
                 partition_key: partition_key,
                 fencing_token: claimed.fencing_token,
                 lease_ms: 30_000,
                 now_ms: now_ms
               )
    end)

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition_key,
               fencing_token: claimed.fencing_token,
               now_ms: 93
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, cold_events} =
             Ferricstore.Flow.history(ctx, id,
               partition_key: partition_key,
               count: 10,
               include_cold: true
             )

    assert Enum.map(cold_events, fn {event_id, _fields} -> history_event_ms(event_id) end) ==
             Enum.to_list(84..93)
  end

  test "LMDB rebuild counts cold-read failures and marks mirror degraded" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_rebuild_cold_read_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    File.mkdir_p!(shard_path)
    keydir = :ets.new(:flow_lmdb_rebuild_cold_read_keydir, [:set])
    degraded = :atomics.new(1, signed: false)
    state_key = Ferricstore.Flow.Keys.state_key("cold-read-missing", "tenant-cold-read")
    test_pid = self()
    handler_id = {:flow_lmdb_rebuild_cold_read, self(), make_ref()}

    :telemetry.attach_many(
      handler_id,
      [
        [:ferricstore, :flow, :lmdb_rebuild, :cold_read_error],
        [:ferricstore, :flow, :lmdb_rebuild]
      ],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:flow_lmdb_rebuild_cold_read, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf!(data_dir)
    end)

    :ets.insert(keydir, {state_key, nil, 0, 0, 99, 0, 16})

    assert :ok =
             Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               %{flow_lmdb_mirror_degraded: degraded},
               nil,
               nil,
               nil,
               nil
             )

    assert :atomics.get(degraded, 1) == 1

    assert_receive {:flow_lmdb_rebuild_cold_read,
                    [:ferricstore, :flow, :lmdb_rebuild, :cold_read_error], %{count: 1},
                    %{reason: _reason}}

    assert_receive {:flow_lmdb_rebuild_cold_read, [:ferricstore, :flow, :lmdb_rebuild],
                    %{cold_read_errors: 1}, %{shard_index: 0}}
  end

  test "mirror startup rebuilds flow working indexes and prunes terminal state to LMDB" do
    old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

    Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

    on_exit(fn ->
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
      restore_env(:flow_lmdb_mode, old_mode)
    end)

    partition_key = "tenant-startup"
    flow_type = "startup"

    assert :ok =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-queued",
               type: flow_type,
               state: "queued",
               run_at_ms: 10,
               partition_key: partition_key,
               now_ms: 1
             })

    assert {:ok, queued} =
             Ferricstore.Flow.get(ctx, "flow-queued", partition_key: partition_key)

    assert :ok =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: "flow-completed",
               type: flow_type,
               state: "queued",
               run_at_ms: 10,
               partition_key: partition_key,
               now_ms: 2
             })

    assert {:ok, completed_start} =
             Ferricstore.Flow.get(ctx, "flow-completed", partition_key: partition_key)

    assert {:ok, [claimed]} =
             Ferricstore.Store.Router.flow_claim_due(ctx, %{
               type: flow_type,
               state: "queued",
               priority: nil,
               worker: "worker-startup",
               lease_ms: 30_000,
               limit: 1,
               now_ms: 10,
               partition_key: partition_key
             })

    assert claimed.id == completed_start.id

    assert :ok =
             Ferricstore.Store.Router.flow_complete(ctx, %{
               id: claimed.id,
               lease_token: claimed.lease_token,
               fencing_token: claimed.fencing_token,
               now_ms: 11,
               partition_key: partition_key
             })

    assert {:ok, completed} =
             Ferricstore.Flow.get(ctx, claimed.id, partition_key: partition_key)

    assert completed.state == "completed"

    completed_index_key =
      Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

    terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)

    lmdb_path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

    stale_terminal_key =
      Ferricstore.Flow.LMDB.terminal_index_key(completed_index_key, queued.id, 99)

    queued_state_key = Ferricstore.Flow.Keys.state_key(queued.id, partition_key)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, stale_terminal_key,
                Ferricstore.Flow.LMDB.encode_terminal_index_value(queued.id, 99)},
               {:put, Ferricstore.Flow.LMDB.terminal_by_state_key_key(queued_state_key),
                stale_terminal_key}
             ])

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)

    restart_isolated_shard!(ctx, 0)

    terminal_state_key = Ferricstore.Flow.Keys.state_key(completed.id, partition_key)
    assert [{^terminal_state_key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}] =
             :ets.lookup(elem(ctx.keydir_refs, 0), terminal_state_key)

    assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)

    assert {:ok, terminal_entries} =
             Ferricstore.Flow.LMDB.prefix_entries(lmdb_path, terminal_prefix, 10)

    assert ["flow-completed"] =
             Enum.map(terminal_entries, fn {_key, value} ->
               {:ok, {id, _updated_at_ms, _expire_at_ms, _state_key}} =
                 Ferricstore.Flow.LMDB.decode_terminal_index_value(value)

               id
             end)

    assert {:ok, %{id: "flow-completed", state: "completed"}} =
             Ferricstore.Flow.get(ctx, completed.id, partition_key: partition_key)
  end
    end
  end
end
