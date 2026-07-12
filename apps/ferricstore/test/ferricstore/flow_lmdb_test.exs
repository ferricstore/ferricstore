Code.require_file(
  "flow_lmdb_test/sections/warm_opens_empty_shard_env_before_first_user_read.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/history_projector_fsyncs_copied_generated_values_before_publishing_lmdb.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/startup_rebuilds_active_flow_indexes_dedicated_lmdb_active_index.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/default_startup_repairs_active_projection_clears_stale_lmdb_flush_marker.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/mirror_writer_projects_terminal_history_hot_flow_index_during_flush.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/state_machine_still_requires_lmdb_mirror_enqueue_mode_configured_off.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/mirror_flow_reads_reject_stale_lmdb_record_fall_back_bitcask_truth.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/lineage_include_cold_reverse_reads_newest_lmdb_prefix_rows.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/async_terminal_history_cold_only_after_lmdb_projection.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/partial_retention_cleanup_keeps_values_still_referenced_terminal_state.exs",
  __DIR__
)

Code.require_file(
  "flow_lmdb_test/sections/startup_keeps_terminal_history_cold_while_durable_history_remains_querya.exs",
  __DIR__
)

defmodule Ferricstore.Flow.LMDBTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  setup do
    release_all_lmdb_envs!()

    on_exit(fn ->
      release_all_lmdb_envs!()
    end)

    :ok
  end

  defp release_all_lmdb_envs! do
    case Ferricstore.Flow.LMDB.release_all(30_000) do
      :ok -> :ok
      {:ok, _released} -> :ok
    end
  end

  defp await_lmdb_busy_release(path, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_lmdb_busy_release(path, deadline)
  end

  defp do_await_lmdb_busy_release(path, deadline) do
    case Ferricstore.Bitcask.NIF.lmdb_release(path) do
      {:busy, _count} = busy ->
        busy

      {:ok, _released} ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(1)
          do_await_lmdb_busy_release(path, deadline)
        else
          flunk("LMDB prefix scan completed before exposing an in-flight environment lease")
        end

      {:error, reason} ->
        flunk("LMDB environment release probe failed: #{inspect(reason)}")
    end
  end

  defmodule FlushProbeWriter do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
    end

    @impl true
    def init(opts) do
      {:ok,
       %{
         parent: Keyword.fetch!(opts, :parent),
         shard_index: Keyword.fetch!(opts, :shard_index)
       }}
    end

    @impl true
    def handle_call(:flush, _from, state) do
      send(state.parent, {:flush_entered, state.shard_index})

      receive do
        :release_flush -> :ok
      after
        2_000 -> :ok
      end

      {:reply, :ok, state}
    end

    @impl true
    def handle_cast(:suspend_without_flush, state) do
      send(state.parent, {:suspend_without_flush, state.shard_index})
      {:noreply, state}
    end
  end

  use Ferricstore.Flow.LMDBTest.Sections.WarmOpensEmptyShardEnvBeforeFirstUserRead

  use Ferricstore.Flow.LMDBTest.Sections.HistoryProjectorFsyncsCopiedGeneratedValuesBeforePublishingLmdb

  use Ferricstore.Flow.LMDBTest.Sections.StartupRebuildsActiveFlowIndexesDedicatedLmdbActiveIndex

  use Ferricstore.Flow.LMDBTest.Sections.DefaultStartupRepairsActiveProjectionClearsStaleLmdbFlushMarker

  use Ferricstore.Flow.LMDBTest.Sections.MirrorWriterProjectsTerminalHistoryHotFlowIndexDuringFlush

  use Ferricstore.Flow.LMDBTest.Sections.StateMachineStillRequiresLmdbMirrorEnqueueModeConfiguredOff

  use Ferricstore.Flow.LMDBTest.Sections.MirrorFlowReadsRejectStaleLmdbRecordFallBackBitcaskTruth
  use Ferricstore.Flow.LMDBTest.Sections.LineageIncludeColdReverseReadsNewestLmdbPrefixRows
  use Ferricstore.Flow.LMDBTest.Sections.AsyncTerminalHistoryColdOnlyAfterLmdbProjection

  use Ferricstore.Flow.LMDBTest.Sections.PartialRetentionCleanupKeepsValuesStillReferencedTerminalState

  use Ferricstore.Flow.LMDBTest.Sections.StartupKeepsTerminalHistoryColdWhileDurableHistoryRemainsQuerya

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp start_active_lmdb_projection_fixture!(label) do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_lmdb_#{label}_#{System.unique_integer([:positive])}"
      )

    instance_name = :"flow_lmdb_#{label}_#{System.unique_integer([:positive])}"
    shard_index = 0
    source_keydir = :ets.new(:"flow_lmdb_#{label}_source", [:set, :public])

    on_exit(fn ->
      if :ets.info(source_keydir) != :undefined, do: :ets.delete(source_keydir)
      File.rm_rf!(data_dir)
    end)

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    start_supervised!(
      {Ferricstore.Flow.LMDBWriter,
       shard_index: shard_index,
       data_dir: data_dir,
       instance_name: instance_name,
       instance_ctx: %{name: instance_name, keydir_refs: {source_keydir}}}
    )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

    %{
      data_dir: data_dir,
      instance_name: instance_name,
      shard_index: shard_index,
      source_keydir: source_keydir,
      partition_key: "tenant-#{label}",
      lmdb_path: Ferricstore.Flow.LMDB.path(shard_path)
    }
  end

  defp collect_flow_lmdb_active_chunks(acc \\ []) do
    receive do
      {:flow_lmdb_active_chunk, [:ferricstore, :flow, :lmdb_startup_active_index_chunk],
       measurements, _metadata} ->
        collect_flow_lmdb_active_chunks([measurements | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end

  defp wait_until_true(fun, attempts)
  defp wait_until_true(_fun, 0), do: false

  defp wait_until_true(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(5)
      wait_until_true(fun, attempts - 1)
    end
  end

  defp active_lmdb_record(id, type, state, opts) do
    partition_key = Keyword.fetch!(opts, :partition_key)
    updated_at_ms = Keyword.fetch!(opts, :updated_at_ms)

    %{
      id: id,
      type: type,
      state: state,
      version: Keyword.get(opts, :version, 1),
      state_enter_seq: Keyword.get(opts, :state_enter_seq, 1),
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: updated_at_ms,
      next_run_at_ms: Keyword.get(opts, :next_run_at_ms),
      priority: Keyword.get(opts, :priority, 0),
      partition_key: partition_key,
      root_flow_id: id,
      lease_owner: Keyword.get(opts, :lease_owner),
      lease_token: Keyword.get(opts, :lease_token),
      lease_deadline_ms: Keyword.get(opts, :lease_deadline_ms)
    }
  end

  defp project_active_lmdb_record!(fixture, record) do
    state_key = Ferricstore.Flow.Keys.state_key(record.id, record.partition_key)
    encoded = Ferricstore.Flow.encode_record(record)

    :ets.insert(
      fixture.source_keydir,
      {state_key, encoded, 0, 0, :hot, 0, byte_size(encoded)}
    )

    assert :ok =
             Ferricstore.Flow.LMDBWriter.enqueue(fixture.instance_name, fixture.shard_index, [
               {:project_flow_state_from_source, state_key}
             ])

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(fixture.instance_name, fixture.shard_index)
    state_key
  end

  def handle_lmdb_writer_unavailable(event, measurements, metadata, test_pid) do
    send(test_pid, {:flow_lmdb_writer_unavailable, event, measurements, metadata})
  end

  def forward_flow_lmdb_rebuild_event(event, measurements, metadata, test_pid) do
    send(test_pid, {:flow_lmdb_rebuild, event, measurements, metadata})
  end

  defp history_event_ms(event_id) do
    event_id
    |> String.split("-", parts: 2)
    |> case do
      [ms, _seq] -> String.to_integer(ms)
      _ -> 0
    end
  end

  defp refute_keydir_row!(ctx, key) do
    idx = Ferricstore.Store.Router.shard_for(ctx, key)
    assert [] = :ets.lookup(elem(ctx.keydir_refs, idx), key)
  end

  defp delete_keydir_entries_containing!(ctx, shard_index, token) do
    table = elem(ctx.keydir_refs, shard_index)

    table
    |> :ets.tab2list()
    |> Enum.each(fn
      {key, _value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}
      when is_binary(key) ->
        if String.contains?(key, token), do: :ets.delete(table, key)

      _other ->
        :ok
    end)
  end

  defp flush_shard!(ctx, shard_index) do
    assert :ok = GenServer.call(elem(ctx.shard_names, shard_index), :flush, 5_000)
  end

  defp materialize_keydir_value!(ctx, shard_index, key) do
    keydir = elem(ctx.keydir_refs, shard_index)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index)

    case :ets.lookup(keydir, key) do
      [{^key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}]
      when is_binary(value) ->
        value

      [{^key, nil, _expire_at_ms, _lfu, {:flow_history, _file_id} = file_id, offset, _value_size}] ->
        assert {:ok, value} =
                 Ferricstore.Flow.HistoryProjector.read_value(shard_path, file_id, offset)

        value

      [{^key, nil, _expire_at_ms, _lfu, file_id, offset, _value_size}]
      when is_integer(file_id) ->
        path = Ferricstore.Store.Shard.ETS.file_path(shard_path, file_id)
        assert {:ok, value} = Ferricstore.Store.ColdRead.pread_at(path, offset, key, 10_000)
        value

      [{^key, nil, _expire_at_ms, _lfu, file_id, _offset, _value_size}] when is_tuple(file_id) ->
        assert {:ok, value} =
                 Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                   ctx,
                   shard_index,
                   file_id,
                   key
                 )

        value
    end
  end

  defp set_apply_projection_value!(ctx, key, value, index)
       when is_binary(value) and is_integer(index) and index > 0 do
    idx = Ferricstore.Store.Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, idx)

    assert [{^key, _old_value, expire_at_ms, lfu, file_id, offset, value_size}] =
             :ets.lookup(keydir, key)

    if file_id != :pending do
      assert readable_locator?(file_id, offset, value_size)
    end

    :ok =
      Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(ctx.data_dir, idx, index, [
        {key, value, expire_at_ms}
      ])

    :ets.insert(
      keydir,
      {key, nil, expire_at_ms, lfu, {:waraft_apply_projection, index}, 0, byte_size(value)}
    )
  end

  defp apply_projection_cache_count(ctx, shard_index) do
    Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(ctx.data_dir, shard_index)
  end

  defp terminal_count_cache_member?(path, state_index_key) do
    table = :ets.whereis(:ferricstore_flow_lmdb_terminal_count_cache)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    table != :undefined and :ets.member(table, {path, count_key})
  end

  defp readable_locator?(file_id, offset, value_size)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  defp readable_locator?({tag, index}, offset, value_size)
       when tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  defp readable_locator?(_file_id, _offset, _value_size), do: false

  defp collect_apply_projection_disk_reads(acc \\ []) do
    receive do
      {:apply_projection_disk_read, source} ->
        collect_apply_projection_disk_reads([source | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp restart_isolated_shard!(ctx, shard_index) do
    shard_name = elem(ctx.shard_names, shard_index)
    :ok = GenServer.call(shard_name, :flush, 5_000)
    :ok = GenServer.stop(Process.whereis(shard_name), :normal, 5_000)

    unless Process.whereis(Ferricstore.Flow.LMDBWriter.name(ctx.name, shard_index)) do
      {:ok, _pid} =
        Ferricstore.Flow.LMDBWriter.start_link(
          shard_index: shard_index,
          data_dir: ctx.data_dir,
          instance_ctx: ctx
        )
    end

    {:ok, _pid} =
      Ferricstore.Store.Shard.start_link(
        index: shard_index,
        data_dir: ctx.data_dir,
        instance_ctx: ctx
      )

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        try do
          match?({:ok, _}, GenServer.call(shard_name, :shard_stats, 500))
        catch
          :exit, _ -> false
        end
      end,
      "shard #{shard_index} not ready after restart",
      50,
      20
    )
  end
end
