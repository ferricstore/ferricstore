defmodule Ferricstore.Flow.HibernationDeterminismTest do
  @moduledoc """
  From feature level 8 hibernation drops only the cached value of a Flow's
  keydir row. The row and the Flow's index entries stay, so a claim picks from
  the same entries on every replica, whenever each node's LMDB writer gets to
  the eviction, and reads a hibernated record from disk.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 180_000

  alias Ferricstore.Flow.{Hibernation, Keys, LMDB}
  alias Ferricstore.Flow.LMDBWriter.AfterFlush
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @hot_window_ms 1_000

  setup do
    previous = Application.fetch_env(:ferricstore, :flow_hibernation_hot_window_ms)
    Application.put_env(:ferricstore, :flow_hibernation_hot_window_ms, @hot_window_ms)
    # The apply context reads the window when the instance starts, and feature
    # levels survive FLUSHDB, so each test needs a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, :flow_hibernation_hot_window_ms, value)
        :error -> Application.delete_env(:ferricstore, :flow_hibernation_hot_window_ms)
      end

      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    Ferricstore.Flow.Admission.clear_create_pause()
    {:ok, isolated: isolated, unique: System.unique_integer([:positive])}
  end

  test "a hibernated Flow keeps its keydir row and index entries and writes no cold rows",
       %{unique: unique} do
    assert {:ok, %{active: 8}} = FerricStore.raise_cluster_feature_level(8)
    %{type: type, id: id, due_ms: due_ms, index_keys: index_keys} = create_waiting(unique)

    await_value_dropped!(id)
    assert_indexed!(id, index_keys)
    assert_no_cold_rows!(id)

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type, worker: "w", limit: 1, now_ms: due_ms + 1)
  end

  test "a hibernated Flow stays indexed and uncached across a process restart",
       %{isolated: isolated, unique: unique} do
    assert {:ok, %{active: 8}} = FerricStore.raise_cluster_feature_level(8)

    %{type: type, id: id, due_ms: due_ms, index_keys: index_keys} =
      create_waiting(unique, due_in_ms: 60_000)

    await_value_dropped!(id)
    ctx = FerricStore.Instance.get(:default)
    :ok = restart_as_new_process(isolated, ctx.shard_count)

    # A restart loads every value; startup drops a hibernated one again.
    assert [{_key, nil, _, _, _, _, _}] = keydir_row(id)
    assert_indexed!(id, index_keys)

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type, worker: "w", limit: 1, now_ms: due_ms + 1)
  end

  test "raising level 8 restores Flows that hibernated before it",
       %{isolated: isolated, unique: unique} do
    assert {:ok, %{active: 7}} = FerricStore.raise_cluster_feature_level(7)
    %{type: type, id: id, due_ms: due_ms, index_keys: index_keys} = create_waiting(unique)

    # Below level 8 hibernation removes the keydir row and the index entries;
    # only the node's LMDB mirror locates the Flow.
    await_evicted!(id)
    assert_not_indexed!(id, index_keys)

    assert {:ok, %{active: 8}} = FerricStore.raise_cluster_feature_level(8)
    assert_keydir_row!(id)
    assert_indexed!(id, index_keys)

    ctx = FerricStore.Instance.get(:default)
    me = self()

    :telemetry.attach(
      "probe-restore",
      [:ferricstore, :flow, :hibernation, :restore],
      fn _e, m, md, _ -> send(me, {:restore, m, md}) end,
      nil
    )

    :ok = restart_as_new_process(isolated, ctx.shard_count)
    :telemetry.detach("probe-restore")

    receive do
      msg -> IO.inspect(msg, label: "PROBE restore during restart")
    after
      0 -> IO.puts("PROBE no restore during restart")
    end

    assert_keydir_row!(id)
    assert_indexed!(id, index_keys)

    # The cold rows that located it stay until a claim replaces their version.
    %{path: path, park_key: park_key} = cold_location(id)
    assert {:ok, park_blob} = LMDB.get(path, park_key)
    assert {:ok, %{locator: locator}} = LMDB.decode_cold_park(park_blob)
    by_segment_key = LMDB.cold_by_segment_key(locator)
    assert {:ok, ^park_key} = LMDB.get(path, by_segment_key)

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type, worker: "w", limit: 1, now_ms: due_ms + 1)

    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 10_000)
    assert :not_found = LMDB.get(path, park_key)
    assert :not_found = LMDB.get(path, by_segment_key)
    assert {:ok, []} = LMDB.prefix_entries(path, LMDB.cold_due_prefix(), 10)
  end

  test "an eviction queued before level 8 drops only the value", %{unique: unique} do
    assert {:ok, %{active: 7}} = FerricStore.raise_cluster_feature_level(7)
    ctx = FerricStore.Instance.get(:default)
    id = "flow-#{unique}"
    shard = Router.shard_for(ctx, Keys.state_key(id, nil))
    writer = Process.whereis(Ferricstore.Flow.LMDBWriter.name(ctx.name, shard))
    :ok = :sys.suspend(writer)

    %{type: type, due_ms: due_ms, index_keys: index_keys} =
      try do
        created = create_waiting(unique, hold_writer?: false)
        assert {:ok, %{active: 8}} = FerricStore.raise_cluster_feature_level(8)
        created
      after
        :ok = :sys.resume(writer)
      end

    await_value_dropped!(id)
    assert_indexed!(id, index_keys)

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type, worker: "w", limit: 1, now_ms: due_ms + 1)
  end

  test "after level 8 a claim with nothing due ignores leftover cold rows",
       %{unique: unique} do
    assert {:ok, %{active: 7}} = FerricStore.raise_cluster_feature_level(7)
    type = "hibernation-precheck-#{unique}"
    id = "flow-#{unique}"
    partition = "p-#{unique}"
    now_ms = System.system_time(:millisecond)
    due_ms = now_ms + 3 * @hot_window_ms

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: partition,
               run_at_ms: due_ms,
               now_ms: now_ms
             )

    await_keydir!(id, partition, "the Flow should leave the keydir", &(&1 == []))
    assert {:ok, %{active: 8}} = FerricStore.raise_cluster_feature_level(8)

    assert {:ok, [%{id: ^id}]} =
             FerricStore.flow_claim_due(type,
               worker: "w",
               limit: 1,
               partition_key: partition,
               now_ms: due_ms + 1
             )

    # The cold due row hibernation wrote before level 8 is still there, but
    # the hot index, which holds every waiting Flow from level 8, is empty, so
    # the claim needs no Raft entry.
    ctx = FerricStore.Instance.get(:default)
    state_key = Keys.state_key(id, partition)

    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(Router.shard_for(ctx, state_key))
      |> LMDB.path()

    assert {:ok, [_ | _]} = LMDB.prefix_entries(path, LMDB.cold_due_prefix(), 10)

    assert :empty =
             Router.flow_claim_due_presence(ctx, %{
               type: type,
               state: "queued",
               priority: nil,
               partition_key: partition,
               cold_due_mode: :allow,
               now_ms: due_ms + 1
             })
  end

  # Creates a Flow due after the hot window, so it hibernates once its shard's
  # LMDB writer flushes. The writer is held until the hot record has been read,
  # unless the caller holds it already.
  defp create_waiting(unique, opts \\ []) do
    type = "hibernation-determinism-#{unique}"
    id = "flow-#{unique}"
    now_ms = System.system_time(:millisecond)
    due_ms = now_ms + Keyword.get(opts, :due_in_ms, 3 * @hot_window_ms)
    ctx = FerricStore.Instance.get(:default)
    state_key = Keys.state_key(id, nil)
    shard = Router.shard_for(ctx, state_key)
    hold? = Keyword.get(opts, :hold_writer?, true)
    writer = Process.whereis(Ferricstore.Flow.LMDBWriter.name(ctx.name, shard))
    if hold?, do: :ok = :sys.suspend(writer)

    record =
      try do
        Ferricstore.Flow.Admission.clear_create_pause()
        assert :ok = FerricStore.flow_create(id, type: type, run_at_ms: due_ms, now_ms: now_ms)

        assert {:ok, record} =
                 AfterFlush.hot_flow_record_from_ets(elem(ctx.keydir_refs, shard), state_key)

        record
      after
        if hold?, do: :ok = :sys.resume(writer)
      end

    # The keys hibernation evicted below level 8, less the due-any index when
    # it is off.
    native = native_index(id)

    index_keys =
      record
      |> Hibernation.hot_index_keys(due_any?: true)
      |> Enum.filter(&match?({:ok, _score}, NativeOrderedIndex.score_of(native, &1, id)))

    assert Keys.due_key(type, record.state, Map.get(record, :priority, 0), record.partition_key) in index_keys

    %{type: type, id: id, due_ms: due_ms, index_keys: index_keys}
  end

  defp keydir_row(id, partition \\ nil) do
    ctx = FerricStore.Instance.get(:default)
    state_key = Keys.state_key(id, partition)
    :ets.lookup(elem(ctx.keydir_refs, Router.shard_for(ctx, state_key)), state_key)
  end

  defp await_keydir!(id, message, fun), do: await_keydir!(id, nil, message, fun)

  defp await_keydir!(id, partition, message, fun) do
    ctx = FerricStore.Instance.get(:default)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 10_000)
                 fun.(keydir_row(id, partition))
               end,
               message,
               100,
               50
             )
  end

  # From level 8 hibernation drops the cached value once the LMDB writer has
  # flushed past the Flow's write.
  defp await_value_dropped!(id) do
    await_keydir!(
      id,
      "the Flow's cached value should be dropped",
      &match?([{_, nil, _, _, _, _, _}], &1)
    )
  end

  defp await_evicted!(id), do: await_keydir!(id, "the Flow should leave the keydir", &(&1 == []))

  defp assert_keydir_row!(id), do: assert([{_key, _value, _, _, _, _, _}] = keydir_row(id))

  defp native_index(id) do
    ctx = FerricStore.Instance.get(:default)
    shard = Router.shard_for(ctx, Keys.state_key(id, nil))
    {index_table, lookup_table} = NativeOrderedIndex.table_names(ctx.name, shard)
    NativeOrderedIndex.get(index_table, lookup_table)
  end

  defp assert_indexed!(id, index_keys) do
    native = native_index(id)

    for key <- index_keys do
      assert match?({:ok, _score}, NativeOrderedIndex.score_of(native, key, id)),
             "#{inspect(key)} should hold #{id}"
    end
  end

  defp assert_not_indexed!(id, index_keys) do
    native = native_index(id)

    for key <- index_keys do
      refute match?({:ok, _score}, NativeOrderedIndex.score_of(native, key, id)),
             "#{inspect(key)} should not hold #{id}"
    end
  end

  defp cold_location(id) do
    ctx = FerricStore.Instance.get(:default)
    state_key = Keys.state_key(id, nil)

    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(Router.shard_for(ctx, state_key))
      |> LMDB.path()

    %{path: path, park_key: LMDB.cold_park_key_for_state_key(state_key)}
  end

  defp assert_no_cold_rows!(id) do
    %{path: path, park_key: park_key} = cold_location(id)
    assert :not_found = LMDB.get(path, park_key)
    assert {:ok, []} = LMDB.prefix_entries(path, LMDB.cold_due_prefix(), 10)
  end

  # A real process restart does not inherit VM-local LMDB writer generations,
  # so startup must not run the in-VM "writer restarted" dirty reconcile.
  defp restart_as_new_process(%{tmp_dir: data_dir, server_started?: server_started?}, shards) do
    for app <- [:ferricstore_server, :ferricstore],
        Enum.any?(Application.started_applications(), &(elem(&1, 0) == app)) do
      _ = Application.stop(app)
    end

    Ferricstore.Raft.WARaftBackend.stop()

    for shard_index <- 0..(shards - 1) do
      :persistent_term.erase(
        Ferricstore.Flow.LMDBWriter.Registry.enqueue_seq_key(:default, shard_index)
      )
    end

    Application.put_env(:ferricstore, :data_dir, data_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ShardHelpers.wait_shards_alive(30_000)

    if server_started? do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    end

    Ferricstore.Health.set_ready(true)
    :ok
  end
end
