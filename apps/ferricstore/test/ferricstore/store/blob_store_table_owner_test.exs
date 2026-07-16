defmodule Ferricstore.Store.BlobStoreTableOwnerTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Raft.WARaftBackend.RuntimeSupervisor
  alias Ferricstore.Raft.WARaftSegmentReader.TableOwner, as: ApplyProjectionTableOwner
  alias Ferricstore.Store.ActiveFile.TableOwner, as: ActiveFileTableOwner
  alias Ferricstore.Store.BlobStore
  alias Ferricstore.Store.BlobStore.TableOwner

  @active_file_table_heir Ferricstore.Store.ActiveFile.TableHeir
  @table_heir Ferricstore.Store.BlobStore.TableHeir
  @apply_projection_table_heir Ferricstore.Raft.WARaftSegmentReader.TableHeir

  defmodule BlockingBlobTableOwner do
    @moduledoc false

    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)

    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call(:ensure_tables, _from, state), do: {:reply, :ok, state}
  end

  test "ensure_tables never starts an owner outside supervision" do
    assert :ok = Supervisor.terminate_child(Ferricstore.Supervisor, TableOwner)
    assert Process.whereis(TableOwner) == nil

    try do
      assert {:error, :table_owner_unavailable} = TableOwner.ensure_tables()
      assert Process.whereis(TableOwner) == nil
    after
      case Process.whereis(TableOwner) do
        pid when is_pid(pid) -> GenServer.stop(pid)
        nil -> :ok
      end

      assert {:ok, _pid} = Supervisor.restart_child(Ferricstore.Supervisor, TableOwner)
    end
  end

  test "standalone WARaft blob runtime starts once under supervision" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-standalone-blob-runtime-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    application_started? = application_started?(:ferricstore)

    try do
      if application_started? do
        assert :ok = Application.stop(:ferricstore)
      end

      assert Process.whereis(TableOwner) == nil
      assert Process.whereis(@table_heir) == nil
      assert Process.whereis(ActiveFileTableOwner) == nil
      assert Process.whereis(@active_file_table_heir) == nil
      assert Process.whereis(ApplyProjectionTableOwner) == nil
      assert Process.whereis(@apply_projection_table_heir) == nil

      results =
        1..16
        |> Task.async_stream(
          fn _ -> RuntimeSupervisor.ensure_started() end,
          max_concurrency: 16,
          ordered: false,
          timeout: 5_000
        )
        |> Enum.to_list()

      assert Enum.all?(results, &(&1 == {:ok, :ok}))

      runtime = Process.whereis(RuntimeSupervisor)
      owner = Process.whereis(TableOwner)
      heir = Process.whereis(@table_heir)
      active_file_owner = Process.whereis(ActiveFileTableOwner)
      active_file_heir = Process.whereis(@active_file_table_heir)
      apply_projection_owner = Process.whereis(ApplyProjectionTableOwner)
      apply_projection_heir = Process.whereis(@apply_projection_table_heir)

      assert is_pid(runtime)
      assert is_pid(owner)
      assert is_pid(heir)
      assert is_pid(active_file_owner)
      assert is_pid(active_file_heir)
      assert is_pid(apply_projection_owner)
      assert is_pid(apply_projection_heir)
      assert Enum.any?(Supervisor.which_children(:kernel_sup), &(elem(&1, 1) == runtime))
      assert :ets.info(:ferricstore_active_files, :owner) == active_file_owner
      assert :ets.info(:ferricstore_blob_store_locks, :owner) == owner

      assert :ets.info(:ferricstore_waraft_apply_projection_cache, :owner) ==
               apply_projection_owner

      writes =
        1..16
        |> Task.async_stream(
          fn n -> BlobStore.put(root, 0, :binary.copy(<<n>>, 128)) end,
          max_concurrency: 16,
          ordered: false,
          timeout: 5_000
        )
        |> Enum.to_list()

      assert Enum.all?(writes, fn
               {:ok, {:ok, %Ferricstore.Store.BlobRef{}}} -> true
               _other -> false
             end)

      backend_root = root <> "-waraft"

      ctx =
        FerricStore.Instance.build(
          :"standalone_blob_runtime_#{System.unique_integer([:positive])}",
          data_dir: backend_root,
          shard_count: 1,
          blob_side_channel_threshold_bytes: 64
        )

      try do
        assert :ok =
                 Ferricstore.Raft.WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert is_pid(Process.whereis(RuntimeSupervisor))
        assert is_pid(Process.whereis(TableOwner))
        assert is_pid(Process.whereis(ApplyProjectionTableOwner))

        assert :ok =
                 Ferricstore.Raft.WARaftBackend.write(
                   0,
                   {:put, "standalone-blob-runtime", :binary.copy("v", 256), 0}
                 )
      after
        Ferricstore.Raft.WARaftBackend.stop()
        FerricStore.Instance.cleanup(ctx.name)
        File.rm_rf!(backend_root)
      end
    after
      try do
        if function_exported?(RuntimeSupervisor, :stop, 0) do
          _ = RuntimeSupervisor.stop()
        end

        File.rm_rf!(root)
      after
        if application_started? do
          assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
        end
      end
    end
  end

  test "WARaft backend startup returns an error when its table runtime cannot start" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-blocked-blob-runtime-#{System.unique_integer([:positive])}"
      )

    application_started? = application_started?(:ferricstore)

    ctx =
      FerricStore.Instance.build(
        :"blocked_blob_runtime_#{System.unique_integer([:positive])}",
        data_dir: root,
        shard_count: 1
      )

    try do
      if application_started? do
        assert :ok = Application.stop(:ferricstore)
      end

      assert {:ok, _fake_owner} = BlockingBlobTableOwner.start_link(TableOwner)

      assert {:error, _reason} =
               Ferricstore.Raft.WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert Process.whereis(RuntimeSupervisor) == nil

      refute Enum.any?(Supervisor.which_children(:kernel_sup), fn
               {RuntimeSupervisor, _pid, _type, _modules} -> true
               _other -> false
             end)

      assert_raise ArgumentError, fn ->
        Ferricstore.Raft.WARaftBackend.context!(:ferricstore_waraft_backend)
      end
    after
      _ = Ferricstore.Raft.WARaftBackend.stop()

      case Process.whereis(TableOwner) do
        pid when is_pid(pid) ->
          GenServer.stop(pid)

        nil ->
          :ok
      end

      FerricStore.Instance.cleanup(ctx.name)
      File.rm_rf!(root)

      if application_started? do
        assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
      end
    end
  end

  @tag :waraft_runtime_transition
  test "application startup replaces an existing standalone WARaft runtime" do
    standalone_root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-standalone-runtime-transition-#{System.unique_integer([:positive])}"
      )

    application_root = standalone_root <> "-application"
    application_started? = application_started?(:ferricstore)
    previous_data_dir = Application.get_env(:ferricstore, :data_dir)
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)

    standalone_ctx =
      FerricStore.Instance.build(
        :"standalone_runtime_transition_#{System.unique_integer([:positive])}",
        data_dir: standalone_root,
        shard_count: 1
      )

    try do
      if application_started? do
        assert :ok = Application.stop(:ferricstore)
      end

      Application.put_env(:ferricstore, :data_dir, application_root)
      Application.put_env(:ferricstore, :shard_count, 1)

      assert :ok =
               Ferricstore.Raft.WARaftBackend.start(standalone_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert :ok =
               Ferricstore.Raft.WARaftBackend.write(
                 0,
                 {:put, "standalone-runtime-transition", "value", 0}
               )

      runtime = Process.whereis(RuntimeSupervisor)

      old_storage =
        Process.whereis(:wa_raft_storage.registered_name(:ferricstore_waraft_backend, 1))

      old_apply_projection_owner = Process.whereis(ApplyProjectionTableOwner)
      old_active_file_owner = Process.whereis(ActiveFileTableOwner)
      runtime_monitor = Process.monitor(runtime)
      storage_monitor = Process.monitor(old_storage)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
      assert_receive {:DOWN, ^runtime_monitor, :process, ^runtime, _reason}, 5_000
      assert_receive {:DOWN, ^storage_monitor, :process, ^old_storage, _reason}, 5_000

      assert Process.whereis(RuntimeSupervisor) == nil

      app_apply_projection_owner = Process.whereis(ApplyProjectionTableOwner)
      app_active_file_owner = Process.whereis(ActiveFileTableOwner)

      app_storage =
        Process.whereis(:wa_raft_storage.registered_name(:ferricstore_waraft_backend, 1))

      assert is_pid(app_apply_projection_owner)
      assert app_apply_projection_owner != old_apply_projection_owner
      assert is_pid(app_active_file_owner)
      assert app_active_file_owner != old_active_file_owner
      assert is_pid(app_storage)
      assert app_storage != old_storage

      assert :ets.info(:ferricstore_waraft_apply_projection_cache, :owner) ==
               app_apply_projection_owner

      assert :ets.info(:ferricstore_active_files, :owner) == app_active_file_owner
    after
      if application_started?(:ferricstore) do
        _ = Application.stop(:ferricstore)
      else
        _ = RuntimeSupervisor.stop()
      end

      FerricStore.Instance.cleanup(standalone_ctx.name)
      File.rm_rf!(standalone_root)
      File.rm_rf!(application_root)
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)

      if application_started? do
        assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
      end
    end
  end

  test "blob table owner crashes preserve in-flight protection state" do
    BlobStore.init_tables()
    table = :ferricstore_blob_store_hardened_protections
    table_tid = :ets.whereis(table)
    owner = Process.whereis(TableOwner)
    id = make_ref()
    row = {id, "owner-crash", 0, ["segments/0.bloblog"], 1, %{}}
    :ets.insert(table, row)

    on_exit(fn ->
      if :ets.whereis(table) != :undefined, do: :ets.delete(table, id)
    end)

    monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, 5_000
    assert table_tid == :ets.whereis(table)
    assert [^row] = :ets.lookup(table, id)
    restarted_owner = await_restarted_owner(owner, 100)
    assert is_pid(restarted_owner)
    assert_owner_eventually(table, restarted_owner, 100)
  end

  test "apply projection table owner crash preserves rows and counters through its heir" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-apply-projection-owner-crash-#{System.unique_integer([:positive])}"
      )

    table = :ferricstore_waraft_apply_projection_cache
    table_tid = :ets.whereis(table)
    owner = Process.whereis(ApplyProjectionTableOwner)
    value = :binary.copy("projection", 128)

    on_exit(fn ->
      Ferricstore.Raft.WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(data_dir, 0, 211, [
               {"owner-crash", value, 0}
             ])

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) ==
             byte_size(value)

    monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, 5_000

    assert table_tid == :ets.whereis(table)
    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1

    assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_bytes(data_dir, 0) ==
             byte_size(value)

    restarted_owner = await_restarted_process(ApplyProjectionTableOwner, owner, 100)
    assert is_pid(restarted_owner)
    assert_owner_eventually(table, restarted_owner, 100)
  end

  test "a restarted table heir is rearmed before a later owner crash" do
    BlobStore.init_tables()
    table = :ferricstore_blob_store_hardened_protections
    table_tid = :ets.whereis(table)
    owner = Process.whereis(TableOwner)
    old_heir = Process.whereis(@table_heir)
    id = make_ref()
    row = {id, "heir-then-owner-crash", 0, ["segments/1.bloblog"], 1, %{}}
    :ets.insert(table, row)

    on_exit(fn ->
      if :ets.whereis(table) != :undefined, do: :ets.delete(table, id)

      current_heir = Process.whereis(@table_heir)

      if is_pid(current_heir) and :ets.info(table, :heir) != current_heir do
        current_owner = Process.whereis(TableOwner)
        if is_pid(current_owner), do: Process.exit(current_owner, :kill)
      end
    end)

    heir_monitor = Process.monitor(old_heir)
    Process.exit(old_heir, :kill)
    assert_receive {:DOWN, ^heir_monitor, :process, ^old_heir, :killed}, 5_000

    restarted_heir = await_restarted_process(@table_heir, old_heir, 100)
    assert is_pid(restarted_heir)
    assert_heir_eventually(table, restarted_heir, 100)

    owner_monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, 5_000

    assert table_tid == :ets.whereis(table)
    assert [^row] = :ets.lookup(table, id)
    restarted_owner = await_restarted_owner(owner, 100)
    assert is_pid(restarted_owner)
    assert_owner_eventually(table, restarted_owner, 100)
  end

  defp await_restarted_owner(_old_owner, 0), do: nil

  defp await_restarted_owner(old_owner, attempts) do
    case Process.whereis(TableOwner) do
      pid when is_pid(pid) and pid != old_owner ->
        pid

      _missing ->
        Process.sleep(10)
        await_restarted_owner(old_owner, attempts - 1)
    end
  end

  defp await_restarted_process(_name, _old_pid, 0), do: nil

  defp await_restarted_process(name, old_pid, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != old_pid ->
        pid

      _missing ->
        Process.sleep(10)
        await_restarted_process(name, old_pid, attempts - 1)
    end
  end

  defp assert_owner_eventually(table, expected_owner, 0) do
    assert :ets.info(table, :owner) == expected_owner
  end

  defp assert_owner_eventually(table, expected_owner, attempts) do
    if :ets.info(table, :owner) == expected_owner do
      :ok
    else
      Process.sleep(10)
      assert_owner_eventually(table, expected_owner, attempts - 1)
    end
  end

  defp assert_heir_eventually(table, expected_heir, 0) do
    assert :ets.info(table, :heir) == expected_heir
  end

  defp assert_heir_eventually(table, expected_heir, attempts) do
    if :ets.info(table, :heir) == expected_heir do
      :ok
    else
      Process.sleep(10)
      assert_heir_eventually(table, expected_heir, attempts - 1)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp application_started?(app) do
    Enum.any?(Application.started_applications(), fn {started_app, _description, _version} ->
      started_app == app
    end)
  end
end
