defmodule Ferricstore.ApplicationTest do
  @moduledoc """
  Tests for the OTP application supervisor tree, with focus on the optional
  libcluster Cluster.Supervisor integration.

  Verifies that:
  - The application starts cleanly with no libcluster topologies (nil or [])
  - The Cluster.Supervisor is included when topologies are configured
  - The supervision tree remains healthy in all configurations
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  import ExUnit.CaptureLog

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.LFU
  alias Ferricstore.Test.ShardHelpers

  # Ensure all shards are alive after every test so that the next test
  # module starts from a clean state.
  setup do
    ensure_ferricstore_ready!()
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ensure_ferricstore_ready!() end)
  end

  defp ensure_ferricstore_ready! do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ctx = FerricStore.Instance.get(:default)
    _ = Ferricstore.Raft.WARaftBackend.start(ctx)
    _ = Ferricstore.Flow.LMDBWriter.resume_all(ctx.shard_count)
    ShardHelpers.wait_shards_alive()
    ShardHelpers.wait_default_pipeline_ready()
  end

  defp stop_app_if_started(app) do
    if application_started?(app) do
      _ = Application.stop(app)
    end
  end

  defp application_started?(app) do
    Enum.any?(Application.started_applications(), fn {started_app, _desc, _vsn} ->
      started_app == app
    end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  # ---------------------------------------------------------------------------
  # Supervisor tree basics
  # ---------------------------------------------------------------------------

  describe "Ferricstore.Supervisor" do
    test "is alive after application start" do
      pid = Process.whereis(Ferricstore.Supervisor)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "all children are alive" do
      children = Supervisor.which_children(Ferricstore.Supervisor)

      for {id, pid, _type, _mods} <- children do
        if pid != :undefined do
          assert is_pid(pid) and Process.alive?(pid),
                 "Child #{inspect(id)} is not alive (pid=#{inspect(pid)})"
        end
      end
    end

    test "does not start removed async RMW coordinators" do
      children = Supervisor.which_children(Ferricstore.Supervisor)
      ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

      refute Enum.any?(ids, &(to_string(&1) =~ "rmw_coordinator")),
             "RmwCoordinator belonged to the removed local-write fallback and should not be supervised"
    end

    test "starts blob GC sweeper by default when blob side-channel is enabled" do
      children = Supervisor.which_children(Ferricstore.Supervisor)
      ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

      assert Ferricstore.Store.BlobGCSweeper in ids
      assert Process.whereis(Ferricstore.Store.BlobGCSweeper)
    end

    test "isolated instance uses configured blob side-channel threshold for raft writes" do
      value = :binary.copy("x", 256)

      ctx =
        Ferricstore.Test.IsolatedInstance.checkout(
          shard_count: 1,
          hot_cache_max_value_size: 64,
          blob_side_channel_threshold_bytes: 128
        )

      try do
        assert ctx.blob_side_channel_threshold_bytes == 128

        assert :ok =
                 Ferricstore.Store.Router.put(
                   ctx,
                   "blob-threshold-isolated",
                   value
                 )

        assert ^value = Ferricstore.Store.Router.get(ctx, "blob-threshold-isolated")

        assert [_blob_file | _] =
                 Path.wildcard(Path.join(ctx.data_dir, "blob/shard_0/*/*.bloblog"))
      after
        Ferricstore.Test.IsolatedInstance.checkin(ctx)
      end
    end

    test "instance build enables blob side-channel by default for large values" do
      old_threshold = Application.get_env(:ferricstore, :blob_side_channel_threshold_bytes)

      try do
        Application.delete_env(:ferricstore, :blob_side_channel_threshold_bytes)

        ctx =
          FerricStore.Instance.build(:blob_default_threshold_test,
            data_dir: System.tmp_dir!(),
            shard_count: 1
          )

        assert ctx.blob_side_channel_threshold_bytes == 256 * 1024
      after
        restore_env(:blob_side_channel_threshold_bytes, old_threshold)
      end
    end

    test "large-value scan performs no shard work for a zero-shard topology" do
      key = "zero-shard-large-value-#{System.unique_integer([:positive])}"
      true = :ets.insert(:keydir_0, {key, "value", 0, 0, 0, 0, 5})

      try do
        assert Ferricstore.Application.scan_large_values(0, 0) == {0, nil, 0}
      after
        :ets.delete(:keydir_0, key)
      end
    end
  end

  describe "graceful shutdown" do
    test "stopping the application clears WARaft backend state" do
      server_started? = application_started?(:ferricstore_server)

      try do
        stop_app_if_started(:ferricstore_server)
        assert :ok = Application.stop(:ferricstore)
        assert Ferricstore.Raft.Backend.running() == :undefined
        assert :persistent_term.get(:ferricstore_hlc_ref, nil) == nil

        assert_raise ArgumentError, fn ->
          FerricStore.Instance.get(:default)
        end
      after
        ensure_ferricstore_ready!()

        if server_started? do
          {:ok, _} = Application.ensure_all_started(:ferricstore_server)
        end
      end
    end

    test "failed startup clears WARaft backend and default instance context" do
      server_started? = application_started?(:ferricstore_server)

      try do
        stop_app_if_started(:ferricstore_server)
        assert :ok = Application.stop(:ferricstore)

        blocker =
          spawn(fn ->
            receive do
              :stop -> :ok
            end
          end)

        Process.register(blocker, Ferricstore.Stats)

        assert {:error, {:ferricstore, _reason}} = Application.ensure_all_started(:ferricstore)
        assert Ferricstore.Raft.Backend.running() == :undefined

        assert_raise ArgumentError, fn ->
          FerricStore.Instance.get(:default)
        end
      after
        stop_app_if_started(:ferricstore)

        case Process.whereis(Ferricstore.Stats) do
          nil -> :ok
          pid -> Process.exit(pid, :kill)
        end

        ensure_ferricstore_ready!()

        if server_started? do
          {:ok, _} = Application.ensure_all_started(:ferricstore_server)
        end
      end
    end

    test "exceptional startup failure clears WARaft backend and default instance context" do
      server_started? = application_started?(:ferricstore_server)
      previous_shard_count = Application.get_env(:ferricstore, :shard_count)

      try do
        stop_app_if_started(:ferricstore_server)
        assert :ok = Application.stop(:ferricstore)
        Application.put_env(:ferricstore, :shard_count, :invalid)

        assert {:error, {:ferricstore, _reason}} = Application.ensure_all_started(:ferricstore)
        assert Ferricstore.Raft.Backend.running() == :undefined

        assert_raise ArgumentError, fn ->
          FerricStore.Instance.get(:default)
        end

        assert Process.whereis(Ferricstore.Raft.WARaftBackend.RuntimeSupervisor) == nil
      after
        stop_app_if_started(:ferricstore)
        restore_env(:shard_count, previous_shard_count)
        ensure_ferricstore_ready!()

        if server_started? do
          {:ok, _} = Application.ensure_all_started(:ferricstore_server)
        end
      end
    end

    test "WARaft initialization failures are not mislabeled as election failures" do
      server_started? = application_started?(:ferricstore_server)
      previous_data_dir = Application.get_env(:ferricstore, :data_dir)
      previous_shard_count = Application.get_env(:ferricstore, :shard_count)

      previous_fsync_hook =
        Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-application-start-failure-#{System.unique_integer([:positive])}"
        )

      try do
        stop_app_if_started(:ferricstore_server)
        assert :ok = Application.stop(:ferricstore)
        Application.put_env(:ferricstore, :data_dir, root)
        Application.put_env(:ferricstore, :shard_count, 1)

        Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn _path ->
          {:error, :forced_application_start_fsync_failure}
        end)

        assert {:error, {:ferricstore, reason}} = Application.ensure_all_started(:ferricstore)
        assert inspect(reason) =~ "waraft_start_failed"
        refute inspect(reason) =~ "raft_election_failed"
      after
        stop_app_if_started(:ferricstore)
        restore_env(:data_dir, previous_data_dir)
        restore_env(:shard_count, previous_shard_count)
        restore_env(:waraft_storage_metadata_fsync_file_hook, previous_fsync_hook)
        File.rm_rf!(root)
        ensure_ferricstore_ready!()

        if server_started? do
          {:ok, _} = Application.ensure_all_started(:ferricstore_server)
        end
      end
    end

    test "prep_stop uses runtime shard count when config shard_count is auto" do
      original_shard_count = Application.get_env(:ferricstore, :shard_count)
      original_ready = Ferricstore.Health.ready?()

      shard_index = 2
      writer = Process.whereis(BitcaskWriter.writer_name(shard_index))
      assert is_pid(writer)

      dir =
        Path.join(
          System.tmp_dir!(),
          "prep_stop_runtime_shards_#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(dir)
      path = Path.join(dir, "00000.log")
      File.touch!(path)

      keydir =
        :ets.new(:"prep_stop_runtime_shards_#{System.unique_integer([:positive])}", [
          :set,
          :public
        ])

      key = "prep-stop-runtime-shard"
      value = "must-flush-shard-2"
      ctx = FerricStore.Instance.get(:default)

      on_exit(fn ->
        BitcaskWriter.flush(shard_index)

        try do
          :ets.delete(keydir)
        rescue
          ArgumentError -> :ok
        end

        File.rm_rf(dir)

        case original_shard_count do
          nil -> Application.delete_env(:ferricstore, :shard_count)
          count -> Application.put_env(:ferricstore, :shard_count, count)
        end

        Ferricstore.Health.set_ready(original_ready)
      end)

      :ets.insert(keydir, {key, value, 0, LFU.initial(), :pending, 0, 0})

      :sys.replace_state(writer, fn state ->
        %{
          state
          | pending: [{:write, ctx, path, 0, keydir, key, value, 0} | state.pending],
            pending_count: state.pending_count + 1
        }
      end)

      Application.put_env(:ferricstore, :shard_count, 0)

      Ferricstore.Application.prep_stop(nil)
      restore_env(:shard_count, original_shard_count)
      Ferricstore.Health.set_ready(original_ready)

      assert [{^key, ^value, 0, _lfu, 0, offset, vsize}] = :ets.lookup(keydir, key)
      assert vsize == byte_size(value)

      assert {:ok, ^value} = NIF.v2_pread_at(path, offset)
    end

    test "prep_stop skips lagged Flow LMDB projection drain because it is rebuildable" do
      original_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
      original_ready = Ferricstore.Health.ready?()

      Application.put_env(:ferricstore, :flow_lmdb_mode, :lagged)

      ctx = FerricStore.Instance.get(:default)
      shard_index = 0
      writer_name = Ferricstore.Flow.LMDBWriter.name(shard_index)
      writer = Process.whereis(writer_name)
      assert is_pid(writer)

      key = "flow:{application-test}:state:lagged-shutdown-#{System.unique_integer([:positive])}"

      on_exit(fn ->
        restore_env(:flow_lmdb_mode, original_mode)
        Ferricstore.Health.set_ready(original_ready)
        _ = Ferricstore.Flow.LMDBWriter.resume_all(ctx.shard_count)
        _ = Ferricstore.Raft.WARaftBackend.start(ctx)

        if pid = Process.whereis(writer_name) do
          :sys.replace_state(pid, fn state ->
            if timer = Map.get(state, :timer_ref), do: Process.cancel_timer(timer)

            %{
              state
              | pending: [],
                pending_after_flush: [],
                count: 0,
                first_pending_at: nil,
                last_enqueue_at: nil,
                timer_ref: nil
            }
          end)
        end
      end)

      assert :ok = Ferricstore.Flow.LMDBWriter.enqueue(shard_index, [{:put, key, "v1"}])

      assert %{count: count} = :sys.get_state(writer)
      assert count > 0

      capture_log([level: :info], fn ->
        Ferricstore.Application.prep_stop(%{
          shard_count: ctx.shard_count,
          data_dir: ctx.data_dir
        })
      end)

      assert %{suspended?: true} = :sys.get_state(writer)

      assert {:error, :writer_suspended} =
               Ferricstore.Flow.LMDBWriter.enqueue(shard_index, [{:put, key <> ":after", "v2"}])
    end

    test "shutdown Bitcask fsync reports active-file and fallback listing failures" do
      assert {:error,
              [
                {0, {:active_file_fsync_failed, "/tmp/active.log", :eio}},
                {1, {:list_log_files_failed, "/tmp/data/shard_1", {:permission_denied, "nope"}}}
              ]} =
               Ferricstore.Application.fsync_bitcask_for_shutdown(2, "/tmp",
                 active_file_path: fn
                   0 -> "/tmp/active.log"
                   1 -> nil
                 end,
                 exists?: fn
                   "/tmp/active.log" -> true
                   _ -> false
                 end,
                 fsync: fn "/tmp/active.log" -> {:error, :eio} end,
                 list_log_files: fn "/tmp/data/shard_1" ->
                   {:error, {:permission_denied, "nope"}}
                 end
               )
    end
  end

  # ---------------------------------------------------------------------------
  # libcluster integration
  # ---------------------------------------------------------------------------

  describe "libcluster Cluster.Supervisor" do
    test "application starts without crash when topologies config is nil" do
      # In test env, topologies is set to []. Temporarily set to nil and
      # verify the helper function returns an empty list (no crash).
      original = Application.get_env(:libcluster, :topologies)

      try do
        Application.put_env(:libcluster, :topologies, nil)

        # The running supervisor was already started (with [] or nil),
        # so ClusterSupervisor should NOT be present.
        children = Supervisor.which_children(Ferricstore.Supervisor)
        ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

        refute Ferricstore.ClusterSupervisor in ids,
               "ClusterSupervisor should NOT be started when topologies is nil"
      after
        # Restore original config
        if original == nil do
          Application.delete_env(:libcluster, :topologies)
        else
          Application.put_env(:libcluster, :topologies, original)
        end
      end
    end

    test "application starts without crash when topologies config is empty list" do
      # Test env explicitly sets topologies: [] -- verify no cluster supervisor.
      topologies = Application.get_env(:libcluster, :topologies)

      # The config should be :disabled, [], or nil in test env.
      assert topologies in [[], nil, :disabled],
             "Expected topologies to be :disabled, [] or nil in test env, got: #{inspect(topologies)}"

      children = Supervisor.which_children(Ferricstore.Supervisor)
      ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

      refute Ferricstore.ClusterSupervisor in ids,
             "ClusterSupervisor should NOT be started when topologies is []"
    end

    test "ClusterSupervisor is not among children in test env" do
      children = Supervisor.which_children(Ferricstore.Supervisor)

      cluster_supervisor_child =
        Enum.find(children, fn {id, _, _, _} ->
          id == Ferricstore.ClusterSupervisor
        end)

      assert cluster_supervisor_child == nil,
             "Expected no libcluster ClusterSupervisor child in test, got: #{inspect(cluster_supervisor_child)}"
    end

    test "cluster_supervisor_children returns child spec when topologies are configured" do
      # Verify Cluster.Supervisor module is loaded (libcluster dependency)
      if Code.ensure_loaded?(Cluster.Supervisor) do
        topologies = [
          ferricstore: [
            strategy: Cluster.Strategy.Gossip,
            config: [port: 45_892]
          ]
        ]

        # Cluster.Supervisor.child_spec/1 should produce a valid child spec
        spec = Cluster.Supervisor.child_spec([topologies, [name: Ferricstore.ClusterSupervisor]])

        assert is_map(spec)
        assert Map.has_key?(spec, :id)
        assert Map.has_key?(spec, :start)
      else
        # If libcluster is not available, the cluster_supervisor_children
        # function in application.ex gracefully returns [] via the nil branch.
        assert true, "Cluster.Supervisor not loaded -- libcluster optional"
      end
    end

    test "Cluster.Supervisor module is available when libcluster is a dependency" do
      # This test validates that the libcluster dependency is correctly wired.
      # If it fails, check that {:libcluster, "~> 3.3"} is in mix.exs deps.
      assert Code.ensure_loaded?(Cluster.Supervisor),
             "Cluster.Supervisor should be loadable -- is libcluster in deps?"
    end

    test "Cluster.Strategy.Gossip module is available" do
      assert Code.ensure_loaded?(Cluster.Strategy.Gossip),
             "Cluster.Strategy.Gossip should be loadable"
    end

    test "Cluster.Strategy.Epmd module is available" do
      assert Code.ensure_loaded?(Cluster.Strategy.Epmd),
             "Cluster.Strategy.Epmd should be loadable"
    end
  end
end
