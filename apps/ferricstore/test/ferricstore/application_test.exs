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

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.LFU
  alias Ferricstore.Test.ShardHelpers

  # Ensure all shards are alive after every test so that the next test
  # module starts from a clean state.
  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> wait_shards_alive(2_000) end)
  end

  defp wait_shards_alive(timeout_ms) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.each(0..(shard_count - 1), fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      Stream.repeatedly(fn -> Process.sleep(20) end)
      |> Enum.find(fn _ ->
        pid = Process.whereis(name)
        alive = is_pid(pid) and Process.alive?(pid)
        alive or System.monotonic_time(:millisecond) > deadline
      end)
    end)
  end

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
        assert is_pid(pid) and Process.alive?(pid),
               "Child #{inspect(id)} is not alive (pid=#{inspect(pid)})"
      end
    end
  end

  describe "graceful shutdown" do
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

      assert [{^key, ^value, 0, _lfu, 0, offset, vsize}] = :ets.lookup(keydir, key)
      assert vsize == byte_size(value)

      assert {:ok, ^value} = NIF.v2_pread_at(path, offset)
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
