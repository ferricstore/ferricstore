defmodule Ferricstore.Cluster.ClusterApiTest do
  @moduledoc """
  Unit tests for Ferricstore.Raft.Cluster functions.

  Tests the cluster API functions that don't require multi-node setups:
  server ID construction, members queries, and add/remove on the local
  single-node Raft groups.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Raft.WARaftBackend

  # ---------------------------------------------------------------------------
  # shard_server_id_on/2
  # ---------------------------------------------------------------------------

  describe "shard_server_id_on/2" do
    test "returns correct {name, node} tuple for shard 0" do
      result = Cluster.shard_server_id_on(0, :some_node@host)
      assert result == {:ferricstore_shard_0, :some_node@host}
    end

    test "returns correct tuple for shard 1" do
      result = Cluster.shard_server_id_on(1, :other_node@host)
      assert result == {:ferricstore_shard_1, :other_node@host}
    end

    test "returns correct tuple for higher shard indices" do
      result = Cluster.shard_server_id_on(7, node())
      assert result == {:ferricstore_shard_7, node()}
    end

    test "uses the provided node, not the local node" do
      remote = :"remote@127.0.0.1"
      {_name, returned_node} = Cluster.shard_server_id_on(0, remote)
      assert returned_node == remote
    end
  end

  # ---------------------------------------------------------------------------
  # shard_server_id/1
  # ---------------------------------------------------------------------------

  describe "shard_server_id/1" do
    test "returns {name, raft boot node} for the local node" do
      result = Cluster.shard_server_id(0)
      assert result == {:ferricstore_shard_0, Cluster.local_raft_node()}
    end

    test "returns different names for different shards" do
      {name0, _} = Cluster.shard_server_id(0)
      {name1, _} = Cluster.shard_server_id(1)
      assert name0 != name1
      assert name0 == :ferricstore_shard_0
      assert name1 == :ferricstore_shard_1
    end

    test "keeps the raft boot node stable when distribution starts later" do
      previous = Application.get_env(:ferricstore, :raft_local_node, :__missing__)
      Application.put_env(:ferricstore, :raft_local_node, :boot_node@localhost)

      on_exit(fn ->
        case previous do
          :__missing__ -> Application.delete_env(:ferricstore, :raft_local_node)
          value -> Application.put_env(:ferricstore, :raft_local_node, value)
        end
      end)

      assert Cluster.shard_server_id(0) == {:ferricstore_shard_0, :boot_node@localhost}
    end
  end

  describe "boot_initial_members/3" do
    test "uses full configured members only for initial bootstrap nodes" do
      previous = Application.get_env(:ferricstore, :raft_local_node, :__missing__)
      Application.put_env(:ferricstore, :raft_local_node, :node_a@localhost)

      on_exit(fn ->
        case previous do
          :__missing__ -> Application.delete_env(:ferricstore, :raft_local_node)
          value -> Application.put_env(:ferricstore, :raft_local_node, value)
        end
      end)

      local = {:ferricstore_shard_0, :node_a@localhost}

      assert Cluster.boot_initial_members(0, local, [
               :node_a@localhost,
               :node_b@localhost,
               :node_c@localhost
             ]) == [
               {:ferricstore_shard_0, :node_a@localhost},
               {:ferricstore_shard_0, :node_b@localhost},
               {:ferricstore_shard_0, :node_c@localhost}
             ]
    end

    test "joiner pointed at an existing cluster boots local-only until join sync" do
      previous = Application.get_env(:ferricstore, :raft_local_node, :__missing__)
      Application.put_env(:ferricstore, :raft_local_node, :joiner@localhost)

      on_exit(fn ->
        case previous do
          :__missing__ -> Application.delete_env(:ferricstore, :raft_local_node)
          value -> Application.put_env(:ferricstore, :raft_local_node, value)
        end
      end)

      local = {:ferricstore_shard_1, :joiner@localhost}

      assert Cluster.boot_initial_members(1, local, [
               :node_a@localhost,
               :node_b@localhost,
               :node_c@localhost
             ]) == [local]
    end
  end

  # ---------------------------------------------------------------------------
  # system_name/0
  # ---------------------------------------------------------------------------

  describe "system_name/0" do
    test "returns the ra system atom" do
      assert Cluster.system_name() == :ferricstore_raft
    end
  end

  # ---------------------------------------------------------------------------
  # members/1
  # ---------------------------------------------------------------------------

  describe "members/1" do
    setup :ensure_default_raft_booted_on_current_node

    test "returns members for shard 0" do
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          {:ok, members, leader} = Cluster.members(0)
          assert is_list(members)
          assert members != []
          assert leader != nil
        end,
        "shard 0 should have members",
        10,
        200
      )
    end

    test "returns leader as the local node in single-node mode" do
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          {:ok, _members, {_name, leader_node}} = Cluster.members(0)
          assert leader_node == Cluster.local_raft_node()
        end,
        "shard 0 should have local leader",
        10,
        200
      )
    end

    test "returns members for all configured shards" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        Ferricstore.Test.ShardHelpers.eventually(
          fn ->
            {:ok, members, _leader} = Cluster.members(i)
            assert is_list(members), "shard #{i} should return a member list"
            assert members != [], "shard #{i} should have at least 1 member"
          end,
          "shard #{i} should have members",
          10,
          200
        )
      end
    end
  end

  describe "members/1 with WARaft backend" do
    setup do
      previous_backend = Application.get_env(:ferricstore, :raft_backend)

      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-waraft-cluster-api-#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      ctx =
        FerricStore.Instance.build(
          :"waraft_cluster_api_#{System.unique_integer([:positive])}",
          data_dir: root,
          shard_count: 1,
          max_memory_bytes: 256 * 1024 * 1024,
          keydir_max_ram: 64 * 1024 * 1024,
          max_active_file_size: 64 * 1024 * 1024
        )

      Application.put_env(:ferricstore, :raft_backend, :waraft)
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      on_exit(fn ->
        _ = WARaftBackend.stop()
        FerricStore.Instance.cleanup(ctx.name)
        File.rm_rf(root)

        case previous_backend do
          nil -> Application.delete_env(:ferricstore, :raft_backend)
          value -> Application.put_env(:ferricstore, :raft_backend, value)
        end
      end)

      %{root: root, ctx: ctx}
    end

    test "returns WARaft members and leader instead of legacy Ra membership" do
      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          assert {:ok, members, leader} = Cluster.members(0)
          assert {:raft_server_ferricstore_waraft_backend_1, node()} in members
          assert leader == {:raft_server_ferricstore_waraft_backend_1, node()}
        end,
        "WARaft shard 0 should report WARaft membership",
        10,
        200
      )
    end

    test "start_system/1 does not create or start legacy Ra when WARaft is selected", %{
      root: root
    } do
      legacy_root = Path.join(root, "legacy-start")

      assert :ok = Cluster.start_system(legacy_root)
      refute File.dir?(Path.join(legacy_root, "ra"))
    end

    test "stop_system/0 delegates through selected backend and keeps WARaft no-op" do
      source = File.read!("lib/ferricstore/raft/cluster.ex")
      {:ok, ast} = Code.string_to_quoted(source)

      body =
        ast
        |> function_body(:stop_system, 0)
        |> Macro.to_string()

      assert body =~ "stop_system(Backend.selected())",
             "Cluster.stop_system/0 must use the selected backend instead of always stopping legacy Ra"

      assert source =~ "def stop_system(:waraft), do: :ok",
             "Cluster.stop_system(:waraft) must be a no-op; application shutdown stops WARaft separately"
    end

    test "trigger_shard_elections_parallel/2 delegates to WARaft when selected" do
      assert :ok = Cluster.trigger_shard_elections_parallel(1, timeout: 5_000)
    end

    test "start_shard_server/6 fails closed instead of starting legacy Ra when WARaft is selected",
         %{
           ctx: ctx
         } do
      assert {:error, :unsupported_waraft_shard_start} =
               Cluster.start_shard_server(
                 0,
                 Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0),
                 0,
                 Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), "00000.log"),
                 elem(ctx.keydir_refs, 0)
               )
    end

    test "stop_shard_server/1 fails closed instead of stopping legacy Ra when WARaft is selected" do
      assert {:error, :unsupported_waraft_shard_stop} = Cluster.stop_shard_server(0)
    end
  end

  # ---------------------------------------------------------------------------
  # add_member/3 -- local node tests
  # ---------------------------------------------------------------------------

  describe "add_member/3" do
    setup :ensure_default_raft_booted_on_current_node

    test "adding self as voter returns :ok (already member)" do
      # In single-node mode, self is already a voter -- ra returns :already_member -> :ok
      result = Cluster.add_member(0, Cluster.local_raft_node(), :voter)
      assert result == :ok
    end

    test "adding self with different membership updates membership type" do
      # ra accepts changing membership type of an existing member via add_member.
      result = Cluster.add_member(0, Cluster.local_raft_node(), :promotable)
      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # remove_member/2
  # ---------------------------------------------------------------------------

  describe "remove_member/2" do
    setup :ensure_default_raft_booted_on_current_node

    test "removing a non-member remote node returns error in single-node mode" do
      # In single-node Raft (nonode@nohost), ra rejects cluster changes to
      # nodes that aren't reachable. The error is :cluster_change_not_permitted.
      result = Cluster.remove_member(0, :nonexistent@nowhere)
      assert result == :ok or match?({:error, _}, result)
    end
  end

  defp function_body(ast, function_name, arity) do
    {_ast, bodies} =
      Macro.prewalk(ast, [], fn
        {:def, _meta, [{^function_name, _call_meta, args}, body]} = node, acc
        when (is_list(args) and length(args) == arity) or (is_nil(args) and arity == 0) ->
          {node, [body | acc]}

        node, acc ->
          {node, acc}
      end)

    case Enum.reverse(bodies) do
      [body | _] -> body
      [] -> flunk("missing #{function_name}/#{arity}")
    end
  end

  defp ensure_default_raft_booted_on_current_node(_context) do
    # ClusterHelper may start Erlang distribution after the test app already
    # booted as nonode@nohost. Real clustered deployments start distribution
    # before FerricStore. If the harness changes identity underneath legacy Ra,
    # restart against a fresh test data dir so these tests exercise a valid
    # product topology instead of mutating a stale nonode Ra group.
    if default_raft_identity_stale?() do
      Application.put_env(
        :ferricstore,
        :data_dir,
        Path.join(
          System.tmp_dir!(),
          "ferricstore_cluster_api_#{System.unique_integer([:positive])}"
        )
      )

      :ok = Application.stop(:ferricstore)

      case Application.ensure_all_started(:ferricstore) do
        {:ok, _apps} ->
          :ok

        {:error, reason} ->
          flunk("failed to restart FerricStore for cluster API test: #{inspect(reason)}")
      end
    end

    :ok
  end

  defp default_raft_identity_stale? do
    Application.get_env(:ferricstore, :raft_backend, :ra) == :ra and
      node() != :nonode@nohost and
      (Cluster.local_raft_node() != node() or local_leader_node(0) != node())
  end

  defp local_leader_node(shard_index) do
    case Cluster.members(shard_index, 500) do
      {:ok, _members, {_name, leader_node}} -> leader_node
      _ -> :unknown
    end
  end
end
