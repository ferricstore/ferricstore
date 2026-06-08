defmodule Ferricstore.Raft.WARaftBackendTest.Sections.BackendAddMemberRetriesStagedParticipantAfterFailedTransfer do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

      test "backend add_member retries from staged participant after failed transfer" do
        unless Ferricstore.Test.ClusterHelper.peer_available?() do
          flunk(":peer is required for WARaft backend cluster test")
        end

        ensure_distribution!()

        unique = :erlang.unique_integer([:positive])
        nodes = start_waraft_backend_peers(unique, 4)
        initial_nodes = Enum.take(nodes, 3)
        joining_node = List.last(nodes)

        on_exit(fn ->
          Enum.each(nodes, fn node ->
            try do
              :rpc.call(node.name, WARaftBackend, :stop, [])
            catch
              _, _ -> :ok
            end

            try do
              :peer.stop(node.peer)
            catch
              _, _ -> :ok
            end

            File.rm_rf(node.data_dir)
          end)
        end)

        names = Enum.map(nodes, & &1.name)

        for left <- names, right <- names, left != right do
          :rpc.call(left, Node, :connect, [right])
        end

        leader = start_waraft_backend_peer_cluster!(initial_nodes, unique)
        joining_peer = {:raft_server_ferricstore_waraft_backend_1, joining_node.name}

        assert :ok =
                 :rpc.call(leader, WARaftBackend, :write, [
                   0,
                   {:put, "backend-cluster-add-retry:before", "v1", 0}
                 ])

        assert {:error, _reason} =
                 :rpc.call(leader, WARaftBackend, :add_member, [
                   0,
                   joining_node.name,
                   [timeout_ms: 1_000]
                 ])

        assert eventually(fn ->
                 membership = :rpc.call(leader, WARaftBackend, :membership, [0])
                 is_list(membership) and joining_peer not in membership
               end)

        start_waraft_backend_peer!(joining_node, unique)

        assert {:ok, {:raft_log_pos, _, _}} =
                 :rpc.call(leader, WARaftBackend, :add_member, [0, joining_node.name])

        assert eventually(fn ->
                 membership = :rpc.call(leader, WARaftBackend, :membership, [0])
                 is_list(membership) and joining_peer in membership
               end)

        assert eventually(fn ->
                 :rpc.call(joining_node.name, WARaftBackend, :local_get, [
                   0,
                   "backend-cluster-add-retry:before"
                 ]) == "v1"
               end)
      end
    end
  end
end
