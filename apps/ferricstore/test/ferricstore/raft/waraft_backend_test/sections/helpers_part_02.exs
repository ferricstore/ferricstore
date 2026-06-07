defmodule Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart02 do
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

  defp shard_bitcask_payload_present?(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Path.join("*.log")
    |> Path.wildcard()
    |> Enum.any?(&regular_payload_present?/1)
  end

  defp waraft_segment_payload_present?(root, shard_index) do
    root
    |> waraft_segment_log_dir(shard_index)
    |> Path.join("*")
    |> Path.wildcard()
    |> Enum.any?(&regular_payload_present?/1)
  end

  defp regular_payload_present?(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} when size > 0 -> true
      _other -> false
    end
  end

  defp unknown_atom_payload(atom_name) when is_binary(atom_name) do
    <<131, 100, byte_size(atom_name)::16, atom_name::binary>>
  end

  defp existing_atom?(atom_name) when is_binary(atom_name) do
    _ = String.to_existing_atom(atom_name)
    true
  rescue
    ArgumentError -> false
  end

  defp restore_chmoded_snapshot_dirs do
    receive do
      {:snapshot_payload_dir_chmod, path} ->
        _ = File.chmod(path, 0o700)
        restore_chmoded_snapshot_dirs()
    after
      0 -> :ok
    end
  end

  defp drain_storage_metadata_fsyncs do
    receive do
      {:storage_metadata_fsync, _path} -> drain_storage_metadata_fsyncs()
    after
      0 -> :ok
    end
  end

  defp collect_storage_metadata_fsyncs(acc \\ []) do
    receive do
      {:storage_metadata_fsync, path} -> collect_storage_metadata_fsyncs([path | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end

  defp flush_segment_append_telemetry do
    receive do
      {:waraft_segment_log_telemetry, [:ferricstore, :waraft, :segment_log, :append],
       _measurements, _metadata} ->
        flush_segment_append_telemetry()
    after
      0 -> :ok
    end
  end

  defp position_index({:raft_log_pos, index, _term}) when is_integer(index), do: index
  defp position_index(_position), do: 0

  defp assert_eventually(fun, expected, attempts \\ 50)

  defp assert_eventually(_fun, expected, 0),
    do: flunk("expected eventual value #{inspect(expected)}")

  defp assert_eventually(fun, expected, attempts) do
    case fun.() do
      ^expected ->
        :ok

      _other ->
        Process.sleep(20)
        assert_eventually(fun, expected, attempts - 1)
    end
  end

  defp raw_waraft_async_put(acceptor, reply_ref, key, value) do
    stamped = Ferricstore.Raft.CommandClock.to_ttb({:put, key, value, 0})
    :wa_raft_acceptor.commit_async(acceptor, {self(), reply_ref}, {make_ref(), stamped}, :low)
  end

  defp start_waraft_backend_peers(unique, count) do
    for i <- 1..count do
      name = :"waraft_backend_#{unique}_#{i}"
      data_dir = Path.join(System.tmp_dir!(), "ferricstore-waraft-backend-peer-#{unique}-#{i}")
      File.rm_rf!(data_dir)
      File.mkdir_p!(data_dir)

      node = start_waraft_backend_peer_node!(name, data_dir)
      start_peer_runtime_apps!(node.name)
      node
    end
  end

  defp restart_waraft_backend_peer!(%{name: node_name, data_dir: data_dir}) do
    node_name
    |> peer_local_name()
    |> start_waraft_backend_peer_node!(data_dir)
  end

  defp kill_peer_os_process!(%{name: node_name, peer: peer}) do
    pid = :rpc.call(node_name, System, :pid, [])
    assert is_binary(pid) or is_list(pid)

    previous_trap_exit = Process.flag(:trap_exit, true)
    monitor_ref = Process.monitor(peer)
    Node.monitor(node_name, true)

    try do
      assert {"", 0} = System.cmd("kill", ["-9", to_string(pid)])
      wait_for_node_down!(node_name)

      receive do
        {:DOWN, ^monitor_ref, :process, ^peer, _reason} -> :ok
      after
        1_000 -> :ok
      end

      receive do
        {:EXIT, ^peer, _reason} -> :ok
      after
        0 -> :ok
      end
    after
      Process.demonitor(monitor_ref, [:flush])
      Node.monitor(node_name, false)
      Process.flag(:trap_exit, previous_trap_exit)
    end
  end

  defp wait_for_node_down!(node_name, attempts \\ 100)

  defp wait_for_node_down!(node_name, 0),
    do: flunk("expected #{inspect(node_name)} to go down after OS kill")

  defp wait_for_node_down!(node_name, attempts) do
    if Node.ping(node_name) == :pang do
      :ok
    else
      receive do
        {:nodedown, ^node_name} ->
          :ok

        {:nodedown, ^node_name, _info} ->
          :ok
      after
        50 ->
          wait_for_node_down!(node_name, attempts - 1)
      end
    end
  end

  defp partition_waraft_peer!(node, all_nodes) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))
    real_cookie = :rpc.call(node.name, :erlang, :get_cookie, [])
    assert is_atom(real_cookie)

    assert true = :rpc.call(node.name, :erlang, :set_cookie, [node.name, :waraft_partitioned])

    for other <- others do
      :rpc.call(node.name, :erlang, :disconnect_node, [other.name])
      :rpc.call(other.name, :erlang, :disconnect_node, [node.name])
    end

    assert eventually(
             fn ->
               Enum.all?(others, fn other ->
                 :rpc.call(other.name, Node, :ping, [node.name]) == :pang and
                   :rpc.call(node.name, Node, :ping, [other.name]) == :pang
               end)
             end,
             40
           )

    real_cookie
  end

  defp heal_waraft_peer_partition!(node, all_nodes, real_cookie) do
    others = Enum.reject(all_nodes, &(&1.name == node.name))
    assert true = :rpc.call(node.name, :erlang, :set_cookie, [node.name, real_cookie])

    assert eventually(
             fn ->
               Enum.each(others, fn other ->
                 :rpc.call(other.name, Node, :connect, [node.name])
                 :rpc.call(node.name, Node, :connect, [other.name])
               end)

               Enum.all?(others, fn other ->
                 node_sees_other =
                   case :rpc.call(node.name, Node, :list, []) do
                     peers when is_list(peers) -> other.name in peers
                     _other -> false
                   end

                 other_sees_node =
                   case :rpc.call(other.name, Node, :list, []) do
                     peers when is_list(peers) -> node.name in peers
                     _other -> false
                   end

                 node_sees_other and other_sees_node
               end)
             end,
             40
           )

    :ok
  end

  defp start_waraft_backend_peer_node!(name, data_dir) do
    code_paths = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    {:ok, peer, node_name} =
      :peer.start(%{
        name: name,
        args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
        wait_boot: 120_000
      })

    %{name: node_name, peer: peer, data_dir: data_dir}
  end

  defp peer_local_name(node_name) when is_atom(node_name) do
    node_name
    |> Atom.to_string()
    |> String.split("@", parts: 2)
    |> hd()
    |> String.to_atom()
  end

  defp start_waraft_backend_peer_cluster!(nodes, unique, opts \\ []) do
    names = Enum.map(nodes, & &1.name)
    shard_count = Keyword.get(opts, :shard_count, 1)
    backend_opts = Keyword.take(opts, [:election_timeout_ms, :election_timeout_ms_max])

    for node <- nodes do
      start_waraft_backend_peer!(node, unique, [shard_count: shard_count] ++ backend_opts)
    end

    for node <- names do
      assert :ok = :rpc.call(node, WARaftBackend, :bootstrap_cluster, [names])
    end

    leaders =
      for shard_index <- 0..(shard_count - 1) do
        assert :ok = :rpc.call(hd(names), WARaftBackend, :trigger_election, [shard_index])
        wait_for_waraft_backend_leader(names, shard_index)
      end

    hd(leaders)
  end

  defp stop_waraft_backend_peer_cluster!(nodes, unique) do
    instance_name = waraft_backend_peer_instance_name(unique)

    for node <- nodes do
      assert :ok = :rpc.call(node.name, WARaftBackend, :stop, [])
      _ = :rpc.call(node.name, FerricStore.Instance, :cleanup, [instance_name])
    end

    :ok
  end

  defp waraft_backend_peer_instance_name(unique), do: :"waraft_backend_peer_#{unique}"

  defp start_waraft_backend_peer!(node, unique, opts \\ []) do
    shard_count = Keyword.get(opts, :shard_count, 1)
    backend_opts = Keyword.take(opts, [:election_timeout_ms, :election_timeout_ms_max])

    ctx =
      :rpc.call(node.name, FerricStore.Instance, :build, [
        waraft_backend_peer_instance_name(unique),
        instance_opts(node.data_dir, shard_count: shard_count)
      ])

    assert %FerricStore.Instance{} = ctx

    assert :ok =
             :rpc.call(node.name, WARaftBackend, :start, [
               ctx,
               [bootstrap: false, log_module: :ferricstore_waraft_spike_segment_log] ++
                 backend_opts
             ])
  end

  defp key_for_shard(ctx, shard_index, prefix) do
    1..10_000
    |> Enum.map(&"#{prefix}:#{shard_index}:#{&1}")
    |> Enum.find(&(Router.shard_for(ctx, &1) == shard_index))
  end

  defp flow_key_for_shard(ctx, shard_index, prefix) do
    1..10_000
    |> Enum.map(fn n ->
      id = "#{prefix}:#{shard_index}:#{n}"
      partition_key = "#{prefix}:partition:#{shard_index}:#{n}"
      key = Ferricstore.Flow.Keys.state_key(id, partition_key)
      {id, partition_key, key}
    end)
    |> Enum.find(fn {_id, _partition_key, key} -> Router.shard_for(ctx, key) == shard_index end)
  end

  defp delayed_parallel_batch_record?(
         {:put, "waraft-parallel-batch:" <> _, _value, _expire_at_ms}
       ),
       do: true

  defp delayed_parallel_batch_record?(_other), do: false

  defp start_peer_runtime_apps!(node_name) do
    quiet_peer_logger!(node_name)
    assert {:ok, _} = :rpc.call(node_name, Application, :ensure_all_started, [:telemetry])
    assert {:ok, _} = :rpc.call(node_name, Application, :ensure_all_started, [:os_mon])
  end

  defp quiet_peer_logger!(node_name) do
    assert :ok = :rpc.call(node_name, Application, :put_env, [:logger, :level, :warning])
    assert :ok = :rpc.call(node_name, Logger, :configure, [[level: :warning]])
  end

  defp wait_for_waraft_backend_leader(names, shard_index),
    do: wait_for_waraft_backend_leader(names, shard_index, 100)

  defp wait_for_waraft_backend_leader(_names, shard_index, 0),
    do: flunk("WARaft backend leader was not elected for shard #{shard_index}")

  defp wait_for_waraft_backend_leader(names, shard_index, attempts) do
    case Enum.find(names, fn node ->
           case :rpc.call(node, WARaftBackend, :status, [shard_index]) do
             status when is_list(status) -> Keyword.get(status, :state) == :leader
             _other -> false
           end
         end) do
      nil ->
        Process.sleep(50)
        wait_for_waraft_backend_leader(names, shard_index, attempts - 1)

      leader ->
        leader
    end
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(50)
      eventually(fun, attempts - 1)
    end
  end

  defp ensure_distribution! do
    Ferricstore.Test.ShardHelpers.ensure_distribution_started!(:waraft_backend_runner)

    Node.set_cookie(:ferricstore_waraft_backend_test)
  end
    end
  end
end
