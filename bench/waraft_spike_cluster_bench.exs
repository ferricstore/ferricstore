Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule WaraftSpikeClusterBench do
  @default_total 200_000
  @default_concurrency 200
  @default_pipeline 50
  @default_data_size 256
  @default_warmup 10_000

  def run do
    total = env_int("TOTAL", @default_total)
    concurrency = env_int("CONCURRENCY", @default_concurrency)
    pipeline = env_int("PIPELINE", @default_pipeline)
    data_size = env_int("DATA_SIZE", @default_data_size)
    warmup = env_int("WARMUP", @default_warmup)
    log = System.get_env("LOG", "segment")
    mode = System.get_env("BENCH_MODE", "set")

    ensure_distribution!()

    unique = :erlang.unique_integer([:positive])
    nodes = start_peers(unique, 3)

    try do
      names = Enum.map(nodes, & &1.name)
      connect_all(names)
      start_cluster_members(nodes, log, waraft_wal_env())
      bootstrap_all(names)
      :ok = :rpc.call(hd(names), :ferricstore_waraft_spike, :trigger_election, [])
      leader = wait_for_leader(names)

      {:ok, result} =
        :rpc.call(leader, :ferricstore_waraft_spike_load, load_fun(mode), [
          total,
          concurrency,
          pipeline,
          data_size,
          warmup
        ])

      IO.puts("""
      WARaft spike 3-node quorum #{mode}
      log=#{log} mode=#{mode} leader=#{leader}
      total=#{result.ops} concurrency=#{concurrency} pipeline=#{pipeline} data_size=#{data_size} warmup=#{warmup}
      reads=#{Map.get(result, :reads, 0)} writes=#{Map.get(result, :writes, result.ops)}
      elapsed_ms=#{Float.round(result.elapsed_us / 1000, 2)}
      ops_per_sec=#{Float.round(result.ops_per_sec, 2)}
      mb_per_sec=#{Float.round(result.mb_per_sec, 2)}
      """)
    after
      Enum.each(nodes, fn node ->
        try do
          :peer.stop(node.peer)
        catch
          _, _ -> :ok
        end

        File.rm_rf(node.data_dir)
      end)
    end
  end

  defp start_peers(unique, count) do
    code_paths = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    for i <- 1..count do
      name = :"waraft_bench_#{unique}_#{i}"
      data_dir = Path.join(System.tmp_dir!(), "ferricstore-waraft-bench-peer-#{unique}-#{i}")
      File.rm_rf!(data_dir)
      File.mkdir_p!(data_dir)

      {:ok, peer, node_name} =
        :peer.start(%{
          name: name,
          args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
          wait_boot: 120_000
        })

      :rpc.call(node_name, :logger, :set_primary_config, [:level, :warning])

      %{name: node_name, peer: peer, data_dir: data_dir}
    end
  end

  defp connect_all(names) do
    for left <- names, right <- names, left != right do
      true = :rpc.call(left, Node, :connect, [right])
    end
  end

  defp start_cluster_members(nodes, log, wal_env) do
    start_fun =
      case log do
        "segment" -> :start_cluster_member_segment_log
        other -> raise("unsupported LOG=#{inspect(other)}; expected segment")
      end

    for node <- nodes do
      configure_waraft_wal_env_on(node.name, wal_env)

      :ok =
        :rpc.call(node.name, :ferricstore_waraft_spike, start_fun, [
          String.to_charlist(node.data_dir)
        ])
    end
  end

  defp configure_waraft_wal_env_on(node, wal_env) do
    for {key, value} <- wal_env do
      :ok = :rpc.call(node, Application, :put_env, [:ferricstore, key, value])
    end
  end

  defp bootstrap_all(names) do
    for node <- names do
      :ok = :rpc.call(node, :ferricstore_waraft_spike, :bootstrap_cluster, [names])
    end
  end

  defp wait_for_leader(names, attempts \\ 100)
  defp wait_for_leader(_names, 0), do: raise("WARaft leader was not elected")

  defp wait_for_leader(names, attempts) do
    case Enum.find(names, fn node ->
           case :rpc.call(node, :ferricstore_waraft_spike, :status, []) do
             status when is_list(status) -> Keyword.get(status, :state) == :leader
             _ -> false
           end
         end) do
      nil ->
        Process.sleep(50)
        wait_for_leader(names, attempts - 1)

      leader ->
        leader
    end
  end

  defp load_fun("set"), do: :run
  defp load_fun("mixed"), do: :run_mixed

  defp load_fun(other) do
    raise("unsupported BENCH_MODE=#{inspect(other)}; expected set or mixed")
  end

  defp ensure_distribution! do
    case Node.self() do
      :nonode@nohost ->
        node_name = :"waraft_bench_runner_#{:erlang.unique_integer([:positive])}"

        case Node.start(node_name, :shortnames) do
          {:ok, _} -> :ok
          {:error, reason} -> raise("failed to start distributed node: #{inspect(reason)}")
        end

      _ ->
        :ok
    end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp waraft_wal_env do
    []
    |> maybe_put_int_env("WAL_COMMIT_DELAY_US", :wal_commit_delay_us)
    |> maybe_put_int_env("WAL_MAX_BUFFER_BYTES", :wal_max_buffer_bytes)
    |> maybe_put_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    |> maybe_put_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)
    |> maybe_put_int_env("WARAFT_SEGMENT_SYNC_DELAY_US", :waraft_segment_log_sync_delay_us)
    |> Keyword.put(
      :waraft_segment_log_io_mode,
      env_atom("WARAFT_SEGMENT_IO_MODE", :file, [:file, :wal_nif])
    )
    |> Keyword.put(
      :waraft_segment_log_preallocate_bytes,
      env_int("WARAFT_SEGMENT_PREALLOCATE_BYTES", 256 * 1024 * 1024)
    )
    |> maybe_put_atom_env("WARAFT_SEGMENT_SYNC_METHOD", :waraft_segment_log_sync_method, [
      :datasync,
      :sync,
      :auto
    ])
  end

  defp maybe_put_int_env(acc, env, app_key) do
    case System.get_env(env) do
      nil -> acc
      value -> [{app_key, String.to_integer(value)} | acc]
    end
  end

  defp env_atom(name, default, allowed) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        atom = String.to_existing_atom(value)

        unless atom in allowed do
          raise "unsupported #{name}=#{inspect(value)}; expected one of #{inspect(allowed)}"
        end

        atom
    end
  end

  defp maybe_put_atom_env(acc, env, app_key, allowed) do
    case System.get_env(env) do
      nil ->
        acc

      value ->
        atom = String.to_existing_atom(value)

        unless atom in allowed do
          raise "unsupported #{env}=#{inspect(value)}; expected one of #{inspect(allowed)}"
        end

        [{app_key, atom} | acc]
    end
  end
end

WaraftSpikeClusterBench.run()
