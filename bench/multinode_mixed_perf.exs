# MIX_ENV=test ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start bench/multinode_mixed_perf.exs
# Local :peer nodes share a host and disk. Reads target each shard's leader;
# writes are distributed over nodes and therefore include leader forwarding.

defmodule FerricstoreBench.MultinodeMixed do
  alias Ferricstore.Test.ClusterHelper
  alias Ferricstore.Store.Router

  def run do
    fixture = prepare_segment_terms_variant()

    try do
      run_benchmark()
    after
      if fixture, do: File.rm_rf!(fixture)
    end
  end

  defp run_benchmark do
    Logger.configure(level: :error)
    candidate_ebin = System.get_env("BENCH_WARAFT_EBIN")

    if candidate_ebin do
      # Repeated -pa arguments in ClusterHelper reverse peer path precedence.
      true = Code.append_path(candidate_ebin)

      for file <- Path.wildcard(Path.join(candidate_ebin, "wa_raft*.beam")) do
        module = file |> Path.basename(".beam") |> String.to_atom()
        {:module, ^module} = :code.load_abs(file |> Path.rootname() |> String.to_charlist())
      end
    end

    leader_writes? = System.get_env("BENCH_LEADER_WRITES") == "1"
    trace? = System.get_env("BENCH_TRACE") == "1"
    heartbeat_ms = System.get_env("BENCH_HEARTBEAT_MS")
    profile? = System.get_env("BENCH_PROFILE") == "1"

    duration_us =
      System.get_env("BENCH_SECONDS", if(trace?, do: "5", else: "30"))
      |> String.to_integer()
      |> Kernel.*(1_000_000)

    warmup_us =
      System.get_env("BENCH_WARMUP_SECONDS", "0") |> String.to_integer() |> Kernel.*(1_000_000)

    trial_count = System.get_env("BENCH_TRIALS", "2") |> String.to_integer()

    node_counts =
      case System.get_env("BENCH_NODES") do
        nil -> nil
        counts -> counts |> String.split(",") |> Enum.map(&String.to_integer/1)
      end

    profile_modules =
      if profile?,
        do: Code.compile_file(Path.join(__DIR__, "support/waraft_perf_metrics.exs")),
        else: []

    results =
      for trial <- if(trace?, do: [1], else: 1..trial_count),
          count <-
            node_counts ||
              if(trace? or leader_writes?,
                do: [3],
                else: if(rem(trial, 2) == 1, do: [1, 3], else: [3, 1])
              ) do
        nodes = ClusterHelper.start_cluster(count, shards: 4, timeout: 30_000)

        try do
          for node <- nodes do
            peer_md5 = :erpc.call(node.name, :wa_raft_server, :module_info, [:md5])
            true = peer_md5 == :wa_raft_server.module_info(:md5)
            provider = :ferricstore_waraft_spike_segment_log

            true =
              :erpc.call(node.name, provider, :module_info, [:md5]) == provider.module_info(:md5)
          end

          if profile? do
            for node <- nodes, {module, binary} <- profile_modules do
              {:module, ^module} =
                :erpc.call(node.name, :code, :load_binary, [module, ~c"benchmark_metrics", binary])

              {:ok, _pid} = :erpc.call(node.name, module, :start, [])
            end
          end

          if heartbeat_ms do
            interval = String.to_integer(heartbeat_ms)
            if interval <= 0, do: raise("heartbeat interval must be positive")

            for node <- nodes do
              :ok =
                :erpc.call(node.name, Application, :put_env, [
                  :ferricstore_waraft_backend,
                  :raft_heartbeat_interval_ms,
                  interval
                ])

              ^interval =
                :erpc.call(node.name, :wa_raft_env, :get_table_env, [
                  :ferricstore_waraft_backend,
                  :ferricstore_waraft_backend,
                  :raft_heartbeat_interval_ms,
                  120
                ])
            end
          end

          contexts =
            Map.new(
              nodes,
              &{&1.name, :erpc.call(&1.name, FerricStore.Instance, :get, [:default])}
            )

          leaders = Map.new(0..3, &{&1, ClusterHelper.find_leader(nodes, &1)})
          for {_shard, leader} <- leaders, do: true = Map.has_key?(contexts, leader)

          parent = self()

          tasks =
            for id <- 0..7 do
              Task.async(fn ->
                writer = Enum.at(nodes, rem(id, count)).name
                writer_ctx = contexts[writer]
                key = "multinode-perf:#{id}"
                shard = :erpc.call(writer, Router, :shard_for, [writer_ctx, key])
                leader = leaders[shard]
                read_ctx = contexts[leader]
                writer = if leader_writes?, do: leader, else: writer
                writer_ctx = contexts[writer]
                :ok = :erpc.call(writer, Router, :put, [writer_ctx, key, value(0), 0], 30_000)

                warmup =
                  loop(
                    writer,
                    writer_ctx,
                    leader,
                    read_ctx,
                    key,
                    System.monotonic_time(:microsecond) + warmup_us,
                    0,
                    0,
                    %{read: [], write: []}
                  )

                send(parent, {:benchmark_ready, self()})

                receive do
                  {:benchmark_run, deadline} ->
                    loop(
                      writer,
                      writer_ctx,
                      leader,
                      read_ctx,
                      key,
                      deadline,
                      0,
                      warmup.version,
                      %{read: [], write: []}
                    )
                end
              end)
            end

          for %{pid: pid} <- tasks do
            receive do
              {:benchmark_ready, ^pid} -> :ok
            after
              60_000 -> raise("warmup failed to complete")
            end
          end

          if profile? do
            for node <- nodes do
              :ok = :erpc.call(node.name, FerricstoreBench.WARaftPerfMetrics, :reset, [])
            end
          end

          started = System.monotonic_time(:microsecond)
          for %{pid: pid} <- tasks, do: send(pid, {:benchmark_run, started + duration_us})
          reports = Task.await_many(tasks, 60_000)
          elapsed = (System.monotonic_time(:microsecond) - started) / 1.0e6

          for {report, id} <- Enum.zip(reports, 0..7), node <- nodes do
            expected = value(report.version)

            eventually(
              fn ->
                :erpc.call(node.name, Router, :get, [contexts[node.name], "multinode-perf:#{id}"]) ==
                  expected
              end,
              50
            )
          end

          operations =
            for kind <- [:read, :write], into: %{} do
              samples = reports |> Enum.flat_map(&Map.fetch!(&1.samples, kind)) |> Enum.sort()

              {kind,
               %{
                 count: length(samples),
                 p50_us: q(samples, 0.5),
                 p95_us: q(samples, 0.95),
                 p99_us: q(samples, 0.99),
                 max_us: List.last(samples)
               }}
            end

          result = %{
            segment_terms_variant: System.get_env("BENCH_SEGMENT_TERMS", "bounded"),
            segment_provider_beam_md5:
              Base.encode16(:ferricstore_waraft_spike_segment_log.module_info(:md5), case: :lower),
            dependency_variant:
              System.get_env(
                "BENCH_VARIANT",
                if(candidate_ebin, do: "candidate", else: "installed")
              ),
            server_compiler:
              :wa_raft_server.module_info(:compile) |> Keyword.fetch!(:version) |> to_string(),
            trial_label: System.get_env("BENCH_TRIAL_ID", to_string(trial)),
            dependency_candidate: candidate_ebin != nil,
            server_beam_md5: Base.encode16(:wa_raft_server.module_info(:md5), case: :lower),
            heartbeat_ms: if(heartbeat_ms, do: String.to_integer(heartbeat_ms), else: 120),
            leader_targeted_writes: leader_writes?,
            nodes: count,
            trial: trial,
            shards: 4,
            clients: 8,
            seconds: elapsed,
            ops_per_second: (operations.read.count + operations.write.count) / elapsed,
            operations: operations,
            verified_replicas: count,
            errors: 0
          }

          config_sample =
            :erpc.call(hd(nodes).name, :wa_raft_env, :get_table_env, [
              :ferricstore_waraft_backend,
              :ferricstore_waraft_backend,
              :raft_commit_batch_interval_ms,
              2
            ])

          result = Map.put(result, :effective_commit_batch_interval_ms, config_sample)

          result =
            Map.merge(result, %{
              warmup_seconds: warmup_us / 1.0e6,
              requested_seconds: duration_us / 1.0e6,
              profile:
                if(profile?,
                  do:
                    Enum.map(
                      nodes,
                      &:erpc.call(&1.name, FerricstoreBench.WARaftPerfMetrics, :snapshot, [])
                    ),
                  else: []
                )
            })

          traces =
            if trace? do
              for shard <- 0..3, target <- [:leader, :follower], iteration <- 1..4 do
                leader = leaders[shard]

                writer =
                  if target == :leader,
                    do: leader,
                    else: Enum.find(nodes, &(&1.name != leader)).name

                code = """
                ctx = FerricStore.Instance.get(:default)
                key = Enum.find_value(0..1000, fn i ->
                  key = "multinode-trace:" <> Integer.to_string(i)
                  if Ferricstore.Store.Router.shard_for(ctx, key) == #{shard}, do: key
                end)
                previous = Ferricstore.LatencyTrace.start()
                started = System.monotonic_time(:microsecond)
                :ok = Ferricstore.Store.Router.put(ctx, key, "#{iteration}", 0)
                elapsed = System.monotonic_time(:microsecond) - started
                trace = Ferricstore.LatencyTrace.finish(previous)
                %{elapsed_us: elapsed, trace: trace}
                """

                {sample, _bindings} = :erpc.call(writer, Code, :eval_string, [code], 30_000)
                Map.merge(sample, %{shard: shard, target: target})
              end
            else
              []
            end

          result = Map.put(result, :trace_samples, traces)

          IO.puts("MULTINODE_RESULT " <> Jason.encode!(result))
          result
        after
          ClusterHelper.stop_cluster(nodes)
        end
      end

    File.mkdir_p!("bench/results")

    output =
      if heartbeat_ms,
        do:
          "bench/results/multinode-heartbeat-#{heartbeat_ms}#{if trace?, do: "-trace", else: ""}.json",
        else:
          if(trace?,
            do: "bench/results/multinode-write-trace.json",
            else:
              if(leader_writes?,
                do: "bench/results/multinode-mixed-leader-perf.json",
                else: "bench/results/multinode-mixed-perf.json"
              )
          )

    output = System.get_env("BENCH_OUTPUT", output)

    File.write!(
      output,
      Jason.encode!(%{results: results, same_host_and_disk: true}, pretty: true)
    )
  end

  defp prepare_segment_terms_variant do
    case System.get_env("BENCH_SEGMENT_TERMS", "bounded") do
      "bounded" ->
        nil

      "baseline" ->
        source_dir = Path.expand("apps/ferricstore/src")
        header = "ferricstore_waraft_spike_segment_log/sections/part_01.hrl"
        source = File.read!(Path.join(source_dir, header))

        true =
          String.contains?(
            source,
            "Last -> fold_terms_impl(Log, Start, min(End, Last), Func, Acc)"
          )

        # Bound the replacement at the following get/2 definition, preserving
        # all other source sections and the candidate's existing review changes.
        start = :binary.match(source, "fold_terms(Log, Start, End, Func, Acc) ->\n") |> elem(0)
        {finish, _} = :binary.match(source, "\nget(#raft_log")

        baseline =
          binary_part(source, 0, start) <>
            "fold_terms(Log, Start, End, Func, Acc) ->\n    fold_terms_impl(Log, Start, End, Func, Acc).\n" <>
            binary_part(source, finish, byte_size(source) - finish)

        true = baseline != source
        root = Path.join([System.tmp_dir!(), "opencode", "segment-terms-control-#{System.pid()}"])
        if File.exists?(root), do: raise("segment control fixture exists")
        File.mkdir_p!(root)
        header_path = Path.join(root, "part_01.hrl")
        File.write!(header_path, baseline)
        module = :ferricstore_waraft_spike_segment_log
        input = Path.join(root, "#{module}.erl")
        module_source = File.read!(Path.join(source_dir, "#{module}.erl"))

        File.write!(
          input,
          String.replace(
            module_source,
            "-include(\"#{header}\").",
            "-include(\"#{header_path}\")."
          )
        )

        {:ok, ^module, beam, []} =
          :compile.file(
            String.to_charlist(input),
            [:binary, :return_errors, :return_warnings, {:i, String.to_charlist(source_dir)}]
          )

        beam_path = Path.join(root, "#{module}.beam")
        File.write!(beam_path, beam)
        true = Code.append_path(root)
        {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        root

      other ->
        raise("invalid BENCH_SEGMENT_TERMS: #{other}")
    end
  end

  defp loop(writer, wctx, leader, rctx, key, deadline, i, version, samples) do
    start = System.monotonic_time(:microsecond)

    if start >= deadline do
      %{version: version, samples: samples}
    else
      {kind, version} =
        if rem(i, 4) == 0 do
          module =
            if System.get_env("BENCH_PROFILE") == "1",
              do: FerricstoreBench.WARaftPerfMetrics,
              else: Router

          function = if module == Router, do: :put, else: :traced_put
          :ok = :erpc.call(writer, module, function, [wctx, key, value(version + 1), 0], 30_000)
          {:write, version + 1}
        else
          expected = value(version)
          ^expected = :erpc.call(leader, Router, :get, [rctx, key], 30_000)
          {:read, version}
        end

      us = System.monotonic_time(:microsecond) - start

      loop(
        writer,
        wctx,
        leader,
        rctx,
        key,
        deadline,
        i + 1,
        version,
        Map.update!(samples, kind, &[us | &1])
      )
    end
  end

  defp value(n), do: <<n::unsigned-64, 0::size(248 * 8)>>
  defp q(samples, fraction), do: Enum.at(samples, ceil(length(samples) * fraction) - 1)

  defp eventually(fun, tries) do
    if fun.(),
      do: :ok,
      else:
        (
          if tries <= 0, do: raise("replica did not converge")
          Process.sleep(100)
          eventually(fun, tries - 1)
        )
  end
end

FerricstoreBench.MultinodeMixed.run()
