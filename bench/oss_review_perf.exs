# Compare post-0.11.23 review changes in fresh VMs with identical dependencies/NIFs.
# ERL_FLAGS='+S 8:8' BENCH_VARIANT=release BENCH_TRIAL=1 \
#   mise exec -- mix run --no-start bench/oss_review_perf.exs
# Repeat with BENCH_VARIANT=review, alternating order between trials.
# Results: bench/output/oss-review/<variant>-<trial>.json
# This is an in-process worker/NIF benchmark, not a TCP or cold-device benchmark.

defmodule FerricstoreBench.OSSReview do
  alias Ferricstore.Bitcask.{Async, NIF}
  alias Ferricstore.Commands.List, as: ListCmd
  alias Ferricstore.Waiters
  alias FerricstoreServer.Native.{Blocking, ResourceBudget, Session}

  @base "e5f59ba7959710773729812343ae9bcd62d8d15f"
  @storage "apps/ferricstore/lib/ferricstore/raft/waraft_storage.ex"

  def run do
    variant = System.get_env("BENCH_VARIANT", "review")
    if variant not in ["release", "review"], do: raise("invalid BENCH_VARIANT")
    trial = System.get_env("BENCH_TRIAL", "1")
    async_seconds = env_integer("BENCH_ASYNC_SECONDS", 5)
    list_seconds = env_integer("BENCH_LIST_SECONDS", 20)
    warmup_seconds = env_integer("BENCH_WARMUP_SECONDS", 1)
    Logger.configure(level: :error)

    root = Path.join(System.tmp_dir!(), "ferricstore-review-perf-#{System.pid()}")
    if File.exists?(root), do: raise("benchmark directory already exists: #{root}")
    File.mkdir_p!(root)

    Application.put_env(:ferricstore, :data_dir, Path.join(root, "data"))
    Application.put_env(:ferricstore, :shard_count, 4)
    Application.put_env(:ferricstore, :node_name, nil)
    Application.put_env(:ferricstore, :cluster_nodes, [])
    Application.put_env(:ferricstore, :cluster_auto_join, false)
    Application.put_env(:ferricstore, :native_port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :health_probe_port, 0)

    try do
      manifest = load_variant(variant, root)
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
      budget = Process.whereis(ResourceBudget)
      ctx = FerricStore.Instance.get(:default)

      IO.puts(
        "BENCH_START #{variant}/#{trial} backend=#{Ferricstore.Raft.Backend.running_or_selected()}"
      )

      fixture = Path.join(root, "reads.log")
      value = :binary.copy("r", 4096)
      records = for i <- 0..255, do: {"read-#{i}", value, 0}
      {:ok, locations} = NIF.v2_append_batch(fixture, records)
      :ok = NIF.v2_fsync(fixture)
      reads = Enum.zip(locations, records) |> List.to_tuple()

      scenarios =
        for concurrency <- [1, 16],
            kind <- [:proxy_only, :pread_4k, :append_fsync_256b, :list_ready, :list_blocked] do
          seconds = if kind in [:list_ready, :list_blocked], do: list_seconds, else: async_seconds
          prepare = fn worker -> operation(kind, worker, root, fixture, reads, value, ctx) end

          sample_every =
            case kind do
              :proxy_only -> 100
              :pread_4k -> 10
              _ -> 1
            end

          measure(prepare, concurrency, warmup_seconds, 0)
          result = measure(prepare, concurrency, seconds, sample_every)
          result = Map.merge(result, %{scenario: kind, concurrency: concurrency})
          IO.puts("BENCH_SCENARIO " <> Jason.encode!(result))
          result
        end

      burst = burst_probe(ctx)
      true = Process.alive?(budget)

      result = %{
        variant: variant,
        trial: trial,
        base: @base,
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
        elixir: System.version(),
        otp: :erlang.system_info(:otp_release) |> to_string(),
        schedulers: :erlang.system_info(:schedulers_online),
        dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers),
        backend: Ferricstore.Raft.Backend.running_or_selected(),
        shard_count: ctx.shard_count,
        async_seconds: async_seconds,
        list_seconds: list_seconds,
        warmup_seconds: warmup_seconds,
        source_sha256: manifest,
        scenarios: scenarios,
        burst_probe: burst
      }

      output = Path.join(["bench", "output", "oss-review", "#{variant}-#{trial}.json"])
      File.mkdir_p!(Path.dirname(output))
      File.write!(output, Jason.encode!(result, pretty: true))
      IO.puts("BENCH_RESULT #{output} burst=#{inspect(burst)}")
    after
      Application.stop(:ferricstore_server)
      Application.stop(:ferricstore)
      File.rm_rf!(root)
    end
  end

  # Load the exact changed release sources before any application starts. All
  # unchanged BEAM modules, dependencies, compiler settings, and native binaries
  # are shared. Recompile the storage macro's caller for both variants as well.
  # No disk BEAM artifacts or working-tree source files are overwritten.
  defp load_variant(variant, root) do
    {head, 0} = System.cmd("git", ["rev-parse", "HEAD"])
    if String.trim(head) != @base, do: raise("run from the review worktree based on #{@base}")

    {changed, 0} =
      System.cmd("git", [
        "diff",
        "--name-only",
        @base,
        "--",
        "apps",
        "config",
        "mix.exs",
        "mix.lock",
        "vendor"
      ])

    paths = String.split(changed, "\n", trim: true)

    unless Enum.all?(paths, fn path ->
             (String.contains?(path, "/lib/") and String.ends_with?(path, ".ex")) or
               (String.contains?(path, "/test/") and String.ends_with?(path, [".ex", ".exs"]))
           end) do
      raise "comparison requires identical dependencies, configuration, and native code"
    end

    sources =
      Enum.filter(paths, &(String.contains?(&1, "/lib/") and String.ends_with?(&1, ".ex")))

    Code.compiler_options(ignore_module_conflict: true)

    for path <- sources ++ [@storage], into: %{} do
      source =
        if variant == "release" do
          {source, 0} = System.cmd("git", ["show", "#{@base}:#{path}"])
          source
        else
          File.read!(path)
        end

      for {module, binary} <- Code.compile_string(source, path) do
        # Recovery preloads atoms by reading each module's BEAM filename.
        beam_path = Path.join(root, "#{module}.beam")
        File.write!(beam_path, binary)
        :code.purge(module)
        {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), binary)
      end

      {path, Base.encode16(:crypto.hash(:sha256, source), case: :lower)}
    end
  end

  defp operation(:proxy_only, _worker, _root, _fixture, _reads, _value, _ctx) do
    fn _iteration ->
      {:ok, :ok} =
        Async.await(
          fn pid, id ->
            send(pid, {:tokio_complete, id, :ok})
            :ok
          end,
          5_000
        )
    end
  end

  defp operation(:pread_4k, worker, _root, fixture, reads, value, _ctx) do
    fn iteration ->
      {{offset, _size}, {key, _, _}} =
        elem(reads, rem(iteration + worker * 17, tuple_size(reads)))

      {:ok, ^value} = Async.await(&NIF.v2_pread_at_key_async(&1, &2, fixture, offset, key), 5_000)
    end
  end

  defp operation(:append_fsync_256b, worker, root, _fixture, _reads, _value, _ctx) do
    path = Path.join(root, "writes-#{worker}.log")
    record = [{"key", :binary.copy("w", 256), 0}]

    fn _iteration ->
      {:ok, [_location]} = Async.await(&NIF.v2_append_batch_async(&1, &2, path, record), 5_000)
      {:ok, :ok} = Async.await(&NIF.v2_fsync_async(&1, &2, path), 5_000)
    end
  end

  defp operation(kind, worker, _root, _fixture, _reads, _value, ctx)
       when kind in [:list_ready, :list_blocked] do
    key = "review-perf:#{kind}:#{worker}"
    {:ok, prepared} = Session.prepare_command(%{"command" => "BLPOP", "args" => [key, "5"]})

    state = %{
      instance_ctx: ctx,
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    meta = %{request_id: worker}
    push = fn -> 1 = ListCmd.handle_ast({:rpush, [key, "value"]}, ctx) end

    fn _iteration ->
      if kind == :list_ready, do: push.()
      {:ok, pid, monitor} = Blocking.start_prepared(prepared, state, meta)

      if kind == :list_blocked do
        await_registration(key, pid, now_ns() + 5_000_000_000)
        push.()
      end

      receive do
        {:native_blocking_response, ^meta, ^pid, :ok, [^key, "value"]} ->
          :ok

        {:DOWN, ^monitor, :process, ^pid, reason} ->
          raise "blocking worker died: #{inspect(reason)}"
      after
        5_000 -> raise "blocking result timed out"
      end

      receive do
        {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
      after
        5_000 -> raise "blocking worker did not exit"
      end
    end
  end

  defp measure(prepare, concurrency, seconds, sample_every) do
    parent = self()

    workers =
      for worker <- 1..concurrency do
        spawn_monitor(fn ->
          operation = prepare.(worker)
          send(parent, {:ready, self()})

          receive do
            {:run, deadline} ->
              {count, samples, finished} = loop(operation, deadline, sample_every, 0, [])
              send(parent, {:result, self(), count, samples, finished})
          end
        end)
      end

    for {pid, _ref} <- workers do
      receive do
        {:ready, ^pid} -> :ok
      after
        10_000 -> raise "worker preparation timed out"
      end
    end

    started = now_ns()
    for {pid, _ref} <- workers, do: send(pid, {:run, started + seconds * 1_000_000_000})

    reports =
      for {pid, ref} <- workers do
        report =
          receive do
            {:result, ^pid, count, samples, finished} -> {count, samples, finished}
            {:DOWN, ^ref, :process, ^pid, reason} -> raise "benchmark failed: #{inspect(reason)}"
          after
            (seconds + 30) * 1_000 -> raise "benchmark timed out"
          end

        receive do
          {:DOWN, ^ref, :process, ^pid, :normal} -> :ok
        after
          5_000 -> raise "benchmark worker did not exit"
        end

        report
      end

    count = Enum.sum(Enum.map(reports, &elem(&1, 0)))
    elapsed = (Enum.max(Enum.map(reports, &elem(&1, 2))) - started) / 1.0e9
    samples = Enum.flat_map(reports, &elem(&1, 1)) |> Enum.sort()

    %{
      operations: count,
      seconds: elapsed,
      ops_per_second: count / elapsed,
      samples: length(samples),
      sample_every: sample_every,
      p50_us: percentile(samples, 0.50),
      p95_us: percentile(samples, 0.95),
      p99_us: percentile(samples, 0.99)
    }
  end

  defp loop(operation, deadline, sample_every, count, samples) do
    started = now_ns()

    if started >= deadline do
      {count, samples, started}
    else
      operation.(count)
      finished = now_ns()
      # Sample throughout the whole interval; do not bias tails toward warmup.
      samples =
        if sample_every > 0 and rem(count, sample_every) == 0,
          do: [(finished - started) / 1_000 | samples],
          else: samples

      loop(operation, deadline, sample_every, count + 1, samples)
    end
  end

  defp burst_probe(ctx) do
    key = "review-perf:burst"
    {:ok, prepared} = Session.prepare_command(%{"command" => "BLPOP", "args" => [key, "0"]})

    state = %{
      instance_ctx: ctx,
      acl_cache: :full_access,
      require_auth: false,
      authenticated: true
    }

    workers =
      for id <- 1..3 do
        {:ok, pid, ref} = Blocking.start_prepared(prepared, state, %{request_id: id})
        await_registration(key, pid, now_ns() + 5_000_000_000)
        {pid, ref, id}
      end

    try do
      3 = ListCmd.handle_ast({:rpush, [key, "one", "two", "three"]}, ctx)
      deadline = now_ns() + 1_000_000_000

      replies =
        for {pid, _ref, id} <- workers do
          receive do
            {:native_blocking_response, %{request_id: ^id}, ^pid, :ok, [^key, value]} -> value
          after
            max(div(deadline - now_ns(), 1_000_000), 0) -> nil
          end
        end

      %{
        completed: Enum.count(replies, &(not is_nil(&1))),
        expected: 3,
        values_in_waiter_order: replies
      }
    after
      for {pid, ref, _} <- workers do
        Process.exit(pid, :kill)
        Process.demonitor(ref, [:flush])
        Waiters.cleanup(pid)
      end
    end
  end

  defp await_registration(key, pid, deadline) do
    if Enum.any?(:ets.lookup(:ferricstore_waiters, key), &(elem(&1, 1) == pid)) do
      :ok
    else
      if now_ns() >= deadline, do: raise("waiter did not register")
      :erlang.yield()
      await_registration(key, pid, deadline)
    end
  end

  defp percentile([], _p), do: nil
  defp percentile(samples, p), do: Enum.at(samples, max(ceil(length(samples) * p) - 1, 0))
  defp now_ns, do: System.monotonic_time(:nanosecond)

  defp env_integer(key, default),
    do: System.get_env(key, to_string(default)) |> String.to_integer()
end

FerricstoreBench.OSSReview.run()
