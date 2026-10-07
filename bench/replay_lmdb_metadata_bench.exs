# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/replay_lmdb_metadata_bench.exs
# Isolates filesystem-discovery overhead in the existing empty cold-due proof.
# This is a component benchmark, not an end-to-end recovery-time claim.

defmodule FerricstoreBench.ReplayLMDBMetadata do
  alias Ferricstore.Flow.LMDB

  @source "apps/ferricstore/lib/ferricstore/flow/lmdb.ex"
  @output "bench/results/replay-lmdb-metadata.json"

  def run do
    baseline =
      if File.exists?(@output),
        do: Jason.decode!(File.read!(@output))["baseline_source"],
        else: File.read!(@source)

    posix = String.replace(baseline, "File.lstat(path)", "File.lstat(path, time: :posix)")

    raw =
      baseline
      |> String.replace("File.lstat(path)", "local_lstat(path)")
      |> String.replace(
        "  def env_present?(_path), do: false\n",
        """
          def env_present?(_path), do: false

          defp local_lstat(path) do
            case :file.read_link_info(path, [:raw, {:time, :posix}]) do
              {:ok, info} -> {:ok, File.Stat.from_record(info)}
              error -> error
            end
          end
        """
      )

    sources = [baseline: baseline, posix: posix, raw: raw]

    variants =
      for {name, source} <- sources do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        [{^module, _}] =
          source
          |> String.replace(
            "defmodule Ferricstore.Flow.LMDB do",
            "defmodule #{inspect(module)} do"
          )
          |> Code.compile_string()

        {name, module}
      end

    root = Path.join(System.tmp_dir!(), "ferricstore-replay-metadata-#{System.pid()}")
    if File.exists?(root), do: raise("benchmark directory already exists")
    File.mkdir_p!(root)
    paths = for i <- 1..4, do: Path.join(root, "shard-#{i}")
    for path <- paths, do: :ok = LMDB.write_batch(path, [{:put, "unrelated", "value"}])

    try do
      measurements =
        for concurrency <- [1, 4],
            scenario <- [:discovery, :empty_proof],
            trial <- 1..3,
            {name, module} <- order(variants, trial) do
          iterations = if scenario == :discovery, do: 10_000, else: 5_000
          selected = Enum.take(paths, concurrency)
          operation = fn path -> operation(module, scenario, path) end
          measure(selected, operation, 100)
          result = measure(selected, operation, iterations)

          result =
            Map.merge(result, %{
              variant: name,
              concurrency: concurrency,
              scenario: scenario,
              trial: trial
            })

          IO.puts(Jason.encode!(result))
          result
        end

      result = %{
        baseline_source: baseline,
        candidate_sources: Map.new(sources),
        source_sha256:
          Map.new(sources, fn {name, source} ->
            {name, Base.encode16(:crypto.hash(:sha256, source), case: :lower)}
          end),
        elixir: System.version(),
        otp: to_string(:erlang.system_info(:otp_release)),
        schedulers: :erlang.system_info(:schedulers_online),
        results: measurements
      }

      File.mkdir_p!(Path.dirname(@output))
      File.write!(@output, Jason.encode!(result, pretty: true))
    after
      for path <- paths, do: :ok = LMDB.release(path)
      File.rm_rf!(root)
    end
  end

  defp order(variants, 1), do: variants
  defp order(variants, 2), do: Enum.reverse(variants)
  defp order([first | rest], 3), do: rest ++ [first]

  defp operation(module, :discovery, path), do: true = module.env_present?(path)

  defp operation(module, :empty_proof, path) do
    first_prefix = LMDB.cold_due_bucket_prefix(120_000)
    after_key = binary_part(first_prefix, 0, byte_size(first_prefix) - 1)
    false = module.flush_in_progress?(path)
    {:ok, txn} = module.last_txn_id(path)

    {:ok, [], true, 0} =
      module.range_entries_bounded(
        path,
        LMDB.cold_due_prefix(),
        after_key,
        LMDB.cold_due_bucket_prefix(420_000),
        1,
        1_048_576
      )

    {:ok, ^txn} = module.last_txn_id(path)
    false = module.flush_in_progress?(path)
    {:ok, ^txn} = module.last_txn_id(path)
    :ok
  end

  defp measure(paths, operation, iterations) do
    parent = self()

    tasks =
      for path <- paths do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go ->
              {us, _} = :timer.tc(fn -> for _ <- 1..iterations, do: operation.(path) end)
              us
          end
        end)
      end

    for %{pid: pid} <- tasks do
      receive do
        {:ready, ^pid} -> :ok
      after
        5_000 -> raise("worker startup timed out")
      end
    end

    started = System.monotonic_time(:microsecond)
    for %{pid: pid} <- tasks, do: send(pid, :go)
    worker_us = Task.await_many(tasks, 120_000)
    elapsed = System.monotonic_time(:microsecond) - started

    %{
      operations: length(paths) * iterations,
      elapsed_ms: elapsed / 1_000,
      us_per_operation: elapsed / (length(paths) * iterations),
      worker_ms: Enum.map(worker_us, &(&1 / 1_000))
    }
  end
end

FerricstoreBench.ReplayLMDBMetadata.run()
