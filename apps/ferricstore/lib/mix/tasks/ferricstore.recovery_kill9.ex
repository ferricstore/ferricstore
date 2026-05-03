defmodule Mix.Tasks.Ferricstore.RecoveryKill9 do
  @moduledoc """
  Runs a manual kill-9 recovery benchmark in a separate BEAM OS process.

  The parent task starts a child `mix ferricstore.recovery_kill9` process in
  writer mode, waits until it has written a dataset, sends SIGKILL to that OS
  process, then starts a second child process in verifier mode against the same
  data directory and measures startup/readiness.

  This is intentionally a manual benchmark, not part of the normal test suite:

      mix ferricstore.recovery_kill9 --writes 2000

  Options:

    * `--writes N` - number of keys to write, default 2000
    * `--batch-size N` - keys per quorum batch during setup, default 1000
    * `--data-dir PATH` - data directory, default is a temp directory
    * `--timeout-ms N` - child marker timeout, default 120000
    * `--release-cursor-interval N` - interval used by the child, default 20000
    * `--prefix PREFIX` - key prefix, default unique kill9 prefix
    * `--keep-data` - keep the data directory after the run
  """

  use Mix.Task

  alias Ferricstore.Store.Router

  @shortdoc "Manual kill-9 recovery benchmark"
  @marker "FERRICSTORE_KILL9"
  @default_writes 2_000
  @default_batch_size 1_000
  @default_timeout_ms 120_000
  @default_release_cursor_interval 20_000

  @impl Mix.Task
  def run(args) do
    case System.get_env("FERRICSTORE_KILL9_CHILD") do
      "writer" -> run_writer_child(opts_from_env())
      "verifier" -> run_verifier_child(opts_from_env())
      _ -> run_parent(parse_args!(args))
    end
  end

  @doc false
  def parse_args!(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          writes: :integer,
          batch_size: :integer,
          data_dir: :string,
          timeout_ms: :integer,
          release_cursor_interval: :integer,
          prefix: :string,
          keep_data: :boolean
        ],
        aliases: [w: :writes]
      )

    if rest != [] or invalid != [] do
      Mix.raise("invalid arguments: #{inspect(rest ++ invalid)}")
    end

    unique = System.unique_integer([:positive])

    %{
      writes: positive_int!(Keyword.get(opts, :writes, @default_writes), "--writes"),
      batch_size:
        positive_int!(Keyword.get(opts, :batch_size, @default_batch_size), "--batch-size"),
      data_dir:
        Keyword.get(
          opts,
          :data_dir,
          Path.join(System.tmp_dir!(), "ferricstore_kill9_#{unique}")
        ),
      timeout_ms:
        positive_int!(Keyword.get(opts, :timeout_ms, @default_timeout_ms), "--timeout-ms"),
      release_cursor_interval:
        positive_int!(
          Keyword.get(opts, :release_cursor_interval, @default_release_cursor_interval),
          "--release-cursor-interval"
        ),
      prefix: Keyword.get(opts, :prefix, "kill9_#{unique}"),
      keep_data: Keyword.get(opts, :keep_data, false)
    }
  end

  @doc false
  def child_env(mode, opts) when mode in [:writer, :verifier] do
    [
      {"FERRICSTORE_KILL9_CHILD", Atom.to_string(mode)},
      {"FERRICSTORE_KILL9_DATA_DIR", opts.data_dir},
      {"FERRICSTORE_KILL9_WRITES", Integer.to_string(opts.writes)},
      {"FERRICSTORE_KILL9_BATCH_SIZE", Integer.to_string(opts.batch_size)},
      {"FERRICSTORE_KILL9_TIMEOUT_MS", Integer.to_string(opts.timeout_ms)},
      {"FERRICSTORE_KILL9_PREFIX", opts.prefix},
      {"FERRICSTORE_KILL9_RELEASE_CURSOR_INTERVAL",
       Integer.to_string(opts.release_cursor_interval)}
    ]
  end

  @doc false
  def child_args(mode, opts) when mode in [:writer, :verifier] do
    mix = System.find_executable("mix") || Mix.raise("could not find mix executable")

    env_args =
      Enum.map(child_env(mode, opts), fn {key, value} ->
        key <> "=" <> value
      end)

    env_args ++ [mix, "ferricstore.recovery_kill9"]
  end

  @doc false
  def parse_marker(line) when is_binary(line) do
    case String.split(String.trim(line)) do
      [@marker | pairs] ->
        marker =
          Enum.reduce(pairs, %{}, fn pair, acc ->
            case String.split(pair, "=", parts: 2) do
              [key, value] -> Map.put(acc, key, value)
              _ -> acc
            end
          end)

        {:ok, marker}

      _ ->
        :ignore
    end
  end

  defp run_parent(opts) do
    Mix.shell().info("kill9 data_dir=#{opts.data_dir}")
    Mix.shell().info("kill9 writes=#{opts.writes} prefix=#{opts.prefix}")

    unless opts.keep_data do
      File.rm_rf!(opts.data_dir)
    end

    File.mkdir_p!(opts.data_dir)

    writer = start_child(:writer, opts)
    {:ok, write_marker} = wait_for_marker(writer, "WRITE_DONE", opts.timeout_ms)
    writer_pid = marker_pid!(write_marker)

    Mix.shell().info(
      "writer ready pid=#{writer_pid} applied=#{write_marker["applied"]} released=#{write_marker["released"]} gap=#{write_marker["gap"]}"
    )

    kill9(writer_pid)
    wait_for_exit(writer, 10_000)
    Mix.shell().info("writer killed with SIGKILL")

    verifier = start_child(:verifier, opts)
    {:ok, verify_marker} = wait_for_marker(verifier, "VERIFY_DONE", opts.timeout_ms)
    wait_for_exit(verifier, 10_000)

    Mix.shell().info("verify ok")
    Mix.shell().info("recovery_time_ms=#{verify_marker["recovery_ms"]}")
    Mix.shell().info("dbsize=#{verify_marker["dbsize"]}")
    Mix.shell().info("data_dir=#{opts.data_dir}")

    unless opts.keep_data do
      File.rm_rf!(opts.data_dir)
    end

    :ok
  end

  defp run_writer_child(opts) do
    configure_child!(opts)
    {startup_us, :ok} = :timer.tc(fn -> start_ferricstore!(opts.timeout_ms) end)
    ctx = FerricStore.Instance.get(:default)

    {write_us, :ok} =
      :timer.tc(fn ->
        write_dataset!(ctx, opts)
      end)

    applied = max_atomic(ctx, :last_applied_index)
    released = max_atomic(ctx, :last_released_cursor_index)
    gap = max(applied - released, 0)

    marker(
      event: "WRITE_DONE",
      pid: :os.getpid(),
      writes: opts.writes,
      batch_size: opts.batch_size,
      startup_ms: div(startup_us, 1000),
      write_ms: div(write_us, 1000),
      applied: applied,
      released: released,
      gap: gap
    )

    Process.sleep(:infinity)
  end

  defp run_verifier_child(opts) do
    configure_child!(opts)

    {recovery_us, :ok} =
      :timer.tc(fn ->
        start_ferricstore!(opts.timeout_ms)
      end)

    ctx = FerricStore.Instance.get(:default)
    verify_samples!(ctx, opts)

    marker(
      event: "VERIFY_DONE",
      pid: :os.getpid(),
      writes: opts.writes,
      recovery_ms: div(recovery_us, 1000),
      dbsize: Router.dbsize(ctx)
    )
  end

  defp opts_from_env do
    %{
      data_dir: fetch_env!("FERRICSTORE_KILL9_DATA_DIR"),
      writes: env_int!("FERRICSTORE_KILL9_WRITES"),
      batch_size: env_int!("FERRICSTORE_KILL9_BATCH_SIZE"),
      timeout_ms: env_int!("FERRICSTORE_KILL9_TIMEOUT_MS"),
      prefix: fetch_env!("FERRICSTORE_KILL9_PREFIX"),
      release_cursor_interval: env_int!("FERRICSTORE_KILL9_RELEASE_CURSOR_INTERVAL"),
      keep_data: true
    }
  end

  defp configure_child!(opts) do
    Application.put_env(:ferricstore, :data_dir, opts.data_dir)
    Application.put_env(:ferricstore, :release_cursor_interval, opts.release_cursor_interval)
  end

  defp start_ferricstore!(timeout_ms) do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    FerricStore.await_ready(timeout: timeout_ms)
  end

  defp verify_samples!(ctx, opts) do
    samples =
      [1, div(opts.writes + 1, 2), opts.writes]
      |> Enum.uniq()

    Enum.each(samples, fn i ->
      expected = value(i)

      case Router.get(ctx, key(opts.prefix, i)) do
        ^expected -> :ok
        other -> Mix.raise("key #{i} expected #{inspect(expected)}, got #{inspect(other)}")
      end
    end)
  end

  defp write_dataset!(ctx, opts) do
    1..opts.writes
    |> Enum.chunk_every(opts.batch_size)
    |> Enum.each(fn indexes ->
      kv_pairs = Enum.map(indexes, fn i -> {key(opts.prefix, i), value(i)} end)

      case Router.batch_quorum_put(ctx, kv_pairs) do
        results when is_list(results) ->
          case Enum.find(results, &(&1 != :ok)) do
            nil -> :ok
            error -> Mix.raise("batch_quorum_put failed: #{inspect(error)}")
          end

        other ->
          Mix.raise("batch_quorum_put returned unexpected result: #{inspect(other)}")
      end
    end)
  end

  defp start_child(mode, opts) do
    env = System.find_executable("env") || Mix.raise("could not find env executable")

    Port.open({:spawn_executable, env}, [
      :binary,
      :exit_status,
      args: child_args(mode, opts)
    ])
  end

  defp wait_for_marker(port, event, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_marker(port, event, "", [], deadline)
  end

  defp do_wait_for_marker(port, event, buffer, recent, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, data}} ->
        {buffer, recent, marker} = consume_lines(buffer <> data, recent, event)

        case marker do
          nil -> do_wait_for_marker(port, event, buffer, recent, deadline)
          marker -> {:ok, marker}
        end

      {^port, {:exit_status, status}} ->
        Mix.raise("child exited before #{event}: status=#{status}, recent=#{inspect(recent)}")
    after
      timeout ->
        Mix.raise("timed out waiting for #{event}; recent=#{inspect(recent)}")
    end
  end

  defp consume_lines(buffer, recent, event) do
    parts = String.split(buffer, "\n")
    {lines, [tail]} = Enum.split(parts, -1)

    Enum.reduce(lines, {tail, recent, nil}, fn line, {tail, recent, found} ->
      recent = keep_recent([String.trim(line) | recent])

      marker =
        case {found, parse_marker(line)} do
          {nil, {:ok, %{"event" => ^event} = marker}} -> marker
          {current, _} -> current
        end

      {tail, recent, marker}
    end)
  end

  defp keep_recent(lines), do: Enum.take(lines, 20)

  defp marker_pid!(%{"pid" => pid}) do
    case Integer.parse(pid) do
      {int, ""} when int > 0 -> int
      _ -> Mix.raise("invalid child pid marker: #{inspect(pid)}")
    end
  end

  defp marker_pid!(marker), do: Mix.raise("missing child pid marker: #{inspect(marker)}")

  defp kill9(pid) do
    case System.cmd("kill", ["-9", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {out, status} -> Mix.raise("kill -9 #{pid} failed status=#{status}: #{out}")
    end
  end

  defp wait_for_exit(port, timeout_ms) do
    receive do
      {^port, {:exit_status, _status}} -> :ok
    after
      timeout_ms -> Mix.raise("timed out waiting for child exit")
    end
  end

  defp marker(fields) do
    encoded =
      fields
      |> Enum.map(fn {key, value} -> [Atom.to_string(key), "=", to_string(value)] end)
      |> Enum.intersperse(" ")

    IO.puts([@marker, " ", encoded])
  end

  defp max_atomic(ctx, field) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size

        1..size
        |> Enum.map(&:atomics.get(ref, &1))
        |> Enum.max(fn -> 0 end)

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp key(prefix, i), do: prefix <> ":" <> Integer.to_string(i)
  defp value(i), do: "v" <> Integer.to_string(i)

  defp fetch_env!(key), do: System.get_env(key) || Mix.raise("missing #{key}")

  defp env_int!(key) do
    key
    |> fetch_env!()
    |> String.to_integer()
    |> positive_int!(key)
  end

  defp positive_int!(value, _name) when is_integer(value) and value > 0, do: value
  defp positive_int!(_value, name), do: Mix.raise("#{name} must be a positive integer")
end
