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
    * `--release-cursor-interval N` - interval used by the child, default 500
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
  @default_release_cursor_interval 500
  @max_verify_batch_size 1_000
  @max_partial_output_bytes 65_536
  @max_recent_line_bytes 4_096
  @max_recent_lines 20

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

  @doc false
  def format_profile(profile) when is_map(profile) do
    profile
    |> Enum.sort_by(fn {phase, _duration_us} -> Atom.to_string(phase) end)
    |> Enum.map(fn {phase, duration_us} ->
      Atom.to_string(phase) <> ":" <> Integer.to_string(duration_us)
    end)
    |> Enum.join(",")
  end

  @doc false
  def with_child_cleanup(child, work, cleanup)
      when is_function(work, 1) and is_function(cleanup, 1) do
    try do
      work.(child)
    after
      cleanup.(child)
    end
  end

  @doc false
  def validate_marker_pid!(marker, expected_pid) when is_integer(expected_pid) do
    marker_pid = marker_pid!(marker)

    if marker_pid == expected_pid do
      marker_pid
    else
      Mix.raise("marker pid #{marker_pid} does not match child port pid #{expected_pid}")
    end
  end

  defp run_parent(opts) do
    Mix.shell().info("kill9 data_dir=#{opts.data_dir}")
    Mix.shell().info("kill9 writes=#{opts.writes} prefix=#{opts.prefix}")

    unless opts.keep_data do
      File.rm_rf!(opts.data_dir)
    end

    File.mkdir_p!(opts.data_dir)

    try do
      _write_marker =
        :writer
        |> start_child(opts)
        |> with_child_cleanup(
          fn writer ->
            {:ok, marker} = wait_for_marker(writer, "WRITE_DONE", opts.timeout_ms)
            writer_pid = validate_marker_pid!(marker, child_os_pid!(writer))

            Mix.shell().info(
              "writer ready pid=#{writer_pid} applied=#{marker["applied"]} released=#{marker["released"]} gap=#{marker["gap"]}"
            )

            kill9(writer_pid)
            wait_for_exit(writer, 10_000)
            Mix.shell().info("writer killed with SIGKILL")
            marker
          end,
          &terminate_child/1
        )

      verify_marker =
        :verifier
        |> start_child(opts)
        |> with_child_cleanup(
          fn verifier ->
            {:ok, marker} = wait_for_marker(verifier, "VERIFY_DONE", opts.timeout_ms)
            wait_for_exit(verifier, 10_000)
            marker
          end,
          &terminate_child/1
        )

      Mix.shell().info("verify ok")
      Mix.shell().info("recovery_time_ms=#{verify_marker["recovery_ms"]}")
      Mix.shell().info("recovery_profile_us=#{verify_marker["profile_us"]}")
      Mix.shell().info("dbsize=#{verify_marker["dbsize"]}")
      Mix.shell().info("data_dir=#{opts.data_dir}")
      :ok
    after
      unless opts.keep_data do
        File.rm_rf!(opts.data_dir)
      end
    end
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
    handler_id = attach_startup_profile()

    {recovery_us, :ok} =
      :timer.tc(fn ->
        start_ferricstore!(opts.timeout_ms)
      end)

    profile = collect_startup_profile(handler_id)
    ctx = FerricStore.Instance.get(:default)

    verify_dataset!(opts, fn keys ->
      Router.batch_get(ctx, keys)
    end)

    marker(
      event: "VERIFY_DONE",
      pid: :os.getpid(),
      writes: opts.writes,
      recovery_ms: div(recovery_us, 1000),
      profile_us: format_profile(profile),
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

  defp attach_startup_profile do
    {:ok, _} = Application.ensure_all_started(:telemetry)
    owner = self()
    handler_id = "ferricstore-kill9-profile-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :shard, :startup_phase],
      &__MODULE__.handle_startup_profile_event/4,
      owner
    )

    handler_id
  end

  @doc false
  def handle_startup_profile_event(
        _event,
        %{duration_us: duration_us},
        %{phase: phase},
        owner
      ) do
    send(owner, {:startup_profile, phase, duration_us})
  end

  defp collect_startup_profile(handler_id) do
    :telemetry.detach(handler_id)
    drain_startup_profile(%{})
  end

  defp drain_startup_profile(acc) do
    receive do
      {:startup_profile, phase, duration_us}
      when is_atom(phase) and is_integer(duration_us) ->
        drain_startup_profile(Map.update(acc, phase, duration_us, &(&1 + duration_us)))
    after
      0 -> acc
    end
  end

  @doc false
  def verify_dataset!(
        %{writes: writes, batch_size: batch_size, prefix: prefix},
        fetch_batch
      )
      when is_integer(writes) and writes > 0 and is_integer(batch_size) and batch_size > 0 and
             is_binary(prefix) and is_function(fetch_batch, 1) do
    verify_batch_size = min(batch_size, @max_verify_batch_size)

    1..writes
    |> Enum.chunk_every(verify_batch_size)
    |> Enum.each(fn indexes ->
      keys = Enum.map(indexes, &key(prefix, &1))

      case fetch_batch.(keys) do
        values when is_list(values) -> verify_batch!(indexes, values)
        other -> Mix.raise("batch read failed during verification: #{inspect(other)}")
      end
    end)

    :ok
  end

  defp verify_batch!([index | indexes], [actual | values]) do
    expected = value(index)

    if actual == expected do
      verify_batch!(indexes, values)
    else
      Mix.raise("key #{index} expected #{inspect(expected)}, got #{inspect(actual)}")
    end
  end

  defp verify_batch!([], []), do: :ok

  defp verify_batch!(indexes, values) do
    Mix.raise(
      "batch read returned wrong result count: expected #{length(indexes)}, got #{length(values)}"
    )
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
        {buffer, recent, marker} = consume_output(buffer, data, recent, event)

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

  @doc false
  def consume_output(buffer, data, recent, event)
      when is_binary(buffer) and is_binary(data) and is_list(recent) and is_binary(event) do
    parts = :binary.split(buffer <> data, "\n", [:global])
    {lines, [tail]} = Enum.split(parts, -1)

    {_, recent, marker} =
      Enum.reduce(lines, {nil, recent, nil}, fn line, {_tail, recent, found} ->
        recent_line = line |> keep_suffix(@max_recent_line_bytes) |> String.trim()
        recent = keep_recent([recent_line | recent])

        marker =
          case {found, parse_bounded_marker(line)} do
            {nil, {:ok, %{"event" => ^event} = marker}} -> marker
            {current, _} -> current
          end

        {nil, recent, marker}
      end)

    {keep_suffix(tail, @max_partial_output_bytes), recent, marker}
  end

  defp parse_bounded_marker(line) when byte_size(line) <= @max_recent_line_bytes,
    do: parse_marker(line)

  defp parse_bounded_marker(_line), do: :ignore

  defp keep_recent(lines), do: Enum.take(lines, @max_recent_lines)

  defp keep_suffix(binary, limit) when byte_size(binary) <= limit, do: binary

  defp keep_suffix(binary, limit) do
    binary_part(binary, byte_size(binary) - limit, limit)
  end

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

  defp child_os_pid!(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} when is_integer(pid) and pid > 0 -> pid
      _ -> Mix.raise("child port closed before reporting its OS pid")
    end
  end

  defp terminate_child(port) when is_port(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} when is_integer(pid) and pid > 0 ->
        _ = System.cmd("kill", ["-9", Integer.to_string(pid)], stderr_to_stdout: true)

      _closed ->
        :ok
    end

    try do
      Port.close(port)
    catch
      :error, :badarg -> :ok
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
