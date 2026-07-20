defmodule Ferricstore.SlowLog do
  @moduledoc """
  ETS-backed ring buffer that records commands whose execution time exceeds a
  configurable threshold.

  Mirrors the Redis SLOWLOG facility: each entry captures a monotonically
  increasing ID, a Unix timestamp (microseconds), the wall-clock duration
  (microseconds), and the command with its arguments.

  ## Configuration (application env)

    * `:slowlog_log_slower_than_us` -- threshold in microseconds; commands
      taking longer than this are logged. Default: `10_000` (10 ms).
      Set to `0` to log every command, or `-1` to disable.
    * `:slowlog_max_len` -- maximum number of entries kept in the ring buffer.
      Default: `128`. When full, the oldest entry is evicted.

  ## Ownership

  This module is a GenServer that owns the ETS table
  `:ferricstore_slowlog`. It must be started in the application supervision
  tree before any command dispatch can call `maybe_log/3`.
  """

  use GenServer

  @table :ferricstore_slowlog
  @default_threshold_us 10_000
  @default_max_len 128
  @max_stored_args 32
  @max_stored_arg_bytes 128
  @redacted "[redacted]"
  @arguments_omitted "[more arguments omitted]"

  # -------------------------------------------------------------------------
  # Types
  # -------------------------------------------------------------------------

  @typedoc "A single slow log entry."
  @type entry ::
          {id :: non_neg_integer(), timestamp_us :: integer(), duration_us :: non_neg_integer(),
           command :: [binary()]}

  # -------------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------------

  @doc """
  Starts the SlowLog GenServer and creates the backing ETS table.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Records a command if its duration exceeds the configured threshold.

  This function is designed to be called from the hot dispatch path.
  When the threshold is `-1` (disabled), this is a no-op.

  ## Parameters

    - `command` -- list of binaries, e.g. `["SET", "key", "value"]`
    - `duration_us` -- execution time in microseconds
    - `_metadata` -- reserved for future use (client address, etc.)
  """
  @spec maybe_log([binary()], non_neg_integer(), term()) :: :ok
  def maybe_log(command, duration_us, _metadata \\ nil) do
    threshold = threshold()

    if threshold >= 0 and duration_us > threshold do
      GenServer.cast(__MODULE__, {:log, sanitize_command(command), duration_us})
    end

    :ok
  end

  @doc """
  Returns the last `count` slow log entries, newest first.

  When `count` is `nil` or omitted, returns all entries up to `max_len`.
  """
  @spec get(non_neg_integer() | nil) :: [entry()]
  def get(count \\ nil) do
    limit = if is_nil(count), do: :all, else: count
    newest_entries(:ets.last(@table), limit, [])
  end

  @doc """
  Returns the number of entries currently in the slow log.
  """
  @spec len() :: non_neg_integer()
  def len do
    :ets.info(@table, :size)
  end

  @doc """
  Clears all entries from the slow log and resets the ID counter.
  """
  @spec reset() :: :ok
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @doc """
  Returns the configured threshold in microseconds.

  A value of `-1` means slow logging is disabled.

  Reads from `persistent_term` (~5ns) rather than `Application.get_env`
  (~100-200ns ETS lookup). The persistent_term is initialized at GenServer
  startup and updated whenever the threshold changes via `set_threshold/1`
  or CONFIG SET.
  """
  @spec threshold() :: integer()
  def threshold do
    case :persistent_term.get(:ferricstore_slowlog_threshold, @default_threshold_us) do
      value when is_integer(value) and value >= -1 -> value
      _invalid -> @default_threshold_us
    end
  end

  @doc """
  Updates the slowlog threshold in both Application env and persistent_term.

  Called by CONFIG SET and may be called from tests.
  """
  @spec set_threshold(integer()) :: :ok
  def set_threshold(value) when is_integer(value) do
    Application.put_env(:ferricstore, :slowlog_log_slower_than_us, value)
    :persistent_term.put(:ferricstore_slowlog_threshold, value)
    :ok
  end

  @doc """
  Returns the configured maximum number of entries.

  Reads from `persistent_term` (~5ns) rather than `Application.get_env`.
  """
  @spec max_len() :: non_neg_integer()
  def max_len do
    case :persistent_term.get(:ferricstore_slowlog_max_len, @default_max_len) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> @default_max_len
    end
  end

  @doc """
  Updates the slowlog max length in both Application env and persistent_term.

  Called by CONFIG SET and may be called from tests.
  """
  @spec set_max_len(non_neg_integer()) :: :ok
  def set_max_len(value) when is_integer(value) and value >= 0 do
    Application.put_env(:ferricstore, :slowlog_max_len, value)
    :persistent_term.put(:ferricstore_slowlog_max_len, value)

    if Process.whereis(__MODULE__) do
      GenServer.call(__MODULE__, :trim)
    end

    :ok
  end

  # -------------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:ordered_set, :public, :named_table])

    # Cache slowlog threshold and max_len in persistent_term for hot-path reads.
    # This runs once at startup; values can be updated via CONFIG SET which calls
    # update_threshold/1 and update_max_len/1.
    :persistent_term.put(
      :ferricstore_slowlog_threshold,
      Application.get_env(:ferricstore, :slowlog_log_slower_than_us, @default_threshold_us)
    )

    :persistent_term.put(
      :ferricstore_slowlog_max_len,
      Application.get_env(:ferricstore, :slowlog_max_len, @default_max_len)
    )

    {:ok, %{table: table, next_id: 0}}
  end

  @impl true
  def handle_cast({:log, command, duration_us}, state) do
    id = state.next_id
    timestamp_us = System.os_time(:microsecond)
    :ets.insert(@table, {id, timestamp_us, duration_us, command})

    # Evict oldest entries if we exceed max_len.
    state = %{state | next_id: id + 1}
    evict_if_needed(state)
    check_near_full(state)
    {:noreply, state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@table)
    {:reply, :ok, %{state | next_id: 0}}
  end

  def handle_call(:trim, _from, state) do
    evict_if_needed(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}

  # -------------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------------

  defp evict_if_needed(_state) do
    max = max_len()
    size = :ets.info(@table, :size)

    if size > max do
      delete_oldest(size - max)
    end
  end

  defp delete_oldest(remaining) when remaining <= 0, do: :ok

  defp delete_oldest(remaining) do
    case :ets.first(@table) do
      :"$end_of_table" ->
        :ok

      id ->
        :ets.delete(@table, id)
        delete_oldest(remaining - 1)
    end
  end

  defp newest_entries(:"$end_of_table", _remaining, acc), do: Enum.reverse(acc)
  defp newest_entries(_id, 0, acc), do: Enum.reverse(acc)

  defp newest_entries(id, remaining, acc) do
    previous_id = :ets.prev(@table, id)

    case :ets.lookup(@table, id) do
      [entry] -> newest_entries(previous_id, decrement_limit(remaining), [entry | acc])
      [] -> newest_entries(previous_id, remaining, acc)
    end
  end

  defp decrement_limit(:all), do: :all
  defp decrement_limit(remaining), do: remaining - 1

  defp sanitize_command([command | args]) when is_binary(command) do
    case {String.upcase(command), args} do
      {"AUTH", _args} ->
        [copy_arg(command), @redacted]

      {"FLOW.QUERY", _args} ->
        [copy_arg(command), @redacted]

      {"CONFIG", [subcommand, key, _value | _rest]}
      when is_binary(subcommand) and is_binary(key) ->
        if String.upcase(subcommand) == "SET" and Ferricstore.Config.sensitive_param?(key) do
          [copy_arg(command), copy_arg(subcommand), copy_arg(key), @redacted]
        else
          bound_command([command | args])
        end

      {"ACL", [subcommand, username | _rules]}
      when is_binary(subcommand) and is_binary(username) ->
        if String.upcase(subcommand) == "SETUSER" do
          [copy_arg(command), copy_arg(subcommand), copy_arg(username), @redacted]
        else
          bound_command([command | args])
        end

      _other ->
        bound_command([command | args])
    end
  end

  defp sanitize_command(command) when is_list(command), do: bound_command(command)

  defp bound_command(command) do
    take_bounded_args(command, @max_stored_args, [])
  end

  defp take_bounded_args([], _remaining, acc), do: Enum.reverse(acc)
  defp take_bounded_args([arg], 1, acc), do: Enum.reverse([copy_arg(arg) | acc])
  defp take_bounded_args([_arg | _rest], 1, acc), do: Enum.reverse([@arguments_omitted | acc])

  defp take_bounded_args([arg | rest], remaining, acc) do
    take_bounded_args(rest, remaining - 1, [copy_arg(arg) | acc])
  end

  defp copy_arg(arg) when is_binary(arg) and byte_size(arg) <= @max_stored_arg_bytes do
    :binary.copy(arg)
  end

  defp copy_arg(arg) when is_binary(arg) do
    omitted = byte_size(arg) - @max_stored_arg_bytes
    prefix = arg |> binary_part(0, @max_stored_arg_bytes) |> :binary.copy()
    prefix <> "...[#{omitted} more bytes]"
  end

  defp copy_arg(arg), do: arg |> inspect(limit: 8, printable_limit: 64) |> copy_arg()

  @near_full_threshold 0.90

  # Emits a telemetry event when the slowlog ring buffer is at or above 90%
  # of its capacity. Only fires after eviction has run, so `size` reflects
  # the post-eviction count.
  defp check_near_full(_state) do
    max = max_len()
    size = :ets.info(@table, :size)

    if max > 0 and size / max >= @near_full_threshold do
      :telemetry.execute(
        [:ferricstore, :slow_log, :near_full],
        %{size: size, max: max, ratio: size / max},
        %{}
      )
    end
  end
end
