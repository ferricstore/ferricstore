defmodule Ferricstore.AuditLog do
  @moduledoc """
  ETS-backed ring buffer that records security-relevant events.

  Mirrors the SlowLog facility in design: a GenServer owns a named ETS table
  and exposes a simple public API for logging and querying audit events.

  ## Event types

    * `:auth_success`      -- successful AUTH command (username, client IP)
    * `:auth_failure`      -- failed AUTH attempt (username, client IP)
    * `:config_change`     -- CONFIG SET mutation (parameter, old/new values)
    * `:connection_open`   -- new client TCP connection (client IP)
    * `:connection_close`  -- client disconnection (client IP, duration)
    * `:dangerous_command` -- execution of FLUSHDB, FLUSHALL, or DEBUG
    * `:command_denied`    -- ACL command denial (username, command, client IP)

  ## Configuration (application env)

    * `:audit_log_enabled`     -- boolean, default `false`. When false, `log/2`
      is a no-op.
    * `:audit_log_max_entries` -- maximum entries in the ring buffer, default
      `128`. When full, the oldest entry is evicted.

  ## Ownership

  This module is a GenServer that owns the ETS table
  `:ferricstore_audit_log`. It must be started in the application supervision
  tree before any connection handler calls `log/2`.

  ## ACL LOG command

  The `ACL LOG` Redis command is wired to read from this audit log:

    * `ACL LOG`          -- returns all entries (up to max_entries)
    * `ACL LOG COUNT n`  -- returns the last `n` entries
    * `ACL LOG RESET`    -- clears all entries
  """

  use GenServer

  @table :ferricstore_audit_log
  @default_max_entries 128
  @max_detail_fields 32
  @max_collection_items 32
  @max_metadata_bytes 256

  # -------------------------------------------------------------------------
  # Types
  # -------------------------------------------------------------------------

  @typedoc "Supported audit event types."
  @type event_type ::
          :auth_success
          | :auth_failure
          | :config_change
          | :connection_open
          | :connection_close
          | :dangerous_command
          | :command_denied

  @typedoc "Details map attached to an audit event."
  @type details :: %{optional(atom()) => term()}

  @typedoc "A single audit log entry."
  @type entry ::
          {id :: non_neg_integer(), timestamp_us :: integer(), event_type(), details()}

  # -------------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------------

  @doc """
  Starts the AuditLog GenServer and creates the backing ETS table.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Records a security-relevant event in the audit log.

  When audit logging is disabled (`:audit_log_enabled` is `false`), this
  function is a no-op and returns `:ok` immediately.

  ## Parameters

    - `event_type` -- one of the supported event type atoms
    - `details`    -- a map of event-specific details

  ## Examples

      AuditLog.log(:auth_success, %{username: "default", client_ip: "127.0.0.1"})
      AuditLog.log(:dangerous_command, %{command: "FLUSHDB", args: []})
  """
  @spec log(event_type(), details()) :: :ok
  def log(event_type, details) when is_atom(event_type) and is_map(details) do
    if enabled?() do
      GenServer.cast(__MODULE__, {:log, event_type, sanitize_details(details, 0)})
    end

    :ok
  end

  def log(_event_type, _details), do: :ok

  @doc """
  Returns the last `count` audit log entries, newest first.

  When `count` is `nil` or omitted, returns all entries up to `max_entries`.
  """
  @spec get(non_neg_integer() | nil) :: [entry()]
  def get(count \\ nil) do
    limit = if is_nil(count), do: :all, else: count
    newest_entries(:ets.last(@table), limit, [])
  end

  @doc """
  Returns the number of entries currently in the audit log.
  """
  @spec len() :: non_neg_integer()
  def len do
    :ets.info(@table, :size)
  end

  @doc """
  Clears all entries from the audit log and resets the ID counter.
  """
  @spec reset() :: :ok
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @doc """
  Returns whether audit logging is currently enabled.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    Application.get_env(:ferricstore, :audit_log_enabled, false) == true
  end

  @doc """
  Returns the configured maximum number of entries.
  """
  @spec max_entries() :: non_neg_integer()
  def max_entries do
    case Application.get_env(:ferricstore, :audit_log_max_entries, @default_max_entries) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> @default_max_entries
    end
  end

  # -------------------------------------------------------------------------
  # Formatting for ACL LOG command
  # -------------------------------------------------------------------------

  @doc """
  Formats audit log entries into the list-of-maps structure returned by
  the `ACL LOG` command.

  Each entry is converted to a flat list of alternating key-value pairs,
  matching Redis ACL LOG output format:

      [id, timestamp, event_type_string, details_string, ...]
  """
  @spec format_entries([entry()]) :: [list()]
  def format_entries(entries) do
    Enum.map(entries, fn {id, timestamp_us, event_type, details} ->
      [
        id,
        div(timestamp_us, 1_000_000),
        Atom.to_string(event_type),
        format_details(details)
      ]
    end)
  end

  # -------------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:ordered_set, :public, :named_table])
    {:ok, %{table: table, next_id: 0}}
  end

  @impl true
  def handle_cast({:log, event_type, details}, state) do
    id = state.next_id
    timestamp_us = System.os_time(:microsecond)
    :ets.insert(@table, {id, timestamp_us, event_type, details})

    state = %{state | next_id: id + 1}
    evict_if_needed()
    {:noreply, state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(@table)
    {:reply, :ok, %{state | next_id: 0}}
  end

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}

  # -------------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------------

  defp evict_if_needed do
    max = max_entries()
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

  defp sanitize_details(details, depth) do
    details
    |> Enum.take(@max_detail_fields)
    |> Map.new(fn {key, value} -> {sanitize_key(key), sanitize_value(value, depth + 1)} end)
  end

  defp sanitize_key(key) when is_atom(key), do: key
  defp sanitize_key(key) when is_binary(key), do: copy_bounded_binary(key)
  defp sanitize_key(key), do: key |> bounded_inspect() |> copy_bounded_binary()

  defp sanitize_value(value, _depth) when is_binary(value), do: copy_bounded_binary(value)

  defp sanitize_value(value, _depth)
       when is_atom(value) or is_integer(value) or is_float(value),
       do: value

  defp sanitize_value(value, depth) when is_list(value) and depth < 3 do
    value
    |> Enum.take(@max_collection_items)
    |> Enum.map(&sanitize_value(&1, depth + 1))
  end

  defp sanitize_value(value, depth) when is_map(value) and depth < 3,
    do: sanitize_details(value, depth)

  defp sanitize_value(value, _depth), do: value |> bounded_inspect() |> copy_bounded_binary()

  defp bounded_inspect(value) do
    inspect(value,
      limit: @max_collection_items,
      printable_limit: @max_metadata_bytes,
      charlists: :as_lists
    )
  end

  defp copy_bounded_binary(value) when byte_size(value) <= @max_metadata_bytes,
    do: :binary.copy(value)

  defp copy_bounded_binary(value) do
    omitted = byte_size(value) - @max_metadata_bytes
    prefix = value |> binary_part(0, @max_metadata_bytes) |> :binary.copy()
    prefix <> "...[#{omitted} more bytes]"
  end

  defp format_details(details) when map_size(details) == 0, do: ""

  defp format_details(details) do
    Enum.map_join(details, " ", fn {k, v} -> "#{k}=#{inspect(v)}" end)
  end
end
