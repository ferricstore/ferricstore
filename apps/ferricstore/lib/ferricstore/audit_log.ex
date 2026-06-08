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
  def log(event_type, details) do
    if enabled?() do
      GenServer.cast(__MODULE__, {:log, event_type, details})
    end

    :ok
  end

  @doc """
  Returns the last `count` audit log entries, newest first.

  When `count` is `nil` or omitted, returns all entries up to `max_entries`.
  """
  @spec get(non_neg_integer() | nil) :: [entry()]
  def get(count \\ nil) do
    entries =
      @table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {id, _, _, _} -> id end, :desc)

    case count do
      nil -> entries
      n when is_integer(n) and n >= 0 -> Enum.take(entries, n)
    end
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
    Application.get_env(:ferricstore, :audit_log_enabled, false)
  end

  @doc """
  Returns the configured maximum number of entries.
  """
  @spec max_entries() :: pos_integer()
  def max_entries do
    Application.get_env(:ferricstore, :audit_log_max_entries, 128)
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
    table = :ets.new(@table, [:set, :public, :named_table])
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
      to_remove = size - max

      @table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {id, _, _, _} -> id end)
      |> Enum.take(to_remove)
      |> Enum.each(fn {id, _, _, _} -> :ets.delete(@table, id) end)
    end
  end

  defp format_details(details) when map_size(details) == 0, do: ""

  defp format_details(details) do
    Enum.map_join(details, " ", fn {k, v} -> "#{k}=#{inspect(v)}" end)
  end
end
