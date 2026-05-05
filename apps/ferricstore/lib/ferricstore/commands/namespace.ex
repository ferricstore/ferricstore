defmodule Ferricstore.Commands.Namespace do
  @moduledoc """
  Handles the `FERRICSTORE.CONFIG` command for namespace-aware configuration.

  Provides three subcommands for managing per-namespace (prefix) settings
  such as commit window timing and durability mode:

  ## Supported commands

    * `FERRICSTORE.CONFIG SET prefix field value` -- sets a namespace config field
    * `FERRICSTORE.CONFIG GET [prefix]` -- returns config for one or all namespaces
    * `FERRICSTORE.CONFIG RESET [prefix]` -- resets one or all namespaces to defaults

  ## Fields

    * `window_ms` -- commit window in milliseconds (positive integer)
    * `durability` -- durability mode: `"quorum"`

  ## Examples

      FERRICSTORE.CONFIG SET rate window_ms 10
      FERRICSTORE.CONFIG SET ts durability quorum
      FERRICSTORE.CONFIG GET rate
      FERRICSTORE.CONFIG GET
      FERRICSTORE.CONFIG RESET rate
      FERRICSTORE.CONFIG RESET
  """

  alias Ferricstore.NamespaceConfig

  @doc """
  Handles a `FERRICSTORE.CONFIG` command.

  ## Parameters

    * `cmd` -- the full uppercased command name (`"FERRICSTORE.CONFIG"`)
    * `args` -- list of string arguments (subcommand + params)
    * `_store` -- injected store map (unused by namespace config commands)

  ## Returns

  Command-specific return values:
    * SET: `:ok` or `{:error, reason}`
    * GET: flat key-value list or `{:error, reason}`
    * RESET: `:ok`
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, _store)

  # ---------------------------------------------------------------------------
  # FERRICSTORE.CONFIG SET prefix field value
  # ---------------------------------------------------------------------------

  def handle("FERRICSTORE.CONFIG", ["SET", prefix, field, value], _store) do
    NamespaceConfig.set(prefix, String.downcase(field), value)
  end

  def handle("FERRICSTORE.CONFIG", ["SET" | _rest], _store) do
    {:error, "ERR wrong number of arguments for 'ferricstore.config set' command"}
  end

  # ---------------------------------------------------------------------------
  # FERRICSTORE.CONFIG GET [prefix]
  # ---------------------------------------------------------------------------

  def handle("FERRICSTORE.CONFIG", ["GET", prefix], _store) do
    {:ok, entry} = NamespaceConfig.get(prefix)
    format_entry(entry)
  end

  def handle("FERRICSTORE.CONFIG", ["GET"], _store) do
    entries = NamespaceConfig.get_all()
    Enum.flat_map(entries, &format_entry/1)
  end

  # ---------------------------------------------------------------------------
  # FERRICSTORE.CONFIG RESET [prefix]
  # ---------------------------------------------------------------------------

  def handle("FERRICSTORE.CONFIG", ["RESET", prefix], _store) do
    NamespaceConfig.reset(prefix)
    :ok
  end

  def handle("FERRICSTORE.CONFIG", ["RESET"], _store) do
    NamespaceConfig.reset_all()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Unknown subcommand / wrong args
  # ---------------------------------------------------------------------------

  def handle("FERRICSTORE.CONFIG", [subcmd | _rest], _store) do
    {:error,
     "ERR unknown subcommand '#{String.downcase(subcmd)}' for 'ferricstore.config' command. " <>
       "Try SET, GET, or RESET."}
  end

  def handle("FERRICSTORE.CONFIG", [], _store) do
    {:error, "ERR wrong number of arguments for 'ferricstore.config' command"}
  end

  # ---------------------------------------------------------------------------
  # 4-arity variant: accepts conn_state for audit trail (changed_by)
  # ---------------------------------------------------------------------------

  @doc """
  4-arity variant that accepts `conn_state` for audit trail (changed_by).

  When `conn_state` contains a `:client_id`, it is recorded as `"client:<id>"`
  in the audit trail.
  """
  @spec handle(binary(), [binary()], map(), map()) :: term()
  def handle("FERRICSTORE.CONFIG", ["SET", prefix, field, value], _store, conn_state) do
    changed_by =
      case Map.get(conn_state, :client_id) do
        nil -> ""
        id -> "client:#{id}"
      end

    NamespaceConfig.set(prefix, String.downcase(field), value, changed_by)
  end

  def handle(cmd, args, store, _conn_state) do
    handle(cmd, args, store)
  end

  # ---------------------------------------------------------------------------
  # Private -- formatting
  # ---------------------------------------------------------------------------

  defp format_entry(%{prefix: prefix, window_ms: window_ms, durability: durability} = entry) do
    changed_at = Map.get(entry, :changed_at, 0)
    changed_by = Map.get(entry, :changed_by, "")

    [
      "prefix",
      prefix,
      "window_ms",
      Integer.to_string(window_ms),
      "durability",
      Atom.to_string(durability),
      "changed_at",
      Integer.to_string(changed_at),
      "changed_by",
      changed_by
    ]
  end
end
