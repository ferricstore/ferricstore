defmodule Ferricstore.Commands.Namespace do
  @moduledoc """
  Handles the `FERRICSTORE.CONFIG` command for namespace-aware configuration.

  Provides three subcommands for managing per-namespace commit window timing:

  ## Supported commands

    * `FERRICSTORE.CONFIG SET prefix field value` -- sets a namespace config field
    * `FERRICSTORE.CONFIG GET [prefix]` -- returns config for one or all namespaces
    * `FERRICSTORE.CONFIG RESET [prefix]` -- resets one or all namespaces to defaults

  ## Fields

    * `window_ms` -- commit window in milliseconds (positive integer)
  Durability is not configurable; writes always use quorum.

  ## Examples

      FERRICSTORE.CONFIG SET rate window_ms 10
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

  def handle("FERRICSTORE.CONFIG", args, _store), do: handle_config(args, "")

  # ---------------------------------------------------------------------------
  # 4-arity variant: accepts conn_state for audit trail (changed_by)
  # ---------------------------------------------------------------------------

  @doc """
  4-arity variant that accepts `conn_state` for audit trail (changed_by).

  When `conn_state` contains a `:client_id`, it is recorded as `"client:<id>"`
  in the audit trail.
  """
  @spec handle(binary(), [binary()], map(), map()) :: term()
  def handle("FERRICSTORE.CONFIG", args, _store, conn_state) do
    changed_by =
      case Map.get(conn_state, :client_id) do
        nil -> ""
        id -> "client:#{id}"
      end

    handle_config(args, changed_by)
  end

  def handle(cmd, args, store, _conn_state) do
    handle(cmd, args, store)
  end

  # ---------------------------------------------------------------------------
  # Private -- formatting
  # ---------------------------------------------------------------------------

  defp format_entry(%{prefix: prefix, window_ms: window_ms} = entry) do
    changed_at = Map.get(entry, :changed_at, 0)
    changed_by = Map.get(entry, :changed_by, "")

    [
      "prefix",
      prefix,
      "window_ms",
      Integer.to_string(window_ms),
      "changed_at",
      Integer.to_string(changed_at),
      "changed_by",
      changed_by
    ]
  end

  defp handle_config([], _changed_by) do
    {:error, "ERR wrong number of arguments for 'ferricstore.config' command"}
  end

  defp handle_config([subcmd | rest], changed_by) do
    case String.upcase(subcmd) do
      "SET" ->
        case rest do
          [prefix, field, value] ->
            NamespaceConfig.set(prefix, String.downcase(field), value, changed_by)

          _ ->
            {:error, "ERR wrong number of arguments for 'ferricstore.config set' command"}
        end

      "GET" ->
        case rest do
          [prefix] ->
            {:ok, entry} = NamespaceConfig.get(prefix)
            format_entry(entry)

          [] ->
            NamespaceConfig.get_all()
            |> Enum.flat_map(&format_entry/1)

          _ ->
            {:error, "ERR wrong number of arguments for 'ferricstore.config get' command"}
        end

      "RESET" ->
        case rest do
          [prefix] ->
            NamespaceConfig.reset(prefix)
            :ok

          [] ->
            NamespaceConfig.reset_all()
            :ok

          _ ->
            {:error, "ERR wrong number of arguments for 'ferricstore.config reset' command"}
        end

      _ ->
        {:error,
         "ERR unknown subcommand '#{String.downcase(subcmd)}' for 'ferricstore.config' command. " <>
           "Try SET, GET, or RESET."}
    end
  end
end
