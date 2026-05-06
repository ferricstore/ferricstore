defmodule Ferricstore.Commands.Catalog do
  @moduledoc """
  Central registry of all commands supported by FerricStore.

  Each entry contains the command name, arity (negative = variadic), flags,
  first-key / last-key / step indices, and a short summary string.

  This module is the single source of truth consumed by:

    * `COMMAND COUNT`
    * `COMMAND LIST`
    * `COMMAND INFO`
    * `COMMAND DOCS`
    * `COMMAND GETKEYS`

  ## Arity convention (Redis)

    * Positive N — command takes exactly N arguments (including the command name).
    * Negative N — command takes at least |N| arguments.

  ## Key position convention

    * `first_key: 0` means the command has no key arguments.
    * For single-key commands: `first_key: 1, last_key: 1, step: 1`.
    * For multi-key variadic commands: `last_key: -1, step: 1`.
  """

  @type command_entry :: %{
          name: binary(),
          arity: integer(),
          flags: [binary()],
          first_key: integer(),
          last_key: integer(),
          step: integer(),
          summary: binary()
        }

  @commands [
    # -- Strings -----------------------------------------------------------
    %{
      name: "get",
      arity: 2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the string value of a key."
    },
    %{
      name: "set",
      arity: -3,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Sets the string value of a key."
    },
    %{
      name: "del",
      arity: -2,
      flags: ["write"],
      first_key: 1,
      last_key: -1,
      step: 1,
      summary: "Deletes one or more keys."
    },
    %{
      name: "exists",
      arity: -2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: -1,
      step: 1,
      summary: "Determines whether one or more keys exist."
    },
    %{
      name: "mget",
      arity: -2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: -1,
      step: 1,
      summary: "Returns the values of one or more keys."
    },
    %{
      name: "mset",
      arity: -3,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: -1,
      step: 2,
      summary: "Atomically sets one or more key-value pairs."
    },

    # -- Expiry ------------------------------------------------------------
    %{
      name: "expire",
      arity: 3,
      flags: ["write", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Sets the expiration time of a key in seconds."
    },
    %{
      name: "pexpire",
      arity: 3,
      flags: ["write", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Sets the expiration time of a key in milliseconds."
    },
    %{
      name: "expireat",
      arity: 3,
      flags: ["write", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Sets the expiration time of a key to a Unix timestamp."
    },
    %{
      name: "pexpireat",
      arity: 3,
      flags: ["write", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Sets the expiration time of a key to a Unix timestamp in ms."
    },
    %{
      name: "ttl",
      arity: 2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the remaining time-to-live of a key in seconds."
    },
    %{
      name: "pttl",
      arity: 2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the remaining time-to-live of a key in milliseconds."
    },
    %{
      name: "persist",
      arity: 2,
      flags: ["write", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Removes the expiration from a key."
    },

    # -- Server ------------------------------------------------------------
    %{
      name: "ping",
      arity: -1,
      flags: ["fast", "stale"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Returns PONG or echoes the given message."
    },
    %{
      name: "echo",
      arity: 2,
      flags: ["fast"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Returns the given string."
    },
    %{
      name: "dbsize",
      arity: 1,
      flags: ["readonly", "fast"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Returns the number of keys in the database."
    },
    %{
      name: "keys",
      arity: 2,
      flags: ["readonly", "sort_for_script"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Returns all key names that match a pattern."
    },
    %{
      name: "flushdb",
      arity: -1,
      flags: ["write"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Removes all keys from the current database."
    },
    %{
      name: "flushall",
      arity: -1,
      flags: ["write"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Removes all keys from all databases."
    },
    %{
      name: "info",
      arity: -1,
      flags: ["stale", "fast"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Returns information and statistics about the server."
    },
    %{
      name: "select",
      arity: 2,
      flags: ["fast"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Not supported. Use named caches."
    },
    %{
      name: "lolwut",
      arity: -1,
      flags: ["fast"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Displays the FerricStore ASCII logo."
    },
    %{
      name: "debug",
      arity: -2,
      flags: ["admin"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Debugging command."
    },

    # -- COMMAND subcommands -----------------------------------------------
    %{
      name: "command",
      arity: -1,
      flags: ["random", "stale"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Returns information about all or specific commands."
    },

    # -- CLIENT subcommands ------------------------------------------------
    %{
      name: "client",
      arity: -2,
      flags: ["admin", "stale"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Manages client connections."
    },

    # -- Connection --------------------------------------------------------
    %{
      name: "hello",
      arity: -1,
      flags: ["fast", "stale"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Handshakes with the server."
    },
    %{
      name: "quit",
      arity: 1,
      flags: ["fast"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Closes the connection."
    },
    %{
      name: "reset",
      arity: 1,
      flags: ["fast", "stale"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Resets the connection."
    },

    # -- FerricStore-native -------------------------------------------------
    %{
      name: "ferricstore.config",
      arity: -2,
      flags: ["admin"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Manages per-namespace configuration (SET/GET/RESET)."
    },
    %{
      name: "ferricstore.key_info",
      arity: 2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns diagnostic metadata about a key (type, size, TTL, cache status, shard)."
    },

    # -- Flow --------------------------------------------------------------
    %{
      name: "flow.create",
      arity: -4,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Creates a workflow record."
    },
    %{
      name: "flow.create_many",
      arity: -7,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Creates workflow records, atomic per partition/shard group."
    },
    %{
      name: "flow.get",
      arity: -2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns a workflow record."
    },
    %{
      name: "flow.claim_due",
      arity: -4,
      flags: ["write"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Claims due workflow records for a worker."
    },
    %{
      name: "flow.reclaim",
      arity: -4,
      flags: ["write"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Reclaims expired running workflow leases for a worker."
    },
    %{
      name: "flow.complete",
      arity: -5,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Completes a leased workflow record."
    },
    %{
      name: "flow.complete_many",
      arity: -6,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Completes leased workflow records, atomic per partition/shard group."
    },
    %{
      name: "flow.retry_many",
      arity: -6,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Retries leased workflow records, atomic per partition/shard group."
    },
    %{
      name: "flow.fail_many",
      arity: -6,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Fails leased workflow records, atomic per partition/shard group."
    },
    %{
      name: "flow.transition",
      arity: -6,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Moves a workflow record between states."
    },
    %{
      name: "flow.transition_many",
      arity: -8,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Moves workflow records between states, atomic per partition/shard group."
    },
    %{
      name: "flow.retry",
      arity: -5,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Retries a leased workflow record."
    },
    %{
      name: "flow.fail",
      arity: -5,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Fails a leased workflow record."
    },
    %{
      name: "flow.cancel",
      arity: -4,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Cancels a workflow record."
    },
    %{
      name: "flow.rewind",
      arity: -4,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Rewinds a workflow record to a previous history event."
    },
    %{
      name: "flow.list",
      arity: -2,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Lists workflow records by type and optional state."
    },
    %{
      name: "flow.by_parent",
      arity: -2,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Lists workflow records by parent flow id."
    },
    %{
      name: "flow.by_root",
      arity: -2,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Lists workflow records by root flow id."
    },
    %{
      name: "flow.by_correlation",
      arity: -2,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Lists workflow records by correlation id."
    },
    %{
      name: "flow.info",
      arity: -2,
      flags: ["readonly", "fast"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Returns workflow counters for a type."
    },
    %{
      name: "flow.stuck",
      arity: -2,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Lists stale running workflow records."
    },
    %{
      name: "flow.history",
      arity: -2,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns workflow history events."
    },

    # -- T-Digest ---------------------------------------------------------------
    %{
      name: "tdigest.create",
      arity: -2,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Creates a new t-digest sketch."
    },
    %{
      name: "tdigest.add",
      arity: -3,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Adds one or more observations to a t-digest sketch."
    },
    %{
      name: "tdigest.reset",
      arity: 2,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Resets a t-digest sketch to empty, preserving compression."
    },
    %{
      name: "tdigest.quantile",
      arity: -3,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns estimated values at one or more quantile positions."
    },
    %{
      name: "tdigest.cdf",
      arity: -3,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the estimated CDF for one or more values."
    },
    %{
      name: "tdigest.rank",
      arity: -3,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the estimated rank of one or more values."
    },
    %{
      name: "tdigest.revrank",
      arity: -3,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the estimated reverse rank of one or more values."
    },
    %{
      name: "tdigest.byrank",
      arity: -3,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the estimated value at one or more rank positions."
    },
    %{
      name: "tdigest.byrevrank",
      arity: -3,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the estimated value at one or more reverse-rank positions."
    },
    %{
      name: "tdigest.trimmed_mean",
      arity: 4,
      flags: ["readonly"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the mean of observations between two quantile boundaries."
    },
    %{
      name: "tdigest.min",
      arity: 2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the minimum observed value."
    },
    %{
      name: "tdigest.max",
      arity: 2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns the maximum observed value."
    },
    %{
      name: "tdigest.info",
      arity: 2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns metadata about a t-digest sketch."
    },
    %{
      name: "tdigest.merge",
      arity: -4,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Merges one or more source t-digests into a destination."
    }
  ]

  @commands_by_name Map.new(@commands, fn cmd -> {cmd.name, cmd} end)

  # Uppercase lookup map: avoids String.downcase on hot path when caller
  # already has an uppercased command name (e.g. from normalise_cmd).
  @commands_by_upper Map.new(@commands, fn cmd -> {String.upcase(cmd.name), cmd} end)

  @doc "Returns the full list of command entries."
  @spec all() :: [command_entry()]
  def all, do: @commands

  @doc "Returns the number of supported commands."
  @spec count() :: non_neg_integer()
  def count, do: length(@commands)

  @doc "Returns all command names as lowercase strings."
  @spec names() :: [binary()]
  def names, do: Enum.map(@commands, & &1.name)

  @doc """
  Looks up a command entry by name (case-insensitive).

  Returns `{:ok, entry}` or `:error`.
  """
  @spec lookup(binary()) :: {:ok, command_entry()} | :error
  def lookup(name) when is_binary(name) do
    case Map.fetch(@commands_by_name, String.downcase(name)) do
      {:ok, _} = ok -> ok
      :error -> :error
    end
  end

  @doc """
  Looks up a command entry by an already-uppercase name.

  Avoids the `String.downcase/1` call in `lookup/1` when the caller
  already has an uppercase command name (e.g. from `normalise_cmd`).

  Returns `{:ok, entry}` or `:error`.
  """
  @spec lookup_upper(binary()) :: {:ok, command_entry()} | :error
  def lookup_upper(name) when is_binary(name) do
    case Map.fetch(@commands_by_upper, name) do
      {:ok, _} = ok -> ok
      :error -> :error
    end
  end

  @doc """
  Returns the Redis-style info tuple for a command entry.

  Format: `[name, arity, [flags...], first_key, last_key, step]`
  """
  @spec info_tuple(command_entry()) :: list()
  def info_tuple(%{} = cmd) do
    [cmd.name, cmd.arity, cmd.flags, cmd.first_key, cmd.last_key, cmd.step]
  end

  @doc """
  Extracts the key arguments from a command invocation.

  Given a command name and its arguments list, returns the arguments that are keys
  based on the catalog's first_key/last_key/step metadata.

  Returns `{:ok, keys}` or `{:error, reason}`.
  """
  @spec get_keys(binary(), [binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def get_keys(name, args) when is_binary(name) and is_list(args) do
    case lookup(name) do
      {:ok, %{first_key: 0}} ->
        {:ok, []}

      {:ok, %{first_key: first, last_key: last, step: step}} ->
        extract_keys(first, last, step, args)

      :error ->
        {:error, "ERR Invalid command specified"}
    end
  end

  @doc """
  Same as `get_keys/2` but accepts an already-uppercase command name,
  avoiding the `String.downcase/1` call inside `lookup/1`.
  """
  @spec get_keys_upper(binary(), [binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def get_keys_upper(name, args) when is_binary(name) and is_list(args) do
    case lookup_upper(name) do
      {:ok, %{first_key: 0}} ->
        {:ok, []}

      {:ok, %{first_key: first, last_key: last, step: step}} ->
        extract_keys(first, last, step, args)

      :error ->
        {:error, "ERR Invalid command specified"}
    end
  end

  # Shared key extraction logic.
  defp extract_keys(first, last, step, args) do
    # Arguments are 0-indexed in our list, but first_key is 1-indexed
    # (position 1 = first arg after the command name).
    first_idx = first - 1
    last_idx = if last == -1, do: length(args) - 1, else: last - 1
    step_val = max(step, 1)

    keys =
      first_idx..last_idx//step_val
      |> Enum.map(fn i -> Enum.at(args, i) end)
      |> Enum.reject(&is_nil/1)

    {:ok, keys}
  end
end
