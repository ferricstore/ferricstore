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
      name: "ferricstore.blobgc",
      arity: 1,
      flags: ["admin", "slow"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Runs conservative garbage collection for unreferenced large-value blobs."
    },
    %{
      name: "ferricstore.doctor",
      arity: -2,
      flags: ["admin", "slow"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Runs storage/projection diagnostics and safe background repair jobs."
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
      name: "flow.spawn_children",
      arity: -4,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Creates child workflow records and updates parent wait groups."
    },
    %{
      name: "flow.value.put",
      arity: -2,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Stores a reusable Flow value payload and returns its reference."
    },
    %{
      name: "flow.signal",
      arity: -4,
      flags: ["write", "denyoom"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Appends a signal event to a workflow and optionally transitions it."
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
      name: "flow.policy.set",
      arity: -2,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Configures default and state retry policies for a workflow type."
    },
    %{
      name: "flow.policy.get",
      arity: -2,
      flags: ["readonly", "fast"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Returns effective retry policy for a workflow type or state."
    },
    %{
      name: "flow.retention_cleanup",
      arity: -1,
      flags: ["write", "admin"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Runs bounded Flow retention cleanup."
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
      name: "flow.extend_lease",
      arity: -5,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Extends a running workflow lease."
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
      name: "flow.cancel_many",
      arity: -6,
      flags: ["write"],
      first_key: 1,
      last_key: 1,
      step: 1,
      summary: "Cancels workflow records, atomic per partition/shard group."
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
      name: "flow.failures",
      arity: -2,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Lists failed workflow records by type and optional time range."
    },
    %{
      name: "flow.terminals",
      arity: -2,
      flags: ["readonly"],
      first_key: 0,
      last_key: 0,
      step: 0,
      summary: "Lists terminal workflow records by type, state, and optional time range."
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
    upper_name = String.upcase(name)

    with :not_flow <- flow_dynamic_keys(upper_name, args) do
      case lookup(name) do
        {:ok, %{first_key: 0}} ->
          {:ok, []}

        {:ok, %{first_key: first, last_key: last, step: step}} ->
          extract_keys(first, last, step, args)

        :error ->
          {:error, "ERR Invalid command specified"}
      end
    end
  end

  @doc """
  Same as `get_keys/2` but accepts an already-uppercase command name,
  avoiding the `String.downcase/1` call inside `lookup/1`.
  """
  @spec get_keys_upper(binary(), [binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def get_keys_upper(name, args) when is_binary(name) and is_list(args) do
    with :not_flow <- flow_dynamic_keys(name, args) do
      case lookup_upper(name) do
        {:ok, %{first_key: 0}} ->
          {:ok, []}

        {:ok, %{first_key: first, last_key: last, step: step}} ->
          extract_keys(first, last, step, args)

        :error ->
          {:error, "ERR Invalid command specified"}
      end
    end
  end

  defp flow_dynamic_keys("FLOW.CREATE_MANY", args),
    do: {:ok, args_at(args, flow_create_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.SPAWN_CHILDREN", args),
    do: {:ok, args_at(args, flow_spawn_children_key_indices(args))}

  defp flow_dynamic_keys("FLOW.COMPLETE_MANY", args),
    do: {:ok, args_at(args, flow_complete_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.RETRY_MANY", args),
    do: {:ok, args_at(args, flow_complete_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.FAIL_MANY", args),
    do: {:ok, args_at(args, flow_complete_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.CANCEL_MANY", args),
    do: {:ok, args_at(args, flow_cancel_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.TRANSITION_MANY", args),
    do: {:ok, args_at(args, flow_transition_many_key_indices(args))}

  defp flow_dynamic_keys("FLOW.VALUE.PUT", args),
    do: {:ok, args_at(args, flow_partition_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.CREATE", "FLOW.GET", "FLOW.HISTORY", "FLOW.SIGNAL"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL", "FLOW.EXTEND_LEASE"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 2))}

  defp flow_dynamic_keys("FLOW.TRANSITION", args),
    do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 3))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.CANCEL", "FLOW.REWIND"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.BY_PARENT", "FLOW.BY_ROOT", "FLOW.BY_CORRELATION"],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in [
              "FLOW.CLAIM_DUE",
              "FLOW.RECLAIM",
              "FLOW.LIST",
              "FLOW.TERMINALS",
              "FLOW.FAILURES",
              "FLOW.INFO",
              "FLOW.STUCK"
            ],
       do: {:ok, args_at(args, flow_partition_or_first_key_indices(args, 1))}

  defp flow_dynamic_keys(name, args)
       when name in ["FLOW.POLICY.SET", "FLOW.POLICY.GET"],
       do: {:ok, Enum.take(args, 1)}

  defp flow_dynamic_keys(_name, _args), do: :not_flow

  defp flow_partition_or_first_key_indices([], _option_start), do: []

  defp flow_partition_or_first_key_indices(args, option_start) do
    case flow_partition_key_indices(args, option_start) do
      [] -> [0]
      indices -> dedup_indices(indices)
    end
  end

  defp flow_partition_key_indices(args, option_start) do
    flow_partition_key_indices_until(args, option_start, length(args))
  end

  defp flow_partition_key_indices_until(args, option_start, option_end) do
    option_end = min(option_end, length(args))

    option_start..max(option_start, option_end - 1)
    |> Enum.reduce([], fn idx, acc ->
      cond do
        idx + 1 < option_end and arg_eq?(Enum.at(args, idx), "PARTITION") ->
          [idx + 1 | acc]

        idx + 1 < option_end and arg_eq?(Enum.at(args, idx), "PARTITIONS") ->
          flow_partition_count_indices(args, idx + 1, option_end) ++ acc

        true ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  defp flow_partition_count_indices(args, count_idx, option_end) do
    with count_arg when not is_nil(count_arg) <- Enum.at(args, count_idx),
         {count, ""} when count > 0 <- Integer.parse(to_string(count_arg)),
         true <- count_idx + count < option_end do
      Enum.to_list((count_idx + 1)..(count_idx + count))
    else
      _ -> []
    end
  end

  defp flow_create_many_key_indices([]), do: []

  defp flow_create_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 1, "ITEMS") do
        nil ->
          [0]

        items_idx ->
          item_width =
            if flow_option_present_until?(args, 1, items_idx, "PAYLOAD_REF"), do: 2, else: 3

          repeated_item_partition_indices(args, items_idx + 1, item_width)
      end
    end
  end

  defp flow_option_present_until?(args, start, option_end, option) do
    start..max(start, option_end - 1)
    |> Enum.any?(fn idx ->
      rem(idx - start, 2) == 0 and idx < option_end and arg_eq?(Enum.at(args, idx), option)
    end)
  end

  defp flow_spawn_children_key_indices(args) do
    option_end = option_index(args, 1, "ITEMS") || length(args)
    partition_keys = flow_partition_key_indices_until(args, 1, option_end)

    partition_keys =
      if option_end + 1 < length(args) and arg_eq?(Enum.at(args, option_end + 1), "MIXED") do
        partition_keys ++ repeated_item_partition_indices(args, option_end + 2, 4)
      else
        partition_keys
      end

    case partition_keys do
      [] -> Enum.take(0..(length(args) - 1), min(length(args), 1))
      keys -> dedup_indices(keys)
    end
  end

  defp flow_transition_many_key_indices([]), do: []

  defp flow_transition_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 3, "ITEMS") do
        nil -> [0]
        items_idx -> repeated_item_partition_indices(args, items_idx + 1, 4)
      end
    end
  end

  defp flow_complete_many_key_indices([]), do: []

  defp flow_complete_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 1, "ITEMS") do
        nil -> [0]
        items_idx -> repeated_item_partition_indices(args, items_idx + 1, 4)
      end
    end
  end

  defp flow_cancel_many_key_indices([]), do: []

  defp flow_cancel_many_key_indices(args) do
    if not arg_eq?(hd(args), "MIXED") do
      [0]
    else
      case option_index(args, 1, "ITEMS") do
        nil -> [0]
        items_idx -> repeated_item_partition_indices(args, items_idx + 1, 3)
      end
    end
  end

  defp repeated_item_partition_indices(args, start_idx, step) do
    do_repeated_item_partition_indices(args, start_idx, step, [])
  end

  defp do_repeated_item_partition_indices(args, idx, step, acc) do
    if idx + step - 1 < length(args) do
      do_repeated_item_partition_indices(args, idx + step, step, [idx + 1 | acc])
    else
      Enum.reverse(acc)
    end
  end

  defp option_index(args, start, name), do: option_index(args, start, name, length(args))

  defp option_index(_args, idx, _name, len) when idx >= len, do: nil

  defp option_index(args, idx, name, len) do
    if arg_eq?(Enum.at(args, idx), name) do
      idx
    else
      option_index(args, idx + 2, name, len)
    end
  end

  defp dedup_indices(indices), do: Enum.uniq(indices)

  defp args_at(args, indices) do
    indices
    |> Enum.map(&Enum.at(args, &1))
    |> Enum.reject(&is_nil/1)
  end

  defp arg_eq?(arg, expected) when is_binary(arg) and is_binary(expected),
    do: ascii_eq_ignore_case?(arg, expected)

  defp arg_eq?(_arg, _expected), do: false

  defp ascii_eq_ignore_case?(left, right) when byte_size(left) != byte_size(right), do: false
  defp ascii_eq_ignore_case?(<<>>, <<>>), do: true

  defp ascii_eq_ignore_case?(<<left, left_rest::binary>>, <<right, right_rest::binary>>) do
    ascii_upper(left) == ascii_upper(right) and ascii_eq_ignore_case?(left_rest, right_rest)
  end

  defp ascii_upper(char) when char >= ?a and char <= ?z, do: char - 32
  defp ascii_upper(char), do: char

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
