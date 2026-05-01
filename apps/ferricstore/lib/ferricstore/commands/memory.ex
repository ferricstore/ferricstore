defmodule Ferricstore.Commands.Memory do
  alias Ferricstore.Store.Ops

  @moduledoc """
  Handles Redis MEMORY subcommands.

  Provides introspection into memory usage of keys and the overall BEAM VM.

  ## Supported subcommands

    * `MEMORY USAGE key [SAMPLES count]` -- estimate memory used by a key
    * `MEMORY DOCTOR` -- diagnostic string (stub)
    * `MEMORY STATS` -- memory statistics from `:erlang.memory/0`
    * `MEMORY HELP` -- list of subcommand help strings
    * `MEMORY PURGE` -- trigger garbage collection
    * `MEMORY MALLOC-STATS` -- allocator info (stub)
  """

  # Overhead estimate per key: ~64 bytes for the ETS tuple metadata +
  # ~32 bytes for the Bitcask keydir entry.
  @key_overhead_bytes 96

  @doc """
  Handles a MEMORY subcommand.

  ## Parameters

    - `subcmd` -- uppercased subcommand name (e.g. `"USAGE"`, `"DOCTOR"`)
    - `args` -- list of string arguments following the subcommand
    - `store` -- injected store map with `get`, `exists?` callbacks

  ## Returns

  Plain Elixir term suitable for RESP encoding.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(subcmd, args, store)

  # ---------------------------------------------------------------------------
  # MEMORY USAGE key [SAMPLES count]
  # ---------------------------------------------------------------------------

  def handle("USAGE", [key | _opts], store) do
    case Ops.value_size(store, key) do
      nil -> nil
      value_size -> @key_overhead_bytes + byte_size(key) + value_size
    end
  end

  def handle("USAGE", [], _store) do
    {:error, "ERR wrong number of arguments for 'memory|usage' command"}
  end

  # ---------------------------------------------------------------------------
  # MEMORY DOCTOR
  # ---------------------------------------------------------------------------

  def handle("DOCTOR", [], _store) do
    "Sam, I have no memory problems"
  end

  def handle("DOCTOR", _args, _store) do
    {:error, "ERR wrong number of arguments for 'memory|doctor' command"}
  end

  # ---------------------------------------------------------------------------
  # MEMORY STATS
  # ---------------------------------------------------------------------------

  def handle("STATS", [], _store) do
    mem = :erlang.memory()

    stats =
      Enum.flat_map(
        [
          {"peak.allocated", Keyword.get(mem, :total, 0)},
          {"total.allocated", Keyword.get(mem, :total, 0)},
          {"startup.allocated", 0},
          {"replication.backlog", 0},
          {"clients.slaves", 0},
          {"clients.normal", 0},
          {"aof.buffer", 0},
          {"keys.count", Keyword.get(mem, :ets, 0)},
          {"keys.bytes-per-key", 0},
          {"dataset.bytes", Keyword.get(mem, :binary, 0)},
          {"dataset.percentage", 0.0},
          {"peak.percentage", 100.0},
          {"allocator.allocated", Keyword.get(mem, :total, 0)},
          {"allocator.active", Keyword.get(mem, :total, 0)},
          {"allocator.resident", Keyword.get(mem, :total, 0)},
          {"process.memory", Keyword.get(mem, :processes, 0)},
          {"atom.memory", Keyword.get(mem, :atom, 0)},
          {"ets.memory", Keyword.get(mem, :ets, 0)},
          {"system.memory", Keyword.get(mem, :system, 0)}
        ],
        fn {k, v} -> [k, v] end
      )

    stats
  end

  def handle("STATS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'memory|stats' command"}
  end

  # ---------------------------------------------------------------------------
  # MEMORY HELP
  # ---------------------------------------------------------------------------

  def handle("HELP", [], _store) do
    [
      "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "DOCTOR",
      "    Return memory problems reports.",
      "HELP",
      "    Return subcommand help summary.",
      "MALLOC-STATS",
      "    Return internal statistics report from the memory allocator.",
      "PURGE",
      "    Ask the allocator to release memory.",
      "STATS",
      "    Return information about the memory usage of the server.",
      "USAGE <key> [SAMPLES <count>]",
      "    Return memory in bytes used by <key> and its value."
    ]
  end

  def handle("HELP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'memory|help' command"}
  end

  # ---------------------------------------------------------------------------
  # MEMORY PURGE
  # ---------------------------------------------------------------------------

  def handle("PURGE", [], _store) do
    :erlang.garbage_collect()
    :ok
  end

  def handle("PURGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'memory|purge' command"}
  end

  # ---------------------------------------------------------------------------
  # MEMORY MALLOC-STATS
  # ---------------------------------------------------------------------------

  def handle("MALLOC-STATS", [], _store) do
    "Memory allocator stats not available (BEAM VM)"
  end

  def handle("MALLOC-STATS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'memory|malloc-stats' command"}
  end

  # ---------------------------------------------------------------------------
  # Unknown subcommand
  # ---------------------------------------------------------------------------

  def handle(subcmd, _args, _store) do
    {:error, "ERR unknown subcommand '#{String.downcase(subcmd)}'. Try MEMORY HELP."}
  end
end
