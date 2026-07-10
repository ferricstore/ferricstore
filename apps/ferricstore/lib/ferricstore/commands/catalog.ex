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

  alias Ferricstore.Commands.Catalog.Entries
  alias Ferricstore.Commands.{Extension, KeyDiscovery}

  @doc "Returns the full list of command entries."
  @spec all() :: [command_entry()]
  def all, do: Entries.all() ++ extension_commands()

  @doc "Returns the number of supported commands."
  @spec count() :: non_neg_integer()
  def count, do: Entries.count() + length(extension_commands())

  @doc "Returns all command names as lowercase strings."
  @spec names() :: [binary()]
  def names, do: Entries.names() ++ Enum.map(extension_commands(), & &1.name)

  @doc """
  Looks up a command entry by name (case-insensitive).

  Returns `{:ok, entry}` or `:error`.
  """
  @spec lookup(binary()) :: {:ok, command_entry()} | :error
  def lookup(name) when is_binary(name) do
    case Entries.lookup(name) do
      {:ok, _} = ok -> ok
      :error -> Extension.lookup(name)
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
    case Entries.lookup_upper(name) do
      {:ok, _} = ok -> ok
      :error -> Extension.lookup_upper(name)
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
    get_keys_upper(String.upcase(name), args)
  end

  @doc """
  Same as `get_keys/2` but accepts an already-uppercase command name,
  avoiding the `String.downcase/1` call inside `lookup/1`.
  """
  @spec get_keys_upper(binary(), [binary()]) :: {:ok, [binary()]} | {:error, binary()}
  def get_keys_upper("FLOW." <> _rest = name, args) when is_list(args) do
    case Ferricstore.Commands.NativeAstParser.parse(name, args) do
      {:ok, ^name, _parsed_args, {:unknown, ^name, _unknown_args}, _keys} ->
        case Extension.keys(name, args) do
          {:ok, keys} -> {:ok, keys}
          :error -> {:error, "ERR Invalid command specified"}
        end

      {:ok, ^name, _parsed_args, _ast, keys} ->
        {:ok, keys}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_keys_upper(name, args) when is_binary(name) and is_list(args) do
    case KeyDiscovery.extract(name, args) do
      {:ok, keys} ->
        {:ok, keys}

      :not_dynamic ->
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

  defp extension_commands do
    Extension.non_shadowing_commands()
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
