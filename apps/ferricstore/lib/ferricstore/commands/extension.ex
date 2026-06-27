defmodule Ferricstore.Commands.Extension do
  @moduledoc """
  Behaviour and registry helpers for optional command providers.

  A provider is configured with:

      config :ferricstore, :command_extensions, [MyApp.Commands]

  Providers declare command metadata with `commands/0` and execute commands
  with `handle/3`. The core dispatcher uses this metadata for routing and key
  ACL extraction, while leaving command semantics in the provider module.
  """

  @type access :: :read | :write | :rw

  @type command_entry :: %{
          required(:name) => binary(),
          optional(:arity) => integer(),
          optional(:flags) => [binary()],
          optional(:first_key) => integer(),
          optional(:last_key) => integer(),
          optional(:step) => integer(),
          optional(:access) => access(),
          optional(:summary) => binary()
        }

  @callback commands() :: [command_entry()]
  @callback handle(binary(), [binary()], map()) :: term()

  alias Ferricstore.Commands.Catalog.Entries

  @default_entry %{
    arity: -1,
    flags: [],
    first_key: 0,
    last_key: 0,
    step: 0,
    access: :rw,
    summary: "Extension command."
  }

  @doc "Returns configured provider modules."
  @spec modules() :: [module()]
  def modules do
    case Application.get_env(:ferricstore, :command_extensions, []) do
      nil -> []
      module when is_atom(module) -> [module]
      modules when is_list(modules) -> Enum.filter(modules, &is_atom/1)
      _other -> []
    end
  end

  @doc "Returns normalized command metadata from all configured providers."
  @spec commands() :: [map()]
  def commands do
    modules()
    |> Enum.flat_map(&module_commands/1)
    |> deduplicate()
  end

  @doc "Returns configured command metadata excluding names already owned by built-in commands."
  @spec non_shadowing_commands() :: [map()]
  def non_shadowing_commands do
    Enum.reject(commands(), &builtin_command?/1)
  end

  @doc "Returns configured command names in uppercase form."
  @spec command_names_upper() :: MapSet.t(binary())
  def command_names_upper do
    commands()
    |> Enum.map(&String.upcase(&1.name))
    |> MapSet.new()
  end

  @doc "Returns uppercase configured command names excluding built-in shadows."
  @spec non_shadowing_command_names_upper() :: MapSet.t(binary())
  def non_shadowing_command_names_upper do
    non_shadowing_commands()
    |> Enum.map(&String.upcase(&1.name))
    |> MapSet.new()
  end

  @doc "Returns the access type for a non-shadowing configured command."
  @spec non_shadowing_command_access_type(binary()) :: access() | nil
  def non_shadowing_command_access_type(command) when is_binary(command) do
    upper = String.upcase(command)

    case Enum.find(non_shadowing_commands(), &(String.upcase(&1.name) == upper)) do
      %{access: access} -> access
      nil -> nil
    end
  end

  @doc "Looks up configured command metadata by command name."
  @spec lookup(binary()) :: {:ok, map()} | :error
  def lookup(command) when is_binary(command), do: lookup_upper(String.upcase(command))

  @doc "Looks up configured command metadata by an already-uppercase command name."
  @spec lookup_upper(binary()) :: {:ok, map()} | :error
  def lookup_upper(command) when is_binary(command) do
    upper = String.upcase(command)

    case Enum.find(commands(), &(String.upcase(&1.name) == upper)) do
      nil -> :error
      command -> {:ok, command}
    end
  end

  @doc "Returns true when a configured provider owns the command."
  @spec command?(binary()) :: boolean()
  def command?(command) when is_binary(command), do: lookup(command) != :error

  @doc "Builds the dispatcher AST for a configured command."
  @spec ast(binary(), [binary()]) :: {:extension_command, binary(), [binary()]}
  def ast(command, args), do: {:extension_command, String.upcase(command), args}

  @doc "Extracts key arguments for a configured command."
  @spec keys(binary(), [binary()]) :: {:ok, [binary()]} | :error
  def keys(command, args) when is_binary(command) and is_list(args) do
    case lookup(command) do
      {:ok, %{first_key: 0}} ->
        {:ok, []}

      {:ok, %{first_key: first, last_key: last, step: step}} ->
        {:ok, extract_keys(first, last, step, args)}

      :error ->
        :error
    end
  end

  @doc "Returns the key access type for a configured command."
  @spec command_access_type(binary()) :: access() | nil
  def command_access_type(command) when is_binary(command) do
    case lookup(command) do
      {:ok, %{access: access}} -> access
      :error -> nil
    end
  end

  @doc "Dispatches a configured command to its provider module."
  @spec handle(binary(), [binary()], map()) :: term() | :not_found
  def handle(command, args, store) when is_binary(command) and is_list(args) do
    upper = String.upcase(command)

    case handler_module(upper) do
      {:ok, module} -> module.handle(upper, args, store)
      :error -> :not_found
    end
  end

  def handle(_command, _args, _store), do: :not_found

  defp module_commands(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :commands, 0) do
      module.commands()
      |> List.wrap()
      |> Enum.flat_map(&normalize_entry/1)
    else
      []
    end
  end

  defp normalize_entry(%{} = entry) do
    case entry_value(entry, :name) do
      name when is_binary(name) and name != "" ->
        upper = String.upcase(name)
        flags = normalize_flags(entry_value(entry, :flags, @default_entry.flags))

        [
          %{
            name: String.downcase(upper),
            arity: normalize_int(entry_value(entry, :arity, @default_entry.arity)),
            flags: flags,
            first_key: normalize_int(entry_value(entry, :first_key, @default_entry.first_key)),
            last_key: normalize_int(entry_value(entry, :last_key, @default_entry.last_key)),
            step: normalize_int(entry_value(entry, :step, @default_entry.step)),
            access: normalize_access(entry_value(entry, :access), flags),
            summary: normalize_summary(entry_value(entry, :summary, @default_entry.summary))
          }
        ]

      _other ->
        []
    end
  end

  defp normalize_entry(_entry), do: []

  defp entry_value(entry, key, default \\ nil) do
    Map.get(entry, key, Map.get(entry, Atom.to_string(key), default))
  end

  defp normalize_flags(flags) when is_list(flags) do
    flags
    |> Enum.filter(&is_binary/1)
    |> Enum.map(&String.downcase/1)
  end

  defp normalize_flags(_flags), do: []

  defp normalize_int(value) when is_integer(value), do: value
  defp normalize_int(_value), do: 0

  defp normalize_access(access, _flags) when access in [:read, :write, :rw], do: access
  defp normalize_access("read", _flags), do: :read
  defp normalize_access("write", _flags), do: :write
  defp normalize_access("rw", _flags), do: :rw

  defp normalize_access(_access, flags) do
    cond do
      "readonly" in flags -> :read
      "write" in flags -> :write
      true -> :rw
    end
  end

  defp normalize_summary(summary) when is_binary(summary), do: summary
  defp normalize_summary(_summary), do: @default_entry.summary

  defp deduplicate(commands) do
    {_seen, commands} =
      Enum.reduce(commands, {MapSet.new(), []}, fn command, {seen, acc} ->
        upper = String.upcase(command.name)

        if MapSet.member?(seen, upper) do
          {seen, acc}
        else
          {MapSet.put(seen, upper), [command | acc]}
        end
      end)

    Enum.reverse(commands)
  end

  defp handler_module(upper_command) do
    Enum.find_value(modules(), :error, fn module ->
      if provider_handles?(module, upper_command), do: {:ok, module}, else: nil
    end)
  end

  defp provider_handles?(module, upper_command) do
    Code.ensure_loaded?(module) and function_exported?(module, :handle, 3) and
      module
      |> module_commands()
      |> Enum.any?(&(String.upcase(&1.name) == upper_command))
  end

  defp builtin_command?(command) do
    match?({:ok, _entry}, Entries.lookup_upper(String.upcase(command.name)))
  end

  defp extract_keys(first, last, step, args) do
    first_idx = max(first - 1, 0)
    last_idx = if last == -1, do: length(args) - 1, else: last - 1
    step = max(step, 1)

    if last_idx < first_idx do
      []
    else
      first_idx..last_idx//step
      |> Enum.map(&Enum.at(args, &1))
      |> Enum.reject(&is_nil/1)
      |> Enum.filter(&is_binary/1)
    end
  end
end
