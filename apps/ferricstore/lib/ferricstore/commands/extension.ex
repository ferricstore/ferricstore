defmodule Ferricstore.Commands.Extension do
  @moduledoc """
  Behaviour and registry helpers for optional command providers.

  A provider is configured with:

      config :ferricstore, :command_extensions, [MyApp.Commands]

  Providers declare command metadata with `commands/0` and execute commands
  with `handle/3`. The core dispatcher uses this metadata for routing and key
  ACL extraction, while leaving command semantics in the provider module.
  `access` describes the command's key footprint. `acl_categories` independently
  controls category grants and defaults to `READ` or `WRITE` for the matching
  access mode.
  """

  @type access :: :read | :write | :rw
  @type acl_category :: atom() | binary()

  @type prepared :: %{
          ast: {:extension_command, module(), binary(), [binary()], access()},
          keys: [binary()]
        }

  @type command_entry :: %{
          required(:name) => binary(),
          optional(:arity) => integer(),
          optional(:flags) => [binary()],
          optional(:first_key) => integer(),
          optional(:last_key) => integer(),
          optional(:step) => integer(),
          optional(:access) => access(),
          optional(:acl_categories) => [acl_category()],
          optional(:summary) => binary()
        }

  @callback commands() :: [command_entry()]
  @callback handle(binary(), [binary()], map()) :: term()
  @callback keys(binary(), [binary()]) :: {:ok, [binary()]} | :error
  @optional_callbacks keys: 2

  alias Ferricstore.Commands.Catalog.Entries

  @retired_command_names ~w(
    FLOW.LIST
    FLOW.SEARCH
    FLOW.TERMINALS
    FLOW.FAILURES
    FLOW.STUCK
    FLOW.BY_PARENT
    FLOW.BY_ROOT
    FLOW.BY_CORRELATION
  )
  @core_modules [Ferricstore.Flow.Query.Commands]

  @default_entry %{
    arity: -1,
    flags: [],
    first_key: 0,
    last_key: 0,
    step: 0,
    access: :rw,
    summary: "Extension command."
  }

  @doc "Returns intrinsic OSS and configured provider modules."
  @spec modules() :: [module()]
  def modules do
    (@core_modules ++ configured_modules())
    |> Enum.uniq()
  end

  defp configured_modules do
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

  @doc "Returns non-shadowing command names assigned to an ACL category."
  @spec non_shadowing_command_names_in_acl_category(binary()) :: MapSet.t(binary())
  def non_shadowing_command_names_in_acl_category(category) when is_binary(category) do
    category = String.upcase(category)

    non_shadowing_commands()
    |> Enum.filter(&(category in &1.acl_categories))
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

  @doc "Resolves one immutable provider snapshot for preparation and dispatch."
  @spec prepare(binary(), [binary()]) :: {:ok, prepared()} | {:error, :invalid_keys} | :error
  def prepare(command, args) when is_binary(command) and is_list(args) do
    upper = String.upcase(command)

    if match?({:ok, _entry}, Entries.lookup_upper(upper)) do
      :error
    else
      prepare_from_modules(modules(), upper, args)
    end
  end

  @doc "Extracts key arguments for a configured command."
  @spec keys(binary(), [binary()]) :: {:ok, [binary()]} | :error
  def keys(command, args) when is_binary(command) and is_list(args) do
    upper = String.upcase(command)

    case handler(upper) do
      {:ok, module, entry} ->
        extension_keys(module, upper, args, entry)

      :error ->
        :error
    end
  end

  defp extension_keys(module, command, args, entry) do
    if function_exported?(module, :keys, 2) do
      case module.keys(command, args) do
        {:ok, keys} when is_list(keys) -> {:ok, keys}
        :error -> {:ok, metadata_keys(entry, args)}
        _other -> :error
      end
    else
      {:ok, metadata_keys(entry, args)}
    end
  end

  defp metadata_keys(%{first_key: 0}, _args), do: []

  defp metadata_keys(%{first_key: first, last_key: last, step: step}, args),
    do: extract_keys(first, last, step, args)

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

    case handler(upper) do
      {:ok, module, _entry} -> module.handle(upper, args, store)
      :error -> :not_found
    end
  end

  def handle(_command, _args, _store), do: :not_found

  @doc false
  @spec handle_prepared(module(), binary(), [binary()], map()) :: term()
  def handle_prepared(module, command, args, store)
      when is_atom(module) and is_binary(command) and is_list(args) do
    if Code.ensure_loaded?(module) and function_exported?(module, :handle, 3) do
      module.handle(command, args, store)
    else
      {:error, "ERR prepared extension provider is unavailable"}
    end
  end

  @doc """
  Returns trusted request context attached by the server protocol layer.

  Extension providers can use this to read authenticated subject, tenant, and
  scope data without parsing command arguments as authority.
  """
  @spec request_context(map()) :: map()
  def request_context(%{} = store) do
    case Map.get(store, :request_context) || Map.get(store, "request_context") do
      %{} = context -> context
      _other -> %{}
    end
  end

  def request_context(_store), do: %{}

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

        if upper in @retired_command_names do
          []
        else
          flags = normalize_flags(entry_value(entry, :flags, @default_entry.flags))
          access = normalize_access(entry_value(entry, :access), flags)

          [
            %{
              name: String.downcase(upper),
              arity: normalize_int(entry_value(entry, :arity, @default_entry.arity)),
              flags: flags,
              first_key: normalize_int(entry_value(entry, :first_key, @default_entry.first_key)),
              last_key: normalize_int(entry_value(entry, :last_key, @default_entry.last_key)),
              step: normalize_int(entry_value(entry, :step, @default_entry.step)),
              access: access,
              acl_categories:
                normalize_acl_categories(entry_value(entry, :acl_categories), access),
              summary: normalize_summary(entry_value(entry, :summary, @default_entry.summary))
            }
          ]
        end

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

  defp normalize_acl_categories(nil, :read), do: ["READ"]
  defp normalize_acl_categories(nil, :write), do: ["WRITE"]
  defp normalize_acl_categories(nil, :rw), do: []

  defp normalize_acl_categories(categories, _access) when is_list(categories) do
    categories
    |> Enum.flat_map(fn
      category when is_atom(category) -> [category |> Atom.to_string() |> String.upcase()]
      category when is_binary(category) and category != "" -> [String.upcase(category)]
      _invalid -> []
    end)
    |> Enum.uniq()
  end

  defp normalize_acl_categories(_invalid, access), do: normalize_acl_categories(nil, access)

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

  defp prepare_from_modules([], _upper, _args), do: :error

  defp prepare_from_modules([module | rest], upper, args) do
    case provider_entry(module, upper) do
      {:ok, entry} ->
        case extension_keys(module, upper, args, entry) do
          {:ok, keys} ->
            if Enum.all?(keys, &is_binary/1) do
              {:ok,
               %{
                 ast: {:extension_command, module, upper, args, entry.access},
                 keys: keys
               }}
            else
              {:error, :invalid_keys}
            end

          :error ->
            {:error, :invalid_keys}
        end

      :error ->
        prepare_from_modules(rest, upper, args)
    end
  end

  defp handler(upper_command) do
    Enum.find_value(modules(), :error, fn module ->
      case provider_entry(module, upper_command) do
        {:ok, entry} -> {:ok, module, entry}
        :error -> nil
      end
    end)
  end

  defp provider_entry(module, upper_command) do
    if Code.ensure_loaded?(module) and function_exported?(module, :handle, 3) do
      case Enum.find(module_commands(module), &(String.upcase(&1.name) == upper_command)) do
        nil -> :error
        entry -> {:ok, entry}
      end
    else
      :error
    end
  end

  defp builtin_command?(command) do
    match?({:ok, _entry}, Entries.lookup_upper(String.upcase(command.name)))
  end

  defp extract_keys(first, last, step, args) do
    first_idx = max(first - 1, 0)
    last_idx = if last == -1, do: :last, else: last - 1
    step = max(step, 1)

    positional_keys(args, 0, first_idx, last_idx, step, [])
  end

  defp positional_keys([], _index, _first, _last, _step, acc), do: Enum.reverse(acc)

  defp positional_keys(_args, index, _first, last, _step, acc)
       when is_integer(last) and index > last,
       do: Enum.reverse(acc)

  defp positional_keys([arg | rest], index, first, last, step, acc) do
    acc =
      if index >= first and rem(index - first, step) == 0 and is_binary(arg),
        do: [arg | acc],
        else: acc

    positional_keys(rest, index + 1, first, last, step, acc)
  end
end
