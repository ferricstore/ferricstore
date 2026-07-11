defmodule Ferricstore.Commands.PreparedCommand do
  @moduledoc """
  Immutable command metadata shared by parsing, authorization, and routing.

  Preparation normalizes the command and arguments once. Consumers should keep
  this value instead of rediscovering keys from the raw command payload.
  """

  alias Ferricstore.Commands.{Extension, KeyDiscovery, NativeAstParser}
  alias Ferricstore.Store.Router

  @enforce_keys [
    :command,
    :args,
    :ast,
    :acl_keys,
    :routing_scope,
    :routing_keys,
    :read_keys,
    :write_keys
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          command: binary(),
          args: [binary()],
          ast: term(),
          acl_keys: [binary()],
          routing_scope: :none | :keys | :coordinated,
          routing_keys: [binary()],
          read_keys: [binary()],
          write_keys: [binary()]
        }

  @type shard_resolver :: (binary() -> term())

  @spec prepare(binary(), [term()]) :: {:ok, t()} | {:error, binary()}
  def prepare(name, args) do
    case NativeAstParser.parse(name, args) do
      {:ok, command, parsed_args, {:unknown, unknown_command, _unknown_args} = ast, acl_keys}
      when unknown_command == command ->
        if Extension.command?(command) do
          case Extension.keys(command, parsed_args) do
            {:ok, keys} when is_list(keys) ->
              if Enum.all?(keys, &is_binary/1) do
                {:ok,
                 from_parsed(
                   command,
                   parsed_args,
                   Extension.ast(command, parsed_args),
                   keys
                 )}
              else
                invalid_extension_keys(command)
              end

            :error ->
              invalid_extension_keys(command)
          end
        else
          {:ok, from_parsed(command, parsed_args, ast, acl_keys)}
        end

      {:ok, command, parsed_args, ast, acl_keys} ->
        {:ok, from_parsed(command, parsed_args, ast, acl_keys)}

      {:error, _reason} = error ->
        error
    end
  end

  @spec from_parsed(binary(), [binary()], term(), [binary()]) :: t()
  def from_parsed(command, args, ast, acl_keys)
      when is_binary(command) and is_list(args) and is_list(acl_keys) do
    metadata = KeyDiscovery.describe(command, ast, acl_keys)

    %__MODULE__{
      command: command,
      args: args,
      ast: ast,
      acl_keys: metadata.acl_keys,
      routing_scope: metadata.routing_scope,
      routing_keys: metadata.routing_keys,
      read_keys: metadata.read_keys,
      write_keys: metadata.write_keys
    }
  end

  @spec mutation_footprint(t()) :: %{read: [binary()], write: [binary()]}
  def mutation_footprint(%__MODULE__{} = prepared) do
    %{read: prepared.read_keys, write: prepared.write_keys}
  end

  @spec shard_indexes(t(), FerricStore.Instance.t() | shard_resolver()) :: [term()]
  def shard_indexes(%__MODULE__{routing_keys: keys}, resolver) when is_function(resolver, 1) do
    keys
    |> Enum.map(resolver)
    |> Enum.uniq()
  end

  def shard_indexes(%__MODULE__{routing_keys: keys}, store) do
    keys
    |> Enum.map(&Router.shard_for(store, &1))
    |> Enum.uniq()
  end

  @spec cross_shard?(t(), FerricStore.Instance.t() | shard_resolver()) :: boolean()
  def cross_shard?(%__MODULE__{routing_scope: :coordinated}, _resolver_or_store), do: true
  def cross_shard?(%__MODULE__{routing_keys: []}, _resolver_or_store), do: false
  def cross_shard?(%__MODULE__{routing_keys: [_one]}, _resolver_or_store), do: false

  def cross_shard?(%__MODULE__{routing_keys: [first | rest]}, resolver_or_store) do
    first_shard = resolve_shard(first, resolver_or_store)
    Enum.any?(rest, &(resolve_shard(&1, resolver_or_store) != first_shard))
  end

  defp resolve_shard(key, resolver) when is_function(resolver, 1), do: resolver.(key)
  defp resolve_shard(key, store), do: Router.shard_for(store, key)

  defp invalid_extension_keys(command) do
    {:error, "ERR invalid key metadata for extension command '#{String.downcase(command)}'"}
  end
end
