defmodule Ferricstore.Commands.PreparedCommand do
  @moduledoc """
  Immutable command metadata shared by parsing, authorization, and routing.

  Preparation normalizes the command and arguments once. Consumers should keep
  this value instead of rediscovering keys from the raw command payload.
  """

  alias Ferricstore.Commands.KeyDiscovery
  alias Ferricstore.Store.Router

  @enforce_keys [
    :command,
    :args,
    :ast,
    :acl_keys,
    :routing_scope,
    :routing_keys,
    :read_keys,
    :write_keys,
    :transaction_mode
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
          write_keys: [binary()],
          transaction_mode: Ferricstore.Commands.TransactionPolicy.mode()
        }

  @type shard_resolver :: (binary() -> term())

  @spec prepare(binary(), [term()]) :: {:ok, t()} | {:error, binary()}
  def prepare(name, args) do
    case KeyDiscovery.prepare(name, args) do
      {:ok, description} -> {:ok, from_description(description)}
      {:error, _reason} = error -> error
    end
  end

  defp from_description(description) do
    %__MODULE__{
      command: description.command,
      args: description.args,
      ast: description.ast,
      acl_keys: description.acl_keys,
      routing_scope: description.routing_scope,
      routing_keys: description.routing_keys,
      read_keys: description.read_keys,
      write_keys: description.write_keys,
      transaction_mode: description.transaction_mode
    }
  end

  @spec transaction_safe?(t()) :: boolean()
  def transaction_safe?(%__MODULE__{transaction_mode: :local}), do: true
  def transaction_safe?(%__MODULE__{}), do: false

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
end
