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
    :command_keys,
    :acl_keys,
    :channel_keys,
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
          command_keys: [binary()],
          acl_keys: [binary()],
          channel_keys: [binary()],
          routing_scope: :none | :keys | :coordinated,
          routing_keys: [binary()],
          read_keys: [binary()],
          write_keys: [binary()],
          transaction_mode: Ferricstore.Commands.TransactionPolicy.mode()
        }

  @type shard_resolver :: (binary() -> term())

  @spec prepare(binary(), [term()], keyword()) ::
          {:ok, t()} | {:error, binary() | Ferricstore.Flow.Query.Error.t()}
  def prepare(name, args, opts \\ []) when is_list(opts) do
    case KeyDiscovery.prepare(name, args, opts) do
      {:ok, description} -> {:ok, from_description(description)}
      {:error, _reason} = error -> error
    end
  end

  defp from_description(description) do
    %__MODULE__{
      command: description.command,
      args: description.args,
      ast: description.ast,
      command_keys: description.command_keys,
      acl_keys: description.acl_keys,
      channel_keys: description.channel_keys,
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

  @doc """
  Detaches binaries that would retain a larger parent binary.

  Prepared commands normally live only for one request, so preparation keeps
  decoder sub-binaries without copying. Long-lived consumers such as MULTI
  queues must call this at their retention boundary.
  """
  @spec detach_retained_binaries(t()) :: t()
  def detach_retained_binaries(%__MODULE__{} = prepared) do
    {detached, _copies} = detach_term(prepared, %{})
    detached
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

  defp detach_term(binary, copies) when is_binary(binary) do
    case copies do
      %{^binary => detached} ->
        {detached, copies}

      _missing ->
        detached =
          if :binary.referenced_byte_size(binary) > byte_size(binary),
            do: :binary.copy(binary),
            else: binary

        {detached, Map.put(copies, binary, detached)}
    end
  end

  defp detach_term([head | tail], copies) do
    {detached_head, copies} = detach_term(head, copies)
    {detached_tail, copies} = detach_term(tail, copies)
    {[detached_head | detached_tail], copies}
  end

  defp detach_term([], copies), do: {[], copies}

  defp detach_term(tuple, copies) when is_tuple(tuple) do
    {items, copies} =
      tuple
      |> Tuple.to_list()
      |> detach_term(copies)

    {List.to_tuple(items), copies}
  end

  defp detach_term(map, copies) when is_map(map) do
    map
    |> :maps.to_list()
    |> Enum.reduce({%{}, copies}, fn {key, value}, {detached_map, copies} ->
      {detached_key, copies} = detach_term(key, copies)
      {detached_value, copies} = detach_term(value, copies)
      {Map.put(detached_map, detached_key, detached_value), copies}
    end)
  end

  defp detach_term(term, copies), do: {term, copies}
end
