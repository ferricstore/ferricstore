defmodule Ferricstore.Flow.Governance.Scope do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  defstruct [:kind, :name, :key, :partition_key, :type, :state, :effect_type]

  @kinds %{
    "partition" => :partition,
    "tenant" => :tenant,
    "type" => :type,
    "type_state" => :type_state,
    "effect" => :effect,
    "budget" => :budget,
    "global" => :global
  }

  def resolve(attrs, record \\ nil) when is_map(attrs) do
    explicit = fetch(attrs, :governance_scope, "governance_scope")

    partition_key =
      fetch(attrs, :partition_key, "partition_key") || map_get(record, :partition_key)

    id = fetch(attrs, :id, "id") || map_get(record, :id)
    type = fetch(attrs, :type, "type") || map_get(record, :type)
    state = fetch(attrs, :state, "state") || map_get(record, :state)
    effect_type = fetch(attrs, :effect_type, "effect_type")

    cond do
      present_binary?(explicit) ->
        normalize(explicit,
          partition_key: partition_key,
          type: type,
          state: state,
          effect_type: effect_type
        )

      present_binary?(partition_key) ->
        normalize("partition:" <> partition_key,
          partition_key: partition_key,
          type: type,
          state: state,
          effect_type: effect_type
        )

      present_binary?(id) ->
        auto_partition = Keys.auto_partition_key(id)

        normalize("partition:" <> auto_partition,
          partition_key: auto_partition,
          type: type,
          state: state,
          effect_type: effect_type
        )

      true ->
        {:error, "ERR governance scope requires governance_scope, partition_key, or flow id"}
    end
  end

  def normalize(scope, metadata \\ []) when is_binary(scope) do
    case String.split(scope, ":", parts: 2) do
      [kind, name] when kind != "" and name != "" ->
        case Map.fetch(@kinds, kind) do
          {:ok, atom_kind} ->
            {:ok,
             %__MODULE__{
               kind: atom_kind,
               name: name,
               key: kind <> ":" <> name,
               partition_key: Keyword.get(metadata, :partition_key),
               type: Keyword.get(metadata, :type),
               state: Keyword.get(metadata, :state),
               effect_type: Keyword.get(metadata, :effect_type)
             }}

          :error ->
            {:error, "ERR invalid governance scope kind"}
        end

      _other ->
        {:error, "ERR governance scope must use kind:name format"}
    end
  end

  def key(%__MODULE__{key: key}), do: key

  def storage_key(%__MODULE__{key: key}), do: Keys.governance_scope_key(key)

  defp fetch(map, atom_key, binary_key) do
    cond do
      is_map(map) and Map.has_key?(map, atom_key) -> Map.fetch!(map, atom_key)
      is_map(map) and Map.has_key?(map, binary_key) -> Map.fetch!(map, binary_key)
      true -> nil
    end
  end

  defp map_get(nil, _key), do: nil
  defp map_get(map, key) when is_map(map), do: Map.get(map, key)
  defp present_binary?(value), do: is_binary(value) and value != ""
end
