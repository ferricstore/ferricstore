defmodule FerricstoreServer.Health.Dashboard.Flow.QueryProjection do
  @moduledoc false
  alias Ferricstore.Flow.Query.Field

  @guided_fields [:run_id, :type, :state, :run_state, :updated_at_ms]

  def guided_fields("stuck"), do: @guided_fields ++ [:lease_deadline_ms]
  def guided_fields(_kind), do: @guided_fields

  def guided_labels("stuck"),
    do: guided_labels(nil) ++ ["Lease deadline"]

  def guided_labels(_kind), do: ["Workflow", "Type", "Stored state", "Workflow state", "Updated"]

  # Table and export must resolve the same prepared selectors without hydrating records.
  def value(record, :runs, selector) when selector in [:attributes, :state_meta],
    do: fetch(record, selector)

  def value(record, :runs, selector) when is_map(record) do
    case Field.fetch(record, selector) do
      {:ok, value} -> value
      :missing -> nil
    end
  end

  def value(record, :events, {:event_field, name}),
    do: record |> fetch(:fields) |> fetch(name)

  def value(record, :events, selector) when selector in [:event_id, :fields],
    do: fetch(record, selector)

  def value(_record, _source, _selector), do: nil

  defp fetch(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp fetch(map, key) when is_map(map) and is_binary(key), do: Map.get(map, key)
  defp fetch(_map, _key), do: nil
end
