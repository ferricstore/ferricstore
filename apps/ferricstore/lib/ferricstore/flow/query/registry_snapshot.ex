defmodule Ferricstore.Flow.Query.RegistrySnapshot do
  @moduledoc false

  alias Ferricstore.Flow.Query.{IndexDefinition, RegisteredIndex}

  @max_indexes 32
  @max_projection_entries 128
  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  @enforce_keys [:epoch, :catalog_version, :indexes]
  defstruct [:epoch, :catalog_version, :indexes]

  @type t :: %__MODULE__{
          epoch: non_neg_integer(),
          catalog_version: pos_integer(),
          indexes: [RegisteredIndex.t()]
        }

  @spec empty() :: t()
  def empty, do: %__MODULE__{epoch: 0, catalog_version: 1, indexes: []}

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, atom()}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(%{} = attrs) do
    epoch = Map.get(attrs, :epoch) || Map.get(attrs, "epoch")
    catalog_version = Map.get(attrs, :catalog_version) || Map.get(attrs, "catalog_version")
    indexes = Map.get(attrs, :indexes) || Map.get(attrs, "indexes")

    with true <- is_integer(epoch) and epoch >= 0 and epoch <= @max_u64,
         true <-
           is_integer(catalog_version) and catalog_version > 0 and catalog_version <= @max_u64,
         true <- is_list(indexes) and length(indexes) <= @max_indexes,
         true <- Enum.all?(indexes, &(RegisteredIndex.validate(&1) == :ok)),
         :ok <- validate_unique(indexes),
         :ok <- validate_projection_budget(indexes) do
      {:ok, %__MODULE__{epoch: epoch, catalog_version: catalog_version, indexes: indexes}}
    else
      false -> {:error, :invalid_query_index_snapshot}
      {:error, _reason} = error -> error
    end
  end

  def new(_attrs), do: {:error, :invalid_query_index_snapshot}

  @spec new!(map() | keyword()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, snapshot} -> snapshot
      {:error, reason} -> raise ArgumentError, "invalid query index snapshot: #{reason}"
    end
  end

  @spec validate(t()) :: :ok | {:error, atom()}
  def validate(%__MODULE__{} = snapshot) do
    case new(Map.from_struct(snapshot)) do
      {:ok, _validated} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def validate(_snapshot), do: {:error, :invalid_query_index_snapshot}

  defp validate_unique(indexes) do
    logical = Enum.map(indexes, &{&1.definition.id, &1.definition.version})
    physical = Enum.map(indexes, &IndexDefinition.storage_prefix(&1.definition))

    if length(logical) == length(Enum.uniq(logical)) and
         length(physical) == length(Enum.uniq(physical)),
       do: :ok,
       else: {:error, :duplicate_query_index}
  end

  defp validate_projection_budget(indexes) do
    entries =
      indexes
      |> Enum.filter(&RegisteredIndex.projection?/1)
      |> Enum.reduce(0, fn index, count -> count + definition_fanout(index.definition) end)

    if entries <= @max_projection_entries,
      do: :ok,
      else: {:error, :query_index_projection_budget_exceeded}
  end

  defp definition_fanout(%IndexDefinition{fields: fields}) do
    if Enum.any?(fields, fn {field, _direction, _encoding} ->
         match?({:attribute, _name}, field)
       end),
       do: 16,
       else: 1
  end
end
