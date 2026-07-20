defmodule Ferricstore.Flow.Query.RegisteredIndex do
  @moduledoc false

  alias Ferricstore.Flow.Query.IndexDefinition

  @states [:building, :validating, :active, :retiring, :failed]
  @projection_states [:building, :validating, :active]

  @enforce_keys [:definition, :state, :build_id]
  defstruct [:definition, :state, :build_id, coverage: %{}, stats: nil]

  @type state :: :building | :validating | :active | :retiring | :failed
  @type t :: %__MODULE__{
          definition: IndexDefinition.t(),
          state: state(),
          build_id: binary(),
          coverage: map(),
          stats: term()
        }

  @spec new(IndexDefinition.t(), state(), keyword()) :: {:ok, t()} | {:error, atom()}
  def new(definition, state, opts \\ [])

  def new(%IndexDefinition{} = definition, state, opts)
      when state in @states and is_list(opts) do
    build_id = Keyword.get(opts, :build_id, default_build_id(definition))
    coverage = Keyword.get(opts, :coverage, %{})
    stats = Keyword.get(opts, :stats)

    if IndexDefinition.validate(definition) == :ok and valid_build_id?(build_id) and
         is_map(coverage) and map_size(coverage) <= 16 do
      {:ok,
       %__MODULE__{
         definition: definition,
         state: state,
         build_id: build_id,
         coverage: coverage,
         stats: stats
       }}
    else
      {:error, :invalid_registered_index}
    end
  end

  def new(%IndexDefinition{}, _state, _opts), do: {:error, :invalid_registered_index}

  @spec validate(t()) :: :ok | {:error, atom()}
  def validate(%__MODULE__{} = index) do
    with :ok <- IndexDefinition.validate(index.definition),
         {:ok, rebuilt} <-
           new(index.definition, index.state,
             build_id: index.build_id,
             coverage: index.coverage,
             stats: index.stats
           ),
         true <- rebuilt == index do
      :ok
    else
      _invalid -> {:error, :invalid_registered_index}
    end
  end

  def validate(_index), do: {:error, :invalid_registered_index}

  @spec new!(IndexDefinition.t(), state(), keyword()) :: t()
  def new!(definition, state, opts \\ []) do
    case new(definition, state, opts) do
      {:ok, index} -> index
      {:error, reason} -> raise ArgumentError, "invalid registered index: #{reason}"
    end
  end

  @spec projection?(t()) :: boolean()
  def projection?(%__MODULE__{state: state}), do: state in @projection_states

  @spec queryable?(t()) :: boolean()
  def queryable?(%__MODULE__{state: :active}), do: true
  def queryable?(%__MODULE__{}), do: false

  defp default_build_id(definition),
    do: Base.url_encode64(definition.fingerprint, padding: false)

  defp valid_build_id?(build_id),
    do: is_binary(build_id) and build_id != "" and byte_size(build_id) <= 128
end
