defmodule Ferricstore.Flow.Locator do
  @moduledoc false

  @enforce_keys [:flow_id, :kind, :version, :raft_index, :file_id, :offset, :value_size]
  defstruct [
    :flow_id,
    :kind,
    :version,
    :raft_index,
    :file_id,
    :offset,
    :value_size,
    :checksum,
    :expire_at_ms,
    :segment_generation
  ]

  @type kind :: :state | :value | :history

  @type t :: %__MODULE__{
          flow_id: binary(),
          kind: kind(),
          version: non_neg_integer(),
          raft_index: non_neg_integer(),
          file_id: term(),
          offset: non_neg_integer(),
          value_size: non_neg_integer(),
          checksum: binary() | nil,
          expire_at_ms: non_neg_integer() | nil,
          segment_generation: non_neg_integer() | nil
        }

  @type source :: :hot | :cold
  @type resolution :: {:ok, source(), t()} | {:error, :flow_invisible}

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, :bad_locator}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    locator = %__MODULE__{
      flow_id: Map.get(attrs, :flow_id),
      kind: Map.get(attrs, :kind),
      version: Map.get(attrs, :version),
      raft_index: Map.get(attrs, :raft_index),
      file_id: Map.get(attrs, :file_id),
      offset: Map.get(attrs, :offset),
      value_size: Map.get(attrs, :value_size),
      checksum: Map.get(attrs, :checksum),
      expire_at_ms: Map.get(attrs, :expire_at_ms),
      segment_generation: Map.get(attrs, :segment_generation)
    }

    if valid?(locator), do: {:ok, locator}, else: {:error, :bad_locator}
  end

  @spec new!(map() | keyword()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, locator} -> locator
      {:error, reason} -> raise ArgumentError, "invalid Flow locator: #{inspect(reason)}"
    end
  end

  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{
        flow_id: flow_id,
        kind: kind,
        version: version,
        raft_index: raft_index,
        offset: offset,
        value_size: value_size,
        checksum: checksum,
        expire_at_ms: expire_at_ms,
        segment_generation: segment_generation
      }) do
    is_binary(flow_id) and flow_id != "" and kind in [:state, :value, :history] and
      non_neg_int?(version) and non_neg_int?(raft_index) and non_neg_int?(offset) and
      non_neg_int?(value_size) and optional_checksum?(checksum) and
      optional_non_neg_int?(expire_at_ms) and optional_non_neg_int?(segment_generation)
  end

  def valid?(_locator), do: false

  @spec generation(t()) :: {non_neg_integer(), non_neg_integer()}
  def generation(%__MODULE__{version: version, raft_index: raft_index}),
    do: {version, raft_index}

  @spec logical_key(t()) :: {binary(), kind()}
  def logical_key(%__MODULE__{flow_id: flow_id, kind: kind}), do: {flow_id, kind}

  @spec same_logical_record?(t(), t()) :: boolean()
  def same_logical_record?(%__MODULE__{} = left, %__MODULE__{} = right) do
    logical_key(left) == logical_key(right) and generation(left) == generation(right)
  end

  @spec same_physical_record?(t(), t()) :: boolean()
  def same_physical_record?(%__MODULE__{} = left, %__MODULE__{} = right) do
    same_logical_record?(left, right) and left.file_id == right.file_id and
      left.offset == right.offset and left.value_size == right.value_size and
      left.checksum == right.checksum and left.segment_generation == right.segment_generation
  end

  @spec compare_generation(t(), t()) :: :lt | :eq | :gt
  def compare_generation(%__MODULE__{} = left, %__MODULE__{} = right) do
    compare_tuple(generation(left), generation(right))
  end

  @spec newer?(t(), t()) :: boolean()
  def newer?(%__MODULE__{} = left, %__MODULE__{} = right),
    do: compare_generation(left, right) == :gt

  @spec stale_for?(t(), t()) :: boolean()
  def stale_for?(%__MODULE__{} = candidate, %__MODULE__{} = current),
    do: compare_generation(candidate, current) == :lt

  @spec resolve(t() | nil, t() | nil) :: resolution()
  def resolve(nil, nil), do: {:error, :flow_invisible}
  def resolve(%__MODULE__{} = hot, nil), do: {:ok, :hot, hot}
  def resolve(nil, %__MODULE__{} = cold), do: {:ok, :cold, cold}

  def resolve(%__MODULE__{} = hot, %__MODULE__{} = cold) do
    cond do
      newer?(cold, hot) -> {:ok, :cold, cold}
      true -> {:ok, :hot, hot}
    end
  end

  @spec safe_to_evict_hot?(t(), t() | nil, t() | nil) :: boolean()
  def safe_to_evict_hot?(
        %__MODULE__{} = snapshot,
        %__MODULE__{} = cold,
        %__MODULE__{} = current_hot
      ) do
    same_physical_record?(snapshot, current_hot) and same_physical_record?(snapshot, cold)
  end

  def safe_to_evict_hot?(_snapshot, _cold, _current_hot), do: false

  @spec stale_delete?(t(), t() | nil) :: boolean()
  def stale_delete?(_delete_locator, nil), do: false

  def stale_delete?(%__MODULE__{} = delete_locator, %__MODULE__{} = current_locator) do
    logical_key(delete_locator) == logical_key(current_locator) and
      stale_for?(delete_locator, current_locator)
  end

  @spec relocate(t(), keyword() | map()) :: {:ok, t()} | {:error, :bad_locator}
  def relocate(%__MODULE__{} = locator, attrs) when is_list(attrs) or is_map(attrs) do
    attrs = Map.new(attrs)

    locator
    |> Map.put(:file_id, Map.get(attrs, :file_id, locator.file_id))
    |> Map.put(:offset, Map.get(attrs, :offset, locator.offset))
    |> Map.put(:value_size, Map.get(attrs, :value_size, locator.value_size))
    |> Map.put(:checksum, Map.get(attrs, :checksum, locator.checksum))
    |> Map.put(
      :segment_generation,
      Map.get(attrs, :segment_generation, locator.segment_generation)
    )
    |> then(fn relocated ->
      if valid?(relocated), do: {:ok, relocated}, else: {:error, :bad_locator}
    end)
  end

  @spec relocate!(t(), keyword() | map()) :: t()
  def relocate!(%__MODULE__{} = locator, attrs) do
    case relocate(locator, attrs) do
      {:ok, relocated} ->
        relocated

      {:error, reason} ->
        raise ArgumentError, "invalid relocated Flow locator: #{inspect(reason)}"
    end
  end

  defp compare_tuple(left, right) when left < right, do: :lt
  defp compare_tuple(left, right) when left > right, do: :gt
  defp compare_tuple(_left, _right), do: :eq

  defp non_neg_int?(value), do: is_integer(value) and value >= 0
  defp optional_non_neg_int?(nil), do: true
  defp optional_non_neg_int?(value), do: non_neg_int?(value)
  defp optional_checksum?(nil), do: true
  defp optional_checksum?(checksum), do: is_binary(checksum) and byte_size(checksum) > 0
end
