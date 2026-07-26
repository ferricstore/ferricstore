defmodule Ferricstore.Flow.Locator do
  @moduledoc false

  @max_u64 18_446_744_073_709_551_615
  @max_flow_id_bytes 65_535
  @max_checksum_bytes 64

  @enforce_keys [:flow_id, :kind, :version, :raft_index, :file_id, :offset, :value_size]
  defstruct [
    :flow_id,
    :kind,
    :version,
    :raft_index,
    :file_id,
    :offset,
    :value_size,
    :frame_size,
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
          frame_size: non_neg_integer() | nil,
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
      frame_size: Map.get(attrs, :frame_size),
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
        frame_size: frame_size,
        checksum: checksum,
        expire_at_ms: expire_at_ms,
        segment_generation: segment_generation
      }) do
    is_binary(flow_id) and flow_id != "" and kind in [:state, :value, :history] and
      non_neg_int?(version) and non_neg_int?(raft_index) and non_neg_int?(offset) and
      non_neg_int?(value_size) and optional_non_neg_int?(frame_size) and
      optional_checksum?(checksum) and
      optional_non_neg_int?(expire_at_ms) and optional_non_neg_int?(segment_generation)
  end

  def valid?(_locator), do: false

  @doc false
  @spec durable?(term()) :: boolean()
  def durable?(%__MODULE__{} = locator) do
    valid?(locator) and bounded_binary?(locator.flow_id, @max_flow_id_bytes) and
      u64?(locator.version) and u64?(locator.raft_index) and u64?(locator.offset) and
      u64?(locator.value_size) and optional_u64?(locator.frame_size) and
      optional_bounded_binary?(locator.checksum, @max_checksum_bytes) and
      optional_u64?(locator.expire_at_ms) and optional_u64?(locator.segment_generation) and
      match?({:ok, _tag, _id}, storage_source(locator.file_id))
  end

  def durable?(_locator), do: false

  @doc false
  @spec hydration_ready?(term()) :: boolean()
  def hydration_ready?(
        %__MODULE__{kind: :state, value_size: value_size, checksum: checksum} = locator
      ) do
    durable?(locator) and value_size > 0 and is_binary(checksum) and byte_size(checksum) == 32 and
      physical_address?(locator)
  end

  def hydration_ready?(_locator), do: false

  @doc false
  @spec storage_source(term()) :: {:ok, 0..3, non_neg_integer()} | :error
  def storage_source(file_id)
      when is_integer(file_id) and file_id >= 0 and file_id <= @max_u64,
      do: {:ok, 0, file_id}

  def storage_source({tag, index})
      when tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
             is_integer(index) and index > 0 and index <= @max_u64 do
    source_tag =
      case tag do
        :waraft_segment -> 1
        :waraft_projection -> 2
        :waraft_apply_projection -> 3
      end

    {:ok, source_tag, index}
  end

  def storage_source(_file_id), do: :error

  @doc false
  @spec storage_file_id(byte(), non_neg_integer()) :: {:ok, term()} | :error
  def storage_file_id(0, file_id)
      when is_integer(file_id) and file_id >= 0 and file_id <= @max_u64,
      do: {:ok, file_id}

  def storage_file_id(1, index) when is_integer(index) and index > 0 and index <= @max_u64,
    do: {:ok, {:waraft_segment, index}}

  def storage_file_id(2, index) when is_integer(index) and index > 0 and index <= @max_u64,
    do: {:ok, {:waraft_projection, index}}

  def storage_file_id(3, index) when is_integer(index) and index > 0 and index <= @max_u64,
    do: {:ok, {:waraft_apply_projection, index}}

  def storage_file_id(_tag, _id), do: :error

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
      left.frame_size == right.frame_size and left.checksum == right.checksum and
      left.segment_generation == right.segment_generation
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
    |> Map.put(:frame_size, Map.get(attrs, :frame_size, locator.frame_size))
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

  defp physical_address?(%__MODULE__{file_id: file_id}) when is_integer(file_id), do: true

  defp physical_address?(%__MODULE__{
         file_id: {kind, index},
         raft_index: raft_index,
         segment_generation: ordinal,
         frame_size: frame_size
       })
       when kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0 and index == raft_index and is_integer(ordinal) and
              ordinal >= 0 and is_integer(frame_size) and frame_size >= 8,
       do: true

  defp physical_address?(_locator), do: false
  defp u64?(value), do: is_integer(value) and value >= 0 and value <= @max_u64
  defp optional_non_neg_int?(nil), do: true
  defp optional_non_neg_int?(value), do: non_neg_int?(value)
  defp optional_u64?(nil), do: true
  defp optional_u64?(value), do: u64?(value)
  defp optional_checksum?(nil), do: true
  defp optional_checksum?(checksum), do: is_binary(checksum) and byte_size(checksum) > 0

  defp bounded_binary?(value, maximum),
    do: is_binary(value) and value != "" and byte_size(value) <= maximum

  defp optional_bounded_binary?(nil, _maximum), do: true
  defp optional_bounded_binary?(value, maximum), do: bounded_binary?(value, maximum)
end
