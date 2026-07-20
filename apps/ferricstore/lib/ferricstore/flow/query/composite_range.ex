defmodule Ferricstore.Flow.Query.CompositeRange do
  @moduledoc false

  alias Ferricstore.Flow.Query.{CompositeIndex, IndexDefinition, TupleCodec}

  @enforce_keys [:index_id, :index_version, :prefix, :after_key, :before_key]
  defstruct [:index_id, :index_version, :prefix, :after_key, :before_key]

  @type t :: %__MODULE__{
          index_id: binary(),
          index_version: pos_integer(),
          prefix: binary(),
          after_key: binary(),
          before_key: binary()
        }

  @max_key_bytes 511
  @max_index_id_bytes 64
  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  @spec validate(t()) :: :ok | {:error, :invalid_composite_range}
  def validate(%__MODULE__{} = range) do
    storage_prefix = IndexDefinition.global_storage_prefix()
    storage_prefix_bytes = byte_size(storage_prefix)

    with true <-
           is_binary(range.index_id) and range.index_id != "" and
             byte_size(range.index_id) <= @max_index_id_bytes,
         true <-
           is_integer(range.index_version) and range.index_version > 0 and
             range.index_version <= @max_u64,
         true <-
           is_binary(range.prefix) and byte_size(range.prefix) <= @max_key_bytes and
             byte_size(range.prefix) > storage_prefix_bytes + 40 and
             String.starts_with?(range.prefix, storage_prefix),
         <<_::binary-size(storage_prefix_bytes), embedded_version::unsigned-big-64,
           _fingerprint::binary-size(32), components::binary>> <- range.prefix,
         true <- embedded_version == range.index_version and components != "",
         true <- valid_boundary?(range.after_key, range.prefix, :after),
         true <- valid_boundary?(range.before_key, range.prefix, :before),
         true <-
           range.after_key == "" or range.before_key == "" or
             range.after_key < range.before_key do
      :ok
    else
      _invalid -> {:error, :invalid_composite_range}
    end
  end

  def validate(_range), do: {:error, :invalid_composite_range}

  @spec prefix(IndexDefinition.t(), [term()]) :: {:ok, t()} | {:error, atom()}
  def prefix(%IndexDefinition{fields: fields} = definition, equality_values)
      when is_list(equality_values) and equality_values != [] and
             length(equality_values) <= length(fields) do
    prefix(definition, nil, equality_values)
  end

  def prefix(%IndexDefinition{}, _equality_values), do: {:error, :invalid_range_prefix}

  @spec prefix(IndexDefinition.t(), binary() | nil, [term()]) ::
          {:ok, t()} | {:error, atom()}
  def prefix(%IndexDefinition{fields: fields} = definition, scope_prefix, equality_values)
      when (is_binary(scope_prefix) or is_nil(scope_prefix)) and is_list(equality_values) and
             equality_values != [] and length(equality_values) <= length(fields) do
    with :ok <- IndexDefinition.validate(definition),
         {:ok, prefix} <- CompositeIndex.encode_prefix(definition, scope_prefix, equality_values) do
      {:ok, range(definition, prefix, "", "")}
    end
  end

  def prefix(%IndexDefinition{}, _scope_prefix, _equality_values),
    do: {:error, :invalid_range_prefix}

  @spec bounded(
          IndexDefinition.t(),
          [term()],
          term(),
          :inclusive | :exclusive,
          term(),
          :inclusive | :exclusive
        ) :: {:ok, t()} | {:error, atom()}
  def bounded(
        %IndexDefinition{fields: fields} = definition,
        equality_values,
        lower,
        lower_kind,
        upper,
        upper_kind
      )
      when is_list(equality_values) and equality_values != [] and
             length(equality_values) < length(fields) and
             lower_kind in [:inclusive, :exclusive] and upper_kind in [:inclusive, :exclusive] do
    bounded(definition, nil, equality_values, lower, lower_kind, upper, upper_kind)
  end

  def bounded(%IndexDefinition{}, _equalities, _lower, _lower_kind, _upper, _upper_kind),
    do: {:error, :invalid_composite_range}

  @spec bounded(
          IndexDefinition.t(),
          binary() | nil,
          [term()],
          term(),
          :inclusive | :exclusive,
          term(),
          :inclusive | :exclusive
        ) :: {:ok, t()} | {:error, atom()}
  def bounded(
        %IndexDefinition{fields: fields} = definition,
        scope_prefix,
        equality_values,
        lower,
        lower_kind,
        upper,
        upper_kind
      )
      when (is_binary(scope_prefix) or is_nil(scope_prefix)) and is_list(equality_values) and
             equality_values != [] and length(equality_values) < length(fields) and
             lower_kind in [:inclusive, :exclusive] and upper_kind in [:inclusive, :exclusive] do
    range_field = Enum.at(fields, length(equality_values))

    with :ok <- IndexDefinition.validate(definition),
         {_field, direction, :ordered} <- range_field,
         :ok <- validate_logical_bounds(lower, lower_kind, upper, upper_kind),
         {:ok, prefix} <- CompositeIndex.encode_prefix(definition, scope_prefix, equality_values),
         {:ok, lower_prefix} <-
           CompositeIndex.encode_prefix(definition, scope_prefix, equality_values ++ [lower]),
         {:ok, upper_prefix} <-
           CompositeIndex.encode_prefix(definition, scope_prefix, equality_values ++ [upper]),
         {:ok, after_key, before_key} <-
           physical_bounds(direction, lower_prefix, lower_kind, upper_prefix, upper_kind),
         true <-
           String.starts_with?(after_key, prefix) and String.starts_with?(before_key, prefix) and
             after_key < before_key do
      {:ok, range(definition, prefix, after_key, before_key)}
    else
      {_field, _direction, _encoding} -> {:error, :range_field_not_ordered}
      false -> {:error, :invalid_range_order}
      {:error, _reason} = error -> error
    end
  end

  def bounded(
        %IndexDefinition{},
        _scope_prefix,
        _equalities,
        _lower,
        _lower_kind,
        _upper,
        _upper_kind
      ),
      do: {:error, :invalid_composite_range}

  defp validate_logical_bounds(lower, lower_kind, upper, upper_kind) do
    try do
      case TupleCodec.compare_values(lower, upper) do
        :lt -> :ok
        :eq when lower_kind == :inclusive and upper_kind == :inclusive -> :ok
        _other -> {:error, :invalid_range_order}
      end
    rescue
      ArgumentError -> {:error, :unsupported_index_value}
    end
  end

  defp physical_bounds(:asc, lower_prefix, lower_kind, upper_prefix, upper_kind) do
    with {:ok, after_key} <- lower_boundary(lower_prefix, lower_kind),
         {:ok, before_key} <- upper_boundary(upper_prefix, upper_kind) do
      {:ok, after_key, before_key}
    end
  end

  defp physical_bounds(:desc, lower_prefix, lower_kind, upper_prefix, upper_kind) do
    with {:ok, after_key} <- lower_boundary(upper_prefix, upper_kind),
         {:ok, before_key} <- upper_boundary(lower_prefix, lower_kind) do
      {:ok, after_key, before_key}
    end
  end

  # LMDB starts strictly after after_key and stops strictly before before_key.
  # A component prefix is therefore the inclusive boundary for all keys that
  # extend it; its lexicographic successor is the exclusive boundary.
  defp lower_boundary(prefix, :inclusive), do: {:ok, prefix}
  defp lower_boundary(prefix, :exclusive), do: successor(prefix)
  defp upper_boundary(prefix, :exclusive), do: {:ok, prefix}
  defp upper_boundary(prefix, :inclusive), do: successor(prefix)

  defp successor(binary), do: successor(binary, byte_size(binary) - 1)

  defp successor(_binary, -1), do: {:error, :range_bound_overflow}

  defp successor(binary, offset) do
    case :binary.at(binary, offset) do
      0xFF -> successor(binary_part(binary, 0, offset), offset - 1)
      byte -> {:ok, binary_part(binary, 0, offset) <> <<byte + 1>>}
    end
  end

  defp range(definition, prefix, after_key, before_key) do
    %__MODULE__{
      index_id: definition.id,
      index_version: definition.version,
      prefix: prefix,
      after_key: after_key,
      before_key: before_key
    }
  end

  defp valid_boundary?("", _prefix, _kind), do: true

  defp valid_boundary?(boundary, prefix, kind) when is_binary(boundary) do
    byte_size(boundary) <= @max_key_bytes and String.starts_with?(boundary, prefix) and
      case kind do
        :after -> boundary >= prefix
        :before -> boundary > prefix
      end
  end

  defp valid_boundary?(_boundary, _prefix, _kind), do: false
end
