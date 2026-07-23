defmodule Ferricstore.Flow.Query.CompositeCounter do
  @moduledoc false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.{CompositeIndex, IndexDefinition}
  alias Ferricstore.TermCodec

  @storage_prefix "flow-composite-count:1:"
  @value_tag :flow_composite_count
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @max_value_bytes 2_048
  @max_read_prefixes 32
  @hash_component_bytes 33
  @ordered_component_bytes 9
  @scope_header_bytes 3
  @entry_identity_bytes 33

  @spec key(IndexDefinition.t(), binary()) :: binary()
  def key(%IndexDefinition{} = definition, prefix) when is_binary(prefix) do
    if valid_declared_prefix?(definition, prefix) do
      @storage_prefix <>
        <<definition.version::unsigned-big-64, definition.fingerprint::binary-size(32),
          :crypto.hash(:sha256, prefix)::binary-size(32)>>
    else
      raise ArgumentError, "counter prefix is not declared by the index definition"
    end
  end

  @spec storage_prefix() :: binary()
  def storage_prefix, do: @storage_prefix

  @spec definition_storage_prefix(IndexDefinition.t()) :: binary()
  def definition_storage_prefix(%IndexDefinition{version: version, fingerprint: fingerprint}) do
    @storage_prefix <> <<version::unsigned-big-64, fingerprint::binary-size(32)>>
  end

  @spec encode_value(binary(), non_neg_integer()) :: binary()
  def encode_value(prefix, count)
      when is_binary(prefix) and is_integer(count),
      do: encode_value(prefix, count, 0, count)

  def encode_value(_prefix, _count),
    do: raise(ArgumentError, "counter value requires a bounded prefix and unsigned count")

  @spec encode_value(binary(), non_neg_integer(), non_neg_integer()) :: binary()
  def encode_value(prefix, count, expiring_count)
      when is_binary(prefix) and is_integer(count) and is_integer(expiring_count),
      do: encode_value(prefix, count, expiring_count, count)

  def encode_value(_prefix, _count, _expiring_count),
    do:
      raise(
        ArgumentError,
        "counter value requires bounded logical, expiring, and physical counts"
      )

  @spec encode_value(binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          binary()
  def encode_value(prefix, count, expiring_count, physical_count)
      when is_binary(prefix) and prefix != "" and is_integer(count) and count >= 0 and
             count <= @max_u64 and is_integer(expiring_count) and expiring_count >= 0 and
             expiring_count <= count and is_integer(physical_count) and
             physical_count >= count and physical_count <= @max_u64 do
    TermCodec.encode({@value_tag, 1, prefix, count, expiring_count, physical_count})
  end

  def encode_value(_prefix, _count, _expiring_count, _physical_count),
    do:
      raise(
        ArgumentError,
        "counter value requires bounded logical, expiring, and physical counts"
      )

  @spec decode_state(binary(), binary()) ::
          {:ok,
           %{
             count: non_neg_integer(),
             expiring_count: non_neg_integer(),
             physical_count: non_neg_integer()
           }}
          | :error
  def decode_state(blob, expected_prefix)
      when is_binary(blob) and byte_size(blob) <= @max_value_bytes and
             is_binary(expected_prefix) and expected_prefix != "" do
    case TermCodec.decode(blob) do
      {:ok, {@value_tag, 1, ^expected_prefix, count, expiring_count, physical_count}}
      when is_integer(count) and count >= 0 and count <= @max_u64 and
             is_integer(expiring_count) and expiring_count >= 0 and expiring_count <= count and
             is_integer(physical_count) and physical_count >= count and
             physical_count <= @max_u64 ->
        {:ok, %{count: count, expiring_count: expiring_count, physical_count: physical_count}}

      _invalid ->
        :error
    end
  rescue
    _error -> :error
  end

  def decode_state(_blob, _expected_prefix), do: :error

  @spec decode_value(binary(), binary()) :: {:ok, non_neg_integer()} | :error
  def decode_value(blob, expected_prefix)
      when is_binary(blob) and byte_size(blob) <= @max_value_bytes and
             is_binary(expected_prefix) and expected_prefix != "" do
    case decode_state(blob, expected_prefix) do
      {:ok, %{count: count}} -> {:ok, count}
      :error -> :error
    end
  end

  def decode_value(_blob, _expected_prefix), do: :error

  @spec decode_storage_entry(IndexDefinition.t(), binary(), binary()) ::
          {:ok,
           %{
             prefix: binary(),
             count: non_neg_integer(),
             expiring_count: non_neg_integer(),
             physical_count: non_neg_integer()
           }}
          | :error
  def decode_storage_entry(%IndexDefinition{} = definition, storage_key, blob)
      when is_binary(storage_key) and is_binary(blob) and byte_size(blob) <= @max_value_bytes do
    with :ok <- IndexDefinition.validate(definition) do
      decode_validated_storage_entry(definition, storage_key, blob)
    else
      _invalid -> :error
    end
  end

  def decode_storage_entry(%IndexDefinition{}, _storage_key, _blob), do: :error

  @doc false
  @spec decode_validated_storage_entry(IndexDefinition.t(), binary(), binary()) ::
          {:ok,
           %{
             prefix: binary(),
             count: non_neg_integer(),
             expiring_count: non_neg_integer(),
             physical_count: non_neg_integer()
           }}
          | :error
  def decode_validated_storage_entry(%IndexDefinition{} = definition, storage_key, blob)
      when is_binary(storage_key) and is_binary(blob) and byte_size(blob) <= @max_value_bytes do
    with {:ok, {@value_tag, 1, prefix, count, expiring_count, physical_count}} <-
           TermCodec.decode(blob),
         true <-
           is_binary(prefix) and prefix != "" and nonnegative_u64?(count) and
             nonnegative_u64?(expiring_count) and expiring_count <= count and
             nonnegative_u64?(physical_count) and physical_count >= count,
         true <- valid_declared_prefix?(definition, prefix),
         true <- storage_key == key(definition, prefix) do
      {:ok,
       %{
         prefix: prefix,
         count: count,
         expiring_count: expiring_count,
         physical_count: physical_count
       }}
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  end

  def decode_validated_storage_entry(%IndexDefinition{}, _storage_key, _blob), do: :error

  @spec read(binary(), IndexDefinition.t(), binary() | nil, [term()]) ::
          {:ok, non_neg_integer()} | {:error, atom() | term()}
  def read(path, %IndexDefinition{} = definition, scope_prefix, values)
      when is_binary(path) and (is_binary(scope_prefix) or is_nil(scope_prefix)) and
             is_list(values) do
    with {:ok, prefix} <- CompositeIndex.encode_prefix(definition, scope_prefix, values) do
      counter_key = key(definition, prefix)

      case LMDB.get(path, counter_key) do
        :not_found -> {:ok, 0}
        {:ok, blob} -> decode_read_value(blob, prefix)
        {:error, _reason} = error -> error
        _invalid -> {:error, :invalid_composite_counter_read}
      end
    end
  rescue
    _error -> {:error, :invalid_composite_counter_prefix}
  end

  @spec read_prefixes(binary(), IndexDefinition.t(), [binary()], pos_integer()) ::
          {:ok,
           %{
             counts: [non_neg_integer()],
             expiring_counts: [non_neg_integer()],
             scanned_entries: pos_integer(),
             scanned_bytes: non_neg_integer(),
             memory_bytes: non_neg_integer()
           }}
          | {:error, atom() | term()}
  def read_prefixes(path, %IndexDefinition{} = definition, prefixes, max_bytes)
      when is_binary(path) and path != "" and is_list(prefixes) and is_integer(max_bytes) and
             max_bytes > 0 do
    with :ok <- IndexDefinition.validate(definition),
         :ok <- validate_read_prefixes(definition, prefixes),
         counter_keys <- Enum.map(prefixes, &key(definition, &1)),
         {:ok, values, scanned_bytes} <-
           read_values(path, counter_keys, max_bytes, length(prefixes) * @max_value_bytes),
         {:ok, counts, expiring_counts} <- decode_read_values(values, prefixes, [], []) do
      {:ok,
       %{
         counts: counts,
         expiring_counts: expiring_counts,
         scanned_entries: length(prefixes),
         scanned_bytes: scanned_bytes,
         memory_bytes:
           :erlang.external_size(
             {counter_keys, values, counts, expiring_counts},
             minor_version: 2
           )
       }}
    end
  rescue
    _error -> {:error, :invalid_composite_counter_prefixes}
  end

  def read_prefixes(_path, %IndexDefinition{}, _prefixes, _max_bytes),
    do: {:error, :invalid_composite_counter_prefixes}

  @spec prefixes_for_keys([IndexDefinition.t()], [binary()]) ::
          {:ok, MapSet.t({IndexDefinition.t(), binary()})} | {:error, atom()}
  def prefixes_for_keys(definitions, keys) when is_list(definitions) and is_list(keys) do
    with true <- Enum.all?(definitions, &(IndexDefinition.validate(&1) == :ok)),
         true <- Enum.all?(keys, &is_binary/1) do
      {:ok,
       Enum.reduce(definitions, MapSet.new(), fn definition, acc ->
         definition_prefixes(definition, keys, acc)
       end)}
    else
      false -> {:error, :invalid_composite_counter_keys}
    end
  end

  def prefixes_for_keys(_definitions, _keys),
    do: {:error, :invalid_composite_counter_keys}

  @doc false
  @spec prefix_counts_for_keys([IndexDefinition.t()], [binary()]) ::
          {:ok, %{{IndexDefinition.t(), binary()} => pos_integer()}} | {:error, atom()}
  def prefix_counts_for_keys(definitions, keys) when is_list(definitions) and is_list(keys) do
    with true <- Enum.all?(definitions, &(IndexDefinition.validate(&1) == :ok)),
         true <- Enum.all?(keys, &is_binary/1) do
      {:ok,
       Enum.reduce(definitions, %{}, fn definition, acc ->
         definition_prefix_counts(definition, keys, acc)
       end)}
    else
      false -> {:error, :invalid_composite_counter_keys}
    end
  end

  def prefix_counts_for_keys(_definitions, _keys),
    do: {:error, :invalid_composite_counter_keys}

  @spec prefixes_for_key(IndexDefinition.t(), binary()) ::
          {:ok, [binary()]} | {:error, :invalid_composite_counter_key}
  def prefixes_for_key(%IndexDefinition{} = definition, key) when is_binary(key) do
    if IndexDefinition.validate(definition) == :ok do
      prefixes_for_validated_key(definition, key)
    else
      {:error, :invalid_composite_counter_key}
    end
  end

  def prefixes_for_key(%IndexDefinition{}, _key),
    do: {:error, :invalid_composite_counter_key}

  @doc false
  @spec prefixes_for_validated_key(IndexDefinition.t(), binary()) ::
          {:ok, [binary()]} | {:error, :invalid_composite_counter_key}
  def prefixes_for_validated_key(%IndexDefinition{} = definition, key) when is_binary(key) do
    storage_prefix = IndexDefinition.storage_prefix(definition)

    if byte_size(key) == entry_key_bytes(definition) and
         String.starts_with?(key, storage_prefix) do
      {:ok,
       Enum.map(definition.count_prefixes, fn prefix_length ->
         binary_part(key, 0, prefix_bytes(definition, prefix_length))
       end)}
    else
      {:error, :invalid_composite_counter_key}
    end
  end

  def prefixes_for_validated_key(%IndexDefinition{}, _key),
    do: {:error, :invalid_composite_counter_key}

  defp definition_prefixes(%IndexDefinition{count_prefixes: []}, _keys, acc), do: acc

  defp definition_prefixes(definition, keys, acc) do
    storage_prefix = IndexDefinition.storage_prefix(definition)
    entry_bytes = entry_key_bytes(definition)

    Enum.reduce(keys, acc, fn key, prefixes ->
      if byte_size(key) == entry_bytes and String.starts_with?(key, storage_prefix) do
        Enum.reduce(definition.count_prefixes, prefixes, fn prefix_length, inner ->
          size = prefix_bytes(definition, prefix_length)
          MapSet.put(inner, {definition, binary_part(key, 0, size)})
        end)
      else
        prefixes
      end
    end)
  end

  defp definition_prefix_counts(%IndexDefinition{count_prefixes: []}, _keys, acc), do: acc

  defp definition_prefix_counts(definition, keys, acc) do
    storage_prefix = IndexDefinition.storage_prefix(definition)
    entry_bytes = entry_key_bytes(definition)

    Enum.reduce(keys, acc, fn key, counts ->
      if byte_size(key) == entry_bytes and String.starts_with?(key, storage_prefix) do
        Enum.reduce(definition.count_prefixes, counts, fn prefix_length, inner ->
          size = prefix_bytes(definition, prefix_length)
          item = {definition, binary_part(key, 0, size)}
          Map.update(inner, item, 1, &(&1 + 1))
        end)
      else
        counts
      end
    end)
  end

  defp valid_declared_prefix?(definition, prefix) do
    storage_prefix = IndexDefinition.storage_prefix(definition)

    String.starts_with?(prefix, storage_prefix) and
      Enum.any?(definition.count_prefixes, fn prefix_length ->
        byte_size(prefix) == prefix_bytes(definition, prefix_length)
      end)
  end

  defp validate_read_prefixes(definition, prefixes) do
    valid =
      prefixes != [] and length(prefixes) <= @max_read_prefixes and
        length(prefixes) == length(Enum.uniq(prefixes)) and
        Enum.all?(prefixes, &(is_binary(&1) and valid_declared_prefix?(definition, &1)))

    if valid, do: :ok, else: {:error, :invalid_composite_counter_prefixes}
  end

  defp decode_read_values([], [], counts, expiring_counts),
    do: {:ok, Enum.reverse(counts), Enum.reverse(expiring_counts)}

  defp decode_read_values([:not_found | values], [_prefix | prefixes], counts, expiring_counts),
    do: decode_read_values(values, prefixes, [0 | counts], [0 | expiring_counts])

  defp decode_read_values(
         [{:ok, blob} | values],
         [prefix | prefixes],
         counts,
         expiring_counts
       ) do
    case decode_state(blob, prefix) do
      {:ok, %{count: count, expiring_count: expiring_count}} ->
        decode_read_values(
          values,
          prefixes,
          [count | counts],
          [expiring_count | expiring_counts]
        )

      :error ->
        {:error, :invalid_composite_counter}
    end
  end

  defp decode_read_values(_values, _prefixes, _counts, _expiring_counts),
    do: {:error, :invalid_composite_counter_read}

  defp read_values(path, keys, max_bytes, valid_value_ceiling) do
    read_limit = min(max_bytes, valid_value_ceiling)

    case LMDB.get_many_bounded(path, keys, read_limit) do
      {:error, :batch_value_budget_exceeded} when max_bytes >= valid_value_ceiling ->
        {:error, :invalid_composite_counter}

      result ->
        result
    end
  end

  defp prefix_bytes(definition, prefix_length) do
    byte_size(IndexDefinition.storage_prefix(definition)) + scope_storage_bytes(definition) +
      (definition.fields
       |> Enum.take(prefix_length)
       |> Enum.reduce(0, fn
         {_field, _direction, :hashed}, bytes -> bytes + @hash_component_bytes
         {_field, _direction, :ordered}, bytes -> bytes + @ordered_component_bytes
       end))
  end

  defp entry_key_bytes(definition) do
    byte_size(IndexDefinition.storage_prefix(definition)) + scope_storage_bytes(definition) +
      Enum.reduce(definition.fields, @entry_identity_bytes, fn
        {_field, _direction, :hashed}, bytes -> bytes + @hash_component_bytes
        {_field, _direction, :ordered}, bytes -> bytes + @ordered_component_bytes
      end)
  end

  defp scope_storage_bytes(%IndexDefinition{scope_bytes: 0}), do: 0

  defp scope_storage_bytes(%IndexDefinition{scope_bytes: scope_bytes}),
    do: @scope_header_bytes + scope_bytes

  defp decode_read_value(blob, prefix) do
    case decode_value(blob, prefix) do
      {:ok, count} -> {:ok, count}
      :error -> {:error, :invalid_composite_counter}
    end
  end

  defp nonnegative_u64?(value),
    do: is_integer(value) and value >= 0 and value <= @max_u64
end
