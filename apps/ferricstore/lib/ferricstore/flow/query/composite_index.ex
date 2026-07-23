defmodule Ferricstore.Flow.Query.CompositeIndex do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, StorageScope, SystemMetadata}
  alias Ferricstore.Flow.Query.{Field, IndexDefinition, Limits, TupleCodec}
  alias Ferricstore.TermCodec

  @hash_tag 0x50
  @scope_tag 0x70
  @entry_identity_tag 0x60
  @entry_identity_bytes 33
  @entry_value_version 1
  @reverse_prefix "flow-composite-reverse:1:"
  @reverse_value_tag :flow_composite_reverse
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @max_exact_integer 9_007_199_254_740_991
  @max_reverse_entries 128
  @lmdb_max_key_bytes 511
  @max_run_id_bytes Limits.max_run_id_bytes()
  @max_state_key_bytes Limits.max_state_key_bytes()
  @max_entry_value_bytes @max_run_id_bytes + @max_state_key_bytes + 256
  @max_reverse_value_bytes @max_state_key_bytes +
                             @max_reverse_entries * (@lmdb_max_key_bytes + 5) + 1_024

  @type entry :: %{
          key: binary(),
          value: binary(),
          index_id: binary(),
          index_version: pos_integer()
        }

  @opaque record_matcher ::
            {:flow_composite_record_matcher, binary(), [IndexDefinition.field_spec()]}

  @spec max_entries_per_record() :: pos_integer()
  def max_entries_per_record, do: @max_reverse_entries

  @doc false
  @spec max_reverse_value_bytes() :: pos_integer()
  def max_reverse_value_bytes, do: @max_reverse_value_bytes

  @spec entries(IndexDefinition.t(), map(), binary(), non_neg_integer()) ::
          {:ok, [entry()]} | {:error, atom()}
  def entries(
        %IndexDefinition{} = definition,
        record,
        state_key,
        expire_at_ms
      )
      when is_map(record) and is_binary(state_key) and state_key != "" and
             byte_size(state_key) <= @max_state_key_bytes and
             is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64 do
    with :ok <- IndexDefinition.validate(definition) do
      entries_validated(definition, record, state_key, expire_at_ms)
    end
  end

  def entries(%IndexDefinition{}, record, _state_key, _expire_at_ms) when is_map(record),
    do: {:error, :invalid_composite_record}

  def entries(%IndexDefinition{}, _record, _state_key, _expire_at_ms),
    do: {:error, :invalid_composite_record}

  @doc false
  @spec entries_validated(IndexDefinition.t(), map(), binary(), non_neg_integer()) ::
          {:ok, [entry()]} | {:error, atom()}
  def entries_validated(
        %IndexDefinition{} = definition,
        record,
        state_key,
        expire_at_ms
      )
      when is_map(record) and is_binary(state_key) and state_key != "" and
             byte_size(state_key) <= @max_state_key_bytes and
             is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64 do
    with {:ok, id} <- required_binary(record, :run_id),
         :ok <- validate_record_owner(record, id, state_key),
         {:ok, scope_prefix} <- record_scope_prefix(definition, record),
         {:ok, projection_record} <- logical_projection_record(record),
         {:ok, record_version} <- record_version(record),
         {:ok, value_sets} <- field_value_sets(definition, projection_record),
         :ok <- validate_projection_cardinality(value_sets),
         {:ok, keys} <- entry_keys(definition, scope_prefix, value_sets, id),
         value <- encode_entry_value(id, state_key, record_version, expire_at_ms) do
      {:ok,
       Enum.map(keys, fn key ->
         %{
           key: key,
           value: value,
           index_id: definition.id,
           index_version: definition.version
         }
       end)}
    end
  end

  def entries_validated(%IndexDefinition{}, record, _state_key, _expire_at_ms)
      when is_map(record),
      do: {:error, :invalid_composite_record}

  def entries_validated(%IndexDefinition{}, _record, _state_key, _expire_at_ms),
    do: {:error, :invalid_composite_record}

  @spec encode_prefix(IndexDefinition.t(), [term()]) :: {:ok, binary()} | {:error, atom()}
  def encode_prefix(%IndexDefinition{} = definition, values) when is_list(values) do
    encode_prefix(definition, nil, values)
  end

  def encode_prefix(%IndexDefinition{}, _values), do: {:error, :invalid_tuple_arity}

  @spec encode_prefix(IndexDefinition.t(), binary() | nil, [term()]) ::
          {:ok, binary()} | {:error, atom()}
  def encode_prefix(%IndexDefinition{} = definition, scope_prefix, values)
      when (is_binary(scope_prefix) or is_nil(scope_prefix)) and is_list(values) do
    with :ok <- IndexDefinition.validate(definition),
         true <- length(values) <= length(definition.fields),
         {:ok, prefix} <- encode_validated_prefix(definition, scope_prefix, values) do
      {:ok, prefix}
    else
      false -> {:error, :invalid_tuple_arity}
      {:error, _reason} = error -> error
    end
  end

  def encode_prefix(%IndexDefinition{}, _scope_prefix, _values),
    do: {:error, :invalid_tuple_arity}

  @spec reverse_key(binary()) :: binary()
  def reverse_key(state_key)
      when is_binary(state_key) and state_key != "" and
             byte_size(state_key) <= @max_state_key_bytes do
    @reverse_prefix <> :crypto.hash(:sha256, state_key)
  end

  @spec reverse_prefix() :: binary()
  def reverse_prefix, do: @reverse_prefix

  @spec encode_reverse_value(binary(), [binary()]) :: binary()
  def encode_reverse_value(state_key, keys)
      when is_binary(state_key) and is_list(keys),
      do: encode_reverse_value(state_key, keys, 0)

  @spec encode_reverse_value(binary(), [binary()], non_neg_integer()) :: binary()
  def encode_reverse_value(state_key, keys, expire_at_ms)
      when is_binary(state_key) and state_key != "" and
             byte_size(state_key) <= @max_state_key_bytes and is_list(keys) and
             is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64 do
    with {:ok, id} <- Keys.run_id_from_state_key(state_key),
         true <- valid_reverse_keys?(keys, id) do
      TermCodec.encode({@reverse_value_tag, 1, state_key, keys, expire_at_ms})
    else
      _invalid -> raise ArgumentError, "composite reverse keys are invalid"
    end
  end

  def encode_reverse_value(_state_key, _keys, _expire_at_ms),
    do: raise(ArgumentError, "composite reverse keys are invalid")

  @spec decode_reverse_state(binary(), binary()) ::
          {:ok, %{keys: [binary()], expire_at_ms: non_neg_integer()}} | :error
  def decode_reverse_state(blob, expected_state_key)
      when is_binary(blob) and byte_size(blob) <= @max_reverse_value_bytes and
             is_binary(expected_state_key) and expected_state_key != "" and
             byte_size(expected_state_key) <= @max_state_key_bytes do
    with {:ok, id} <- Keys.run_id_from_state_key(expected_state_key) do
      case TermCodec.decode(blob) do
        {:ok, {@reverse_value_tag, 1, ^expected_state_key, keys, expire_at_ms}}
        when is_list(keys) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
               expire_at_ms <= @max_u64 ->
          if valid_reverse_keys?(keys, id),
            do: {:ok, %{keys: keys, expire_at_ms: expire_at_ms}},
            else: :error

        _other ->
          :error
      end
    end
  rescue
    _error -> :error
  end

  def decode_reverse_state(_blob, _expected_state_key), do: :error

  @spec decode_reverse_value(binary(), binary()) :: {:ok, [binary()]} | :error
  def decode_reverse_value(blob, expected_state_key)
      when is_binary(blob) and byte_size(blob) <= @max_reverse_value_bytes and
             is_binary(expected_state_key) and expected_state_key != "" and
             byte_size(expected_state_key) <= @max_state_key_bytes do
    case decode_reverse_state(blob, expected_state_key) do
      {:ok, %{keys: keys}} -> {:ok, keys}
      :error -> :error
    end
  end

  def decode_reverse_value(_blob, _expected_state_key), do: :error

  @spec decode_reverse_row(binary(), binary()) ::
          {:ok, {binary(), [binary()], non_neg_integer()}} | :error
  def decode_reverse_row(key, blob)
      when is_binary(key) and is_binary(blob) and
             byte_size(blob) <= @max_reverse_value_bytes do
    case TermCodec.decode(blob) do
      {:ok, {@reverse_value_tag, 1, state_key, keys, expire_at_ms}}
      when is_binary(state_key) and state_key != "" and
             byte_size(state_key) <= @max_state_key_bytes and is_list(keys) and
             is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64 ->
        with {:ok, id} <- Keys.run_id_from_state_key(state_key),
             true <- key == reverse_key(state_key),
             true <- valid_reverse_keys?(keys, id) do
          {:ok, {state_key, keys, expire_at_ms}}
        else
          _invalid -> :error
        end

      _other ->
        :error
    end
  rescue
    _error -> :error
  end

  def decode_reverse_row(_key, _blob), do: :error

  @spec decode_entry_value(binary()) :: {:ok, map()} | :error
  def decode_entry_value(blob)
      when is_binary(blob) and byte_size(blob) <= @max_entry_value_bytes do
    with <<@entry_value_version, id_bytes::unsigned-big-32, record_version::unsigned-big-64,
           expire_at_ms::unsigned-big-64, payload::binary>> <-
           blob,
         true <- id_bytes > 0 and id_bytes <= @max_run_id_bytes,
         true <- id_bytes < byte_size(payload),
         <<id::binary-size(id_bytes), state_key::binary>> <- payload,
         true <- byte_size(state_key) <= @max_state_key_bytes,
         true <- record_version <= @max_exact_integer,
         {:ok, ^id} <- Keys.run_id_from_state_key(state_key) do
      {:ok,
       %{
         id: id,
         state_key: state_key,
         record_version: record_version,
         expire_at_ms: expire_at_ms
       }}
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  end

  def decode_entry_value(_blob), do: :error

  @spec entry_key_matches_id?(binary(), binary()) :: boolean()
  def entry_key_matches_id?(key, id)
      when is_binary(key) and is_binary(id) and id != "" and
             byte_size(id) <= @max_run_id_bytes do
    suffix = <<@entry_identity_tag, :crypto.hash(:sha256, id)::binary-size(32)>>

    byte_size(key) >= byte_size(suffix) and
      binary_part(key, byte_size(key) - byte_size(suffix), byte_size(suffix)) == suffix
  end

  def entry_key_matches_id?(_key, _id), do: false

  @doc false
  @spec entry_key_matches_record?(IndexDefinition.t(), map(), binary(), binary()) :: boolean()
  def entry_key_matches_record?(%IndexDefinition{} = definition, record, state_key, key)
      when is_map(record) and is_binary(state_key) and state_key != "" and is_binary(key) and
             byte_size(key) <= @lmdb_max_key_bytes do
    with :ok <- IndexDefinition.validate(definition) do
      entry_key_matches_record_validated?(definition, record, state_key, key)
    else
      _invalid -> false
    end
  rescue
    _error -> false
  end

  def entry_key_matches_record?(_definition, _record, _state_key, _key), do: false

  @doc false
  @spec entry_key_matches_record_validated?(IndexDefinition.t(), map(), binary(), binary()) ::
          boolean()
  def entry_key_matches_record_validated?(%IndexDefinition{} = definition, record, state_key, key)
      when is_map(record) and is_binary(state_key) and state_key != "" and is_binary(key) and
             byte_size(key) <= @lmdb_max_key_bytes do
    with {:ok, id} <- required_binary(record, :run_id),
         :ok <- validate_record_owner(record, id, state_key),
         {:ok, scope_prefix} <- record_scope_prefix(definition, record),
         {:ok, projection_record} <- logical_projection_record(record),
         {:ok, prefix} <- encode_validated_prefix(definition, scope_prefix, []),
         suffix = <<@entry_identity_tag, :crypto.hash(:sha256, id)::binary-size(32)>>,
         true <- byte_size(key) >= byte_size(prefix) + byte_size(suffix),
         true <- binary_part(key, 0, byte_size(prefix)) == prefix,
         true <-
           binary_part(key, byte_size(key) - byte_size(suffix), byte_size(suffix)) == suffix,
         component_bytes = byte_size(key) - byte_size(prefix) - byte_size(suffix),
         components = binary_part(key, byte_size(prefix), component_bytes),
         true <- key_components_match?(components, definition.fields, projection_record) do
      true
    else
      _invalid -> false
    end
  rescue
    _error -> false
  end

  def entry_key_matches_record_validated?(_definition, _record, _state_key, _key), do: false

  @doc false
  @spec prepare_record_matcher_validated(
          IndexDefinition.t(),
          binary() | nil,
          binary()
        ) :: {:ok, record_matcher()} | {:error, atom()}
  def prepare_record_matcher_validated(
        %IndexDefinition{fields: [{:partition_key, :asc, :hashed} | fields]} = definition,
        scope_prefix,
        logical_partition
      )
      when (is_binary(scope_prefix) or is_nil(scope_prefix)) and is_binary(logical_partition) and
             logical_partition != "" do
    with {:ok, prefix} <-
           encode_validated_prefix(definition, scope_prefix, [logical_partition]) do
      {:ok, {:flow_composite_record_matcher, prefix, fields}}
    end
  end

  def prepare_record_matcher_validated(%IndexDefinition{}, _scope_prefix, _logical_partition),
    do: {:error, :invalid_composite_record_matcher}

  @doc false
  @spec entry_key_matches_record_validated?(record_matcher(), map(), binary()) :: boolean()
  def entry_key_matches_record_validated?(
        {:flow_composite_record_matcher, prefix, fields},
        record,
        key
      )
      when is_binary(prefix) and prefix != "" and is_list(fields) and is_map(record) and
             is_binary(key) and byte_size(key) <= @lmdb_max_key_bytes do
    prefix_bytes = byte_size(prefix)
    component_bytes = byte_size(key) - prefix_bytes - @entry_identity_bytes

    if component_bytes >= 0 and String.starts_with?(key, prefix) do
      components = binary_part(key, prefix_bytes, component_bytes)
      identity = binary_part(key, prefix_bytes + component_bytes, @entry_identity_bytes)

      match?(<<@entry_identity_tag, _run_ref::binary-size(32)>>, identity) and
        key_components_match?(components, fields, record)
    else
      false
    end
  rescue
    _error -> false
  end

  def entry_key_matches_record_validated?(_matcher, _record, _key), do: false

  defp required_binary(record, field) do
    case Field.fetch(record, field) do
      {:ok, value}
      when is_binary(value) and value != "" and byte_size(value) <= @max_run_id_bytes ->
        {:ok, value}

      _other ->
        {:error, :invalid_composite_record}
    end
  end

  defp validate_record_owner(record, id, state_key) do
    case Field.fetch(record, :partition_key) do
      {:ok, partition_key} when is_binary(partition_key) and partition_key != "" ->
        if Keys.state_key(id, partition_key) == state_key,
          do: :ok,
          else: {:error, :invalid_composite_record}

      _other ->
        {:error, :unscoped_record}
    end
  end

  defp record_version(record) do
    case Field.fetch(record, :version) do
      {:ok, value}
      when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      :missing ->
        {:ok, 0}

      _other ->
        {:error, :invalid_composite_record}
    end
  end

  defp field_value_sets(%IndexDefinition{fields: fields}, record) do
    Enum.reduce_while(fields, {:ok, []}, fn {field, _direction, _encoding}, {:ok, acc} ->
      case projected_values(record, field) do
        {:ok, values} -> {:cont, {:ok, [values | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} ->
        value_sets = Enum.reverse(reversed)

        case value_sets do
          [[tenant] | _rest] when is_binary(tenant) and tenant != "" -> {:ok, value_sets}
          _other -> {:error, :unscoped_record}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp projected_values(record, {:attribute, _name} = field) do
    case Field.fetch(record, field) do
      :missing ->
        {:ok, [Field.missing()]}

      {:ok, values} when is_list(values) ->
        bounded_unique_values(values)

      {:ok, value} ->
        {:ok, [value]}
    end
  end

  defp projected_values(record, field) do
    case Field.fetch(record, field) do
      :missing -> {:ok, [Field.missing()]}
      {:ok, value} when is_list(value) -> {:error, :unsupported_index_value}
      {:ok, value} -> {:ok, [value]}
    end
  end

  defp bounded_unique_values([]), do: {:ok, [Field.missing()]}

  defp bounded_unique_values(values) do
    values
    |> Enum.reduce_while({MapSet.new(), []}, fn value, {seen, acc} ->
      cond do
        MapSet.member?(seen, value) ->
          {:cont, {seen, acc}}

        MapSet.size(seen) >= @max_reverse_entries ->
          {:halt, :too_many}

        true ->
          {:cont, {MapSet.put(seen, value), [value | acc]}}
      end
    end)
    |> case do
      {_seen, reversed} -> {:ok, Enum.reverse(reversed)}
      :too_many -> {:error, :too_many_composite_entries}
    end
  end

  defp entry_keys(definition, scope_prefix, value_sets, id) do
    suffix = <<@entry_identity_tag, :crypto.hash(:sha256, id)::binary-size(32)>>

    value_sets
    |> cartesian_values([[]])
    |> Enum.reduce_while({:ok, []}, fn values, {:ok, acc} ->
      case encode_validated_prefix(definition, scope_prefix, values) do
        {:ok, prefix} ->
          key = prefix <> suffix

          if byte_size(key) <= @lmdb_max_key_bytes,
            do: {:cont, {:ok, [key | acc]}},
            else: {:halt, {:error, :composite_key_too_large}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, keys} -> {:ok, keys |> Enum.reverse() |> Enum.uniq()}
      {:error, _reason} = error -> error
    end
  end

  defp validate_projection_cardinality(value_sets) do
    value_sets
    |> Enum.reduce_while(1, fn values, count ->
      value_count = length(values)

      if value_count > 0 and count <= div(@max_reverse_entries, value_count),
        do: {:cont, count * value_count},
        else: {:halt, :too_many}
    end)
    |> case do
      count when is_integer(count) and count > 0 -> :ok
      :too_many -> {:error, :too_many_composite_entries}
    end
  end

  defp cartesian_values([], acc), do: Enum.map(acc, &Enum.reverse/1)

  defp cartesian_values([values | rest], acc) do
    next = for prefix <- acc, value <- values, do: [value | prefix]
    cartesian_values(rest, next)
  end

  defp encode_components([], [], acc),
    do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}

  defp encode_components([value | values], [{field, _direction, :hashed} | fields], acc) do
    with :ok <- validate_component_type(field, value),
         {:ok, canonical} <- TupleCodec.encode_component_safe(value, :asc) do
      component = <<@hash_tag, :crypto.hash(:sha256, canonical)::binary-size(32)>>
      encode_components(values, fields, [component | acc])
    end
  end

  defp encode_components([value | values], [{field, direction, :ordered} | fields], acc) do
    with :ok <- validate_component_type(field, value),
         {:ok, component} <- TupleCodec.encode_component_safe(value, direction) do
      encode_components(values, fields, [component | acc])
    end
  end

  defp encode_components(_values, _fields, _acc), do: {:error, :invalid_tuple_arity}

  defp key_components_match?(<<>>, [], _record), do: true

  defp key_components_match?(components, [field_spec | fields], record) do
    {field, _direction, _encoding} = field_spec

    case projected_values(record, field) do
      {:ok, values} ->
        Enum.any?(values, fn value ->
          case encode_key_component(value, field_spec) do
            {:ok, encoded} when byte_size(components) >= byte_size(encoded) ->
              if binary_part(components, 0, byte_size(encoded)) == encoded do
                tail =
                  binary_part(
                    components,
                    byte_size(encoded),
                    byte_size(components) - byte_size(encoded)
                  )

                key_components_match?(tail, fields, record)
              else
                false
              end

            _invalid ->
              false
          end
        end)

      {:error, _reason} ->
        false
    end
  end

  defp key_components_match?(_components, _fields, _record), do: false

  defp encode_key_component(value, {field, _direction, :hashed}) do
    with :ok <- validate_component_type(field, value),
         {:ok, canonical} <- TupleCodec.encode_component_safe(value, :asc) do
      {:ok, <<@hash_tag, :crypto.hash(:sha256, canonical)::binary-size(32)>>}
    end
  end

  defp encode_key_component(value, {field, direction, :ordered}) do
    with :ok <- validate_component_type(field, value) do
      TupleCodec.encode_component_safe(value, direction)
    end
  end

  defp validate_tenant_value([tenant | _rest]) when is_binary(tenant) and tenant != "", do: :ok
  defp validate_tenant_value([]), do: :ok
  defp validate_tenant_value(_values), do: {:error, :unscoped_record}

  defp encode_validated_prefix(
         %IndexDefinition{fields: fields} = definition,
         scope_prefix,
         values
       ) do
    with :ok <- validate_tenant_value(values),
         {:ok, encoded_scope} <- encode_scope_prefix(definition, scope_prefix),
         {:ok, encoded} <- encode_components(values, Enum.take(fields, length(values)), []) do
      {:ok, IndexDefinition.storage_prefix(definition) <> encoded_scope <> encoded}
    end
  end

  defp record_scope_prefix(%IndexDefinition{} = definition, record) do
    with {:ok, scope_prefix} <-
           SystemMetadata.scope_prefix(Map.get(record, :system_metadata, %{})),
         {:ok, _encoded} <- encode_scope_prefix(definition, scope_prefix) do
      {:ok, scope_prefix}
    else
      _invalid -> {:error, :invalid_composite_scope}
    end
  end

  defp logical_projection_record(record) do
    case StorageScope.logical_partition_key(record) do
      {:ok, logical_partition_key} ->
        {:ok, Map.put(record, :partition_key, logical_partition_key)}

      {:error, _reason} ->
        {:error, :invalid_composite_scope}
    end
  end

  defp encode_scope_prefix(%IndexDefinition{scope_bytes: 0}, nil), do: {:ok, <<>>}

  defp encode_scope_prefix(%IndexDefinition{scope_bytes: scope_bytes}, scope_prefix)
       when scope_bytes > 0 and is_binary(scope_prefix) and byte_size(scope_prefix) == scope_bytes,
       do: {:ok, <<@scope_tag, scope_bytes::unsigned-big-16, scope_prefix::binary>>}

  defp encode_scope_prefix(%IndexDefinition{}, _scope_prefix),
    do: {:error, :invalid_composite_scope}

  defp validate_component_type(_field, value)
       when value == {:ferric_query, :missing} or is_nil(value),
       do: :ok

  defp validate_component_type(field, value) do
    case Field.value_type(field) do
      :integer when is_integer(value) -> :ok
      :keyword when is_binary(value) -> :ok
      :dynamic -> :ok
      _mismatch -> {:error, :invalid_index_value_type}
    end
  end

  defp encode_entry_value(id, state_key, record_version, expire_at_ms) do
    <<@entry_value_version, byte_size(id)::unsigned-big-32, record_version::unsigned-big-64,
      expire_at_ms::unsigned-big-64, id::binary, state_key::binary>>
  end

  defp valid_reverse_keys?(keys, id) when length(keys) <= @max_reverse_entries do
    keys != [] and length(keys) == length(Enum.uniq(keys)) and
      Enum.all?(keys, fn key ->
        is_binary(key) and byte_size(key) <= @lmdb_max_key_bytes and
          String.starts_with?(key, IndexDefinition.global_storage_prefix()) and
          entry_key_matches_id?(key, id)
      end)
  end

  defp valid_reverse_keys?(_keys, _id), do: false
end
