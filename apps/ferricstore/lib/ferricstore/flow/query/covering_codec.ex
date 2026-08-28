defmodule Ferricstore.Flow.Query.CoveringCodec do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Flow.{Attributes, StateMeta}
  alias Ferricstore.Flow.Query.{Field, Limits}

  @version 2
  @nil_tag 0
  @integer_tag 1
  @binary_tag 2
  @false_tag 3
  @true_tag 4
  @float_tag 5
  @list_tag 6
  @maximum_bytes 64 * 1_024
  @maximum_fields Limits.max_return_fields()
  @maximum_record_fields @maximum_fields + 2
  @maximum_dynamic_name_bytes 512
  @maximum_list_values 16
  @maximum_exact_integer 9_007_199_254_740_991
  @minimum_integer -0x8000_0000_0000_0000
  @maximum_integer 0x7FFF_FFFF_FFFF_FFFF
  @float_exponent_mask 0x7FF
  @float_exponent_shift 52
  @maximum_id_bytes Limits.max_run_id_bytes()

  @fields [
    :type,
    :state,
    :priority,
    :partition_key,
    :created_at_ms,
    :updated_at_ms,
    :next_run_at_ms,
    :lease_deadline_ms,
    :attempts,
    :run_state,
    :max_active_ms,
    :parent_flow_id,
    :root_flow_id,
    :correlation_id
  ]
  @supported_fields [:run_id, :version | @fields]
  @field_indexes @fields |> Enum.with_index() |> Map.new()
  @valid_bitmap (1 <<< length(@fields)) - 1
  @integer_fields @fields
                  |> Enum.filter(&(Field.value_type(&1) == :integer))
                  |> MapSet.new()

  @type value :: nil | boolean() | integer() | float() | binary() | [value()]
  @type decoded_record :: %{required(:id) => binary(), required(:version) => non_neg_integer()}

  @doc false
  @spec max_encoded_bytes() :: pos_integer()
  def max_encoded_bytes, do: @maximum_bytes

  @doc false
  @spec max_fields() :: pos_integer()
  def max_fields, do: @maximum_fields

  @doc false
  @spec max_record_fields() :: pos_integer()
  def max_record_fields, do: @maximum_record_fields

  @doc false
  @spec supported_fields() :: [Field.builtin()]
  def supported_fields, do: @supported_fields

  @doc false
  @spec supported_field?(term()) :: boolean()
  def supported_field?(field), do: Field.valid?(field) and field != :event_id

  @doc false
  @spec valid_record?(map(), binary(), non_neg_integer()) :: boolean()
  def valid_record?(record, id, version)
      when is_map(record) and map_size(record) >= 3 and
             map_size(record) <= @maximum_record_fields and is_binary(id) and id != "" and
             byte_size(id) <= @maximum_id_bytes and is_integer(version) and version >= 0 and
             version <= @maximum_exact_integer do
    validate_record(record, id, version) == :ok
  end

  def valid_record?(_record, _id, _version), do: false

  @spec encode(map()) :: {:ok, binary()} | :error
  def encode(record)
      when is_map(record) and map_size(record) >= 3 and
             map_size(record) <= @maximum_record_fields do
    with :ok <- validate_identity(record),
         :ok <- validate_dynamic_record(record),
         {:ok, bitmap, indexed, dynamic} <- encode_fields(record),
         true <- length(indexed) + length(dynamic) <= @maximum_fields,
         indexed_values = indexed |> Enum.sort() |> Enum.map(&elem(&1, 1)),
         dynamic_values = dynamic |> Enum.sort() |> Enum.map(&elem(&1, 1)),
         encoded =
           IO.iodata_to_binary([
             <<@version, bitmap::unsigned-big-16, length(dynamic)::unsigned-big-8>>,
             indexed_values,
             dynamic_values
           ]),
         true <- byte_size(encoded) <= @maximum_bytes do
      {:ok, encoded}
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  def encode(_record), do: :error

  @spec decode(binary(), binary(), non_neg_integer()) :: {:ok, decoded_record()} | :error
  def decode(encoded, id, version)
      when is_binary(encoded) and byte_size(encoded) >= 4 and
             byte_size(encoded) <= @maximum_bytes and is_binary(id) and id != "" and
             byte_size(id) <= @maximum_id_bytes and is_integer(version) and version >= 0 and
             version <= @maximum_exact_integer do
    with <<@version, bitmap::unsigned-big-16, dynamic_count::unsigned-big-8, values::binary>> <-
           encoded,
         true <- (bitmap &&& bnot(@valid_bitmap)) == 0,
         true <- popcount(bitmap) + dynamic_count <= @maximum_fields,
         {:ok, record, dynamic_values} <- decode_fields(@fields, bitmap, values, %{}),
         {:ok, record, <<>>} <-
           decode_dynamic_fields(dynamic_count, dynamic_values, nil, record),
         record = record |> Map.put(:id, id) |> Map.put(:version, version),
         :ok <- validate_record(record, id, version) do
      {:ok, record}
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  def decode(_encoded, _id, _version), do: :error

  defp validate_identity(%{id: id, version: version, partition_key: partition})
       when is_binary(id) and id != "" and byte_size(id) <= @maximum_id_bytes and
              is_integer(version) and version >= 0 and version <= @maximum_exact_integer and
              (is_nil(partition) or (is_binary(partition) and partition != "")),
       do: :ok

  defp validate_identity(_record), do: :error

  defp validate_record(record, id, version) do
    if Map.get(record, :id) == id and Map.get(record, :version) == version and
         Map.has_key?(record, :partition_key) and valid_partition?(record.partition_key) do
      with :ok <-
             Enum.reduce_while(record, :ok, fn
               {:id, ^id}, :ok ->
                 {:cont, :ok}

               {:version, ^version}, :ok ->
                 {:cont, :ok}

               {field, value}, :ok ->
                 if supported_storage_field?(field) and validate_value(field, value) == :ok,
                   do: {:cont, :ok},
                   else: {:halt, :error}
             end),
           :ok <- validate_dynamic_record(record) do
        :ok
      end
    else
      :error
    end
  end

  defp valid_partition?(nil), do: true
  defp valid_partition?(partition), do: is_binary(partition) and partition != ""

  defp encode_fields(record) do
    Enum.reduce_while(record, {:ok, 0, [], []}, fn
      {field, _value}, {:ok, bitmap, indexed, dynamic} when field in [:id, :version] ->
        {:cont, {:ok, bitmap, indexed, dynamic}}

      {field, value}, {:ok, bitmap, indexed, dynamic} when is_atom(field) ->
        with {:ok, index} <- Map.fetch(@field_indexes, field),
             :ok <- validate_value(field, value),
             {:ok, encoded} <- encode_scalar(value) do
          {:cont, {:ok, bitmap ||| 1 <<< index, [{index, encoded} | indexed], dynamic}}
        else
          _invalid -> {:halt, :error}
        end

      {field, value}, {:ok, bitmap, indexed, dynamic} ->
        with true <- supported_dynamic_field?(field),
             :ok <- validate_value(field, value),
             {:ok, encoded_value} <- encode_dynamic_value(value),
             name = Field.external_name(field),
             true <- byte_size(name) > 0 and byte_size(name) <= @maximum_dynamic_name_bytes,
             encoded =
               <<byte_size(name)::unsigned-big-16, name::binary,
                 byte_size(encoded_value)::unsigned-big-32, encoded_value::binary>> do
          {:cont, {:ok, bitmap, indexed, [{name, encoded} | dynamic]}}
        else
          _invalid -> {:halt, :error}
        end
    end)
    |> case do
      {:ok, bitmap, indexed, dynamic} -> {:ok, bitmap, indexed, dynamic}
      :error -> :error
    end
  end

  defp decode_fields([], 0, rest, record), do: {:ok, record, rest}

  defp decode_fields([field | fields], bitmap, values, record) do
    if (bitmap &&& 1) == 1 do
      with {:ok, value, rest} <- decode_scalar(values),
           :ok <- validate_value(field, value) do
        decode_fields(fields, bitmap >>> 1, rest, Map.put(record, field, value))
      else
        _invalid -> :error
      end
    else
      decode_fields(fields, bitmap >>> 1, values, record)
    end
  end

  defp decode_fields(_fields, _bitmap, _values, _record), do: :error

  defp decode_dynamic_fields(0, rest, _previous_name, record), do: {:ok, record, rest}

  defp decode_dynamic_fields(count, encoded, previous_name, record) when count > 0 do
    with <<name_bytes::unsigned-big-16, rest::binary>> <- encoded,
         true <- name_bytes > 0 and name_bytes <= @maximum_dynamic_name_bytes,
         <<name::binary-size(^name_bytes), value_bytes::unsigned-big-32, rest::binary>> <- rest,
         true <- value_bytes > 0 and value_bytes <= @maximum_bytes,
         <<encoded_value::binary-size(^value_bytes), rest::binary>> <- rest,
         true <- is_nil(previous_name) or name > previous_name,
         {:ok, field} <- Field.parse(name),
         true <- supported_dynamic_field?(field),
         {:ok, value} <- decode_dynamic_value(encoded_value),
         :ok <- validate_value(field, value) do
      decode_dynamic_fields(
        count - 1,
        rest,
        detach_binary(name),
        Map.put(record, detach_field(field), value)
      )
    else
      _invalid -> :error
    end
  end

  defp decode_dynamic_fields(_count, _encoded, _previous_name, _record), do: :error

  defp encode_dynamic_value(value)
       when is_list(value) and length(value) <= @maximum_list_values do
    with {:ok, values} <- encode_list_values(value, []) do
      {:ok, IO.iodata_to_binary([<<@list_tag, length(value)::unsigned-big-16>>, values])}
    end
  end

  defp encode_dynamic_value(value) when is_list(value), do: :error
  defp encode_dynamic_value(value), do: encode_scalar(value)

  defp encode_list_values([], acc), do: {:ok, Enum.reverse(acc)}

  defp encode_list_values([value | values], acc) do
    case encode_scalar(value) do
      {:ok, encoded} -> encode_list_values(values, [encoded | acc])
      :error -> :error
    end
  end

  defp decode_dynamic_value(<<@list_tag, count::unsigned-big-16, values::binary>>)
       when count <= @maximum_list_values do
    with {:ok, decoded, <<>>} <- decode_list_values(count, values, []) do
      {:ok, decoded}
    end
  end

  defp decode_dynamic_value(encoded) do
    with {:ok, value, <<>>} <- decode_scalar(encoded), do: {:ok, value}
  end

  defp decode_list_values(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_list_values(count, encoded, acc) when count > 0 do
    with {:ok, value, rest} <- decode_scalar(encoded) do
      decode_list_values(count - 1, rest, [value | acc])
    end
  end

  defp encode_scalar(nil), do: {:ok, <<@nil_tag>>}
  defp encode_scalar(false), do: {:ok, <<@false_tag>>}
  defp encode_scalar(true), do: {:ok, <<@true_tag>>}

  defp encode_scalar(value)
       when is_integer(value) and value >= @minimum_integer and value <= @maximum_integer,
       do: {:ok, <<@integer_tag, value::signed-big-64>>}

  defp encode_scalar(value) when is_float(value) do
    <<bits::unsigned-big-64>> = <<value::float-big-64>>

    if finite_float_bits?(bits),
      do: {:ok, <<@float_tag, bits::unsigned-big-64>>},
      else: :error
  end

  defp encode_scalar(value) when is_binary(value) and byte_size(value) <= @maximum_bytes,
    do: {:ok, <<@binary_tag, byte_size(value)::unsigned-big-32, value::binary>>}

  defp encode_scalar(_value), do: :error

  defp decode_scalar(<<@nil_tag, rest::binary>>), do: {:ok, nil, rest}
  defp decode_scalar(<<@false_tag, rest::binary>>), do: {:ok, false, rest}
  defp decode_scalar(<<@true_tag, rest::binary>>), do: {:ok, true, rest}

  defp decode_scalar(<<@integer_tag, value::signed-big-64, rest::binary>>),
    do: {:ok, value, rest}

  defp decode_scalar(<<@float_tag, bits::unsigned-big-64, rest::binary>>) do
    if finite_float_bits?(bits) do
      <<value::float-big-64>> = <<bits::unsigned-big-64>>
      {:ok, value, rest}
    else
      :error
    end
  end

  defp decode_scalar(
         <<@binary_tag, bytes::unsigned-big-32, value::binary-size(bytes), rest::binary>>
       )
       when bytes <= @maximum_bytes,
       do: {:ok, detach_binary(value), rest}

  defp decode_scalar(_encoded), do: :error

  defp validate_value(_field, nil), do: :ok

  defp validate_value(field, value) when is_atom(field) do
    cond do
      MapSet.member?(@integer_fields, field) and is_integer(value) and
        value >= @minimum_integer and value <= @maximum_integer ->
        :ok

      MapSet.member?(@integer_fields, field) ->
        :error

      is_binary(value) and byte_size(value) <= @maximum_bytes ->
        :ok

      true ->
        :error
    end
  end

  defp validate_value({:attribute, _name} = field, _value),
    do: if(supported_dynamic_field?(field), do: :ok, else: :error)

  defp validate_value({:state_meta, _state, _name} = field, _value),
    do: if(supported_dynamic_field?(field), do: :ok, else: :error)

  defp validate_value(_field, _value), do: :error

  defp validate_dynamic_record(record) do
    {attributes, state_meta} =
      Enum.reduce(record, {%{}, %{}}, fn
        {{:attribute, name}, value}, {attributes, state_meta} ->
          {Map.put(attributes, name, value), state_meta}

        {{:state_meta, state, name}, value}, {attributes, state_meta} ->
          {attributes, Map.update(state_meta, state, %{name => value}, &Map.put(&1, name, value))}

        _field, acc ->
          acc
      end)

    if Attributes.valid_normalized?(attributes) and StateMeta.valid_normalized?(state_meta),
      do: :ok,
      else: :error
  end

  defp supported_storage_field?(field) when is_atom(field), do: field in @fields
  defp supported_storage_field?(field), do: supported_dynamic_field?(field)

  defp supported_dynamic_field?({:attribute, _name} = field), do: Field.valid?(field)
  defp supported_dynamic_field?({:state_meta, _state, _name} = field), do: Field.valid?(field)
  defp supported_dynamic_field?(_field), do: false

  defp finite_float_bits?(bits),
    do: (bits >>> @float_exponent_shift &&& @float_exponent_mask) != @float_exponent_mask

  defp detach_field({:attribute, name}), do: {:attribute, :binary.copy(name)}

  defp detach_field({:state_meta, state, name}),
    do: {:state_meta, :binary.copy(state), :binary.copy(name)}

  defp detach_binary(value) when byte_size(value) > 64, do: :binary.copy(value)
  defp detach_binary(value), do: value

  defp popcount(value), do: do_popcount(value, 0)
  defp do_popcount(0, count), do: count
  defp do_popcount(value, count), do: do_popcount(value &&& value - 1, count + 1)
end
