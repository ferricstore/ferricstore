defmodule Ferricstore.Flow.Query.QueryRowCodec do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Flow.{
    Attributes,
    Keys,
    Locator,
    StateMeta,
    StorageScope,
    SystemMetadata
  }

  alias Ferricstore.Flow.RecordProjection, as: FlowRecordProjection

  alias Ferricstore.Flow.Query.{
    CoveringCodec,
    Field,
    Limits,
    QueryRow,
    QueryRowDecodePlan,
    QueryRowReference,
    QueryRowPrimitives,
    RunRecordFields
  }

  @format "ferric.flow.query.row/v1"
  @magic <<0xF7, "FQR", 1>>
  @maximum_encoded_bytes 32 * 1_024
  @maximum_attributes 16
  @maximum_attribute_bytes 2_048
  @maximum_attribute_section_bytes 4_096
  @maximum_state_meta_states 64
  @maximum_state_meta_entries 16
  @maximum_state_meta_bytes 16_384
  @maximum_projection_config_bytes 512
  @maximum_dynamic_name_bytes 64
  @maximum_dynamic_value_bytes 256
  @locator_checksum_bytes 32
  @maximum_locator_bytes 104
  @maximum_scope_bytes 8 * 1_024
  @row_checksum_bytes 4
  @maximum_varint_bytes 10
  @maximum_row_header_bytes byte_size(@magic) + @row_checksum_bytes +
                              2 * @maximum_varint_bytes + 2 + 2 + 2
  @maximum_metadata_bytes @maximum_encoded_bytes - @maximum_row_header_bytes -
                            @maximum_locator_bytes
  @maximum_id_bytes Limits.max_run_id_bytes()
  @maximum_state_key_bytes Limits.max_state_key_bytes()
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @root_is_id_flag 1
  @locator_raft_index_flag 1
  @locator_segment_generation_flag 2
  @locator_nil_expiry_flag 4
  @locator_expiry_override_flag 8
  @locator_frame_size_flag 16
  @valid_locator_flags @locator_raft_index_flag ||| @locator_segment_generation_flag |||
                         @locator_nil_expiry_flag ||| @locator_expiry_override_flag |||
                         @locator_frame_size_flag

  @builtin_storage_fields RunRecordFields.builtins()
  @builtin_encoding_fields @builtin_storage_fields -- [:id, :version]
  @valid_builtin_bitmap (1 <<< length(@builtin_encoding_fields)) - 1
  @internal_storage_fields [
    :state_enter_seq,
    :history_max_events,
    :history_hot_max_events,
    :lease_owner
  ]
  @valid_internal_flags (1 <<< length(@internal_storage_fields)) - 1

  @spec max_encoded_bytes() :: pos_integer()
  def max_encoded_bytes, do: @maximum_encoded_bytes

  @doc false
  @spec format() :: binary()
  def format, do: @format

  @spec max_metadata_bytes() :: pos_integer()
  def max_metadata_bytes, do: @maximum_metadata_bytes

  @spec validate_record(binary(), map()) :: :ok | {:error, binary()}
  def validate_record(state_key, record) when is_binary(state_key) and is_map(record) do
    case validate_and_encode_metadata(state_key, record) do
      {:ok, _public_record, _id, _version, _scope_prefix, _encoded_metadata} ->
        :ok

      {:error, :metadata_too_large} ->
        {:error, "ERR flow query metadata exceeds #{@maximum_metadata_bytes} bytes"}

      _invalid ->
        {:error, "ERR invalid flow query metadata"}
    end
  end

  def validate_record(_state_key, _record), do: {:error, "ERR invalid flow query metadata"}

  @spec encode(binary(), map(), Locator.t(), non_neg_integer()) :: {:ok, binary()} | :error
  def encode(state_key, record, %Locator{} = locator, expire_at_ms)
      when is_binary(state_key) and is_map(record) and is_integer(expire_at_ms) and
             expire_at_ms >= 0 and expire_at_ms <= @max_u64 do
    with {:ok, _public_record, id, version, scope_prefix, encoded_metadata} <-
           validate_and_encode_metadata(state_key, record) do
      encode_components(
        state_key,
        id,
        version,
        scope_prefix,
        encoded_metadata,
        locator,
        expire_at_ms
      )
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  def encode(_state_key, _record, _locator, _expire_at_ms), do: :error

  @spec encode(QueryRow.t()) :: {:ok, binary()} | :error
  def encode(%QueryRow{} = row) do
    encode_public_row(
      row.state_key,
      row.record,
      row.scope_prefix,
      row.locator,
      row.expire_at_ms
    )
  end

  defp encode_public_row(
         state_key,
         record,
         scope_prefix,
         %Locator{} = locator,
         expire_at_ms
       )
       when is_binary(state_key) and is_map(record) and is_integer(expire_at_ms) and
              expire_at_ms >= 0 and expire_at_ms <= @max_u64 do
    with {:ok, state_key_id} <- decode_state_key(state_key),
         :ok <- validate_scope_prefix(scope_prefix),
         {:ok, id, version} <-
           validate_record_identity(record, state_key, scope_prefix, state_key_id),
         {:ok, encoded_metadata} <- encode_metadata(record),
         true <-
           scope_bytes(scope_prefix) + byte_size(encoded_metadata) <= @maximum_metadata_bytes do
      encode_components(
        state_key,
        id,
        version,
        scope_prefix,
        encoded_metadata,
        locator,
        expire_at_ms
      )
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  defp encode_public_row(_state_key, _record, _scope_prefix, _locator, _expire_at_ms), do: :error

  defp encode_components(
         state_key,
         id,
         version,
         scope_prefix,
         encoded_metadata,
         %Locator{} = locator,
         expire_at_ms
       ) do
    encoded_scope = scope_prefix || <<>>

    with :ok <- validate_locator_identity(locator, id, version),
         :ok <- validate_locator_expiry(locator, expire_at_ms),
         {:ok, encoded_version} <- QueryRowPrimitives.encode_u64(version),
         {:ok, encoded_expiry} <- QueryRowPrimitives.encode_u64(expire_at_ms),
         {:ok, encoded_locator} <- encode_locator(locator, expire_at_ms),
         true <- byte_size(encoded_scope) <= @maximum_scope_bytes,
         true <- byte_size(encoded_locator) <= @maximum_locator_bytes,
         true <- byte_size(encoded_metadata) <= 0xFFFF,
         row_body = [
           encoded_version,
           encoded_expiry,
           <<byte_size(encoded_scope)::unsigned-big-16,
             byte_size(encoded_locator)::unsigned-big-16,
             byte_size(encoded_metadata)::unsigned-big-16>>,
           encoded_scope,
           encoded_locator,
           encoded_metadata
         ],
         true <-
           byte_size(@magic) + @row_checksum_bytes + IO.iodata_length(row_body) <=
             @maximum_encoded_bytes,
         checksum = :erlang.crc32([state_key, row_body]),
         encoded = IO.iodata_to_binary([@magic, <<checksum::unsigned-big-32>>, row_body]) do
      {:ok, encoded}
    else
      _invalid -> :error
    end
  end

  @spec decode(binary(), binary()) :: {:ok, QueryRow.t()} | :error
  def decode(encoded, state_key) when is_binary(encoded) and is_binary(state_key) do
    decode_row(encoded, state_key)
  end

  def decode(_encoded, _state_key), do: :error

  @spec decode(binary(), binary(), non_neg_integer()) :: {:ok, QueryRow.t()} | :expired | :error
  def decode(encoded, state_key, now_ms)
      when is_binary(encoded) and is_binary(state_key) and is_integer(now_ms) and now_ms >= 0 and
             now_ms <= @max_u64 do
    case decode_row(encoded, state_key) do
      {:ok, %QueryRow{expire_at_ms: expire_at_ms}}
      when expire_at_ms > 0 and expire_at_ms <= now_ms ->
        :expired

      result ->
        result
    end
  end

  def decode(_encoded, _state_key, _now_ms), do: :error

  @doc false
  @spec decode_for_query(binary(), binary(), non_neg_integer(), QueryRowDecodePlan.t()) ::
          {:ok, QueryRow.t()} | :expired | :error
  def decode_for_query(encoded, state_key, now_ms, %QueryRowDecodePlan{} = decode_plan)
      when is_binary(encoded) and is_binary(state_key) and is_integer(now_ms) and now_ms >= 0 and
             now_ms <= @max_u64 do
    if QueryRowDecodePlan.valid?(decode_plan) do
      case decode_row(encoded, state_key, decode_plan) do
        {:ok, %QueryRow{expire_at_ms: expire_at_ms}}
        when expire_at_ms > 0 and expire_at_ms <= now_ms ->
          :expired

        result ->
          result
      end
    else
      :error
    end
  end

  def decode_for_query(_encoded, _state_key, _now_ms, _decode_plan), do: :error

  @doc false
  @spec decode_reference(binary(), binary(), non_neg_integer()) ::
          {:ok, QueryRowReference.t()} | :expired | :error
  def decode_reference(encoded, state_key, now_ms)
      when is_binary(encoded) and is_binary(state_key) and is_integer(now_ms) and now_ms >= 0 and
             now_ms <= @max_u64 do
    case decode_reference_row(encoded, state_key) do
      {:ok, %QueryRowReference{expire_at_ms: expire_at_ms}}
      when expire_at_ms > 0 and expire_at_ms <= now_ms ->
        :expired

      result ->
        result
    end
  end

  def decode_reference(_encoded, _state_key, _now_ms), do: :error

  @spec relocate(binary(), binary(), Locator.t(), Locator.t()) ::
          {:ok, binary()}
          | {:error, :logical_generation_mismatch | :locator_compare_failed | :invalid_query_row}
  def relocate(encoded, state_key, %Locator{} = expected, %Locator{} = relocated)
      when is_binary(encoded) and is_binary(state_key) do
    with true <- Locator.same_logical_record?(expected, relocated),
         {:ok,
          %QueryRow{
            record: record,
            locator: current,
            expire_at_ms: expire_at_ms,
            scope_prefix: scope_prefix
          }} <-
           decode(encoded, state_key),
         true <- Locator.same_physical_record?(current, expected),
         {:ok, relocated_row} <-
           encode_public_row(state_key, record, scope_prefix, relocated, expire_at_ms) do
      {:ok, relocated_row}
    else
      false ->
        if Locator.same_logical_record?(expected, relocated),
          do: {:error, :locator_compare_failed},
          else: {:error, :logical_generation_mismatch}

      :error ->
        {:error, :invalid_query_row}
    end
  rescue
    _error -> {:error, :invalid_query_row}
  catch
    _kind, _reason -> {:error, :invalid_query_row}
  end

  def relocate(_encoded, _state_key, _expected, _relocated),
    do: {:error, :invalid_query_row}

  defp decode_row(encoded, state_key), do: decode_row(encoded, state_key, :full)

  defp decode_row(encoded, state_key, decode_plan)
       when byte_size(encoded) <= @maximum_encoded_bytes and
              byte_size(state_key) <= @maximum_state_key_bytes do
    with {:ok, id} <- decode_state_key(state_key),
         <<@magic::binary, expected_checksum::unsigned-big-32, row_body::binary>> <- encoded,
         true <- :erlang.crc32([state_key, row_body]) == expected_checksum,
         {:ok, version, rest} <- QueryRowPrimitives.decode_u64(row_body),
         {:ok, expire_at_ms, rest} <- QueryRowPrimitives.decode_u64(rest),
         <<scope_bytes::unsigned-big-16, locator_bytes::unsigned-big-16,
           metadata_bytes::unsigned-big-16, payload::binary>> <- rest,
         true <- scope_bytes <= @maximum_scope_bytes,
         true <- locator_bytes > 0 and locator_bytes <= @maximum_locator_bytes,
         true <- metadata_bytes <= @maximum_metadata_bytes,
         true <- scope_bytes + metadata_bytes <= @maximum_metadata_bytes,
         true <- byte_size(payload) == scope_bytes + locator_bytes + metadata_bytes,
         <<encoded_scope::binary-size(^scope_bytes), encoded_locator::binary-size(^locator_bytes),
           encoded_metadata::binary-size(^metadata_bytes)>> <- payload,
         {:ok, scope_prefix} <- decode_scope_prefix(encoded_scope),
         {:ok, locator} <- decode_locator(encoded_locator, id, version, expire_at_ms),
         {:ok, record} <- decode_metadata(encoded_metadata, id, version, decode_plan),
         {:ok, ^id, ^version} <-
           validate_identity(record, state_key, scope_prefix, locator, id) do
      {:ok,
       %QueryRow{
         state_key: :binary.copy(state_key),
         record: record,
         locator: locator,
         expire_at_ms: expire_at_ms,
         scope_prefix: scope_prefix
       }}
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  defp decode_row(_encoded, _state_key, _decode_plan), do: :error

  defp decode_reference_row(encoded, state_key)
       when byte_size(encoded) <= @maximum_encoded_bytes and
              byte_size(state_key) <= @maximum_state_key_bytes do
    with {:ok, id} <- decode_state_key(state_key),
         <<@magic::binary, expected_checksum::unsigned-big-32, row_body::binary>> <- encoded,
         true <- :erlang.crc32([state_key, row_body]) == expected_checksum,
         {:ok, version, rest} <- QueryRowPrimitives.decode_u64(row_body),
         {:ok, expire_at_ms, rest} <- QueryRowPrimitives.decode_u64(rest),
         <<scope_bytes::unsigned-big-16, locator_bytes::unsigned-big-16,
           metadata_bytes::unsigned-big-16, payload::binary>> <- rest,
         true <- scope_bytes <= @maximum_scope_bytes,
         true <- locator_bytes > 0 and locator_bytes <= @maximum_locator_bytes,
         true <- metadata_bytes <= @maximum_metadata_bytes,
         true <- scope_bytes + metadata_bytes <= @maximum_metadata_bytes,
         true <- byte_size(payload) == scope_bytes + locator_bytes + metadata_bytes,
         <<_encoded_scope::binary-size(^scope_bytes),
           encoded_locator::binary-size(^locator_bytes),
           _encoded_metadata::binary-size(^metadata_bytes)>> <- payload,
         {:ok, locator} <- decode_locator(encoded_locator, id, version, expire_at_ms) do
      {:ok,
       %QueryRowReference{
         state_key: :binary.copy(state_key),
         flow_id: id,
         version: version,
         locator: locator,
         expire_at_ms: expire_at_ms
       }}
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  defp decode_reference_row(_encoded, _state_key), do: :error

  defp public_record(record) do
    public =
      record
      |> FlowRecordProjection.public()
      |> Map.merge(Map.take(record, @internal_storage_fields))

    if is_map(public), do: {:ok, public}, else: :error
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  defp validate_identity(record, state_key, scope_prefix, %Locator{} = locator, state_key_id) do
    with {:ok, id, version} <-
           validate_record_identity(record, state_key, scope_prefix, state_key_id),
         :ok <- validate_locator_identity(locator, id, version) do
      {:ok, id, version}
    end
  end

  defp validate_record_identity(record, state_key, scope_prefix, state_key_id) do
    with id when is_binary(id) and id != "" and byte_size(id) <= @maximum_id_bytes <-
           Map.get(record, :id),
         true <- id == state_key_id,
         version when is_integer(version) and version >= 0 and version <= @max_u64 <-
           Map.get(record, :version),
         partition when is_nil(partition) or (is_binary(partition) and partition != "") <-
           Map.get(record, :partition_key),
         {:ok, physical_partition_key} <- physical_partition_key(partition, scope_prefix),
         true <- Keys.state_key(id, physical_partition_key) == state_key do
      {:ok, id, version}
    else
      _invalid -> :error
    end
  end

  defp validate_locator_identity(%Locator{} = locator, id, version) do
    if Locator.hydration_ready?(locator) and locator.flow_id == id and
         locator.version == version,
       do: :ok,
       else: :error
  end

  defp validate_and_encode_metadata(state_key, record) do
    with {:ok, state_key_id} <- decode_state_key(state_key),
         {:ok, scope_prefix} <- record_scope_prefix(record),
         {:ok, public_record} <- public_record(record),
         {:ok, id, version} <-
           validate_record_identity(public_record, state_key, scope_prefix, state_key_id),
         {:ok, encoded_metadata} <- encode_metadata(public_record) do
      if scope_bytes(scope_prefix) + byte_size(encoded_metadata) <= @maximum_metadata_bytes do
        {:ok, public_record, id, version, scope_prefix, encoded_metadata}
      else
        {:error, :metadata_too_large}
      end
    end
  end

  defp record_scope_prefix(record) when is_map(record) do
    case SystemMetadata.scope_prefix(Map.get(record, :system_metadata, %{})) do
      {:ok, scope_prefix} ->
        case validate_scope_prefix(scope_prefix) do
          :ok -> {:ok, scope_prefix}
          {:error, _reason} -> :error
        end

      {:error, :invalid_flow_system_metadata} ->
        :error
    end
  end

  defp validate_scope_prefix(nil), do: :ok

  defp validate_scope_prefix(scope_prefix)
       when is_binary(scope_prefix) and scope_prefix != "" and
              byte_size(scope_prefix) <= @maximum_scope_bytes,
       do: :ok

  defp validate_scope_prefix(_scope_prefix), do: {:error, :invalid_scope_prefix}

  defp decode_scope_prefix(<<>>), do: {:ok, nil}

  defp decode_scope_prefix(scope_prefix)
       when byte_size(scope_prefix) <= @maximum_scope_bytes do
    case validate_scope_prefix(scope_prefix) do
      :ok -> {:ok, :binary.copy(scope_prefix)}
      {:error, _reason} -> :error
    end
  end

  defp physical_partition_key(nil, nil), do: {:ok, nil}

  defp physical_partition_key(partition_key, scope_prefix) when is_binary(partition_key),
    do: StorageScope.physical_partition_key(partition_key, scope_prefix)

  defp physical_partition_key(_partition_key, _scope_prefix), do: :error

  defp scope_bytes(nil), do: 0
  defp scope_bytes(scope_prefix) when is_binary(scope_prefix), do: byte_size(scope_prefix)

  defp decode_state_key(state_key)
       when state_key != "" and byte_size(state_key) <= @maximum_state_key_bytes do
    case Keys.run_id_from_state_key(state_key) do
      {:ok, id} when is_binary(id) and id != "" and byte_size(id) <= @maximum_id_bytes ->
        {:ok, id}

      _invalid ->
        :error
    end
  end

  defp decode_state_key(_state_key), do: :error

  defp encode_metadata(record) do
    {identity_flags, builtin_record} = compact_builtin_record(record)
    attributes = Map.get(record, :attributes, %{})
    state_meta = Map.get(record, :state_meta, %{})

    with {:ok, encoded_builtin} <- encode_builtin_record(builtin_record),
         {:ok, encoded_internal} <- encode_internal_fields(record),
         {:ok, encoded_projection_config} <- encode_projection_config(record),
         {:ok, encoded_attributes} <- encode_attributes(attributes),
         {:ok, encoded_state_meta} <- encode_state_meta(state_meta) do
      encoded =
        <<identity_flags, byte_size(encoded_builtin)::unsigned-big-16,
          byte_size(encoded_internal)::unsigned-big-16,
          byte_size(encoded_projection_config)::unsigned-big-16,
          byte_size(encoded_attributes)::unsigned-big-16,
          byte_size(encoded_state_meta)::unsigned-big-16, encoded_builtin::binary,
          encoded_internal::binary, encoded_projection_config::binary, encoded_attributes::binary,
          encoded_state_meta::binary>>

      if byte_size(encoded) <= @maximum_metadata_bytes,
        do: {:ok, encoded},
        else: {:error, :metadata_too_large}
    end
  end

  defp decode_metadata(encoded, id, version, :full), do: decode_metadata(encoded, id, version)

  defp decode_metadata(
         <<identity_flags, builtin_bytes::unsigned-big-16, internal_bytes::unsigned-big-16,
           projection_config_bytes::unsigned-big-16, attribute_bytes::unsigned-big-16,
           state_meta_bytes::unsigned-big-16, payload::binary>>,
         id,
         version,
         %QueryRowDecodePlan{} = decode_plan
       ) do
    with true <- identity_flags in [0, @root_is_id_flag],
         true <- projection_config_bytes <= @maximum_projection_config_bytes,
         true <-
           byte_size(payload) ==
             builtin_bytes + internal_bytes + projection_config_bytes + attribute_bytes +
               state_meta_bytes,
         <<encoded_builtin::binary-size(^builtin_bytes),
           encoded_internal::binary-size(^internal_bytes),
           encoded_projection_config::binary-size(^projection_config_bytes),
           encoded_attributes::binary-size(^attribute_bytes),
           encoded_state_meta::binary-size(^state_meta_bytes)>> <- payload,
         {:ok, builtin_record} <- decode_builtin_record(encoded_builtin, id, version),
         {:ok, _internal_fields} <- decode_internal_fields(encoded_internal),
         {:ok, _projection_config} <- decode_projection_config(encoded_projection_config),
         {:ok, attribute_fields} <-
           decode_query_attributes(encoded_attributes, decode_plan.attributes),
         {:ok, state_meta_fields} <-
           decode_query_state_meta(encoded_state_meta, decode_plan.state_meta),
         record =
           builtin_record
           |> restore_builtin_record(id, identity_flags)
           |> Map.merge(attribute_fields)
           |> Map.merge(state_meta_fields) do
      {:ok, record}
    else
      _invalid -> :error
    end
  end

  defp decode_metadata(_encoded, _id, _version, %QueryRowDecodePlan{}), do: :error
  defp decode_metadata(_encoded, _id, _version, _decode_plan), do: :error

  defp decode_metadata(
         <<identity_flags, builtin_bytes::unsigned-big-16, internal_bytes::unsigned-big-16,
           projection_config_bytes::unsigned-big-16, attribute_bytes::unsigned-big-16,
           state_meta_bytes::unsigned-big-16, payload::binary>>,
         id,
         version
       ) do
    with true <- identity_flags in [0, @root_is_id_flag],
         true <- projection_config_bytes <= @maximum_projection_config_bytes,
         true <-
           byte_size(payload) ==
             builtin_bytes + internal_bytes + projection_config_bytes + attribute_bytes +
               state_meta_bytes,
         <<encoded_builtin::binary-size(^builtin_bytes),
           encoded_internal::binary-size(^internal_bytes),
           encoded_projection_config::binary-size(^projection_config_bytes),
           encoded_attributes::binary-size(^attribute_bytes),
           encoded_state_meta::binary-size(^state_meta_bytes)>> <- payload,
         {:ok, builtin_record} <- decode_builtin_record(encoded_builtin, id, version),
         {:ok, internal_fields} <- decode_internal_fields(encoded_internal),
         {:ok, projection_config} <- decode_projection_config(encoded_projection_config),
         {:ok, attributes} <- decode_attributes(encoded_attributes),
         {:ok, state_meta} <- decode_state_meta(encoded_state_meta),
         record =
           builtin_record
           |> restore_builtin_record(id, identity_flags)
           |> Map.merge(internal_fields)
           |> Map.merge(projection_config) do
      {:ok,
       record
       |> maybe_put_metadata(:attributes, attributes)
       |> maybe_put_metadata(:state_meta, state_meta)}
    else
      _invalid -> :error
    end
  end

  defp decode_metadata(_encoded, _id, _version), do: :error

  defp encode_builtin_record(%{id: id, version: version} = record) do
    with true <- CoveringCodec.valid_record?(record, id, version) do
      @builtin_encoding_fields
      |> Enum.with_index()
      |> Enum.reduce_while({0, []}, fn {field, index}, {bitmap, values} ->
        if Map.has_key?(record, field) do
          case QueryRowPrimitives.encode(Map.fetch!(record, field), @maximum_encoded_bytes, false) do
            {:ok, encoded, _semantic_bytes} ->
              {:cont, {bitmap ||| 1 <<< index, [encoded | values]}}

            :error ->
              {:halt, :error}
          end
        else
          {:cont, {bitmap, values}}
        end
      end)
      |> case do
        {bitmap, reversed} ->
          {:ok, IO.iodata_to_binary([<<bitmap::unsigned-big-16>>, Enum.reverse(reversed)])}

        :error ->
          :error
      end
    else
      _invalid -> :error
    end
  end

  defp encode_builtin_record(_record), do: :error

  defp decode_builtin_record(<<bitmap::unsigned-big-16, values::binary>>, id, version)
       when (bitmap &&& bnot(@valid_builtin_bitmap)) == 0 do
    @builtin_encoding_fields
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{id: id, version: version}, values}, fn {field, index},
                                                                        {:ok, record, rest} ->
      if (bitmap &&& 1 <<< index) == 0 do
        {:cont, {:ok, record, rest}}
      else
        case QueryRowPrimitives.decode(rest, @maximum_encoded_bytes, false) do
          {:ok, value, tail, _semantic_bytes} ->
            {:cont, {:ok, Map.put(record, field, value), tail}}

          :error ->
            {:halt, :error}
        end
      end
    end)
    |> case do
      {:ok, record, <<>>} ->
        if CoveringCodec.valid_record?(record, id, version), do: {:ok, record}, else: :error

      _invalid ->
        :error
    end
  end

  defp decode_builtin_record(_encoded, _id, _version), do: :error

  defp encode_internal_fields(record) do
    @internal_storage_fields
    |> Enum.with_index()
    |> Enum.reduce_while({0, []}, fn {field, index}, {flags, values} ->
      case Map.get(record, field) do
        nil ->
          {:cont, {flags, values}}

        value ->
          case encode_internal_field(field, value) do
            {:ok, encoded} ->
              {:cont, {flags ||| 1 <<< index, [encoded | values]}}

            :error ->
              {:halt, :error}
          end
      end
    end)
    |> case do
      {flags, values} -> {:ok, IO.iodata_to_binary([<<flags>>, Enum.reverse(values)])}
      :error -> :error
    end
  end

  defp decode_internal_fields(<<flags, values::binary>>)
       when (flags &&& bnot(@valid_internal_flags)) == 0 do
    @internal_storage_fields
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}, values}, fn {field, index}, {:ok, acc, rest} ->
      if (flags &&& 1 <<< index) == 0 do
        {:cont, {:ok, acc, rest}}
      else
        case decode_internal_field(field, rest) do
          {:ok, value, tail} ->
            {:cont, {:ok, Map.put(acc, field, value), tail}}

          :error ->
            {:halt, :error}
        end
      end
    end)
    |> case do
      {:ok, fields, <<>>} -> {:ok, fields}
      _invalid -> :error
    end
  end

  defp decode_internal_fields(_encoded), do: :error

  defp encode_internal_field(:lease_owner, value) when is_binary(value) do
    case QueryRowPrimitives.encode(value, @maximum_encoded_bytes, false) do
      {:ok, encoded, _semantic_bytes} -> {:ok, encoded}
      :error -> :error
    end
  end

  defp encode_internal_field(field, value)
       when field in [:state_enter_seq, :history_max_events, :history_hot_max_events] and
              is_integer(value) and value >= 0 and value <= @max_u64,
       do: QueryRowPrimitives.encode_u64(value)

  defp encode_internal_field(_field, _value), do: :error

  defp decode_internal_field(:lease_owner, encoded) do
    case QueryRowPrimitives.decode(encoded, @maximum_encoded_bytes, false) do
      {:ok, value, rest, _semantic_bytes} when is_binary(value) -> {:ok, value, rest}
      _invalid -> :error
    end
  end

  defp decode_internal_field(field, encoded)
       when field in [:state_enter_seq, :history_max_events, :history_hot_max_events],
       do: QueryRowPrimitives.decode_u64(encoded)

  defp compact_builtin_record(record) do
    builtin_record = Map.take(record, @builtin_storage_fields)

    if Map.get(builtin_record, :root_flow_id) == Map.get(builtin_record, :id) do
      {@root_is_id_flag, Map.delete(builtin_record, :root_flow_id)}
    else
      {0, builtin_record}
    end
  end

  defp restore_builtin_record(record, id, @root_is_id_flag),
    do: Map.put(record, :root_flow_id, id)

  defp restore_builtin_record(record, _id, 0), do: record

  defp encode_projection_config(record) do
    with {:ok, indexed_attributes} <-
           Attributes.normalize_indexed_names(Map.get(record, :indexed_attributes, [])),
         {:ok, indexed_state_meta} <-
           StateMeta.normalize_indexed_key(Map.get(record, :indexed_state_meta)),
         indexed_attributes = Enum.sort(indexed_attributes),
         encoded_attributes =
           Enum.map(indexed_attributes, fn name ->
             <<byte_size(name)::unsigned-big-8, name::binary>>
           end),
         indexed_state_meta = indexed_state_meta || "",
         encoded =
           IO.iodata_to_binary([
             <<length(indexed_attributes)::unsigned-big-8>>,
             encoded_attributes,
             <<byte_size(indexed_state_meta)::unsigned-big-8, indexed_state_meta::binary>>
           ]),
         true <- byte_size(encoded) <= @maximum_projection_config_bytes do
      {:ok, encoded}
    else
      _invalid -> :error
    end
  end

  defp decode_projection_config(encoded)
       when is_binary(encoded) and byte_size(encoded) <= @maximum_projection_config_bytes do
    with <<attribute_count::unsigned-big-8, rest::binary>> <- encoded,
         {:ok, indexed_attributes, rest} <-
           decode_indexed_attribute_names(attribute_count, rest, nil, []),
         <<state_meta_bytes::unsigned-big-8, indexed_state_meta::binary>> <- rest,
         true <- byte_size(indexed_state_meta) == state_meta_bytes,
         {:ok, normalized_state_meta} <-
           StateMeta.normalize_indexed_key(indexed_state_meta),
         true <- normalized_state_meta == empty_to_nil(indexed_state_meta) do
      {:ok,
       %{}
       |> maybe_put_projection_config(:indexed_attributes, indexed_attributes)
       |> maybe_put_projection_config(:indexed_state_meta, normalized_state_meta)}
    else
      _invalid -> :error
    end
  end

  defp decode_projection_config(_encoded), do: :error

  defp decode_indexed_attribute_names(0, rest, _previous, names) do
    names = Enum.reverse(names)

    case Attributes.normalize_indexed_names(names) do
      {:ok, ^names} -> {:ok, names, rest}
      _invalid -> :error
    end
  end

  defp decode_indexed_attribute_names(count, encoded, previous, names) when count > 0 do
    with <<name_bytes::unsigned-big-8, rest::binary>> <- encoded,
         true <- name_bytes > 0 and name_bytes <= @maximum_dynamic_name_bytes,
         <<name::binary-size(^name_bytes), rest::binary>> <- rest,
         true <- is_nil(previous) or name > previous,
         {:ok, ^name} <- Attributes.normalize_name(name) do
      decode_indexed_attribute_names(count - 1, rest, name, [:binary.copy(name) | names])
    else
      _invalid -> :error
    end
  end

  defp encode_attributes(attributes)
       when is_map(attributes) and map_size(attributes) <= @maximum_attributes do
    with true <- Attributes.valid_normalized?(attributes),
         {:ok, semantic_bytes, entries} <-
           encode_dynamic_entries(attributes, :attribute, true, []),
         true <- semantic_bytes <= @maximum_attribute_bytes,
         encoded = IO.iodata_to_binary([<<map_size(attributes)::unsigned-big-8>>, entries]),
         true <- byte_size(encoded) <= @maximum_attribute_section_bytes do
      {:ok, encoded}
    else
      _invalid -> :error
    end
  end

  defp encode_attributes(_attributes), do: :error

  defp decode_attributes(<<count::unsigned-big-8, entries::binary>>)
       when count <= @maximum_attributes do
    with {:ok, attributes, <<>>, semantic_bytes} <-
           decode_dynamic_entries(count, entries, :attribute, true, nil, %{}, 0),
         true <- semantic_bytes <= @maximum_attribute_bytes,
         true <- Attributes.valid_normalized?(attributes) do
      {:ok, attributes}
    else
      _invalid -> :error
    end
  end

  defp decode_attributes(_encoded), do: :error

  defp decode_query_attributes(encoded, :all) do
    case decode_attributes(encoded) do
      {:ok, attributes} -> {:ok, metadata_section(:attributes, attributes)}
      :error -> :error
    end
  end

  defp decode_query_attributes(encoded, :none) do
    case validate_attributes(encoded) do
      :ok -> {:ok, %{}}
      :error -> :error
    end
  end

  defp decode_query_attributes(encoded, %MapSet{} = selected) do
    decode_selected_attributes(encoded, selected)
  end

  defp decode_query_attributes(_encoded, _selection), do: :error

  defp decode_selected_attributes(<<count::unsigned-big-8, entries::binary>>, selected)
       when count <= @maximum_attributes do
    with {:ok, fields, <<>>, semantic_bytes} <-
           decode_selected_dynamic_entries(
             count,
             entries,
             :attribute,
             true,
             nil,
             selected,
             %{},
             0
           ),
         true <- semantic_bytes <= @maximum_attribute_bytes do
      {:ok, fields}
    else
      _invalid -> :error
    end
  end

  defp decode_selected_attributes(_encoded, _selected), do: :error

  defp validate_attributes(<<count::unsigned-big-8, entries::binary>>)
       when count <= @maximum_attributes do
    with {:ok, <<>>, semantic_bytes} <-
           validate_dynamic_entries(count, entries, :attribute, true, nil, 0),
         true <- semantic_bytes <= @maximum_attribute_bytes do
      :ok
    else
      _invalid -> :error
    end
  end

  defp validate_attributes(_encoded), do: :error

  defp encode_state_meta(state_meta)
       when is_map(state_meta) and map_size(state_meta) <= @maximum_state_meta_states do
    with true <- StateMeta.valid_normalized?(state_meta) do
      state_meta
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.reduce_while({:ok, 0, []}, fn {state, values}, {:ok, bytes, acc} ->
        with true <- valid_dynamic_name?(state, true),
             true <- is_map(values) and map_size(values) <= @maximum_state_meta_entries,
             {:ok, entry_bytes, entries} <-
               encode_dynamic_entries(values, {:state_meta, state}, false, []),
             state_bytes = byte_size(state) + entry_bytes,
             true <- bytes + state_bytes <= @maximum_state_meta_bytes do
          encoded =
            [
              <<byte_size(state)::unsigned-big-8, state::binary,
                map_size(values)::unsigned-big-8>>,
              entries
            ]

          {:cont, {:ok, bytes + state_bytes, [encoded | acc]}}
        else
          _invalid -> {:halt, :error}
        end
      end)
      |> case do
        {:ok, bytes, reversed} when bytes <= @maximum_state_meta_bytes ->
          encoded =
            IO.iodata_to_binary([
              <<map_size(state_meta)::unsigned-big-8>>,
              Enum.reverse(reversed)
            ])

          if byte_size(encoded) <= @maximum_state_meta_bytes + 4_096,
            do: {:ok, encoded},
            else: :error

        _invalid ->
          :error
      end
    else
      _invalid -> :error
    end
  end

  defp encode_state_meta(_state_meta), do: :error

  defp decode_state_meta(<<count::unsigned-big-8, states::binary>>)
       when count <= @maximum_state_meta_states do
    with {:ok, state_meta, <<>>, semantic_bytes} <-
           decode_state_meta_states(count, states, nil, %{}, 0),
         true <- semantic_bytes <= @maximum_state_meta_bytes,
         true <- StateMeta.valid_normalized?(state_meta) do
      {:ok, state_meta}
    else
      _invalid -> :error
    end
  end

  defp decode_state_meta(_encoded), do: :error

  defp decode_query_state_meta(encoded, :all) do
    case decode_state_meta(encoded) do
      {:ok, state_meta} -> {:ok, metadata_section(:state_meta, state_meta)}
      :error -> :error
    end
  end

  defp decode_query_state_meta(encoded, :none) do
    case validate_state_meta(encoded) do
      :ok -> {:ok, %{}}
      :error -> :error
    end
  end

  defp decode_query_state_meta(encoded, selected) when is_map(selected) do
    decode_selected_state_meta(encoded, selected)
  end

  defp decode_query_state_meta(_encoded, _selection), do: :error

  defp decode_selected_state_meta(<<count::unsigned-big-8, states::binary>>, selected)
       when count <= @maximum_state_meta_states do
    with {:ok, fields, <<>>, semantic_bytes} <-
           decode_selected_state_meta_states(count, states, nil, selected, %{}, 0),
         true <- semantic_bytes <= @maximum_state_meta_bytes do
      {:ok, fields}
    else
      _invalid -> :error
    end
  end

  defp decode_selected_state_meta(_encoded, _selected), do: :error

  defp decode_selected_state_meta_states(
         0,
         rest,
         _previous_state,
         _selected,
         fields,
         bytes
       ),
       do: {:ok, fields, rest, bytes}

  defp decode_selected_state_meta_states(
         count,
         encoded,
         previous_state,
         selected,
         fields,
         bytes
       )
       when count > 0 do
    with <<state_bytes::unsigned-big-8, rest::binary>> <- encoded,
         true <- state_bytes > 0 and state_bytes <= @maximum_dynamic_name_bytes,
         <<state::binary-size(^state_bytes), entry_count::unsigned-big-8, rest::binary>> <- rest,
         true <- entry_count <= @maximum_state_meta_entries,
         true <- is_nil(previous_state) or state > previous_state,
         true <- valid_dynamic_name?(state, true),
         selected_names = Map.get(selected, state),
         {:ok, fields, rest, entry_bytes} <-
           decode_selected_dynamic_entries(
             entry_count,
             rest,
             {:state_meta, state},
             false,
             nil,
             selected_names,
             fields,
             0
           ),
         next_bytes = bytes + byte_size(state) + entry_bytes,
         true <- next_bytes <= @maximum_state_meta_bytes do
      decode_selected_state_meta_states(
        count - 1,
        rest,
        state,
        selected,
        fields,
        next_bytes
      )
    else
      _invalid -> :error
    end
  end

  defp validate_state_meta(<<count::unsigned-big-8, states::binary>>)
       when count <= @maximum_state_meta_states do
    with {:ok, <<>>, semantic_bytes} <-
           validate_state_meta_states(count, states, nil, 0),
         true <- semantic_bytes <= @maximum_state_meta_bytes do
      :ok
    else
      _invalid -> :error
    end
  end

  defp validate_state_meta(_encoded), do: :error

  defp validate_state_meta_states(0, rest, _previous_state, bytes),
    do: {:ok, rest, bytes}

  defp validate_state_meta_states(count, encoded, previous_state, bytes) when count > 0 do
    with <<state_bytes::unsigned-big-8, rest::binary>> <- encoded,
         true <- state_bytes > 0 and state_bytes <= @maximum_dynamic_name_bytes,
         <<state::binary-size(^state_bytes), entry_count::unsigned-big-8, rest::binary>> <- rest,
         true <- entry_count <= @maximum_state_meta_entries,
         true <- is_nil(previous_state) or state > previous_state,
         true <- valid_dynamic_name?(state, true),
         {:ok, rest, entry_bytes} <-
           validate_dynamic_entries(
             entry_count,
             rest,
             {:state_meta, state},
             false,
             nil,
             0
           ),
         next_bytes = bytes + byte_size(state) + entry_bytes,
         true <- next_bytes <= @maximum_state_meta_bytes do
      validate_state_meta_states(count - 1, rest, state, next_bytes)
    else
      _invalid -> :error
    end
  end

  defp decode_state_meta_states(0, rest, _previous_state, state_meta, bytes),
    do: {:ok, state_meta, rest, bytes}

  defp decode_state_meta_states(count, encoded, previous_state, state_meta, bytes)
       when count > 0 do
    with <<state_bytes::unsigned-big-8, rest::binary>> <- encoded,
         true <- state_bytes > 0 and state_bytes <= @maximum_dynamic_name_bytes,
         <<state::binary-size(^state_bytes), entry_count::unsigned-big-8, rest::binary>> <- rest,
         true <- entry_count <= @maximum_state_meta_entries,
         true <- is_nil(previous_state) or state > previous_state,
         true <- valid_dynamic_name?(state, true),
         {:ok, values, rest, entry_bytes} <-
           decode_dynamic_entries(
             entry_count,
             rest,
             {:state_meta, state},
             false,
             nil,
             %{},
             0
           ),
         next_bytes = bytes + byte_size(state) + entry_bytes,
         true <- next_bytes <= @maximum_state_meta_bytes do
      state = :binary.copy(state)

      decode_state_meta_states(
        count - 1,
        rest,
        state,
        Map.put(state_meta, state, values),
        next_bytes
      )
    else
      _invalid -> :error
    end
  end

  defp encode_dynamic_entries(values, namespace, allow_list?, acc) do
    values
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({:ok, 0, acc}, fn {name, value}, {:ok, bytes, acc} ->
      with true <- valid_field?(namespace, name),
           {:ok, encoded_value, value_bytes} <-
             QueryRowPrimitives.encode(value, @maximum_dynamic_value_bytes, allow_list?),
           next_bytes = bytes + byte_size(name) + value_bytes,
           true <- next_bytes <= @maximum_state_meta_bytes do
        encoded = <<byte_size(name)::unsigned-big-8, name::binary, encoded_value::binary>>

        {:cont, {:ok, next_bytes, [encoded | acc]}}
      else
        _invalid -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, bytes, reversed} -> {:ok, bytes, Enum.reverse(reversed)}
      :error -> :error
    end
  end

  defp decode_dynamic_entries(
         0,
         rest,
         _namespace,
         _allow_list?,
         _previous_name,
         values,
         bytes
       ),
       do: {:ok, values, rest, bytes}

  defp decode_dynamic_entries(
         count,
         encoded,
         namespace,
         allow_list?,
         previous_name,
         values,
         bytes
       )
       when count > 0 do
    with <<name_bytes::unsigned-big-8, rest::binary>> <- encoded,
         true <- name_bytes > 0 and name_bytes <= @maximum_dynamic_name_bytes,
         <<name::binary-size(^name_bytes), rest::binary>> <- rest,
         true <- is_nil(previous_name) or name > previous_name,
         true <- valid_field?(namespace, name),
         {:ok, value, rest, semantic_value_bytes} <-
           QueryRowPrimitives.decode(rest, @maximum_dynamic_value_bytes, allow_list?),
         next_bytes = bytes + byte_size(name) + semantic_value_bytes,
         true <- next_bytes <= @maximum_state_meta_bytes do
      name = :binary.copy(name)

      decode_dynamic_entries(
        count - 1,
        rest,
        namespace,
        allow_list?,
        name,
        Map.put(values, name, value),
        next_bytes
      )
    else
      _invalid -> :error
    end
  end

  defp decode_selected_dynamic_entries(
         0,
         rest,
         _namespace,
         _allow_list?,
         _previous_name,
         _selected,
         fields,
         bytes
       ),
       do: {:ok, fields, rest, bytes}

  defp decode_selected_dynamic_entries(
         count,
         encoded,
         namespace,
         allow_list?,
         previous_name,
         selected,
         fields,
         bytes
       )
       when count > 0 do
    with <<name_bytes::unsigned-big-8, rest::binary>> <- encoded,
         true <- name_bytes > 0 and name_bytes <= @maximum_dynamic_name_bytes,
         <<name::binary-size(^name_bytes), rest::binary>> <- rest,
         true <- is_nil(previous_name) or name > previous_name,
         true <- valid_field?(namespace, name),
         {:ok, fields, rest, semantic_value_bytes} <-
           decode_selected_dynamic_value(rest, namespace, name, allow_list?, selected, fields),
         next_bytes = bytes + byte_size(name) + semantic_value_bytes,
         true <- next_bytes <= @maximum_state_meta_bytes do
      decode_selected_dynamic_entries(
        count - 1,
        rest,
        namespace,
        allow_list?,
        name,
        selected,
        fields,
        next_bytes
      )
    else
      _invalid -> :error
    end
  end

  defp decode_selected_dynamic_value(
         encoded,
         namespace,
         name,
         allow_list?,
         selected,
         fields
       ) do
    if selected_dynamic_field?(selected, name) do
      with {:ok, value, rest, semantic_bytes} <-
             QueryRowPrimitives.decode(encoded, @maximum_dynamic_value_bytes, allow_list?),
           true <- valid_selected_dynamic_value?(namespace, value) do
        {:ok, Map.put(fields, selected_field(namespace, name), value), rest, semantic_bytes}
      else
        _invalid -> :error
      end
    else
      case QueryRowPrimitives.skip(encoded, @maximum_dynamic_value_bytes, allow_list?) do
        {:ok, rest, semantic_bytes} -> {:ok, fields, rest, semantic_bytes}
        :error -> :error
      end
    end
  end

  defp valid_selected_dynamic_value?(:attribute, value),
    do: Attributes.valid_scalar?(value) or Attributes.valid_list?(value)

  defp valid_selected_dynamic_value?({:state_meta, _state}, _value), do: true

  defp selected_dynamic_field?(%MapSet{} = selected, name), do: MapSet.member?(selected, name)
  defp selected_dynamic_field?(nil, _name), do: false

  defp selected_field(:attribute, name), do: {:attribute, :binary.copy(name)}

  defp selected_field({:state_meta, state}, name),
    do: {:state_meta, :binary.copy(state), :binary.copy(name)}

  defp validate_dynamic_entries(
         0,
         rest,
         _namespace,
         _allow_list?,
         _previous_name,
         bytes
       ),
       do: {:ok, rest, bytes}

  defp validate_dynamic_entries(
         count,
         encoded,
         namespace,
         allow_list?,
         previous_name,
         bytes
       )
       when count > 0 do
    with <<name_bytes::unsigned-big-8, rest::binary>> <- encoded,
         true <- name_bytes > 0 and name_bytes <= @maximum_dynamic_name_bytes,
         <<name::binary-size(^name_bytes), rest::binary>> <- rest,
         true <- is_nil(previous_name) or name > previous_name,
         true <- valid_field?(namespace, name),
         {:ok, rest, semantic_value_bytes} <-
           QueryRowPrimitives.skip(rest, @maximum_dynamic_value_bytes, allow_list?),
         next_bytes = bytes + byte_size(name) + semantic_value_bytes,
         true <- next_bytes <= @maximum_state_meta_bytes do
      validate_dynamic_entries(
        count - 1,
        rest,
        namespace,
        allow_list?,
        name,
        next_bytes
      )
    else
      _invalid -> :error
    end
  end

  defp valid_field?(:attribute, name), do: Field.valid?({:attribute, name})

  defp valid_field?({:state_meta, state}, name),
    do: Field.valid?({:state_meta, state, name})

  defp valid_dynamic_name?(name, allow_reserved?)
       when is_binary(name) and name != "" and byte_size(name) <= @maximum_dynamic_name_bytes do
    String.valid?(name) and (allow_reserved? or not String.starts_with?(name, "__"))
  end

  defp valid_dynamic_name?(_name, _allow_reserved?), do: false

  defp encode_locator(%Locator{} = locator, expire_at_ms) do
    with true <- Locator.hydration_ready?(locator),
         {:ok, source_tag, source_id} <- Locator.storage_source(locator.file_id),
         :ok <- validate_locator_expiry(locator, expire_at_ms),
         {:ok, encoded_source_id} <- QueryRowPrimitives.encode_u64(source_id),
         {:ok, encoded_raft_index} <- QueryRowPrimitives.encode_u64(locator.raft_index),
         {:ok, encoded_offset} <- QueryRowPrimitives.encode_u64(locator.offset),
         {:ok, encoded_value_size} <- QueryRowPrimitives.encode_u64(locator.value_size),
         {:ok, encoded_frame_size} <- encode_locator_frame_size(locator.frame_size),
         {:ok, expiry_flags, encoded_locator_expiry} <-
           encode_locator_expiry(locator.expire_at_ms, expire_at_ms),
         {:ok, encoded_generation} <- encode_locator_generation(locator.segment_generation) do
      raft_index? = locator.raft_index != source_id
      segment_generation? = not is_nil(locator.segment_generation)
      frame_size? = not is_nil(locator.frame_size)

      flags =
        expiry_flags
        |> maybe_set_flag(@locator_raft_index_flag, raft_index?)
        |> maybe_set_flag(@locator_segment_generation_flag, segment_generation?)
        |> maybe_set_flag(@locator_frame_size_flag, frame_size?)

      encoded =
        IO.iodata_to_binary([
          <<source_tag, flags>>,
          encoded_source_id,
          if(raft_index?, do: encoded_raft_index, else: []),
          encoded_offset,
          encoded_value_size,
          encoded_frame_size,
          encoded_locator_expiry,
          encoded_generation,
          locator.checksum
        ])

      if byte_size(encoded) <= @maximum_locator_bytes, do: {:ok, encoded}, else: :error
    else
      _invalid -> :error
    end
  end

  defp decode_locator(<<source_tag, flags, encoded::binary>>, id, version, row_expire_at_ms)
       when (flags &&& bnot(@valid_locator_flags)) == 0 do
    with true <-
           (flags &&& @locator_nil_expiry_flag) == 0 or
             (flags &&& @locator_expiry_override_flag) == 0,
         {:ok, source_id, rest} <- QueryRowPrimitives.decode_u64(encoded),
         {:ok, raft_index, rest} <- decode_locator_raft_index(rest, flags, source_id),
         {:ok, offset, rest} <- QueryRowPrimitives.decode_u64(rest),
         {:ok, value_size, rest} <- QueryRowPrimitives.decode_u64(rest),
         {:ok, frame_size, rest} <- decode_locator_frame_size(rest, flags),
         {:ok, expire_at_ms, rest} <-
           decode_locator_expiry(rest, flags, row_expire_at_ms),
         {:ok, segment_generation, rest} <- decode_locator_generation(rest, flags),
         <<checksum::binary-size(@locator_checksum_bytes)>> <- rest,
         {:ok, file_id} <- Locator.storage_file_id(source_tag, source_id),
         :ok <- validate_source_lifetime(expire_at_ms, row_expire_at_ms),
         {:ok, locator} <-
           Locator.new(
             flow_id: id,
             kind: :state,
             version: version,
             raft_index: raft_index,
             file_id: file_id,
             offset: offset,
             value_size: value_size,
             frame_size: frame_size,
             checksum: checksum,
             expire_at_ms: expire_at_ms,
             segment_generation: segment_generation
           ),
         true <- Locator.hydration_ready?(locator) do
      {:ok, locator}
    else
      _invalid -> :error
    end
  end

  defp decode_locator(_encoded, _id, _version, _row_expire_at_ms), do: :error

  defp validate_locator_expiry(%Locator{expire_at_ms: source_expiry}, row_expiry),
    do: validate_source_lifetime(source_expiry, row_expiry)

  defp validate_source_lifetime(source_expiry, row_expiry)
       when source_expiry in [nil, 0] and is_integer(row_expiry) and row_expiry >= 0,
       do: :ok

  defp validate_source_lifetime(source_expiry, row_expiry)
       when is_integer(source_expiry) and source_expiry > 0 and is_integer(row_expiry) and
              row_expiry > 0 and source_expiry >= row_expiry,
       do: :ok

  defp validate_source_lifetime(_source_expiry, _row_expiry), do: :error

  defp encode_locator_expiry(nil, _row_expiry), do: {:ok, @locator_nil_expiry_flag, []}
  defp encode_locator_expiry(expiry, expiry), do: {:ok, 0, []}

  defp encode_locator_expiry(expiry, _row_expiry) when is_integer(expiry) and expiry >= 0 do
    case QueryRowPrimitives.encode_u64(expiry) do
      {:ok, encoded} -> {:ok, @locator_expiry_override_flag, encoded}
      :error -> :error
    end
  end

  defp encode_locator_expiry(_expiry, _row_expiry), do: :error

  defp decode_locator_expiry(encoded, flags, row_expiry) do
    cond do
      (flags &&& @locator_nil_expiry_flag) != 0 ->
        {:ok, nil, encoded}

      (flags &&& @locator_expiry_override_flag) != 0 ->
        QueryRowPrimitives.decode_u64(encoded)

      true ->
        {:ok, row_expiry, encoded}
    end
  end

  defp encode_locator_generation(nil), do: {:ok, []}

  defp encode_locator_generation(value) when is_integer(value) and value >= 0,
    do: QueryRowPrimitives.encode_u64(value)

  defp encode_locator_generation(_value), do: :error

  defp decode_locator_raft_index(encoded, flags, source_id) do
    if (flags &&& @locator_raft_index_flag) == 0 do
      {:ok, source_id, encoded}
    else
      case QueryRowPrimitives.decode_u64(encoded) do
        {:ok, ^source_id, _rest} -> :error
        {:ok, raft_index, rest} -> {:ok, raft_index, rest}
        :error -> :error
      end
    end
  end

  defp decode_locator_generation(encoded, flags) do
    if (flags &&& @locator_segment_generation_flag) == 0 do
      {:ok, nil, encoded}
    else
      QueryRowPrimitives.decode_u64(encoded)
    end
  end

  defp encode_locator_frame_size(nil), do: {:ok, []}

  defp encode_locator_frame_size(value) when is_integer(value) and value >= 0,
    do: QueryRowPrimitives.encode_u64(value)

  defp encode_locator_frame_size(_value), do: :error

  defp decode_locator_frame_size(encoded, flags) do
    if (flags &&& @locator_frame_size_flag) == 0 do
      {:ok, nil, encoded}
    else
      QueryRowPrimitives.decode_u64(encoded)
    end
  end

  defp maybe_set_flag(flags, flag, true), do: flags ||| flag
  defp maybe_set_flag(flags, _flag, false), do: flags

  defp maybe_put_metadata(record, _field, metadata) when map_size(metadata) == 0, do: record
  defp maybe_put_metadata(record, field, metadata), do: Map.put(record, field, metadata)

  defp metadata_section(_field, metadata) when map_size(metadata) == 0, do: %{}
  defp metadata_section(field, metadata), do: %{field => metadata}

  defp maybe_put_projection_config(record, _field, value) when value in [nil, []], do: record
  defp maybe_put_projection_config(record, field, value), do: Map.put(record, field, value)

  defp empty_to_nil(""), do: nil
  defp empty_to_nil(value), do: value
end
