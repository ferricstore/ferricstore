defmodule Ferricstore.Flow.LMDB.Cold do
  @moduledoc false

  alias Ferricstore.Flow.Locator
  alias Ferricstore.Flow.Query.Limits
  alias Ferricstore.TermCodec

  @u64_decimal_zero_pad "00000000000000000000"
  @max_u64 18_446_744_073_709_551_615
  @min_i64 -9_223_372_036_854_775_808
  @max_i64 9_223_372_036_854_775_807
  @max_encoded_bytes 384 * 1_024
  @max_key_bytes 65_535
  @max_run_id_bytes Limits.max_run_id_bytes()
  @max_checksum_bytes 64
  @value_refs_digest_bytes 32
  @value_ref_kinds [:payload, :result, :error, :shared]
  @park_version 2

  def park_key(flow_id) when is_binary(flow_id), do: "flow:park:v1:" <> flow_id

  def park_key_for_state_key(state_key) when is_binary(state_key),
    do: "flow:park:v1:key:" <> escape_key_part(state_key)

  def due_bucket_ms(due_at_ms, bucket_ms \\ 60_000)

  def due_bucket_ms(due_at_ms, bucket_ms)
      when is_integer(due_at_ms) and due_at_ms >= 0 and due_at_ms <= @max_u64 and
             is_integer(bucket_ms) and bucket_ms > 0 and bucket_ms <= @max_u64 do
    div(due_at_ms, bucket_ms) * bucket_ms
  end

  def due_bucket_ms(_due_at_ms, _bucket_ms),
    do: raise(ArgumentError, "cold due bucket inputs must be unsigned 64-bit integers")

  def due_key(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)
    due_at_ms = Map.fetch!(attrs, :due_at_ms)
    bucket_ms = Map.get(attrs, :bucket_ms, due_bucket_ms(due_at_ms))

    [
      "flow:due:v1",
      encode_u64(bucket_ms),
      escape_key_part(Map.fetch!(attrs, :type)),
      escape_key_part(Map.fetch!(attrs, :state)),
      escape_key_part(Map.get(attrs, :partition_key, "")),
      encode_i64(Map.get(attrs, :priority, 0)),
      encode_u64(due_at_ms),
      escape_key_part(Map.fetch!(attrs, :flow_id)),
      encode_u64(Map.fetch!(attrs, :version))
    ]
    |> Enum.join(":")
  end

  def due_bucket_prefix(bucket_ms) when is_integer(bucket_ms) and bucket_ms >= 0 do
    "flow:due:v1:" <> encode_u64(bucket_ms)
  end

  def due_type_bucket_prefix(bucket_ms, type)
      when is_integer(bucket_ms) and bucket_ms >= 0 and is_binary(type) do
    ["flow:due:v1", encode_u64(bucket_ms), escape_key_part(type)]
    |> Enum.join(":")
    |> Kernel.<>(":")
  end

  def due_state_bucket_prefix(bucket_ms, type, state)
      when is_integer(bucket_ms) and bucket_ms >= 0 and is_binary(type) and is_binary(state) do
    ["flow:due:v1", encode_u64(bucket_ms), escape_key_part(type), escape_key_part(state)]
    |> Enum.join(":")
    |> Kernel.<>(":")
  end

  def due_claim_prefix(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    [
      "flow:due:v1",
      encode_u64(Map.fetch!(attrs, :bucket_ms)),
      escape_key_part(Map.fetch!(attrs, :type)),
      escape_key_part(Map.fetch!(attrs, :state)),
      escape_key_part(Map.get(attrs, :partition_key, "")),
      encode_i64(Map.get(attrs, :priority, 0))
    ]
    |> Enum.join(":")
    |> Kernel.<>(":")
  end

  def by_segment_key(%Locator{} = locator) do
    by_segment_key(locator.file_id, locator.offset)
  end

  def by_segment_key(file_id, offset)
      when is_integer(offset) and offset >= 0 and offset <= @max_u64,
      do: by_segment_prefix(file_id) <> ":" <> encode_u64(offset)

  def by_segment_prefix(file_id) do
    case Locator.storage_source(file_id) do
      {:ok, source_tag, source_id} ->
        ["flow:cold:by-segment:v1", escape_key_part(<<source_tag, source_id::unsigned-big-64>>)]
        |> Enum.join(":")

      :error ->
        raise ArgumentError, "invalid Flow cold storage source"
    end
  end

  def encode_park(%Locator{kind: :state} = locator, attrs)
      when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    if Map.has_key?(attrs, :state_value) do
      raise ArgumentError, "cold park cannot embed a Flow state record"
    end

    fields = %{
      locator: locator,
      due_at_ms: Map.get(attrs, :due_at_ms),
      type: Map.get(attrs, :type),
      state: Map.get(attrs, :state),
      partition_key: Map.get(attrs, :partition_key),
      state_key: Map.get(attrs, :state_key),
      priority: Map.get(attrs, :priority, 0),
      lease_until_ms: Map.get(attrs, :lease_until_ms),
      fencing_token: Map.get(attrs, :fencing_token),
      retention_at_ms: Map.get(attrs, :retention_at_ms),
      value_refs_digest: Map.get(attrs, :value_refs_digest),
      checksum: Map.get(attrs, :checksum)
    }

    if valid_park_fields?(fields) do
      encode_bounded!({:flow_cold_park, @park_version, fields}, "invalid Flow cold park fields")
    else
      raise ArgumentError, "invalid Flow cold park fields"
    end
  end

  def decode_park(blob) when is_binary(blob) and byte_size(blob) <= @max_encoded_bytes do
    case TermCodec.decode(blob) do
      {:ok, {:flow_cold_park, @park_version, %{locator: %Locator{} = locator} = fields}} ->
        if Locator.valid?(locator) and valid_park_fields?(fields), do: {:ok, fields}, else: :error

      {:ok, {:flow_cold_park, @park_version, _invalid_fields}} ->
        :error

      {:ok, {:flow_cold_park, _unsupported_version, _fields}} ->
        :error

      {:ok, _other} ->
        :not_cold_park

      {:error, :invalid_external_term} ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_park(blob) when is_binary(blob), do: :error

  def encode_value_locator(
        value_ref,
        owner_flow_id,
        owner_version,
        locator,
        attrs \\ []
      )

  def encode_value_locator(
        value_ref,
        owner_flow_id,
        owner_version,
        %Locator{kind: :value} = locator,
        attrs
      )
      when is_binary(value_ref) and is_binary(owner_flow_id) and is_integer(owner_version) and
             owner_version >= 0 and (is_map(attrs) or is_list(attrs)) do
    attrs = Map.new(attrs)

    fields = %{
      value_ref: value_ref,
      owner_flow_id: owner_flow_id,
      owner_version: owner_version,
      locator: locator,
      ref_kind: Map.get(attrs, :ref_kind),
      expire_at_ms: Map.get(attrs, :expire_at_ms),
      checksum: Map.get(attrs, :checksum)
    }

    if valid_value_locator_fields?(fields) do
      encode_bounded!(
        {:flow_cold_value_locator, 1, fields},
        "invalid Flow cold value locator fields"
      )
    else
      raise ArgumentError, "invalid Flow cold value locator fields"
    end
  end

  def encode_value_locator(_value_ref, _owner_flow_id, _owner_version, _locator, _attrs),
    do: raise(ArgumentError, "invalid Flow cold value locator fields")

  def decode_value_locator(blob)
      when is_binary(blob) and byte_size(blob) <= @max_encoded_bytes do
    case TermCodec.decode(blob) do
      {:ok, {:flow_cold_value_locator, 1, %{locator: %Locator{} = locator} = fields}} ->
        if Locator.valid?(locator) and valid_value_locator_fields?(fields),
          do: {:ok, fields},
          else: :error

      {:ok, {:flow_cold_value_locator, 1, _invalid_fields}} ->
        :error

      {:ok, _other} ->
        :not_cold_value_locator

      {:error, :invalid_external_term} ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode_value_locator(blob) when is_binary(blob), do: :error

  defp pad_u64(value) do
    encoded = Integer.to_string(value)

    case byte_size(encoded) do
      size when size < 20 -> binary_part(@u64_decimal_zero_pad, 0, 20 - size) <> encoded
      _size -> encoded
    end
  end

  defp encode_u64(value) when is_integer(value) and value >= 0 and value <= @max_u64,
    do: pad_u64(value)

  defp encode_u64(_value),
    do: raise(ArgumentError, "cold index value must be an unsigned 64-bit integer")

  defp encode_i64(value) when is_integer(value) and value >= @min_i64 and value <= @max_i64 do
    value
    |> Kernel.+(9_223_372_036_854_775_808)
    |> encode_u64()
  end

  defp encode_i64(_value),
    do: raise(ArgumentError, "cold priority must be a signed 64-bit integer")

  defp escape_key_part(value) when is_binary(value), do: Base.url_encode64(value, padding: false)

  defp escape_key_part(value) when is_atom(value),
    do: value |> Atom.to_string() |> escape_key_part()

  defp escape_key_part(_value), do: raise(ArgumentError, "cold index key parts must be strings")

  defp valid_park_fields?(
         %{
           locator: %Locator{kind: :state} = locator,
           due_at_ms: due_at_ms,
           type: type,
           state: state,
           partition_key: partition_key,
           state_key: state_key,
           priority: priority,
           lease_until_ms: lease_until_ms,
           fencing_token: fencing_token,
           retention_at_ms: retention_at_ms,
           value_refs_digest: value_refs_digest,
           checksum: checksum
         } = fields
       ) do
    map_size(fields) == 12 and Locator.durable?(locator) and optional_u64?(due_at_ms) and
      optional_bounded_binary?(type, @max_key_bytes) and
      optional_bounded_binary?(state, @max_key_bytes) and
      optional_bounded_binary?(partition_key, @max_key_bytes) and
      optional_bounded_binary?(state_key, @max_key_bytes) and signed_i64?(priority) and
      optional_u64?(lease_until_ms) and optional_u64?(fencing_token) and
      optional_u64?(retention_at_ms) and optional_digest?(value_refs_digest) and
      optional_bounded_binary?(checksum, @max_checksum_bytes)
  end

  defp valid_park_fields?(_fields), do: false

  defp valid_value_locator_fields?(
         %{
           value_ref: value_ref,
           owner_flow_id: owner_flow_id,
           owner_version: owner_version,
           locator: %Locator{kind: :value} = locator,
           ref_kind: ref_kind,
           expire_at_ms: expire_at_ms,
           checksum: checksum
         } = fields
       ) do
    map_size(fields) == 7 and Locator.durable?(locator) and
      bounded_binary?(value_ref, @max_key_bytes) and
      bounded_binary?(owner_flow_id, @max_run_id_bytes) and u64?(owner_version) and
      locator.flow_id == owner_flow_id and
      locator.version == owner_version and
      optional_ref_kind?(ref_kind) and optional_u64?(expire_at_ms) and
      optional_bounded_binary?(checksum, @max_checksum_bytes)
  end

  defp valid_value_locator_fields?(_fields), do: false

  defp u64?(value), do: is_integer(value) and value >= 0 and value <= @max_u64
  defp optional_u64?(nil), do: true
  defp optional_u64?(value), do: u64?(value)
  defp signed_i64?(value), do: is_integer(value) and value >= @min_i64 and value <= @max_i64

  defp bounded_binary?(value, maximum),
    do: is_binary(value) and value != "" and byte_size(value) <= maximum

  defp optional_bounded_binary?(nil, _maximum), do: true
  defp optional_bounded_binary?(value, maximum), do: bounded_binary?(value, maximum)
  defp optional_digest?(nil), do: true

  defp optional_digest?(digest),
    do: is_binary(digest) and byte_size(digest) == @value_refs_digest_bytes

  defp optional_ref_kind?(nil), do: true
  defp optional_ref_kind?(value), do: value in @value_ref_kinds

  defp encode_bounded!(term, error_message) do
    encoded = TermCodec.encode(term)

    if byte_size(encoded) <= @max_encoded_bytes,
      do: encoded,
      else: raise(ArgumentError, error_message)
  end
end
