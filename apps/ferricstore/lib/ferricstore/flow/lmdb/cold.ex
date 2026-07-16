defmodule Ferricstore.Flow.LMDB.Cold do
  @moduledoc false

  alias Ferricstore.Flow.Locator
  alias Ferricstore.TermCodec

  @u64_decimal_zero_pad "00000000000000000000"
  @max_u64 18_446_744_073_709_551_615
  @min_i64 -9_223_372_036_854_775_808
  @max_i64 9_223_372_036_854_775_807

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
    ["flow:cold:by-segment:v1", escape_key_part(TermCodec.encode(file_id))]
    |> Enum.join(":")
  end

  def encode_park(%Locator{kind: :state} = locator, attrs)
      when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

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
      state_value: Map.get(attrs, :state_value),
      checksum: Map.get(attrs, :checksum)
    }

    if valid_park_fields?(fields) do
      TermCodec.encode({:flow_cold_park, 1, fields})
    else
      raise ArgumentError, "invalid Flow cold park fields"
    end
  end

  def decode_park(blob) when is_binary(blob) do
    case TermCodec.decode(blob) do
      {:ok, {:flow_cold_park, 1, %{locator: %Locator{} = locator} = fields}} ->
        if Locator.valid?(locator) and valid_park_fields?(fields), do: {:ok, fields}, else: :error

      {:ok, {:flow_cold_park, 1, _invalid_fields}} ->
        :error

      {:ok, _other} ->
        :not_cold_park

      {:error, :invalid_external_term} ->
        :error
    end
  rescue
    _ -> :error
  end

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
      TermCodec.encode({:flow_cold_value_locator, 1, fields})
    else
      raise ArgumentError, "invalid Flow cold value locator fields"
    end
  end

  def encode_value_locator(_value_ref, _owner_flow_id, _owner_version, _locator, _attrs),
    do: raise(ArgumentError, "invalid Flow cold value locator fields")

  def decode_value_locator(blob) when is_binary(blob) do
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

  defp valid_park_fields?(%{
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
         state_value: state_value,
         checksum: checksum
       }) do
    Locator.valid?(locator) and optional_u64?(due_at_ms) and optional_nonempty_binary?(type) and
      optional_nonempty_binary?(state) and optional_nonempty_binary?(partition_key) and
      optional_nonempty_binary?(state_key) and signed_i64?(priority) and
      optional_u64?(lease_until_ms) and optional_u64?(fencing_token) and
      optional_u64?(retention_at_ms) and optional_nonempty_binary?(value_refs_digest) and
      optional_binary?(state_value) and optional_nonempty_binary?(checksum)
  end

  defp valid_park_fields?(_fields), do: false

  defp valid_value_locator_fields?(%{
         value_ref: value_ref,
         owner_flow_id: owner_flow_id,
         owner_version: owner_version,
         locator: %Locator{kind: :value} = locator,
         ref_kind: ref_kind,
         expire_at_ms: expire_at_ms,
         checksum: checksum
       }) do
    Locator.valid?(locator) and nonempty_binary?(value_ref) and nonempty_binary?(owner_flow_id) and
      u64?(owner_version) and locator.flow_id == owner_flow_id and
      locator.version == owner_version and
      optional_ref_kind?(ref_kind) and optional_u64?(expire_at_ms) and
      optional_nonempty_binary?(checksum)
  end

  defp valid_value_locator_fields?(_fields), do: false

  defp u64?(value), do: is_integer(value) and value >= 0 and value <= @max_u64
  defp optional_u64?(nil), do: true
  defp optional_u64?(value), do: u64?(value)
  defp signed_i64?(value), do: is_integer(value) and value >= @min_i64 and value <= @max_i64
  defp nonempty_binary?(value), do: is_binary(value) and value != ""
  defp optional_nonempty_binary?(nil), do: true
  defp optional_nonempty_binary?(value), do: nonempty_binary?(value)
  defp optional_binary?(nil), do: true
  defp optional_binary?(value), do: is_binary(value)
  defp optional_ref_kind?(nil), do: true
  defp optional_ref_kind?(value), do: is_atom(value) or nonempty_binary?(value)
end
