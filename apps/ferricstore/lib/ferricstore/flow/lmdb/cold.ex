defmodule Ferricstore.Flow.LMDB.Cold do
  @moduledoc false

  alias Ferricstore.Flow.Locator

  @u64_decimal_zero_pad "00000000000000000000"

  def park_key(flow_id) when is_binary(flow_id), do: "flow:park:v1:" <> flow_id

  def park_key_for_state_key(state_key) when is_binary(state_key),
    do: "flow:park:v1:key:" <> escape_key_part(state_key)

  def due_bucket_ms(due_at_ms, bucket_ms \\ 60_000)

  def due_bucket_ms(due_at_ms, bucket_ms)
      when is_integer(due_at_ms) and due_at_ms >= 0 and is_integer(bucket_ms) and bucket_ms > 0 do
    div(due_at_ms, bucket_ms) * bucket_ms
  end

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
    by_segment_prefix(locator.file_id) <>
      ":" <>
      Enum.join(
        [
          encode_u64(locator.offset),
          escape_key_part(locator.flow_id),
          encode_u64(locator.version)
        ],
        ":"
      )
  end

  def by_segment_prefix(file_id) do
    ["flow:cold:by-segment:v1", escape_key_part(:erlang.term_to_binary(file_id))]
    |> Enum.join(":")
  end

  def encode_park(%Locator{kind: :state} = locator, attrs)
      when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)

    :erlang.term_to_binary(
      {:flow_cold_park, 1,
       %{
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
       }}
    )
  end

  def decode_park(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {:flow_cold_park, 1, %{locator: %Locator{} = locator} = fields} ->
        if Locator.valid?(locator), do: {:ok, fields}, else: :error

      _other ->
        :not_cold_park
    end
  rescue
    _ -> :error
  end

  def encode_value_locator(value_ref, owner_flow_id, owner_version, %Locator{kind: :value} = locator, attrs \\ [])
      when is_binary(value_ref) and is_binary(owner_flow_id) and is_integer(owner_version) and
             owner_version >= 0 and (is_map(attrs) or is_list(attrs)) do
    attrs = Map.new(attrs)

    :erlang.term_to_binary(
      {:flow_cold_value_locator, 1,
       %{
         value_ref: value_ref,
         owner_flow_id: owner_flow_id,
         owner_version: owner_version,
         locator: locator,
         ref_kind: Map.get(attrs, :ref_kind),
         expire_at_ms: Map.get(attrs, :expire_at_ms),
         checksum: Map.get(attrs, :checksum)
       }}
    )
  end

  def decode_value_locator(blob) when is_binary(blob) do
    case :erlang.binary_to_term(blob, [:safe]) do
      {:flow_cold_value_locator, 1, %{locator: %Locator{} = locator} = fields} ->
        if Locator.valid?(locator), do: {:ok, fields}, else: :error

      _other ->
        :not_cold_value_locator
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

  defp encode_u64(value) when is_integer(value) and value >= 0, do: pad_u64(value)

  defp encode_i64(value) when is_integer(value) do
    value
    |> Kernel.+(9_223_372_036_854_775_808)
    |> encode_u64()
  end

  defp escape_key_part(value) when is_binary(value), do: Base.url_encode64(value, padding: false)

  defp escape_key_part(value) when is_atom(value),
    do: value |> Atom.to_string() |> escape_key_part()
end
