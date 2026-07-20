defmodule Ferricstore.Flow.SystemMetadata do
  @moduledoc false

  alias Ferricstore.TermCodec

  @tag :flow_system_metadata
  @max_fields 16
  @max_encoded_bytes 8 * 1_024
  @max_field_id 0xFFFF
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @min_i64 -0x8000_0000_0000_0000
  @max_i64 0x7FFF_FFFF_FFFF_FFFF
  @max_keyword_bytes 4_096
  @types [:uint64, :int64, :keyword, :boolean, :datetime]
  @roles [:isolation_scope, :system_metadata]

  @type field_id :: non_neg_integer()
  @type type :: :uint64 | :int64 | :keyword | :boolean | :datetime
  @type role :: :isolation_scope | :system_metadata
  @type sealed_value :: {pos_integer(), type(), role(), term()}
  @type t :: %{optional(field_id()) => sealed_value()}

  @spec seal(map(), map()) :: {:ok, t()} | {:error, :invalid_flow_system_metadata}
  def seal(fields, values) when is_map(fields) and is_map(values) do
    if map_size(values) <= @max_fields do
      values
      |> Enum.reduce_while({:ok, %{}}, fn {id, value}, {:ok, acc} ->
        case Map.fetch(fields, id) do
          {:ok, %{version: version, type: type, role: role}}
          when is_integer(version) and version > 0 and role in @roles ->
            case seal_value(type, value) do
              {:ok, {^type, sealed_value}} ->
                {:cont, {:ok, Map.put(acc, id, {version, type, role, sealed_value})}}

              :error ->
                {:halt, invalid()}
            end

          :error ->
            {:halt, invalid()}
        end
      end)
      |> validate_encoded_result()
    else
      invalid()
    end
  end

  def seal(_fields, _values), do: invalid()

  @spec validate(t()) :: :ok | {:error, :invalid_flow_system_metadata}
  def validate(metadata) when is_map(metadata) and map_size(metadata) <= @max_fields do
    valid? =
      Enum.all?(metadata, fn
        {id, {version, type, role, value}}
        when is_integer(id) and id > 0 and id <= @max_field_id and is_integer(version) and
               version > 0 and type in @types and role in @roles ->
          match?({:ok, {^type, ^value}}, seal_value(type, value))

        _invalid ->
          false
      end)

    if valid? and encoded_size(metadata) <= @max_encoded_bytes, do: :ok, else: invalid()
  end

  def validate(_metadata), do: invalid()

  @spec record(map()) :: t()
  def record(record) when is_map(record) do
    metadata = Map.get(record, :system_metadata, %{})

    case validate(metadata) do
      :ok ->
        metadata

      {:error, :invalid_flow_system_metadata} ->
        raise ArgumentError, "invalid flow system metadata"
    end
  end

  def record(_record), do: %{}

  @spec encode(t()) :: binary()
  def encode(metadata) when is_map(metadata) do
    case validate(metadata) do
      :ok ->
        entries = metadata |> Enum.sort_by(&elem(&1, 0))
        encoded = TermCodec.encode({@tag, entries})

        if byte_size(encoded) <= @max_encoded_bytes,
          do: Base.url_encode64(encoded, padding: false),
          else: raise(ArgumentError, "invalid flow system metadata")

      {:error, :invalid_flow_system_metadata} ->
        raise ArgumentError, "invalid flow system metadata"
    end
  end

  @spec decode(term()) :: {:ok, t()} | {:error, :invalid_flow_system_metadata}
  def decode(encoded) when is_binary(encoded) and byte_size(encoded) <= @max_encoded_bytes * 2 do
    with {:ok, blob} <- Base.url_decode64(encoded, padding: false),
         true <- byte_size(blob) <= @max_encoded_bytes,
         {:ok, {@tag, entries}} <- TermCodec.decode(blob),
         true <- is_list(entries) and length(entries) <= @max_fields,
         metadata <- Map.new(entries),
         true <- map_size(metadata) == length(entries),
         :ok <- validate(metadata) do
      {:ok, metadata}
    else
      _invalid -> invalid()
    end
  rescue
    _error -> invalid()
  end

  def decode(_encoded), do: invalid()

  @spec scope_prefix(t()) :: {:ok, binary() | nil} | {:error, :invalid_flow_system_metadata}
  def scope_prefix(metadata) when is_map(metadata) do
    with :ok <- validate(metadata) do
      entries =
        metadata
        |> Enum.filter(fn {_id, {_version, _type, role, _value}} ->
          role == :isolation_scope
        end)
        |> Enum.sort_by(&elem(&1, 0))

      case entries do
        [] ->
          {:ok, nil}

        [{_id, {_version, :uint64, :isolation_scope, value}}] ->
          {:ok, <<value::unsigned-big-64>>}

        _multiple_or_non_u64 ->
          {:ok, TermCodec.encode({:flow_isolation_scope, entries})}
      end
    end
  end

  def scope_prefix(_metadata), do: invalid()

  @spec validate_against(t(), map()) :: :ok | {:error, :invalid_flow_system_metadata}
  def validate_against(metadata, fields) when is_map(metadata) and is_map(fields) do
    with :ok <- validate(metadata) do
      if Enum.all?(metadata, fn {id, {version, type, role, _value}} ->
           case Map.fetch(fields, id) do
             {:ok, field} ->
               field.version == version and field.type == type and field.role == role

             :error ->
               false
           end
         end),
         do: :ok,
         else: invalid()
    end
  end

  def validate_against(_metadata, _fields), do: invalid()

  @spec fetch(map(), field_id()) :: {:ok, term()} | :missing
  def fetch(record, id) when is_map(record) and is_integer(id) do
    case Map.get(record, :system_metadata, %{}) do
      %{^id => {_version, _type, _role, value}} -> {:ok, value}
      _missing -> :missing
    end
  end

  @doc false
  @spec typed_value(sealed_value()) :: {type(), term()}
  def typed_value({_version, type, _role, value}), do: {type, value}

  @spec put_record(map(), t()) :: map()
  def put_record(record, metadata) when is_map(record) and metadata == %{},
    do: Map.delete(record, :system_metadata)

  def put_record(record, metadata) when is_map(record) and is_map(metadata) do
    case validate(metadata) do
      :ok ->
        Map.put(record, :system_metadata, metadata)

      {:error, :invalid_flow_system_metadata} ->
        raise ArgumentError, "invalid flow system metadata"
    end
  end

  defp validate_encoded_result({:ok, metadata}) do
    if encoded_size(metadata) <= @max_encoded_bytes, do: {:ok, metadata}, else: invalid()
  end

  defp validate_encoded_result({:error, _reason} = error), do: error

  defp encoded_size(metadata),
    do: metadata |> Map.to_list() |> then(&TermCodec.encode({@tag, &1})) |> byte_size()

  defp seal_value(:uint64, value)
       when is_integer(value) and value >= 0 and value <= @max_u64,
       do: {:ok, {:uint64, value}}

  defp seal_value(type, value)
       when type in [:int64, :datetime] and is_integer(value) and value >= @min_i64 and
              value <= @max_i64,
       do: {:ok, {type, value}}

  defp seal_value(:keyword, value)
       when is_binary(value) and value != "" and byte_size(value) <= @max_keyword_bytes do
    if String.valid?(value), do: {:ok, {:keyword, value}}, else: :error
  end

  defp seal_value(:boolean, value) when is_boolean(value), do: {:ok, {:boolean, value}}
  defp seal_value(_type, _value), do: :error

  defp invalid, do: {:error, :invalid_flow_system_metadata}
end
