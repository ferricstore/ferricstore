defmodule FerricstoreServer.Native.Connection.FrameBuffer do
  @moduledoc false

  import Bitwise

  @header_bytes 24
  @max_buffer_bytes 128 * 1024 * 1024
  @max_coalesced_continuation_bytes 64 * 1024
  @max_u32 4_294_967_295
  @max_frame_body_bytes min(@max_buffer_bytes - @header_bytes, @max_u32)
  @magic "FSNP"
  @version 1
  @response_direction 0x80

  defstruct chunks_rev: [],
            buffered_bytes: 0,
            header: "",
            expected_bytes: nil

  @type expected_bytes :: non_neg_integer() | :invalid | nil

  @type t :: %__MODULE__{
          chunks_rev: [binary()],
          buffered_bytes: non_neg_integer(),
          header: binary(),
          expected_bytes: expected_bytes()
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec max_buffer_bytes() :: pos_integer()
  def max_buffer_bytes, do: @max_buffer_bytes

  @spec max_frame_body_bytes() :: pos_integer()
  def max_frame_body_bytes, do: @max_frame_body_bytes

  @spec validate_max_frame_bytes!(term()) :: pos_integer()
  def validate_max_frame_bytes!(value)
      when is_integer(value) and value >= 1 and value <= @max_frame_body_bytes,
      do: value

  def validate_max_frame_bytes!(value) do
    raise ArgumentError,
          "native_max_frame_bytes must be an integer between 1 and " <>
            "#{@max_frame_body_bytes}, got: #{inspect(value)}"
  end

  @spec validate_frame_body_bytes!(term()) :: non_neg_integer()
  def validate_frame_body_bytes!(value)
      when is_integer(value) and value >= 0 and value <= @max_frame_body_bytes,
      do: value

  def validate_frame_body_bytes!(value) do
    raise ArgumentError,
          "native frame body must be between 0 and #{@max_frame_body_bytes} bytes, " <>
            "got: #{inspect(value)}"
  end

  @spec from_binary(binary(), non_neg_integer()) :: t()
  def from_binary("", _max_frame_bytes), do: new()

  def from_binary(binary, max_frame_bytes)
      when is_binary(binary) and is_integer(max_frame_bytes) and max_frame_bytes >= 0 do
    new()
    |> add_data(binary)
    |> classify_header(max_frame_bytes)
  end

  @spec append(t(), binary(), non_neg_integer(), non_neg_integer()) ::
          {:incomplete | :ready, t()} | {:error, :buffer_limit}
  def append(
        %__MODULE__{} = buffer,
        data,
        max_frame_bytes,
        max_buffer_bytes
      )
      when is_binary(data) and is_integer(max_frame_bytes) and max_frame_bytes >= 0 and
             is_integer(max_buffer_bytes) and max_buffer_bytes >= 0 do
    buffer =
      buffer
      |> add_data(data)
      |> classify_header(max_frame_bytes)

    cond do
      buffer.buffered_bytes <= max_buffer_bytes and complete?(buffer) ->
        {:ready, buffer}

      buffer.buffered_bytes <= max_buffer_bytes ->
        {:incomplete, buffer}

      valid_complete_frame?(buffer, max_buffer_bytes) ->
        {:ready, buffer}

      true ->
        {:error, :buffer_limit}
    end
  end

  @spec materialize(t()) :: binary()
  def materialize(%__MODULE__{chunks_rev: []}), do: ""
  def materialize(%__MODULE__{chunks_rev: [binary]}), do: binary

  def materialize(%__MODULE__{chunks_rev: chunks_rev}) do
    chunks_rev
    |> Enum.reverse()
    |> IO.iodata_to_binary()
  end

  @doc false
  @spec stats(t()) :: map()
  def stats(%__MODULE__{} = buffer) do
    %{
      buffered_bytes: buffer.buffered_bytes,
      chunk_count: length(buffer.chunks_rev),
      complete?: complete?(buffer),
      header_bytes: byte_size(buffer.header),
      storage: :iodata
    }
  end

  defp add_data(buffer, ""), do: buffer

  defp add_data(buffer, data) do
    %{
      buffer
      | chunks_rev: [data | buffer.chunks_rev],
        buffered_bytes: buffer.buffered_bytes + byte_size(data),
        header: append_header(buffer.header, data)
    }
  end

  defp append_header(header, _data) when byte_size(header) == @header_bytes, do: header

  defp append_header(header, data) do
    take_bytes = min(@header_bytes - byte_size(header), byte_size(data))
    header <> binary_part(data, 0, take_bytes)
  end

  defp classify_header(%__MODULE__{expected_bytes: nil, header: header} = buffer, max_frame_bytes)
       when byte_size(header) == @header_bytes do
    %{buffer | expected_bytes: expected_frame_bytes(header, max_frame_bytes)}
  end

  defp classify_header(buffer, _max_frame_bytes), do: buffer

  defp expected_frame_bytes(
         <<@magic, version_byte, _flags, _lane_id::unsigned-32, _opcode::unsigned-16,
           _request_id::unsigned-64, body_bytes::unsigned-32>>,
         max_frame_bytes
       ) do
    version = band(version_byte, 0x7F)
    direction = band(version_byte, @response_direction)

    if version == @version and direction == 0 and body_bytes <= max_frame_bytes do
      @header_bytes + body_bytes
    else
      :invalid
    end
  end

  defp expected_frame_bytes(_header, _max_frame_bytes), do: :invalid

  defp complete?(%__MODULE__{expected_bytes: :invalid}), do: true

  defp complete?(%__MODULE__{expected_bytes: expected, buffered_bytes: buffered})
       when is_integer(expected),
       do: buffered >= expected

  defp complete?(%__MODULE__{}), do: false

  defp valid_complete_frame?(%__MODULE__{expected_bytes: expected} = buffer, max_buffer_bytes)
       when is_integer(expected) and expected <= max_buffer_bytes do
    complete?(buffer) and
      buffer.buffered_bytes <= max_buffer_bytes + @max_coalesced_continuation_bytes
  end

  defp valid_complete_frame?(_buffer, _max_buffer_bytes), do: false
end
