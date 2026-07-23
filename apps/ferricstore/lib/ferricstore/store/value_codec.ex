defmodule Ferricstore.Store.ValueCodec do
  @moduledoc """
  Shared helpers for parsing, formatting, and encoding values.

  Extracted from `Shard` and `StateMachine` to eliminate code duplication
  (performance audit L2 / memory audit L7). Both modules delegate to this
  module instead of maintaining identical private copies.
  """

  alias Ferricstore.HLC

  @integer_float_fast_path_bytes 32
  @ratelimit_encoded_size 24

  # ---------------------------------------------------------------------------
  # Integer parsing
  # ---------------------------------------------------------------------------

  @doc "Parses a binary as an integer. Returns `{:ok, integer}` or `:error`."
  @spec parse_integer(binary()) :: {:ok, integer()} | :error
  def parse_integer(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} -> {:ok, val}
      _ -> :error
    end
  end

  # ---------------------------------------------------------------------------
  # Float parsing and formatting
  # ---------------------------------------------------------------------------

  @doc """
  Parses a binary as a float. Accepts integer strings ("10") and float
  strings ("3.14"). Returns `{:ok, float}` or `:error`.
  """
  @spec parse_float(binary()) :: {:ok, float()} | :error
  def parse_float(str)
      when is_binary(str) and byte_size(str) <= @integer_float_fast_path_bytes do
    case Integer.parse(str) do
      {val, ""} ->
        {:ok, val * 1.0}

      _ ->
        parse_float_value(str)
    end
  end

  def parse_float(str) when is_binary(str), do: parse_float_value(str)

  defp parse_float_value(str) do
    case Float.parse(str) do
      {val, ""} when val not in [:infinity, :neg_infinity, :nan] -> {:ok, val}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
    ArithmeticError -> :error
  end

  @doc "Converts an integer or float to a finite BEAM float without raising."
  @spec number_to_float(number()) :: {:ok, float()} | :error
  def number_to_float(value) when is_float(value), do: {:ok, value}

  def number_to_float(value) when is_integer(value) do
    {:ok, value * 1.0}
  rescue
    ArithmeticError -> :error
  end

  def number_to_float(_value), do: :error

  @doc "Adds numeric operands without allowing float conversion or arithmetic exceptions to escape."
  @spec checked_float_add(term(), term()) :: {:ok, float()} | :error | :overflow
  def checked_float_add(left, right) when is_float(left) and is_float(right) do
    {:ok, left + right}
  rescue
    ArithmeticError -> :overflow
  end

  def checked_float_add(left, right) do
    with {:ok, left_float} <- number_to_float(left),
         {:ok, right_float} <- number_to_float(right) do
      checked_float_add(left_float, right_float)
    end
  end

  @doc """
  Formats a float for Redis INCRBYFLOAT output: compact decimals, strips
  trailing zeros and unnecessary decimal point.

  Uses `:binary.match/2` instead of `String.contains?/2` and removes the
  dead-code no-op `then` that was present in the original duplicated versions.
  """
  @spec format_float(float()) :: binary()
  def format_float(val) when is_float(val) do
    formatted = :erlang.float_to_binary(val, [:short])

    case :binary.match(formatted, "e") do
      {exponent_offset, 1} ->
        mantissa = binary_part(formatted, 0, exponent_offset)
        exponent = binary_part(formatted, exponent_offset, byte_size(formatted) - exponent_offset)
        trim_float_mantissa(mantissa) <> exponent

      :nomatch ->
        trim_float_mantissa(formatted)
    end
  end

  defp trim_float_mantissa(mantissa) do
    case :binary.match(mantissa, ".") do
      :nomatch ->
        mantissa

      _ ->
        mantissa
        |> String.trim_trailing("0")
        |> String.trim_trailing(".")
    end
  end

  # ---------------------------------------------------------------------------
  # Rate limiter encoding
  # ---------------------------------------------------------------------------

  @doc """
  Encodes rate limiter state as a fixed 24-byte binary.

  ~10x faster than the old string format (`"\#{cur}:\#{start}:\#{prev}"`).
  No allocation, no parsing — just bit packing.
  """
  @spec encode_ratelimit(integer(), integer(), integer()) :: binary()
  def encode_ratelimit(cur, start, prev), do: <<cur::64, start::64, prev::64>>

  @doc false
  @spec ratelimit_encoded_size() :: 24
  def ratelimit_encoded_size, do: @ratelimit_encoded_size

  @doc """
  Decodes rate limiter state from its fixed-width 24-byte binary format.
  """
  @spec decode_ratelimit(binary()) :: {integer(), integer(), integer()}
  def decode_ratelimit(value), do: decode_ratelimit(value, HLC.now_ms())

  @doc """
  Decodes rate limiter state, using `fallback_start_ms` for malformed values.

  Raft apply paths pass their stamped apply time here so corrupted or malformed
  state is repaired deterministically on every replica.
  """
  @spec decode_ratelimit(binary(), integer()) :: {integer(), integer(), integer()}
  def decode_ratelimit(<<cur::64, start::64, prev::64>>, _fallback_start_ms),
    do: {cur, start, prev}

  def decode_ratelimit(_value, fallback_start_ms),
    do: {0, fallback_start_ms, 0}
end
