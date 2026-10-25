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

  @doc """
  Parses an integer the way Redis's string2ll does: only its canonical form,
  so no "+" sign, no leading zeros and no "-0" ("05", "+5", "-0" are not
  integers). Returns `{:ok, integer}` or `:error`.
  """
  @spec parse_canonical_integer(binary()) :: {:ok, integer()} | :error
  def parse_canonical_integer(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} -> if Integer.to_string(val) == str, do: {:ok, val}, else: :error
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

  @doc """
  Formats a float as Redis formats INCRBYFLOAT and HINCRBYFLOAT results: the
  shortest digits that read back as the same float, in plain decimal notation
  without an exponent or a trailing ".0" ("3", "5200", "0.00000015").
  """
  @spec format_float_plain(float()) :: binary()
  def format_float_plain(val) when is_float(val) do
    {sign, short} =
      case :erlang.float_to_binary(val, [:short]) do
        "-" <> rest -> {"-", rest}
        short -> {"", short}
      end

    {mantissa, exponent} =
      case :binary.split(short, "e") do
        [mantissa, exponent] -> {mantissa, String.to_integer(exponent)}
        [mantissa] -> {mantissa, 0}
      end

    {integer_digits, fraction_digits} =
      case :binary.split(mantissa, ".") do
        [integer_digits, fraction_digits] -> {integer_digits, fraction_digits}
        [integer_digits] -> {integer_digits, ""}
      end

    case plain_decimal(integer_digits <> fraction_digits, byte_size(integer_digits) + exponent) do
      "0" -> "0"
      plain -> sign <> plain
    end
  end

  @doc """
  Formats a float as Redis replies with a double, such as a sorted set score:
  an integral value within +/-2^62 as an integer ("3", "100000000000000000"),
  any other with the shortest digits that read back as the same float, in
  plain notation for moderate exponents ("2.5", "0.00001",
  "12345678901234567000") and in exponent form otherwise ("1.5e-7", "1e+20").
  """
  @spec format_double(float()) :: binary()
  def format_double(val) when is_float(val) do
    cond do
      val == 0.0 ->
        "0"

      abs(val) <= 4.611686018427388e18 and val == Float.round(val) ->
        Integer.to_string(trunc(val))

      val < 0 ->
        "-" <> format_shortest(-val)

      true ->
        format_shortest(val)
    end
  end

  # Redis's fpconv layout of the shortest digits of a positive float.
  defp format_shortest(val) do
    {digits, k} = shortest_digits(val)
    count = byte_size(digits)
    exponent = abs(k + count - 1)

    cond do
      k >= 0 and exponent < count + 7 ->
        digits <> String.duplicate("0", k)

      k < 0 and (k > -7 or exponent < 4) ->
        plain_decimal(digits, count + k)

      true ->
        mantissa =
          if count > 1,
            do: binary_part(digits, 0, 1) <> "." <> binary_part(digits, 1, count - 1),
            else: digits

        sign = if k + count - 1 < 0, do: "-", else: "+"
        mantissa <> "e" <> sign <> Integer.to_string(exponent)
    end
  end

  # The shortest significant digits of a positive float, without leading or
  # trailing zeros, and the power of ten of the last one.
  defp shortest_digits(val) do
    {mantissa, exponent} =
      case :binary.split(:erlang.float_to_binary(val, [:short]), "e") do
        [mantissa, exponent] -> {mantissa, String.to_integer(exponent)}
        [mantissa] -> {mantissa, 0}
      end

    {integer_digits, fraction_digits} =
      case :binary.split(mantissa, ".") do
        [integer_digits, fraction_digits] -> {integer_digits, fraction_digits}
        [integer_digits] -> {integer_digits, ""}
      end

    digits = String.trim_leading(integer_digits <> fraction_digits, "0")
    trimmed = String.trim_trailing(digits, "0")
    {trimmed, exponent - byte_size(fraction_digits) + byte_size(digits) - byte_size(trimmed)}
  end

  # `digits` with the decimal point after `point` of them.
  defp plain_decimal(digits, point) do
    trimmed = String.trim_leading(digits, "0")
    point = point - (byte_size(digits) - byte_size(trimmed))
    digits = String.trim_trailing(trimmed, "0")
    count = byte_size(digits)

    cond do
      digits == "" -> "0"
      point <= 0 -> "0." <> String.duplicate("0", -point) <> digits
      point >= count -> digits <> String.duplicate("0", point - count)
      true -> binary_part(digits, 0, point) <> "." <> binary_part(digits, point, count - point)
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
