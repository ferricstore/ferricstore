defmodule Ferricstore.HyperLogLog do
  @moduledoc """
  Pure-Elixir HyperLogLog probabilistic cardinality estimator.

  HyperLogLog (HLL) is a space-efficient probabilistic data structure that
  estimates the number of distinct elements (cardinality) in a multiset.
  It uses a fixed-size sketch of ~16 KB to provide estimates with a
  standard error of approximately 0.81%.

  ## Design

  This implementation uses:

    * **14-bit precision** -- 2^14 = 16,384 registers
    * **1 byte per register** -- only the lower 6 bits are used (values 0..63)
    * **128-bit hash** -- MD5 via `:crypto.hash/2`, truncated to 64 bits for
      register index and rank computation
    * **Bias correction** -- the standard HyperLogLog formula with small-range
      (linear counting) and large-range corrections

  Sketches are stored as plain 16,384-byte binaries compatible with the store
  layer's `put/3` and `get/1` interface. No special encoding or headers are
  needed.

  ## Algorithm

  For each element added:

  1. Compute a 64-bit hash from the element's MD5 digest.
  2. Use the lower 14 bits as a register index (0..16,383).
  3. Count the number of leading zeros in the remaining upper 50 bits, add 1
     to get the "rank".
  4. Store the rank in the register if it exceeds the current value (max wins).

  Cardinality estimation applies the harmonic mean across all registers with
  the alpha correction constant and small/large range bias corrections.

  ## Examples

      iex> sketch = Ferricstore.HyperLogLog.new()
      iex> {sketch, true} = Ferricstore.HyperLogLog.add(sketch, "hello")
      iex> Ferricstore.HyperLogLog.count(sketch) > 0
      true

      iex> s1 = Ferricstore.HyperLogLog.new()
      iex> s2 = Ferricstore.HyperLogLog.new()
      iex> {s1, _} = Ferricstore.HyperLogLog.add(s1, "a")
      iex> {s2, _} = Ferricstore.HyperLogLog.add(s2, "b")
      iex> merged = Ferricstore.HyperLogLog.merge(s1, s2)
      iex> Ferricstore.HyperLogLog.count(merged) >= 1
      true
  """

  import Bitwise

  # 14-bit precision = 16,384 registers.
  @precision 14
  @num_registers Bitwise.bsl(1, @precision)
  @register_mask @num_registers - 1

  # Number of bits remaining after extracting the register index.
  # 64 total hash bits - 14 precision bits = 50 bits for rank.
  @remaining_bits 64 - @precision
  @max_rank @remaining_bits + 1
  @hash_space_size 18_446_744_073_709_551_616.0
  @large_range_threshold @hash_space_size / 30.0
  @inverse_powers 0..@max_rank
                  |> Enum.map(&:math.pow(2.0, -&1))
                  |> List.to_tuple()

  @typedoc "A 16,384-byte binary representing a HyperLogLog sketch."
  @type sketch :: <<_::131_072>>

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new empty HyperLogLog sketch.

  Returns a 16,384-byte binary of zeroes.

  ## Examples

      iex> sketch = Ferricstore.HyperLogLog.new()
      iex> byte_size(sketch)
      16384
  """
  @spec new() :: sketch()
  def new, do: :binary.copy(<<0>>, @num_registers)

  @doc false
  @spec sketch_size() :: pos_integer()
  def sketch_size, do: @num_registers

  @doc """
  Adds an element to the sketch.

  The element is hashed with MD5 and the resulting 64-bit value determines
  the register index and rank. The register is updated only if the new rank
  exceeds the stored value.

  ## Parameters

    * `sketch` -- existing 16,384-byte HLL sketch
    * `element` -- any binary element to add

  ## Returns

  `{updated_sketch, modified?}` where `modified?` is `true` if the sketch
  was changed (i.e. the register was updated), `false` otherwise.

  ## Examples

      iex> sketch = Ferricstore.HyperLogLog.new()
      iex> {sketch, true} = Ferricstore.HyperLogLog.add(sketch, "hello")
      iex> {_sketch, false} = Ferricstore.HyperLogLog.add(sketch, "hello")
  """
  @spec add(sketch(), binary()) :: {sketch(), boolean()}
  def add(sketch, element) when is_binary(sketch) and is_binary(element) do
    hash = hash64(element)
    index = hash &&& @register_mask
    remaining = hash >>> @precision
    rank = count_leading_zeros(remaining) + 1
    current = :binary.at(sketch, index)

    if rank > current do
      <<prefix::binary-size(index), _old, suffix::binary>> = sketch
      {<<prefix::binary, rank::8, suffix::binary>>, true}
    else
      {sketch, false}
    end
  end

  @doc """
  Estimates the cardinality (number of distinct elements) of the sketch.

  Applies the standard HyperLogLog estimation formula with small-range
  (linear counting) and large-range (2^64 correction) bias adjustments.

  ## Parameters

    * `sketch` -- a 16,384-byte HLL sketch

  ## Returns

  A non-negative integer representing the estimated cardinality.

  ## Examples

      iex> sketch = Ferricstore.HyperLogLog.new()
      iex> Ferricstore.HyperLogLog.count(sketch)
      0
  """
  @spec count(sketch()) :: non_neg_integer()
  def count(sketch) when is_binary(sketch) and byte_size(sketch) == @num_registers do
    {inverse_sum, zeros} = estimate_inputs!(sketch, 0.0, 0)
    alpha = alpha_m(@num_registers)
    m = @num_registers
    raw_estimate = alpha * m * m / inverse_sum

    estimate = correct_estimate(raw_estimate, zeros, m)
    round(estimate)
  end

  def count(_sketch), do: raise(ArgumentError, "invalid HyperLogLog sketch")

  @doc """
  Merges two sketches by taking the maximum register value at each position.

  The merged sketch represents the union of the two input multisets.

  ## Parameters

    * `sketch1` -- first 16,384-byte HLL sketch
    * `sketch2` -- second 16,384-byte HLL sketch

  ## Returns

  A new 16,384-byte HLL sketch containing the element-wise maximum.

  ## Examples

      iex> s1 = Ferricstore.HyperLogLog.new()
      iex> s2 = Ferricstore.HyperLogLog.new()
      iex> merged = Ferricstore.HyperLogLog.merge(s1, s2)
      iex> byte_size(merged)
      16384
  """
  @spec merge(sketch(), sketch()) :: sketch()
  def merge(sketch1, sketch2)
      when is_binary(sketch1) and is_binary(sketch2) and
             byte_size(sketch1) == @num_registers and
             byte_size(sketch2) == @num_registers do
    do_merge(sketch1, sketch2, 0, [])
  end

  @doc """
  Returns the number of registers in the sketch.

  This is a compile-time constant (16,384 for 14-bit precision).

  ## Examples

      iex> Ferricstore.HyperLogLog.num_registers()
      16384
  """
  @spec num_registers() :: pos_integer()
  def num_registers, do: @num_registers

  @doc """
  Validates that a binary is a well-formed HLL sketch.

  A valid sketch is exactly 16,384 bytes long and every register contains a
  rank in the range produced by the 50-bit rank calculation (0..51).

  ## Parameters

    * `binary` -- the binary to validate

  ## Returns

  `true` if the binary has the expected length and register values, `false`
  otherwise.

  ## Examples

      iex> Ferricstore.HyperLogLog.valid_sketch?(Ferricstore.HyperLogLog.new())
      true

      iex> Ferricstore.HyperLogLog.valid_sketch?(<<1, 2, 3>>)
      false
  """
  @spec valid_sketch?(binary()) :: boolean()
  def valid_sketch?(binary) when is_binary(binary) and byte_size(binary) == @num_registers,
    do: valid_registers?(binary)

  def valid_sketch?(_), do: false

  # ---------------------------------------------------------------------------
  # Private — Hashing
  # ---------------------------------------------------------------------------

  # Produces a 64-bit unsigned integer from the element's MD5 digest.
  # MD5 is not cryptographically relevant here — we only need uniform
  # distribution across the 64-bit space.
  @spec hash64(binary()) :: non_neg_integer()
  defp hash64(element) do
    <<hash::unsigned-big-64, _rest::binary>> = :crypto.hash(:md5, element)
    hash
  end

  # ---------------------------------------------------------------------------
  # Private — Leading-zero count
  # ---------------------------------------------------------------------------

  # Counts leading zeros in the upper @remaining_bits bits of `value`.
  # If the value is 0, all bits are zero, so we return @remaining_bits.
  @spec count_leading_zeros(non_neg_integer()) :: non_neg_integer()
  defp count_leading_zeros(0), do: @remaining_bits

  defp count_leading_zeros(value) do
    do_clz(value, @remaining_bits - 1, 0)
  end

  defp do_clz(_value, -1, count), do: count

  defp do_clz(value, bit_pos, count) do
    if (value &&& 1 <<< bit_pos) != 0 do
      count
    else
      do_clz(value, bit_pos - 1, count + 1)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — Register validation and estimation inputs
  # ---------------------------------------------------------------------------

  defp valid_registers?(<<>>), do: true

  defp valid_registers?(<<register, rest::binary>>) when register <= @max_rank,
    do: valid_registers?(rest)

  defp valid_registers?(_invalid), do: false

  defp estimate_inputs!(<<>>, inverse_sum, zeros), do: {inverse_sum, zeros}

  defp estimate_inputs!(<<register, rest::binary>>, inverse_sum, zeros)
       when register <= @max_rank do
    estimate_inputs!(
      rest,
      inverse_sum + elem(@inverse_powers, register),
      if(register == 0, do: zeros + 1, else: zeros)
    )
  end

  defp estimate_inputs!(_invalid, _inverse_sum, _zeros),
    do: raise(ArgumentError, "invalid HyperLogLog sketch")

  # ---------------------------------------------------------------------------
  # Private — Merge (element-wise max)
  # ---------------------------------------------------------------------------

  defp do_merge(_s1, _s2, @num_registers, acc) do
    acc
    |> Enum.reverse()
    |> :erlang.list_to_binary()
  end

  defp do_merge(s1, s2, i, acc) do
    v1 = :binary.at(s1, i)
    v2 = :binary.at(s2, i)
    do_merge(s1, s2, i + 1, [max(v1, v2) | acc])
  end

  # ---------------------------------------------------------------------------
  # Private — Estimation corrections
  # ---------------------------------------------------------------------------

  # Alpha constant for the given number of registers.
  @spec alpha_m(pos_integer()) :: float()
  defp alpha_m(16), do: 0.673
  defp alpha_m(32), do: 0.697
  defp alpha_m(64), do: 0.709

  defp alpha_m(m) when m >= 128 do
    0.7213 / (1.0 + 1.079 / m)
  end

  # Apply small-range (linear counting) and large-range (2^64) corrections.
  @spec correct_estimate(float(), non_neg_integer(), pos_integer()) :: float()
  defp correct_estimate(raw_estimate, zeros, m) do
    cond do
      # Small range correction: use linear counting when estimate is small
      # and there are zero-valued registers.
      raw_estimate <= 2.5 * m ->
        if zeros > 0 do
          # Linear counting
          m * :math.log(m / zeros)
        else
          raw_estimate
        end

      # The estimator cannot represent a cardinality beyond its hash space.
      raw_estimate >= @hash_space_size ->
        @hash_space_size

      # Large range correction: when estimate approaches 2^64
      raw_estimate > @large_range_threshold ->
        # -2^64 * log(1 - E/2^64)
        -@hash_space_size * :math.log(1.0 - raw_estimate / @hash_space_size)

      # Normal range — no correction needed
      true ->
        raw_estimate
    end
  end
end
