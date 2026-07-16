defmodule Ferricstore.TDigest.Core do
  @moduledoc """
  Pure functional t-digest data structure for accurate on-line accumulation of
  rank-based statistics such as quantiles, trimmed means, and cumulative
  distribution values.

  A t-digest compresses a potentially unbounded stream of floating-point
  observations into a compact set of **centroids** -- (mean, weight) pairs --
  that can answer percentile queries with bounded memory.

  Key properties:

    * **Accuracy is best at the tails.** P99, P99.9, and P0.1 estimates are
      more accurate than P50 estimates.
    * **Constant memory.** A t-digest with compression=100 holds roughly
      100-300 centroids regardless of how many samples are added.
    * **Mergeable.** Two t-digests can be merged in O(c log c) time.
    * **Streaming.** Samples can be added one at a time with amortized O(1)
      cost per sample.

  ## Scale function

  Uses the k1 scale function from Dunning's paper:

      k(q) = (delta / (2 * pi)) * arcsin(2q - 1)

  Centroids near the tails (q near 0 or 1) are forced to be small (few
  observations), while centroids near the median can be large.

  ## Usage

      digest = Core.new(100)
      digest = Core.add(digest, 42.0)
      digest = Core.add_many(digest, [1.0, 2.0, 3.0])
      Core.quantile(digest, 0.5)
      Core.cdf(digest, 2.5)
  """

  defstruct compression: 100,
            centroids: [],
            count: 0,
            min: nil,
            max: nil,
            buffer: [],
            buffer_size: 0,
            total_compressions: 0

  @max_compression 1_000

  @type centroid :: {float(), float()}
  @type t :: %__MODULE__{
          compression: pos_integer(),
          centroids: [centroid()],
          count: non_neg_integer(),
          min: float() | nil,
          max: float() | nil,
          buffer: [float()],
          buffer_size: non_neg_integer(),
          total_compressions: non_neg_integer()
        }

  @doc """
  Creates a new empty t-digest with the given compression parameter.

  The compression parameter (delta) controls the accuracy/memory tradeoff.
  Higher values retain more centroids. Typical values: 50-500.

  ## Parameters

    - `compression` - positive integer, default 100

  ## Examples

      iex> Ferricstore.TDigest.Core.new()
      %Ferricstore.TDigest.Core{compression: 100}

      iex> Ferricstore.TDigest.Core.new(200)
      %Ferricstore.TDigest.Core{compression: 200}
  """
  @spec new(pos_integer()) :: t()
  def new(compression \\ 100)

  def new(compression)
      when is_integer(compression) and compression > 0 and compression <= @max_compression do
    %__MODULE__{compression: compression}
  end

  def new(compression) do
    raise ArgumentError,
          "compression must be an integer between 1 and #{@max_compression}, got: #{inspect(compression)}"
  end

  @doc """
  Adds a single floating-point value to the digest.

  The value is buffered internally. When the buffer fills (capacity =
  ceil(compression * 3)), a compression pass runs automatically.

  ## Parameters

    - `digest` - the t-digest struct
    - `value` - a float or integer value to add

  ## Returns

  The updated t-digest struct.
  """
  @spec add(t(), number()) :: t()
  def add(%__MODULE__{} = digest, value) when is_number(value) do
    value = value / 1

    digest =
      digest
      |> update_min_max(value)
      |> buffer_value(value)

    maybe_compress(digest)
  end

  @doc """
  Adds multiple floating-point values to the digest.

  More efficient than calling `add/2` repeatedly as it batches the values
  into the buffer before triggering compression.

  ## Parameters

    - `digest` - the t-digest struct
    - `values` - list of float or integer values

  ## Returns

  The updated t-digest struct.
  """
  @spec add_many(t(), [number()]) :: t()
  def add_many(%__MODULE__{} = digest, values) when is_list(values) do
    Enum.reduce(values, digest, &add(&2, &1))
  end

  @doc """
  Forces a compression pass, flushing the buffer into the centroids.

  This is called automatically when the buffer fills or when a query
  is issued. You can call it manually to force the merge.

  ## Returns

  The compressed t-digest struct with an empty buffer.
  """
  @spec compress(t()) :: t()
  def compress(%__MODULE__{buffer: [], buffer_size: 0} = digest), do: digest

  def compress(%__MODULE__{} = digest) do
    # Convert buffer samples to weight-1 centroids and combine with existing
    buffer_centroids = Enum.map(digest.buffer, &{&1, 1.0})
    all = digest.centroids ++ buffer_centroids

    # Sort by mean
    sorted = Enum.sort_by(all, fn {mean, _weight} -> mean end)

    total_weight = Enum.reduce(sorted, 0.0, fn {_m, w}, acc -> acc + w end)

    # Alternate merge direction to avoid bias
    sorted_for_merge =
      if rem(digest.total_compressions, 2) == 0 do
        sorted
      else
        Enum.reverse(sorted)
      end

    merged = merge_centroids(sorted_for_merge, total_weight, digest.compression)

    # Ensure result is always sorted ascending by mean
    merged =
      if rem(digest.total_compressions, 2) == 0 do
        merged
      else
        Enum.reverse(merged)
      end

    %__MODULE__{
      digest
      | centroids: merged,
        buffer: [],
        buffer_size: 0,
        total_compressions: digest.total_compressions + 1
    }
  end

  @doc """
  Estimates the value at a given quantile position.

  ## Parameters

    - `digest` - the t-digest struct
    - `q` - quantile in [0.0, 1.0]

  ## Returns

    - The estimated value at quantile `q`
    - `:nan` if the digest is empty
  """
  @spec quantile(t(), float()) :: float() | :nan
  def quantile(%__MODULE__{} = digest, q) when is_float(q) and q >= 0.0 and q <= 1.0 do
    digest = ensure_compressed(digest)

    if digest.count == 0 do
      :nan
    else
      do_quantile(digest, q)
    end
  end

  @doc """
  Estimates the cumulative distribution function value for a given observation.

  Returns the estimated fraction of observations less than or equal to `value`.

  ## Parameters

    - `digest` - the t-digest struct
    - `value` - the query value

  ## Returns

    - A float in [0.0, 1.0]
    - `:nan` if the digest is empty
  """
  @spec cdf(t(), float()) :: float() | :nan
  def cdf(%__MODULE__{} = digest, value) when is_number(value) do
    digest = ensure_compressed(digest)

    if digest.count == 0 do
      :nan
    else
      do_cdf(digest, value / 1)
    end
  end

  @doc """
  Estimates the number of observations less than the given value.

  ## Parameters

    - `digest` - the t-digest struct
    - `value` - the query value

  ## Returns

  An integer estimate of the rank (number of values below `value`).
  Returns -1 if `value` is below the minimum observation.
  Returns -2 for empty digest.
  """
  @spec rank(t(), number()) :: integer()
  def rank(%__MODULE__{} = digest, value) when is_number(value) do
    digest = ensure_compressed(digest)

    if digest.count == 0 do
      -2
    else
      value = value / 1

      cond do
        value < digest.min -> -1
        value >= digest.max -> digest.count
        true -> floor(do_cdf(digest, value) * digest.count)
      end
    end
  end

  @doc """
  Estimates the number of observations greater than the given value.

  ## Parameters

    - `digest` - the t-digest struct
    - `value` - the query value

  ## Returns

  An integer estimate of the reverse rank.
  """
  @spec rev_rank(t(), number()) :: integer()
  def rev_rank(%__MODULE__{} = digest, value) when is_number(value) do
    digest = ensure_compressed(digest)

    if digest.count == 0 do
      -2
    else
      r = rank(digest, value)

      cond do
        r == -1 -> digest.count
        r >= digest.count -> -1
        true -> digest.count - r - 1
      end
    end
  end

  @doc """
  Returns the estimated value at a given rank position (0-based).

  ## Parameters

    - `digest` - the t-digest struct
    - `r` - the rank (0-based non-negative integer)

  ## Returns

    - The estimated value at rank `r`
    - `:"-inf"` for rank < 0
    - `:inf` for rank >= count
    - `:nan` for empty digest
  """
  @spec by_rank(t(), integer()) :: float() | :inf | :"-inf" | :nan
  def by_rank(%__MODULE__{} = digest, r) when is_integer(r) do
    digest = ensure_compressed(digest)

    cond do
      digest.count == 0 -> :nan
      r < 0 -> :"-inf"
      r >= digest.count -> :inf
      true -> do_quantile(digest, (r + 0.5) / digest.count)
    end
  end

  @doc """
  Returns the estimated value at a given reverse-rank position (0-based).

  Rank 0 is the largest observation.

  ## Parameters

    - `digest` - the t-digest struct
    - `r` - the reverse rank (0-based)

  ## Returns

    - The estimated value at reverse rank `r`
    - `:inf` for r < 0
    - `:"-inf"` for r >= count
    - `:nan` for empty digest
  """
  @spec by_rev_rank(t(), integer()) :: float() | :inf | :"-inf" | :nan
  def by_rev_rank(%__MODULE__{} = digest, r) when is_integer(r) do
    digest = ensure_compressed(digest)

    cond do
      digest.count == 0 -> :nan
      r < 0 -> :inf
      r >= digest.count -> :"-inf"
      true -> do_quantile(digest, 1.0 - (r + 0.5) / digest.count)
    end
  end

  @doc """
  Computes the trimmed mean between two quantile boundaries.

  Returns the mean of observations between `lo` and `hi` quantiles,
  weighted by the fraction of each centroid that falls within the range.

  ## Parameters

    - `digest` - the t-digest struct
    - `lo` - lower quantile boundary in [0.0, 1.0)
    - `hi` - upper quantile boundary in (0.0, 1.0]

  ## Returns

    - The trimmed mean as a float
    - `:nan` if the digest is empty
  """
  @spec trimmed_mean(t(), float(), float()) :: float() | :nan
  def trimmed_mean(%__MODULE__{} = digest, lo, hi)
      when is_float(lo) and is_float(hi) and lo >= 0.0 and hi <= 1.0 and lo < hi do
    digest = ensure_compressed(digest)

    if digest.count == 0 do
      :nan
    else
      do_trimmed_mean(digest, lo, hi)
    end
  end

  @doc """
  Merges another t-digest into this one.

  The resulting digest contains all observations from both digests.

  ## Parameters

    - `a` - the destination t-digest
    - `b` - the source t-digest to merge in

  ## Returns

  A new t-digest containing all observations from both.
  """
  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = a, %__MODULE__{} = b) do
    # Ensure both are compressed first
    a = ensure_compressed(a)
    b = ensure_compressed(b)

    all_centroids = a.centroids ++ b.centroids
    total_weight = a.count + b.count

    sorted = Enum.sort_by(all_centroids, fn {mean, _} -> mean end)
    merged = merge_centroids(sorted, total_weight / 1, a.compression)

    new_min =
      case {a.min, b.min} do
        {nil, nil} -> nil
        {nil, bm} -> bm
        {am, nil} -> am
        {am, bm} -> min(am, bm)
      end

    new_max =
      case {a.max, b.max} do
        {nil, nil} -> nil
        {nil, bm} -> bm
        {am, nil} -> am
        {am, bm} -> max(am, bm)
      end

    %__MODULE__{
      compression: a.compression,
      centroids: merged,
      count: total_weight,
      min: new_min,
      max: new_max,
      buffer: [],
      buffer_size: 0,
      total_compressions: a.total_compressions + b.total_compressions + 1
    }
  end

  @doc """
  Merges multiple t-digests into a single digest with the given compression.

  ## Parameters

    - `digests` - list of t-digest structs to merge
    - `compression` - compression parameter for the result

  ## Returns

  A new merged t-digest.
  """
  @spec merge_many([t()], pos_integer()) :: t()
  def merge_many([], compression), do: new(compression)

  def merge_many(digests, compression) do
    validate_compression!(compression)

    # Compress all digests first
    digests = Enum.map(digests, &ensure_compressed/1)

    all_centroids =
      Enum.flat_map(digests, fn d -> d.centroids end)

    total_weight =
      Enum.reduce(digests, 0, fn d, acc -> acc + d.count end)

    sorted = Enum.sort_by(all_centroids, fn {mean, _} -> mean end)
    merged = merge_centroids(sorted, total_weight / 1, compression)

    mins = Enum.map(digests, & &1.min) |> Enum.reject(&is_nil/1)
    maxs = Enum.map(digests, & &1.max) |> Enum.reject(&is_nil/1)

    new_min = if mins == [], do: nil, else: Enum.min(mins)
    new_max = if maxs == [], do: nil, else: Enum.max(maxs)

    total_comps = Enum.reduce(digests, 0, fn d, acc -> acc + d.total_compressions end)

    %__MODULE__{
      compression: compression,
      centroids: merged,
      count: total_weight,
      min: new_min,
      max: new_max,
      buffer: [],
      buffer_size: 0,
      total_compressions: total_comps + 1
    }
  end

  @doc """
  Resets the digest to empty, preserving its compression setting.

  ## Returns

  A fresh empty t-digest with the same compression parameter.
  """
  @spec reset(t()) :: t()
  def reset(%__MODULE__{compression: c}), do: new(c)

  @doc """
  Returns metadata about the digest as a map.

  ## Returns

  A map with keys: `:compression`, `:capacity`, `:merged_nodes`,
  `:unmerged_nodes`, `:merged_weight`, `:unmerged_weight`,
  `:total_compressions`, `:memory_usage`.
  """
  @spec info(t()) :: map()
  def info(%__MODULE__{} = digest) do
    merged_weight =
      Enum.reduce(digest.centroids, 0.0, fn {_m, w}, acc -> acc + w end)

    # Estimate memory: each centroid is {float, float} = ~48 bytes in Erlang,
    # each buffer element is a float = ~24 bytes, plus struct overhead
    centroid_mem = length(digest.centroids) * 48
    buffer_mem = digest.buffer_size * 24
    struct_overhead = 200

    %{
      compression: digest.compression,
      capacity: ceil(digest.compression * 3),
      merged_nodes: length(digest.centroids),
      unmerged_nodes: digest.buffer_size,
      merged_weight: merged_weight,
      unmerged_weight: digest.buffer_size * 1.0,
      total_compressions: digest.total_compressions,
      memory_usage: centroid_mem + buffer_mem + struct_overhead
    }
  end

  # ===========================================================================
  # Private: buffer management
  # ===========================================================================

  defp buffer_capacity(%__MODULE__{compression: c}), do: ceil(c * 3)

  defp buffer_value(%__MODULE__{} = digest, value) do
    %__MODULE__{
      digest
      | buffer: [value | digest.buffer],
        buffer_size: digest.buffer_size + 1,
        count: digest.count + 1
    }
  end

  defp update_min_max(%__MODULE__{min: nil, max: nil} = digest, value) do
    %__MODULE__{digest | min: value, max: value}
  end

  defp update_min_max(%__MODULE__{} = digest, value) do
    %__MODULE__{
      digest
      | min: min(digest.min, value),
        max: max(digest.max, value)
    }
  end

  defp maybe_compress(%__MODULE__{buffer_size: bs} = digest) do
    if bs >= buffer_capacity(digest) do
      compress(digest)
    else
      digest
    end
  end

  defp ensure_compressed(%__MODULE__{buffer: [], buffer_size: 0} = digest), do: digest
  defp ensure_compressed(%__MODULE__{} = digest), do: compress(digest)

  # ===========================================================================
  # Private: merging digest compression
  # ===========================================================================

  # The merging digest algorithm (Algorithm 3 from Dunning's paper):
  # Walk the sorted centroids, greedily merging adjacent entries as long as
  # the resulting weight does not violate the scale function constraint.
  defp merge_centroids([], _total_weight, _compression), do: []

  defp merge_centroids(sorted, total_weight, compression) when total_weight > 0 do
    [{first_mean, first_weight} | rest] = sorted

    {result, current_mean, current_weight, _weight_before} =
      Enum.reduce(rest, {[], first_mean, first_weight, 0.0}, fn
        {mean, weight}, {acc, c_mean, c_weight, weight_before} ->
          # Quantile position of the proposed merged centroid
          proposed_weight = c_weight + weight
          q = (weight_before + proposed_weight / 2.0) / total_weight

          max_w = max_weight(q, total_weight, compression)

          if proposed_weight <= max_w do
            # Merge into current centroid
            new_mean = (c_mean * c_weight + mean * weight) / proposed_weight
            {acc, new_mean, proposed_weight, weight_before}
          else
            # Start a new centroid
            {[{c_mean, c_weight} | acc], mean, weight, weight_before + c_weight}
          end
      end)

    [{current_mean, current_weight} | result]
    |> Enum.reverse()
  end

  defp merge_centroids(_sorted, _total_weight, _compression), do: []

  # Maximum weight a centroid at quantile position q can have.
  # Uses the k1 scale function derivative:
  #   k'(q) = delta / (2 * pi * sqrt(q * (1-q)))
  #   max_weight = 4 * N * q * (1-q) / delta
  # (simplified from: 4 * N / (delta * k'(q)) )
  defp max_weight(q, total_weight, compression) do
    # Clamp q away from exact 0 and 1 to avoid division issues
    q = max(1.0e-10, min(q, 1.0 - 1.0e-10))
    4.0 * total_weight * q * (1.0 - q) / compression
  end

  # ===========================================================================
  # Private: quantile estimation
  # ===========================================================================

  defp do_quantile(%__MODULE__{centroids: [], min: min_val, max: max_val, count: count}, q)
       when count > 0 do
    # No centroids but count > 0 means everything was in the buffer and got
    # compressed to zero centroids (shouldn't happen, but handle defensively)
    if min_val != nil and max_val != nil do
      min_val + q * (max_val - min_val)
    else
      :nan
    end
  end

  defp do_quantile(%__MODULE__{centroids: centroids, min: min_val, max: max_val, count: count}, q) do
    total_weight = count / 1

    # Special cases for extreme quantiles
    if q == 0.0 do
      min_val
    else
      if q == 1.0 do
        max_val
      else
        target = q * total_weight

        # Walk centroids accumulating weight
        {result, _} =
          Enum.reduce_while(centroids, {nil, 0.0}, fn {mean, weight},
                                                      {_prev_result, cum_weight} ->
            new_cum = cum_weight + weight

            if new_cum >= target do
              # This centroid spans the target
              if cum_weight == 0.0 do
                # First centroid: interpolate between min and centroid mean
                if target < weight / 2.0 do
                  # Target is in the left half of the first centroid
                  frac = target / (weight / 2.0)
                  val = min_val + frac * (mean - min_val)
                  {:halt, {val, new_cum}}
                else
                  {:halt, {mean, new_cum}}
                end
              else
                # Interpolate within this centroid
                {:halt, {mean, new_cum}}
              end
            else
              {:cont, {mean, new_cum}}
            end
          end)

        case result do
          nil -> max_val
          val -> clamp(val, min_val, max_val)
        end
      end
    end
  end

  # ===========================================================================
  # Private: CDF estimation
  # ===========================================================================

  defp do_cdf(%__MODULE__{centroids: [], min: min_val, max: max_val}, value) do
    cond do
      value <= min_val -> 0.0
      value >= max_val -> 1.0
      max_val == min_val -> 0.5
      true -> (value - min_val) / (max_val - min_val)
    end
  end

  defp do_cdf(%__MODULE__{centroids: centroids, min: min_val, max: max_val, count: count}, value) do
    total_weight = count / 1

    cond do
      value <= min_val ->
        0.0

      value >= max_val ->
        1.0

      true ->
        # Walk centroids, accumulating weight
        {cum_weight, _} =
          Enum.reduce(centroids, {0.0, nil}, fn {mean, weight}, {cum, prev} ->
            cond do
              value < mean and prev == nil ->
                # Before the first centroid: interpolate between min and first centroid
                frac = (value - min_val) / (mean - min_val)
                partial = frac * weight / 2.0
                throw({:result, partial / total_weight})

              value < mean ->
                # Between prev and this centroid: interpolate
                {prev_mean, prev_weight} = prev
                frac = (value - prev_mean) / (mean - prev_mean)
                partial = cum - prev_weight / 2.0 + frac * (prev_weight / 2.0 + weight / 2.0)
                throw({:result, partial / total_weight})

              value == mean ->
                # Exactly at this centroid's mean
                partial = cum + weight / 2.0
                throw({:result, partial / total_weight})

              true ->
                {cum + weight, {mean, weight}}
            end
          end)

        # Value is beyond the last centroid but within max
        {last_mean, last_weight} =
          case List.last(centroids) do
            nil -> {max_val, 0.0}
            c -> c
          end

        if max_val == last_mean do
          1.0
        else
          frac = (value - last_mean) / (max_val - last_mean)
          partial = cum_weight - last_weight / 2.0 + frac * (last_weight / 2.0)
          min(1.0, partial / total_weight)
        end
    end
  catch
    {:result, r} -> max(0.0, min(1.0, r))
  end

  # ===========================================================================
  # Private: trimmed mean
  # ===========================================================================

  defp do_trimmed_mean(%__MODULE__{} = digest, lo, hi) do
    total_weight = digest.count / 1
    lo_weight = lo * total_weight
    hi_weight = hi * total_weight

    {sum, weight_sum, _cum} =
      Enum.reduce(digest.centroids, {0.0, 0.0, 0.0}, fn {mean, weight}, {s, ws, cum} ->
        centroid_lo = cum
        centroid_hi = cum + weight

        # Fraction of this centroid that falls within [lo_weight, hi_weight]
        overlap_lo = max(centroid_lo, lo_weight)
        overlap_hi = min(centroid_hi, hi_weight)

        if overlap_hi > overlap_lo do
          overlap_weight = overlap_hi - overlap_lo
          {s + mean * overlap_weight, ws + overlap_weight, cum + weight}
        else
          {s, ws, cum + weight}
        end
      end)

    if weight_sum > 0.0 do
      sum / weight_sum
    else
      :nan
    end
  end

  # ===========================================================================
  # Private: utility
  # ===========================================================================

  defp clamp(value, min_val, max_val) when is_number(value) do
    value
    |> max(min_val)
    |> min(max_val)
  end

  defp clamp(value, _min_val, _max_val), do: value

  defp validate_compression!(compression)
       when is_integer(compression) and compression > 0 and compression <= @max_compression,
       do: :ok

  defp validate_compression!(compression) do
    raise ArgumentError,
          "compression must be an integer between 1 and #{@max_compression}, got: #{inspect(compression)}"
  end
end
