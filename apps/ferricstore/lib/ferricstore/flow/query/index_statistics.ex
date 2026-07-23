defmodule Ferricstore.Flow.Query.IndexStatistics do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, TupleCodec}

  @max_prefix_counts 256
  @max_histograms 8
  @max_histogram_bins 64
  @max_counter_fields 32
  @max_age_ms 5 * 60 * 1_000
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @confidences [:none, :low, :medium, :high]
  @sample_keys [:histograms, :average_entry_bytes, :average_row_bytes]

  @derive {Inspect,
           only: [
             :index_id,
             :index_version,
             :collected_at_ms,
             :source_watermark,
             :total_entries,
             :distinct_runs,
             :sample_rate_ppm,
             :confidence
           ]}
  @enforce_keys [
    :index_id,
    :index_version,
    :scope_digest,
    :collected_at_ms,
    :source_watermark,
    :total_entries,
    :distinct_runs,
    :prefix_counts,
    :prefix_observed_at_ms,
    :histograms,
    :null_counts,
    :missing_counts,
    :sample_observed_at_ms,
    :average_entry_bytes,
    :average_row_bytes,
    :sample_rate_ppm,
    :confidence
  ]
  defstruct version: 1,
            index_id: nil,
            index_version: nil,
            scope_digest: nil,
            collected_at_ms: nil,
            source_watermark: nil,
            total_entries: nil,
            distinct_runs: nil,
            prefix_counts: %{},
            prefix_observed_at_ms: %{},
            histograms: %{},
            null_counts: %{},
            missing_counts: %{},
            sample_observed_at_ms: %{},
            average_entry_bytes: nil,
            average_row_bytes: nil,
            sample_rate_ppm: nil,
            confidence: nil

  @type histogram_bin :: %{
          required(:lower) => integer() | float(),
          required(:upper) => integer() | float(),
          required(:count) => non_neg_integer()
        }
  @type t :: %__MODULE__{}

  @doc false
  @spec max_prefix_counts() :: pos_integer()
  def max_prefix_counts, do: @max_prefix_counts

  @doc false
  @spec max_age_ms() :: pos_integer()
  def max_age_ms, do: @max_age_ms

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, :invalid_query_index_statistics}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(%{} = attrs) do
    attrs = normalize_sample_observations(attrs)
    stat = struct(__MODULE__, attrs)

    if valid?(stat),
      do: {:ok, stat},
      else: {:error, :invalid_query_index_statistics}
  rescue
    _error -> {:error, :invalid_query_index_statistics}
  end

  def new(_attrs), do: {:error, :invalid_query_index_statistics}

  @spec new!(map() | keyword()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, stat} -> stat
      {:error, reason} -> raise ArgumentError, "invalid query index statistics: #{reason}"
    end
  end

  @spec fresh?(t(), non_neg_integer()) :: boolean()
  def fresh?(%__MODULE__{collected_at_ms: collected_at_ms}, now_ms)
      when is_integer(now_ms) and now_ms >= 0 do
    collected_at_ms <= now_ms and now_ms - collected_at_ms <= @max_age_ms
  end

  def fresh?(%__MODULE__{}, _now_ms), do: false

  @spec prefix_count(t(), [term()], non_neg_integer()) ::
          {:ok, non_neg_integer()} | :unknown
  def prefix_count(
        %__MODULE__{prefix_counts: counts, prefix_observed_at_ms: observed},
        values,
        now_ms
      )
      when is_list(values) and is_integer(now_ms) and now_ms >= 0 do
    digest = prefix_digest(values)

    case {Map.fetch(counts, digest), Map.fetch(observed, digest)} do
      {{:ok, count}, {:ok, observed_at_ms}}
      when observed_at_ms <= now_ms and now_ms - observed_at_ms <= @max_age_ms ->
        {:ok, count}

      _missing_or_stale ->
        :unknown
    end
  end

  def prefix_count(%__MODULE__{}, _values, _now_ms), do: :unknown

  @spec sample_fresh?(t(), atom(), non_neg_integer()) :: boolean()
  def sample_fresh?(%__MODULE__{sample_observed_at_ms: observed}, sample, now_ms)
      when sample in @sample_keys and is_integer(now_ms) and now_ms >= 0 do
    case Map.fetch(observed, sample) do
      {:ok, observed_at_ms} ->
        observed_at_ms <= now_ms and now_ms - observed_at_ms <= @max_age_ms

      :error ->
        false
    end
  end

  def sample_fresh?(%__MODULE__{}, _sample, _now_ms), do: false

  @spec scope_digest(binary()) :: <<_::256>>
  def scope_digest(scope) when is_binary(scope),
    do: :crypto.hash(:sha256, ["ferric.flow.query.scope/v1", scope])

  @spec prefix_digest([term()]) :: <<_::256>>
  def prefix_digest(values) when is_list(values) do
    encoded =
      Enum.map(values, fn value ->
        component = TupleCodec.encode_component(value, :asc)
        <<byte_size(component)::unsigned-big-32, component::binary>>
      end)

    :crypto.hash(:sha256, ["ferric.flow.query.prefix/v1", encoded])
  end

  @spec histogram_fraction_ppm(t(), Field.t(), term(), term(), boolean()) ::
          non_neg_integer() | :unknown
  def histogram_fraction_ppm(
        %__MODULE__{} = stat,
        field,
        lower,
        upper,
        upper_inclusive?
      ) do
    histogram_fraction_ppm(
      stat,
      field,
      lower,
      upper,
      upper_inclusive?,
      stat.collected_at_ms
    )
  end

  @spec histogram_fraction_ppm(t(), Field.t(), term(), term(), boolean(), non_neg_integer()) ::
          non_neg_integer() | :unknown
  def histogram_fraction_ppm(
        %__MODULE__{histograms: histograms} = stat,
        field,
        lower,
        upper,
        upper_inclusive?,
        now_ms
      ) do
    if sample_fresh?(stat, :histograms, now_ms) do
      histogram_fraction(histograms, field, lower, upper, upper_inclusive?)
    else
      :unknown
    end
  end

  defp histogram_fraction(histograms, field, lower, upper, upper_inclusive?) do
    case Map.get(histograms, field) do
      [%{lower: first_lower} | _rest] = bins ->
        kind = numeric_kind(first_lower)

        if numeric_kind(lower) == kind and numeric_kind(upper) == kind do
          total = Enum.reduce(bins, 0, &(&1.count + &2))

          matching =
            Enum.reduce(bins, 0, fn bin, count ->
              if overlaps?(bin, lower, upper, upper_inclusive?),
                do: count + bin.count,
                else: count
            end)

          if total == 0,
            do: 0,
            else: min(1_000_000, div(matching * 1_000_000 + total - 1, total))
        else
          :unknown
        end

      _missing ->
        :unknown
    end
  end

  defp valid?(%__MODULE__{} = stat) do
    stat.version == 1 and valid_id?(stat.index_id) and positive_u64?(stat.index_version) and
      is_binary(stat.scope_digest) and byte_size(stat.scope_digest) == 32 and
      nonnegative_u64?(stat.collected_at_ms) and nonnegative_u64?(stat.source_watermark) and
      nonnegative_u64?(stat.total_entries) and nonnegative_u64?(stat.distinct_runs) and
      stat.distinct_runs <= stat.total_entries and
      valid_prefix_counts?(
        stat.prefix_counts,
        stat.prefix_observed_at_ms,
        stat.collected_at_ms,
        stat.total_entries
      ) and
      valid_histograms?(stat.histograms, stat.total_entries) and
      valid_field_counts?(stat.null_counts, stat.total_entries) and
      valid_field_counts?(stat.missing_counts, stat.total_entries) and
      valid_absence_counts?(stat.null_counts, stat.missing_counts, stat.total_entries) and
      valid_sample_observations?(stat.sample_observed_at_ms, stat.collected_at_ms) and
      positive_u64?(stat.average_entry_bytes) and
      positive_u64?(stat.average_row_bytes) and is_integer(stat.sample_rate_ppm) and
      stat.sample_rate_ppm in 1..1_000_000 and stat.confidence in @confidences
  end

  defp valid_id?(id), do: is_binary(id) and id != "" and byte_size(id) <= 64

  defp valid_prefix_counts?(counts, observed, collected_at_ms, total_entries)
       when is_map(counts) and is_map(observed) and map_size(counts) <= @max_prefix_counts and
              map_size(counts) == map_size(observed) do
    Enum.all?(counts, fn {digest, count} ->
      is_binary(digest) and byte_size(digest) == 32 and nonnegative_u64?(count) and
        count <= total_entries and
        case Map.fetch(observed, digest) do
          {:ok, observed_at_ms} ->
            nonnegative_u64?(observed_at_ms) and observed_at_ms <= collected_at_ms

          :error ->
            false
        end
    end)
  end

  defp valid_prefix_counts?(_counts, _observed, _collected_at_ms, _total_entries), do: false

  defp valid_histograms?(histograms, total_entries)
       when is_map(histograms) and map_size(histograms) <= @max_histograms do
    Enum.all?(histograms, fn {field, bins} ->
      Field.valid?(field) and is_list(bins) and bins != [] and
        length(bins) <= @max_histogram_bins and valid_bins?(field, bins, total_entries)
    end)
  end

  defp valid_histograms?(_histograms, _total_entries), do: false

  defp valid_bins?(field, [%{lower: lower} | _rest] = bins, total_entries) do
    kind = numeric_kind(lower)

    kind in [:integer, :float] and valid_histogram_kind?(field, kind) and
      valid_bins?(bins, nil, kind, 0, total_entries)
  end

  defp valid_bins?(_field, _bins, _total_entries), do: false

  defp valid_bins?([], _previous_upper, _kind, count, total_entries),
    do: count <= total_entries

  defp valid_bins?(
         [%{lower: lower, upper: upper, count: count} | rest],
         previous_upper,
         kind,
         total,
         total_entries
       )
       when is_integer(count) do
    next_total = total + count

    numeric_kind(lower) == kind and numeric_kind(upper) == kind and
      valid_histogram_value?(lower) and valid_histogram_value?(upper) and
      nonnegative_u64?(count) and next_total <= total_entries and lower <= upper and
      (is_nil(previous_upper) or previous_upper <= lower) and
      valid_bins?(rest, upper, kind, next_total, total_entries)
  end

  defp valid_bins?(_bins, _previous_upper, _kind, _total, _total_entries), do: false

  defp valid_field_counts?(counts, total_entries)
       when is_map(counts) and map_size(counts) <= @max_counter_fields do
    Enum.all?(counts, fn {field, count} ->
      Field.valid?(field) and nonnegative_u64?(count) and count <= total_entries
    end)
  end

  defp valid_field_counts?(_counts, _total_entries), do: false

  defp valid_absence_counts?(null_counts, missing_counts, total_entries) do
    null_counts
    |> Map.keys()
    |> Kernel.++(Map.keys(missing_counts))
    |> Enum.uniq()
    |> Enum.all?(fn field ->
      Map.get(null_counts, field, 0) + Map.get(missing_counts, field, 0) <= total_entries
    end)
  end

  defp valid_sample_observations?(observed, collected_at_ms)
       when is_map(observed) and map_size(observed) <= length(@sample_keys) do
    Enum.all?(observed, fn {sample, observed_at_ms} ->
      sample in @sample_keys and nonnegative_u64?(observed_at_ms) and
        observed_at_ms <= collected_at_ms
    end)
  end

  defp valid_sample_observations?(_observed, _collected_at_ms), do: false

  defp normalize_sample_observations(attrs) do
    if Map.has_key?(attrs, :sample_observed_at_ms) do
      attrs
    else
      observed_at_ms = Map.get(attrs, :collected_at_ms)
      observed = Map.new(@sample_keys, &{&1, observed_at_ms})
      Map.put(attrs, :sample_observed_at_ms, observed)
    end
  end

  defp valid_histogram_kind?(field, kind) do
    case Field.value_type(field) do
      :integer -> kind == :integer
      :keyword -> false
      :dynamic -> true
    end
  end

  defp valid_histogram_value?(value),
    do: match?({:ok, _encoded}, TupleCodec.encode_component_safe(value, :asc))

  defp numeric_kind(value) when is_integer(value), do: :integer
  defp numeric_kind(value) when is_float(value), do: :float
  defp numeric_kind(_value), do: :other

  defp overlaps?(bin, lower, upper, upper_inclusive?) do
    bin.upper >= lower and
      (bin.lower < upper or (upper_inclusive? and bin.lower == upper))
  end

  defp positive_u64?(value), do: is_integer(value) and value > 0 and value <= @max_u64
  defp nonnegative_u64?(value), do: is_integer(value) and value >= 0 and value <= @max_u64
end
