defmodule FerricStore.API.Probabilistic do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TDigest, TopK}
  alias Ferricstore.Store.Router

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Creates a Bloom filter with specific error rate and capacity.

  ## Examples

      :ok = FerricStore.bf_reserve("filter", 0.01, 1000)

  """
  @spec bf_reserve(key(), float(), pos_integer()) :: :ok | {:error, binary()}
  def bf_reserve(key, error_rate, capacity) do
    store = build_prob_store(key)
    Bloom.handle_ast({:bf_reserve, key, error_rate * 1.0, capacity}, store)
  end

  @doc """
  Adds an element to the Bloom filter at `key`, auto-creating if needed.

  ## Returns

    * `{:ok, 1}` if the element was added.
    * `{:ok, 0}` if the element was already present.

  ## Examples

      {:ok, 1} = FerricStore.bf_add("filter", "hello")

  """
  @spec bf_add(key(), binary()) :: {:ok, 0 | 1}
  def bf_add(key, element) do
    store = build_prob_store(key)
    result = Bloom.handle_ast({:bf_add, [key, element]}, store)
    wrap_result(result)
  end

  @doc """
  Adds multiple elements to the Bloom filter at `key`.

  ## Returns

    * `{:ok, [0 | 1, ...]}` for each element.

  """
  @spec bf_madd(key(), [binary()]) :: {:ok, [0 | 1]}
  def bf_madd(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = Bloom.handle_ast({:bf_madd, [key | elements]}, store)
    wrap_result(result)
  end

  @doc """
  Checks if an element may exist in the Bloom filter at `key`.

  ## Returns

    * `{:ok, 1}` if the element may exist.
    * `{:ok, 0}` if the element definitely does not exist.

  """
  @spec bf_exists(key(), binary()) :: {:ok, 0 | 1}
  def bf_exists(key, element) do
    store = build_prob_store(key)
    result = Bloom.handle_ast({:bf_exists, [key, element]}, store)
    wrap_result(result)
  end

  @doc """
  Checks if multiple elements may exist in the Bloom filter at `key`.

  ## Returns

    * `{:ok, [0 | 1, ...]}` for each element.

  """
  @spec bf_mexists(key(), [binary()]) :: {:ok, [0 | 1]}
  def bf_mexists(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = Bloom.handle_ast({:bf_mexists, [key | elements]}, store)
    wrap_result(result)
  end

  @doc """
  Returns the approximate number of unique elements added to the Bloom filter at `key`.

  ## Returns

    * `{:ok, count}` on success.

  ## Examples

      iex> FerricStore.bf_card("emails:seen")
      {:ok, 42}

  """
  @spec bf_card(key()) :: {:ok, non_neg_integer()}
  def bf_card(key) do
    store = build_prob_store(key)
    result = Bloom.handle_ast({:bf_card, [key]}, store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Bloom filter at `key` (capacity, error rate, size, etc.).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of filter properties.
    * `{:error, reason}` if the filter does not exist.

  ## Examples

      iex> FerricStore.bf_info("emails:seen")
      {:ok, ["Capacity", 100000, "Size", 120048, "Number of filters", 1, "Number of items inserted", 42, "Expansion rate", 2]}

  """
  @spec bf_info(key()) :: {:ok, list()} | {:error, binary()}
  def bf_info(key) do
    store = build_prob_store(key)
    result = Bloom.handle_ast({:bf_info, [key]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Cuckoo Filter operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Cuckoo filter with the specified capacity.

  A Cuckoo filter is similar to a Bloom filter but supports deletion and counting.
  Use it when you need probabilistic membership checks with the ability to remove
  items later (e.g., tracking active sessions that can be revoked).

  ## Parameters

    * `key` - the Cuckoo filter key
    * `capacity` - expected number of elements

  ## Examples

      iex> FerricStore.cf_reserve("sessions:active", 50_000)
      :ok

  """
  @spec cf_reserve(key(), pos_integer()) :: :ok | {:error, binary()}
  def cf_reserve(key, capacity) do
    store = build_prob_store(key)
    Cuckoo.handle_ast({:cf_reserve, key, capacity}, store)
  end

  @doc """
  Adds an element to the Cuckoo filter at `key`, auto-creating if needed.

  Unlike Bloom filters, duplicate insertions increase the count for the element.

  ## Returns

    * `{:ok, 1}` on success.
    * `{:error, reason}` if the filter is full and cannot accommodate the element.

  ## Examples

      iex> FerricStore.cf_add("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_add(key(), binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def cf_add(key, element) do
    store = build_prob_store(key)
    result = Cuckoo.handle_ast({:cf_add, [key, element]}, store)
    wrap_result(result)
  end

  @doc """
  Adds an element to the Cuckoo filter only if it is not already present.

  ## Returns

    * `{:ok, 1}` if the element was newly added.
    * `{:ok, 0}` if the element already exists.
    * `{:error, reason}` if the filter is full.

  ## Examples

      iex> FerricStore.cf_addnx("sessions:active", "sess_abc123")
      {:ok, 1}

      iex> FerricStore.cf_addnx("sessions:active", "sess_abc123")
      {:ok, 0}

  """
  @spec cf_addnx(key(), binary()) :: {:ok, 0 | 1} | {:error, binary()}
  def cf_addnx(key, element) do
    store = build_prob_store(key)
    result = Cuckoo.handle_ast({:cf_addnx, [key, element]}, store)
    wrap_result(result)
  end

  @doc """
  Deletes one occurrence of an element from the Cuckoo filter at `key`.

  This is the key advantage of Cuckoo filters over Bloom filters -- elements
  can be removed. Only deletes one occurrence if the element was added multiple times.

  ## Returns

    * `{:ok, 1}` if the element was deleted.
    * `{:ok, 0}` if the element was not found.

  ## Examples

      iex> FerricStore.cf_del("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_del(key(), binary()) :: {:ok, 0 | 1}
  def cf_del(key, element) do
    store = build_prob_store(key)
    result = Cuckoo.handle_ast({:cf_del, [key, element]}, store)
    wrap_result(result)
  end

  @doc """
  Checks if an element may exist in the Cuckoo filter at `key`.

  ## Returns

    * `{:ok, 1}` if the element probably exists.
    * `{:ok, 0}` if the element definitely does not exist.

  ## Examples

      iex> FerricStore.cf_exists("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_exists(key(), binary()) :: {:ok, 0 | 1}
  def cf_exists(key, element) do
    store = build_prob_store(key)
    result = Cuckoo.handle_ast({:cf_exists, [key, element]}, store)
    wrap_result(result)
  end

  @doc """
  Checks multiple elements against the Cuckoo filter at `key` in a single call.

  ## Returns

    * `{:ok, [0 | 1, ...]}` - `1` for probably present, `0` for definitely absent,
      one per element.

  ## Examples

      iex> FerricStore.cf_mexists("sessions:active", ["sess_abc123", "sess_unknown"])
      {:ok, [1, 0]}

  """
  @spec cf_mexists(key(), [binary()]) :: {:ok, [0 | 1]}
  def cf_mexists(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = Cuckoo.handle_ast({:cf_mexists, [key | elements]}, store)
    wrap_result(result)
  end

  @doc """
  Returns the approximate number of times an element was added to the Cuckoo filter.

  ## Returns

    * `{:ok, count}` - estimated insertion count for the element.

  ## Examples

      iex> FerricStore.cf_count("sessions:active", "sess_abc123")
      {:ok, 1}

  """
  @spec cf_count(key(), binary()) :: {:ok, non_neg_integer()}
  def cf_count(key, element) do
    store = build_prob_store(key)
    result = Cuckoo.handle_ast({:cf_count, [key, element]}, store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Cuckoo filter at `key` (size, bucket count, etc.).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of filter properties.
    * `{:error, reason}` if the filter does not exist.

  ## Examples

      iex> FerricStore.cf_info("sessions:active")
      {:ok, ["Size", 1024, "Number of buckets", 512, "Number of filters", 1, "Number of items inserted", 3, "Number of items deleted", 0, "Bucket size", 2, "Expansion rate", 1, "Max iterations", 20]}

  """
  @spec cf_info(key()) :: {:ok, list()} | {:error, binary()}
  def cf_info(key) do
    store = build_prob_store(key)
    result = Cuckoo.handle_ast({:cf_info, [key]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Count-Min Sketch operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Count-Min Sketch with the given `width` and `depth` dimensions.

  A Count-Min Sketch is a probabilistic structure for approximate frequency counting.
  It uses sub-linear space and answers "how many times has X been seen?" with bounded
  over-estimation. Ideal for view counts, click tracking, and frequency analysis
  where exact counts are not required.

  ## Parameters

    * `key` - the CMS key
    * `width` - number of counters per hash function (larger = more accurate)
    * `depth` - number of hash functions (larger = lower error probability)

  ## Examples

      iex> FerricStore.cms_initbydim("page:views", 2000, 5)
      :ok

  """
  @spec cms_initbydim(key(), pos_integer(), pos_integer()) :: :ok | {:error, binary()}
  def cms_initbydim(key, width, depth) do
    store = build_prob_store(key)
    CMS.handle_ast({:cms_initbydim, key, width, depth}, store)
  end

  @doc """
  Creates a Count-Min Sketch with a target error rate and over-estimation probability.

  The sketch dimensions (width/depth) are computed automatically from the error bounds.

  ## Parameters

    * `key` - the CMS key
    * `error` - acceptable error rate as a fraction (e.g. `0.001` for 0.1%)
    * `probability` - probability of exceeding the error rate (e.g. `0.01` for 1%)

  ## Examples

      iex> FerricStore.cms_initbyprob("click:tracking", 0.001, 0.01)
      :ok

  """
  @spec cms_initbyprob(key(), float(), float()) :: :ok | {:error, binary()}
  def cms_initbyprob(key, error, probability) do
    store = build_prob_store(key)
    CMS.handle_ast({:cms_initbyprob, key, error * 1.0, probability * 1.0}, store)
  end

  @doc """
  Increments the count for one or more elements in the Count-Min Sketch.

  ## Parameters

    * `key` - the CMS key
    * `pairs` - list of `{element, increment}` tuples

  ## Returns

    * `{:ok, [new_count, ...]}` - estimated count after increment, one per element.
    * `{:error, reason}` if the sketch does not exist.

  ## Examples

      iex> FerricStore.cms_incrby("page:views", [{"homepage", 1}, {"about", 3}])
      {:ok, [1, 3]}

  """
  @spec cms_incrby(key(), [{binary(), pos_integer()}]) ::
          {:ok, [non_neg_integer()]} | {:error, binary()}
  def cms_incrby(key, pairs) when is_list(pairs) do
    store = build_prob_store(key)
    result = CMS.handle_ast({:cms_incrby, key, pairs}, store)
    wrap_result(result)
  end

  @doc """
  Queries the estimated frequency count for one or more elements in the Count-Min Sketch.

  Counts may be over-estimated but never under-estimated.

  ## Returns

    * `{:ok, [count, ...]}` - estimated count per element.
    * `{:error, reason}` if the sketch does not exist.

  ## Examples

      iex> FerricStore.cms_query("page:views", ["homepage", "about", "unknown"])
      {:ok, [42, 7, 0]}

  """
  @spec cms_query(key(), [binary()]) :: {:ok, [non_neg_integer()]} | {:error, binary()}
  def cms_query(key, elements) when is_list(elements) do
    store = build_prob_store(key)
    result = CMS.handle_ast({:cms_query, [key | elements]}, store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Count-Min Sketch at `key` (width, depth, total count).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of sketch properties.
    * `{:error, reason}` if the sketch does not exist.

  ## Examples

      iex> FerricStore.cms_info("page:views")
      {:ok, ["width", 2000, "depth", 5, "count", 49]}

  """
  @spec cms_info(key()) :: {:ok, list()} | {:error, binary()}
  def cms_info(key) do
    store = build_prob_store(key)
    result = CMS.handle_ast({:cms_info, [key]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # TopK operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a Top-K tracker that maintains the `k` most frequent elements.

  A Top-K structure uses the Heavy Keeper algorithm to efficiently track the
  most popular items in a data stream with bounded memory. Ideal for trending
  topics, popular search queries, and hot product tracking.

  ## Parameters

    * `key` - the Top-K tracker key
    * `k` - number of top elements to track

  ## Examples

      iex> FerricStore.topk_reserve("trending:searches", 10)
      :ok

  """
  @spec topk_reserve(key(), pos_integer()) :: :ok | {:error, binary()}
  def topk_reserve(key, k) do
    store = build_topk_store(key)
    TopK.handle_ast({:topk_reserve, key, k, 8, 7, 0.9}, store)
  end

  @doc """
  Adds one or more elements to the Top-K tracker, updating frequency counts.

  If an element displaces another from the top-k, the displaced element is returned.

  ## Returns

    * `{:ok, [displaced | nil, ...]}` - `nil` if no element was displaced,
      or the name of the displaced element, one per input.

  ## Examples

      iex> FerricStore.topk_add("trending:searches", ["elixir", "rust", "golang"])
      {:ok, [nil, nil, nil]}

  """
  @spec topk_add(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def topk_add(key, elements) when is_list(elements) do
    store = build_topk_store(key)
    result = TopK.handle_ast({:topk_add, [key | elements]}, store)
    wrap_result(result)
  end

  @doc """
  Checks whether elements are currently in the Top-K set.

  ## Returns

    * `{:ok, [0 | 1, ...]}` - `1` if the element is in the top-k, `0` otherwise.

  ## Examples

      iex> FerricStore.topk_query("trending:searches", ["elixir", "obscure-lang"])
      {:ok, [1, 0]}

  """
  @spec topk_query(key(), [binary()]) :: {:ok, list()} | {:error, binary()}
  def topk_query(key, elements) when is_list(elements) do
    store = build_topk_store(key)
    result = TopK.handle_ast({:topk_query, [key | elements]}, store)
    wrap_result(result)
  end

  @doc """
  Returns the current Top-K elements, ordered by estimated frequency (descending).

  ## Returns

    * `{:ok, [element, ...]}` - the top-k element names.
    * `{:error, reason}` if the tracker does not exist.

  ## Examples

      iex> FerricStore.topk_list("trending:searches")
      {:ok, ["elixir", "rust", "golang"]}

  """
  @spec topk_list(key()) :: {:ok, [binary()]} | {:error, binary()}
  def topk_list(key) do
    store = build_topk_store(key)
    result = TopK.handle_ast({:topk_list, key, false}, store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the Top-K tracker at `key` (k, width, depth, decay).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of tracker properties.
    * `{:error, reason}` if the tracker does not exist.

  ## Examples

      iex> FerricStore.topk_info("trending:searches")
      {:ok, ["k", 10, "width", 8, "depth", 7, "decay", "0.9"]}

  """
  @spec topk_info(key()) :: {:ok, list()} | {:error, binary()}
  def topk_info(key) do
    store = build_topk_store(key)
    result = TopK.handle_ast({:topk_info, [key]}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # T-Digest operations
  # ---------------------------------------------------------------------------

  @doc """
  Creates a T-Digest structure at `key` for estimating quantiles and percentiles.

  A T-Digest compactly summarizes a distribution of numeric values, enabling
  accurate estimation of percentiles (p50, p95, p99) with bounded memory.
  Ideal for latency monitoring, response time analysis, and SLA tracking.

  ## Examples

      iex> FerricStore.tdigest_create("api:latency:ms")
      :ok

  """
  @spec tdigest_create(key()) :: :ok | {:error, binary()}
  def tdigest_create(key) do
    ctx = default_ctx()

    Router.with_key_latch(ctx, key, fn ->
      TDigest.handle_ast({:tdigest_create, key, nil}, build_tdigest_store(ctx))
    end)
  end

  @doc """
  Adds one or more numeric observations to the T-Digest at `key`.

  ## Parameters

    * `key` - the T-Digest key
    * `values` - list of numeric values to add

  ## Examples

      iex> FerricStore.tdigest_add("api:latency:ms", [12.5, 45.0, 3.2, 89.1, 150.0])
      :ok

  """
  @spec tdigest_add(key(), [number()]) :: :ok | {:error, binary()}
  def tdigest_add(key, values) when is_list(values) do
    ctx = default_ctx()

    Router.with_key_latch(ctx, key, fn ->
      TDigest.handle_ast(
        {:tdigest_add, key, Enum.map(values, &(&1 * 1.0))},
        build_tdigest_store(ctx)
      )
    end)
  end

  @doc """
  Estimates the values at the given quantile points (0.0 to 1.0).

  For example, quantile `0.5` is the median, `0.95` is the 95th percentile.

  ## Returns

    * `{:ok, [value, ...]}` - estimated value at each quantile.
    * `{:error, reason}` if the digest does not exist.

  ## Examples

      iex> FerricStore.tdigest_quantile("api:latency:ms", [0.5, 0.95, 0.99])
      {:ok, ["45.0", "150.0", "150.0"]}

  """
  @spec tdigest_quantile(key(), [float()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_quantile(key, quantiles) when is_list(quantiles) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_quantile, key, Enum.map(quantiles, &(&1 * 1.0))}, store)
    wrap_result(result)
  end

  @doc """
  Estimates the cumulative distribution function (CDF) at the given values.

  Returns the fraction of observations less than or equal to each value.
  For example, a CDF of `0.95` at value `100` means 95% of observations
  were <= 100.

  ## Returns

    * `{:ok, [fraction, ...]}` - CDF value (0.0 to 1.0) at each input.

  ## Examples

      iex> FerricStore.tdigest_cdf("api:latency:ms", [50.0, 100.0])
      {:ok, ["0.6", "0.8"]}

  """
  @spec tdigest_cdf(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_cdf(key, values) when is_list(values) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_cdf, key, Enum.map(values, &(&1 * 1.0))}, store)
    wrap_result(result)
  end

  @doc """
  Returns the minimum value observed in the T-Digest at `key`.

  ## Examples

      iex> FerricStore.tdigest_min("api:latency:ms")
      {:ok, "3.2"}

  """
  @spec tdigest_min(key()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_min(key) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_min, [key]}, store)
    wrap_result(result)
  end

  @doc """
  Returns the maximum value observed in the T-Digest at `key`.

  ## Examples

      iex> FerricStore.tdigest_max("api:latency:ms")
      {:ok, "150.0"}

  """
  @spec tdigest_max(key()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_max(key) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_max, [key]}, store)
    wrap_result(result)
  end

  @doc """
  Returns metadata about the T-Digest at `key` (compression, total observations, etc.).

  ## Returns

    * `{:ok, info_list}` - flat key-value list of digest properties.
    * `{:error, reason}` if the digest does not exist.

  ## Examples

      iex> FerricStore.tdigest_info("api:latency:ms")
      {:ok, ["Compression", 100, "Capacity", 610, "Merged nodes", 5, "Unmerged nodes", 0, "Merged weight", "5.0", "Unmerged weight", "0.0", "Total compressions", 1]}

  """
  @spec tdigest_info(key()) :: {:ok, list()} | {:error, binary()}
  def tdigest_info(key) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_info, [key]}, store)
    wrap_result(result)
  end

  @doc """
  Resets the T-Digest at `key`, discarding all observations.

  ## Examples

      iex> FerricStore.tdigest_reset("api:latency:ms")
      :ok

  """
  @spec tdigest_reset(key()) :: :ok | {:error, binary()}
  def tdigest_reset(key) do
    ctx = default_ctx()

    Router.with_key_latch(ctx, key, fn ->
      TDigest.handle_ast({:tdigest_reset, [key]}, build_tdigest_store(ctx))
    end)
  end

  @doc """
  Computes the trimmed mean of values between quantile bounds `lo` and `hi`.

  A trimmed mean excludes outliers by only averaging values within the specified
  quantile range. For example, `tdigest_trimmed_mean(key, 0.1, 0.9)` averages
  the middle 80% of the distribution.

  ## Parameters

    * `key` - the T-Digest key
    * `lo` - lower quantile bound (0.0 to 1.0)
    * `hi` - upper quantile bound (0.0 to 1.0)

  ## Examples

      iex> FerricStore.tdigest_trimmed_mean("api:latency:ms", 0.1, 0.9)
      {:ok, "45.5"}

  """
  @spec tdigest_trimmed_mean(key(), float(), float()) :: {:ok, binary()} | {:error, binary()}
  def tdigest_trimmed_mean(key, lo, hi) do
    store = build_tdigest_store()

    result = TDigest.handle_ast({:tdigest_trimmed_mean, key, lo * 1.0, hi * 1.0}, store)

    wrap_result(result)
  end

  @doc """
  Estimates the rank (number of observations less than or equal to) for each value.

  ## Returns

    * `{:ok, [rank, ...]}` - estimated rank per value.

  ## Examples

      iex> FerricStore.tdigest_rank("api:latency:ms", [50.0, 100.0])
      {:ok, [3, 4]}

  """
  @spec tdigest_rank(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_rank(key, values) when is_list(values) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_rank, key, Enum.map(values, &(&1 * 1.0))}, store)
    wrap_result(result)
  end

  @doc """
  Estimates the reverse rank (number of observations greater than) for each value.

  ## Returns

    * `{:ok, [reverse_rank, ...]}` - estimated reverse rank per value.

  ## Examples

      iex> FerricStore.tdigest_revrank("api:latency:ms", [50.0, 100.0])
      {:ok, [2, 1]}

  """
  @spec tdigest_revrank(key(), [number()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_revrank(key, values) when is_list(values) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_revrank, key, Enum.map(values, &(&1 * 1.0))}, store)
    wrap_result(result)
  end

  @doc """
  Estimates the value at each given rank (0-based position in sorted order).

  ## Returns

    * `{:ok, [value, ...]}` - estimated value at each rank.

  ## Examples

      iex> FerricStore.tdigest_byrank("api:latency:ms", [0, 2, 4])
      {:ok, ["3.2", "45.0", "150.0"]}

  """
  @spec tdigest_byrank(key(), [integer()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_byrank(key, ranks) when is_list(ranks) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_byrank, key, ranks}, store)
    wrap_result(result)
  end

  @doc """
  Estimates the value at each given reverse rank (0 = largest, 1 = second largest, etc.).

  ## Returns

    * `{:ok, [value, ...]}` - estimated value at each reverse rank.

  ## Examples

      iex> FerricStore.tdigest_byrevrank("api:latency:ms", [0, 1])
      {:ok, ["150.0", "89.1"]}

  """
  @spec tdigest_byrevrank(key(), [integer()]) :: {:ok, list()} | {:error, binary()}
  def tdigest_byrevrank(key, ranks) when is_list(ranks) do
    store = build_tdigest_store()
    result = TDigest.handle_ast({:tdigest_byrevrank, key, ranks}, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Geo operations
  # ---------------------------------------------------------------------------
end
