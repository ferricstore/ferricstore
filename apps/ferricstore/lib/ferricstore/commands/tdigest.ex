defmodule Ferricstore.Commands.TDigest do
  @moduledoc """
  Handles Redis-compatible TDIGEST.* commands.

  A t-digest is a probabilistic data structure for accurate on-line accumulation
  of rank-based statistics such as quantiles, trimmed means, and cumulative
  distribution values. It provides high accuracy at the tails (P99, P99.9) while
  using bounded memory.

  ## Storage format

  T-digests are stored as tagged tuples via the injected store map:

      {:tdigest, centroids_list, metadata}

  where `centroids_list` is a list of `{mean, weight}` tuples sorted by mean,
  and `metadata` is a map containing compression, count, min, max, buffer, and
  total_compressions.

  ## Supported commands

    * `TDIGEST.CREATE key [COMPRESSION compression]` -- create a new t-digest
    * `TDIGEST.ADD key value [value ...]` -- add observations
    * `TDIGEST.RESET key` -- clear data, preserve compression
    * `TDIGEST.QUANTILE key quantile [quantile ...]` -- estimate values at quantiles
    * `TDIGEST.CDF key value [value ...]` -- estimate CDF at values
    * `TDIGEST.RANK key value [value ...]` -- estimate rank of values
    * `TDIGEST.REVRANK key value [value ...]` -- estimate reverse rank
    * `TDIGEST.BYRANK key rank [rank ...]` -- estimate value at rank
    * `TDIGEST.BYREVRANK key rank [rank ...]` -- estimate value at reverse rank
    * `TDIGEST.TRIMMED_MEAN key low_quantile high_quantile` -- trimmed mean
    * `TDIGEST.MIN key` -- minimum observed value
    * `TDIGEST.MAX key` -- maximum observed value
    * `TDIGEST.INFO key` -- digest metadata
    * `TDIGEST.MERGE destkey numkeys src [src ...] [COMPRESSION c] [OVERRIDE]`
  """

  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry
  alias Ferricstore.TDigest.Core

  @wrongtype_msg "WRONGTYPE Operation against a key holding the wrong kind of value"

  @doc """
  Handles a TDIGEST command.

  ## Parameters

    - `cmd` -- uppercased command name (e.g. `"TDIGEST.CREATE"`)
    - `args` -- list of string arguments
    - `store` -- injected store map with `get`, `put`, `exists?` callbacks

  ## Returns

  Plain Elixir term: `:ok`, float, list, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # TDIGEST.CREATE key [COMPRESSION compression]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.CREATE", [key], store) do
    with :ok <- check_create_available(key, store) do
      create_digest(key, 100, store)
    end
  end

  def handle("TDIGEST.CREATE", [key, "COMPRESSION", comp_str], store) do
    with {:ok, compression} <- parse_pos_integer(comp_str, "compression"),
         :ok <- check_create_available(key, store) do
      create_digest(key, compression, store)
    end
  end

  def handle("TDIGEST.CREATE", [], _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.create' command"}
  end

  def handle("TDIGEST.CREATE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.create' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.ADD key value [value ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.ADD", [key | values], store) when values != [] do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, floats} <- parse_float_list(values) do
      updated = Core.add_many(digest, floats)
      persist!(key, updated, store)
      :ok
    end
  end

  def handle("TDIGEST.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.add' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.RESET key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.RESET", [key], store) do
    with {:ok, digest} <- get_digest(store, key) do
      updated = Core.reset(digest)
      persist!(key, updated, store)
      :ok
    end
  end

  def handle("TDIGEST.RESET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.reset' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.QUANTILE key quantile [quantile ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.QUANTILE", [key | qs], store) when qs != [] do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, quantiles} <- parse_quantile_list(qs) do
      results = Enum.map(quantiles, fn q -> Core.quantile(digest, q) end)
      format_float_results(results)
    end
  end

  def handle("TDIGEST.QUANTILE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.quantile' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.CDF key value [value ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.CDF", [key | values], store) when values != [] do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, floats} <- parse_float_list(values) do
      results = Enum.map(floats, fn v -> Core.cdf(digest, v) end)
      format_float_results(results)
    end
  end

  def handle("TDIGEST.CDF", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.cdf' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.RANK key value [value ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.RANK", [key | values], store) when values != [] do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, floats} <- parse_float_list(values) do
      Enum.map(floats, fn v -> Core.rank(digest, v) end)
    end
  end

  def handle("TDIGEST.RANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.rank' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.REVRANK key value [value ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.REVRANK", [key | values], store) when values != [] do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, floats} <- parse_float_list(values) do
      Enum.map(floats, fn v -> Core.rev_rank(digest, v) end)
    end
  end

  def handle("TDIGEST.REVRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.revrank' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.BYRANK key rank [rank ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.BYRANK", [key | ranks], store) when ranks != [] do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, rank_ints} <- parse_integer_list(ranks) do
      results = Enum.map(rank_ints, fn r -> Core.by_rank(digest, r) end)
      format_rank_results(results)
    end
  end

  def handle("TDIGEST.BYRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.byrank' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.BYREVRANK key rank [rank ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.BYREVRANK", [key | ranks], store) when ranks != [] do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, rank_ints} <- parse_integer_list(ranks) do
      results = Enum.map(rank_ints, fn r -> Core.by_rev_rank(digest, r) end)
      format_rank_results(results)
    end
  end

  def handle("TDIGEST.BYREVRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.byrevrank' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.TRIMMED_MEAN key low_quantile high_quantile
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.TRIMMED_MEAN", [key, lo_str, hi_str], store) do
    with {:ok, digest} <- get_digest(store, key),
         {:ok, lo} <- parse_quantile(lo_str),
         {:ok, hi} <- parse_quantile(hi_str) do
      if lo >= hi do
        {:error, "ERR TDIGEST: low_quantile must be less than high_quantile"}
      else
        result = Core.trimmed_mean(digest, lo, hi)
        format_single_float(result)
      end
    end
  end

  def handle("TDIGEST.TRIMMED_MEAN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.trimmed_mean' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.MIN key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.MIN", [key], store) do
    with {:ok, digest} <- get_digest(store, key) do
      case digest.min do
        nil -> "nan"
        val -> format_number(val)
      end
    end
  end

  def handle("TDIGEST.MIN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.min' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.MAX key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.MAX", [key], store) do
    with {:ok, digest} <- get_digest(store, key) do
      case digest.max do
        nil -> "nan"
        val -> format_number(val)
      end
    end
  end

  def handle("TDIGEST.MAX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.max' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.INFO key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.INFO", [key], store) do
    with {:ok, digest} <- get_digest(store, key) do
      info = Core.info(digest)

      [
        "Compression",
        info.compression,
        "Capacity",
        info.capacity,
        "Merged nodes",
        info.merged_nodes,
        "Unmerged nodes",
        info.unmerged_nodes,
        "Merged weight",
        format_number(info.merged_weight),
        "Unmerged weight",
        format_number(info.unmerged_weight),
        "Total compressions",
        info.total_compressions,
        "Memory usage",
        info.memory_usage
      ]
    end
  end

  def handle("TDIGEST.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.info' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.MERGE destkey numkeys src [src ...] [COMPRESSION c] [OVERRIDE]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.MERGE", [dest, numkeys_str | rest], store) do
    with {:ok, numkeys} <- parse_pos_integer(numkeys_str, "numkeys"),
         {:ok, src_keys, opts} <- parse_merge_args(rest, numkeys),
         {:ok, src_digests} <- load_source_digests(store, src_keys) do
      do_merge(store, dest, src_digests, opts)
    end
  end

  def handle("TDIGEST.MERGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.merge' command"}
  end

  # ===========================================================================
  # Private: store operations
  # ===========================================================================

  defp get_digest(store, key) do
    case Ops.get(store, key) do
      nil ->
        if key_held_by_other_registry?(key, store) do
          {:error, @wrongtype_msg}
        else
          {:error, "ERR TDIGEST: key does not exist"}
        end

      {:tdigest, centroids, metadata} ->
        {:ok, deserialize(centroids, metadata)}

      bin when is_binary(bin) ->
        try do
          case :erlang.binary_to_term(bin) do
            {:tdigest, centroids, metadata} ->
              {:ok, deserialize(centroids, metadata)}

            _ ->
              {:error, @wrongtype_msg}
          end
        rescue
          ArgumentError ->
            {:error, @wrongtype_msg}
        end

      _ ->
        {:error, @wrongtype_msg}
    end
  end

  defp check_create_available(key, store) do
    case Ops.get(store, key) do
      nil ->
        if key_held_by_other_registry?(key, store), do: {:error, @wrongtype_msg}, else: :ok

      raw ->
        case decode_raw_digest(raw) do
          {:ok, _digest} -> {:error, "ERR TDIGEST: key already exists"}
          {:error, _wrongtype} -> {:error, @wrongtype_msg}
        end
    end
  end

  defp create_digest(key, compression, store) do
    digest = Core.new(compression)
    persist!(key, digest, store)
    :ok
  end

  defp key_held_by_other_registry?(key, store) do
    case TypeRegistry.get_type(key, store) do
      "none" -> false
      "string" -> false
      _type -> true
    end
  rescue
    _ -> false
  end

  defp decode_raw_digest({:tdigest, centroids, metadata}) do
    {:ok, deserialize(centroids, metadata)}
  end

  defp decode_raw_digest(bin) when is_binary(bin) do
    try do
      case :erlang.binary_to_term(bin) do
        {:tdigest, centroids, metadata} -> {:ok, deserialize(centroids, metadata)}
        _ -> {:error, :wrongtype}
      end
    rescue
      ArgumentError -> {:error, :wrongtype}
    end
  end

  defp decode_raw_digest(_raw), do: {:error, :wrongtype}

  defp persist!(key, %Core{} = digest, store) do
    encoded = serialize(digest)
    Ops.put(store, key, :erlang.term_to_binary(encoded), 0)
  end

  defp serialize(%Core{} = digest) do
    metadata = %{
      compression: digest.compression,
      count: digest.count,
      min: digest.min,
      max: digest.max,
      buffer: digest.buffer,
      buffer_size: digest.buffer_size,
      total_compressions: digest.total_compressions
    }

    {:tdigest, digest.centroids, metadata}
  end

  defp deserialize(centroids, metadata) do
    %Core{
      compression: metadata.compression,
      centroids: centroids,
      count: metadata.count,
      min: metadata.min,
      max: metadata.max,
      buffer: Map.get(metadata, :buffer, []),
      buffer_size: Map.get(metadata, :buffer_size, 0),
      total_compressions: Map.get(metadata, :total_compressions, 0)
    }
  end

  # ===========================================================================
  # Private: merge operations
  # ===========================================================================

  defp load_source_digests(store, src_keys) do
    results = Enum.map(src_keys, &get_digest(store, &1))
    error = Enum.find(results, &match?({:error, _}, &1))

    if error do
      error
    else
      {:ok, Enum.map(results, fn {:ok, d} -> d end)}
    end
  end

  defp do_merge(store, dest, src_digests, opts) do
    override = Keyword.get(opts, :override, false)
    compression = Keyword.get(opts, :compression, nil)

    # Determine the compression for the result
    max_src_compression =
      src_digests
      |> Enum.map(& &1.compression)
      |> Enum.max()

    # Load existing destination if it exists and we're not overriding
    dest_digest =
      if override do
        nil
      else
        case get_digest(store, dest) do
          {:ok, d} -> d
          _ -> nil
        end
      end

    final_compression =
      cond do
        compression != nil -> compression
        dest_digest != nil -> dest_digest.compression
        true -> max_src_compression
      end

    # Combine all digests (including existing dest if not overriding)
    all_digests =
      if dest_digest != nil do
        [dest_digest | src_digests]
      else
        src_digests
      end

    merged = Core.merge_many(all_digests, final_compression)
    persist!(dest, merged, store)
    :ok
  end

  defp parse_merge_args(args, numkeys) when length(args) < numkeys do
    _ = numkeys
    {:error, "ERR wrong number of arguments for 'tdigest.merge' command"}
  end

  defp parse_merge_args(args, numkeys) do
    {src_keys, remaining} = Enum.split(args, numkeys)
    parse_merge_opts(src_keys, remaining, [])
  end

  defp parse_merge_opts(src_keys, [], opts) do
    {:ok, src_keys, opts}
  end

  defp parse_merge_opts(src_keys, ["COMPRESSION", comp_str | rest], opts) do
    case parse_pos_integer(comp_str, "compression") do
      {:ok, comp} -> parse_merge_opts(src_keys, rest, [{:compression, comp} | opts])
      error -> error
    end
  end

  defp parse_merge_opts(src_keys, ["OVERRIDE" | rest], opts) do
    parse_merge_opts(src_keys, rest, [{:override, true} | opts])
  end

  defp parse_merge_opts(_src_keys, _remaining, _opts) do
    {:error, "ERR syntax error in 'tdigest.merge' command"}
  end

  # ===========================================================================
  # Private: argument parsing
  # ===========================================================================

  defp parse_pos_integer(str, label) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      {_n, ""} -> {:error, "ERR #{label} must be a positive integer"}
      _ -> {:error, "ERR #{label} is not an integer or out of range"}
    end
  end

  defp parse_float_value(str) do
    case Float.parse(str) do
      {f, ""} ->
        {:ok, f}

      :error ->
        case Integer.parse(str) do
          {i, ""} -> {:ok, i / 1}
          _ -> {:error, "ERR TDIGEST: value is not a valid number"}
        end
    end
  end

  defp parse_float_list(strs) do
    result =
      Enum.reduce_while(strs, [], fn str, acc ->
        case parse_float_value(str) do
          {:ok, f} -> {:cont, [f | acc]}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case result do
      {:error, _} = err -> err
      list -> {:ok, Enum.reverse(list)}
    end
  end

  defp parse_quantile(str) do
    case parse_float_value(str) do
      {:ok, q} when q >= 0.0 and q <= 1.0 -> {:ok, q}
      {:ok, _} -> {:error, "ERR TDIGEST: quantile must be between 0 and 1"}
      error -> error
    end
  end

  defp parse_quantile_list(strs) do
    result =
      Enum.reduce_while(strs, [], fn str, acc ->
        case parse_quantile(str) do
          {:ok, q} -> {:cont, [q | acc]}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case result do
      {:error, _} = err -> err
      list -> {:ok, Enum.reverse(list)}
    end
  end

  defp parse_integer_list(strs) do
    result =
      Enum.reduce_while(strs, [], fn str, acc ->
        case Integer.parse(str) do
          {i, ""} -> {:cont, [i | acc]}
          _ -> {:halt, {:error, "ERR TDIGEST: value is not an integer"}}
        end
      end)

    case result do
      {:error, _} = err -> err
      list -> {:ok, Enum.reverse(list)}
    end
  end

  # ===========================================================================
  # Private: result formatting
  # ===========================================================================

  defp format_number(val) when is_float(val) do
    :erlang.float_to_binary(val, [:compact, decimals: 17])
  end

  defp format_number(val) when is_integer(val) do
    Integer.to_string(val)
  end

  defp format_number(:nan), do: "nan"
  defp format_number(:inf), do: "inf"
  defp format_number(:"-inf"), do: "-inf"

  defp format_float_results(results) do
    Enum.map(results, &format_number/1)
  end

  defp format_rank_results(results) do
    Enum.map(results, fn
      :nan -> "nan"
      :inf -> "inf"
      :"-inf" -> "-inf"
      val when is_float(val) -> format_number(val)
      val when is_integer(val) -> format_number(val)
    end)
  end

  defp format_single_float(:nan), do: "nan"
  defp format_single_float(val), do: format_number(val)
end
