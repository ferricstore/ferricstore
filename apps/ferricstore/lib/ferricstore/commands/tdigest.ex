defmodule Ferricstore.Commands.TDigest do
  @moduledoc """
  Handles TDIGEST.* commands.

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
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.TypeRegistry
  alias Ferricstore.TDigest.Core
  alias Ferricstore.TermCodec

  @wrongtype_msg "WRONGTYPE Operation against a key holding the wrong kind of value"
  @max_compression 1_000
  # Centroid weights are IEEE-754 doubles, whose largest consecutive integer is 2^53.
  @max_digest_count 9_007_199_254_740_992
  @max_batch_items 10_000
  @max_merge_sources 10_000

  @doc """
  Handles a TDIGEST command.

  ## Parameters

    - `cmd` -- uppercased command name (e.g. `"TDIGEST.CREATE"`)
    - `args` -- list of string arguments
    - `store` -- injected store map with `get`, `put`, `exists?` callbacks

  ## Returns

  Plain Elixir term: `:ok`, float, list, or `{:error, message}`.
  """
  @spec handle_ast(term(), map()) :: term()
  def handle_ast({tag, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}
  def handle_ast({tag, _key, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}

  def handle_ast({:tdigest_create, key, nil}, store) do
    handle_ast({:tdigest_create, key, 100}, store)
  end

  def handle_ast({:tdigest_create, key, compression}, store) do
    with :ok <- validate_compression(compression),
         :ok <- check_create_available(key, store) do
      create_digest(key, compression, store)
    end
  end

  def handle_ast({:tdigest_add, key, floats}, store) do
    with :ok <- validate_float_list(floats),
         {:ok, digest} <- get_digest(store, key),
         :ok <- validate_observation_capacity(digest, length(floats)) do
      updated = Core.add_many(digest, floats)
      persist!(key, updated, store)
    end
  end

  def handle_ast({:tdigest_reset, [key]}, store), do: reset_digest(key, store)

  def handle_ast({:tdigest_quantile, key, quantiles}, store) do
    with :ok <- validate_quantile_list(quantiles),
         {:ok, digest} <- get_digest(store, key) do
      quantiles
      |> Enum.map(fn q -> Core.quantile(digest, q) end)
      |> format_float_results()
    end
  end

  def handle_ast({:tdigest_cdf, key, floats}, store) do
    with :ok <- validate_float_list(floats),
         {:ok, digest} <- get_digest(store, key) do
      floats
      |> Enum.map(fn v -> Core.cdf(digest, v) end)
      |> format_float_results()
    end
  end

  def handle_ast({:tdigest_rank, key, floats}, store) do
    with :ok <- validate_float_list(floats),
         {:ok, digest} <- get_digest(store, key) do
      Enum.map(floats, fn v -> Core.rank(digest, v) end)
    end
  end

  def handle_ast({:tdigest_revrank, key, floats}, store) do
    with :ok <- validate_float_list(floats),
         {:ok, digest} <- get_digest(store, key) do
      Enum.map(floats, fn v -> Core.rev_rank(digest, v) end)
    end
  end

  def handle_ast({:tdigest_byrank, key, ranks}, store) do
    with :ok <- validate_rank_list(ranks),
         {:ok, digest} <- get_digest(store, key) do
      ranks
      |> Enum.map(fn r -> Core.by_rank(digest, r) end)
      |> format_rank_results()
    end
  end

  def handle_ast({:tdigest_byrevrank, key, ranks}, store) do
    with :ok <- validate_rank_list(ranks),
         {:ok, digest} <- get_digest(store, key) do
      ranks
      |> Enum.map(fn r -> Core.by_rev_rank(digest, r) end)
      |> format_rank_results()
    end
  end

  def handle_ast({:tdigest_trimmed_mean, key, lo, hi}, store) do
    with :ok <- validate_trimmed_quantiles(lo, hi),
         {:ok, digest} <- get_digest(store, key) do
      digest
      |> Core.trimmed_mean(lo, hi)
      |> format_single_float()
    end
  end

  def handle_ast({:tdigest_min, [key]}, store), do: min_digest(key, store)
  def handle_ast({:tdigest_max, [key]}, store), do: max_digest(key, store)
  def handle_ast({:tdigest_info, [key]}, store), do: info_digest(key, store)

  def handle_ast({:tdigest_merge, dest, src_keys, opts}, store) do
    with :ok <- validate_merge_args(dest, src_keys, opts),
         {:ok, dest_digest} <- load_destination_digest(store, dest, opts),
         {:ok, src_digests} <- load_source_digests(store, src_keys) do
      do_merge(store, dest, dest_digest, src_digests, opts)
    end
  end

  @doc false
  @spec encoded_empty_size(pos_integer()) :: pos_integer()
  def encoded_empty_size(compression)
      when is_integer(compression) and compression > 0 and compression <= @max_compression do
    compression
    |> Core.new()
    |> serialize()
    |> TermCodec.encode()
    |> byte_size()
  end

  @doc false
  @spec encoded_size_after_add(term(), [binary()]) :: {:ok, pos_integer()} | {:error, term()}
  def encoded_size_after_add(raw, value_args) when is_list(value_args) do
    with {:ok, values} <- parse_float_list(value_args),
         {:ok, digest} <- decode_raw_digest(raw),
         :ok <- validate_observation_capacity(digest, length(values)) do
      size =
        digest
        |> Core.add_many(values)
        |> serialize()
        |> TermCodec.encode()
        |> byte_size()

      {:ok, size}
    end
  end

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

  def handle("TDIGEST.CREATE", [key, option, comp_str], store) do
    case String.upcase(option) do
      "COMPRESSION" ->
        with {:ok, compression} <- parse_pos_integer(comp_str, "compression"),
             :ok <- validate_compression(compression),
             :ok <- check_create_available(key, store) do
          create_digest(key, compression, store)
        end

      _ ->
        {:error, "ERR wrong number of arguments for 'tdigest.create' command"}
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
    with {:ok, floats} <- parse_float_list(values),
         {:ok, digest} <- get_digest(store, key),
         :ok <- validate_observation_capacity(digest, length(floats)) do
      updated = Core.add_many(digest, floats)
      persist!(key, updated, store)
    end
  end

  def handle("TDIGEST.ADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.add' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.RESET key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.RESET", [key], store), do: reset_digest(key, store)

  def handle("TDIGEST.RESET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.reset' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.QUANTILE key quantile [quantile ...]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.QUANTILE", [key | qs], store) when qs != [] do
    with {:ok, quantiles} <- parse_quantile_list(qs),
         {:ok, digest} <- get_digest(store, key) do
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
    with {:ok, floats} <- parse_float_list(values),
         {:ok, digest} <- get_digest(store, key) do
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
    with {:ok, floats} <- parse_float_list(values),
         {:ok, digest} <- get_digest(store, key) do
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
    with {:ok, floats} <- parse_float_list(values),
         {:ok, digest} <- get_digest(store, key) do
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
    with {:ok, rank_ints} <- parse_integer_list(ranks),
         {:ok, digest} <- get_digest(store, key) do
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
    with {:ok, rank_ints} <- parse_integer_list(ranks),
         {:ok, digest} <- get_digest(store, key) do
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
    with {:ok, lo} <- parse_quantile(lo_str),
         {:ok, hi} <- parse_quantile(hi_str),
         :ok <- validate_trimmed_quantiles(lo, hi),
         {:ok, digest} <- get_digest(store, key) do
      result = Core.trimmed_mean(digest, lo, hi)
      format_single_float(result)
    end
  end

  def handle("TDIGEST.TRIMMED_MEAN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.trimmed_mean' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.MIN key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.MIN", [key], store), do: min_digest(key, store)

  def handle("TDIGEST.MIN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.min' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.MAX key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.MAX", [key], store), do: max_digest(key, store)

  def handle("TDIGEST.MAX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.max' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.INFO key
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.INFO", [key], store), do: info_digest(key, store)

  def handle("TDIGEST.INFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.info' command"}
  end

  # ---------------------------------------------------------------------------
  # TDIGEST.MERGE destkey numkeys src [src ...] [COMPRESSION c] [OVERRIDE]
  # ---------------------------------------------------------------------------

  def handle("TDIGEST.MERGE", [dest, numkeys_str | rest], store) do
    with {:ok, numkeys} <- parse_pos_integer(numkeys_str, "numkeys"),
         :ok <- validate_merge_source_count(numkeys),
         {:ok, src_keys, opts} <- parse_merge_args(rest, numkeys),
         {:ok, dest_digest} <- load_destination_digest(store, dest, opts),
         {:ok, src_digests} <- load_source_digests(store, src_keys) do
      do_merge(store, dest, dest_digest, src_digests, opts)
    end
  end

  def handle("TDIGEST.MERGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'tdigest.merge' command"}
  end

  # ===========================================================================
  # Private: store operations
  # ===========================================================================

  defp reset_digest(key, store) do
    with {:ok, digest} <- get_digest(store, key) do
      updated = Core.reset(digest)
      persist!(key, updated, store)
    end
  end

  defp min_digest(key, store) do
    with {:ok, digest} <- get_digest(store, key) do
      case digest.min do
        nil -> "nan"
        val -> format_number(val)
      end
    end
  end

  defp max_digest(key, store) do
    with {:ok, digest} <- get_digest(store, key) do
      case digest.max do
        nil -> "nan"
        val -> format_number(val)
      end
    end
  end

  defp info_digest(key, store) do
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

  defp get_digest(store, key) do
    case Ops.get(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        case key_held_by_other_registry?(key, store) do
          {:ok, true} -> {:error, @wrongtype_msg}
          {:ok, false} -> {:error, "ERR TDIGEST: key does not exist"}
          {:error, _reason} = error -> error
        end

      raw ->
        case decode_raw_digest(raw) do
          {:ok, digest} -> {:ok, digest}
          {:error, _wrongtype} -> {:error, @wrongtype_msg}
        end
    end
  end

  defp decode_raw_digest({:tdigest, centroids, metadata}) do
    safe_deserialize(centroids, metadata)
  end

  defp decode_raw_digest(bin) when is_binary(bin) do
    case TermCodec.decode(bin) do
      {:ok, {:tdigest, centroids, metadata}} -> safe_deserialize(centroids, metadata)
      _invalid -> {:error, :wrongtype}
    end
  end

  defp decode_raw_digest(_raw), do: {:error, :wrongtype}

  defp check_create_available(key, store) do
    case Ops.get(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        case key_held_by_other_registry?(key, store) do
          {:ok, true} -> {:error, @wrongtype_msg}
          {:ok, false} -> :ok
          {:error, _reason} = error -> error
        end

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
  end

  defp key_held_by_other_registry?(key, store) do
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      "none" -> {:ok, false}
      "string" -> {:ok, false}
      _type -> {:ok, true}
    end
  end

  defp persist!(key, %Core{} = digest, store) do
    encoded = serialize(digest)
    Ops.put(store, key, TermCodec.encode(encoded), 0)
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

  defp safe_deserialize(
         centroids,
         %{
           compression: compression,
           count: count,
           min: min,
           max: max,
           buffer: buffer,
           buffer_size: buffer_size,
           total_compressions: total_compressions
         } = metadata
       )
       when map_size(metadata) == 7 and is_integer(compression) and compression > 0 and
              compression <= @max_compression and is_integer(count) and count >= 0 and
              count <= @max_digest_count and is_integer(buffer_size) and
              buffer_size >= 0 and is_integer(total_compressions) and total_compressions >= 0 do
    if valid_digest_shape?(centroids, buffer, buffer_size, compression, min, max, count) do
      {:ok, deserialize(centroids, metadata)}
    else
      {:error, :wrongtype}
    end
  end

  defp safe_deserialize(_centroids, _metadata), do: {:error, :wrongtype}

  defp valid_digest_shape?([], [], 0, _compression, nil, nil, 0), do: true

  defp valid_digest_shape?(centroids, buffer, buffer_size, compression, min, max, count)
       when is_list(centroids) and is_list(buffer) and is_float(min) and is_float(max) and
              count > 0 and min <= max and buffer_size < compression * 3 do
    with {:ok, centroid_weight} <-
           centroid_weight(centroids, nil, 0.0, min, max, count),
         :ok <- valid_buffer_values(buffer, buffer_size, min, max) do
      centroid_weight + buffer_size == count
    else
      _invalid -> false
    end
  end

  defp valid_digest_shape?(
         _centroids,
         _buffer,
         _buffer_size,
         _compression,
         _min,
         _max,
         _count
       ),
       do: false

  defp centroid_weight([], _previous_mean, total, _min, _max, _count), do: {:ok, total}

  defp centroid_weight(
         [{mean, weight} | rest],
         previous_mean,
         total,
         min,
         max,
         count
       )
       when is_float(mean) and is_float(weight) and weight > 0.0 and mean >= min and mean <= max do
    next_total = total + weight

    if (is_nil(previous_mean) or mean >= previous_mean) and next_total <= count do
      centroid_weight(rest, mean, next_total, min, max, count)
    else
      :error
    end
  end

  defp centroid_weight(_centroids, _previous_mean, _total, _min, _max, _count), do: :error

  defp valid_buffer_values([], 0, _min, _max), do: :ok

  defp valid_buffer_values([value | rest], remaining, min, max)
       when is_float(value) and value >= min and value <= max and remaining > 0,
       do: valid_buffer_values(rest, remaining - 1, min, max)

  defp valid_buffer_values(_buffer, _remaining, _min, _max), do: :error

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
    Enum.reduce_while(src_keys, {:ok, []}, fn key, {:ok, acc} ->
      case get_digest(store, key) do
        {:ok, digest} -> {:cont, {:ok, [digest | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, digests} -> {:ok, Enum.reverse(digests)}
      {:error, _reason} = error -> error
    end
  end

  defp load_destination_digest(store, dest, opts) do
    override? = Keyword.get(opts, :override, false)

    case get_digest(store, dest) do
      {:ok, digest} -> {:ok, if(override?, do: nil, else: digest)}
      {:error, "ERR TDIGEST: key does not exist"} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  defp do_merge(store, dest, dest_digest, src_digests, opts) do
    with :ok <- validate_merged_count(dest_digest, src_digests) do
      do_merge_validated(store, dest, dest_digest, src_digests, opts)
    end
  end

  defp do_merge_validated(store, dest, dest_digest, src_digests, opts) do
    compression = Keyword.get(opts, :compression, nil)

    # Determine the compression for the result
    max_src_compression =
      src_digests
      |> Enum.map(& &1.compression)
      |> Enum.max()

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

  defp parse_merge_opts(src_keys, [opt | rest], opts) do
    case String.upcase(opt) do
      "COMPRESSION" ->
        case rest do
          [comp_str | rest] ->
            case parse_pos_integer(comp_str, "compression") do
              {:ok, comp} ->
                with :ok <- validate_compression(comp) do
                  parse_merge_opts(src_keys, rest, [{:compression, comp} | opts])
                end

              error ->
                error
            end

          _ ->
            {:error, "ERR syntax error in 'tdigest.merge' command"}
        end

      "OVERRIDE" ->
        parse_merge_opts(src_keys, rest, [{:override, true} | opts])

      _ ->
        {:error, "ERR syntax error in 'tdigest.merge' command"}
    end
  end

  # ===========================================================================
  # Private: argument parsing
  # ===========================================================================

  defp validate_compression(compression)
       when is_integer(compression) and compression > 0 and compression <= @max_compression,
       do: :ok

  defp validate_compression(compression)
       when is_integer(compression) and compression > @max_compression,
       do: {:error, "ERR compression must be <= #{@max_compression}"}

  defp validate_compression(_compression),
    do: {:error, "ERR compression must be a positive integer"}

  defp validate_float_list([_ | _] = values) do
    with :ok <- validate_batch_count(values) do
      if Enum.all?(values, &is_float/1) do
        :ok
      else
        {:error, "ERR TDIGEST: value is not a valid number"}
      end
    end
  end

  defp validate_float_list(_values),
    do: {:error, "ERR TDIGEST: values must be a non-empty list"}

  defp validate_quantile_list([_ | _] = quantiles) do
    with :ok <- validate_batch_count(quantiles) do
      if Enum.all?(quantiles, &(is_float(&1) and &1 >= 0.0 and &1 <= 1.0)) do
        :ok
      else
        {:error, "ERR TDIGEST: quantile must be between 0 and 1"}
      end
    end
  end

  defp validate_quantile_list(_quantiles),
    do: {:error, "ERR TDIGEST: quantiles must be a non-empty list"}

  defp validate_rank_list([_ | _] = ranks) do
    with :ok <- validate_batch_count(ranks) do
      if Enum.all?(ranks, &is_integer/1) do
        :ok
      else
        {:error, "ERR TDIGEST: rank must be an integer"}
      end
    end
  end

  defp validate_rank_list(_ranks),
    do: {:error, "ERR TDIGEST: ranks must be a non-empty list"}

  defp validate_trimmed_quantiles(lo, hi)
       when is_float(lo) and is_float(hi) and lo >= 0.0 and hi <= 1.0 and lo < hi,
       do: :ok

  defp validate_trimmed_quantiles(_lo, _hi),
    do: {:error, "ERR TDIGEST: low_quantile must be less than high_quantile in [0, 1]"}

  defp validate_merge_args(dest, [_ | _] = src_keys, opts)
       when is_binary(dest) and is_list(opts) do
    with :ok <- validate_merge_source_count(length(src_keys)) do
      if Enum.all?(src_keys, &is_binary/1) do
        validate_merge_options(opts)
      else
        {:error, "ERR TDIGEST: source keys must be binaries"}
      end
    end
  end

  defp validate_merge_args(_dest, _src_keys, _opts),
    do: {:error, "ERR TDIGEST: invalid merge arguments"}

  defp validate_merge_source_count(count) when count <= @max_merge_sources, do: :ok

  defp validate_merge_source_count(_count),
    do: {:error, "ERR TDIGEST: too many source keys (maximum #{@max_merge_sources})"}

  defp validate_merge_options([]), do: :ok

  defp validate_merge_options([{:compression, compression} | rest]) do
    with :ok <- validate_compression(compression) do
      validate_merge_options(rest)
    end
  end

  defp validate_merge_options([{:override, override} | rest]) when is_boolean(override),
    do: validate_merge_options(rest)

  defp validate_merge_options(_opts), do: {:error, "ERR TDIGEST: invalid merge options"}

  defp validate_batch_count(values) do
    case Enum.reduce_while(values, 0, fn _value, count ->
           if count < @max_batch_items,
             do: {:cont, count + 1},
             else: {:halt, :too_many}
         end) do
      :too_many -> {:error, "ERR TDIGEST: batch exceeds maximum of 10000 items"}
      _count -> :ok
    end
  end

  defp validate_observation_capacity(%Core{count: count}, additional)
       when is_integer(additional) and additional >= 0 and
              count <= @max_digest_count - additional,
       do: :ok

  defp validate_observation_capacity(_digest, _additional),
    do: {:error, "ERR TDIGEST: observation count exceeds supported maximum"}

  defp validate_merged_count(dest_digest, src_digests) do
    digests = if is_nil(dest_digest), do: src_digests, else: [dest_digest | src_digests]

    Enum.reduce_while(digests, 0, fn %Core{count: count}, total ->
      if count <= @max_digest_count - total do
        {:cont, total + count}
      else
        {:halt, {:error, "ERR TDIGEST: observation count exceeds supported maximum"}}
      end
    end)
    |> case do
      {:error, _message} = error -> error
      _count -> :ok
    end
  end

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
  rescue
    ArgumentError -> {:error, "ERR TDIGEST: value is not a valid number"}
    ArithmeticError -> {:error, "ERR TDIGEST: value is not a valid number"}
  end

  defp parse_float_list(strs) do
    with :ok <- validate_batch_count(strs) do
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
  end

  defp parse_quantile(str) do
    case parse_float_value(str) do
      {:ok, q} when q >= 0.0 and q <= 1.0 -> {:ok, q}
      {:ok, _} -> {:error, "ERR TDIGEST: quantile must be between 0 and 1"}
      error -> error
    end
  end

  defp parse_quantile_list(strs) do
    with :ok <- validate_batch_count(strs) do
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
  end

  defp parse_integer_list(strs) do
    with :ok <- validate_batch_count(strs) do
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
