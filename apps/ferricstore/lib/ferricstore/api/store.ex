defmodule FerricStore.API.Store do
  @moduledoc false

  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  def default_ctx do
    FerricStore.Instance.get(:default)
  end

  # Private — result wrapping helper
  # ---------------------------------------------------------------------------

  def wrap_result({:error, _} = err), do: err
  def wrap_result(result), do: {:ok, result}

  def parse_zbound("-inf"), do: :neg_inf
  def parse_zbound("+inf"), do: :inf
  def parse_zbound("inf"), do: :inf

  def parse_zbound("(" <> rest) do
    case Float.parse(rest) do
      {score, ""} -> {:exclusive, score}
      _ -> {:error, "ERR min or max is not a float"}
    end
  end

  def parse_zbound(value) when is_binary(value) do
    case Float.parse(value) do
      {score, ""} -> {:inclusive, score}
      _ -> {:error, "ERR min or max is not a float"}
    end
  end

  def parse_stream_range_id("-", true), do: :min
  def parse_stream_range_id("+", false), do: :max

  def parse_stream_range_id(id, _is_start) do
    case String.split(id, "-", parts: 2) do
      [ms, seq] ->
        with {ms_int, ""} <- Integer.parse(ms),
             {seq_int, ""} <- Integer.parse(seq),
             true <- ms_int >= 0 and seq_int >= 0 do
          {ms_int, seq_int}
        else
          _ -> {:error, "ERR Invalid stream ID specified as stream command argument"}
        end

      [ms] ->
        with {ms_int, ""} <- Integer.parse(ms),
             true <- ms_int >= 0 do
          {ms_int, 0}
        else
          _ -> {:error, "ERR Invalid stream ID specified as stream command argument"}
        end
    end
  end

  def normalize_geo_unit(unit) when is_binary(unit) do
    case String.upcase(unit) do
      value when value in ["M", "KM", "FT", "MI"] ->
        value

      _ ->
        {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — string store builder for bitmap/hyperloglog operations
  # ---------------------------------------------------------------------------

  def build_string_store(_key) do
    ctx = default_ctx()

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      put: fn k, v, exp -> Router.put(ctx, k, v, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      incr: fn k, d -> Router.incr(ctx, k, d) end,
      incr_float: fn k, d -> Router.incr_float(ctx, k, d) end,
      append: fn k, s -> Router.append(ctx, k, s) end,
      getset: fn k, v -> Router.getset(ctx, k, v) end,
      getdel: fn k -> Router.getdel(ctx, k) end,
      getex: fn k, e -> Router.getex(ctx, k, e) end,
      setrange: fn k, o, v -> Router.setrange(ctx, k, o, v) end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        Router.compound_get_meta(ctx, redis_key, compound_key)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        Router.compound_batch_get(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        Router.compound_batch_get_meta(ctx, redis_key, compound_keys)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        Router.compound_put(ctx, redis_key, compound_key, value, expire_at_ms)
      end,
      compound_batch_put: fn redis_key, entries ->
        Router.compound_batch_put(ctx, redis_key, entries)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end,
      compound_scan: fn redis_key, prefix -> Router.compound_scan(ctx, redis_key, prefix) end,
      compound_count: fn redis_key, prefix -> Router.compound_count(ctx, redis_key, prefix) end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end
    }
  end

  # ---------------------------------------------------------------------------
  # Private — stream store builder
  # ---------------------------------------------------------------------------

  def build_stream_store(key) do
    build_string_store(key)
  end

  # ---------------------------------------------------------------------------
  # Private — probabilistic structure store builder
  # ---------------------------------------------------------------------------

  def build_prob_store(key) do
    ctx = default_ctx()
    # Probabilistic structures route writes through Raft and reads via
    # stateless pread NIFs. The store needs prob_dir and prob_write.
    index = Router.shard_for(ctx, key)
    ensure_prob_registry_tables(index)

    data_dir = ctx.data_dir
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, index)

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      put: fn k, v, exp -> Router.put(ctx, k, v, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      prob_dir: fn -> Path.join(shard_data_path, "prob") end,
      prob_dir_for_key: fn key ->
        idx = Router.shard_for(ctx, key)
        sp = Ferricstore.DataDir.shard_data_path(data_dir, idx)
        Path.join(sp, "prob")
      end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end
    }
  end

  def ensure_prob_registry_tables(_index), do: :ok

  # ---------------------------------------------------------------------------
  # Private — TopK store builder
  # ---------------------------------------------------------------------------

  def build_topk_store(key) do
    ctx = default_ctx()
    data_dir = ctx.data_dir
    index = Router.shard_for(ctx, key)
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, index)

    %{
      get: fn key ->
        case Router.get(ctx, key) do
          nil ->
            nil

          bin when is_binary(bin) ->
            decode_topk_metadata(bin)
        end
      end,
      put: fn key, val, exp ->
        Router.put(ctx, key, encode_topk_metadata(val), exp)
      end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      # Route topk writes through Raft so all replicas materialize the same
      # mmap file and follower TOPK.LIST/QUERY work. Without prob_write the
      # command falls back to applying locally on the originating node only.
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end,
      prob_dir: fn ->
        prob_dir = Path.join(shard_data_path, "prob")
        Ferricstore.FS.mkdir_p!(prob_dir)
        prob_dir
      end,
      prob_dir_for_key: fn key ->
        idx = Router.shard_for(ctx, key)
        sp = Ferricstore.DataDir.shard_data_path(data_dir, idx)
        prob_dir = Path.join(sp, "prob")
        Ferricstore.FS.mkdir_p!(prob_dir)
        prob_dir
      end
    }
  end

  # ---------------------------------------------------------------------------
  # Private — TDigest store builder
  # ---------------------------------------------------------------------------
  # TDigest commands now enter through typed command AST handlers with this
  # store. Writes use prob_write so replicas materialize the same mmap files.

  def build_tdigest_store(ctx \\ default_ctx()) do
    %{
      get: fn key ->
        case Router.get(ctx, key) do
          nil ->
            nil

          bin when is_binary(bin) ->
            decode_tdigest(bin)
        end
      end,
      put: fn key, val, exp ->
        Router.put(ctx, key, encode_tdigest(val), exp)
      end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn key ->
        Router.get(ctx, key) != nil
      end,
      keys: fn -> Router.keys(ctx) end
    }
  end

  defp decode_topk_metadata(binary) do
    case TermCodec.decode(binary) do
      {:ok, {:topk_meta, metadata} = value} when is_map(metadata) -> value
      {:ok, {:topk_path, path} = value} when is_binary(path) -> value
      _invalid_or_unrelated -> binary
    end
  end

  defp encode_topk_metadata({:topk_meta, metadata} = value) when is_map(metadata),
    do: TermCodec.encode(value)

  defp encode_topk_metadata({:topk_path, path} = value) when is_binary(path),
    do: TermCodec.encode(value)

  defp encode_topk_metadata(value), do: value

  defp decode_tdigest(binary) do
    case TermCodec.decode(binary) do
      {:ok, {:tdigest, _centroids, _metadata} = value} -> value
      _invalid_or_unrelated -> binary
    end
  end

  defp encode_tdigest({:tdigest, _centroids, _metadata} = value), do: TermCodec.encode(value)
  defp encode_tdigest(value), do: value

  # ---------------------------------------------------------------------------
  # Private — compound key store builder for set/sorted-set operations
  # ---------------------------------------------------------------------------

  # Builds the store map expected by Commands.Set and Commands.SortedSet.
  # The store maps compound key operations to the correct shard GenServer
  # using the Redis key for routing (all sub-keys for one Redis key live
  # on the same shard).
  def build_compound_store(_key) do
    ctx = default_ctx()

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      put: fn k, v, exp -> Router.put(ctx, k, v, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end,
      # Compound ops route through Router so they get the same not_leader →
      # forward + read-your-write barrier as plain Router.put. Going direct
      # to the local shard skips that and silently loses writes when the
      # local node isn't the leader for this key's shard.
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        Router.compound_get_meta(ctx, redis_key, compound_key)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        Router.compound_batch_get(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        Router.compound_batch_get_meta(ctx, redis_key, compound_keys)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        Router.compound_put(ctx, redis_key, compound_key, value, expire_at_ms)
      end,
      compound_batch_put: fn redis_key, entries ->
        Router.compound_batch_put(ctx, redis_key, entries)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end,
      compound_scan: fn redis_key, prefix -> Router.compound_scan(ctx, redis_key, prefix) end,
      compound_count: fn redis_key, prefix -> Router.compound_count(ctx, redis_key, prefix) end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end,
      zset_score_range: fn redis_key, min_bound, max_bound, reverse? ->
        Router.zset_score_range(ctx, redis_key, min_bound, max_bound, reverse?)
      end,
      zset_score_range_slice: fn redis_key, min_bound, max_bound, reverse?, offset, count ->
        Router.zset_score_range_slice(
          ctx,
          redis_key,
          min_bound,
          max_bound,
          reverse?,
          offset,
          count
        )
      end,
      zset_score_count: fn redis_key, min_bound, max_bound ->
        Router.zset_score_count(ctx, redis_key, min_bound, max_bound)
      end,
      zset_rank_range: fn redis_key, start_idx, stop_idx, reverse? ->
        Router.zset_rank_range(ctx, redis_key, start_idx, stop_idx, reverse?)
      end,
      zset_member_rank: fn redis_key, member, reverse? ->
        Router.zset_member_rank(ctx, redis_key, member, reverse?)
      end
    }
  end
end
