defmodule Ferricstore.Store.Shard.Compound do
  @moduledoc "Compound-key CRUD, prefix scan/count, promoted-collection dedicated storage, and automatic compaction."

  alias Ferricstore.Store.Shard.Compound.{Ops, Promoted, Read}

  @spec handle_compound_get(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get(redis_key, compound_key, state),
    do: Read.handle_compound_get(redis_key, compound_key, state)

  @spec handle_compound_batch_get(binary(), [binary()], map()) :: {:reply, [term()], map()}
  @doc false
  def handle_compound_batch_get(redis_key, compound_keys, state),
    do: Read.handle_compound_batch_get(redis_key, compound_keys, state)

  @spec handle_compound_get_meta(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get_meta(redis_key, compound_key, state),
    do: Read.handle_compound_get_meta(redis_key, compound_key, state)

  @spec handle_compound_batch_get_meta(binary(), [binary()], map()) :: {:reply, [term()], map()}
  @doc false
  def handle_compound_batch_get_meta(redis_key, compound_keys, state),
    do: Read.handle_compound_batch_get_meta(redis_key, compound_keys, state)

  @spec handle_compound_put(binary(), binary(), binary(), non_neg_integer(), map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_compound_put(redis_key, compound_key, value, expire_at_ms, state),
    do: Ops.handle_compound_put(redis_key, compound_key, value, expire_at_ms, state)

  @spec handle_compound_batch_put(binary(), [{binary(), binary(), non_neg_integer()}], map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_compound_batch_put(redis_key, entries, state),
    do: Ops.handle_compound_batch_put(redis_key, entries, state)

  @spec handle_compound_delete(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_delete(redis_key, compound_key, state),
    do: Ops.handle_compound_delete(redis_key, compound_key, state)

  @spec handle_compound_batch_delete(binary(), [binary()], map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_batch_delete(redis_key, compound_keys, state),
    do: Ops.handle_compound_batch_delete(redis_key, compound_keys, state)

  @spec handle_compound_scan(binary(), binary(), map()) :: {:reply, [{binary(), binary()}], map()}
  @doc false
  def handle_compound_scan(redis_key, prefix, state),
    do: Ops.handle_compound_scan(redis_key, prefix, state)

  @spec handle_compound_scan_bounded(binary(), binary(), map(), map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_compound_scan_bounded(redis_key, prefix, limits, state),
    do: Ops.handle_compound_scan_bounded(redis_key, prefix, limits, state)

  @spec handle_compound_scan_page(
          binary(),
          binary(),
          0 | {:after, binary()},
          pos_integer(),
          binary() | nil,
          boolean(),
          map()
        ) :: {:reply, term(), map()}
  @doc false
  def handle_compound_scan_page(
        redis_key,
        prefix,
        cursor,
        count,
        match_pattern,
        fields_only,
        state
      ),
      do:
        Ops.handle_compound_scan_page(
          redis_key,
          prefix,
          cursor,
          count,
          match_pattern,
          fields_only,
          state
        )

  @spec handle_compound_fields(binary(), binary(), map()) :: {:reply, [binary()], map()}
  @doc false
  def handle_compound_fields(redis_key, prefix, state),
    do: Ops.handle_compound_fields(redis_key, prefix, state)

  @spec handle_compound_count(binary(), binary(), map()) ::
          {:reply, non_neg_integer() | Ferricstore.Store.ReadResult.failure(), map()}
  @doc false
  def handle_compound_count(redis_key, prefix, state),
    do: Ops.handle_compound_count(redis_key, prefix, state)

  @spec handle_zset_score_range(binary(), term(), term(), boolean(), map()) ::
          {:reply, [{binary(), binary()}], map()}
  @doc false
  def handle_zset_score_range(redis_key, min_bound, max_bound, reverse?, state),
    do: Ops.handle_zset_score_range(redis_key, min_bound, max_bound, reverse?, state)

  @spec handle_zset_score_range_slice(
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer(),
          map()
        ) :: {:reply, [{binary(), binary()}], map()}
  @doc false
  def handle_zset_score_range_slice(
        redis_key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count,
        state
      ),
      do:
        Ops.handle_zset_score_range_slice(
          redis_key,
          min_bound,
          max_bound,
          reverse?,
          offset,
          count,
          state
        )

  @spec handle_zset_score_count(binary(), term(), term(), map()) ::
          {:reply, non_neg_integer(), map()}
  @doc false
  def handle_zset_score_count(redis_key, min_bound, max_bound, state),
    do: Ops.handle_zset_score_count(redis_key, min_bound, max_bound, state)

  @spec handle_zset_score_count_many([{binary(), term(), term()}], map()) ::
          {:reply, [non_neg_integer()], map()}
  @doc false
  def handle_zset_score_count_many(queries, state),
    do: Ops.handle_zset_score_count_many(queries, state)

  @spec handle_zset_score_count_all_many_no_build([binary()], map()) ::
          {:reply, [non_neg_integer()], map()}
  @doc false
  def handle_zset_score_count_all_many_no_build(keys, state),
    do: Ops.handle_zset_score_count_all_many_no_build(keys, state)

  @spec handle_zset_rank_range(binary(), non_neg_integer(), non_neg_integer(), boolean(), map()) ::
          {:reply, [{binary(), binary()}], map()}
  @doc false
  def handle_zset_rank_range(redis_key, start_idx, stop_idx, reverse?, state),
    do: Ops.handle_zset_rank_range(redis_key, start_idx, stop_idx, reverse?, state)

  @spec handle_zset_member_rank(binary(), binary(), boolean(), map()) ::
          {:reply, non_neg_integer() | nil, map()}
  @doc false
  def handle_zset_member_rank(redis_key, member, reverse?, state),
    do: Ops.handle_zset_member_rank(redis_key, member, reverse?, state)

  @spec handle_compound_delete_prefix(binary(), binary(), map()) :: {:reply, :ok, map()}
  @doc false
  def handle_compound_delete_prefix(redis_key, prefix, state),
    do: Ops.handle_compound_delete_prefix(redis_key, prefix, state)

  @spec promoted_store(map(), binary()) :: binary() | nil
  @doc false
  def promoted_store(state, redis_key), do: Promoted.promoted_store(state, redis_key)

  @spec promoted_read(binary(), binary(), map()) ::
          {:ok, binary() | nil}
          | {:ok, binary(), non_neg_integer()}
          | {:ok, binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
             non_neg_integer()}
          | {:error, term()}
  @doc false
  def promoted_read(dedicated_path, compound_key, state),
    do: Promoted.promoted_read(dedicated_path, compound_key, state)

  @spec promoted_write(binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer(), non_neg_integer()} | {:error, term()}
  @doc false
  def promoted_write(dedicated_path, compound_key, value, expire_at_ms),
    do: Promoted.promoted_write(dedicated_path, compound_key, value, expire_at_ms)

  @spec promoted_tombstone(binary(), binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc false
  def promoted_tombstone(dedicated_path, compound_key),
    do: Promoted.promoted_tombstone(dedicated_path, compound_key)

  @spec promoted_tombstone_batch(binary(), [binary()]) :: {:ok, list()} | {:error, term()}
  @doc false
  def promoted_tombstone_batch(dedicated_path, compound_keys),
    do: Promoted.promoted_tombstone_batch(dedicated_path, compound_keys)

  @spec parse_fid_from_path(binary()) :: non_neg_integer()
  @doc false
  def parse_fid_from_path(path), do: Promoted.parse_fid_from_path(path)

  @spec dedicated_file_path(binary(), non_neg_integer()) :: binary()
  @doc false
  def dedicated_file_path(dedicated_path, file_id),
    do: Promoted.dedicated_file_path(dedicated_path, file_id)

  @spec bump_promoted_writes(map(), binary()) :: map()
  @doc false
  def bump_promoted_writes(state, redis_key),
    do: Promoted.bump_promoted_writes(state, redis_key)

  @spec apply_promoted_maintenance(map(), binary(), map()) :: map()
  @doc false
  def apply_promoted_maintenance(state, redis_key, maintenance),
    do: Promoted.apply_promoted_maintenance(state, redis_key, maintenance)

  @spec promoted_compaction_due?(map(), binary(), integer()) :: boolean()
  @doc false
  def promoted_compaction_due?(state, redis_key, now_ms \\ System.monotonic_time(:millisecond)),
    do: Promoted.promoted_compaction_due?(state, redis_key, now_ms)

  @spec compact_dedicated_result(map(), binary(), binary()) :: {:ok | :error, map()}
  @doc false
  def compact_dedicated_result(state, redis_key, dedicated_path),
    do: Promoted.compact_dedicated_result(state, redis_key, dedicated_path)

  @doc false
  @spec compact_dedicated_result_latched(map(), binary(), binary()) :: {:ok | :error, map()}
  def compact_dedicated_result_latched(state, redis_key, dedicated_path),
    do: Promoted.compact_dedicated_result_latched(state, redis_key, dedicated_path)

  @spec promoted_dir_size(binary()) :: non_neg_integer()
  @doc false
  def promoted_dir_size(dir_path), do: Promoted.promoted_dir_size(dir_path)

  @spec track_promoted_dead_bytes(map(), binary(), binary(), non_neg_integer()) :: map()
  @doc false
  def track_promoted_dead_bytes(state, redis_key, compound_key, new_record_size),
    do: Promoted.track_promoted_dead_bytes(state, redis_key, compound_key, new_record_size)

  @spec track_promoted_delete_bytes(map(), binary(), binary()) :: map()
  @doc false
  def track_promoted_delete_bytes(state, redis_key, compound_key),
    do: Promoted.track_promoted_delete_bytes(state, redis_key, compound_key)

  @doc false
  def track_promoted_delete_bytes_entry(state, redis_key, entry),
    do: Promoted.track_promoted_delete_bytes_entry(state, redis_key, entry)

  @spec compact_dedicated(map(), binary(), binary()) :: map()
  @doc false
  def compact_dedicated(state, redis_key, dedicated_path),
    do: Promoted.compact_dedicated(state, redis_key, dedicated_path)

  @spec promoted_prefix_for(map(), binary()) :: binary() | nil
  @doc false
  def promoted_prefix_for(state, redis_key),
    do: Promoted.promoted_prefix_for(state, redis_key)

  @spec maybe_promote(map(), binary(), binary()) :: map()
  @doc false
  def maybe_promote(state, redis_key, compound_key),
    do: Promoted.maybe_promote(state, redis_key, compound_key)

  @spec detect_compound_type(binary(), binary()) :: {atom(), binary()} | nil
  @doc false
  def detect_compound_type(redis_key, compound_key),
    do: Promoted.detect_compound_type(redis_key, compound_key)
end
