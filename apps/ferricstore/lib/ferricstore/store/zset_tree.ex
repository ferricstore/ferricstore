defmodule Ferricstore.Store.ZSetTree do
  @moduledoc """
  Sorted sets past the promotion threshold, from cluster feature level 9.

  A tree-encoded sorted set keeps its members in one file,
  `dedicated/shard_N/zset_tree:<sha256 of key>/<generation>.tree`, holding two counted
  copy-on-write B+trees (`native/ferricstore_bitcask/src/zset_tree`): members
  by score and scores by member. The keydir keeps only the key's type and
  promotion marker, so a set costs no RAM per member and needs no index
  rebuilt after a restart; every read, positions included, costs O(log N).

  Apply writes through `apply_ops/3` with the entry's Raft position; a
  replayed entry cuts the file back to before it and applies again, so it
  gets the outcome it had the first time. Reads open the file, read what they
  need and close it. This module translates the compound-member interface
  (`Z:key\\0member` keys with `Float.to_string/1` scores) to the tree.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundKey, Promotion}

  @type token :: {non_neg_integer(), non_neg_integer()}
  @type bound :: :neg_inf | :inf | {:inclusive, float()} | {:exclusive, float()}

  @nx 1
  @xx 2
  @gt 4
  @lt 8

  @spec dir(binary(), non_neg_integer(), binary()) :: binary()
  def dir(data_dir, shard_index, redis_key),
    do: Promotion.dedicated_path(data_dir, shard_index, :zset_tree, redis_key)

  @doc """
  The file of one incarnation of the set. The marker's generation is the Raft
  index of the entry that converted it, so every replica and every replay name
  the same file, and a set deleted and created again never shares one.
  """
  @spec path(binary(), non_neg_integer(), binary(), pos_integer()) :: binary()
  def path(data_dir, shard_index, redis_key, generation),
    do: Path.join(dir(data_dir, shard_index, redis_key), "#{generation}.tree")

  @doc """
  Written when the set is deleted, with the deleting entry's Raft index. The
  file stays until the durable position passes that index: a replay of an
  entry before the delete still reads it.
  """
  @spec removed_path(binary()) :: binary()
  def removed_path(tree_path), do: Path.rootname(tree_path) <> ".removed"

  @doc "The generation a promotion marker value names for a tree-encoded set, or nil."
  @spec marker_generation(term()) :: pos_integer() | nil
  def marker_generation(value) when is_binary(value) do
    case Promotion.decode_marker(value) do
      {:ok, :zset_tree, :promoted, generation} when generation > 0 -> generation
      _other -> nil
    end
  end

  def marker_generation(_value), do: nil

  @spec marker?(term()) :: boolean()
  def marker?(value), do: marker_generation(value) != nil

  @spec marker_value(pos_integer()) :: binary()
  def marker_value(generation), do: Promotion.encode_marker(:zset_tree, :promoted, generation)

  @doc "The member of `redis_key` a compound key names, or `:error`."
  @spec member(binary(), binary()) :: {:ok, binary()} | :error
  def member(redis_key, compound_key) do
    prefix = CompoundKey.zset_prefix(redis_key)
    size = byte_size(prefix)

    case compound_key do
      <<^prefix::binary-size(^size), member::binary>> -> {:ok, member}
      _other -> :error
    end
  end

  @spec member_prefix?(binary(), binary()) :: boolean()
  def member_prefix?(redis_key, prefix), do: prefix == CompoundKey.zset_prefix(redis_key)

  @doc "Scores are stored and returned in `Float.to_string/1`'s form."
  @spec score_string(float()) :: binary()
  def score_string(score) when is_float(score), do: Float.to_string(score)

  @spec parse_score(binary()) :: {:ok, float()} | :error
  def parse_score(value) when is_binary(value) do
    case Float.parse(value) do
      {score, ""} -> {:ok, score}
      _other -> :error
    end
  end

  def parse_score(_value), do: :error

  @spec flags(keyword() | map()) :: non_neg_integer()
  def flags(opts) do
    get = fn key ->
      if is_map(opts), do: Map.get(opts, key, false), else: Keyword.get(opts, key, false)
    end

    Enum.reduce([nx: @nx, xx: @xx, gt: @gt, lt: @lt], 0, fn {key, bit}, acc ->
      if get.(key), do: Bitwise.bor(acc, bit), else: acc
    end)
  end

  # --- Writes (apply only) ---

  @doc """
  Applies tree ops in order as one frame, all or nothing. Ops:
  `{:add, member, score, flags}`, `{:incr, member, delta, flags}`,
  `{:remove, member}`, `{:pop_min, n}`, `{:pop_max, n}`.
  """
  @spec apply_ops(binary(), token(), [tuple()]) :: {:ok, [term()]} | {:error, term()}
  def apply_ops(_path, _token, []), do: {:ok, []}

  def apply_ops(path, {index, ordinal}, ops) do
    with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(path)) do
      NIF.zset_tree_apply(path, index, ordinal, ops)
    end
  end

  @doc "Unconditional score puts, from compound `{compound_key, score_string, _exp}` entries."
  @spec put_ops(binary(), [{binary(), binary(), term()}]) :: {:ok, [tuple()]} | {:error, term()}
  def put_ops(redis_key, entries) do
    Enum.reduce_while(entries, {:ok, []}, fn {compound_key, value, _expire_at_ms}, {:ok, acc} ->
      with {:ok, member} <- member(redis_key, compound_key),
           {:ok, score} <- parse_score(value) do
        {:cont, {:ok, [{:add, member, score, 0} | acc]}}
      else
        :error -> {:halt, {:error, {:invalid_zset_tree_member_write, compound_key}}}
      end
    end)
    |> case do
      {:ok, ops} -> {:ok, Enum.reverse(ops)}
      error -> error
    end
  end

  @spec remove_ops(binary(), [binary()]) :: {:ok, [tuple()]} | {:error, term()}
  def remove_ops(redis_key, compound_keys) do
    Enum.reduce_while(compound_keys, {:ok, []}, fn compound_key, {:ok, acc} ->
      case member(redis_key, compound_key) do
        {:ok, member} -> {:cont, {:ok, [{:remove, member} | acc]}}
        :error -> {:halt, {:error, {:invalid_zset_tree_member_delete, compound_key}}}
      end
    end)
    |> case do
      {:ok, ops} -> {:ok, Enum.reverse(ops)}
      error -> error
    end
  end

  # --- Reads ---

  @spec card(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def card(path), do: NIF.zset_tree_card(path)

  @doc "A member's stored score string, or nil."
  @spec score(binary(), binary()) :: {:ok, binary() | nil} | {:error, term()}
  def score(path, member) do
    case NIF.zset_tree_scores(path, [member]) do
      {:ok, [nil]} -> {:ok, nil}
      {:ok, [score]} -> {:ok, score_string(score)}
      {:error, _reason} = error -> error
    end
  end

  @spec scores(binary(), [binary()]) :: {:ok, [binary() | nil]} | {:error, term()}
  def scores(path, members) do
    with {:ok, scores} <- NIF.zset_tree_scores(path, members) do
      {:ok, Enum.map(scores, &(&1 && score_string(&1)))}
    end
  end

  @doc "Every member with its score string, in member order."
  @spec members(binary()) :: {:ok, [{binary(), binary()}]} | {:error, term()}
  def members(path) do
    with {:ok, entries} <- NIF.zset_tree_range_by_lex(path, :min, :max, false, 0, nil) do
      {:ok, Enum.map(entries, fn {member, score} -> {member, score_string(score)} end)}
    end
  end

  @doc "Members at positions `[start, start + count)` in member order."
  @spec member_slice(binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def member_slice(path, start, count) do
    with {:ok, entries} <- NIF.zset_tree_range_by_lex(path, :min, :max, false, start, count) do
      {:ok, Enum.map(entries, fn {member, score} -> {member, score_string(score)} end)}
    end
  end

  @spec scan(binary(), binary() | nil, pos_integer()) ::
          {:ok, {[{binary(), binary()}], binary() | nil}} | {:error, term()}
  def scan(path, after_member, count) do
    with {:ok, {entries, next}} <- NIF.zset_tree_scan(path, after_member, count) do
      {:ok, {Enum.map(entries, fn {member, score} -> {member, score_string(score)} end), next}}
    end
  end

  @doc """
  One ZSCAN page in the compound member index's terms: `count` members
  inspected after the cursor (`0` or `{:after, member}`), those matching
  `match_pattern` returned, with scores unless `fields_only`.
  """
  @spec scan_page(binary(), 0 | {:after, binary()}, pos_integer(), binary() | nil, boolean()) ::
          {:ok, {0 | {:after, binary()}, [{binary(), binary() | nil}]}} | {:error, term()}
  def scan_page(path, cursor, count, match_pattern, fields_only) do
    after_member =
      case cursor do
        0 -> nil
        {:after, member} -> member
      end

    with {:ok, {entries, next}} <- scan(path, after_member, count) do
      page =
        for {member, score} <- entries,
            match_pattern == nil or Ferricstore.GlobMatcher.match?(member, match_pattern),
            do: {member, if(fields_only, do: nil, else: score)}

      {:ok, {if(next, do: {:after, next}, else: 0), page}}
    end
  end

  defp nif_bound(:neg_inf), do: :neg_inf
  defp nif_bound(:inf), do: :inf
  defp nif_bound({:inclusive, score}), do: {:incl, score * 1.0}
  defp nif_bound({:exclusive, score}), do: {:excl, score * 1.0}

  @spec score_range_slice(
          binary(),
          bound(),
          bound(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) ::
          {:ok, [{binary(), float()}]} | {:error, term()}
  def score_range_slice(path, min_bound, max_bound, reverse?, offset, count) do
    limit = if count == :all, do: nil, else: count

    NIF.zset_tree_range_by_score(
      path,
      nif_bound(min_bound),
      nif_bound(max_bound),
      reverse?,
      offset,
      limit
    )
  end

  @spec score_count(binary(), bound(), bound()) :: {:ok, non_neg_integer()} | {:error, term()}
  def score_count(path, min_bound, max_bound),
    do: NIF.zset_tree_count_by_score(path, nif_bound(min_bound), nif_bound(max_bound))

  @doc "Members at positions `start..stop` (non-negative, inclusive), from the top when reversed."
  @spec rank_range(binary(), non_neg_integer(), non_neg_integer(), boolean()) ::
          {:ok, [{binary(), float()}]} | {:error, term()}
  def rank_range(_path, start, stop, _reverse?) when start > stop, do: {:ok, []}

  def rank_range(path, start, stop, reverse?),
    do: NIF.zset_tree_range_by_index(path, start, stop, reverse?)

  @spec member_rank(binary(), binary(), boolean()) ::
          {:ok, non_neg_integer() | nil} | {:error, term()}
  def member_rank(path, member, reverse?), do: NIF.zset_tree_rank(path, member, reverse?)

  # --- Maintenance ---

  @spec recover(binary()) :: {:ok, term()} | {:error, term()}
  def recover(path), do: NIF.zset_tree_recover(path)

  @spec sync(binary()) :: :ok | {:error, term()}
  def sync(path), do: NIF.zset_tree_sync(path)

  @spec compact(binary(), non_neg_integer()) :: {:ok, term()} | {:error, term()}
  def compact(path, floor_index), do: NIF.zset_tree_compact(path, floor_index)
end
