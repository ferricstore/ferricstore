defmodule FerricStore.API.SortedSets do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Commands.SortedSet
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
  Adds members with scores to the sorted set stored at `key`.

  `score_member_pairs` is a list of `{score, member}` tuples where `score` is
  a number and `member` is a binary string. If a member already exists, its
  score is updated. Returns the count of new members added (not counting
  score updates).

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      {:ok, 2}

      iex> FerricStore.zadd("leaderboard", [{150.0, "alice"}, {300.0, "charlie"}])
      {:ok, 1}

  """
  @spec zadd(key(), [{number(), binary()}]) :: {:ok, non_neg_integer()}
  def zadd(key, score_member_pairs) when is_list(score_member_pairs) do
    pairs = Enum.map(score_member_pairs, fn {score, member} -> {score * 1.0, member} end)

    case SortedSet.handle_ast({:zadd, key, [], pairs}, default_ctx()) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Returns members in the sorted set stored at `key` within the rank range `start..stop`.

  Indices are zero-based and inclusive. Negative indices count from the end
  (-1 is the last element). Members are ordered by score ascending.

  ## Options

    * `:withscores` - When `true`, returns `{member, score}` tuples instead
      of bare member strings. Defaults to `false`.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrange("leaderboard", 0, -1)
      {:ok, ["alice", "bob", "charlie"]}

      iex> FerricStore.zrange("leaderboard", 0, 1, withscores: true)
      {:ok, [{"alice", 100.0}, {"bob", 200.0}]}

      iex> FerricStore.zrange("nonexistent", 0, -1)
      {:ok, []}

  """
  @spec zrange(key(), integer(), integer(), zrange_opts()) ::
          {:ok, [binary() | {binary(), float()}]}
  def zrange(key, start, stop, opts \\ []) do
    _ctx = default_ctx()
    store = build_compound_store(key)
    with_scores = Keyword.get(opts, :withscores, false)

    case SortedSet.handle_ast({:zrange, key, start, stop, with_scores}, store) do
      {:error, _} = err ->
        err

      result when with_scores and is_list(result) and result != [] ->
        pairs =
          result
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)

        {:ok, pairs}

      result ->
        {:ok, result}
    end
  end

  @doc """
  Returns the score of `member` in the sorted set stored at `key`.

  Returns `{:ok, score}` if the member exists, or `{:ok, nil}` if the member
  or the key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zscore("leaderboard", "alice")
      {:ok, 100.0}

      iex> FerricStore.zscore("leaderboard", "unknown")
      {:ok, nil}

  """
  @spec zscore(key(), binary()) :: {:ok, float() | nil}
  def zscore(key, member) do
    store = build_compound_store(key)

    case SortedSet.handle_ast({:zscore, key, member}, store) do
      {:error, _} = err ->
        err

      nil ->
        {:ok, nil}

      score_str when is_binary(score_str) ->
        {score, _} = Float.parse(score_str)
        {:ok, score}
    end
  end

  @doc """
  Returns the number of members in the sorted set stored at `key`.

  Returns `{:ok, 0}` if the key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zcard("leaderboard")
      {:ok, 2}

      iex> FerricStore.zcard("nonexistent")
      {:ok, 0}

  """
  @spec zcard(key()) :: {:ok, non_neg_integer()}
  def zcard(key) do
    store = build_compound_store(key)

    case SortedSet.handle_ast({:zcard, key}, store) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  @doc """
  Removes one or more members from the sorted set stored at `key`.

  Members that do not exist are ignored. Returns the count of members
  actually removed.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zrem("leaderboard", ["alice"])
      {:ok, 1}

      iex> FerricStore.zrem("leaderboard", ["nonexistent"])
      {:ok, 0}

  """
  @spec zrem(key(), [binary()]) :: {:ok, non_neg_integer()}
  def zrem(key, members) when is_list(members) do
    case SortedSet.handle_ast({:zrem, [key | members]}, default_ctx()) do
      {:error, _} = err -> err
      result -> {:ok, result}
    end
  end

  # ---------------------------------------------------------------------------
  # Native Commands
  # ---------------------------------------------------------------------------

  @doc """
  Returns the rank of `member` in the sorted set at `key` (ascending score order).

  Rank is 0-based (the member with the lowest score has rank 0). Returns
  `{:ok, nil}` if the member or key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrank("leaderboard", "alice")
      {:ok, 0}

      iex> FerricStore.zrank("leaderboard", "charlie")
      {:ok, 2}

      iex> FerricStore.zrank("leaderboard", "unknown")
      {:ok, nil}

  """
  @spec zrank(key(), binary()) :: {:ok, non_neg_integer() | nil}
  def zrank(key, member) do
    store = build_compound_store(key)
    result = SortedSet.handle_ast({:zrank, key, member}, store)
    wrap_result(result)
  end

  @doc """
  Returns the reverse rank of `member` in the sorted set at `key` (descending score order).

  Rank is 0-based (the member with the highest score has rank 0). Returns
  `{:ok, nil}` if the member or key does not exist.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrevrank("leaderboard", "charlie")
      {:ok, 0}

      iex> FerricStore.zrevrank("leaderboard", "alice")
      {:ok, 2}

      iex> FerricStore.zrevrank("leaderboard", "unknown")
      {:ok, nil}

  """
  @spec zrevrank(key(), binary()) :: {:ok, non_neg_integer() | nil}
  def zrevrank(key, member) do
    store = build_compound_store(key)
    result = SortedSet.handle_ast({:zrevrank, key, member}, store)
    wrap_result(result)
  end

  @doc """
  Returns members with scores between `min` and `max` (inclusive by default).

  Use "-inf" and "+inf" for unbounded ranges. Prefix a bound with "(" for
  exclusive (e.g., "(100" means score > 100).

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zrangebyscore("leaderboard", "100", "200")
      {:ok, ["alice", "bob"]}

      iex> FerricStore.zrangebyscore("leaderboard", "-inf", "+inf")
      {:ok, ["alice", "bob", "charlie"]}

      iex> FerricStore.zrangebyscore("leaderboard", "(200", "+inf")
      {:ok, ["charlie"]}

  """
  @spec zrangebyscore(key(), binary(), binary(), keyword()) :: {:ok, [binary()]}
  def zrangebyscore(key, min, max, _opts \\ []) do
    store = build_compound_store(key)

    result =
      SortedSet.handle_ast({:zrangebyscore, key, parse_zbound(min), parse_zbound(max), []}, store)

    wrap_result(result)
  end

  @doc """
  Counts members in the sorted set at `key` with scores between `min` and `max`.

  Use "-inf" and "+inf" for unbounded ranges. Prefix a bound with "(" for
  exclusive.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zcount("leaderboard", "100", "200")
      {:ok, 2}

      iex> FerricStore.zcount("leaderboard", "-inf", "+inf")
      {:ok, 3}

  """
  @spec zcount(key(), binary(), binary()) :: {:ok, non_neg_integer()}
  def zcount(key, min, max) do
    store = build_compound_store(key)
    result = SortedSet.handle_ast({:zcount, key, parse_zbound(min), parse_zbound(max)}, store)
    wrap_result(result)
  end

  @doc """
  Increments the score of `member` in the sorted set at `key` by `increment`.

  Creates the member with the given increment as score if it does not exist.
  Returns the new score as a string.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}])
      iex> FerricStore.zincrby("leaderboard", 50.0, "alice")
      {:ok, "150.0"}

      iex> FerricStore.zincrby("leaderboard", 25.0, "newcomer")
      {:ok, "25.0"}

  """
  @spec zincrby(key(), number(), binary()) :: {:ok, binary()} | {:error, binary()}
  def zincrby(key, increment, member) do
    wrap_result(Router.zincrby(default_ctx(), key, increment * 1.0, member))
  end

  @doc """
  Returns one or more random members from the sorted set at `key`.

  Without `count`, returns a single member or `nil` for empty/nonexistent keys.
  With positive `count`, returns up to `count` unique members. With negative
  `count`, returns `abs(count)` members with possible duplicates.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> {:ok, member} = FerricStore.zrandmember("leaderboard")
      iex> member in ["alice", "bob", "charlie"]
      true

      iex> {:ok, members} = FerricStore.zrandmember("leaderboard", 2)
      iex> length(members)
      2

      iex> FerricStore.zrandmember("nonexistent")
      {:ok, nil}

  """
  @spec zrandmember(key(), integer() | nil) :: {:ok, binary() | [binary()] | nil}
  def zrandmember(key, count \\ nil) do
    store = build_compound_store(key)

    case count do
      nil ->
        result = SortedSet.handle_ast({:zrandmember, key}, store)
        wrap_result(result)

      n ->
        result = SortedSet.handle_ast({:zrandmember, key, n, false}, store)
        wrap_result(result)
    end
  end

  @doc """
  Removes and returns up to `count` members with the lowest scores.

  Returns `{:ok, []}` if the key does not exist or the sorted set is empty.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zpopmin("leaderboard", 1)
      {:ok, [{"alice", 100.0}]}

      iex> FerricStore.zpopmin("leaderboard", 2)
      {:ok, [{"bob", 200.0}, {"charlie", 300.0}]}

  """
  @spec zpopmin(key(), pos_integer()) :: {:ok, [{binary(), float()}]}
  def zpopmin(key, count \\ 1) do
    result = Router.zpopmin(default_ctx(), key, count)

    case result do
      {:error, _} = err ->
        err

      flat when is_list(flat) ->
        pairs =
          flat
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)

        {:ok, pairs}
    end
  end

  @doc """
  Removes and returns up to `count` members with the highest scores.

  Returns `{:ok, []}` if the key does not exist or the sorted set is empty.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}, {300.0, "charlie"}])
      iex> FerricStore.zpopmax("leaderboard", 1)
      {:ok, [{"charlie", 300.0}]}

      iex> FerricStore.zpopmax("leaderboard", 2)
      {:ok, [{"bob", 200.0}, {"alice", 100.0}]}

  """
  @spec zpopmax(key(), pos_integer()) :: {:ok, [{binary(), float()}]}
  def zpopmax(key, count \\ 1) do
    result = Router.zpopmax(default_ctx(), key, count)

    case result do
      {:error, _} = err ->
        err

      flat when is_list(flat) ->
        pairs =
          flat
          |> Enum.chunk_every(2)
          |> Enum.map(fn [member, score_str] ->
            {score, _} = Float.parse(score_str)
            {member, score}
          end)

        {:ok, pairs}
    end
  end

  @doc """
  Returns scores for multiple members in the sorted set at `key`.

  Returns `nil` for members that do not exist. The order of returned scores
  matches the order of the input members.

  ## Examples

      iex> FerricStore.zadd("leaderboard", [{100.0, "alice"}, {200.0, "bob"}])
      iex> FerricStore.zmscore("leaderboard", ["alice", "unknown", "bob"])
      {:ok, [100.0, nil, 200.0]}

  """
  @spec zmscore(key(), [binary()]) :: {:ok, [float() | nil]}
  def zmscore(key, members) when is_list(members) do
    store = build_compound_store(key)
    result = SortedSet.handle_ast({:zmscore, [key | members]}, store)

    case result do
      {:error, _} = err ->
        err

      scores when is_list(scores) ->
        parsed =
          Enum.map(scores, fn
            nil ->
              nil

            score_str when is_binary(score_str) ->
              {score, _} = Float.parse(score_str)
              score
          end)

        {:ok, parsed}
    end
  end

  @doc """
  Stores in `destination` the union of the sorted sets (or sets, whose
  members score 1) at `keys`, replacing whatever it held; an empty result
  deletes it. Returns the number of members stored.

  ## Options

    * `:weights` - one multiplier per key for its scores.
    * `:aggregate` - `:sum` (default), `:min` or `:max`: how a member's
      scores from several keys combine.

  ## Examples

      iex> FerricStore.zadd("{lb}:a", [{1, "x"}, {2, "y"}])
      iex> FerricStore.zadd("{lb}:b", [{3, "y"}])
      iex> FerricStore.zunionstore("{lb}:all", ["{lb}:a", "{lb}:b"])
      {:ok, 2}

  """
  @spec zunionstore(key(), [key()], keyword()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def zunionstore(destination, keys, opts \\ []) when is_list(keys),
    do: store_command("ZUNIONSTORE", destination, keys, combine_args(opts))

  @doc """
  Stores in `destination` the members present in every key, with their
  scores combined (`:weights`, `:aggregate`, as in `zunionstore/3`).
  """
  @spec zinterstore(key(), [key()], keyword()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def zinterstore(destination, keys, opts \\ []) when is_list(keys),
    do: store_command("ZINTERSTORE", destination, keys, combine_args(opts))

  @doc """
  Stores in `destination` the members of the first key that no other key
  holds, with their scores.
  """
  @spec zdiffstore(key(), [key()]) :: {:ok, non_neg_integer()} | {:error, binary()}
  def zdiffstore(destination, keys) when is_list(keys),
    do: store_command("ZDIFFSTORE", destination, keys, [])

  @doc """
  Returns the union of the sorted sets at `keys`, ordered by score.
  Takes `zunionstore/3`'s options and `:withscores` (`{member, score}`
  tuples).
  """
  @spec zunion([key()], keyword()) :: {:ok, [binary() | {binary(), float()}]} | {:error, binary()}
  def zunion(keys, opts \\ []) when is_list(keys),
    do: read_command("ZUNION", keys, combine_args(opts), opts)

  @doc "Returns the members present in every key; options as `zunion/2`."
  @spec zinter([key()], keyword()) :: {:ok, [binary() | {binary(), float()}]} | {:error, binary()}
  def zinter(keys, opts \\ []) when is_list(keys),
    do: read_command("ZINTER", keys, combine_args(opts), opts)

  @doc "Returns the first key's members no other key holds; takes `:withscores`."
  @spec zdiff([key()], keyword()) :: {:ok, [binary() | {binary(), float()}]} | {:error, binary()}
  def zdiff(keys, opts \\ []) when is_list(keys), do: read_command("ZDIFF", keys, [], opts)

  @doc """
  Counts the members present in every key, stopping at `:limit` when given
  (0, the default, counts them all).
  """
  @spec zintercard([key()], keyword()) :: {:ok, non_neg_integer()} | {:error, binary()}
  def zintercard(keys, opts \\ []) when is_list(keys) do
    limit = Keyword.get(opts, :limit, 0)
    args = [Integer.to_string(length(keys)) | keys] ++ ["LIMIT", Integer.to_string(limit)]
    wrap(SortedSet.handle("ZINTERCARD", args, default_ctx()))
  end

  @doc """
  Stores in `destination` the members of `source` in a range, as `ZRANGE`
  selects them: by rank (default), or with `by: :score` or `by: :lex` by
  score or member bounds given as Redis writes them (`"(1"`, `"+inf"`,
  `"[a"`). `rev: true` reverses the order and `limit: {offset, count}`
  pages a score or member range. Returns the number of members stored.
  """
  @spec zrangestore(key(), key(), term(), term(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  def zrangestore(destination, source, start, stop, opts \\ []) do
    by =
      case Keyword.get(opts, :by) do
        :score -> ["BYSCORE"]
        :lex -> ["BYLEX"]
        _rank -> []
      end

    rev = if Keyword.get(opts, :rev, false), do: ["REV"], else: []

    limit =
      case Keyword.get(opts, :limit) do
        {offset, count} -> ["LIMIT", to_string(offset), to_string(count)]
        nil -> []
      end

    args = [destination, source, to_string(start), to_string(stop)] ++ by ++ rev ++ limit
    wrap(SortedSet.handle("ZRANGESTORE", args, default_ctx()))
  end

  defp combine_args(opts) do
    weights =
      case Keyword.get(opts, :weights) do
        nil -> []
        weights -> ["WEIGHTS" | Enum.map(weights, &format_number/1)]
      end

    aggregate =
      case Keyword.get(opts, :aggregate) do
        nil ->
          []

        mode when mode in [:sum, :min, :max] ->
          ["AGGREGATE", mode |> Atom.to_string() |> String.upcase()]
      end

    weights ++ aggregate
  end

  defp format_number(number) when is_integer(number), do: Integer.to_string(number)
  defp format_number(number) when is_float(number), do: Float.to_string(number)

  # The instance as the store: a destination that is also a source then runs
  # as one transaction entry, as on the gateways.
  defp store_command(command, destination, keys, options) do
    args = [destination, Integer.to_string(length(keys)) | keys] ++ options
    wrap(SortedSet.handle(command, args, default_ctx()))
  end

  defp read_command(command, keys, options, opts) do
    with_scores = Keyword.get(opts, :withscores, false)
    scores = if with_scores, do: ["WITHSCORES"], else: []
    args = [Integer.to_string(length(keys)) | keys] ++ options ++ scores

    case SortedSet.handle(command, args, default_ctx()) do
      {:error, _reason} = error ->
        error

      flat when with_scores ->
        {:ok,
         flat
         |> Enum.chunk_every(2)
         |> Enum.map(fn [member, score] ->
           {value, _rest} = Float.parse(score)
           {member, value}
         end)}

      members ->
        {:ok, members}
    end
  end

  defp wrap({:error, _reason} = error), do: error
  defp wrap(result), do: {:ok, result}

  # ---------------------------------------------------------------------------
  # Streams
  # ---------------------------------------------------------------------------
end
