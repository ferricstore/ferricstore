defmodule Ferricstore.Commands.SortedSet do
  @moduledoc """
  Handles Redis sorted set commands: ZADD, ZSCORE, ZRANK, ZRANGE, ZCARD,
  ZREM, ZINCRBY, ZCOUNT, ZPOPMIN, ZPOPMAX, ZRANGEBYSCORE, ZREVRANGE,
  ZSCAN, ZRANDMEMBER, ZMSCORE.

  Each sorted set member is stored as a compound key:

      Z:redis_key\\0member -> score_string

  The score is stored as a string representation of a float64. This allows
  O(1) score lookups by member. For range queries, all members are loaded
  and sorted in memory -- acceptable for typical sorted set sizes in cache
  workloads.

  ## Type Enforcement

  All sorted set commands check type metadata. Using sorted set commands on
  a key that holds a different type returns WRONGTYPE.
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @doc """
  Handles a sorted set command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"ZADD"`, `"ZRANGE"`)
    - `args` - List of string arguments
    - `store` - Injected store map with compound key callbacks

  ## Returns

  Plain Elixir term: integer, float, string, list, nil, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
  # ---------------------------------------------------------------------------

  def handle("ZADD", [key | rest], store) when rest != [] do
    with {:ok, opts, score_member_pairs} <- parse_zadd_opts(rest),
         :ok <- TypeRegistry.check_or_set(key, :zset, store) do
      unique_members =
        score_member_pairs |> Enum.map(fn {_score, member} -> member end) |> Enum.uniq()

      compound_keys = Enum.map(unique_members, &CompoundKey.zset_member(key, &1))

      current_by_member =
        store
        |> Ops.compound_batch_get(key, compound_keys)
        |> then(&Enum.zip(unique_members, &1))
        |> Map.new()

      {added, changed, _current_by_member, writes_by_member} =
        Enum.reduce(score_member_pairs, {0, 0, current_by_member, %{}}, fn {score, member},
                                                                           {add_acc, ch_acc,
                                                                            current_acc,
                                                                            writes_acc} ->
          existing = Map.get(current_acc, member)
          score_str = Float.to_string(score)

          cond do
            # NX: only add new elements, don't update existing
            opts.nx and existing != nil ->
              {add_acc, ch_acc, current_acc, writes_acc}

            # XX: only update existing elements, don't add new
            opts.xx and existing == nil ->
              {add_acc, ch_acc, current_acc, writes_acc}

            existing == nil ->
              {add_acc + 1, ch_acc, Map.put(current_acc, member, score_str),
               Map.put(writes_acc, member, score_str)}

            true ->
              existing_score =
                case Float.parse(existing) do
                  {score, ""} -> score
                  _ -> 0.0
                end

              should_update =
                cond do
                  opts.gt -> score > existing_score
                  opts.lt -> score < existing_score
                  true -> true
                end

              if should_update and score != existing_score do
                {add_acc, ch_acc + 1, Map.put(current_acc, member, score_str),
                 Map.put(writes_acc, member, score_str)}
              else
                {add_acc, ch_acc, current_acc, writes_acc}
              end
          end
        end)

      Enum.each(Enum.zip(unique_members, compound_keys), fn {member, compound_key} ->
        case Map.fetch(writes_by_member, member) do
          {:ok, score_str} -> Ops.compound_put(store, key, compound_key, score_str, 0)
          :error -> :ok
        end
      end)

      if opts.ch, do: added + changed, else: added
    end
  end

  def handle("ZADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zadd' command"}
  end

  # ---------------------------------------------------------------------------
  # ZSCORE key member
  # ---------------------------------------------------------------------------

  def handle("ZSCORE", [key, member], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_key = CompoundKey.zset_member(key, member)

      case Ops.compound_get(store, key, compound_key) do
        nil -> nil
        score_str -> format_score_str(score_str)
      end
    end
  end

  def handle("ZSCORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zscore' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANK key member
  # ---------------------------------------------------------------------------

  def handle("ZRANK", [key, member], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      sorted = load_sorted_members(key, store)

      case Enum.find_index(sorted, fn {m, _s} -> m == member end) do
        nil -> nil
        idx -> idx
      end
    end
  end

  def handle("ZRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrank' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANGE key start stop [WITHSCORES]
  # ---------------------------------------------------------------------------

  def handle("ZRANGE", [key, start_str, stop_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {Integer.parse(start_str), Integer.parse(stop_str)} do
        {{start, ""}, {stop, ""}} ->
          sorted = load_sorted_members(key, store)
          len = length(sorted)
          start_idx = normalize_index(start, len)
          stop_idx = normalize_index(stop, len)
          with_scores = "WITHSCORES" in opts

          if start_idx > stop_idx or start_idx >= len do
            []
          else
            sliced = Enum.slice(sorted, start_idx..stop_idx)

            if with_scores do
              Enum.flat_map(sliced, fn {member, score} ->
                [member, format_score(score)]
              end)
            else
              Enum.map(sliced, fn {member, _score} -> member end)
            end
          end

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrange' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGE key start stop [WITHSCORES]
  # ---------------------------------------------------------------------------

  def handle("ZREVRANGE", [key, start_str, stop_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {Integer.parse(start_str), Integer.parse(stop_str)} do
        {{start, ""}, {stop, ""}} ->
          sorted = load_sorted_members(key, store) |> Enum.reverse()
          len = length(sorted)
          start_idx = normalize_index(start, len)
          stop_idx = normalize_index(stop, len)
          with_scores = "WITHSCORES" in opts

          if start_idx > stop_idx or start_idx >= len do
            []
          else
            sliced = Enum.slice(sorted, start_idx..stop_idx)

            if with_scores do
              Enum.flat_map(sliced, fn {member, score} ->
                [member, format_score(score)]
              end)
            else
              Enum.map(sliced, fn {member, _score} -> member end)
            end
          end

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZREVRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrange' command"}
  end

  # ---------------------------------------------------------------------------
  # ZCARD key
  # ---------------------------------------------------------------------------

  def handle("ZCARD", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      prefix = CompoundKey.zset_prefix(key)
      Ops.compound_count(store, key, prefix)
    end
  end

  def handle("ZCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zcard' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREM key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("ZREM", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_keys =
        members
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.zset_member(key, &1))

      removed_keys =
        store
        |> Ops.compound_batch_get(key, compound_keys)
        |> Enum.zip(compound_keys)
        |> Enum.flat_map(fn
          {nil, _compound_key} -> []
          {_value, compound_key} -> [compound_key]
        end)

      Enum.each(removed_keys, &Ops.compound_delete(store, key, &1))

      removed = length(removed_keys)

      if removed > 0 do
        prefix = CompoundKey.zset_prefix(key)

        if Ops.compound_count(store, key, prefix) == 0 do
          TypeRegistry.delete_type(key, store)
        end
      end

      removed
    end
  end

  def handle("ZREM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrem' command"}
  end

  # ---------------------------------------------------------------------------
  # ZINCRBY key increment member
  # ---------------------------------------------------------------------------

  def handle("ZINCRBY", [key, increment_str, member], store) do
    with :ok <- TypeRegistry.check_or_set(key, :zset, store) do
      case Float.parse(increment_str) do
        {increment, ""} ->
          compound_key = CompoundKey.zset_member(key, member)
          existing = Ops.compound_get(store, key, compound_key)

          current_score =
            case existing do
              nil ->
                0.0

              score_str ->
                case Float.parse(score_str) do
                  {score, ""} -> score
                  _ -> 0.0
                end
            end

          new_score = current_score + increment
          Ops.compound_put(store, key, compound_key, Float.to_string(new_score), 0)
          format_score(new_score)

        :error ->
          # Try integer parse
          case Integer.parse(increment_str) do
            {increment, ""} ->
              compound_key = CompoundKey.zset_member(key, member)
              existing = Ops.compound_get(store, key, compound_key)

              current_score =
                case existing do
                  nil ->
                    0.0

                  score_str ->
                    case Float.parse(score_str) do
                      {score, ""} -> score
                      _ -> 0.0
                    end
                end

              new_score = current_score + increment * 1.0
              Ops.compound_put(store, key, compound_key, Float.to_string(new_score), 0)
              format_score(new_score)

            _ ->
              {:error, "ERR value is not a valid float"}
          end
      end
    end
  end

  def handle("ZINCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zincrby' command"}
  end

  # ---------------------------------------------------------------------------
  # ZCOUNT key min max
  # ---------------------------------------------------------------------------

  def handle("ZCOUNT", [key, min_str, max_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {parse_score_bound(min_str), parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          sorted = load_sorted_members(key, store)

          Enum.count(sorted, fn {_member, score} ->
            above_min = score_gte?(score, min_val, min_excl)
            below_max = score_lte?(score, max_val, max_excl)
            above_min and below_max
          end)

        _ ->
          {:error, "ERR min or max is not a float"}
      end
    end
  end

  def handle("ZCOUNT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zcount' command"}
  end

  # ---------------------------------------------------------------------------
  # ZPOPMIN key [count]
  # ---------------------------------------------------------------------------

  def handle("ZPOPMIN", [key], store) do
    handle("ZPOPMIN", [key, "1"], store)
  end

  def handle("ZPOPMIN", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          sorted = load_sorted_members(key, store)
          to_pop = Enum.take(sorted, count)

          result =
            Enum.flat_map(to_pop, fn {member, score} ->
              compound_key = CompoundKey.zset_member(key, member)
              Ops.compound_delete(store, key, compound_key)
              [member, format_score(score)]
            end)

          if to_pop != [] do
            prefix = CompoundKey.zset_prefix(key)

            if Ops.compound_count(store, key, prefix) == 0 do
              TypeRegistry.delete_type(key, store)
            end
          end

          result

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZPOPMIN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zpopmin' command"}
  end

  # ---------------------------------------------------------------------------
  # ZPOPMAX key [count]
  # ---------------------------------------------------------------------------

  def handle("ZPOPMAX", [key], store) do
    handle("ZPOPMAX", [key, "1"], store)
  end

  def handle("ZPOPMAX", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          sorted = load_sorted_members(key, store) |> Enum.reverse()
          to_pop = Enum.take(sorted, count)

          result =
            Enum.flat_map(to_pop, fn {member, score} ->
              compound_key = CompoundKey.zset_member(key, member)
              Ops.compound_delete(store, key, compound_key)
              [member, format_score(score)]
            end)

          if to_pop != [] do
            prefix = CompoundKey.zset_prefix(key)

            if Ops.compound_count(store, key, prefix) == 0 do
              TypeRegistry.delete_type(key, store)
            end
          end

          result

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZPOPMAX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zpopmax' command"}
  end

  # ---------------------------------------------------------------------------
  # ZSCAN key cursor [MATCH pattern] [COUNT count]
  # ---------------------------------------------------------------------------

  def handle("ZSCAN", [key, cursor_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store),
         {:ok, cursor} <- parse_cursor(cursor_str),
         {:ok, match_pattern, count} <- parse_zscan_opts(opts) do
      prefix = CompoundKey.zset_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)

      filtered =
        case match_pattern do
          nil ->
            pairs

          pattern ->
            Enum.filter(pairs, fn {member, _score} ->
              Ferricstore.GlobMatcher.match?(member, pattern)
            end)
        end

      {next_cursor, batch} = paginate(filtered, cursor, count)
      elements = Enum.flat_map(batch, fn {member, score} -> [member, format_score_str(score)] end)
      [next_cursor, elements]
    end
  end

  def handle("ZSCAN", [_key], _store) do
    {:error, "ERR wrong number of arguments for 'zscan' command"}
  end

  def handle("ZSCAN", [], _store) do
    {:error, "ERR wrong number of arguments for 'zscan' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANDMEMBER key [count [WITHSCORES]]
  # ---------------------------------------------------------------------------

  def handle("ZRANDMEMBER", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      prefix = CompoundKey.zset_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)

      case pairs do
        [] ->
          nil

        _ ->
          {member, _score} = Enum.random(pairs)
          member
      end
    end
  end

  def handle("ZRANDMEMBER", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Integer.parse(count_str) do
        {count, ""} ->
          prefix = CompoundKey.zset_prefix(key)
          pairs = Ops.compound_scan(store, key, prefix)
          select_random_zset_members(pairs, count, false)

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZRANDMEMBER", [key, count_str, withscores_str], store) do
    if String.upcase(withscores_str) != "WITHSCORES" do
      {:error, "ERR syntax error"}
    else
      with :ok <- TypeRegistry.check_type(key, :zset, store) do
        case Integer.parse(count_str) do
          {count, ""} ->
            prefix = CompoundKey.zset_prefix(key)
            pairs = Ops.compound_scan(store, key, prefix)
            select_random_zset_members(pairs, count, true)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end
      end
    end
  end

  def handle("ZRANDMEMBER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrandmember' command"}
  end

  # ---------------------------------------------------------------------------
  # ZMSCORE key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("ZMSCORE", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_keys = Enum.map(members, &CompoundKey.zset_member(key, &1))

      store
      |> Ops.compound_batch_get(key, compound_keys)
      |> Enum.map(fn
        nil -> nil
        score_str -> format_score_str(score_str)
      end)
    end
  end

  def handle("ZMSCORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zmscore' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
  # ---------------------------------------------------------------------------

  def handle("ZRANGEBYSCORE", [key, min_str, max_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {parse_score_bound(min_str), parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          case parse_range_by_score_opts(opts) do
            {:error, _} = err ->
              err

            {with_scores, offset, count} ->
              sorted = load_sorted_members(key, store)

              filtered =
                Enum.filter(sorted, fn {_member, score} ->
                  score_gte?(score, min_val, min_excl) and score_lte?(score, max_val, max_excl)
                end)

              paginated = apply_limit(filtered, offset, count)

              if with_scores do
                Enum.flat_map(paginated, fn {member, score} -> [member, format_score(score)] end)
              else
                Enum.map(paginated, fn {member, _score} -> member end)
              end
          end

        _ ->
          {:error, "ERR min or max is not a float"}
      end
    end
  end

  def handle("ZRANGEBYSCORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrangebyscore' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
  # ---------------------------------------------------------------------------

  def handle("ZREVRANGEBYSCORE", [key, max_str, min_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {parse_score_bound(min_str), parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          case parse_range_by_score_opts(opts) do
            {:error, _} = err ->
              err

            {with_scores, offset, count} ->
              sorted = load_sorted_members(key, store) |> Enum.reverse()

              filtered =
                Enum.filter(sorted, fn {_member, score} ->
                  score_gte?(score, min_val, min_excl) and score_lte?(score, max_val, max_excl)
                end)

              paginated = apply_limit(filtered, offset, count)

              if with_scores do
                Enum.flat_map(paginated, fn {member, score} -> [member, format_score(score)] end)
              else
                Enum.map(paginated, fn {member, _score} -> member end)
              end
          end

        _ ->
          {:error, "ERR min or max is not a float"}
      end
    end
  end

  def handle("ZREVRANGEBYSCORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrangebyscore' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREVRANK key member
  # ---------------------------------------------------------------------------

  def handle("ZREVRANK", [key, member], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      sorted = load_sorted_members(key, store)
      reversed = Enum.reverse(sorted)

      case Enum.find_index(reversed, fn {m, _s} -> m == member end) do
        nil -> nil
        idx -> idx
      end
    end
  end

  def handle("ZREVRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrank' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp load_sorted_members(key, store) do
    prefix = CompoundKey.zset_prefix(key)
    pairs = Ops.compound_scan(store, key, prefix)

    pairs
    |> Enum.map(fn {member, score_str} ->
      score =
        case Float.parse(score_str) do
          {score, ""} -> score
          _ -> 0.0
        end

      {member, score}
    end)
    |> Enum.sort_by(fn {member, score} -> {score, member} end)
  end

  defp normalize_index(index, len) when index < 0, do: max(0, len + index)
  defp normalize_index(index, _len), do: index

  defp format_score(score) when is_float(score) do
    # Redis returns scores as strings
    :erlang.float_to_binary(score, [:compact, decimals: 17])
  end

  # Parses a stored score string to float, then formats it consistently.
  # Ensures ZSCORE, ZMSCORE, and ZSCAN use the same format as ZRANGE WITHSCORES.
  defp format_score_str(score_str) do
    case Float.parse(score_str) do
      {score, ""} -> format_score(score)
      _ -> score_str
    end
  end

  # Parse ZADD options and score/member pairs
  defp parse_zadd_opts(args) do
    parse_zadd_opts(args, %{nx: false, xx: false, gt: false, lt: false, ch: false})
  end

  defp parse_zadd_opts(["NX" | rest], opts), do: parse_zadd_opts(rest, %{opts | nx: true})
  defp parse_zadd_opts(["XX" | rest], opts), do: parse_zadd_opts(rest, %{opts | xx: true})
  defp parse_zadd_opts(["GT" | rest], opts), do: parse_zadd_opts(rest, %{opts | gt: true})
  defp parse_zadd_opts(["LT" | rest], opts), do: parse_zadd_opts(rest, %{opts | lt: true})
  defp parse_zadd_opts(["CH" | rest], opts), do: parse_zadd_opts(rest, %{opts | ch: true})

  defp parse_zadd_opts(score_member_args, opts) do
    # Validate mutually exclusive flag combinations (Redis compat)
    cond do
      opts.nx and opts.xx ->
        {:error, "ERR XX and NX options at the same time are not compatible"}

      opts.gt and opts.lt ->
        {:error, "ERR GT, LT, and NX options at the same time are not compatible"}

      opts.gt and opts.nx ->
        {:error, "ERR GT, LT, and NX options at the same time are not compatible"}

      opts.lt and opts.nx ->
        {:error, "ERR GT, LT, and NX options at the same time are not compatible"}

      rem(length(score_member_args), 2) != 0 or score_member_args == [] ->
        {:error, "ERR wrong number of arguments for 'zadd' command"}

      true ->
        pairs =
          score_member_args
          |> Enum.chunk_every(2)
          |> Enum.reduce_while([], fn [score_str, member], acc ->
            case parse_score(score_str) do
              {:ok, score} -> {:cont, [{score, member} | acc]}
              :error -> {:halt, :error}
            end
          end)

        case pairs do
          :error -> {:error, "ERR value is not a valid float"}
          pairs -> {:ok, opts, Enum.reverse(pairs)}
        end
    end
  end

  defp parse_score(str) do
    case Float.parse(str) do
      {score, ""} ->
        {:ok, score}

      _ ->
        case Integer.parse(str) do
          {int, ""} -> {:ok, int * 1.0}
          _ -> :error
        end
    end
  end

  defp parse_score_bound("-inf"), do: {:ok, :neg_infinity, false}
  defp parse_score_bound("+inf"), do: {:ok, :infinity, false}
  defp parse_score_bound("inf"), do: {:ok, :infinity, false}

  defp parse_score_bound("(" <> rest) do
    case parse_score(rest) do
      {:ok, score} -> {:ok, score, true}
      :error -> :error
    end
  end

  defp parse_score_bound(str) do
    case parse_score(str) do
      {:ok, score} -> {:ok, score, false}
      :error -> :error
    end
  end

  # Score comparison helpers that handle :infinity and :neg_infinity atoms.
  defp score_gte?(_score, :neg_infinity, _exclusive), do: true
  defp score_gte?(_score, :infinity, _exclusive), do: false
  defp score_gte?(score, bound, true), do: score > bound
  defp score_gte?(score, bound, false), do: score >= bound

  defp score_lte?(_score, :infinity, _exclusive), do: true
  defp score_lte?(_score, :neg_infinity, _exclusive), do: false
  defp score_lte?(score, bound, true), do: score < bound
  defp score_lte?(score, bound, false), do: score <= bound

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE / ZREVRANGEBYSCORE option parsing
  # ---------------------------------------------------------------------------

  # Parses optional [WITHSCORES] [LIMIT offset count] from the trailing args.
  # Returns {with_scores, offset, count} where count == :all means no limit.
  defp parse_range_by_score_opts(opts) do
    do_parse_range_by_score_opts(opts, false, 0, :all)
  end

  defp do_parse_range_by_score_opts([], ws, offset, count), do: {ws, offset, count}

  defp do_parse_range_by_score_opts([opt | rest], ws, offset, count) do
    case String.upcase(opt) do
      "WITHSCORES" ->
        do_parse_range_by_score_opts(rest, true, offset, count)

      "LIMIT" ->
        case rest do
          [offset_str, count_str | remaining] ->
            with {off, ""} <- Integer.parse(offset_str),
                 {cnt, ""} <- Integer.parse(count_str) do
              if off < 0 do
                {:error, "ERR syntax error"}
              else
                # Redis: negative count means all remaining from offset
                real_count = if cnt < 0, do: :all, else: cnt
                do_parse_range_by_score_opts(remaining, ws, off, real_count)
              end
            else
              _ -> {:error, "ERR value is not an integer or out of range"}
            end

          _ ->
            {ws, offset, count}
        end

      _ ->
        {ws, offset, count}
    end
  end

  # Applies LIMIT offset count to a filtered list.
  defp apply_limit(list, 0, :all), do: list

  defp apply_limit(list, offset, :all) do
    Enum.drop(list, offset)
  end

  defp apply_limit(list, offset, count) do
    list |> Enum.drop(offset) |> Enum.take(count)
  end

  # ---------------------------------------------------------------------------
  # ZSCAN helpers
  # ---------------------------------------------------------------------------

  defp parse_cursor(cursor_str) do
    case Integer.parse(cursor_str) do
      {cursor, ""} when cursor >= 0 -> {:ok, cursor}
      _ -> {:error, "ERR invalid cursor"}
    end
  end

  defp parse_zscan_opts(opts), do: do_parse_zscan_opts(opts, nil, 10)

  defp do_parse_zscan_opts([], match, count), do: {:ok, match, count}

  defp do_parse_zscan_opts([opt, value | rest], match, count) do
    case String.upcase(opt) do
      "MATCH" ->
        do_parse_zscan_opts(rest, value, count)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> do_parse_zscan_opts(rest, match, n)
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_parse_zscan_opts([_ | _], _match, _count) do
    {:error, "ERR syntax error"}
  end

  defp paginate(items, cursor, count) do
    total = length(items)

    if cursor >= total do
      {"0", []}
    else
      batch = Enum.slice(items, cursor, count)
      batch_len = min(count, total - cursor)
      next_pos = cursor + batch_len

      if next_pos >= total do
        {"0", batch}
      else
        {Integer.to_string(next_pos), batch}
      end
    end
  end

  defp select_random_zset_members(pairs, count, with_scores) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        selected = Enum.take_random(pairs, count)

        if with_scores do
          Enum.flat_map(selected, fn {member, score} -> [member, format_score_str(score)] end)
        else
          Enum.map(selected, fn {member, _score} -> member end)
        end

      count < 0 ->
        abs_count = abs(count)

        if pairs == [] do
          []
        else
          selected = for _ <- 1..abs_count, do: Enum.random(pairs)

          if with_scores do
            Enum.flat_map(selected, fn {member, score} -> [member, format_score_str(score)] end)
          else
            Enum.map(selected, fn {member, _score} -> member end)
          end
        end
    end
  end
end
