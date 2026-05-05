defmodule Ferricstore.Commands.SortedSet do
  @moduledoc """
  Handles Redis sorted set commands: ZADD, ZSCORE, ZRANK, ZRANGE, ZCARD,
  ZREM, ZINCRBY, ZCOUNT, ZPOPMIN, ZPOPMAX, ZRANGEBYSCORE, ZREVRANGE,
  ZSCAN, ZRANDMEMBER, ZMSCORE.

  Each sorted set member is stored as a compound key:

      Z:redis_key\\0member -> score_string

  The score is stored as a string representation of a float64. This allows
  O(1) score lookups by member. For range queries, all members are loaded
  and sorted in memory when a command needs rank ordering.

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

      write_entries =
        Enum.flat_map(Enum.zip(unique_members, compound_keys), fn {member, compound_key} ->
          case Map.fetch(writes_by_member, member) do
            {:ok, score_str} -> [{compound_key, score_str, 0}]
            :error -> []
          end
        end)

      case Ops.compound_batch_put(store, key, write_entries) do
        :ok -> if opts.ch, do: added + changed, else: added
        {:error, _} = err -> err
      end
    end
  end

  def handle("ZADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zadd' command"}
  end

  # ---------------------------------------------------------------------------
  # ZSCORE key member
  # ---------------------------------------------------------------------------

  def handle("ZSCORE", [key, member], store), do: zscore_member(key, member, store)

  def handle("ZSCORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zscore' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANK key member
  # ---------------------------------------------------------------------------

  def handle("ZRANK", [key, member], store), do: zrank_member(key, member, false, store)

  def handle("ZRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrank' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANGE key start stop [WITHSCORES]
  # ---------------------------------------------------------------------------

  def handle("ZRANGE", [key, start_str, stop_str | opts], store) do
    case {Integer.parse(start_str), Integer.parse(stop_str)} do
      {{start, ""}, {stop, ""}} ->
        zrange_rank_parsed(key, start, stop, "WITHSCORES" in opts, false, store)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("ZRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrange' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREVRANGE key start stop [WITHSCORES]
  # ---------------------------------------------------------------------------

  def handle("ZREVRANGE", [key, start_str, stop_str | opts], store) do
    case {Integer.parse(start_str), Integer.parse(stop_str)} do
      {{start, ""}, {stop, ""}} ->
        zrange_rank_parsed(key, start, stop, "WITHSCORES" in opts, true, store)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("ZREVRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrange' command"}
  end

  # ---------------------------------------------------------------------------
  # ZCARD key
  # ---------------------------------------------------------------------------

  def handle("ZCARD", [key], store), do: zcard_key(key, store)

  def handle("ZCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zcard' command"}
  end

  # ---------------------------------------------------------------------------
  # ZREM key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("ZREM", args, store), do: zrem_args(args, store)

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
          min_bound = raw_score_bound(min_val, min_excl)
          max_bound = raw_score_bound(max_val, max_excl)

          case Ops.zset_score_count(store, key, min_bound, max_bound) do
            {:ok, count} ->
              count

            :unavailable ->
              key
              |> load_members(store)
              |> Enum.count(fn {_member, score} ->
                above_min = score_gte?(score, min_val, min_excl)
                below_max = score_lte?(score, max_val, max_excl)
                above_min and below_max
              end)
          end

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
              [member, format_score(score)]
            end)

          compound_keys =
            Enum.map(to_pop, fn {member, _score} -> CompoundKey.zset_member(key, member) end)

          case Ops.compound_batch_delete(store, key, compound_keys) do
            :ok ->
              if to_pop != [] do
                prefix = CompoundKey.zset_prefix(key)

                if Ops.compound_count(store, key, prefix) == 0 do
                  TypeRegistry.delete_type(key, store)
                end
              end

              result

            {:error, _} = err ->
              err
          end

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
              [member, format_score(score)]
            end)

          compound_keys =
            Enum.map(to_pop, fn {member, _score} -> CompoundKey.zset_member(key, member) end)

          case Ops.compound_batch_delete(store, key, compound_keys) do
            :ok ->
              if to_pop != [] do
                prefix = CompoundKey.zset_prefix(key)

                if Ops.compound_count(store, key, prefix) == 0 do
                  TypeRegistry.delete_type(key, store)
                end
              end

              result

            {:error, _} = err ->
              err
          end

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

  def handle("ZRANDMEMBER", [key], store), do: zrandmember_one(key, store)

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

  def handle("ZMSCORE", args, store), do: zmscore_args(args, store)

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
              min_bound = raw_score_bound(min_val, min_excl)
              max_bound = raw_score_bound(max_val, max_excl)

              filtered =
                case Ops.zset_score_range_slice(
                       store,
                       key,
                       min_bound,
                       max_bound,
                       false,
                       offset,
                       count
                     ) do
                  {:ok, members} ->
                    members

                  :unavailable ->
                    zrangebyscore_full_range(
                      key,
                      min_bound,
                      max_bound,
                      false,
                      offset,
                      count,
                      store
                    )
                end

              if with_scores do
                Enum.flat_map(filtered, fn {member, score} -> [member, format_score(score)] end)
              else
                Enum.map(filtered, fn {member, _score} -> member end)
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
              min_bound = raw_score_bound(min_val, min_excl)
              max_bound = raw_score_bound(max_val, max_excl)

              filtered =
                case Ops.zset_score_range_slice(
                       store,
                       key,
                       min_bound,
                       max_bound,
                       true,
                       offset,
                       count
                     ) do
                  {:ok, members} ->
                    members

                  :unavailable ->
                    zrangebyscore_full_range(
                      key,
                      min_bound,
                      max_bound,
                      true,
                      offset,
                      count,
                      store
                    )
                end

              if with_scores do
                Enum.flat_map(filtered, fn {member, score} -> [member, format_score(score)] end)
              else
                Enum.map(filtered, fn {member, _score} -> member end)
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

  def handle("ZREVRANK", [key, member], store), do: zrank_member(key, member, true, store)

  def handle("ZREVRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrank' command"}
  end

  @doc false
  def handle_ast(ast, store)

  def handle_ast({:zadd, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zadd, key, opts, score_member_pairs}, store),
    do: zadd_parsed(key, opts, score_member_pairs, store)

  def handle_ast({:zrem, args}, store), do: zrem_args(args, store)
  def handle_ast({:zmscore, args}, store), do: zmscore_args(args, store)
  def handle_ast({:zscore, key, member}, store), do: zscore_member(key, member, store)
  def handle_ast({:zrank, key, member}, store), do: zrank_member(key, member, false, store)
  def handle_ast({:zrevrank, key, member}, store), do: zrank_member(key, member, true, store)
  def handle_ast({:zcard, key}, store), do: zcard_key(key, store)

  def handle_ast({:zincrby, _key, {:error, reason}, _member}, _store), do: {:error, reason}

  def handle_ast({:zincrby, key, increment, member}, store),
    do: zincrby_parsed(key, increment, member, store)

  def handle_ast({:zrange, _key, {:error, reason}, _tail}, _store), do: {:error, reason}
  def handle_ast({:zrevrange, _key, {:error, reason}, _tail}, _store), do: {:error, reason}

  def handle_ast({:zrange, key, start, stop, with_scores}, store),
    do: zrange_rank_parsed(key, start, stop, with_scores, false, store)

  def handle_ast({:zrevrange, key, start, stop, with_scores}, store),
    do: zrange_rank_parsed(key, start, stop, with_scores, true, store)

  def handle_ast({:zcount, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zcount, key, min_bound, max_bound}, store),
    do: zcount_parsed(key, min_bound, max_bound, store)

  def handle_ast({:zpopmin, key}, store), do: zpop_parsed(key, 1, false, store)
  def handle_ast({:zpopmax, key}, store), do: zpop_parsed(key, 1, true, store)
  def handle_ast({:zpopmin, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:zpopmax, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:zpopmin, key, count}, store), do: zpop_parsed(key, count, false, store)
  def handle_ast({:zpopmax, key, count}, store), do: zpop_parsed(key, count, true, store)

  def handle_ast({:zrandmember, key}, store), do: zrandmember_one(key, store)
  def handle_ast({:zrandmember, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:zrandmember, _key, _count, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zrandmember, key, count, with_scores}, store),
    do: zrandmember_parsed(key, count, with_scores, store)

  def handle_ast({:zscan, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:zscan, key, cursor, opts}, store), do: zscan_parsed(key, cursor, opts, store)

  def handle_ast({:zrangebyscore, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:zrevrangebyscore, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zrangebyscore, key, min_bound, max_bound, opts}, store),
    do: zrangebyscore_parsed(key, min_bound, max_bound, opts, false, store)

  def handle_ast({:zrevrangebyscore, key, max_bound, min_bound, opts}, store),
    do: zrangebyscore_parsed(key, min_bound, max_bound, opts, true, store)

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported zset command AST"}

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp zscore_member(key, member, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_key = CompoundKey.zset_member(key, member)

      case Ops.compound_get(store, key, compound_key) do
        nil -> nil
        score_str -> format_score_str(score_str)
      end
    end
  end

  defp zrank_member(key, member, reverse?, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Ops.zset_member_rank(store, key, member, reverse?) do
        {:ok, rank} ->
          rank

        :unavailable ->
          sorted = load_sorted_members(key, store)
          ranked = if reverse?, do: Enum.reverse(sorted), else: sorted

          case Enum.find_index(ranked, fn {m, _s} -> m == member end) do
            nil -> nil
            idx -> idx
          end
      end
    end
  end

  defp zcard_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Ops.zset_score_count(store, key, :neg_inf, :inf) do
        {:ok, count} ->
          count

        :unavailable ->
          prefix = CompoundKey.zset_prefix(key)
          Ops.compound_count(store, key, prefix)
      end
    end
  end

  defp zrem_args([key | members], store) when members != [] do
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

      removed = length(removed_keys)

      case Ops.compound_batch_delete(store, key, removed_keys) do
        :ok ->
          if removed > 0 do
            prefix = CompoundKey.zset_prefix(key)

            if Ops.compound_count(store, key, prefix) == 0 do
              TypeRegistry.delete_type(key, store)
            end
          end

          removed

        {:error, _} = err ->
          err
      end
    end
  end

  defp zrem_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'zrem' command"}
  end

  defp zrandmember_one(key, store) do
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

  defp zmscore_args([key | members], store) when members != [] do
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

  defp zmscore_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'zmscore' command"}
  end

  defp zadd_parsed(key, opts, score_member_pairs, store) do
    opts = %{
      nx: :nx in opts,
      xx: :xx in opts,
      gt: :gt in opts,
      lt: :lt in opts,
      ch: :ch in opts
    }

    with :ok <- TypeRegistry.check_or_set(key, :zset, store) do
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
            opts.nx and existing != nil ->
              {add_acc, ch_acc, current_acc, writes_acc}

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

      write_entries =
        Enum.flat_map(Enum.zip(unique_members, compound_keys), fn {member, compound_key} ->
          case Map.fetch(writes_by_member, member) do
            {:ok, score_str} -> [{compound_key, score_str, 0}]
            :error -> []
          end
        end)

      case Ops.compound_batch_put(store, key, write_entries) do
        :ok -> if opts.ch, do: added + changed, else: added
        {:error, _} = err -> err
      end
    end
  end

  defp zincrby_parsed(key, increment, member, store) do
    with :ok <- TypeRegistry.check_or_set(key, :zset, store) do
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
    end
  end

  defp zrange_rank_parsed(key, start, stop, with_scores, reverse?, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      {start_idx, stop_idx} = normalize_rank_bounds(key, start, stop, store)

      zrange_rank_from_index_or_scan(key, start_idx, stop_idx, with_scores, reverse?, store)
    end
  end

  defp normalize_rank_bounds(key, start, stop, store) when start < 0 or stop < 0 do
    len = zcard_count(key, store)
    {normalize_index(start, len), normalize_index(stop, len)}
  end

  defp normalize_rank_bounds(_key, start, stop, _store), do: {start, stop}

  defp zrange_rank_from_index_or_scan(_key, start_idx, stop_idx, _with_scores, _reverse?, _store)
       when start_idx > stop_idx do
    []
  end

  defp zrange_rank_from_index_or_scan(key, start_idx, stop_idx, with_scores, reverse?, store) do
    case Ops.zset_rank_range(store, key, start_idx, stop_idx, reverse?) do
      {:ok, members} ->
        format_rank_members(members, with_scores)

      :unavailable ->
        sorted =
          if reverse? do
            load_sorted_members(key, store) |> Enum.reverse()
          else
            load_sorted_members(key, store)
          end

        len = length(sorted)

        if start_idx >= len do
          []
        else
          sorted
          |> Enum.slice(start_idx..stop_idx)
          |> format_rank_members(with_scores)
        end
    end
  end

  defp format_rank_members(members, true) do
    Enum.flat_map(members, fn {member, score} -> [member, format_score(score)] end)
  end

  defp format_rank_members(members, false) do
    Enum.map(members, fn {member, _score} -> member end)
  end

  defp zcount_parsed(key, min_bound, max_bound, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Ops.zset_score_count(store, key, min_bound, max_bound) do
        {:ok, count} ->
          count

        :unavailable ->
          key
          |> load_members(store)
          |> Enum.count(fn {_member, score} ->
            score_gte_bound?(score, min_bound) and score_lte_bound?(score, max_bound)
          end)
      end
    end
  end

  defp zpop_parsed(key, count, reverse?, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      sorted =
        if reverse? do
          load_sorted_members(key, store) |> Enum.reverse()
        else
          load_sorted_members(key, store)
        end

      to_pop = Enum.take(sorted, count)

      result =
        Enum.flat_map(to_pop, fn {member, score} ->
          [member, format_score(score)]
        end)

      compound_keys =
        Enum.map(to_pop, fn {member, _score} -> CompoundKey.zset_member(key, member) end)

      case Ops.compound_batch_delete(store, key, compound_keys) do
        :ok ->
          if to_pop != [] do
            prefix = CompoundKey.zset_prefix(key)

            if Ops.compound_count(store, key, prefix) == 0 do
              TypeRegistry.delete_type(key, store)
            end
          end

          result

        {:error, _} = err ->
          err
      end
    end
  end

  defp zrandmember_parsed(key, count, with_scores, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      prefix = CompoundKey.zset_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      select_random_zset_members(pairs, count, with_scores)
    end
  end

  defp zscan_parsed(key, cursor, opts, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store),
         {:ok, match_pattern, count} <- typed_scan_opts(opts) do
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

  defp zrangebyscore_parsed(key, min_bound, max_bound, opts, reverse?, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store),
         {:ok, with_scores, offset, count} <- typed_range_by_score_opts(opts) do
      filtered =
        case Ops.zset_score_range_slice(
               store,
               key,
               min_bound,
               max_bound,
               reverse?,
               offset,
               count
             ) do
          {:ok, members} ->
            members

          :unavailable ->
            zrangebyscore_full_range(key, min_bound, max_bound, reverse?, offset, count, store)
        end

      if with_scores do
        Enum.flat_map(filtered, fn {member, score} -> [member, format_score(score)] end)
      else
        Enum.map(filtered, fn {member, _score} -> member end)
      end
    end
  end

  defp zrangebyscore_full_range(key, min_bound, max_bound, reverse?, offset, count, store) do
    filtered =
      case Ops.zset_score_range(store, key, min_bound, max_bound, reverse?) do
        {:ok, members} ->
          members

        :unavailable ->
          key
          |> load_members(store)
          |> Enum.filter(fn {_member, score} ->
            score_gte_bound?(score, min_bound) and score_lte_bound?(score, max_bound)
          end)
          |> sort_members(reverse?)
      end

    apply_limit(filtered, offset, count)
  end

  defp load_sorted_members(key, store) do
    key
    |> load_members(store)
    |> sort_members(false)
  end

  defp zcard_count(key, store) do
    prefix = CompoundKey.zset_prefix(key)
    Ops.compound_count(store, key, prefix)
  end

  defp load_members(key, store) do
    prefix = CompoundKey.zset_prefix(key)
    pairs = Ops.compound_scan(store, key, prefix)

    Enum.map(pairs, fn {member, score_str} ->
      {member, parse_stored_score(score_str)}
    end)
  end

  defp sort_members(members, false) do
    Enum.sort_by(members, fn {member, score} -> {score, member} end)
  end

  defp sort_members(members, true), do: members |> sort_members(false) |> Enum.reverse()

  defp parse_stored_score(score_str) do
    case Float.parse(score_str) do
      {score, ""} -> score
      _ -> 0.0
    end
  end

  defp raw_score_bound(:neg_infinity, _exclusive), do: :neg_inf
  defp raw_score_bound(:infinity, _exclusive), do: :inf
  defp raw_score_bound(score, true), do: {:exclusive, score}
  defp raw_score_bound(score, false), do: {:inclusive, score}

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

  defp score_gte_bound?(_score, :neg_inf), do: true
  defp score_gte_bound?(_score, :inf), do: false
  defp score_gte_bound?(score, {:exclusive, bound}), do: score > bound
  defp score_gte_bound?(score, {:inclusive, bound}), do: score >= bound

  defp score_lte_bound?(_score, :inf), do: true
  defp score_lte_bound?(_score, :neg_inf), do: false
  defp score_lte_bound?(score, {:exclusive, bound}), do: score < bound
  defp score_lte_bound?(score, {:inclusive, bound}), do: score <= bound

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

  defp typed_range_by_score_opts(opts), do: do_typed_range_by_score_opts(opts, false, 0, :all)

  defp do_typed_range_by_score_opts([], with_scores, offset, count),
    do: {:ok, with_scores, offset, count}

  defp do_typed_range_by_score_opts([{:withscores, true} | rest], _with_scores, offset, count),
    do: do_typed_range_by_score_opts(rest, true, offset, count)

  defp do_typed_range_by_score_opts(
         [{:limit, {offset, count}} | rest],
         with_scores,
         _offset,
         _count
       )
       when is_integer(offset) and offset >= 0 and is_integer(count) do
    do_typed_range_by_score_opts(rest, with_scores, offset, if(count < 0, do: :all, else: count))
  end

  defp do_typed_range_by_score_opts(_opts, _with_scores, _offset, _count),
    do: {:error, "ERR syntax error"}

  # ---------------------------------------------------------------------------
  # ZSCAN helpers
  # ---------------------------------------------------------------------------

  defp typed_scan_opts(opts), do: do_typed_scan_opts(opts, nil, 10)

  defp do_typed_scan_opts([], match, count), do: {:ok, match, count}

  defp do_typed_scan_opts([{:match, pattern} | rest], _match, count) when is_binary(pattern) do
    do_typed_scan_opts(rest, pattern, count)
  end

  defp do_typed_scan_opts([{:count, count} | rest], match, _count)
       when is_integer(count) and count > 0 do
    do_typed_scan_opts(rest, match, count)
  end

  defp do_typed_scan_opts(_opts, _match, _count), do: {:error, "ERR syntax error"}

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
