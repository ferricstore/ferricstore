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

  alias Ferricstore.Commands.SortedSet.Helpers
  alias Ferricstore.Commands.SortedSet.Reads
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
    with {:ok, opts, score_member_pairs} <- Helpers.parse_zadd_opts(rest),
         type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :zset, store) do
      unique_members =
        score_member_pairs |> Enum.map(fn {_score, member} -> member end) |> Enum.uniq()

      compound_keys = Enum.map(unique_members, &CompoundKey.zset_member(key, &1))

      current_values = Ops.compound_batch_get(store, key, compound_keys)
      current_by_member = zset_current_by_member(unique_members, current_values, %{})

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

      write_entries = zset_write_entries(unique_members, compound_keys, writes_by_member, [])

      case Ops.compound_batch_put(store, key, write_entries) do
        :ok -> if opts.ch, do: added + changed, else: added
        {:error, _} = err -> rollback_new_zset_type_marker(key, store, type_status, err)
      end
    end
  end

  def handle("ZADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zadd' command"}
  end

  # ---------------------------------------------------------------------------
  # ZSCORE key member
  # ---------------------------------------------------------------------------

  def handle("ZSCORE", [key, member], store), do: Reads.zscore_member(key, member, store)

  def handle("ZSCORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zscore' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANK key member
  # ---------------------------------------------------------------------------

  def handle("ZRANK", [key, member], store), do: Reads.zrank_member(key, member, false, store)

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

  def handle("ZCARD", [key], store), do: Reads.zcard_key(key, store)

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
    with {:ok, increment} <- parse_zincrby_increment(increment_str),
         type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :zset, store) do
      zincrby_member(key, increment, member, store, type_status)
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
      case {Helpers.parse_score_bound(min_str), Helpers.parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          min_bound = Helpers.raw_score_bound(min_val, min_excl)
          max_bound = Helpers.raw_score_bound(max_val, max_excl)

          case Ops.zset_score_count(store, key, min_bound, max_bound) do
            {:ok, count} ->
              count

            :unavailable ->
              key
              |> load_members(store)
              |> Enum.count(fn {_member, score} ->
                above_min = Helpers.score_gte?(score, min_val, min_excl)
                below_max = Helpers.score_lte?(score, max_val, max_excl)
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
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 -> zpop_parsed(key, count, false, store)
      _ -> {:error, "ERR value is not an integer or out of range"}
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
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 -> zpop_parsed(key, count, true, store)
      _ -> {:error, "ERR value is not an integer or out of range"}
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
         {:ok, cursor} <- Helpers.parse_cursor(cursor_str),
         {:ok, match_pattern, count} <- Helpers.parse_zscan_opts(opts) do
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

      {next_cursor, batch} = Helpers.paginate(filtered, cursor, count)
      elements = Helpers.score_string_pairs_to_flat_list(batch)
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

  def handle("ZRANDMEMBER", [key], store), do: Reads.zrandmember_one(key, store)

  def handle("ZRANDMEMBER", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case Integer.parse(count_str) do
        {0, ""} ->
          []

        {count, ""} ->
          prefix = CompoundKey.zset_prefix(key)
          pairs = Ops.compound_scan(store, key, prefix)
          Helpers.select_random_members(pairs, count, false)

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
          {0, ""} ->
            []

          {count, ""} ->
            prefix = CompoundKey.zset_prefix(key)
            pairs = Ops.compound_scan(store, key, prefix)
            Helpers.select_random_members(pairs, count, true)

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

  def handle("ZMSCORE", args, store), do: Reads.zmscore_args(args, store)

  # ---------------------------------------------------------------------------
  # ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
  # ---------------------------------------------------------------------------

  def handle("ZRANGEBYSCORE", [key, min_str, max_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      case {Helpers.parse_score_bound(min_str), Helpers.parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          case Helpers.parse_range_by_score_opts(opts) do
            {:error, _} = err ->
              err

            {with_scores, offset, count} ->
              min_bound = Helpers.raw_score_bound(min_val, min_excl)
              max_bound = Helpers.raw_score_bound(max_val, max_excl)

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
                Helpers.score_pairs_to_flat_list(filtered)
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
      case {Helpers.parse_score_bound(min_str), Helpers.parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          case Helpers.parse_range_by_score_opts(opts) do
            {:error, _} = err ->
              err

            {with_scores, offset, count} ->
              min_bound = Helpers.raw_score_bound(min_val, min_excl)
              max_bound = Helpers.raw_score_bound(max_val, max_excl)

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
                Helpers.score_pairs_to_flat_list(filtered)
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

  def handle("ZREVRANK", [key, member], store), do: Reads.zrank_member(key, member, true, store)

  def handle("ZREVRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrank' command"}
  end

  @doc false
  def handle_ast(ast, store)

  def handle_ast({:zadd, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zadd, key, opts, score_member_pairs}, store),
    do: zadd_parsed(key, opts, score_member_pairs, store)

  def handle_ast({:zrem, args}, store), do: zrem_args(args, store)
  def handle_ast({:zmscore, args}, store), do: Reads.zmscore_args(args, store)
  def handle_ast({:zscore, key, member}, store), do: Reads.zscore_member(key, member, store)
  def handle_ast({:zrank, key, member}, store), do: Reads.zrank_member(key, member, false, store)

  def handle_ast({:zrevrank, key, member}, store),
    do: Reads.zrank_member(key, member, true, store)

  def handle_ast({:zcard, key}, store), do: Reads.zcard_key(key, store)

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

  def handle_ast({:zrandmember, key}, store), do: Reads.zrandmember_one(key, store)
  def handle_ast({:zrandmember, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:zrandmember, _key, _count, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zrandmember, key, count, with_scores}, store),
    do: Reads.zrandmember_parsed(key, count, with_scores, store)

  def handle_ast({:zscan, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zscan, key, cursor, opts}, store),
    do: Reads.zscan_parsed(key, cursor, opts, store)

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

  defp maybe_cleanup_empty_zset(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_zset(key, _removed, store) do
    if zset_member_count(key, store) == 0 do
      TypeRegistry.delete_type(key, store)
    else
      :ok
    end
  end

  defp delete_zset_members_and_cleanup(key, removed_entries, removed_count, store) do
    removed_keys =
      Enum.map(removed_entries, fn {compound_key, _value, _expire_at_ms} -> compound_key end)

    case Ops.compound_batch_delete(store, key, removed_keys) do
      :ok ->
        case maybe_cleanup_empty_zset(key, removed_count, store) do
          :ok -> :ok
          {:error, _} = error -> rollback_deleted_zset_members(key, removed_entries, store, error)
        end

      {:error, _} = err ->
        err
    end
  end

  defp rollback_deleted_zset_members(_key, [], _store, write_error), do: write_error

  defp rollback_deleted_zset_members(key, removed_entries, store, write_error) do
    case Ops.compound_batch_put(store, key, removed_entries) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:zset_delete_rollback_failed, write_error, rollback_error}}
    end
  end

  defp zrem_args([key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_keys =
        members
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.zset_member(key, &1))

      scores_by_key =
        store
        |> Ops.compound_batch_get(key, compound_keys)
        |> Enum.zip(compound_keys)
        |> Map.new(fn {score_str, compound_key} -> {compound_key, score_str} end)

      removed_entries =
        Enum.flat_map(compound_keys, fn compound_key ->
          case Map.fetch!(scores_by_key, compound_key) do
            nil -> []
            score_str -> [{compound_key, score_str, 0}]
          end
        end)

      removed = length(removed_entries)

      with :ok <- delete_zset_members_and_cleanup(key, removed_entries, removed, store) do
        removed
      end
    end
  end

  defp zrem_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'zrem' command"}
  end

  defp zadd_parsed(key, opts, score_member_pairs, store) do
    opts = %{
      nx: :nx in opts,
      xx: :xx in opts,
      gt: :gt in opts,
      lt: :lt in opts,
      ch: :ch in opts
    }

    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :zset, store) do
      unique_members =
        score_member_pairs |> Enum.map(fn {_score, member} -> member end) |> Enum.uniq()

      compound_keys = Enum.map(unique_members, &CompoundKey.zset_member(key, &1))

      current_values = Ops.compound_batch_get(store, key, compound_keys)
      current_by_member = zset_current_by_member(unique_members, current_values, %{})

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

      write_entries = zset_write_entries(unique_members, compound_keys, writes_by_member, [])

      case Ops.compound_batch_put(store, key, write_entries) do
        :ok -> if opts.ch, do: added + changed, else: added
        {:error, _} = err -> rollback_new_zset_type_marker(key, store, type_status, err)
      end
    end
  end

  defp rollback_new_zset_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.delete_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:zset_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_zset_type_marker(_key, _store, :ok, write_error), do: write_error

  defp zset_current_by_member([member | members], [value | values], acc) do
    zset_current_by_member(members, values, Map.put(acc, member, value))
  end

  defp zset_current_by_member(_members, _values, acc), do: acc

  defp zset_write_entries([], [], _writes_by_member, entries), do: Enum.reverse(entries)

  defp zset_write_entries(
         [member | members],
         [compound_key | compound_keys],
         writes_by_member,
         entries
       ) do
    next_entries =
      case Map.fetch(writes_by_member, member) do
        {:ok, score_str} -> [{compound_key, score_str, 0} | entries]
        :error -> entries
      end

    zset_write_entries(members, compound_keys, writes_by_member, next_entries)
  end

  defp zincrby_parsed(key, increment, member, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :zset, store) do
      zincrby_member(key, increment, member, store, type_status)
    end
  end

  defp parse_zincrby_increment(increment_str) do
    case Float.parse(increment_str) do
      {increment, ""} ->
        {:ok, increment}

      :error ->
        case Integer.parse(increment_str) do
          {increment, ""} -> {:ok, increment * 1.0}
          _ -> {:error, "ERR value is not a valid float"}
        end
    end
  end

  defp zincrby_member(key, increment, member, store, type_status) do
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
    write_zscore(store, key, compound_key, new_score, type_status)
  end

  defp write_zscore(store, key, compound_key, new_score, type_status) do
    case Ops.compound_put(store, key, compound_key, Float.to_string(new_score), 0) do
      :ok -> Helpers.format_score(new_score)
      true -> Helpers.format_score(new_score)
      {:error, _reason} = error -> rollback_new_zset_type_marker(key, store, type_status, error)
      other -> {:error, other}
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
    {Helpers.normalize_index(start, len), Helpers.normalize_index(stop, len)}
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
    Helpers.score_pairs_to_flat_list(members)
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
            Helpers.score_gte_bound?(score, min_bound) and
              Helpers.score_lte_bound?(score, max_bound)
          end)
      end
    end
  end

  defp zpop_parsed(key, count, reverse?, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      to_pop = zpop_members(key, count, reverse?, store)

      result = Helpers.score_pairs_to_flat_list(to_pop)

      compound_keys =
        Enum.map(to_pop, fn {member, _score} -> CompoundKey.zset_member(key, member) end)

      removed_entries =
        Enum.zip(compound_keys, to_pop)
        |> Enum.map(fn {compound_key, {_member, score}} ->
          {compound_key, Helpers.format_score(score), 0}
        end)

      with :ok <- delete_zset_members_and_cleanup(key, removed_entries, length(to_pop), store) do
        result
      end
    end
  end

  defp zpop_members(_key, 0, _reverse?, _store), do: []

  defp zpop_members(key, count, reverse?, store) do
    case Ops.zset_rank_range(store, key, 0, count - 1, reverse?) do
      {:ok, members} ->
        members

      :unavailable ->
        sorted =
          if reverse? do
            load_sorted_members(key, store) |> Enum.reverse()
          else
            load_sorted_members(key, store)
          end

        Enum.take(sorted, count)
    end
  end

  defp zrangebyscore_parsed(key, min_bound, max_bound, opts, reverse?, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store),
         {:ok, with_scores, offset, count} <- Helpers.typed_range_by_score_opts(opts) do
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
        Helpers.score_pairs_to_flat_list(filtered)
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
            Helpers.score_gte_bound?(score, min_bound) and
              Helpers.score_lte_bound?(score, max_bound)
          end)
          |> sort_members(reverse?)
      end

    Helpers.apply_limit(filtered, offset, count)
  end

  defp load_sorted_members(key, store) do
    key
    |> load_members(store)
    |> sort_members(false)
  end

  defp zcard_count(key, store) do
    zset_member_count(key, store)
  end

  defp zset_member_count(key, store) do
    case Ops.zset_score_count(store, key, :neg_inf, :inf) do
      {:ok, count} ->
        count

      :unavailable ->
        prefix = CompoundKey.zset_prefix(key)
        Ops.compound_count(store, key, prefix)
    end
  end

  defp load_members(key, store) do
    prefix = CompoundKey.zset_prefix(key)
    pairs = Ops.compound_scan(store, key, prefix)

    Enum.map(pairs, fn {member, score_str} ->
      {member, Helpers.parse_stored_score(score_str)}
    end)
  end

  defp sort_members(members, false) do
    Enum.sort_by(members, fn {member, score} -> {score, member} end)
  end

  defp sort_members(members, true), do: members |> sort_members(false) |> Enum.reverse()
end
