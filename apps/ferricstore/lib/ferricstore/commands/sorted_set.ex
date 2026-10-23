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

  alias Ferricstore.Commands.CollectionScan
  alias Ferricstore.Commands.SortedSet.Helpers
  alias Ferricstore.Commands.SortedSet.Reads
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
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
    case Helpers.parse_zadd_opts(rest) do
      {:ok, %{incr: true} = opts, pairs} ->
        flags = for flag <- [:nx, :xx, :gt, :lt, :ch, :incr], Map.fetch!(opts, flag), do: flag
        zadd_incr(key, flags, pairs, store)

      {:ok, opts, score_member_pairs} ->
        zadd_args(key, opts, score_member_pairs, store)

      {:error, _reason} = error ->
        error
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

  def handle("ZRANK", [key, member, option], store),
    do: rank_with_score(key, member, option, false, store)

  def handle("ZRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrank' command"}
  end

  # ---------------------------------------------------------------------------
  # ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
  # ---------------------------------------------------------------------------

  def handle("ZRANGE", [key, start_str, stop_str | opts], store) do
    case Helpers.parse_zrange_options(opts) do
      {:ok, %{by: :rank, rev: reverse?, with_scores: with_scores}} ->
        case {Integer.parse(start_str), Integer.parse(stop_str)} do
          {{start, ""}, {stop, ""}} ->
            zrange_rank_parsed(key, start, stop, with_scores, reverse?, store)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end

      # With REV the bounds come max first, as ZREVRANGEBYSCORE takes them.
      {:ok, %{by: :score, rev: reverse?} = parsed} ->
        command = if reverse?, do: "ZREVRANGEBYSCORE", else: "ZRANGEBYSCORE"
        handle(command, [key, start_str, stop_str | Helpers.zrange_score_options(parsed)], store)

      {:ok, %{by: :lex} = parsed} ->
        zrange_lex(key, start_str, stop_str, parsed, store)

      {:error, _reason} = error ->
        error
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
        with {:ok, with_scores} <- parse_rank_range_opts(opts) do
          zrange_rank_parsed(key, start, stop, with_scores, true, store)
        end

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
           TypeRegistry.command_check_or_set_status(key, :zset, store) do
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
    with :ok <- score_range_arguments(min_str, max_str, []),
         :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      case {Helpers.parse_score_bound(min_str), Helpers.parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          min_bound = Helpers.raw_score_bound(min_val, min_excl)
          max_bound = Helpers.raw_score_bound(max_val, max_excl)

          case Ops.zset_score_count(store, key, min_bound, max_bound) do
            {:ok, count} ->
              count

            {:error, {:storage_read_failed, _reason}} = failure ->
              ReadResult.command_error(failure)

            :unavailable ->
              with {:ok, members} <- load_members(key, store) do
                Enum.count(members, fn {_member, score} ->
                  above_min = Helpers.score_gte?(score, min_val, min_excl)
                  below_max = Helpers.score_lte?(score, max_val, max_excl)
                  above_min and below_max
                end)
              end
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
      _ -> zpop_count_error()
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
      _ -> zpop_count_error()
    end
  end

  def handle("ZPOPMAX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zpopmax' command"}
  end

  # ---------------------------------------------------------------------------
  # ZSCAN key cursor [MATCH pattern] [COUNT count]
  # ---------------------------------------------------------------------------

  def handle("ZSCAN", [key, cursor_str | opts], store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store),
         {:ok, cursor} <- Helpers.parse_cursor(cursor_str),
         {:ok, match_pattern, count} <- Helpers.parse_zscan_opts(opts),
         {:ok, {next_cursor, pairs}} <-
           CollectionScan.page(
             store,
             key,
             CompoundKey.zset_prefix(key),
             cursor,
             count,
             match_pattern,
             false
           ) do
      elements = Helpers.score_string_pairs_to_flat_list(pairs)
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
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      case Integer.parse(count_str) do
        {0, ""} ->
          []

        {count, ""} ->
          with {:ok, pairs} <- scan_pairs(key, store) do
            Helpers.select_random_members(pairs, count, false)
          end

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("ZRANDMEMBER", [key, count_str, withscores_str], store) do
    if String.upcase(withscores_str) != "WITHSCORES" do
      {:error, "ERR syntax error"}
    else
      with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
        case Integer.parse(count_str) do
          {0, ""} ->
            []

          {count, ""} ->
            with {:ok, pairs} <- scan_pairs(key, store) do
              Helpers.select_random_members(pairs, count, true)
            end

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
    with :ok <- score_range_arguments(min_str, max_str, opts),
         :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      case {Helpers.parse_score_bound(min_str), Helpers.parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          case Helpers.parse_range_by_score_opts(opts) do
            {:error, _} = err ->
              err

            {with_scores, offset, count} ->
              min_bound = Helpers.raw_score_bound(min_val, min_excl)
              max_bound = Helpers.raw_score_bound(max_val, max_excl)

              with {:ok, filtered} <-
                     score_range_slice(key, min_bound, max_bound, false, offset, count, store) do
                if with_scores do
                  Helpers.score_pairs_to_flat_list(filtered)
                else
                  Enum.map(filtered, fn {member, _score} -> member end)
                end
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
    with :ok <- score_range_arguments(min_str, max_str, opts),
         :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      case {Helpers.parse_score_bound(min_str), Helpers.parse_score_bound(max_str)} do
        {{:ok, min_val, min_excl}, {:ok, max_val, max_excl}} ->
          case Helpers.parse_range_by_score_opts(opts) do
            {:error, _} = err ->
              err

            {with_scores, offset, count} ->
              min_bound = Helpers.raw_score_bound(min_val, min_excl)
              max_bound = Helpers.raw_score_bound(max_val, max_excl)

              with {:ok, filtered} <-
                     score_range_slice(key, min_bound, max_bound, true, offset, count, store) do
                if with_scores do
                  Helpers.score_pairs_to_flat_list(filtered)
                else
                  Enum.map(filtered, fn {member, _score} -> member end)
                end
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

  def handle("ZREVRANK", [key, member, option], store),
    do: rank_with_score(key, member, option, true, store)

  def handle("ZREVRANK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'zrevrank' command"}
  end

  @doc false
  def handle_ast(ast, store)

  def handle_ast({:zadd, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:zadd, _key, _opts, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zadd, key, opts, score_member_pairs}, store),
    do: zadd_parsed(key, opts, score_member_pairs, store)

  def handle_ast({:zrem, args}, store), do: zrem_args(args, store)
  def handle_ast({:zmscore, args}, store), do: Reads.zmscore_args(args, store)
  def handle_ast({:zscore, key, member}, store), do: Reads.zscore_member(key, member, store)
  def handle_ast({:zrank, key, member}, store), do: Reads.zrank_member(key, member, false, store)

  def handle_ast({tag, _key, _member, {:error, reason}}, _store) when tag in [:zrank, :zrevrank],
    do: {:error, reason}

  def handle_ast({:zrank, key, member, :withscore}, store),
    do: Reads.zrank_member_with_score(key, member, false, store)

  def handle_ast({:zrevrank, key, member, :withscore}, store),
    do: Reads.zrank_member_with_score(key, member, true, store)

  def handle_ast({:zrevrank, key, member}, store),
    do: Reads.zrank_member(key, member, true, store)

  def handle_ast({:zcard, key}, store), do: Reads.zcard_key(key, store)

  def handle_ast({:zincrby, _key, {:error, reason}, _member}, _store), do: {:error, reason}

  def handle_ast({:zincrby, key, increment, member}, store),
    do: zincrby_parsed(key, increment, member, store)

  def handle_ast({:zrange, _key, {:error, reason}, _tail}, _store), do: {:error, reason}

  def handle_ast(
        {:zrange, key, {:lex, start, stop}, %{rev: _rev, limit: _limit} = parsed},
        store
      ),
      do: zrange_lex(key, start, stop, parsed, store)

  def handle_ast({:zrevrange, _key, {:error, reason}, _tail}, _store), do: {:error, reason}

  def handle_ast({:zrange, _key, _start, _stop, {:error, reason}}, _store),
    do: {:error, reason}

  def handle_ast({:zrevrange, _key, _start, _stop, {:error, reason}}, _store),
    do: {:error, reason}

  def handle_ast({tag, _key, {:error, reason}, _stop, _with_scores}, _store)
      when tag in [:zrange, :zrevrange],
      do: {:error, reason}

  def handle_ast({tag, _key, _start, {:error, reason}, _with_scores}, _store)
      when tag in [:zrange, :zrevrange],
      do: {:error, reason}

  def handle_ast({:zrange, key, start, stop, with_scores}, store),
    do: zrange_rank_parsed(key, start, stop, with_scores, false, store)

  def handle_ast({:zrevrange, key, start, stop, with_scores}, store),
    do: zrange_rank_parsed(key, start, stop, with_scores, true, store)

  def handle_ast({:zcount, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zcount, key, min_bound, max_bound}, store),
    do: zcount_parsed(key, min_bound, max_bound, store)

  def handle_ast({:zpopmin, key}, store), do: zpop_parsed(key, 1, false, store)
  def handle_ast({:zpopmax, key}, store), do: zpop_parsed(key, 1, true, store)

  def handle_ast({tag, _key, {:error, _reason}}, _store) when tag in [:zpopmin, :zpopmax],
    do: zpop_count_error()

  def handle_ast({:zpopmin, key, count}, store), do: zpop_parsed(key, count, false, store)
  def handle_ast({:zpopmax, key, count}, store), do: zpop_parsed(key, count, true, store)

  def handle_ast({:zrandmember, key}, store), do: Reads.zrandmember_one(key, store)
  def handle_ast({:zrandmember, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:zrandmember, _key, {:error, reason}, _with_scores}, _store),
    do: {:error, reason}

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

  # Redis parses a score range's bounds and options before it looks up the
  # key, so their errors come before a wrong type.
  defp score_range_arguments(min_str, max_str, opts) do
    with {:ok, _min, _min_exclusive} <- Helpers.parse_score_bound(min_str),
         {:ok, _max, _max_exclusive} <- Helpers.parse_score_bound(max_str) do
      case Helpers.parse_range_by_score_opts(opts) do
        {:error, _reason} = error -> error
        _parsed -> :ok
      end
    else
      _not_a_float -> {:error, "ERR min or max is not a float"}
    end
  end

  defp rank_with_score(key, member, option, reverse?, store) do
    if String.upcase(option) == "WITHSCORE",
      do: Reads.zrank_member_with_score(key, member, reverse?, store),
      else: {:error, "ERR syntax error"}
  end

  defp parse_rank_range_opts([]), do: {:ok, false}

  defp parse_rank_range_opts([option]) when is_binary(option) do
    if String.upcase(option) == "WITHSCORES",
      do: {:ok, true},
      else: {:error, "ERR syntax error"}
  end

  defp parse_rank_range_opts(_options), do: {:error, "ERR syntax error"}

  defp maybe_cleanup_empty_zset(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_zset(key, _removed, store) do
    case zset_member_count(key, store) do
      0 ->
        TypeRegistry.delete_type(key, store)

      count when is_integer(count) and count > 0 ->
        :ok

      {:error, _reason} = error ->
        error

      invalid ->
        invalid
        |> ReadResult.failure()
        |> ReadResult.command_error()
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

  # One member on the instance is removed by the state machine's atomic
  # single-member write, so concurrent ZREMs report one removal, as SREM does.
  defp zrem_args([key, member], %FerricStore.Instance{} = ctx) when is_binary(member),
    do: Ops.zrem_single(ctx, key, member)

  # Several members go to the same single-member entries in replicated
  # batches, so racing clients count each removal once.
  defp zrem_args([key | members], %FerricStore.Instance{} = ctx) when members != [] do
    if Enum.all?(members, &is_binary/1) do
      Ops.single_member_writes(
        ctx,
        key,
        members |> Enum.uniq() |> Enum.map(&{:zrem_single, key, &1})
      )
    else
      zrem_args_generic([key | members], ctx)
    end
  end

  defp zrem_args(args, store), do: zrem_args_generic(args, store)

  defp zrem_args_generic([key | members], store) when members != [] do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      compound_keys =
        members
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.zset_member(key, &1))

      with {:ok, values} <- read_score_values(key, compound_keys, store) do
        scores_by_key =
          values
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
  end

  defp zrem_args_generic(_args, _store) do
    {:error, "ERR wrong number of arguments for 'zrem' command"}
  end

  defp zadd_args(key, opts, score_member_pairs, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.command_check_or_set_status(key, :zset, store) do
      unique_members =
        score_member_pairs |> Enum.map(fn {_score, member} -> member end) |> Enum.uniq()

      compound_keys = Enum.map(unique_members, &CompoundKey.zset_member(key, &1))

      with {:ok, current_values} <-
             read_zadd_current_values(key, compound_keys, store, type_status) do
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
                existing_score = Helpers.parse_stored_score(existing)

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
        result = if opts.ch, do: added + changed, else: added
        persist_zadd_entries(key, write_entries, result, store, type_status)
      end
    end
  end

  # ZADD ... INCR adds the increment to the member's score as ZINCRBY does,
  # answering the new score, or nil when NX, XX, GT or LT refuses it.
  defp zadd_incr(key, opts, [{increment, member}], store) do
    if Enum.any?(opts, &(&1 in [:nx, :xx, :gt, :lt])),
      do: conditional_zadd_incr(key, opts, increment, member, store),
      else: zincrby_key(key, increment, member, store)
  end

  # At cluster feature level 5 the condition and the increment run as one
  # replicated entry, so concurrent clients each see one outcome; older
  # releases do not apply ZADD INCR there.
  defp conditional_zadd_incr(key, opts, increment, member, %FerricStore.Instance{} = ctx) do
    case Ops.compound_command(ctx, key, {:zadd, key, opts, [{increment, member}]}, 5) do
      :unsupported -> checked_zadd_incr(key, opts, increment, member, ctx)
      result -> result
    end
  end

  defp conditional_zadd_incr(key, opts, increment, member, store),
    do: checked_zadd_incr(key, opts, increment, member, store)

  defp checked_zadd_incr(key, opts, increment, member, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      case Ops.compound_get(store, key, CompoundKey.zset_member(key, member)) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        stored ->
          current = if stored, do: Helpers.parse_stored_score(stored)

          if zadd_incr_allowed?(opts, current, increment),
            do: zincrby_key(key, increment, member, store),
            else: nil
      end
    end
  end

  defp zadd_incr_allowed?(opts, nil, _increment), do: :xx not in opts

  defp zadd_incr_allowed?(opts, current, increment) do
    cond do
      :nx in opts -> false
      :gt in opts -> current + increment > current
      :lt in opts -> current + increment < current
      true -> true
    end
  end

  defp zincrby_key(key, increment, member, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.command_check_or_set_status(key, :zset, store),
         do: zincrby_member(key, increment, member, store, type_status)
  end

  defp zadd_parsed(key, opts, score_member_pairs, store) do
    with :ok <- validate_zadd_ast(opts, score_member_pairs) do
      if :incr in opts,
        do: zadd_incr(key, opts, score_member_pairs, store),
        else: zadd_validated(key, opts, score_member_pairs, store)
    end
  end

  # A ZADD without options on the instance writes through the state machine's
  # single-member entries in replicated batches: each checks the key's type
  # and reports whether its member is new, so racing clients count each
  # member once and a SET between the type check and the write can no longer
  # leave members under the string. Options keep the read-decide-write path.
  defp zadd_validated(key, [], score_member_pairs, %FerricStore.Instance{} = ctx) do
    if Enum.all?(score_member_pairs, fn {score, member} ->
         is_float(score) and is_binary(member)
       end) do
      Ops.single_member_writes(
        ctx,
        key,
        Enum.map(score_member_pairs, fn {score, member} -> {:zadd_single, key, score, member} end)
      )
    else
      do_zadd_parsed(key, [], score_member_pairs, ctx)
    end
  end

  # With options it reads the members and decides; at cluster feature level 4
  # that runs as one replicated entry, so concurrent clients each see one
  # outcome (one ZADD NX adds the member, one ZADD CH reports the change).
  defp zadd_validated(key, opts, score_member_pairs, %FerricStore.Instance{} = ctx) do
    case Ops.compound_command(ctx, key, {:zadd, key, opts, score_member_pairs}) do
      :unsupported -> do_zadd_parsed(key, opts, score_member_pairs, ctx)
      result -> result
    end
  end

  defp zadd_validated(key, opts, score_member_pairs, store),
    do: do_zadd_parsed(key, opts, score_member_pairs, store)

  defp do_zadd_parsed(key, opts, score_member_pairs, store) do
    opts = %{
      nx: :nx in opts,
      xx: :xx in opts,
      gt: :gt in opts,
      lt: :lt in opts,
      ch: :ch in opts
    }

    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.command_check_or_set_status(key, :zset, store) do
      unique_members =
        score_member_pairs |> Enum.map(fn {_score, member} -> member end) |> Enum.uniq()

      compound_keys = Enum.map(unique_members, &CompoundKey.zset_member(key, &1))

      case read_zadd_current_values(key, compound_keys, store, type_status) do
        {:ok, current_values} ->
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
                  existing_score = Helpers.parse_stored_score(existing)

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
          result = if opts.ch, do: added + changed, else: added
          persist_zadd_entries(key, write_entries, result, store, type_status)

        error ->
          error
      end
    end
  end

  defp rollback_new_zset_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.rollback_created_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:zset_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_zset_type_marker(_key, _store, :ok, write_error), do: write_error

  defp persist_zadd_entries(key, [], result, store, {:ok, :created}) do
    case TypeRegistry.rollback_created_type(key, store) do
      :ok -> result
      {:error, _} = error -> error
    end
  end

  defp persist_zadd_entries(_key, [], result, _store, :ok), do: result

  defp persist_zadd_entries(key, entries, result, store, type_status) do
    case Ops.compound_batch_put(store, key, entries) do
      :ok -> result
      {:error, _} = err -> rollback_new_zset_type_marker(key, store, type_status, err)
    end
  end

  defp read_zadd_current_values(key, compound_keys, store, type_status) do
    case read_score_values(key, compound_keys, store) do
      {:ok, values} ->
        {:ok, values}

      {:error, _} = error ->
        rollback_new_zset_type_marker(key, store, type_status, error)
    end
  end

  defp read_score_values(key, compound_keys, store) do
    values = Ops.compound_batch_get(store, key, compound_keys)

    case ReadResult.first_failure(values) do
      nil -> {:ok, values}
      failure -> ReadResult.command_error(failure)
    end
  end

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
           TypeRegistry.command_check_or_set_status(key, :zset, store) do
      zincrby_member(key, increment, member, store, type_status)
    end
  end

  defp parse_zincrby_increment(increment_str) do
    case Helpers.parse_score(increment_str) do
      {:ok, increment} -> {:ok, increment}
      :error -> {:error, "ERR value is not a valid float"}
    end
  end

  # On the instance (the gateways' store) the increment is one replicated
  # read-modify-write; reading here and writing back lost concurrent
  # increments. Inside a transaction the code below already runs in the state
  # machine.
  defp zincrby_member(key, increment, member, %FerricStore.Instance{} = ctx, type_status) do
    case Ops.zincrby(ctx, key, increment, member) do
      {:error, _reason} = error -> rollback_new_zset_type_marker(key, ctx, type_status, error)
      new_score -> Helpers.format_score_str(new_score)
    end
  end

  defp zincrby_member(key, increment, member, store, type_status) do
    compound_key = CompoundKey.zset_member(key, member)
    existing = Ops.compound_get(store, key, compound_key)

    case existing do
      {:error, {:storage_read_failed, _reason}} = failure ->
        rollback_new_zset_type_marker(key, store, type_status, ReadResult.command_error(failure))

      value ->
        current_score =
          case value do
            nil ->
              0.0

            score_str ->
              Helpers.parse_stored_score(score_str)
          end

        case Helpers.checked_score_add(current_score, increment) do
          {:ok, new_score} ->
            write_zscore(store, key, compound_key, new_score, type_status)

          :overflow ->
            {:error, "ERR resulting score is not a number (NaN)"}
        end
    end
  end

  defp write_zscore(store, key, compound_key, new_score, type_status) do
    case Ops.compound_put(store, key, compound_key, Float.to_string(new_score), 0) do
      :ok -> Helpers.format_score(new_score)
      true -> Helpers.format_score(new_score)
      {:error, _reason} = error -> rollback_new_zset_type_marker(key, store, type_status, error)
      other -> {:error, other}
    end
  end

  # ZRANGE ... BYLEX walks the members in order, as Redis does: from the first
  # at or after min while they lie at or before max (with REV, the bounds
  # come max first and the walk runs backwards). It reads every member.
  defp zrange_lex(key, start, stop, %{rev: rev?, limit: limit}, store) do
    {min, max} = if rev?, do: {stop, start}, else: {start, stop}

    with {:ok, min, max} <- Helpers.parse_lex_range(min, max),
         {:ok, offset, count} <- Helpers.parse_range_limit(limit),
         members when is_list(members) <- zrange_rank_parsed(key, 0, -1, false, rev?, store) do
      {from, to} =
        if rev?,
          do: {&Helpers.lex_before_max?(&1, max), &Helpers.lex_after_min?(&1, min)},
          else: {&Helpers.lex_after_min?(&1, min), &Helpers.lex_before_max?(&1, max)}

      members
      |> Enum.drop_while(&(not from.(&1)))
      |> Enum.take_while(to)
      |> Helpers.apply_limit(offset, count)
    end
  end

  defp zrange_rank_parsed(key, start, stop, with_scores, reverse?, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store),
         {:ok, {start_idx, stop_idx}} <- normalize_rank_bounds(key, start, stop, store) do
      zrange_rank_from_index_or_scan(key, start_idx, stop_idx, with_scores, reverse?, store)
    end
  end

  # As in Redis, a negative start before the first member clamps to it, while
  # a negative stop before the first member leaves the range empty.
  defp normalize_rank_bounds(key, start, stop, store) when start < 0 or stop < 0 do
    case zcard_count(key, store) do
      len when is_integer(len) ->
        stop_idx = if stop < 0, do: len + stop, else: stop
        {:ok, {Helpers.normalize_index(start, len), stop_idx}}

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_rank_bounds(_key, start, stop, _store), do: {:ok, {start, stop}}

  defp zrange_rank_from_index_or_scan(_key, start_idx, stop_idx, _with_scores, _reverse?, _store)
       when start_idx > stop_idx do
    []
  end

  defp zrange_rank_from_index_or_scan(key, start_idx, stop_idx, with_scores, reverse?, store) do
    case Ops.zset_rank_range(store, key, start_idx, stop_idx, reverse?) do
      {:ok, members} ->
        format_rank_members(members, with_scores)

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      :unavailable ->
        with {:ok, members} <- load_sorted_members(key, store) do
          sorted = if reverse?, do: Enum.reverse(members), else: members
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
  end

  defp format_rank_members(members, true) do
    Helpers.score_pairs_to_flat_list(members)
  end

  defp format_rank_members(members, false) do
    Enum.map(members, fn {member, _score} -> member end)
  end

  defp zcount_parsed(key, min_bound, max_bound, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      case Ops.zset_score_count(store, key, min_bound, max_bound) do
        {:ok, count} ->
          count

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        :unavailable ->
          with {:ok, members} <- load_members(key, store) do
            Enum.count(members, fn {_member, score} ->
              Helpers.score_gte_bound?(score, min_bound) and
                Helpers.score_lte_bound?(score, max_bound)
            end)
          end
      end
    end
  end

  # On the instance the pop is one replicated read-modify-write. Reading the
  # lowest members here and deleting them in a later write let concurrent
  # clients pop the same members.
  defp zpop_parsed(key, count, reverse?, %FerricStore.Instance{} = ctx)
       when is_integer(count) and count >= 0 do
    with :ok <- TypeRegistry.command_check_type(key, :zset, ctx) do
      if reverse?, do: Ops.zpopmax(ctx, key, count), else: Ops.zpopmin(ctx, key, count)
    end
  end

  defp zpop_parsed(key, count, reverse?, store) when is_integer(count) and count >= 0 do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store),
         {:ok, to_pop} <- zpop_members(key, count, reverse?, store) do
      result = Helpers.score_pairs_to_flat_list(to_pop)

      compound_keys =
        Enum.map(to_pop, fn {member, _score} -> CompoundKey.zset_member(key, member) end)

      removed_entries =
        Enum.zip(compound_keys, to_pop)
        |> Enum.map(fn {compound_key, {_member, score}} ->
          {compound_key, Float.to_string(score), 0}
        end)

      with :ok <- delete_zset_members_and_cleanup(key, removed_entries, length(to_pop), store) do
        result
      end
    end
  end

  defp zpop_parsed(_key, _count, _reverse?, _store), do: zpop_count_error()

  # Redis's error for a ZPOPMIN or ZPOPMAX count that is negative or no integer.
  defp zpop_count_error, do: {:error, "ERR value is out of range, must be positive"}

  defp zpop_members(_key, 0, _reverse?, _store), do: {:ok, []}

  defp zpop_members(key, count, reverse?, store) do
    case Ops.zset_rank_range(store, key, 0, count - 1, reverse?) do
      {:ok, members} ->
        {:ok, members}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      :unavailable ->
        with {:ok, members} <- load_sorted_members(key, store) do
          sorted = if reverse?, do: Enum.reverse(members), else: members
          {:ok, Enum.take(sorted, count)}
        end
    end
  end

  defp zrangebyscore_parsed(key, min_bound, max_bound, opts, reverse?, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store),
         {:ok, with_scores, offset, count} <- Helpers.typed_range_by_score_opts(opts),
         {:ok, filtered} <-
           score_range_slice(key, min_bound, max_bound, reverse?, offset, count, store) do
      if with_scores do
        Helpers.score_pairs_to_flat_list(filtered)
      else
        Enum.map(filtered, fn {member, _score} -> member end)
      end
    end
  end

  defp zrangebyscore_full_range(key, min_bound, max_bound, reverse?, offset, count, store) do
    case Ops.zset_score_range(store, key, min_bound, max_bound, reverse?) do
      {:ok, members} ->
        {:ok, Helpers.apply_limit(members, offset, count)}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      :unavailable ->
        with {:ok, members} <- load_members(key, store) do
          filtered =
            members
            |> Enum.filter(fn {_member, score} ->
              Helpers.score_gte_bound?(score, min_bound) and
                Helpers.score_lte_bound?(score, max_bound)
            end)
            |> sort_members(reverse?)

          {:ok, Helpers.apply_limit(filtered, offset, count)}
        end
    end
  end

  defp load_sorted_members(key, store) do
    with {:ok, members} <- load_members(key, store) do
      {:ok, sort_members(members, false)}
    end
  end

  defp zcard_count(key, store) do
    zset_member_count(key, store)
  end

  defp zset_member_count(key, store) do
    case Ops.zset_score_count(store, key, :neg_inf, :inf) do
      {:ok, count} ->
        count

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      :unavailable ->
        prefix = CompoundKey.zset_prefix(key)
        store |> Ops.compound_count(key, prefix) |> ReadResult.command_result()
    end
  end

  defp load_members(key, store) do
    with {:ok, pairs} <- scan_pairs(key, store) do
      {:ok,
       Enum.map(pairs, fn {member, score_str} ->
         {member, Helpers.parse_stored_score(score_str)}
       end)}
    end
  end

  defp score_range_slice(key, min_bound, max_bound, reverse?, offset, count, store) do
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
        {:ok, members}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      :unavailable ->
        zrangebyscore_full_range(key, min_bound, max_bound, reverse?, offset, count, store)
    end
  end

  defp scan_pairs(key, store) do
    prefix = CompoundKey.zset_prefix(key)

    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      pairs when is_list(pairs) -> {:ok, pairs}
    end
  end

  defp sort_members(members, false) do
    Enum.sort_by(members, fn {member, score} -> {score, member} end)
  end

  defp sort_members(members, true), do: members |> sort_members(false) |> Enum.reverse()

  defp validate_zadd_ast(opts, score_member_pairs)
       when is_list(opts) and is_list(score_member_pairs) do
    cond do
      Enum.any?(opts, &(&1 not in [:nx, :xx, :gt, :lt, :ch, :incr])) ->
        {:error, "ERR syntax error"}

      :nx in opts and :xx in opts ->
        {:error, "ERR XX and NX options at the same time are not compatible"}

      (:gt in opts and :lt in opts) or (:nx in opts and (:gt in opts or :lt in opts)) ->
        {:error, "ERR GT, LT, and/or NX options at the same time are not compatible"}

      score_member_pairs == [] ->
        {:error, "ERR wrong number of arguments for 'zadd' command"}

      :incr in opts and length(score_member_pairs) > 1 ->
        {:error, "ERR INCR option supports a single increment-element pair"}

      true ->
        validate_zadd_pairs(score_member_pairs)
    end
  end

  defp validate_zadd_ast(_opts, {:error, reason}) when is_binary(reason), do: {:error, reason}
  defp validate_zadd_ast(_opts, _score_member_pairs), do: {:error, "ERR syntax error"}

  defp validate_zadd_pairs(score_member_pairs) do
    Enum.reduce_while(score_member_pairs, :ok, fn
      {score, member}, :ok when is_float(score) and is_binary(member) ->
        {:cont, :ok}

      {{:error, reason}, _member}, :ok when is_binary(reason) ->
        {:halt, {:error, reason}}

      _invalid, :ok ->
        {:halt, {:error, "ERR syntax error"}}
    end)
  end
end
