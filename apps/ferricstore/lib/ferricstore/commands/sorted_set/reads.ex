defmodule Ferricstore.Commands.SortedSet.Reads do
  @moduledoc false

  alias Ferricstore.Commands.CollectionScan
  alias Ferricstore.Commands.SortedSet.Helpers
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.TypeRegistry

  def zscore_member(key, member, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      compound_key = CompoundKey.zset_member(key, member)

      case Ops.compound_get(store, key, compound_key) do
        nil ->
          nil

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        score_str ->
          Helpers.format_score_str(score_str)
      end
    end
  end

  def zrank_member(key, member, reverse?, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      case Ops.zset_member_rank(store, key, member, reverse?) do
        {:ok, rank} ->
          rank

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        :unavailable ->
          with {:ok, sorted} <- load_sorted_members(key, store) do
            ranked = if reverse?, do: Enum.reverse(sorted), else: sorted

            case Enum.find_index(ranked, fn {m, _s} -> m == member end) do
              nil -> nil
              idx -> idx
            end
          end
      end
    end
  end

  def zcard_key(key, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      zset_member_count(key, store)
    end
  end

  def zrandmember_one(key, store) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store),
         {:ok, pairs} <- scan_pairs(key, store) do
      case pairs do
        [] ->
          nil

        _ ->
          {member, _score} = Enum.random(pairs)
          member
      end
    end
  end

  def zmscore_args([key | members], store) when members != [] do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      compound_keys = Enum.map(members, &CompoundKey.zset_member(key, &1))
      values = Ops.compound_batch_get(store, key, compound_keys)

      case ReadResult.first_failure(values) do
        nil ->
          Enum.map(values, fn
            nil -> nil
            score_str -> Helpers.format_score_str(score_str)
          end)

        failure ->
          ReadResult.command_error(failure)
      end
    end
  end

  def zmscore_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'zmscore' command"}
  end

  def zrandmember_parsed(key, count, with_scores, store)
      when is_integer(count) and is_boolean(with_scores) do
    with :ok <- TypeRegistry.command_check_type(key, :zset, store) do
      if count == 0 do
        []
      else
        with {:ok, pairs} <- scan_pairs(key, store) do
          Helpers.select_random_members(pairs, count, with_scores)
        end
      end
    end
  end

  def zrandmember_parsed(_key, _count, _with_scores, _store),
    do: {:error, "ERR value is not an integer or out of range"}

  def zscan_parsed(key, cursor, opts, store) do
    if CollectionScan.valid_cursor?(cursor) do
      with :ok <- TypeRegistry.command_check_type(key, :zset, store),
           {:ok, match_pattern, count} <- Helpers.typed_scan_opts(opts),
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
    else
      {:error, "ERR invalid cursor"}
    end
  end

  defp load_sorted_members(key, store) do
    with {:ok, members} <- load_members(key, store) do
      {:ok, sort_members(members, false)}
    end
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
end
