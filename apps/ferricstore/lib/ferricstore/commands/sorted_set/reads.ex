defmodule Ferricstore.Commands.SortedSet.Reads do
  @moduledoc false

  alias Ferricstore.Commands.SortedSet.Helpers
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  def zscore_member(key, member, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_key = CompoundKey.zset_member(key, member)

      case Ops.compound_get(store, key, compound_key) do
        nil -> nil
        score_str -> Helpers.format_score_str(score_str)
      end
    end
  end

  def zrank_member(key, member, reverse?, store) do
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

  def zcard_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      zset_member_count(key, store)
    end
  end

  def zrandmember_one(key, store) do
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

  def zmscore_args([key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      compound_keys = Enum.map(members, &CompoundKey.zset_member(key, &1))

      store
      |> Ops.compound_batch_get(key, compound_keys)
      |> Enum.map(fn
        nil -> nil
        score_str -> Helpers.format_score_str(score_str)
      end)
    end
  end

  def zmscore_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'zmscore' command"}
  end

  def zrandmember_parsed(key, count, with_scores, store) do
    with :ok <- TypeRegistry.check_type(key, :zset, store) do
      if count == 0 do
        []
      else
        prefix = CompoundKey.zset_prefix(key)
        pairs = Ops.compound_scan(store, key, prefix)
        Helpers.select_random_members(pairs, count, with_scores)
      end
    end
  end

  def zscan_parsed(_key, cursor, _opts, _store) when is_integer(cursor) and cursor < 0 do
    {:error, "ERR invalid cursor"}
  end

  def zscan_parsed(key, cursor, opts, store) when is_integer(cursor) and cursor >= 0 do
    with :ok <- TypeRegistry.check_type(key, :zset, store),
         {:ok, match_pattern, count} <- Helpers.typed_scan_opts(opts) do
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

  def zscan_parsed(_key, _cursor, _opts, _store), do: {:error, "ERR invalid cursor"}

  defp load_sorted_members(key, store) do
    key
    |> load_members(store)
    |> sort_members(false)
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
