defmodule Ferricstore.Commands.SortedSet.RangeCommands do
  @moduledoc false

  # ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT, ZREMRANGEBYRANK, ZREMRANGEBYSCORE,
  # ZREMRANGEBYLEX and ZMPOP, as Redis answers them. They select through
  # ZRANGE (which reads every sorted-set layout), remove through ZREM and pop
  # through ZPOPMIN/ZPOPMAX, so they write as those commands do.

  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Commands.SortedSet.Helpers
  alias Ferricstore.Transaction.SingleCommand

  @commands ~w(ZRANGEBYLEX ZREVRANGEBYLEX ZLEXCOUNT ZREMRANGEBYRANK ZREMRANGEBYSCORE
    ZREMRANGEBYLEX ZMPOP)
  @syntax "ERR syntax error"

  # Releases before feature level 10 have none of these commands, so a
  # transaction may only hold one from then (`ReplicaCompat`).
  @transaction_level 10

  @doc false
  def commands, do: @commands

  def handle("ZRANGEBYLEX", [key, min, max | options], store) do
    with {:ok, limit} <- lex_limit(options) do
      SortedSet.handle("ZRANGE", [key, min, max, "BYLEX" | limit], store)
    end
  end

  # ZRANGE ... BYLEX REV takes the bounds max first, as ZREVRANGEBYLEX does.
  def handle("ZREVRANGEBYLEX", [key, max, min | options], store) do
    with {:ok, limit} <- lex_limit(options) do
      SortedSet.handle("ZRANGE", [key, max, min, "BYLEX", "REV" | limit], store)
    end
  end

  def handle("ZLEXCOUNT", [key, min, max], store) do
    case SortedSet.handle("ZRANGE", [key, min, max, "BYLEX"], store) do
      members when is_list(members) -> length(members)
      error -> error
    end
  end

  def handle("ZREMRANGEBYRANK", [key, start, stop] = args, store) do
    case {Integer.parse(start), Integer.parse(stop)} do
      {{_start, ""}, {_stop, ""}} ->
        remove_range("ZREMRANGEBYRANK", args, store, fn store ->
          SortedSet.handle("ZRANGE", [key, start, stop], store)
        end)

      _invalid ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("ZREMRANGEBYSCORE", [key, min, max] = args, store) do
    with {:ok, _min, _min_exclusive} <- score_bound(min),
         {:ok, _max, _max_exclusive} <- score_bound(max) do
      remove_range("ZREMRANGEBYSCORE", args, store, fn store ->
        SortedSet.handle("ZRANGEBYSCORE", [key, min, max], store)
      end)
    end
  end

  def handle("ZREMRANGEBYLEX", [key, min, max] = args, store) do
    with {:ok, _min, _max} <- Helpers.parse_lex_range(min, max) do
      remove_range("ZREMRANGEBYLEX", args, store, fn store ->
        SortedSet.handle("ZRANGE", [key, min, max, "BYLEX"], store)
      end)
    end
  end

  def handle("ZMPOP", [numkeys | rest] = args, store) do
    with {:ok, keys, pop, count} <- parse_zmpop(numkeys, rest) do
      run_in_apply("ZMPOP", args, store, fn -> zmpop_here(keys, pop, count, store) end)
    end
  end

  def handle(command, _args, _store),
    do: {:error, "ERR wrong number of arguments for '#{String.downcase(command)}' command"}

  # ---------------------------------------------------------------------------

  defp lex_limit([]), do: {:ok, []}

  defp lex_limit([option, offset, count]) do
    if String.upcase(option) == "LIMIT",
      do: {:ok, ["LIMIT", offset, count]},
      else: {:error, @syntax}
  end

  defp lex_limit(_options), do: {:error, @syntax}

  defp score_bound(bound) do
    case Helpers.parse_score_bound(bound) do
      {:ok, _value, _exclusive} = ok -> ok
      _invalid -> {:error, "ERR min or max is not a float"}
    end
  end

  # The members in range are read and then removed. From feature level 10 the
  # shard does both in one transaction, so a member added or rescored in
  # between, or a node that has not applied the latest write yet, cannot
  # change what is removed.
  defp remove_range(command, [key | _rest] = args, store, members_fun) do
    run_in_apply(command, args, store, fn ->
      case members_fun.(store) do
        [] -> 0
        members when is_list(members) -> SortedSet.handle("ZREM", [key | members], store)
        error -> error
      end
    end)
  end

  defp run_in_apply(command, args, store, here) do
    case SingleCommand.run(store, command, args, level: @transaction_level) do
      :fallback -> here.()
      answer -> answer
    end
  end

  defp parse_zmpop(numkeys, rest) do
    with {count, ""} when count > 0 <- Integer.parse(numkeys),
         {keys, [where | options]} when length(keys) == count <- Enum.split(rest, count),
         {:ok, pop} <- zmpop_direction(where),
         {:ok, pop_count} <- zmpop_count(options) do
      {:ok, keys, pop, pop_count}
    else
      {:error, _reason} = error -> error
      {keys, _options} when is_list(keys) -> {:error, @syntax}
      _invalid -> {:error, "ERR numkeys should be greater than 0"}
    end
  end

  defp zmpop_direction(where) do
    case String.upcase(where) do
      "MIN" -> {:ok, "ZPOPMIN"}
      "MAX" -> {:ok, "ZPOPMAX"}
      _other -> {:error, @syntax}
    end
  end

  defp zmpop_count([]), do: {:ok, 1}

  defp zmpop_count([option, count]) do
    if String.upcase(option) == "COUNT" do
      case Integer.parse(count) do
        {count, ""} when count > 0 -> {:ok, count}
        _invalid -> {:error, "ERR count should be greater than 0"}
      end
    else
      {:error, @syntax}
    end
  end

  defp zmpop_count(_options), do: {:error, @syntax}

  # The first key that is not empty answers; a key of another type is refused.
  defp zmpop_here(keys, pop, count, store) do
    Enum.reduce_while(keys, nil, fn key, nil ->
      case SortedSet.handle(pop, [key, Integer.to_string(count)], store) do
        [] -> {:cont, nil}
        flat when is_list(flat) -> {:halt, [key, Enum.chunk_every(flat, 2)]}
        error -> {:halt, error}
      end
    end)
  end
end
