defmodule Ferricstore.Commands.SortedSet.Algebra do
  @moduledoc false

  # ZUNION, ZINTER, ZDIFF, their STORE forms, ZINTERCARD and ZRANGESTORE, as
  # Redis answers them. A source may be a sorted set, a set (its members score
  # 1) or missing (empty). Scores stay finite: a weight or a sum past the
  # largest double is refused, as ZINCRBY refuses one.

  alias Ferricstore.Commands.Set.Destination
  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Commands.SortedSet.Helpers
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}
  alias Ferricstore.Transaction.SingleCommand

  @commands ~w(ZUNION ZINTER ZDIFF ZUNIONSTORE ZINTERSTORE ZDIFFSTORE ZINTERCARD ZRANGESTORE)
  @wrongtype "WRONGTYPE Operation against a key holding the wrong kind of value"
  @overflow "ERR resulting score is not a number (NaN)"
  @syntax "ERR syntax error"

  # Releases before feature level 10 have none of these commands, so a
  # transaction may only hold one from then (`ReplicaCompat`).
  @transaction_level 10

  @doc false
  def commands, do: @commands

  @doc false
  def transaction_level, do: @transaction_level

  def handle("ZUNIONSTORE", [destination | args], store),
    do: store_op(:union, "ZUNIONSTORE", destination, args, store)

  def handle("ZINTERSTORE", [destination | args], store),
    do: store_op(:inter, "ZINTERSTORE", destination, args, store)

  def handle("ZDIFFSTORE", [destination | args], store),
    do: store_op(:diff, "ZDIFFSTORE", destination, args, store)

  def handle(command, args, store) when command in ~w(ZUNION ZINTER ZDIFF),
    do: read_op(op(command), command, args, store)

  def handle("ZINTERCARD", [numkeys | args], store) do
    with {:ok, keys, limit} <- parse_intercard(numkeys, args),
         {:ok, sources} <- read_sources(keys, store) do
      # MIN keeps every score finite; only the members count.
      sources |> combine(:inter, nil, :min) |> elem(1) |> map_size() |> limit_count(limit)
    end
  end

  def handle("ZRANGESTORE", [destination, source, min, max | options], store) do
    if Enum.any?(options, &(String.upcase(&1) == "WITHSCORES")) do
      {:error, @syntax}
    else
      store_result(destination, [source], store, fn unified_store ->
        range_pairs(source, min, max, options, unified_store)
      end)
      |> run_in_apply("ZRANGESTORE", store, [
        destination,
        source,
        min,
        max | options
      ])
    end
  end

  def handle(command, _args, _store),
    do: {:error, "ERR wrong number of arguments for '#{String.downcase(command)}' command"}

  # ---------------------------------------------------------------------------
  # Store and read forms
  # ---------------------------------------------------------------------------

  defp store_op(op, command, destination, args, store) do
    with {:ok, keys, weights, aggregate} <- parse_inputs(op, command, args, false) do
      store_result(destination, keys, store, fn unified_store ->
        with {:ok, sources} <- read_sources(keys, unified_store) do
          sources |> combine(op, weights, aggregate) |> checked_pairs()
        end
      end)
      |> run_in_apply(command, store, [destination | args])
    end
  end

  defp read_op(op, command, args, store) do
    with {:ok, keys, weights, aggregate, with_scores} <- parse_inputs(op, command, args, true),
         {:ok, sources} <- read_sources(keys, store),
         {:ok, pairs} <- sources |> combine(op, weights, aggregate) |> checked_pairs() do
      reply(pairs, with_scores)
    end
  end

  # A STORE reads its sources and writes its destination. Run from a client
  # it read them on its own node, which may not have applied the latest
  # write yet, and a write in between was lost. From feature level 10 it runs
  # as a one-command transaction: the shard computes the result from what it
  # has applied and writes it with nothing in between. `result` is a function
  # of the store, so the older path runs only when that one cannot.
  defp run_in_apply(result, command, store, args) do
    case SingleCommand.run(store, command, args, level: @transaction_level) do
      :fallback -> result.()
      answer -> answer
    end
  end

  defp store_result(destination, keys, store, pairs_fun) do
    fn ->
      keys_with_roles = [{destination, :write} | Enum.map(keys, &{&1, :read})]

      CrossShardOp.execute(
        keys_with_roles,
        fn unified_store ->
          with {:ok, pairs} <- pairs_fun.(unified_store) do
            Destination.store_zset_at(destination, pairs, unified_store)
          end
        end,
        store: store
      )
    end
  end

  defp reply(pairs, false), do: Enum.map(pairs, fn {member, _score} -> member end)

  defp reply(pairs, true),
    do: Enum.flat_map(pairs, fn {member, score} -> [member, Helpers.format_score(score)] end)

  defp limit_count(count, 0), do: count
  defp limit_count(count, limit), do: min(count, limit)

  # ---------------------------------------------------------------------------
  # Parsing
  # ---------------------------------------------------------------------------

  defp op("ZUNION"), do: :union
  defp op("ZINTER"), do: :inter
  defp op("ZDIFF"), do: :diff

  defp parse_inputs(op, command, [numkeys | rest], read?) do
    name = String.downcase(command)

    with {:ok, count} <- parse_numkeys(numkeys, name),
         {keys, options} when length(keys) == count <- Enum.split(rest, count),
         {:ok, parsed} <- parse_options(options, op, count, read?, %{}) do
      weights = Map.get(parsed, :weights)
      aggregate = Map.get(parsed, :aggregate, :sum)

      if read?,
        do: {:ok, keys, weights, aggregate, Map.get(parsed, :with_scores, false)},
        else: {:ok, keys, weights, aggregate}
    else
      {:error, _reason} = error -> error
      {keys, _options} when is_list(keys) -> {:error, @syntax}
    end
  end

  defp parse_inputs(_op, command, [], _read?),
    do: {:error, "ERR wrong number of arguments for '#{String.downcase(command)}' command"}

  defp parse_numkeys(numkeys, name) do
    case Integer.parse(numkeys) do
      {count, ""} when count >= 1 and count <= 2_147_483_647 ->
        {:ok, count}

      {count, ""} when count < 1 ->
        {:error, "ERR at least 1 input key is needed for '#{name}' command"}

      _invalid ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_options([], _op, _count, _read?, parsed), do: {:ok, parsed}

  defp parse_options([option | rest], op, count, read?, parsed) do
    case String.upcase(option) do
      "WEIGHTS" when op != :diff and length(rest) >= count ->
        {weights, rest} = Enum.split(rest, count)

        with {:ok, weights} <- parse_weights(weights) do
          parse_options(rest, op, count, read?, Map.put(parsed, :weights, weights))
        end

      "AGGREGATE" when op != :diff and rest != [] ->
        [mode | rest] = rest

        case String.upcase(mode) do
          "SUM" -> parse_options(rest, op, count, read?, Map.put(parsed, :aggregate, :sum))
          "MIN" -> parse_options(rest, op, count, read?, Map.put(parsed, :aggregate, :min))
          "MAX" -> parse_options(rest, op, count, read?, Map.put(parsed, :aggregate, :max))
          _other -> {:error, @syntax}
        end

      "WITHSCORES" when read? ->
        parse_options(rest, op, count, read?, Map.put(parsed, :with_scores, true))

      _other ->
        {:error, @syntax}
    end
  end

  defp parse_weights(weights) do
    Enum.reduce_while(weights, {:ok, []}, fn weight, {:ok, acc} ->
      case Helpers.parse_score(weight) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        :error -> {:halt, {:error, "ERR weight value is not a float"}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp parse_intercard(numkeys, args) do
    with {count, ""} when count > 0 <- Integer.parse(numkeys),
         {keys, tail} when length(keys) == count <- Enum.split(args, count) do
      parse_intercard_limit(keys, tail)
    else
      {keys, _tail} when is_list(keys) ->
        {:error, "ERR Number of keys can't be greater than number of args"}

      _invalid ->
        {:error, "ERR numkeys should be greater than 0"}
    end
  end

  defp parse_intercard_limit(keys, []), do: {:ok, keys, 0}

  defp parse_intercard_limit(keys, [option, limit]) do
    if String.upcase(option) == "LIMIT" do
      case Integer.parse(limit) do
        {limit, ""} when limit >= 0 -> {:ok, keys, limit}
        _invalid -> {:error, "ERR LIMIT can't be negative"}
      end
    else
      {:error, @syntax}
    end
  end

  defp parse_intercard_limit(_keys, _tail), do: {:error, @syntax}

  # ---------------------------------------------------------------------------
  # Sources and the combination
  # ---------------------------------------------------------------------------

  defp read_sources(keys, store) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case read_source(key, store) do
        {:ok, pairs} -> {:cont, {:ok, [pairs | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp read_source(key, store) do
    case TypeRegistry.check_type(key, :zset, store) do
      :ok ->
        scan(key, CompoundKey.zset_prefix(key), store, &Helpers.parse_stored_score/1)

      {:error, @wrongtype} ->
        case TypeRegistry.check_type(key, :set, store) do
          :ok -> scan(key, CompoundKey.set_prefix(key), store, fn _marker -> 1.0 end)
          other -> ReadResult.command_result(other)
        end

      other ->
        ReadResult.command_result(other)
    end
  end

  defp scan(key, prefix, store, score_fun) do
    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      entries when is_list(entries) ->
        {:ok, Enum.map(entries, fn {member, value} -> {member, score_fun.(value)} end)}
    end
  end

  # Answers {:ok, %{member => score}} or :overflow.
  defp combine([first | rest], op, weights, aggregate) do
    weights = weights || List.duplicate(1.0, length(rest) + 1)
    [first_weight | rest_weights] = weights

    try do
      initial = weighted(first, if(op == :diff, do: 1.0, else: first_weight))

      result =
        rest
        |> Enum.zip(rest_weights)
        |> Enum.reduce(initial, fn {pairs, weight}, acc ->
          step(op, acc, pairs, weight, aggregate)
        end)

      {:ok, result}
    rescue
      ArithmeticError -> {:ok, :overflow}
    end
  end

  defp weighted(pairs, weight),
    do: Map.new(pairs, fn {member, score} -> {member, weight * score} end)

  defp step(:union, acc, pairs, weight, aggregate) do
    Enum.reduce(pairs, acc, fn {member, score}, acc ->
      value = weight * score
      Map.update(acc, member, value, &aggregate(aggregate, &1, value))
    end)
  end

  defp step(:inter, acc, pairs, weight, aggregate) do
    Enum.reduce(pairs, %{}, fn {member, score}, kept ->
      case Map.fetch(acc, member) do
        {:ok, current} -> Map.put(kept, member, aggregate(aggregate, current, weight * score))
        :error -> kept
      end
    end)
  end

  defp step(:diff, acc, pairs, _weight, _aggregate),
    do: Map.drop(acc, Enum.map(pairs, fn {member, _score} -> member end))

  defp aggregate(:sum, left, right), do: left + right
  defp aggregate(:min, left, right), do: min(left, right)
  defp aggregate(:max, left, right), do: max(left, right)

  defp checked_pairs({:ok, :overflow}), do: {:error, @overflow}

  defp checked_pairs({:ok, scores}),
    do: {:ok, Enum.sort_by(scores, fn {member, score} -> {score, member} end)}

  # ---------------------------------------------------------------------------
  # ZRANGESTORE
  # ---------------------------------------------------------------------------

  # ZRANGE reads the range (by rank, score or member, on any layout); BYLEX
  # answers no scores, so ZMSCORE reads them.
  defp range_pairs(source, min, max, options, store) do
    if Enum.any?(options, &(String.upcase(&1) == "BYLEX")) do
      with members when is_list(members) <-
             SortedSet.handle("ZRANGE", [source, min, max | options], store),
           scores when is_list(scores) <- lex_scores(source, members, store) do
        {:ok, Enum.zip(members, Enum.map(scores, &parse_reply_score/1))}
      end
    else
      case SortedSet.handle("ZRANGE", [source, min, max | options] ++ ["WITHSCORES"], store) do
        flat when is_list(flat) ->
          {:ok,
           flat
           |> Enum.chunk_every(2)
           |> Enum.map(fn [member, score] -> {member, parse_reply_score(score)} end)}

        error ->
          error
      end
    end
  end

  defp lex_scores(_source, [], _store), do: []

  defp lex_scores(source, members, store),
    do: SortedSet.handle("ZMSCORE", [source | members], store)

  # Replies carry the shortest digits that parse back to the stored double.
  defp parse_reply_score(score) do
    {:ok, value} = Helpers.parse_score(score)
    value
  end
end
