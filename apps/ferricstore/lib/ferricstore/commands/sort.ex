defmodule Ferricstore.Commands.Sort do
  @moduledoc """
  SORT and SORT_RO, as Redis in cluster mode answers them.

  The elements of a list, set or sorted set sort as numbers, or as strings
  with ALPHA, optionally by weights read from keys named by a BY pattern,
  and answer themselves or values named by GET patterns (`#` the element,
  `key_*` a string, `hash_*->field` a hash field). A BY pattern without `*`
  keeps the collection's order. As in Redis Cluster, BY and GET patterns are
  taken only when the keys they name share the sorted key's hash slot (a
  `{tag}` before any wildcard), and only for users with access to every key
  (`Ferricstore.Commands.KeyDiscovery`).

  SORT ... STORE replaces the destination with the result as a list, or
  deletes it when the result is empty. From cluster feature level 12 the
  read and the write run as one-command transaction, where they apply;
  older releases do not have SORT, so below it STORE is refused.
  """

  alias Ferricstore.Commands.{Hash, List, Set, SortedSet, Strings}
  alias Ferricstore.Store.{SlotMap, TypeRegistry}
  alias Ferricstore.Transaction.{ReplicaCompat, SingleCommand}

  @store_level 12
  @syntax "ERR syntax error"
  @integer_error "ERR value is not an integer or out of range"
  @score_error "ERR One or more scores can't be converted into double"
  @wrongtype "WRONGTYPE Operation against a key holding the wrong kind of value"

  @spec handle(binary(), [binary()], term()) :: term()
  def handle(command, [key | options] = args, store) when command in ["SORT", "SORT_RO"] do
    with {:ok, opts} <- parse(command, options),
         :ok <- check_pattern_slots(key, opts) do
      case opts.store do
        nil -> sorted(key, opts, store)
        destination -> store_sorted(command, args, key, destination, opts, store)
      end
    end
  end

  def handle(command, _args, _store),
    do: {:error, "ERR wrong number of arguments for '#{String.downcase(command)}' command"}

  @doc """
  The keys SORT reads or writes beyond the ones it names: `["*"]` when a BY
  or GET pattern reads keys named by the elements, else `[]`.
  """
  @spec pattern_acl_keys([binary()]) :: [binary()]
  def pattern_acl_keys([_key | options]) do
    case parse("SORT", options) do
      {:ok, %{by: by, gets: gets}} ->
        if dereferences?(by) or Enum.any?(gets, &(&1 != "#" and dereferences?(&1))),
          do: ["*"],
          else: []

      {:error, _reason} ->
        []
    end
  end

  def pattern_acl_keys(_args), do: []

  @doc "The STORE destination of SORT's arguments, if any."
  @spec store_destination([binary()]) :: binary() | nil
  def store_destination([_key | options]) do
    case parse("SORT", options) do
      {:ok, %{store: destination}} -> destination
      {:error, _reason} -> nil
    end
  end

  def store_destination(_args), do: nil

  # ---------------------------------------------------------------------------
  # Options

  defp parse(command, options) do
    parse(command, options, %{
      by: nil,
      limit: nil,
      gets: [],
      desc: false,
      alpha: false,
      store: nil
    })
  end

  defp parse(_command, [], opts), do: {:ok, %{opts | gets: Enum.reverse(opts.gets)}}

  defp parse(command, [option | rest], opts) do
    case {String.upcase(option), rest} do
      {"ASC", rest} ->
        parse(command, rest, %{opts | desc: false})

      {"DESC", rest} ->
        parse(command, rest, %{opts | desc: true})

      {"ALPHA", rest} ->
        parse(command, rest, %{opts | alpha: true})

      {"BY", [pattern | rest]} ->
        parse(command, rest, %{opts | by: pattern})

      {"GET", [pattern | rest]} ->
        parse(command, rest, %{opts | gets: [pattern | opts.gets]})

      {"LIMIT", [offset, count | rest]} ->
        with {:ok, offset} <- integer(offset),
             {:ok, count} <- integer(count),
             do: parse(command, rest, %{opts | limit: {offset, count}})

      {"STORE", [destination | rest]} when command == "SORT" ->
        parse(command, rest, %{opts | store: destination})

      _other ->
        {:error, @syntax}
    end
  end

  defp integer(raw) do
    case Integer.parse(raw) do
      {value, ""} -> {:ok, value}
      _invalid -> {:error, @integer_error}
    end
  end

  defp dereferences?(nil), do: false
  defp dereferences?(pattern), do: String.contains?(pattern, "*")

  # Redis Cluster takes a BY or GET pattern only when the keys it names share
  # the sorted key's slot: the pattern's `{tag}` must come before any
  # wildcard (`patternHashSlot`).
  defp check_pattern_slots(key, %{by: by, gets: gets}) do
    slot = SlotMap.slot_for_key(key)

    cond do
      dereferences?(by) and pattern_slot(by) != slot ->
        {:error, denied("BY")}

      Enum.any?(gets, &(dereferences?(&1) and pattern_slot(&1) != slot)) ->
        {:error, denied("GET")}

      true ->
        :ok
    end
  end

  defp denied(option),
    do:
      "ERR #{option} option of SORT denied in Cluster mode when keys formed by the pattern " <>
        "may be in different slots."

  defp pattern_slot(pattern), do: pattern_slot(pattern, 0, nil)

  defp pattern_slot(pattern, index, _open) when index >= byte_size(pattern),
    do: SlotMap.slot_for_key(pattern)

  defp pattern_slot(pattern, index, open) do
    case :binary.at(pattern, index) do
      char when char in [?*, ??, ?[, ?\\] ->
        :any

      ?{ when open == nil ->
        pattern_slot(pattern, index + 1, index)

      ?} when is_integer(open) and index == open + 1 ->
        pattern_slot(pattern, index + 1, :empty)

      ?} when is_integer(open) ->
        SlotMap.slot_for_key("{" <> binary_part(pattern, open + 1, index - open - 1) <> "}")

      _char ->
        pattern_slot(pattern, index + 1, open)
    end
  end

  # ---------------------------------------------------------------------------
  # Sorting

  defp sorted(key, opts, store) do
    with {:ok, type, elements} <- elements(key, store),
         {:ok, ordered} <- order(type, elements, opts, store) do
      ordered
      |> limit(opts.limit)
      |> project(opts.gets, store)
    end
  end

  defp elements(key, store) do
    case TypeRegistry.get_type(key, store) do
      "none" -> {:ok, :none, []}
      "list" -> read(:list, List.handle("LRANGE", [key, "0", "-1"], store))
      "set" -> read(:set, Set.handle("SMEMBERS", [key], store))
      "zset" -> read(:zset, SortedSet.handle("ZRANGE", [key, "0", "-1"], store))
      {:error, _reason} = error -> error
      _other -> {:error, @wrongtype}
    end
  end

  defp read(type, elements) when is_list(elements), do: {:ok, type, elements}
  defp read(_type, {:error, _reason} = error), do: error

  # A BY pattern without `*` keeps a list's or sorted set's order (reversed
  # by DESC); a set's order is its members' (DESC does not change it, as in
  # Redis, where it is the set's own order).
  defp order(type, elements, %{by: by} = opts, store) when is_binary(by) do
    cond do
      dereferences?(by) -> sort_by_weights(elements, by, opts, store)
      opts.desc and type in [:list, :zset] -> {:ok, Enum.reverse(elements)}
      true -> {:ok, elements}
    end
  end

  defp order(_type, elements, opts, _store), do: sort_elements(elements, opts)

  # Weights read through the BY pattern: a missing one counts as 0, and with
  # ALPHA sorts first.
  defp sort_by_weights(elements, by, %{alpha: true} = opts, store),
    do: {:ok, sort_with(Enum.map(elements, &{lookup(by, &1, store), &1}), opts)}

  defp sort_by_weights(elements, by, opts, store) do
    with {:ok, keyed} <- numeric_keys(elements, &lookup(by, &1, store)),
         do: {:ok, sort_with(keyed, opts)}
  end

  defp sort_elements(elements, %{alpha: true} = opts),
    do: {:ok, sort_with(Enum.map(elements, &{&1, &1}), opts)}

  defp sort_elements(elements, opts) do
    with {:ok, keyed} <- numeric_keys(elements, & &1),
         do: {:ok, sort_with(keyed, opts)}
  end

  defp numeric_keys(elements, weight_of) do
    Enum.reduce_while(elements, {:ok, []}, fn element, {:ok, acc} ->
      case weight_of.(element) do
        nil ->
          {:cont, {:ok, [{{0, 0.0}, element} | acc]}}

        weight ->
          case parse_double(weight) do
            {:ok, score} -> {:cont, {:ok, [{score, element} | acc]}}
            :error -> {:halt, {:error, @score_error}}
          end
      end
    end)
    |> case do
      {:ok, keyed} -> {:ok, Enum.reverse(keyed)}
      error -> error
    end
  end

  # Ties sort by the element, so every node answers the same order.
  defp sort_with(keyed, %{desc: desc}) do
    keyed
    |> Enum.sort(fn {weight_a, a}, {weight_b, b} ->
      compare = compare(weight_a, weight_b)
      compare = if compare == :eq, do: compare(a, b), else: compare
      if desc, do: compare != :lt, else: compare != :gt
    end)
    |> Enum.map(&elem(&1, 1))
  end

  defp compare(a, b) when a < b, do: :lt
  defp compare(a, b) when a > b, do: :gt
  defp compare(_a, _b), do: :eq

  # strtod's forms: optional leading space, a sign, digits, a fraction, an
  # exponent, or inf/infinity; nothing may follow. Scores are {-1 | 0 | 1,
  # value} so the infinities order around every number.
  defp parse_double(raw) do
    trimmed = String.trim_leading(raw)

    case String.downcase(trimmed) do
      inf when inf in ["inf", "+inf", "infinity", "+infinity"] -> {:ok, {1, 0.0}}
      inf when inf in ["-inf", "-infinity"] -> {:ok, {-1, 0.0}}
      _number -> number(trimmed)
    end
  end

  defp number("." <> _fraction = text), do: number("0" <> text)
  defp number("-." <> fraction), do: number("-0." <> fraction)
  defp number("+." <> fraction), do: number("0." <> fraction)

  defp number(text) do
    case Float.parse(text) do
      {value, ""} ->
        {:ok, {0, value}}

      _other ->
        case Integer.parse(text) do
          {value, ""} -> {:ok, {0, value * 1.0}}
          _invalid -> :error
        end
    end
  end

  defp limit(elements, nil), do: elements

  defp limit(elements, {offset, count}) do
    start = max(offset, 0)
    if count < 0, do: Enum.drop(elements, start), else: Enum.slice(elements, start, count)
  end

  defp project(elements, [], _store), do: elements

  defp project(elements, gets, store) do
    Enum.flat_map(elements, fn element -> Enum.map(gets, &lookup(&1, element, store)) end)
  end

  # Redis's lookupKeyByPattern: the first `*` takes the element, and `->field`
  # after it reads that field of a hash; a key of another type reads nil.
  defp lookup("#", element, _store), do: element

  defp lookup(pattern, element, store) do
    case :binary.match(pattern, "*") do
      :nomatch ->
        nil

      {star, 1} ->
        prefix = binary_part(pattern, 0, star)
        rest = binary_part(pattern, star + 1, byte_size(pattern) - star - 1)

        case :binary.match(rest, "->") do
          {arrow, 2} when arrow + 2 < byte_size(rest) ->
            key = prefix <> element <> binary_part(rest, 0, arrow)
            field = binary_part(rest, arrow + 2, byte_size(rest) - arrow - 2)
            value_or_nil(Hash.handle("HGET", [key, field], store))

          _no_field ->
            value_or_nil(Strings.handle_ast({:get, prefix <> element <> rest}, store))
        end
    end
  end

  defp value_or_nil(value) when is_binary(value), do: value
  defp value_or_nil(_missing_or_other_type), do: nil

  # ---------------------------------------------------------------------------
  # STORE

  defp store_sorted(command, args, key, destination, opts, %FerricStore.Instance{} = ctx) do
    if ReplicaCompat.level_active?(ctx, [key], @store_level) do
      case SingleCommand.run(ctx, command, args, level: @store_level) do
        :fallback -> store_here(key, destination, opts, ctx)
        answer -> answer
      end
    else
      {:error,
       "ERR SORT ... STORE needs cluster feature level #{@store_level} " <>
         "(CLUSTER.FEATURES.RAISE #{@store_level} once every node runs this release)"}
    end
  end

  defp store_sorted(_command, _args, key, destination, opts, store),
    do: store_here(key, destination, opts, store)

  defp store_here(key, destination, opts, store) do
    with values when is_list(values) <- sorted(key, opts, store),
         values = Enum.map(values, &(&1 || "")),
         count when is_integer(count) <- Strings.handle_ast({:del, [destination]}, store) do
      case values do
        [] -> 0
        _values -> List.handle("RPUSH", [destination | values], store)
      end
    end
  end
end
