defmodule Ferricstore.Commands.Set do
  @moduledoc """
  Handles Redis set commands: SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER,
  SCARD, SINTER, SUNION, SDIFF, SDIFFSTORE, SINTERSTORE, SUNIONSTORE,
  SINTERCARD, SRANDMEMBER, SPOP, SMOVE, SSCAN.

  Each set member is stored as a compound key:

      S:redis_key\\0member_name -> "1"

  The member name IS the Bitcask sub-key. The value is a presence marker
  `"1"`. This allows O(1) membership testing via direct key lookup.

  ## Type Enforcement

  All set commands check type metadata. Using set commands on a key that
  holds a different type returns WRONGTYPE.
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry
  alias Ferricstore.CrossShardOp

  @presence_marker "1"

  @doc """
  Handles a set command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"SADD"`, `"SMEMBERS"`)
    - `args` - List of string arguments
    - `store` - Injected store map with compound key callbacks

  ## Returns

  Plain Elixir term: integer, list, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # SADD key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("SADD", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_or_set(key, :set, store) do
      Enum.reduce(members, 0, fn member, acc ->
        compound_key = CompoundKey.set_member(key, member)
        existing = Ops.compound_get(store, key, compound_key)
        Ops.compound_put(store, key, compound_key, @presence_marker, 0)
        if existing == nil, do: acc + 1, else: acc
      end)
    end
  end

  def handle("SADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sadd' command"}
  end

  # ---------------------------------------------------------------------------
  # SREM key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("SREM", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      compound_keys =
        members
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.set_member(key, &1))

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

      maybe_cleanup_empty_set(key, removed, store)
      removed
    end
  end

  def handle("SREM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'srem' command"}
  end

  # ---------------------------------------------------------------------------
  # SMEMBERS key
  # ---------------------------------------------------------------------------

  def handle("SMEMBERS", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      prefix = CompoundKey.set_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      Enum.map(pairs, fn {member, _} -> member end)
    end
  end

  def handle("SMEMBERS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'smembers' command"}
  end

  # ---------------------------------------------------------------------------
  # SISMEMBER key member
  # ---------------------------------------------------------------------------

  def handle("SISMEMBER", [key, member], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      compound_key = CompoundKey.set_member(key, member)

      if Ops.compound_get(store, key, compound_key) != nil do
        1
      else
        0
      end
    end
  end

  def handle("SISMEMBER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sismember' command"}
  end

  # ---------------------------------------------------------------------------
  # SMISMEMBER key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("SMISMEMBER", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      compound_keys = Enum.map(members, &CompoundKey.set_member(key, &1))

      store
      |> Ops.compound_batch_get(key, compound_keys)
      |> Enum.map(fn
        nil -> 0
        _value -> 1
      end)
    end
  end

  def handle("SMISMEMBER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'smismember' command"}
  end

  # ---------------------------------------------------------------------------
  # SCARD key
  # ---------------------------------------------------------------------------

  def handle("SCARD", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      prefix = CompoundKey.set_prefix(key)
      Ops.compound_count(store, key, prefix)
    end
  end

  def handle("SCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'scard' command"}
  end

  # ---------------------------------------------------------------------------
  # SINTER key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SINTER", [_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store) do
      sets = Enum.map(keys, fn key -> get_members_set(key, store) end)

      result =
        case sets do
          [first | rest] -> Enum.reduce(rest, first, &MapSet.intersection(&2, &1))
          [] -> MapSet.new()
        end

      MapSet.to_list(result)
    end
  end

  def handle("SINTER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sinter' command"}
  end

  # ---------------------------------------------------------------------------
  # SUNION key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SUNION", [_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store) do
      sets = Enum.map(keys, fn key -> get_members_set(key, store) end)
      result = Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))
      MapSet.to_list(result)
    end
  end

  def handle("SUNION", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sunion' command"}
  end

  # ---------------------------------------------------------------------------
  # SDIFF key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SDIFF", [first_key | rest_keys], store) do
    with :ok <- check_all_types([first_key | rest_keys], store) do
      first_set = get_members_set(first_key, store)
      rest_sets = Enum.map(rest_keys, fn key -> get_members_set(key, store) end)
      result = Enum.reduce(rest_sets, first_set, &MapSet.difference(&2, &1))
      MapSet.to_list(result)
    end
  end

  def handle("SDIFF", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sdiff' command"}
  end

  # ---------------------------------------------------------------------------
  # SSCAN key cursor [MATCH pattern] [COUNT count]
  # ---------------------------------------------------------------------------

  def handle("SSCAN", [key, cursor_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store),
         {:ok, cursor} <- parse_cursor(cursor_str),
         {:ok, match_pattern, count} <- parse_sscan_opts(opts) do
      prefix = CompoundKey.set_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      members = Enum.map(pairs, fn {member, _} -> member end)

      filtered =
        case match_pattern do
          nil ->
            members

          pattern ->
            Enum.filter(members, fn m -> Ferricstore.GlobMatcher.match?(m, pattern) end)
        end

      {next_cursor, batch} = paginate(filtered, cursor, count)
      [next_cursor, batch]
    end
  end

  def handle("SSCAN", [_key], _store) do
    {:error, "ERR wrong number of arguments for 'sscan' command"}
  end

  def handle("SSCAN", [], _store) do
    {:error, "ERR wrong number of arguments for 'sscan' command"}
  end

  # ---------------------------------------------------------------------------
  # SRANDMEMBER key [count]
  # ---------------------------------------------------------------------------

  def handle("SRANDMEMBER", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      members = get_members_list(key, store)

      case members do
        [] -> nil
        _ -> Enum.random(members)
      end
    end
  end

  def handle("SRANDMEMBER", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      case Integer.parse(count_str) do
        {count, ""} ->
          members = get_members_list(key, store)
          select_random_members(members, count)

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("SRANDMEMBER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'srandmember' command"}
  end

  # ---------------------------------------------------------------------------
  # SPOP key [count]
  # ---------------------------------------------------------------------------

  def handle("SPOP", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      members = get_members_list(key, store)

      case members do
        [] ->
          nil

        _ ->
          member = Enum.random(members)
          compound_key = CompoundKey.set_member(key, member)
          Ops.compound_delete(store, key, compound_key)
          maybe_cleanup_empty_set(key, 1, store)
          member
      end
    end
  end

  def handle("SPOP", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          members = get_members_list(key, store)
          selected = Enum.take_random(members, count)

          removed =
            Enum.reduce(selected, 0, fn member, acc ->
              compound_key = CompoundKey.set_member(key, member)
              Ops.compound_delete(store, key, compound_key)
              acc + 1
            end)

          if removed > 0 do
            maybe_cleanup_empty_set(key, removed, store)
          end

          selected

        {_count, ""} ->
          {:error, "ERR value is not an integer or out of range"}

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("SPOP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'spop' command"}
  end

  # ---------------------------------------------------------------------------
  # SMOVE source destination member
  # ---------------------------------------------------------------------------

  def handle("SMOVE", [source, destination, member], store) do
    CrossShardOp.execute(
      [{source, :read_write}, {destination, :write}],
      fn unified_store ->
        do_smove(source, destination, member, unified_store)
      end,
      intent: %{command: :smove, keys: %{source: source, dest: destination}, value_hashes: %{}},
      store: store
    )
  end

  def handle("SMOVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'smove' command"}
  end

  # ---------------------------------------------------------------------------
  # SDIFFSTORE destination key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SDIFFSTORE", [destination | [_ | _] = keys], store) do
    keys_with_roles =
      [{destination, :write}] ++ Enum.map(keys, fn k -> {k, :read} end)

    CrossShardOp.execute(
      keys_with_roles,
      fn unified_store ->
        with :ok <- check_all_types(keys, unified_store) do
          first_set = get_members_set(hd(keys), unified_store)
          rest_sets = Enum.map(tl(keys), fn key -> get_members_set(key, unified_store) end)
          result = Enum.reduce(rest_sets, first_set, &MapSet.difference(&2, &1))

          store_set_at(destination, result, unified_store)
        end
      end,
      intent: %{
        command: :sdiffstore,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      store: store
    )
  end

  def handle("SDIFFSTORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sdiffstore' command"}
  end

  # ---------------------------------------------------------------------------
  # SINTERSTORE destination key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SINTERSTORE", [destination | [_ | _] = keys], store) do
    keys_with_roles =
      [{destination, :write}] ++ Enum.map(keys, fn k -> {k, :read} end)

    CrossShardOp.execute(
      keys_with_roles,
      fn unified_store ->
        with :ok <- check_all_types(keys, unified_store) do
          sets = Enum.map(keys, fn key -> get_members_set(key, unified_store) end)

          result =
            case sets do
              [first | rest] -> Enum.reduce(rest, first, &MapSet.intersection(&2, &1))
              [] -> MapSet.new()
            end

          store_set_at(destination, result, unified_store)
        end
      end,
      intent: %{
        command: :sinterstore,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      store: store
    )
  end

  def handle("SINTERSTORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sinterstore' command"}
  end

  # ---------------------------------------------------------------------------
  # SUNIONSTORE destination key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SUNIONSTORE", [destination | [_ | _] = keys], store) do
    keys_with_roles =
      [{destination, :write}] ++ Enum.map(keys, fn k -> {k, :read} end)

    CrossShardOp.execute(
      keys_with_roles,
      fn unified_store ->
        with :ok <- check_all_types(keys, unified_store) do
          sets = Enum.map(keys, fn key -> get_members_set(key, unified_store) end)
          result = Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))

          store_set_at(destination, result, unified_store)
        end
      end,
      intent: %{
        command: :sunionstore,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      store: store
    )
  end

  def handle("SUNIONSTORE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sunionstore' command"}
  end

  # ---------------------------------------------------------------------------
  # SINTERCARD numkeys key [key ...] [LIMIT limit]
  # ---------------------------------------------------------------------------

  def handle("SINTERCARD", [numkeys_str | rest], store) when rest != [] do
    with {:ok, numkeys} <- parse_numkeys(numkeys_str),
         {:ok, keys, limit} <- parse_sintercard_args(rest, numkeys) do
      with :ok <- check_all_types(keys, store) do
        sets = Enum.map(keys, fn key -> get_members_set(key, store) end)

        intersection =
          case sets do
            [first | others] -> Enum.reduce(others, first, &MapSet.intersection(&2, &1))
            [] -> MapSet.new()
          end

        count = MapSet.size(intersection)

        if limit > 0 and count > limit do
          limit
        else
          count
        end
      end
    end
  end

  def handle("SINTERCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sintercard' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Core SMOVE logic, extracted for use inside CrossShardOp.execute.
  defp do_smove(source, destination, member, store) do
    with :ok <- TypeRegistry.check_type(source, :set, store),
         :ok <- TypeRegistry.check_type(destination, :set, store) do
      compound_key = CompoundKey.set_member(source, member)

      if Ops.compound_get(store, source, compound_key) == nil do
        0
      else
        # Remove from source
        Ops.compound_delete(store, source, compound_key)
        maybe_cleanup_empty_set(source, 1, store)

        # Add to destination (check_or_set for destination since it may not exist)
        TypeRegistry.check_or_set(destination, :set, store)
        dst_key = CompoundKey.set_member(destination, member)
        Ops.compound_put(store, destination, dst_key, @presence_marker, 0)
        1
      end
    end
  end

  # Clears any existing set at `destination`, writes `members` as a new set,
  # and returns the member count.
  defp store_set_at(destination, members, store) do
    # STORE commands replace the destination regardless of its previous type.
    Ops.delete(store, destination)

    # Clear existing destination data structure metadata and members.
    prefix = CompoundKey.set_prefix(destination)
    Ops.compound_delete_prefix(store, destination, prefix)
    TypeRegistry.delete_type(destination, store)

    members_list = MapSet.to_list(members)

    if members_list != [] do
      with :ok <- TypeRegistry.check_or_set(destination, :set, store) do
        Enum.each(members_list, fn member ->
          compound_key = CompoundKey.set_member(destination, member)
          Ops.compound_put(store, destination, compound_key, @presence_marker, 0)
        end)
      end
    end

    length(members_list)
  end

  defp parse_numkeys(str) do
    case Integer.parse(str) do
      {n, ""} when n > 0 -> {:ok, n}
      _ -> {:error, "ERR numkeys can't be non-positive value"}
    end
  end

  defp parse_sintercard_args(args, numkeys) do
    {key_args, tail} = Enum.split(args, numkeys)

    if length(key_args) < numkeys do
      {:error, "ERR Number of keys can't be greater than number of args"}
    else
      case tail do
        [] ->
          {:ok, key_args, 0}

        [opt, limit_str] when is_binary(opt) ->
          if String.upcase(opt) == "LIMIT" do
            case Integer.parse(limit_str) do
              {limit, ""} when limit >= 0 -> {:ok, key_args, limit}
              _ -> {:error, "ERR value is not an integer or out of range"}
            end
          else
            {:error, "ERR syntax error"}
          end

        _ ->
          {:error, "ERR syntax error"}
      end
    end
  end

  defp get_members_set(key, store) do
    prefix = CompoundKey.set_prefix(key)
    pairs = Ops.compound_scan(store, key, prefix)
    MapSet.new(pairs, fn {member, _} -> member end)
  end

  defp get_members_list(key, store) do
    prefix = CompoundKey.set_prefix(key)
    pairs = Ops.compound_scan(store, key, prefix)
    Enum.map(pairs, fn {member, _} -> member end)
  end

  defp check_all_types(keys, store) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case TypeRegistry.check_type(key, :set, store) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp maybe_cleanup_empty_set(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_set(key, _removed, store) do
    prefix = CompoundKey.set_prefix(key)

    if Ops.compound_count(store, key, prefix) == 0 do
      TypeRegistry.delete_type(key, store)
    end
  end

  defp select_random_members(members, count) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        Enum.take_random(members, count)

      count < 0 ->
        abs_count = abs(count)

        if members == [] do
          []
        else
          # Convert to tuple for O(1) random access instead of O(n) Enum.random on list
          tuple = List.to_tuple(members)
          size = tuple_size(tuple)
          for _ <- 1..abs_count, do: elem(tuple, :rand.uniform(size) - 1)
        end
    end
  end

  # ---------------------------------------------------------------------------
  # SSCAN helpers
  # ---------------------------------------------------------------------------

  defp parse_cursor(cursor_str) do
    case Integer.parse(cursor_str) do
      {cursor, ""} when cursor >= 0 -> {:ok, cursor}
      _ -> {:error, "ERR invalid cursor"}
    end
  end

  defp parse_sscan_opts(opts), do: do_parse_sscan_opts(opts, nil, 10)

  defp do_parse_sscan_opts([], match, count), do: {:ok, match, count}

  defp do_parse_sscan_opts([opt, value | rest], match, count) do
    case String.upcase(opt) do
      "MATCH" ->
        do_parse_sscan_opts(rest, value, count)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> do_parse_sscan_opts(rest, match, n)
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_parse_sscan_opts([_ | _], _match, _count) do
    {:error, "ERR syntax error"}
  end

  defp paginate(items, cursor, count) do
    rest = Enum.drop(items, cursor)

    case rest do
      [] ->
        {"0", []}

      _ ->
        {batch, remainder} = Enum.split(rest, count)

        case remainder do
          [] -> {"0", batch}
          _ -> {Integer.to_string(cursor + length(batch)), batch}
        end
    end
  end
end
