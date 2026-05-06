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
    sadd_members(key, members, store)
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

      removed = length(removed_keys)

      case Ops.compound_batch_delete(store, key, removed_keys) do
        :ok ->
          with :ok <- maybe_cleanup_empty_set(key, removed, store) do
            removed
          end

        {:error, _} = err ->
          err
      end
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
      keys
      |> sinter_set(store)
      |> MapSet.to_list()
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
        {0, ""} ->
          []

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

          case Ops.compound_batch_delete(store, key, [compound_key]) do
            :ok ->
              with :ok <- maybe_cleanup_empty_set(key, 1, store) do
                member
              end

            {:error, _} = err ->
              err
          end
      end
    end
  end

  def handle("SPOP", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      case Integer.parse(count_str) do
        {0, ""} ->
          []

        {count, ""} when count >= 0 ->
          members = get_members_list(key, store)
          selected = Enum.take_random(members, count)
          compound_keys = Enum.map(selected, &CompoundKey.set_member(key, &1))
          removed = length(compound_keys)

          case Ops.compound_batch_delete(store, key, compound_keys) do
            :ok ->
              with :ok <- maybe_cleanup_empty_set(key, removed, store) do
                selected
              end

            {:error, _} = err ->
              err
          end

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
        sinter_count(keys, limit, store)
      end
    end
  end

  def handle("SINTERCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sintercard' command"}
  end

  @doc false
  def handle_ast(ast, store)

  def handle_ast({:sadd, args}, store), do: sadd_args(args, store)
  def handle_ast({:srem, args}, store), do: srem_args(args, store)
  def handle_ast({:smismember, args}, store), do: smismember_args(args, store)
  def handle_ast({:sinter, args}, store), do: sinter_keys(args, store)
  def handle_ast({:sunion, args}, store), do: sunion_keys(args, store)
  def handle_ast({:sdiff, args}, store), do: sdiff_keys(args, store)
  def handle_ast({:sdiffstore, args}, store), do: sdiffstore_args(args, store)
  def handle_ast({:sinterstore, args}, store), do: sinterstore_args(args, store)
  def handle_ast({:sunionstore, args}, store), do: sunionstore_args(args, store)

  def handle_ast({:smembers, key}, store), do: smembers_key(key, store)
  def handle_ast({:sismember, key, member}, store), do: sismember_member(key, member, store)
  def handle_ast({:scard, key}, store), do: scard_key(key, store)

  def handle_ast({:smove, source, destination, member}, store),
    do: smove_member(source, destination, member, store)

  def handle_ast({:srandmember, key}, store), do: srandmember_one(key, store)
  def handle_ast({:srandmember, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:srandmember, key, count}, store) when is_integer(count) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      if count == 0 do
        []
      else
        key
        |> get_members_list(store)
        |> select_random_members(count)
      end
    end
  end

  def handle_ast({:spop, key}, store), do: spop_one(key, store)
  def handle_ast({:spop, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:spop, key, count}, store) when is_integer(count) and count >= 0,
    do: spop_count(key, count, store)

  def handle_ast({:sscan, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:sscan, key, cursor, opts}, store) when is_integer(cursor) and cursor >= 0,
    do: sscan_typed(key, cursor, opts, store)

  def handle_ast({:sintercard, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:sintercard, keys, limit}, store)
      when is_list(keys) and is_integer(limit) and limit >= 0,
      do: sintercard_typed(keys, limit, store)

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported set command AST"}

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp sadd_args([key | members], store) when members != [] do
    sadd_members(key, members, store)
  end

  defp sadd_args(_args, _store), do: {:error, "ERR wrong number of arguments for 'sadd' command"}

  defp sadd_members(key, members, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :set, store) do
      compound_keys =
        members
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.set_member(key, &1))

      new_keys =
        store
        |> Ops.compound_batch_get(key, compound_keys)
        |> Enum.zip(compound_keys)
        |> Enum.flat_map(fn
          {nil, compound_key} -> [compound_key]
          {_value, _compound_key} -> []
        end)

      put_new_members(store, key, new_keys, type_status)
    end
  end

  defp put_new_members(store, key, new_keys, type_status) do
    entries = Enum.map(new_keys, &{&1, @presence_marker, 0})

    case Ops.compound_batch_put(store, key, entries) do
      :ok -> length(new_keys)
      {:error, _} = err -> rollback_new_set_type_marker(key, store, type_status, err)
    end
  end

  defp rollback_new_set_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.delete_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:set_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_set_type_marker(_key, _store, :ok, write_error), do: write_error

  defp srem_args([key | members], store) when members != [] do
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

      removed = length(removed_keys)

      case Ops.compound_batch_delete(store, key, removed_keys) do
        :ok ->
          with :ok <- maybe_cleanup_empty_set(key, removed, store) do
            removed
          end

        {:error, _} = err ->
          err
      end
    end
  end

  defp srem_args(_args, _store), do: {:error, "ERR wrong number of arguments for 'srem' command"}

  defp smembers_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      key
      |> get_members_list(store)
    end
  end

  defp sismember_member(key, member, store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      compound_key = CompoundKey.set_member(key, member)
      if Ops.compound_get(store, key, compound_key) != nil, do: 1, else: 0
    end
  end

  defp smismember_args([key | members], store) when members != [] do
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

  defp smismember_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'smismember' command"}

  defp scard_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      prefix = CompoundKey.set_prefix(key)
      Ops.compound_count(store, key, prefix)
    end
  end

  defp sinter_keys([_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store) do
      keys
      |> sinter_set(store)
      |> MapSet.to_list()
    end
  end

  defp sinter_keys(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sinter' command"}

  defp sunion_keys([_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store) do
      sets = Enum.map(keys, fn key -> get_members_set(key, store) end)
      result = Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))
      MapSet.to_list(result)
    end
  end

  defp sunion_keys(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sunion' command"}

  defp sdiff_keys([first_key | rest_keys], store) do
    with :ok <- check_all_types([first_key | rest_keys], store) do
      first_set = get_members_set(first_key, store)
      rest_sets = Enum.map(rest_keys, fn key -> get_members_set(key, store) end)
      result = Enum.reduce(rest_sets, first_set, &MapSet.difference(&2, &1))
      MapSet.to_list(result)
    end
  end

  defp sdiff_keys(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sdiff' command"}

  defp smove_member(source, destination, member, store) do
    CrossShardOp.execute(
      [{source, :read_write}, {destination, :write}],
      fn unified_store ->
        do_smove(source, destination, member, unified_store)
      end,
      intent: %{command: :smove, keys: %{source: source, dest: destination}, value_hashes: %{}},
      store: store
    )
  end

  defp sdiffstore_args([destination | [_ | _] = keys], store) do
    store_result_at(destination, keys, :sdiffstore, store, fn source_keys, unified_store ->
      first_set = get_members_set(hd(source_keys), unified_store)
      rest_sets = Enum.map(tl(source_keys), fn key -> get_members_set(key, unified_store) end)
      Enum.reduce(rest_sets, first_set, &MapSet.difference(&2, &1))
    end)
  end

  defp sdiffstore_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sdiffstore' command"}

  defp sinterstore_args([destination | [_ | _] = keys], store) do
    store_result_at(destination, keys, :sinterstore, store, fn source_keys, unified_store ->
      sinter_set(source_keys, unified_store)
    end)
  end

  defp sinterstore_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sinterstore' command"}

  defp sunionstore_args([destination | [_ | _] = keys], store) do
    store_result_at(destination, keys, :sunionstore, store, fn source_keys, unified_store ->
      sets = Enum.map(source_keys, fn key -> get_members_set(key, unified_store) end)
      Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))
    end)
  end

  defp sunionstore_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sunionstore' command"}

  defp store_result_at(destination, keys, command, store, result_fun) do
    keys_with_roles =
      [{destination, :write}] ++ Enum.map(keys, fn key -> {key, :read} end)

    CrossShardOp.execute(
      keys_with_roles,
      fn unified_store ->
        with :ok <- check_all_types(keys, unified_store) do
          destination
          |> store_set_at(result_fun.(keys, unified_store), unified_store)
        end
      end,
      intent: %{
        command: command,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      store: store
    )
  end

  defp srandmember_one(key, store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      case get_members_list(key, store) do
        [] -> nil
        members -> Enum.random(members)
      end
    end
  end

  defp spop_one(key, store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      case get_members_list(key, store) do
        [] ->
          nil

        members ->
          member = Enum.random(members)
          compound_key = CompoundKey.set_member(key, member)

          case Ops.compound_batch_delete(store, key, [compound_key]) do
            :ok ->
              with :ok <- maybe_cleanup_empty_set(key, 1, store) do
                member
              end

            {:error, _} = err ->
              err
          end
      end
    end
  end

  defp spop_count(key, count, store) do
    with :ok <- TypeRegistry.check_type(key, :set, store) do
      if count == 0 do
        []
      else
        members = get_members_list(key, store)
        selected = Enum.take_random(members, count)
        compound_keys = Enum.map(selected, &CompoundKey.set_member(key, &1))
        removed = length(compound_keys)

        case Ops.compound_batch_delete(store, key, compound_keys) do
          :ok ->
            with :ok <- maybe_cleanup_empty_set(key, removed, store) do
              selected
            end

          {:error, _} = err ->
            err
        end
      end
    end
  end

  defp sscan_typed(key, cursor, opts, store) do
    with :ok <- TypeRegistry.check_type(key, :set, store),
         {:ok, match_pattern, count} <- typed_scan_opts(opts) do
      prefix = CompoundKey.set_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      members = Enum.map(pairs, fn {member, _} -> member end)

      filtered =
        case match_pattern do
          nil ->
            members

          pattern ->
            Enum.filter(members, fn member -> Ferricstore.GlobMatcher.match?(member, pattern) end)
        end

      {next_cursor, batch} = paginate(filtered, cursor, count)
      [next_cursor, batch]
    end
  end

  defp sintercard_typed(keys, limit, store) do
    with :ok <- check_all_types(keys, store) do
      sinter_count(keys, limit, store)
    end
  end

  # Core SMOVE logic, extracted for use inside CrossShardOp.execute.
  defp do_smove(source, destination, member, store) do
    with :ok <- TypeRegistry.check_type(source, :set, store),
         :ok <- TypeRegistry.check_type(destination, :set, store) do
      compound_key = CompoundKey.set_member(source, member)

      cond do
        Ops.compound_get(store, source, compound_key) == nil ->
          0

        source == destination ->
          1

        true ->
          dst_key = CompoundKey.set_member(destination, member)
          destination_had_member? = Ops.compound_get(store, destination, dst_key) != nil

          case maybe_put_smove_destination(destination_had_member?, destination, dst_key, store) do
            :ok ->
              case Ops.compound_batch_delete(store, source, [compound_key]) do
                :ok ->
                  with :ok <- maybe_cleanup_empty_set(source, 1, store) do
                    1
                  end

                {:error, _} = err ->
                  case maybe_rollback_smove_destination(
                         destination_had_member?,
                         destination,
                         dst_key,
                         store
                       ) do
                    :ok ->
                      err

                    {:error, _} = rollback_err ->
                      {:error, {:smove_rollback_failed, err, rollback_err}}
                  end
              end

            {:error, _} = err ->
              err
          end
      end
    end
  end

  defp maybe_put_smove_destination(true, _destination, _dst_key, _store), do: :ok

  defp maybe_put_smove_destination(false, destination, dst_key, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(destination, :set, store) do
      case Ops.compound_put(store, destination, dst_key, @presence_marker, 0) do
        :ok -> :ok
        true -> :ok
        {:error, _} = err -> rollback_new_set_type_marker(destination, store, type_status, err)
        other -> rollback_new_set_type_marker(destination, store, type_status, {:error, other})
      end
    end
  end

  defp maybe_rollback_smove_destination(true, _destination, _dst_key, _store), do: :ok

  defp maybe_rollback_smove_destination(false, destination, dst_key, store) do
    Ops.compound_batch_delete(store, destination, [dst_key])
  end

  # Clears any existing set at `destination`, writes `members` as a new set,
  # and returns the member count.
  defp store_set_at(destination, members, store) do
    with :ok <- clear_set_store_destination(destination, store) do
      members_list = MapSet.to_list(members)

      if members_list == [] do
        0
      else
        with type_status when type_status in [:ok, {:ok, :created}] <-
               TypeRegistry.check_or_set_status(destination, :set, store) do
          case put_set_members(store, destination, members_list) do
            :ok ->
              length(members_list)

            {:error, _} = err ->
              rollback_new_set_type_marker(destination, store, type_status, err)
          end
        end
      end
    end
  end

  defp clear_set_store_destination(destination, store) do
    # STORE commands replace the destination regardless of its previous type.
    prefix = CompoundKey.set_prefix(destination)

    with :ok <- Ops.delete(store, destination),
         :ok <- Ops.compound_delete_prefix(store, destination, prefix),
         :ok <- TypeRegistry.delete_type(destination, store) do
      :ok
    end
  end

  defp put_set_members(store, key, members) do
    entries =
      Enum.map(members, fn member ->
        {CompoundKey.set_member(key, member), @presence_marker, 0}
      end)

    Ops.compound_batch_put(store, key, entries)
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

  defp sinter_set(keys, store) do
    keys
    |> count_sets(store)
    |> intersection_from_counted_keys(store)
  end

  defp sinter_count(keys, limit, store) do
    counted = count_sets(keys, store)

    if Enum.any?(counted, fn {_key, count} -> count == 0 end) do
      0
    else
      counted
      |> pop_smallest_set()
      |> count_intersection_candidates(limit, store)
    end
  end

  defp count_sets(keys, store) do
    Enum.map(keys, fn key ->
      {key, Ops.compound_count(store, key, CompoundKey.set_prefix(key))}
    end)
  end

  defp intersection_from_counted_keys([], _store), do: MapSet.new()

  defp intersection_from_counted_keys(counted, store) do
    if Enum.any?(counted, fn {_key, count} -> count == 0 end) do
      MapSet.new()
    else
      {{base_key, _count}, rest} = pop_smallest_set(counted)

      base_key
      |> get_members_list(store)
      |> Enum.reduce(MapSet.new(), fn member, acc ->
        if member_in_all_sets?(member, rest, store) do
          MapSet.put(acc, member)
        else
          acc
        end
      end)
    end
  end

  defp pop_smallest_set([{_key, _count} | _] = counted) do
    smallest_index =
      counted
      |> Enum.with_index()
      |> Enum.min_by(fn {{_key, count}, _index} -> count end)
      |> elem(1)

    List.pop_at(counted, smallest_index)
  end

  defp count_intersection_candidates({{base_key, _count}, rest}, limit, store) do
    base_key
    |> get_members_list(store)
    |> Enum.reduce_while(0, fn member, count ->
      if member_in_all_sets?(member, rest, store) do
        next_count = count + 1

        if limit > 0 and next_count >= limit do
          {:halt, limit}
        else
          {:cont, next_count}
        end
      else
        {:cont, count}
      end
    end)
  end

  defp member_in_all_sets?(member, counted_keys, store) do
    Enum.all?(counted_keys, fn {key, _count} ->
      Ops.compound_get(store, key, CompoundKey.set_member(key, member)) != nil
    end)
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
    else
      :ok
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
