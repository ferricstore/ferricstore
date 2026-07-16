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

  alias Ferricstore.Commands.CollectionScan
  alias Ferricstore.Commands.Set.{Destination, Intersection, Random, Scan}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
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

  def handle("SREM", [key | members], store) when members != [],
    do: srem_args([key | members], store)

  def handle("SREM", _args, _store) do
    {:error, "ERR wrong number of arguments for 'srem' command"}
  end

  # ---------------------------------------------------------------------------
  # SMEMBERS key
  # ---------------------------------------------------------------------------

  def handle("SMEMBERS", [key], store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, members} <- get_members_list(key, store) do
      members
    end
  end

  def handle("SMEMBERS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'smembers' command"}
  end

  # ---------------------------------------------------------------------------
  # SISMEMBER key member
  # ---------------------------------------------------------------------------

  def handle("SISMEMBER", [key, member], store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, present?} <- member_present?(key, member, store) do
      if present?, do: 1, else: 0
    end
  end

  def handle("SISMEMBER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'sismember' command"}
  end

  # ---------------------------------------------------------------------------
  # SMISMEMBER key member [member ...]
  # ---------------------------------------------------------------------------

  def handle("SMISMEMBER", [key | members], store) when members != [] do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, values} <- member_values(key, members, store) do
      Enum.map(values, fn
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
    with :ok <- TypeRegistry.command_check_type(key, :set, store) do
      prefix = CompoundKey.set_prefix(key)
      store |> Ops.compound_count(key, prefix) |> ReadResult.command_result()
    end
  end

  def handle("SCARD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'scard' command"}
  end

  # ---------------------------------------------------------------------------
  # SINTER key [key ...]
  # ---------------------------------------------------------------------------

  def handle("SINTER", [_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store),
         {:ok, result} <- Intersection.sinter_set(keys, store) do
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
    with :ok <- check_all_types(keys, store),
         {:ok, sets} <- get_member_sets(keys, store) do
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
    keys = [first_key | rest_keys]

    with :ok <- check_all_types(keys, store),
         {:ok, [first_set | rest_sets]} <- get_member_sets(keys, store) do
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
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, cursor} <- Scan.parse_cursor(cursor_str),
         {:ok, match_pattern, count} <- Scan.parse_sscan_opts(opts),
         {:ok, {next_cursor, pairs}} <-
           CollectionScan.page(
             store,
             key,
             CompoundKey.set_prefix(key),
             cursor,
             count,
             match_pattern,
             true
           ) do
      [next_cursor, Enum.map(pairs, &elem(&1, 0))]
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
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, members} <- get_members_list(key, store) do
      case members do
        [] -> nil
        _ -> Enum.random(members)
      end
    end
  end

  def handle("SRANDMEMBER", [key, count_str], store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store) do
      case Integer.parse(count_str) do
        {0, ""} ->
          []

        {count, ""} ->
          with {:ok, members} <- get_members_list(key, store) do
            Random.select_random_members(members, count)
          end

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

  def handle("SPOP", [key], store), do: spop_one(key, store)

  def handle("SPOP", [key, count_str], store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store) do
      case Integer.parse(count_str) do
        {0, ""} ->
          []

        {count, ""} when count >= 0 ->
          spop_count(key, count, store)

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
      tx_entry: {"SMOVE", [source, destination, member], {:smove, source, destination, member}},
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
        with :ok <- check_all_types(keys, unified_store),
             {:ok, [first_set | rest_sets]} <- get_member_sets(keys, unified_store) do
          result = Enum.reduce(rest_sets, first_set, &MapSet.difference(&2, &1))

          Destination.store_set_at(destination, result, unified_store)
        end
      end,
      intent: %{
        command: :sdiffstore,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      tx_entry: {"SDIFFSTORE", [destination | keys], {:sdiffstore, [destination | keys]}},
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
        with :ok <- check_all_types(keys, unified_store),
             {:ok, result} <- Intersection.sinter_set(keys, unified_store) do
          Destination.store_set_at(destination, result, unified_store)
        end
      end,
      intent: %{
        command: :sinterstore,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      tx_entry: {"SINTERSTORE", [destination | keys], {:sinterstore, [destination | keys]}},
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
        with :ok <- check_all_types(keys, unified_store),
             {:ok, sets} <- get_member_sets(keys, unified_store) do
          result = Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))

          Destination.store_set_at(destination, result, unified_store)
        end
      end,
      intent: %{
        command: :sunionstore,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      tx_entry: {"SUNIONSTORE", [destination | keys], {:sunionstore, [destination | keys]}},
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
      with :ok <- check_all_types(keys, store),
           {:ok, count} <- Intersection.sinter_count(keys, limit, store) do
        count
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
    with :ok <- TypeRegistry.command_check_type(key, :set, store) do
      if count == 0 do
        []
      else
        with {:ok, members} <- get_members_list(key, store) do
          Random.select_random_members(members, count)
        end
      end
    end
  end

  def handle_ast({:spop, key}, store), do: spop_one(key, store)
  def handle_ast({:spop, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:spop, key, count}, store) when is_integer(count) and count >= 0,
    do: spop_count(key, count, store)

  def handle_ast({:sscan, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:sscan, key, cursor, opts}, store) do
    if CollectionScan.valid_cursor?(cursor),
      do: sscan_typed(key, cursor, opts, store),
      else: {:error, "ERR invalid cursor"}
  end

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
           TypeRegistry.command_check_or_set_status(key, :set, store) do
      compound_keys =
        members
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.set_member(key, &1))

      values = Ops.compound_batch_get(store, key, compound_keys)

      case ReadResult.first_failure(values) do
        nil ->
          new_entries = set_member_entries_for_missing(compound_keys, values, [])
          put_new_member_entries(store, key, new_entries, type_status)

        failure ->
          rollback_new_set_type_marker(
            key,
            store,
            type_status,
            ReadResult.command_error(failure)
          )
      end
    end
  end

  defp put_new_member_entries(store, key, entries, type_status) do
    case Ops.compound_batch_put(store, key, entries) do
      :ok -> length(entries)
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
    with :ok <- TypeRegistry.command_check_type(key, :set, store) do
      compound_keys =
        members
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.set_member(key, &1))

      values = Ops.compound_batch_get(store, key, compound_keys)

      case ReadResult.first_failure(values) do
        nil ->
          removed_entries = set_member_entries_for_present(compound_keys, values, [])
          removed = length(removed_entries)

          with :ok <- delete_set_members_and_cleanup(key, removed_entries, removed, store) do
            removed
          end

        failure ->
          ReadResult.command_error(failure)
      end
    end
  end

  defp srem_args(_args, _store), do: {:error, "ERR wrong number of arguments for 'srem' command"}

  defp set_member_entries_for_missing([compound_key | compound_keys], [nil | values], acc) do
    set_member_entries_for_missing(compound_keys, values, [
      {compound_key, @presence_marker, 0} | acc
    ])
  end

  defp set_member_entries_for_missing([_compound_key | compound_keys], [_value | values], acc) do
    set_member_entries_for_missing(compound_keys, values, acc)
  end

  defp set_member_entries_for_missing(_compound_keys, _values, acc), do: Enum.reverse(acc)

  defp set_member_entries_for_present([_compound_key | compound_keys], [nil | values], acc) do
    set_member_entries_for_present(compound_keys, values, acc)
  end

  defp set_member_entries_for_present([compound_key | compound_keys], [_value | values], acc) do
    set_member_entries_for_present(compound_keys, values, [
      {compound_key, @presence_marker, 0} | acc
    ])
  end

  defp set_member_entries_for_present(_compound_keys, _values, acc), do: Enum.reverse(acc)

  defp smembers_key(key, store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, members} <- get_members_list(key, store) do
      members
    end
  end

  defp sismember_member(key, member, store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, present?} <- member_present?(key, member, store) do
      if present?, do: 1, else: 0
    end
  end

  defp smismember_args([key | members], store) when members != [] do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, values} <- member_values(key, members, store) do
      Enum.map(values, fn
        nil -> 0
        _value -> 1
      end)
    end
  end

  defp smismember_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'smismember' command"}

  defp scard_key(key, store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store) do
      prefix = CompoundKey.set_prefix(key)
      store |> Ops.compound_count(key, prefix) |> ReadResult.command_result()
    end
  end

  defp sinter_keys([_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store),
         {:ok, result} <- Intersection.sinter_set(keys, store) do
      MapSet.to_list(result)
    end
  end

  defp sinter_keys(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sinter' command"}

  defp sunion_keys([_ | _] = keys, store) do
    with :ok <- check_all_types(keys, store),
         {:ok, sets} <- get_member_sets(keys, store) do
      result = Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))
      MapSet.to_list(result)
    end
  end

  defp sunion_keys(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sunion' command"}

  defp sdiff_keys([first_key | rest_keys], store) do
    keys = [first_key | rest_keys]

    with :ok <- check_all_types(keys, store),
         {:ok, [first_set | rest_sets]} <- get_member_sets(keys, store) do
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
      tx_entry: {"SMOVE", [source, destination, member], {:smove, source, destination, member}},
      store: store
    )
  end

  defp sdiffstore_args([destination | [_ | _] = keys], store) do
    store_result_at(destination, keys, :sdiffstore, store, fn source_keys, unified_store ->
      with {:ok, [first_set | rest_sets]} <- get_member_sets(source_keys, unified_store) do
        {:ok, Enum.reduce(rest_sets, first_set, &MapSet.difference(&2, &1))}
      end
    end)
  end

  defp sdiffstore_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sdiffstore' command"}

  defp sinterstore_args([destination | [_ | _] = keys], store) do
    store_result_at(destination, keys, :sinterstore, store, fn source_keys, unified_store ->
      Intersection.sinter_set(source_keys, unified_store)
    end)
  end

  defp sinterstore_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'sinterstore' command"}

  defp sunionstore_args([destination | [_ | _] = keys], store) do
    store_result_at(destination, keys, :sunionstore, store, fn source_keys, unified_store ->
      with {:ok, sets} <- get_member_sets(source_keys, unified_store) do
        {:ok, Enum.reduce(sets, MapSet.new(), &MapSet.union(&2, &1))}
      end
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
        with :ok <- check_all_types(keys, unified_store),
             {:ok, result} <- result_fun.(keys, unified_store) do
          Destination.store_set_at(destination, result, unified_store)
        end
      end,
      intent: %{
        command: command,
        keys: %{dest: destination, sources: keys},
        value_hashes: %{}
      },
      tx_entry: store_result_tx_entry(command, destination, keys),
      store: store
    )
  end

  defp store_result_tx_entry(command, destination, keys) do
    name = command |> Atom.to_string() |> String.upcase()
    {name, [destination | keys], {command, [destination | keys]}}
  end

  defp srandmember_one(key, store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, members} <- get_members_list(key, store) do
      case members do
        [] -> nil
        members -> Enum.random(members)
      end
    end
  end

  defp spop_one(key, store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, members} <- get_members_list(key, store) do
      case members do
        [] ->
          nil

        members ->
          member = Enum.random(members)
          compound_key = CompoundKey.set_member(key, member)

          with :ok <- delete_set_members_and_cleanup(key, [{compound_key, "1", 0}], 1, store) do
            member
          end
      end
    end
  end

  defp spop_count(key, count, store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store) do
      if count == 0 do
        []
      else
        with {:ok, members} <- get_members_list(key, store) do
          selected = Enum.take_random(members, count)
          compound_keys = Enum.map(selected, &CompoundKey.set_member(key, &1))
          removed = length(compound_keys)
          removed_entries = Enum.map(compound_keys, &{&1, "1", 0})

          with :ok <- delete_set_members_and_cleanup(key, removed_entries, removed, store) do
            selected
          end
        end
      end
    end
  end

  defp sscan_typed(key, cursor, opts, store) do
    with :ok <- TypeRegistry.command_check_type(key, :set, store),
         {:ok, match_pattern, count} <- Scan.typed_scan_opts(opts),
         {:ok, {next_cursor, pairs}} <-
           CollectionScan.page(
             store,
             key,
             CompoundKey.set_prefix(key),
             cursor,
             count,
             match_pattern,
             true
           ) do
      [next_cursor, Enum.map(pairs, &elem(&1, 0))]
    end
  end

  defp sintercard_typed(keys, limit, store) do
    with :ok <- check_all_types(keys, store),
         {:ok, count} <- Intersection.sinter_count(keys, limit, store) do
      count
    end
  end

  # Core SMOVE logic, extracted for use inside CrossShardOp.execute.
  defp do_smove(source, destination, member, store) do
    with :ok <- TypeRegistry.command_check_type(source, :set, store),
         {:ok, source_has_member?} <- member_present?(source, member, store) do
      cond do
        not source_has_member? ->
          0

        source == destination ->
          1

        true ->
          with :ok <- TypeRegistry.command_check_type(destination, :set, store),
               {:ok, destination_had_member?} <- member_present?(destination, member, store) do
            compound_key = CompoundKey.set_member(source, member)
            dst_key = CompoundKey.set_member(destination, member)

            case maybe_put_smove_destination(destination_had_member?, destination, dst_key, store) do
              :ok ->
                case Ops.compound_batch_delete(store, source, [compound_key]) do
                  :ok ->
                    finish_smove_source_delete(
                      source,
                      destination,
                      compound_key,
                      dst_key,
                      destination_had_member?,
                      store
                    )

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
  end

  defp maybe_put_smove_destination(true, _destination, _dst_key, _store), do: :ok

  defp maybe_put_smove_destination(false, destination, dst_key, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.command_check_or_set_status(destination, :set, store) do
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

  defp finish_smove_source_delete(
         source,
         destination,
         source_key,
         dst_key,
         destination_had_member?,
         store
       ) do
    case maybe_cleanup_empty_set(source, 1, store) do
      :ok ->
        1

      {:error, _} = cleanup_err ->
        rollback_smove_cleanup_failure(
          source,
          destination,
          source_key,
          dst_key,
          destination_had_member?,
          store,
          cleanup_err
        )
    end
  end

  defp rollback_smove_cleanup_failure(
         source,
         destination,
         source_key,
         dst_key,
         destination_had_member?,
         store,
         cleanup_err
       ) do
    source_result =
      Ops.compound_batch_put(store, source, [{source_key, @presence_marker, 0}])

    destination_result =
      maybe_rollback_smove_destination(destination_had_member?, destination, dst_key, store)

    case {source_result, destination_result} do
      {:ok, :ok} ->
        cleanup_err

      {{:error, _} = source_err, :ok} ->
        {:error, {:smove_cleanup_rollback_failed, cleanup_err, source_err, :ok}}

      {:ok, {:error, _} = destination_err} ->
        {:error, {:smove_cleanup_rollback_failed, cleanup_err, :ok, destination_err}}

      {{:error, _} = source_err, {:error, _} = destination_err} ->
        {:error, {:smove_cleanup_rollback_failed, cleanup_err, source_err, destination_err}}
    end
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
    with {:ok, members} <- get_members_list(key, store) do
      {:ok, MapSet.new(members)}
    end
  end

  defp get_members_list(key, store) do
    prefix = CompoundKey.set_prefix(key)

    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      pairs when is_list(pairs) ->
        {:ok, Enum.map(pairs, fn {member, _} -> member end)}
    end
  end

  defp get_member_sets(keys, store) do
    keys
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, sets} ->
      case get_members_set(key, store) do
        {:ok, set} -> {:cont, {:ok, [set | sets]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, sets} -> {:ok, Enum.reverse(sets)}
      error -> error
    end
  end

  defp member_present?(key, member, store) do
    case Ops.compound_get(store, key, CompoundKey.set_member(key, member)) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        {:ok, false}

      _value ->
        {:ok, true}
    end
  end

  defp member_values(key, members, store) do
    compound_keys = Enum.map(members, &CompoundKey.set_member(key, &1))
    values = Ops.compound_batch_get(store, key, compound_keys)

    case ReadResult.first_failure(values) do
      nil -> {:ok, values}
      failure -> ReadResult.command_error(failure)
    end
  end

  defp check_all_types(keys, store) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case TypeRegistry.command_check_type(key, :set, store) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp maybe_cleanup_empty_set(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_set(key, _removed, store) do
    prefix = CompoundKey.set_prefix(key)

    case Ops.compound_count(store, key, prefix) do
      0 ->
        TypeRegistry.delete_type(key, store)

      count when is_integer(count) and count > 0 ->
        :ok

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      invalid ->
        ReadResult.failure({:invalid_compound_count_result, invalid})
        |> ReadResult.command_error()
    end
  end

  defp delete_set_members_and_cleanup(key, removed_entries, removed_count, store) do
    removed_keys =
      Enum.map(removed_entries, fn {compound_key, _value, _expire_at_ms} -> compound_key end)

    case Ops.compound_batch_delete(store, key, removed_keys) do
      :ok ->
        case maybe_cleanup_empty_set(key, removed_count, store) do
          :ok -> :ok
          {:error, _} = error -> rollback_deleted_set_members(key, removed_entries, store, error)
        end

      {:error, _} = err ->
        err
    end
  end

  defp rollback_deleted_set_members(_key, [], _store, write_error), do: write_error

  defp rollback_deleted_set_members(key, removed_entries, store, write_error) do
    case Ops.compound_batch_put(store, key, removed_entries) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:set_delete_rollback_failed, write_error, rollback_error}}
    end
  end
end
