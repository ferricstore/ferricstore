defmodule Ferricstore.Commands.Generic do
  alias Ferricstore.Commands.CompoundSnapshot
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.TypeRegistry

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @max_scan_count 10_000

  @moduledoc """
  Handles Redis generic key commands: TYPE, UNLINK, RENAME, RENAMENX, COPY,
  RANDOMKEY, SCAN, EXPIRETIME, PEXPIRETIME, OBJECT, WAIT.

  These commands operate on keys regardless of value type. Each handler takes
  the uppercased command name, a list of string arguments, and an injected
  store map. Returns plain Elixir terms -- the connection layer handles wire
  encoding.

  ## Supported commands

    * `TYPE key` -- returns the type of key ("string" for existing, "none" for missing)
    * `UNLINK key [key ...]` -- async DEL; returns count of deleted keys
    * `RENAME key newkey` -- rename key, error if source missing
    * `RENAMENX key newkey` -- rename only if newkey doesn't exist (1 = renamed, 0 = not)
    * `COPY source destination [REPLACE]` -- copy value+TTL (1 = success, 0 = failure)
    * `RANDOMKEY` -- return a random key, or nil if DB is empty
    * `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` -- cursor-based key iteration
    * `EXPIRETIME key` -- absolute Unix timestamp (seconds) when key expires (-1 / -2)
    * `PEXPIRETIME key` -- absolute Unix timestamp (milliseconds) when key expires (-1 / -2)
    * `OBJECT ENCODING key` -- returns actual encoding based on key type
    * `OBJECT HELP` -- returns list of OBJECT subcommands
    * `OBJECT FREQ key` -- returns decayed LFU access frequency counter
    * `OBJECT IDLETIME key` -- returns idle seconds derived from LFU ldt
    * `OBJECT REFCOUNT key` -- always returns 1
    * `WAIT numreplicas timeout` -- returns 0 immediately (no replication)
  """

  alias Ferricstore.CrossShardOp

  @doc """
  Handles a generic key command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"TYPE"`, `"RENAME"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `get_meta`, `put`, `delete`,
      `exists?`, `keys` callbacks

  ## Returns

  Plain Elixir term: string, integer, list, nil, `{:simple, string}`, or
  `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # TYPE
  # ---------------------------------------------------------------------------

  def handle("TYPE", [key], store) do
    type_key(key, store)
  end

  def handle("TYPE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'type' command"}
  end

  # ---------------------------------------------------------------------------
  # UNLINK (same semantics as DEL -- async reclaim deferred to merge)
  # ---------------------------------------------------------------------------

  def handle("UNLINK", [], _store) do
    {:error, "ERR wrong number of arguments for 'unlink' command"}
  end

  def handle("UNLINK", keys, store) do
    # UNLINK has the same semantics as DEL for data-structure cleanup;
    # async reclaim is deferred to merge.
    Ferricstore.Commands.Strings.handle_ast({:del, keys}, store)
  end

  # ---------------------------------------------------------------------------
  # RENAME
  # ---------------------------------------------------------------------------

  def handle("RENAME", [key, newkey], store) do
    rename_key(key, newkey, store)
  end

  def handle("RENAME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'rename' command"}
  end

  # ---------------------------------------------------------------------------
  # RENAMENX
  # ---------------------------------------------------------------------------

  def handle("RENAMENX", [key, newkey], store) do
    renamenx_key(key, newkey, store)
  end

  def handle("RENAMENX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'renamenx' command"}
  end

  # ---------------------------------------------------------------------------
  # COPY
  # ---------------------------------------------------------------------------

  def handle("COPY", [source, destination | opts], store) do
    case parse_copy_opts(opts) do
      {:ok, replace?} ->
        CrossShardOp.execute(
          [{source, :read}, {destination, :write}],
          fn unified_store ->
            do_copy(source, destination, replace?, unified_store)
          end,
          store: store
        )

      {:error, _} = err ->
        err
    end
  end

  def handle("COPY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'copy' command"}
  end

  # ---------------------------------------------------------------------------
  # RANDOMKEY
  # ---------------------------------------------------------------------------

  def handle("RANDOMKEY", [], store), do: random_key(store)

  def handle("RANDOMKEY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'randomkey' command"}
  end

  # ---------------------------------------------------------------------------
  # SCAN
  # ---------------------------------------------------------------------------

  def handle("SCAN", [cursor_str | opts], store) do
    with {:ok, match_pattern, count, type_filter} <- parse_scan_opts(opts) do
      do_scan(cursor_str, match_pattern, count, type_filter, store)
    end
  end

  def handle("SCAN", [], _store) do
    {:error, "ERR wrong number of arguments for 'scan' command"}
  end

  # ---------------------------------------------------------------------------
  # EXPIRETIME
  # ---------------------------------------------------------------------------

  def handle("EXPIRETIME", [key], store), do: expiretime_key(key, store)

  def handle("EXPIRETIME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'expiretime' command"}
  end

  # ---------------------------------------------------------------------------
  # PEXPIRETIME
  # ---------------------------------------------------------------------------

  def handle("PEXPIRETIME", [key], store), do: pexpiretime_key(key, store)

  def handle("PEXPIRETIME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pexpiretime' command"}
  end

  # ---------------------------------------------------------------------------
  # OBJECT -- subcommand is case-insensitive (uppercased before dispatch)
  # ---------------------------------------------------------------------------

  def handle("OBJECT", [], _store) do
    {:error, "ERR wrong number of arguments for 'object' command"}
  end

  def handle("OBJECT", [subcmd | rest], store) do
    do_object(String.upcase(subcmd), rest, store)
  end

  # ---------------------------------------------------------------------------
  # WAIT
  # ---------------------------------------------------------------------------

  def handle("WAIT", [numreplicas_str, timeout_str], _store) do
    with {:ok, _numreplicas} <- parse_wait_integer(numreplicas_str),
         {:ok, _timeout} <- parse_wait_integer(timeout_str) do
      # Replica-offset tracking is not exposed at this command boundary yet.
      0
    end
  end

  def handle("WAIT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'wait' command"}
  end

  @spec handle_ast(term(), map()) :: term()
  def handle_ast(ast, store)

  def handle_ast({:type, key}, store), do: type_key(key, store)

  def handle_ast({:unlink, keys}, store) when is_list(keys) and keys != [] do
    Ferricstore.Commands.Strings.handle_ast({:del, keys}, store)
  end

  def handle_ast({:rename, key, newkey}, store), do: rename_key(key, newkey, store)
  def handle_ast({:renamenx, key, newkey}, store), do: renamenx_key(key, newkey, store)
  def handle_ast({:copy, _source, _destination, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:copy, source, destination, replace?}, store) when is_boolean(replace?) do
    CrossShardOp.execute(
      [{source, :read}, {destination, :write}],
      fn unified_store ->
        do_copy(source, destination, replace?, unified_store)
      end,
      store: store
    )
  end

  def handle_ast({:randomkey, []}, store), do: random_key(store)
  def handle_ast({:scan, _cursor, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:scan, cursor, opts}, store) when is_binary(cursor) and is_list(opts) do
    match_pattern = Keyword.get(opts, :match)
    count = Keyword.get(opts, :count, 10)
    type_filter = Keyword.get(opts, :type)
    do_scan(cursor, match_pattern, count, type_filter, store)
  end

  def handle_ast({:expiretime, key}, store), do: expiretime_key(key, store)
  def handle_ast({:pexpiretime, key}, store), do: pexpiretime_key(key, store)
  def handle_ast({:object, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:object, :encoding, key}, store), do: do_object("ENCODING", [key], store)
  def handle_ast({:object, :freq, key}, store), do: do_object("FREQ", [key], store)
  def handle_ast({:object, :idletime, key}, store), do: do_object("IDLETIME", [key], store)
  def handle_ast({:object, :refcount, key}, store), do: do_object("REFCOUNT", [key], store)
  def handle_ast({:object, :help}, store), do: do_object("HELP", [], store)
  def handle_ast({:wait, {:error, reason}, _timeout}, _store), do: {:error, reason}
  def handle_ast({:wait, _numreplicas, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:wait, numreplicas, timeout}, _store)
      when is_integer(numreplicas) and numreplicas >= 0 and is_integer(timeout) and timeout >= 0,
      do: 0

  def handle_ast({:wait, _numreplicas, _timeout}, _store),
    do: {:error, "ERR value is not an integer or out of range"}

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported generic command AST"}

  defp type_key(key, store) do
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      type -> {:simple, type}
    end
  end

  defp rename_key(key, newkey, store) do
    CrossShardOp.execute(
      [{key, :read_write}, {newkey, :write}],
      fn unified_store ->
        case maybe_key_lifecycle(unified_store, {:rename, key, newkey}) do
          :not_prob -> rename_non_prob_key(key, newkey, unified_store)
          result -> result
        end
      end,
      store: store
    )
  end

  defp rename_non_prob_key(key, newkey, store) do
    case key_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        {:error, "ERR no such key"}

      _expire_at_ms when key == newkey ->
        :ok

      _expire_at_ms ->
        case key_entry(store, key) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          nil ->
            {:error, "ERR no such key"}

          entry ->
            case rename_entry(key, newkey, entry, store) do
              :ok -> :ok
              {:error, _} = error -> error
            end
        end
    end
  end

  defp renamenx_key(key, newkey, store) do
    CrossShardOp.execute(
      [{key, :read_write}, {newkey, :write}],
      fn unified_store ->
        case maybe_key_lifecycle(unified_store, {:renamenx, key, newkey}) do
          :not_prob -> renamenx_non_prob_key(key, newkey, unified_store)
          result -> result
        end
      end,
      store: store
    )
  end

  defp renamenx_non_prob_key(key, newkey, store) do
    case key_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        {:error, "ERR no such key"}

      _expire_at_ms when key == newkey ->
        0

      _expire_at_ms ->
        case key_exists?(store, newkey) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          {:ok, true} ->
            0

          {:ok, false} ->
            case key_entry(store, key) do
              {:error, {:storage_read_failed, _reason}} = failure ->
                ReadResult.command_error(failure)

              nil ->
                {:error, "ERR no such key"}

              entry ->
                case rename_entry(key, newkey, entry, store) do
                  :ok -> 1
                  {:error, _} = error -> error
                end
            end
        end
    end
  end

  defp maybe_key_lifecycle(store, command) when is_map(store) do
    case Map.get(store, :key_lifecycle) do
      lifecycle when is_function(lifecycle, 1) -> lifecycle.(command)
      _not_replicated -> maybe_prob_lifecycle(store, command)
    end
  end

  defp maybe_key_lifecycle(_store, _command), do: :not_prob

  defp maybe_prob_lifecycle(store, command) do
    case Map.get(store, :prob_lifecycle) do
      lifecycle when is_function(lifecycle, 1) -> lifecycle.(command)
      _unsupported -> :not_prob
    end
  end

  defp key_meta(store, key) do
    case Ops.expire_at_ms(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      nil -> compound_expire_at_ms(store, key)
      expire_at_ms -> expire_at_ms
    end
  end

  defp compound_expire_at_ms(store, key) do
    if Ops.has_compound?(store) do
      case Ops.compound_get_meta(store, key, CompoundKey.type_key(key)) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        nil ->
          case Ops.compound_get_meta(store, key, CompoundKey.list_meta_key(key)) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            nil -> nil
            {_meta, expire_at_ms} -> live_compound_expire_at_ms(store, key, "list", expire_at_ms)
          end

        {type, expire_at_ms} ->
          live_compound_expire_at_ms(store, key, type, expire_at_ms)
      end
    end
  end

  defp live_compound_expire_at_ms(store, key, expected_type, expire_at_ms) do
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      ^expected_type -> expire_at_ms
      _other -> nil
    end
  end

  defp random_key(store) do
    case Ops.random_key(store) do
      {:ok, key} ->
        key

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, reason} ->
        ReadResult.command_error(ReadResult.failure(reason))

      :unsupported ->
        random_key_from_full_key_list(store)
    end
  end

  defp random_key_from_full_key_list(store) do
    case Ops.keys(store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      keys ->
        case CompoundKey.user_visible_keys(keys) do
          [] -> nil
          visible_keys -> Enum.random(visible_keys)
        end
    end
  end

  defp expiretime_key(key, store) do
    case key_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      nil -> -2
      0 -> -1
      expire_at_ms -> div(expire_at_ms, 1_000)
    end
  end

  defp pexpiretime_key(key, store) do
    case key_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      nil -> -2
      0 -> -1
      expire_at_ms -> expire_at_ms
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- OBJECT subcommands
  # ---------------------------------------------------------------------------

  defp do_object("ENCODING", [key], store) do
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      "none" ->
        {:error, "ERR no such key"}

      "hash" ->
        "hashtable"

      "list" ->
        "quicklist"

      "set" ->
        "hashtable"

      "zset" ->
        "skiplist"

      "stream" ->
        "stream"

      "string" ->
        string_encoding(store, key)

      _other ->
        "raw"
    end
  end

  defp do_object("HELP", [], _store) do
    [
      "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "ENCODING <key>",
      "  Return the kind of internal representation the Redis object stored at <key> is using.",
      "FREQ <key>",
      "  Return the logarithmic access frequency counter of a Redis object stored at <key>.",
      "HELP",
      "  Return subcommand help summary.",
      "IDLETIME <key>",
      "  Return the idle time of a Redis object stored at <key>.",
      "REFCOUNT <key>",
      "  Return the reference count of the object stored at <key>."
    ]
  end

  defp do_object("FREQ", [key], store) do
    case object_exists?(store, key) do
      {:ok, true} ->
        case Ops.object_lfu(store, key) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          packed_lfu when is_integer(packed_lfu) ->
            Ferricstore.Store.LFU.effective_counter(packed_lfu)

          _ ->
            0
        end

      {:ok, false} ->
        {:error, "ERR no such key"}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp do_object("IDLETIME", [key], store) do
    case object_exists?(store, key) do
      {:ok, true} ->
        case Ops.object_lfu(store, key) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          packed_lfu when is_integer(packed_lfu) ->
            {ldt, _counter} = Ferricstore.Store.LFU.unpack(packed_lfu)
            now_min = Ferricstore.Store.LFU.now_minutes()
            elapsed = Ferricstore.Store.LFU.elapsed_minutes(now_min, ldt)
            elapsed * 60

          _ ->
            0
        end

      {:ok, false} ->
        {:error, "ERR no such key"}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp do_object("REFCOUNT", [key], store) do
    case object_exists?(store, key) do
      {:ok, true} -> 1
      {:ok, false} -> {:error, "ERR no such key"}
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp do_object(subcmd, _rest, _store) do
    {:error,
     "ERR unknown subcommand or wrong number of arguments for '#{String.downcase(subcmd)}' command"}
  end

  defp string_encoding(store, key) do
    case Ops.value_size(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      size when is_integer(size) and size > 44 ->
        "raw"

      _small_or_unknown ->
        case Ops.get(store, key) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          value ->
            cond do
              int_encoded_string?(value) -> "int"
              value != nil and byte_size(value) <= 44 -> "embstr"
              true -> "raw"
            end
        end
    end
  end

  defp object_exists?(store, key) do
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      "none" -> {:ok, false}
      _type -> {:ok, true}
    end
  end

  defp int_encoded_string?(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} ->
        int >= @min_int64 and int <= @max_int64 and Integer.to_string(int) == value

      _ ->
        false
    end
  end

  defp int_encoded_string?(_value), do: false

  defp parse_wait_integer(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _invalid -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- COPY helpers
  # ---------------------------------------------------------------------------

  defp parse_copy_opts([]), do: {:ok, false}

  defp parse_copy_opts([opt]) do
    if String.upcase(opt) == "REPLACE" do
      {:ok, true}
    else
      {:error, "ERR syntax error"}
    end
  end

  defp parse_copy_opts(_) do
    {:error, "ERR syntax error"}
  end

  defp do_copy(source, destination, replace?, store) do
    case maybe_key_lifecycle(store, {:copy, source, destination, replace?}) do
      :not_prob -> do_copy_non_prob(source, destination, replace?, store)
      result -> result
    end
  end

  defp do_copy_non_prob(source, destination, replace?, store) do
    case key_meta(store, source) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        0

      _expire_at_ms ->
        destination_status = if replace?, do: {:ok, false}, else: key_exists?(store, destination)

        case destination_status do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          {:ok, true} when not replace? ->
            0

          {:ok, _destination_exists} ->
            case key_entry(store, source) do
              {:error, {:storage_read_failed, _reason}} = failure ->
                ReadResult.command_error(failure)

              nil ->
                0

              entry ->
                if source != destination do
                  copy_entry_for_copy(source, destination, entry, replace?, store)
                else
                  1
                end
            end
        end
    end
  end

  defp key_exists?(store, key) do
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      "none" -> {:ok, false}
      _type -> {:ok, true}
    end
  end

  defp key_entry(store, key) do
    case Ops.get_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      nil -> compound_entry(store, key)
      {value, expire_at_ms} -> {:plain, value, expire_at_ms}
    end
  end

  defp compound_entry(store, key) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get_meta(store, key, type_key) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        nil ->
          list_meta_key = CompoundKey.list_meta_key(key)

          case Ops.compound_get_meta(store, key, list_meta_key) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            nil -> nil
            {_meta, expire_at_ms} -> live_compound_entry(store, key, "list", expire_at_ms)
          end

        {type, expire_at_ms} ->
          live_compound_entry(store, key, type, expire_at_ms)
      end
    end
  end

  defp live_compound_entry(store, key, expected_type, expire_at_ms) do
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      ^expected_type -> {:compound, expected_type, expire_at_ms}
      _other -> nil
    end
  end

  defp rename_entry(source, destination, _entry, _store) when source == destination, do: :ok

  defp rename_entry(source, destination, entry, store) do
    case prepare_entry(source, destination, entry, store) do
      {:ok, prepared} ->
        replace_entry_preserving_destination(destination, prepared, store, fn ->
          delete_key_result(source, store)
        end)

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp copy_entry_for_copy(_source, destination, {:plain, value, expire_at_ms}, true, store) do
    case compound_destination_exists?(destination, store) do
      {:ok, true} ->
        destination
        |> replace_entry_preserving_destination(
          {:plain, value, expire_at_ms},
          store,
          fn -> :ok end
        )
        |> copy_result()

      {:ok, false} ->
        store
        |> Ops.put(destination, value, expire_at_ms)
        |> copy_result()

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp copy_entry_for_copy(source, destination, entry, true, store) do
    case prepare_entry(source, destination, entry, store) do
      {:ok, prepared} ->
        destination
        |> replace_entry_preserving_destination(prepared, store, fn -> :ok end)
        |> copy_result()

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp copy_entry_for_copy(source, destination, entry, false, store) do
    case prepare_entry(source, destination, entry, store) do
      {:ok, prepared} -> prepared |> write_prepared_entry(destination, store) |> copy_result()
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp compound_destination_exists?(destination, store) do
    if Ops.has_compound?(store) do
      case Ops.compound_get(store, destination, CompoundKey.type_key(destination)) do
        {:error, {:storage_read_failed, _reason}} = failure -> failure
        nil -> {:ok, false}
        _type -> {:ok, true}
      end
    else
      {:ok, false}
    end
  end

  defp prepare_entry(_source, _destination, {:plain, _value, _expire_at_ms} = entry, _store),
    do: {:ok, entry}

  defp prepare_entry(source, destination, {:compound, type, _expire_at_ms}, store) do
    case CompoundSnapshot.copy(source, destination, type, store) do
      {:ok, entries} -> {:ok, {:compound, entries}}
      {:error, {:storage_read_failed, _reason}} = failure -> failure
    end
  end

  defp write_prepared_entry({:plain, value, expire_at_ms}, destination, store) do
    Ops.put(store, destination, value, expire_at_ms)
  end

  defp write_prepared_entry({:compound, entries}, destination, store) do
    Ops.compound_batch_put(store, destination, entries)
  end

  defp replace_entry_preserving_destination(destination, prepared, store, after_write_fun) do
    case key_backup(store, destination) do
      {:ok, backup} ->
        replace_entry_from_backup(destination, prepared, backup, store, after_write_fun)

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp replace_entry_from_backup(destination, prepared, backup, store, after_write_fun) do
    case write_replacement(destination, prepared, backup, store) do
      :ok ->
        case after_write_fun.() do
          :ok -> :ok
          {:error, _} = error -> restore_backup_or_error(destination, backup, store, error)
        end

      {:error, _} = error ->
        restore_backup_or_error(destination, backup, store, error)

      {{:error, _} = error, :no_restore} ->
        error
    end
  end

  defp write_replacement(destination, {:plain, value, expire_at_ms}, backup, store) do
    case backup do
      {:compound, _entries} ->
        with :ok <- delete_key_result(destination, store) do
          Ops.put(store, destination, value, expire_at_ms)
        end

      _missing_or_plain ->
        case Ops.put(store, destination, value, expire_at_ms) do
          :ok -> :ok
          {:error, _} = error -> {error, :no_restore}
        end
    end
  end

  defp write_replacement(destination, {:compound, _entries} = prepared, backup, store) do
    case backup do
      :missing ->
        write_prepared_entry(prepared, destination, store)

      _existing ->
        with :ok <- delete_key_result(destination, store) do
          write_prepared_entry(prepared, destination, store)
        end
    end
  end

  defp key_backup(store, key) do
    case Ops.get_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      {value, expire_at_ms} ->
        {:ok, {:plain, value, expire_at_ms}}

      nil ->
        compound_key_backup(store, key)
    end
  end

  defp compound_key_backup(store, key) do
    if Ops.has_compound?(store) do
      case Ops.compound_get_meta(store, key, CompoundKey.type_key(key)) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        nil ->
          {:ok, :missing}

        {type, _expire_at_ms} when type in ["hash", "list", "set", "zset", "stream"] ->
          case CompoundSnapshot.snapshot(key, type, store) do
            {:ok, entries} -> {:ok, {:compound, entries}}
            {:error, {:storage_read_failed, _reason}} = failure -> failure
          end

        {type, _expire_at_ms} ->
          ReadResult.failure({:unsupported_destination_type, type})
      end
    else
      {:ok, :missing}
    end
  end

  defp restore_backup_or_error(destination, backup, store, original_error) do
    case restore_key_backup(destination, backup, store) do
      :ok -> original_error
      {:error, _} = restore_error -> restore_error
    end
  end

  defp restore_key_backup(destination, :missing, store) do
    delete_key_result(destination, store)
  end

  defp restore_key_backup(destination, {:plain, value, expire_at_ms}, store) do
    with :ok <- delete_key_result(destination, store) do
      Ops.put(store, destination, value, expire_at_ms)
    end
  end

  defp restore_key_backup(destination, {:compound, entries}, store) do
    with :ok <- delete_key_result(destination, store) do
      Ops.compound_batch_put(store, destination, entries)
    end
  end

  defp delete_key(key, store) do
    Ferricstore.Commands.Strings.handle_ast({:del, [key]}, store)
  end

  defp delete_key_result(key, store) do
    case delete_key(key, store) do
      count when is_integer(count) -> :ok
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  defp copy_result(:ok), do: 1
  defp copy_result({:error, _} = error), do: error

  # ---------------------------------------------------------------------------
  # Private -- SCAN option parsing and execution
  # ---------------------------------------------------------------------------

  defp parse_scan_opts(opts), do: parse_scan_opts(opts, nil, 10, nil)

  defp parse_scan_opts([], match, count, type), do: {:ok, match, count, type}

  defp parse_scan_opts([opt, value | rest], match, count, type) do
    case String.upcase(opt) do
      "MATCH" ->
        parse_scan_opts(rest, value, count, type)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 and n <= @max_scan_count ->
            parse_scan_opts(rest, match, n, type)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end

      "TYPE" ->
        parse_scan_opts(rest, match, count, String.downcase(value))

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_scan_opts([_ | _], _match, _count, _type) do
    {:error, "ERR syntax error"}
  end

  defp do_scan(cursor_str, match_pattern, count, type_filter, store) do
    case Ops.scan_keys_page(store, cursor_str, count, match_pattern, type_filter) do
      {:ok, {next_cursor, keys}} when is_binary(next_cursor) and is_list(keys) ->
        [next_cursor, keys]

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, message} when is_binary(message) ->
        {:error, message}

      {:error, reason} ->
        ReadResult.command_error(ReadResult.failure(reason))

      :unsupported ->
        do_scan_from_full_key_list(cursor_str, match_pattern, count, type_filter, store)
    end
  end

  defp do_scan_from_full_key_list(cursor_str, match_pattern, count, type_filter, store) do
    alias Ferricstore.Store.CompoundKey

    case Ops.keys(store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      keys ->
        keys = CompoundKey.user_visible_keys(keys)

        case filter_by_type(keys, type_filter, store) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          {:ok, typed_keys} ->
            all_keys = typed_keys |> filter_by_match(match_pattern) |> Enum.sort()

            # Cursor "0" means start from the beginning. Otherwise, cursor is the last
            # key seen -- find the first key strictly after it alphabetically.
            remaining =
              if cursor_str == "0" do
                all_keys
              else
                Enum.drop_while(all_keys, fn k -> k <= cursor_str end)
              end

            {batch, rest} = Enum.split(remaining, count)

            next_cursor =
              case {batch, rest} do
                {[], _} -> "0"
                {_, []} -> "0"
                _ -> List.last(batch)
              end

            [next_cursor, batch]
        end
    end
  end

  defp filter_by_type(keys, nil, _store), do: {:ok, keys}

  defp filter_by_type(keys, type_filter, store) do
    keys
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, matching} ->
      case TypeRegistry.get_type(key, store) do
        {:error, {:storage_read_failed, _reason}} = failure -> {:halt, failure}
        ^type_filter -> {:cont, {:ok, [key | matching]}}
        _other -> {:cont, {:ok, matching}}
      end
    end)
    |> case do
      {:ok, matching} -> {:ok, Enum.reverse(matching)}
      failure -> failure
    end
  end

  defp filter_by_match(keys, nil), do: keys

  defp filter_by_match(keys, pattern) do
    Enum.filter(keys, &Ferricstore.GlobMatcher.match?(&1, pattern))
  end
end
