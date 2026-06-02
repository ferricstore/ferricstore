defmodule Ferricstore.Commands.Stream do
  @moduledoc """
  Handles Redis Stream commands: XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM,
  XDEL, XINFO STREAM, XGROUP CREATE, XREADGROUP, and XACK.

  ## Storage layout

  Stream entries are stored in the shared Bitcask via compound keys:

      X:{stream_key}\\x00{ms}-{seq}  =>  :erlang.term_to_binary(field_value_pairs)

  Stream metadata is tracked in an ETS table (`Ferricstore.Stream.Meta`) for
  fast access without Bitcask reads:

      {stream_key} => {length, first_id, last_id, last_ms, last_seq}

  Consumer group state is persisted as stream compound metadata and cached in
  a second ETS table (`Ferricstore.Stream.Groups`):

      {stream_key, group_name} => {last_delivered_id, consumers, pending}

  ## Stream IDs

  Stream IDs follow the Redis format `{milliseconds}-{sequence}`. When the
  client sends `*`, the server auto-generates a monotonically increasing ID
  using `Ferricstore.CommandTime.now_ms/0` as the milliseconds component and an
  incrementing sequence number when multiple entries arrive in the same
  millisecond. Outside Raft this reads the Hybrid Logical Clock; inside Raft it
  reads the stamped log-entry time so replicas generate the same IDs.

  Explicit IDs must be strictly greater than the last entry's ID.
  """

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  alias Ferricstore.CommandTime
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @typedoc "A parsed stream ID as `{milliseconds, sequence}`."
  @type stream_id :: {non_neg_integer(), non_neg_integer()}

  @typedoc "A stream entry: `{id_string, [field, value, ...]}` flat list."
  @type entry :: {binary(), [binary()]}

  @meta_table Ferricstore.Stream.Meta
  @groups_table Ferricstore.Stream.Groups
  @group_locks_table Ferricstore.Stream.GroupLocks
  @index_table Ferricstore.Stream.Index
  @stream_waiters_table :ferricstore_stream_waiters

  # Null byte separator between stream key and entry ID in compound keys.
  @sep <<0>>
  @max_int64 9_223_372_036_854_775_807

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Handles a stream command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"XADD"`, `"XLEN"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks

  ## Returns

  Plain Elixir term: string, integer, list, map, `:ok`, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # -------------------------------------------------------------------------
  # XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|ID field value [field value ...]
  # -------------------------------------------------------------------------

  def handle("XADD", args, store) when length(args) >= 4 do
    case parse_xadd_args(args) do
      {:ok, key, id_spec, fields, trim_opts, nomkstream} ->
        do_xadd(key, id_spec, fields, trim_opts, nomkstream, store)

      {:error, _} = err ->
        err
    end
  end

  def handle("XADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xadd' command"}
  end

  # -------------------------------------------------------------------------
  # XLEN key
  # -------------------------------------------------------------------------

  def handle("XLEN", [key], store), do: xlen_key(key, store)

  def handle("XLEN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xlen' command"}
  end

  # -------------------------------------------------------------------------
  # XRANGE key start end [COUNT count]
  # -------------------------------------------------------------------------

  def handle("XRANGE", [key, start_str, end_str | rest], store) do
    with {:ok, count} <- parse_count_opt(rest),
         {:ok, range_start} <- parse_range_id(start_str, :min),
         {:ok, range_end} <- parse_range_id(end_str, :max) do
      do_xrange(key, range_start, range_end, count, store)
    end
  end

  def handle("XRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xrange' command"}
  end

  # -------------------------------------------------------------------------
  # XREVRANGE key end start [COUNT count]
  # -------------------------------------------------------------------------

  def handle("XREVRANGE", [key, end_str, start_str | rest], store) do
    with {:ok, count} <- parse_count_opt(rest),
         {:ok, range_start} <- parse_range_id(start_str, :min),
         {:ok, range_end} <- parse_range_id(end_str, :max) do
      do_xrevrange(key, range_start, range_end, count, store)
    end
  end

  def handle("XREVRANGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xrevrange' command"}
  end

  # -------------------------------------------------------------------------
  # XREAD [COUNT count] [BLOCK timeout] STREAMS key [key ...] id [id ...]
  # -------------------------------------------------------------------------

  def handle("XREAD", args, store) do
    case parse_xread_args(args) do
      {:ok, count, :no_block, stream_ids} ->
        do_xread(stream_ids, count, store)

      {:ok, count, {:block, timeout_ms}, stream_ids} ->
        # Try an immediate read first. If data is available, return it.
        result = do_xread(stream_ids, count, store)

        if result == [] do
          # No data available -- signal the connection layer to block.
          {:block, timeout_ms, stream_ids, count}
        else
          result
        end

      {:error, _} = err ->
        err
    end
  end

  # -------------------------------------------------------------------------
  # XTRIM key MAXLEN|MINID [=|~] threshold
  # -------------------------------------------------------------------------

  def handle("XTRIM", [key | rest], store) do
    case parse_trim_opts(rest) do
      {:ok, trim_opts} ->
        do_trim(key, trim_opts, store)

      {:error, _} = err ->
        err
    end
  end

  def handle("XTRIM", [], _store) do
    {:error, "ERR wrong number of arguments for 'xtrim' command"}
  end

  # -------------------------------------------------------------------------
  # XDEL key id [id ...]
  # -------------------------------------------------------------------------

  def handle("XDEL", [key | ids], store) when ids != [] do
    do_xdel(key, ids, store)
  end

  def handle("XDEL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xdel' command"}
  end

  # -------------------------------------------------------------------------
  # XINFO STREAM key [FULL [COUNT count]]
  # -------------------------------------------------------------------------

  def handle("XINFO", ["STREAM", key | _rest], store) do
    do_xinfo_stream(key, store)
  end

  def handle("XINFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xinfo' command"}
  end

  # -------------------------------------------------------------------------
  # XGROUP CREATE key groupname id [MKSTREAM]
  # -------------------------------------------------------------------------

  def handle("XGROUP", ["CREATE", key, group, id_str | rest], store) do
    mkstream = Enum.any?(rest, &(String.upcase(&1) == "MKSTREAM"))
    do_xgroup_create(key, group, id_str, mkstream, store)
  end

  def handle("XGROUP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xgroup' command"}
  end

  # -------------------------------------------------------------------------
  # XREADGROUP GROUP group consumer [COUNT count] STREAMS key [key ...] id [id ...]
  # -------------------------------------------------------------------------

  def handle("XREADGROUP", args, store) do
    case parse_xreadgroup_args(args) do
      {:ok, group, consumer, count, :no_block, stream_ids} ->
        do_xreadgroup(group, consumer, stream_ids, count, store)

      {:ok, group, consumer, count, {:block, timeout_ms}, stream_ids} ->
        result = do_xreadgroup(group, consumer, stream_ids, count, store)

        if result == [] do
          {:block, timeout_ms, stream_ids, count}
        else
          result
        end

      {:error, _} = err ->
        err
    end
  end

  # -------------------------------------------------------------------------
  # XACK key group id [id ...]
  # -------------------------------------------------------------------------

  def handle("XACK", [key, group | ids], store) when ids != [] do
    do_xack(key, group, ids, store)
  end

  def handle("XACK", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xack' command"}
  end

  @spec handle_ast(term(), map()) :: term()
  def handle_ast(ast, store)

  def handle_ast({:xadd, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xadd, key, {id_spec, fields, trim_opts, nomkstream}}, store)
      when is_list(fields) and is_boolean(nomkstream) do
    trim_opts = if trim_opts == nil, do: nil, else: trim_opts
    do_xadd(key, id_spec, fields, trim_opts, nomkstream, store)
  end

  def handle_ast({:xlen, key}, store), do: xlen_key(key, store)
  def handle_ast({:xrange, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:xrevrange, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xrange, key, range_start, range_end, count}, store) do
    with {:ok, range_start} <- normalize_ast_range_id(range_start, :min),
         {:ok, range_end} <- normalize_ast_range_id(range_end, :max) do
      count = normalize_ast_count(count)
      do_xrange(key, range_start, range_end, count, store)
    end
  end

  def handle_ast({:xrevrange, key, range_start, range_end, count}, store) do
    with {:ok, range_start} <- normalize_ast_range_id(range_start, :min),
         {:ok, range_end} <- normalize_ast_range_id(range_end, :max) do
      count = normalize_ast_count(count)
      do_xrevrange(key, range_start, range_end, count, store)
    end
  end

  def handle_ast({:xread, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xread, count, :no_block, stream_ids}, store) do
    do_xread(stream_ids, count, store)
  end

  def handle_ast({:xread, count, {:block, timeout_ms}, stream_ids}, store) do
    result = do_xread(stream_ids, count, store)

    if result == [] do
      {:block, timeout_ms, stream_ids, count}
    else
      result
    end
  end

  def handle_ast({:xtrim, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:xtrim, key, trim_opts}, store), do: do_trim(key, trim_opts, store)

  def handle_ast({:xdel, key, ids}, store) when is_list(ids) and ids != [],
    do: do_xdel(key, ids, store)

  def handle_ast({:xinfo_stream, key}, store), do: do_xinfo_stream(key, store)
  def handle_ast({:xinfo, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:xgroup, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xgroup_create, key, group, id_str, mkstream}, store)
      when is_boolean(mkstream) do
    do_xgroup_create(key, group, id_str, mkstream, store)
  end

  def handle_ast({:xreadgroup, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xreadgroup, group, consumer, {count, :no_block, stream_ids}}, store) do
    do_xreadgroup(group, consumer, stream_ids, count, store)
  end

  def handle_ast({:xreadgroup, group, consumer, {count, {:block, timeout_ms}, stream_ids}}, store) do
    result = do_xreadgroup(group, consumer, stream_ids, count, store)

    if result == [] do
      {:block, timeout_ms, stream_ids, count}
    else
      result
    end
  end

  def handle_ast({:xack, key, group, ids}, store) when is_list(ids) and ids != [] do
    do_xack(key, group, ids, store)
  end

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported stream command AST"}

  defp xlen_key(key, store) do
    # `@meta_table` is local-only (not Raft-replicated), so on a follower we
    # don't have it. Fall back to counting the stream's compound entries —
    # those go through Router.compound_put and are present on every node.
    # On the originating node we still consult the meta table for O(1) speed
    # when populated; followers and post-migration always count via prefix.
    ensure_meta_table()

    with :ok <- ensure_stream_read_type(key, store) do
      case stream_meta_entries(key, store) do
        [{^key, len, _first, _last, _ms, _seq}] -> len
        [] -> count_stream_entries(store, key)
      end
    end
  end

  defp ensure_stream_read_type(key, store) do
    case :ets.lookup(@meta_table, key) do
      [_entry] ->
        ensure_live_stream_metadata(key, store)

      [] ->
        if Ops.has_compound?(store), do: TypeRegistry.check_type(key, :stream, store), else: :ok
    end
  end

  defp ensure_live_stream_metadata(key, store) do
    cond do
      not Ops.has_compound?(store) ->
        :ok

      stream_type_marker?(key, store) ->
        :ok

      TypeRegistry.get_type(key, store) == "none" ->
        cleanup_local_stream_metadata(key)
        :ok

      true ->
        TypeRegistry.check_type(key, :stream, store)
    end
  end

  defp stream_meta_entries(key, store) do
    case :ets.lookup(@meta_table, key) do
      [] -> rebuild_stream_meta_entries(key, store)
      entries -> entries
    end
  end

  defp xadd_meta_entries(key, store) do
    case :ets.lookup(@meta_table, key) do
      [] ->
        if stream_type_marker?(key, store), do: rebuild_stream_meta_entries(key, store), else: []

      entries ->
        entries
    end
  end

  defp stream_type_marker?(key, store) do
    Ops.has_compound?(store) and
      Ops.compound_get(store, key, CompoundKey.type_key(key)) == "stream"
  end

  defp rebuild_stream_meta_entries(key, store) do
    if Ops.has_compound?(store) do
      ids =
        store
        |> stream_fields_for(key)
        |> Enum.sort_by(&parse_id!/1)

      case ids do
        [] ->
          case durable_stream_meta_entry(key, store) do
            nil ->
              if stream_type_marker?(key, store) do
                put_local_stream_meta(key, 0, "0-0", "0-0", 0, 0)
                :ets.lookup(@meta_table, key)
              else
                []
              end

            {len, first, last, ms, seq} ->
              put_local_stream_meta(key, len, first, last, ms, seq)
              :ets.lookup(@meta_table, key)
          end

        _ ->
          first = List.first(ids)
          last = List.last(ids)
          {last_ms, last_seq} = parse_id!(last)
          put_stream_meta(key, length(ids), first, last, last_ms, last_seq, store)
          :ets.lookup(@meta_table, key)
      end
    else
      []
    end
  end

  defp durable_stream_meta_entry(key, store) do
    store
    |> Ops.compound_get(key, CompoundKey.stream_meta_key(key))
    |> decode_stream_meta()
  end

  defp decode_stream_meta(nil), do: nil

  defp decode_stream_meta(raw) when is_binary(raw) do
    case :erlang.binary_to_term(raw, [:safe]) do
      {:stream_meta, len, first, last, ms, seq}
      when is_integer(len) and len >= 0 and is_binary(first) and is_binary(last) and
             is_integer(ms) and ms >= 0 and is_integer(seq) and seq >= 0 ->
        {len, first, last, ms, seq}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  defp put_local_stream_meta(key, len, first, last, ms, seq) do
    :ets.insert(@meta_table, {key, len, first, last, ms, seq})
  end

  defp put_stream_meta(key, len, first, last, ms, seq, store) do
    put_local_stream_meta(key, len, first, last, ms, seq)
    persist_stream_meta(key, len, first, last, ms, seq, store)
  end

  defp persist_stream_meta(key, len, first, last, ms, seq, store) do
    if Ops.has_compound?(store) do
      encoded = :erlang.term_to_binary({:stream_meta, len, first, last, ms, seq})
      Ops.compound_put(store, key, CompoundKey.stream_meta_key(key), encoded, 0)
    else
      :ok
    end
  end

  # ---------------------------------------------------------------------------
  # ETS table management
  # ---------------------------------------------------------------------------

  @doc """
  Eagerly creates the stream ETS tables.

  Must be called once during application startup (from `Application.start/2`)
  so that the tables are owned by the long-lived application process.  This
  prevents the tables from being destroyed when short-lived connection
  processes exit.
  """
  @spec init_tables() :: :ok
  def init_tables do
    ensure_meta_table()
  end

  @doc """
  Clears all local, non-durable stream state.

  Stream entries live in the store through compound keys. These ETS tables are
  acceleration/waiter state and must not survive FLUSHDB/FLUSHALL, otherwise a
  recreated stream can retain stale range-index rows or blocked readers.
  """
  @spec clear_local_state() :: :ok
  def clear_local_state do
    Ferricstore.Stream.LocalState.clear()
  end

  @doc """
  Ensures the stream metadata ETS tables exist.

  Called lazily on first use. The tables are `:public` and `:named_table`
  so any process can read and write them. When `init_tables/0` has been
  called at application startup, this is a cheap no-op.
  """
  @spec ensure_meta_table() :: :ok
  def ensure_meta_table do
    case :ets.whereis(@meta_table) do
      :undefined ->
        try do
          :ets.new(@meta_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end

    case :ets.whereis(@groups_table) do
      :undefined ->
        try do
          :ets.new(@groups_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end

    case :ets.whereis(@group_locks_table) do
      :undefined ->
        try do
          :ets.new(@group_locks_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end

    case :ets.whereis(@stream_waiters_table) do
      :undefined ->
        try do
          :ets.new(@stream_waiters_table, [:duplicate_bag, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end

    case :ets.whereis(@index_table) do
      :undefined ->
        try do
          :ets.new(@index_table, [
            :ordered_set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Stream waiter management (for XREAD BLOCK)
  # ---------------------------------------------------------------------------

  @doc """
  Registers `pid` as a waiter for new entries on `stream_key`.

  When XADD inserts a new entry into this stream, all registered waiters
  receive `{:stream_waiter_notify, stream_key}`.

  ## Parameters

    - `stream_key` -- the Redis key of the stream
    - `pid` -- the process to notify
    - `last_seen_id` -- the last ID the caller has seen (for future filtering)
  """
  @spec register_stream_waiter(binary(), pid(), binary()) :: :ok
  def register_stream_waiter(stream_key, pid, last_seen_id) do
    ensure_meta_table()
    Ferricstore.Waiters.Monitor.track(pid)
    registered_at = System.monotonic_time(:microsecond)
    :ets.insert(@stream_waiters_table, {stream_key, pid, last_seen_id, registered_at})
    :ok
  end

  @doc """
  Unregisters `pid` as a waiter for `stream_key`.
  """
  @spec unregister_stream_waiter(binary(), pid()) :: :ok
  def unregister_stream_waiter(stream_key, pid) do
    :ets.match_delete(@stream_waiters_table, {stream_key, pid, :_, :_})
    :ok
  end

  @doc """
  Removes all stream waiters registered by `pid` across all keys.

  Called when a client disconnects.
  """
  @spec cleanup_stream_waiters(pid()) :: :ok
  def cleanup_stream_waiters(pid) do
    if :ets.whereis(@stream_waiters_table) != :undefined do
      :ets.match_delete(@stream_waiters_table, {:_, pid, :_, :_})
    end

    :ok
  end

  @doc """
  Returns the number of stream waiters for `stream_key`.
  """
  @spec stream_waiter_count(binary()) :: non_neg_integer()
  def stream_waiter_count(stream_key) do
    ensure_meta_table()
    :ets.match(@stream_waiters_table, {stream_key, :_, :_, :_}) |> length()
  end

  @doc false
  @spec notify_stream_waiters(binary()) :: :ok
  def notify_stream_waiters(stream_key) do
    case :ets.whereis(@stream_waiters_table) do
      :undefined ->
        :ok

      _ref ->
        entries = :ets.lookup(@stream_waiters_table, stream_key)

        Enum.each(entries, fn {_key, pid, _last_id, _reg_at} ->
          send(pid, {:stream_waiter_notify, stream_key})
        end)

        # Remove all notified waiters.
        :ets.match_delete(@stream_waiters_table, {stream_key, :_, :_, :_})
        :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XADD
  # ---------------------------------------------------------------------------

  defp do_xadd(key, id_spec, fields, trim_opts, nomkstream, store) do
    ensure_meta_table()
    meta_entries = xadd_meta_entries(key, store)

    # Check if stream exists when NOMKSTREAM is set
    case meta_entries do
      [] when nomkstream -> nil
      meta_entries -> do_xadd_insert(key, id_spec, fields, trim_opts, meta_entries, store)
    end
  end

  defp do_xadd_insert(key, id_spec, fields, trim_opts, meta_entries, store) do
    with type_status when type_status in [:ok, {:ok, :created}, :no_marker] <-
           stream_type_status(key, store) do
      {last_ms, last_seq} =
        case meta_entries do
          [{^key, _len, _first, _last, ms, seq}] -> {ms, seq}
          [] -> {0, 0}
        end

      case resolve_id(id_spec, last_ms, last_seq) do
        {:ok, {ms, seq}} ->
          id_str = "#{ms}-#{seq}"
          compound_key = stream_entry_key(key, id_str)

          # Serialize field-value pairs as Erlang binary term.
          encoded = :erlang.term_to_binary(fields)

          case put_stream_entry(store, key, compound_key, encoded) do
            :ok ->
              maybe_index_stream_put(store, key, id_str, compound_key, meta_entries)

              # Update metadata.
              {new_len, new_first} =
                case meta_entries do
                  [{^key, len, first, _last, _ms, _seq}] ->
                    {len + 1, first}

                  [] ->
                    {1, id_str}
                end

              put_stream_meta(key, new_len, new_first, id_str, ms, seq, store)

              # Apply trim if requested.
              maybe_trim(key, trim_opts, store)

              # Notify any XREAD BLOCK waiters watching this stream.
              notify_stream_waiters(key)

              id_str

            {:error, _} = error ->
              rollback_new_stream_type_marker(key, store, type_status, error)
          end

        {:error, _} = err ->
          rollback_new_stream_type_marker(key, store, type_status, err)
      end
    end
  end

  defp rollback_new_stream_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.delete_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:stream_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_stream_type_marker(_key, _store, :ok, write_error), do: write_error
  defp rollback_new_stream_type_marker(_key, _store, :no_marker, write_error), do: write_error

  defp stream_type_status(key, store) do
    if Ops.has_compound?(store) do
      TypeRegistry.check_or_set_status(key, :stream, store)
    else
      :no_marker
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XRANGE / XREVRANGE
  # ---------------------------------------------------------------------------

  defp do_xrange(key, range_start, range_end, count, store) do
    ensure_meta_table()

    with :ok <- ensure_stream_read_type(key, store) do
      if Ops.has_compound?(store) do
        indexed_stream_range(key, range_start, range_end, count, false, store)
      else
        scanned_stream_range(key, range_start, range_end, count, store)
      end
    end
  end

  defp do_xrevrange(key, range_start, range_end, count, store) do
    ensure_meta_table()

    with :ok <- ensure_stream_read_type(key, store) do
      if Ops.has_compound?(store) do
        indexed_stream_range(key, range_start, range_end, count, true, store)
      else
        key
        |> scanned_stream_range(range_start, range_end, :infinity, store)
        |> Enum.reverse()
        |> maybe_take(count)
      end
    end
  end

  defp scanned_stream_range(key, range_start, range_end, count, store) do
    case :ets.lookup(@meta_table, key) do
      [] ->
        []

      [{^key, _len, _first, _last, _ms, _seq}] ->
        selected_entries =
          store
          |> stream_entries_for(key)
          |> Enum.map(fn {id_str, raw} -> {id_str, parse_id!(id_str), raw} end)
          |> Enum.filter(fn {_id_str, id, _raw} ->
            id_in_range?(id, range_start, range_end)
          end)
          |> Enum.sort_by(fn {_id_str, id, _raw} -> id end)
          |> maybe_take(count)

        Enum.flat_map(selected_entries, fn {id_str, _id, raw} ->
          case decode_stream_fields(raw) do
            {:ok, fields} -> [[id_str | fields]]
            :error -> []
          end
        end)
    end
  end

  defp indexed_stream_range(key, range_start, range_end, count, reverse?, store) do
    with :ok <- ensure_stream_index(key, store) do
      key
      |> stream_index_slice(range_start, range_end, count, reverse?)
      |> decode_indexed_stream_entries(key, store)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XREAD
  # ---------------------------------------------------------------------------

  defp do_xread(stream_ids, count, store) do
    ensure_meta_table()

    case xread_results(stream_ids, count, store, []) do
      {:error, _} = err -> err
      results -> Enum.reverse(results)
    end
  end

  defp xread_results([], _count, _store, acc), do: acc

  defp xread_results([{key, id_str} | rest], count, store, acc) do
    # Handle "$" -- resolve to current last ID of the stream.
    resolved_id =
      if id_str == "$" do
        case stream_meta_entries(key, store) do
          [{^key, _len, _first, last, _ms, _seq}] -> last
          [] -> "0-0"
        end
      else
        id_str
      end

    # For XREAD, the start is exclusive (entries > id).
    case parse_exclusive_start(resolved_id) do
      {:ok, excl_start} ->
        case do_xrange(key, excl_start, :max, count, store) do
          [] -> xread_results(rest, count, store, acc)
          entries -> xread_results(rest, count, store, [[key, entries] | acc])
        end

      {:error, _} = err ->
        err
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XTRIM
  # ---------------------------------------------------------------------------

  defp do_trim(key, trim_opts, store) do
    ensure_meta_table()

    case :ets.lookup(@meta_table, key) do
      [] -> 0
      [{^key, _len, _first, _last, _ms, _seq}] -> apply_trim(key, trim_opts, store)
    end
  end

  defp maybe_trim(_key, nil, _store), do: :ok

  defp maybe_trim(key, trim_opts, store) do
    apply_trim(key, trim_opts, store)
    :ok
  end

  defp apply_trim(key, {:maxlen, _approx, max_len}, store) do
    if Ops.has_compound?(store) do
      apply_trim_maxlen_indexed(key, max_len, store)
    else
      apply_trim_maxlen_scanned(key, max_len, store)
    end
  end

  defp apply_trim(key, {:minid, _approx, min_id_str}, store) do
    prefix = "X:#{key}" <> @sep

    case parse_full_id(min_id_str) do
      {:error, _} = err -> err
      {:ok, min_id} -> do_apply_trim_minid(key, prefix, min_id, store)
    end
  end

  defp apply_trim_maxlen_scanned(key, max_len, store) do
    all_ids =
      store
      |> stream_ids_for(key)
      |> Enum.sort_by(&parse_id!/1)

    current_len = length(all_ids)

    if current_len > max_len do
      to_remove = Enum.take(all_ids, current_len - max_len)

      case delete_stream_ids(key, to_remove, store) do
        {:ok, deleted_count} ->
          # Update metadata.
          remaining = Enum.drop(all_ids, deleted_count)
          update_meta_after_trim(key, remaining, store)

          deleted_count

        {:error, reason, deleted_count} ->
          if deleted_count > 0 do
            remaining = Enum.drop(all_ids, deleted_count)
            update_meta_after_trim(key, remaining, store)
          end

          {:error, reason}
      end
    else
      0
    end
  end

  defp do_apply_trim_minid(key, prefix, min_id, store) do
    if Ops.has_compound?(store) do
      do_apply_trim_minid_indexed(key, min_id, store)
    else
      do_apply_trim_minid_scanned(key, prefix, min_id, store)
    end
  end

  defp do_apply_trim_minid_scanned(key, _prefix, min_id, store) do
    all_ids =
      store
      |> stream_ids_for(key)
      |> Enum.sort_by(&parse_id!/1)

    {to_remove, _keep} =
      Enum.split_with(all_ids, fn id_str ->
        id_cmp(parse_id!(id_str), min_id) == :lt
      end)

    case delete_stream_ids(key, to_remove, store) do
      {:ok, deleted_count} ->
        if deleted_count > 0 do
          remaining = all_ids -- to_remove
          update_meta_after_trim(key, remaining, store)
        end

        deleted_count

      {:error, reason, deleted_count} ->
        if deleted_count > 0 do
          deleted_ids = Enum.take(to_remove, deleted_count)
          remaining = all_ids -- deleted_ids
          update_meta_after_trim(key, remaining, store)
        end

        {:error, reason}
    end
  end

  defp apply_trim_maxlen_indexed(key, max_len, store) do
    ensure_stream_index(key, store)

    case stream_meta_entries(key, store) do
      [{^key, len, _first, last, ms, seq}] when len > max_len ->
        delete_count = len - max_len

        ids_to_remove = stream_index_ids(key, delete_count)

        case delete_stream_ids(key, ids_to_remove, store) do
          {:ok, ^delete_count} ->
            update_meta_after_index_mutation(key, max_len, last, ms, seq, store)
            delete_count

          {:error, reason, deleted_count} ->
            if deleted_count > 0 do
              update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)
            end

            {:error, reason}
        end

      _ ->
        0
    end
  end

  defp do_apply_trim_minid_indexed(key, min_id, store) do
    ensure_stream_index(key, store)

    case stream_meta_entries(key, store) do
      [{^key, len, _first, last, ms, seq}] ->
        to_remove =
          key
          |> stream_index_slice(:min, exclusive_upper_bound(min_id), :infinity, false)
          |> Enum.map(fn {id_str, _compound_key} -> id_str end)

        case delete_stream_ids(key, to_remove, store) do
          {:ok, deleted_count} ->
            if deleted_count > 0 do
              update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)
            end

            deleted_count

          {:error, reason, deleted_count} ->
            if deleted_count > 0 do
              update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)
            end

            {:error, reason}
        end

      [] ->
        0
    end
  end

  defp delete_stream_ids(_key, [], _store), do: {:ok, 0}

  defp delete_stream_ids(key, ids, store) do
    compound_keys = delete_stream_entry_keys(key, ids)

    case delete_stream_entries(store, key, compound_keys) do
      :ok ->
        delete_stream_index_ids(key, ids)
        {:ok, length(compound_keys)}

      {:error, reason} ->
        {:error, reason, 0}
    end
  end

  defp update_meta_after_trim(key, [], store) do
    # Preserve metadata with length=0 instead of deleting, so that
    # the stream's last_id is kept for future XADD ordering.
    case :ets.lookup(@meta_table, key) do
      [{^key, _len, _first, last, ms, seq}] ->
        put_stream_meta(key, 0, "0-0", last, ms, seq, store)

      [] ->
        case durable_stream_meta_entry(key, store) do
          {_, _, last, ms, seq} -> put_stream_meta(key, 0, "0-0", last, ms, seq, store)
          nil -> :ok
        end
    end
  end

  defp update_meta_after_trim(key, remaining_ids, store) do
    first_str = List.first(remaining_ids)
    last_str = List.last(remaining_ids)
    {last_ms, last_seq} = parse_id!(last_str)
    put_stream_meta(key, length(remaining_ids), first_str, last_str, last_ms, last_seq, store)
  end

  # ---------------------------------------------------------------------------
  # Private: XDEL
  # ---------------------------------------------------------------------------

  defp do_xdel(key, ids, store) do
    ensure_meta_table()

    unique_ids = Enum.uniq(ids)
    compound_keys = delete_stream_entry_keys(key, unique_ids)
    raw_values = batch_get_stream_entries(store, key, compound_keys)

    existing_ids = existing_stream_ids(unique_ids, raw_values, [])

    delete_result =
      case delete_stream_ids(key, existing_ids, store) do
        {:ok, deleted} -> deleted
        {:error, reason, _deleted_count} -> {:error, reason}
      end

    case delete_result do
      {:error, _} = error ->
        error

      deleted ->
        if deleted > 0 do
          update_meta_after_xdel(key, deleted, store)
        end

        deleted
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XINFO STREAM
  # ---------------------------------------------------------------------------

  defp do_xinfo_stream(key, store) do
    ensure_meta_table()

    with :ok <- ensure_stream_read_type(key, store) do
      case stream_meta_entries(key, store) do
        [] ->
          {:error, "ERR no such key"}

        [{^key, len, first, last, _ms, _seq}] ->
          prefix = "X:#{key}" <> @sep

          {first_entry, last_entry} =
            if len > 0 do
              last_key = prefix <> last

              {first_raw, last_raw} =
                if first != "0-0" do
                  [first_raw, last_raw] =
                    batch_get_stream_entries(store, key, [prefix <> first, last_key])

                  {first_raw, last_raw}
                else
                  [last_raw] = batch_get_stream_entries(store, key, [last_key])
                  {nil, last_raw}
                end

              {
                decode_stream_entry(first, first_raw),
                decode_stream_entry(last, last_raw)
              }
            else
              {nil, nil}
            end

          # Count consumer groups.
          groups = count_groups(key, store)

          %{
            "length" => len,
            "first-entry" => first_entry,
            "last-entry" => last_entry,
            "last-generated-id" => last,
            "groups" => groups
          }
      end
    end
  end

  defp decode_stream_entry(_id, nil), do: nil

  defp decode_stream_entry(id, raw) do
    case decode_stream_fields(raw) do
      {:ok, fields} -> [id | fields]
      :error -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XGROUP CREATE
  # ---------------------------------------------------------------------------

  defp do_xgroup_create(key, group, id_str, mkstream, store) do
    ensure_meta_table()

    with_group_lock(key, group, fn ->
      do_xgroup_create_locked(key, group, id_str, mkstream, store)
    end)
  end

  defp do_xgroup_create_locked(key, group, id_str, mkstream, store) do
    stream_exists? = stream_exists_for_group_create?(key, store)
    group_exists? = lookup_group(store, key, group) != :missing

    cond do
      not stream_exists? and not mkstream ->
        {:error,
         "ERR The XGROUP subcommand requires the key to exist. " <>
           "Note that for CREATE you may want to use the MKSTREAM option to create " <>
           "an empty stream automatically."}

      not stream_exists? and mkstream ->
        # Create an empty stream. The type marker is the durable existence
        # marker; local ETS metadata is only an accelerator.
        case stream_type_status(key, store) do
          type_status when type_status in [:ok, {:ok, :created}, :no_marker] ->
            case create_group(key, group, id_str, store) do
              :ok ->
                put_stream_meta(key, 0, "0-0", "0-0", 0, 0, store)
                :ok

              {:error, _} = error ->
                rollback_new_stream_type_marker(key, store, type_status, error)
            end

          {:error, _} = error ->
            error
        end

      group_exists? ->
        {:error, "BUSYGROUP Consumer Group name already exists"}

      true ->
        create_group(key, group, id_str, store)
    end
  end

  defp stream_exists_for_group_create?(key, store) do
    stream_meta_entries(key, store) != [] or stream_type_marker?(key, store)
  end

  defp create_group(key, group, id_str, store) do
    # last_delivered_id: the ID from which new messages will be delivered.
    # "0" means deliver all messages from the beginning.
    # "$" means deliver only new messages from now on.
    last_delivered =
      case id_str do
        "$" ->
          case :ets.lookup(@meta_table, key) do
            [{^key, _len, _first, last, _ms, _seq}] -> last
            [] -> "0-0"
          end

        other ->
          other
      end

    persist_group_state(store, key, group, last_delivered, %{}, %{})
  end

  defp lookup_group(store, key, group) do
    ensure_meta_table()

    case :ets.lookup(@groups_table, {key, group}) do
      [{{^key, ^group}, last_delivered, consumers, pending}] ->
        {:ok, last_delivered, consumers, pending}

      [] ->
        load_persisted_group_state(store, key, group)
    end
  end

  defp load_persisted_group_state(store, key, group) do
    if Ops.has_compound?(store) do
      case Ops.compound_get(store, key, stream_group_key(key, group)) do
        nil ->
          :missing

        raw ->
          case decode_group_state(raw) do
            {:ok, last_delivered, consumers, pending} ->
              :ets.insert(@groups_table, {{key, group}, last_delivered, consumers, pending})
              {:ok, last_delivered, consumers, pending}

            :error ->
              :missing
          end
      end
    else
      :missing
    end
  end

  defp persist_group_state(store, key, group, last_delivered, consumers, pending) do
    if Ops.has_compound?(store) do
      encoded = encode_group_state(last_delivered, consumers, pending)

      case Ops.compound_put(store, key, stream_group_key(key, group), encoded, 0) do
        :ok ->
          :ets.insert(@groups_table, {{key, group}, last_delivered, consumers, pending})
          :ok

        {:error, _reason} = error ->
          error
      end
    else
      :ets.insert(@groups_table, {{key, group}, last_delivered, consumers, pending})
      :ok
    end
  end

  defp encode_group_state(last_delivered, consumers, pending) do
    :erlang.term_to_binary({:stream_group, 1, last_delivered, consumers, pending})
  end

  defp decode_group_state(raw) when is_binary(raw) do
    case :erlang.binary_to_term(raw, [:safe]) do
      {:stream_group, 1, last_delivered, consumers, pending}
      when is_binary(last_delivered) and is_map(consumers) and is_map(pending) ->
        {:ok, last_delivered, consumers, pending}

      _other ->
        :error
    end
  rescue
    _ -> :error
  end

  defp decode_group_state(_raw), do: :error

  # ---------------------------------------------------------------------------
  # Private: XREADGROUP
  # ---------------------------------------------------------------------------

  defp do_xreadgroup(group, consumer, stream_ids, count, store) do
    ensure_meta_table()

    case xreadgroup_results(group, consumer, stream_ids, count, store, []) do
      {:error, _} = err -> err
      results -> Enum.reverse(results)
    end
  end

  defp xreadgroup_results(_group, _consumer, [], _count, _store, acc), do: acc

  defp xreadgroup_results(group, consumer, [{key, id_str} | rest], count, store, acc) do
    case xreadgroup_stream_result(group, consumer, key, id_str, count, store) do
      {:error, _} = err -> err
      nil -> xreadgroup_results(group, consumer, rest, count, store, acc)
      result -> xreadgroup_results(group, consumer, rest, count, store, [result | acc])
    end
  end

  defp xreadgroup_stream_result(group, consumer, key, id_str, count, store) do
    with_group_lock(key, group, fn ->
      case lookup_group(store, key, group) do
        :missing ->
          {:error, "NOGROUP No such consumer group '#{group}' for key name '#{key}'"}

        {:ok, last_delivered, consumers, pending} ->
          xreadgroup_known_group_result(
            group,
            consumer,
            key,
            id_str,
            count,
            store,
            last_delivered,
            consumers,
            pending
          )
      end
    end)
  end

  defp xreadgroup_known_group_result(
         group,
         consumer,
         key,
         ">",
         count,
         store,
         last_delivered,
         consumers,
         pending
       ) do
    # Deliver new messages after last_delivered_id.
    case parse_exclusive_start(last_delivered) do
      {:ok, excl_start} ->
        case do_xrange(key, excl_start, :max, count, store) do
          [] ->
            nil

          entries ->
            # Update last_delivered_id and pending entries.
            last_entry = List.last(entries)
            new_last_delivered = hd(last_entry)
            now_ms = CommandTime.now_ms()

            new_pending =
              Enum.reduce(entries, pending, fn [id | _], acc ->
                Map.put(acc, id, {consumer, now_ms})
              end)

            new_consumers = Map.put(consumers, consumer, now_ms)

            case persist_group_state(
                   store,
                   key,
                   group,
                   new_last_delivered,
                   new_consumers,
                   new_pending
                 ) do
              :ok -> [key, entries]
              {:error, _reason} = error -> error
            end
        end

      {:error, _} = err ->
        err
    end
  end

  defp xreadgroup_known_group_result(
         _group,
         consumer,
         key,
         id_str,
         count,
         store,
         _last_delivered,
         _consumers,
         pending
       ) do
    # Return pending entries for this consumer with id >= id_str.
    pending_start =
      if id_str == "0" or id_str == "0-0" do
        :min
      else
        parse_id!(id_str)
      end

    pending_ids = xreadgroup_pending_ids(pending, consumer, pending_start, count)

    pending_compound_keys = Enum.map(pending_ids, &stream_entry_key(key, &1))

    pending_entries =
      store
      |> batch_get_stream_entries(key, pending_compound_keys)
      |> Enum.zip(pending_ids)
      |> Enum.reduce([], fn {raw, id_str_inner}, acc ->
        case decode_stream_fields(raw) do
          {:ok, fields} -> [[id_str_inner | fields] | acc]
          :error -> acc
        end
      end)
      |> Enum.reverse()

    if pending_entries == [] do
      nil
    else
      [key, pending_entries]
    end
  end

  defp xreadgroup_pending_ids(pending, consumer, pending_start, count) do
    pending
    |> Enum.reduce([], fn
      {id, {^consumer, _ts}}, acc ->
        parsed_id = parse_id!(id)

        if pending_start == :min or id_cmp(parsed_id, pending_start) != :lt do
          [{parsed_id, id} | acc]
        else
          acc
        end

      _other, acc ->
        acc
    end)
    |> Enum.sort_by(fn {parsed_id, _id} -> parsed_id end)
    |> maybe_take_tuples(count)
    |> Enum.map(fn {_parsed_id, id} -> id end)
  end

  # ---------------------------------------------------------------------------
  # Private: XACK
  # ---------------------------------------------------------------------------

  defp do_xack(key, group, ids, store) do
    ensure_meta_table()

    with_group_lock(key, group, fn ->
      do_xack_locked(key, group, ids, store)
    end)
  end

  defp do_xack_locked(key, group, ids, store) do
    case lookup_group(store, key, group) do
      :missing ->
        0

      {:ok, last_delivered, consumers, pending} ->
        {new_pending, acked} =
          Enum.reduce(ids, {pending, 0}, fn id, {pend, count} ->
            if Map.has_key?(pend, id) do
              {Map.delete(pend, id), count + 1}
            else
              {pend, count}
            end
          end)

        case persist_group_state(store, key, group, last_delivered, consumers, new_pending) do
          :ok -> acked
          {:error, _reason} = error -> error
        end
    end
  end

  defp with_group_lock(key, group, fun) when is_function(fun, 0) do
    lock = {key, group}
    acquire_group_lock(lock)

    try do
      fun.()
    after
      release_group_lock(lock)
    end
  end

  defp acquire_group_lock(lock) do
    ensure_group_lock_table()

    case :ets.insert_new(@group_locks_table, {lock, self()}) do
      true ->
        :ok

      false ->
        wait_for_group_lock(lock)
    end
  end

  defp wait_for_group_lock(lock) do
    case :ets.lookup(@group_locks_table, lock) do
      [{^lock, holder}] when is_pid(holder) ->
        if Process.alive?(holder) do
          receive do
          after
            1 -> :ok
          end
        else
          :ets.select_delete(@group_locks_table, [{{lock, holder}, [], [true]}])
        end

      _other ->
        :ok
    end

    acquire_group_lock(lock)
  end

  defp release_group_lock(lock) do
    ensure_group_lock_table()
    :ets.select_delete(@group_locks_table, [{{lock, self()}, [], [true]}])
    :ok
  end

  defp ensure_group_lock_table do
    case :ets.whereis(@group_locks_table) do
      :undefined ->
        try do
          :ets.new(@group_locks_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Private: ID generation and parsing
  # ---------------------------------------------------------------------------

  defp resolve_id(:auto, last_ms, last_seq) do
    # CommandTime uses HLC outside Raft and stamped log-entry time inside Raft,
    # keeping stream ID generation deterministic during state-machine replay.
    now = CommandTime.now_ms()

    cond do
      now > last_ms -> {:ok, {now, 0}}
      now == last_ms -> {:ok, {now, last_seq + 1}}
      # HLC physical behind last_ms — keep last_ms with incremented seq.
      true -> {:ok, {last_ms, last_seq + 1}}
    end
  end

  defp resolve_id({:explicit, ms, seq}, last_ms, last_seq) do
    case id_cmp({ms, seq}, {last_ms, last_seq}) do
      :gt ->
        {:ok, {ms, seq}}

      _ ->
        {:error,
         "ERR The ID specified in XADD is equal or smaller than the " <>
           "target stream top item"}
    end
  end

  defp resolve_id({:partial, ms}, last_ms, last_seq) do
    # Partial ID: only ms given, seq auto-assigned.
    cond do
      ms > last_ms ->
        {:ok, {ms, 0}}

      ms == last_ms ->
        {:ok, {ms, last_seq + 1}}

      true ->
        {:error,
         "ERR The ID specified in XADD is equal or smaller than the " <>
           "target stream top item"}
    end
  end

  @doc false
  @spec parse_id!(binary()) :: stream_id()
  def parse_id!(id_str) do
    case String.split(id_str, "-", parts: 2) do
      [ms_str, seq_str] ->
        {String.to_integer(ms_str), String.to_integer(seq_str)}

      [ms_str] ->
        {String.to_integer(ms_str), 0}
    end
  end

  defp parse_full_id(id_str) do
    case String.split(id_str, "-", parts: 2) do
      [ms_str, seq_str] ->
        case {Integer.parse(ms_str), Integer.parse(seq_str)} do
          {{ms, ""}, {seq, ""}} -> {:ok, {ms, seq}}
          _ -> {:error, "ERR Invalid stream ID specified as stream command argument"}
        end

      [ms_str] ->
        case Integer.parse(ms_str) do
          {ms, ""} -> {:ok, {ms, 0}}
          _ -> {:error, "ERR Invalid stream ID specified as stream command argument"}
        end
    end
  end

  defp parse_range_id("-", :min), do: {:ok, :min}
  defp parse_range_id("+", :max), do: {:ok, :max}

  defp parse_range_id(id_str, _default) do
    parse_full_id(id_str)
  end

  defp normalize_ast_range_id(:min, _default), do: {:ok, :min}
  defp normalize_ast_range_id(:max, _default), do: {:ok, :max}
  defp normalize_ast_range_id({_ms, _seq} = id, _default), do: {:ok, id}

  defp normalize_ast_range_id(id_str, default) when is_binary(id_str),
    do: parse_range_id(id_str, default)

  defp normalize_ast_count(nil), do: :infinity
  defp normalize_ast_count(count), do: count

  defp parse_exclusive_start("0"), do: {:ok, :min}
  defp parse_exclusive_start("0-0"), do: {:ok, :min}

  defp parse_exclusive_start(id_str) do
    case parse_full_id(id_str) do
      {:ok, {ms, seq}} -> {:ok, {ms, seq + 1}}
      err -> err
    end
  end

  defp id_in_range?(_id, :min, :max), do: true
  defp id_in_range?(id, :min, max), do: id_cmp(id, max) != :gt
  defp id_in_range?(id, min, :max), do: id_cmp(id, min) != :lt
  defp id_in_range?(id, min, max), do: id_cmp(id, min) != :lt and id_cmp(id, max) != :gt

  defp id_cmp({ms1, seq1}, {ms2, seq2}) do
    cond do
      ms1 < ms2 -> :lt
      ms1 > ms2 -> :gt
      seq1 < seq2 -> :lt
      seq1 > seq2 -> :gt
      true -> :eq
    end
  end

  # ---------------------------------------------------------------------------
  # Private: compound key helpers
  # ---------------------------------------------------------------------------

  defp stream_entry_key(stream_key, id_str) do
    "X:#{stream_key}" <> @sep <> id_str
  end

  defp delete_stream_entry_keys(stream_key, ids) do
    prefix = stream_entry_prefix(stream_key)
    Enum.map(ids, &(prefix <> &1))
  end

  defp existing_stream_ids([_id | ids], [nil | raws], acc) do
    existing_stream_ids(ids, raws, acc)
  end

  defp existing_stream_ids([id | ids], [_raw | raws], acc) do
    existing_stream_ids(ids, raws, [id | acc])
  end

  defp existing_stream_ids(_ids, _raws, acc), do: Enum.reverse(acc)

  defp stream_group_key(stream_key, group) do
    CompoundKey.stream_group(stream_key, group)
  end

  defp put_stream_entry(store, stream_key, compound_key, encoded) do
    if Ops.has_compound?(store) do
      Ops.compound_put(store, stream_key, compound_key, encoded, 0)
    else
      Ops.put(store, compound_key, encoded, 0)
    end
  end

  defp batch_get_stream_entries(store, stream_key, compound_keys) do
    if Ops.has_compound?(store) do
      Ops.compound_batch_get(store, stream_key, compound_keys)
    else
      Ops.batch_get(store, compound_keys)
    end
  end

  defp delete_stream_entries(_store, _stream_key, []), do: :ok

  defp delete_stream_entries(store, stream_key, compound_keys) do
    if Ops.has_compound?(store) do
      Ops.compound_batch_delete(store, stream_key, compound_keys)
    else
      Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
        case Ops.delete(store, compound_key) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  defp stream_entries_for(store, stream_key) do
    Ops.compound_scan(store, stream_key, stream_entry_prefix(stream_key))
  end

  defp stream_ids_for(store, stream_key) do
    stream_fields_for(store, stream_key)
  end

  defp stream_fields_for(store, stream_key) do
    Ops.compound_fields(store, stream_key, stream_entry_prefix(stream_key))
  end

  # Count compound entries with prefix `X:<stream_key>\0` — used by XLEN as
  # the fallback path on nodes where the local `@meta_table` doesn't have
  # the stream registered (notably Raft followers).
  defp count_stream_entries(store, stream_key) do
    Ops.compound_count(store, stream_key, stream_entry_prefix(stream_key))
  end

  defp stream_entry_prefix(stream_key) do
    CompoundKey.stream_prefix(stream_key)
  end

  defp maybe_index_stream_put(store, stream_key, id_str, compound_key, meta_entries) do
    if Ops.has_compound?(store) do
      insert_stream_index_entry(stream_key, id_str, compound_key)

      if stream_index_ready?(stream_key) or meta_entries == [] do
        mark_stream_index_ready(stream_key)
      end
    end

    :ok
  end

  defp ensure_stream_index(stream_key, store) do
    ensure_meta_table()

    unless stream_index_ready?(stream_key) do
      rebuild_stream_index(stream_key, store)
    end

    :ok
  end

  defp rebuild_stream_index(stream_key, store) do
    clear_stream_index(stream_key)

    store
    |> stream_fields_for(stream_key)
    |> Enum.each(fn id_str ->
      insert_stream_index_entry(stream_key, id_str, stream_entry_key(stream_key, id_str))
    end)

    mark_stream_index_ready(stream_key)
  end

  defp stream_index_ready?(stream_key) do
    :ets.lookup(@index_table, {:ready, stream_key}) != []
  end

  defp mark_stream_index_ready(stream_key) do
    :ets.insert(@index_table, {{:ready, stream_key}, true})
  end

  defp clear_stream_index(stream_key) do
    :ets.select_delete(@index_table, [{{{stream_key, :_, :_}, :_, :_}, [], [true]}])
    :ets.delete(@index_table, {:ready, stream_key})
  end

  defp cleanup_local_stream_metadata(stream_key) do
    :ets.delete(@meta_table, stream_key)
    :ets.match_delete(@groups_table, {{stream_key, :_}, :_, :_, :_})
    clear_stream_index(stream_key)
  end

  defp insert_stream_index_entry(stream_key, id_str, compound_key) do
    {ms, seq} = parse_id!(id_str)
    :ets.insert(@index_table, {{stream_key, ms, seq}, id_str, compound_key})
  end

  defp delete_stream_index_ids(stream_key, ids) do
    Enum.each(ids, fn id_str ->
      {ms, seq} = parse_id!(id_str)
      :ets.delete(@index_table, {stream_key, ms, seq})
    end)
  end

  defp stream_index_slice(_stream_key, _range_start, _range_end, 0, _reverse?), do: []

  defp stream_index_slice(stream_key, range_start, range_end, count, false) do
    stream_key
    |> forward_stream_index_first(range_start)
    |> collect_stream_index(
      stream_key,
      range_start,
      range_end,
      count,
      &next_stream_index_key/1,
      []
    )
  end

  defp stream_index_slice(stream_key, range_start, range_end, count, true) do
    stream_key
    |> reverse_stream_index_first(range_end)
    |> collect_stream_index(
      stream_key,
      range_start,
      range_end,
      count,
      &prev_stream_index_key/1,
      []
    )
  end

  defp forward_stream_index_first(stream_key, :min) do
    :ets.next(@index_table, {stream_key, -1, -1})
  end

  defp forward_stream_index_first(stream_key, {ms, seq}) do
    :ets.next(@index_table, {stream_key, ms, seq - 1})
  end

  defp reverse_stream_index_first(stream_key, :max) do
    :ets.prev(@index_table, {stream_key, @max_int64, @max_int64})
  end

  defp reverse_stream_index_first(stream_key, {ms, seq}) do
    key = {stream_key, ms, seq}

    case :ets.lookup(@index_table, key) do
      [{^key, _id_str, _compound_key}] -> key
      [] -> :ets.prev(@index_table, key)
    end
  end

  defp next_stream_index_key(:"$end_of_table"), do: :"$end_of_table"
  defp next_stream_index_key(key), do: :ets.next(@index_table, key)

  defp prev_stream_index_key(:"$end_of_table"), do: :"$end_of_table"
  defp prev_stream_index_key(key), do: :ets.prev(@index_table, key)

  defp collect_stream_index(:"$end_of_table", _stream_key, _start, _end, _count, _next, acc),
    do: Enum.reverse(acc)

  defp collect_stream_index(
         {stream_key, ms, seq} = key,
         stream_key,
         range_start,
         range_end,
         count,
         next,
         acc
       ) do
    id = {ms, seq}

    cond do
      count == 0 ->
        Enum.reverse(acc)

      not id_in_range?(id, range_start, range_end) ->
        Enum.reverse(acc)

      true ->
        case :ets.lookup(@index_table, key) do
          [{^key, id_str, compound_key}] ->
            collect_stream_index(
              next.(key),
              stream_key,
              range_start,
              range_end,
              decrement_stream_index_count(count),
              next,
              [{id_str, compound_key} | acc]
            )

          [] ->
            collect_stream_index(next.(key), stream_key, range_start, range_end, count, next, acc)
        end
    end
  end

  defp collect_stream_index(_other_key, _stream_key, _start, _end, _count, _next, acc),
    do: Enum.reverse(acc)

  defp decrement_stream_index_count(:infinity), do: :infinity
  defp decrement_stream_index_count(count), do: count - 1

  defp stream_index_ids(stream_key, count) do
    stream_key
    |> stream_index_slice(:min, :max, count, false)
    |> Enum.map(fn {id_str, _compound_key} -> id_str end)
  end

  defp stream_index_first_last(stream_key) do
    first_key = forward_stream_index_first(stream_key, :min)
    last_key = reverse_stream_index_first(stream_key, :max)

    with {^stream_key, _first_ms, _first_seq} <- first_key,
         {^stream_key, _last_ms, _last_seq} <- last_key,
         [{^first_key, first_id, _first_compound_key}] <- :ets.lookup(@index_table, first_key),
         [{^last_key, last_id, _last_compound_key}] <- :ets.lookup(@index_table, last_key) do
      {first_id, last_id}
    else
      _ -> nil
    end
  end

  defp update_meta_after_index_mutation(key, remaining_len, old_last, old_ms, old_seq, store)
       when remaining_len <= 0 do
    if Ops.has_compound?(store) do
      put_stream_meta(key, 0, "0-0", old_last, old_ms, old_seq, store)
    else
      update_meta_after_trim(key, [], store)
    end
  end

  defp update_meta_after_index_mutation(key, remaining_len, _old_last, _old_ms, _old_seq, store) do
    if Ops.has_compound?(store) do
      case stream_index_first_last(key) do
        {first_str, last_str} ->
          {last_ms, last_seq} = parse_id!(last_str)
          put_stream_meta(key, remaining_len, first_str, last_str, last_ms, last_seq, store)

        nil ->
          remaining_ids =
            store
            |> stream_ids_for(key)
            |> Enum.sort_by(&parse_id!/1)

          update_meta_after_trim(key, remaining_ids, store)
      end
    else
      remaining_ids =
        store
        |> stream_ids_for(key)
        |> Enum.sort_by(&parse_id!/1)

      update_meta_after_trim(key, remaining_ids, store)
    end
  end

  defp update_meta_after_xdel(key, deleted, store) do
    case :ets.lookup(@meta_table, key) do
      [{^key, len, _first, last, ms, seq}] ->
        update_meta_after_index_mutation(key, max(len - deleted, 0), last, ms, seq, store)

      [] ->
        remaining_ids =
          store
          |> stream_ids_for(key)
          |> Enum.sort_by(&parse_id!/1)

        update_meta_after_trim(key, remaining_ids, store)
    end
  end

  defp exclusive_upper_bound({ms, seq}), do: {ms, seq - 1}

  defp decode_indexed_stream_entries([], _stream_key, _store), do: []

  defp decode_indexed_stream_entries(index_entries, stream_key, store) do
    {compound_keys, ids} = indexed_stream_keys_and_ids(index_entries, [], [])
    raw_values = Ops.compound_batch_get(store, stream_key, compound_keys)
    decode_indexed_stream_raw(ids, raw_values, [])
  end

  defp indexed_stream_keys_and_ids([], compound_keys, ids) do
    {Enum.reverse(compound_keys), Enum.reverse(ids)}
  end

  defp indexed_stream_keys_and_ids([{id_str, compound_key} | rest], compound_keys, ids) do
    indexed_stream_keys_and_ids(rest, [compound_key | compound_keys], [id_str | ids])
  end

  defp decode_indexed_stream_raw([id_str | ids], [raw | raws], acc) when is_binary(raw) do
    case decode_stream_fields(raw) do
      {:ok, fields} -> decode_indexed_stream_raw(ids, raws, [[id_str | fields] | acc])
      :error -> decode_indexed_stream_raw(ids, raws, acc)
    end
  end

  defp decode_indexed_stream_raw([_id_str | ids], [_raw | raws], acc) do
    decode_indexed_stream_raw(ids, raws, acc)
  end

  defp decode_indexed_stream_raw(_ids, _raws, acc) do
    Enum.reverse(acc)
  end

  defp decode_stream_fields(raw) when is_binary(raw) do
    case Ferricstore.Flow.decode_history_fields(raw) do
      [_ | _] = fields -> {:ok, fields}
      _ -> decode_term_stream_fields(raw)
    end
  end

  defp decode_stream_fields(_), do: :error

  defp decode_term_stream_fields(raw) do
    case :erlang.binary_to_term(raw, [:safe]) do
      fields when is_list(fields) -> {:ok, fields}
      _other -> :error
    end
  rescue
    _ -> :error
  end

  # ---------------------------------------------------------------------------
  # Private: argument parsing
  # ---------------------------------------------------------------------------

  defp parse_xadd_args(args) do
    {key, rest} = {hd(args), tl(args)}

    {nomkstream, rest} =
      case rest do
        ["NOMKSTREAM" | r] -> {true, r}
        _ -> {false, rest}
      end

    {trim_opts, rest} =
      case rest do
        ["MAXLEN" | r] -> parse_trim_maxlen(r)
        ["MINID" | r] -> parse_trim_minid(r)
        _ -> {nil, rest}
      end

    case trim_opts do
      {:error, _} = err ->
        err

      _ ->
        case rest do
          [id_spec_str | field_values] when field_values != [] ->
            if rem(length(field_values), 2) != 0 do
              {:error, "ERR wrong number of arguments for 'xadd' command"}
            else
              case parse_id_spec(id_spec_str) do
                {:error, _} = err -> err
                id_spec -> {:ok, key, id_spec, field_values, trim_opts, nomkstream}
              end
            end

          _ ->
            {:error, "ERR wrong number of arguments for 'xadd' command"}
        end
    end
  end

  defp parse_id_spec("*"), do: :auto

  defp parse_id_spec(id_str) do
    case String.split(id_str, "-", parts: 2) do
      [ms_str, seq_str] ->
        with {:ok, ms} <- parse_id_component(ms_str),
             {:ok, seq} <- parse_id_component(seq_str) do
          {:explicit, ms, seq}
        end

      [ms_str] ->
        with {:ok, ms} <- parse_id_component(ms_str) do
          {:partial, ms}
        end
    end
  end

  defp parse_id_component(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> {:error, "ERR Invalid stream ID specified as stream command argument"}
    end
  end

  defp parse_trim_opts(rest) do
    case rest do
      ["MAXLEN" | r] ->
        case parse_trim_maxlen(r) do
          {{:error, _} = err, _} -> err
          {opts, _remaining} -> {:ok, opts}
        end

      ["MINID" | r] ->
        case parse_trim_minid(r) do
          {{:error, _} = err, _} -> err
          {opts, _remaining} -> {:ok, opts}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_trim_maxlen(rest) do
    {approx, rest} = consume_approx(rest)

    case rest do
      [threshold_str | remaining] ->
        case Integer.parse(threshold_str) do
          {n, ""} when n >= 0 -> {{:maxlen, approx, n}, remaining}
          _ -> {{:error, "ERR value is not an integer or out of range"}, rest}
        end

      [] ->
        {{:error, "ERR syntax error"}, rest}
    end
  end

  defp parse_trim_minid(rest) do
    {approx, rest} = consume_approx(rest)

    case rest do
      [id_str | remaining] -> {{:minid, approx, id_str}, remaining}
      [] -> {{:error, "ERR syntax error"}, rest}
    end
  end

  defp consume_approx(["~" | rest]), do: {true, rest}
  defp consume_approx(["=" | rest]), do: {false, rest}
  defp consume_approx(rest), do: {false, rest}

  defp parse_count_opt([]), do: {:ok, :infinity}

  defp parse_count_opt(["COUNT", n_str | _rest]) do
    case Integer.parse(n_str) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_count_opt(_), do: {:error, "ERR syntax error"}

  defp parse_xread_args(args) do
    # COUNT and BLOCK can appear in either order before STREAMS.
    {count, rest} = parse_xread_count(args)
    {block, rest} = parse_xread_block(rest)
    # Handle BLOCK before COUNT: XREAD BLOCK 100 COUNT 2 STREAMS ...
    {count, rest} =
      if count == :infinity do
        case parse_xread_count(rest) do
          {:infinity, _} -> {count, rest}
          {n, rest2} -> {n, rest2}
        end
      else
        {count, rest}
      end

    case split_at_streams(rest) do
      {:ok, keys, ids} when length(keys) == length(ids) and keys != [] ->
        stream_ids = Enum.zip(keys, ids)
        {:ok, count, block, stream_ids}

      {:ok, _, _} ->
        {:error,
         "ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified"}

      :not_found ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_xread_count(["COUNT", n_str | rest]) do
    case Integer.parse(n_str) do
      {n, ""} when n >= 0 -> {n, rest}
      _ -> {:infinity, rest}
    end
  end

  defp parse_xread_count(rest), do: {:infinity, rest}

  defp parse_xread_block(["BLOCK", timeout_str | rest]) do
    case Integer.parse(timeout_str) do
      {n, ""} when n >= 0 -> {{:block, n}, rest}
      _ -> {:no_block, ["BLOCK", timeout_str | rest]}
    end
  end

  defp parse_xread_block(rest), do: {:no_block, rest}

  defp split_at_streams(args) do
    case Enum.find_index(args, &(String.upcase(&1) == "STREAMS")) do
      nil ->
        :not_found

      idx ->
        _streams_token = Enum.at(args, idx)
        after_streams = Enum.drop(args, idx + 1)
        half = div(length(after_streams), 2)
        {keys, ids} = Enum.split(after_streams, half)
        {:ok, keys, ids}
    end
  end

  defp parse_xreadgroup_args(args) do
    case args do
      ["GROUP", group, consumer | rest] ->
        # COUNT and BLOCK can appear in either order before STREAMS.
        {count, rest2} = parse_xread_count(rest)
        {block, rest3} = parse_xread_block(rest2)
        # Handle BLOCK before COUNT
        {count, rest3} =
          if count == :infinity do
            case parse_xread_count(rest3) do
              {:infinity, _} -> {count, rest3}
              {n, rest4} -> {n, rest4}
            end
          else
            {count, rest3}
          end

        case split_at_streams(rest3) do
          {:ok, keys, ids} when length(keys) == length(ids) and keys != [] ->
            stream_ids = Enum.zip(keys, ids)
            {:ok, group, consumer, count, block, stream_ids}

          {:ok, _, _} ->
            {:error,
             "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID must be specified"}

          :not_found ->
            {:error, "ERR syntax error"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: helpers
  # ---------------------------------------------------------------------------

  defp maybe_take(entries, :infinity), do: entries
  defp maybe_take(entries, n), do: Enum.take(entries, n)

  defp maybe_take_tuples(entries, :infinity), do: entries
  defp maybe_take_tuples(entries, n), do: Enum.take(entries, n)

  defp count_groups(key, store) do
    if Ops.has_compound?(store) do
      Ops.compound_count(store, key, CompoundKey.stream_group_prefix(key))
    else
      # Count groups by scanning the local table.
      # For v1, we iterate. In production, a secondary index would be better.
      :ets.foldl(
        fn
          {{^key, _group}, _last, _consumers, _pending}, acc -> acc + 1
          _, acc -> acc
        end,
        0,
        @groups_table
      )
    end
  end
end
