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

  Consumer group state is tracked in a second ETS table
  (`Ferricstore.Stream.Groups`):

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
  alias Ferricstore.Store.Ops

  @typedoc "A parsed stream ID as `{milliseconds, sequence}`."
  @type stream_id :: {non_neg_integer(), non_neg_integer()}

  @typedoc "A stream entry: `{id_string, [field, value, ...]}` flat list."
  @type entry :: {binary(), [binary()]}

  @meta_table Ferricstore.Stream.Meta
  @groups_table Ferricstore.Stream.Groups
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

  def handle("XGROUP", ["CREATE", key, group, id_str | rest], _store) do
    mkstream = Enum.any?(rest, &(String.upcase(&1) == "MKSTREAM"))
    do_xgroup_create(key, group, id_str, mkstream)
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

  def handle("XACK", [key, group | ids], _store) when ids != [] do
    do_xack(key, group, ids)
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
    do_xrange(key, range_start, range_end, count, store)
  end

  def handle_ast({:xrevrange, key, range_start, range_end, count}, store) do
    do_xrevrange(key, range_start, range_end, count, store)
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

  def handle_ast({:xgroup_create, key, group, id_str, mkstream}, _store)
      when is_boolean(mkstream) do
    do_xgroup_create(key, group, id_str, mkstream)
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

  def handle_ast({:xack, key, group, ids}, _store) when is_list(ids) and ids != [] do
    do_xack(key, group, ids)
  end

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported stream command AST"}

  defp xlen_key(key, store) do
    # `@meta_table` is local-only (not Raft-replicated), so on a follower we
    # don't have it. Fall back to counting the stream's compound entries —
    # those go through Router.compound_put and are present on every node.
    # On the originating node we still consult the meta table for O(1) speed
    # when populated; followers and post-migration always count via prefix.
    ensure_meta_table()

    case :ets.lookup(@meta_table, key) do
      [{^key, len, _first, _last, _ms, _seq}] -> len
      [] -> count_stream_entries(store, key)
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

    # Check if stream exists when NOMKSTREAM is set
    case :ets.lookup(@meta_table, key) do
      [] when nomkstream -> nil
      meta_entries -> do_xadd_insert(key, id_spec, fields, trim_opts, meta_entries, store)
    end
  end

  defp do_xadd_insert(key, id_spec, fields, trim_opts, meta_entries, store) do
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

        with :ok <- put_stream_entry(store, key, compound_key, encoded) do
          maybe_index_stream_put(store, key, id_str, compound_key, meta_entries)

          # Update metadata.
          {new_len, new_first} =
            case meta_entries do
              [{^key, len, first, _last, _ms, _seq}] ->
                {len + 1, first}

              [] ->
                {1, id_str}
            end

          :ets.insert(@meta_table, {key, new_len, new_first, id_str, ms, seq})

          # Apply trim if requested.
          maybe_trim(key, trim_opts, store)

          # Notify any XREAD BLOCK waiters watching this stream.
          notify_stream_waiters(key)

          id_str
        end

      {:error, _} = err ->
        err
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XRANGE / XREVRANGE
  # ---------------------------------------------------------------------------

  defp do_xrange(key, range_start, range_end, count, store) do
    ensure_meta_table()

    if Ops.has_compound?(store) do
      indexed_stream_range(key, range_start, range_end, count, false, store)
    else
      scanned_stream_range(key, range_start, range_end, count, store)
    end
  end

  defp do_xrevrange(key, range_start, range_end, count, store) do
    ensure_meta_table()

    if Ops.has_compound?(store) do
      indexed_stream_range(key, range_start, range_end, count, true, store)
    else
      key
      |> scanned_stream_range(range_start, range_end, :infinity, store)
      |> Enum.reverse()
      |> maybe_take(count)
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

    results =
      Enum.map(stream_ids, fn {key, id_str} ->
        # Handle "$" -- resolve to current last ID of the stream.
        resolved_id =
          if id_str == "$" do
            case :ets.lookup(@meta_table, key) do
              [{^key, _len, _first, last, _ms, _seq}] -> last
              [] -> "0-0"
            end
          else
            id_str
          end

        # For XREAD, the start is exclusive (entries > id).
        start_id = parse_exclusive_start(resolved_id)

        case start_id do
          {:ok, excl_start} ->
            entries = do_xrange(key, excl_start, :max, count, store)

            if entries == [] do
              nil
            else
              [key, entries]
            end

          {:error, _} = err ->
            err
        end
      end)

    # Check for errors first.
    case Enum.find(results, &match?({:error, _}, &1)) do
      {:error, _} = err -> err
      nil -> Enum.reject(results, &is_nil/1)
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
    prefix = "X:#{key}" <> @sep

    all_ids =
      store
      |> stream_ids_for(key)
      |> Enum.sort_by(&parse_id!/1)

    current_len = length(all_ids)

    if current_len > max_len do
      to_remove = Enum.take(all_ids, current_len - max_len)

      Enum.each(to_remove, fn id_str ->
        delete_stream_entry(store, key, prefix <> id_str)
      end)

      deleted_count = length(to_remove)

      # Update metadata.
      remaining = Enum.drop(all_ids, deleted_count)
      update_meta_after_trim(key, remaining)

      deleted_count
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

  defp do_apply_trim_minid_scanned(key, prefix, min_id, store) do
    all_ids =
      store
      |> stream_ids_for(key)
      |> Enum.sort_by(&parse_id!/1)

    {to_remove, _keep} =
      Enum.split_with(all_ids, fn id_str ->
        id_cmp(parse_id!(id_str), min_id) == :lt
      end)

    Enum.each(to_remove, fn id_str ->
      delete_stream_entry(store, key, prefix <> id_str)
    end)

    deleted_count = length(to_remove)

    if deleted_count > 0 do
      remaining = all_ids -- to_remove
      update_meta_after_trim(key, remaining)
    end

    deleted_count
  end

  defp apply_trim_maxlen_indexed(key, max_len, store) do
    ensure_stream_index(key, store)

    case :ets.lookup(@meta_table, key) do
      [{^key, len, _first, last, ms, seq}] when len > max_len ->
        delete_count = len - max_len

        key
        |> stream_index_ids(delete_count)
        |> Enum.each(fn id_str ->
          delete_stream_entry(store, key, stream_entry_key(key, id_str))
        end)

        update_meta_after_index_mutation(key, max_len, last, ms, seq, store)
        delete_count

      _ ->
        0
    end
  end

  defp do_apply_trim_minid_indexed(key, min_id, store) do
    ensure_stream_index(key, store)

    case :ets.lookup(@meta_table, key) do
      [{^key, len, _first, last, ms, seq}] ->
        to_remove =
          key
          |> stream_index_slice(:min, exclusive_upper_bound(min_id), :infinity, false)
          |> Enum.map(fn {id_str, _compound_key} -> id_str end)

        Enum.each(to_remove, fn id_str ->
          delete_stream_entry(store, key, stream_entry_key(key, id_str))
        end)

        deleted_count = length(to_remove)

        if deleted_count > 0 do
          update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)
        end

        deleted_count

      [] ->
        0
    end
  end

  defp update_meta_after_trim(key, []) do
    # Preserve metadata with length=0 instead of deleting, so that
    # the stream's last_id is kept for future XADD ordering.
    case :ets.lookup(@meta_table, key) do
      [{^key, _len, _first, last, ms, seq}] ->
        :ets.insert(@meta_table, {key, 0, "0-0", last, ms, seq})

      [] ->
        :ok
    end
  end

  defp update_meta_after_trim(key, remaining_ids) do
    first_str = List.first(remaining_ids)
    last_str = List.last(remaining_ids)
    {last_ms, last_seq} = parse_id!(last_str)
    :ets.insert(@meta_table, {key, length(remaining_ids), first_str, last_str, last_ms, last_seq})
  end

  # ---------------------------------------------------------------------------
  # Private: XDEL
  # ---------------------------------------------------------------------------

  defp do_xdel(key, ids, store) do
    ensure_meta_table()

    prefix = "X:#{key}" <> @sep

    delete_result =
      Enum.reduce_while(ids, 0, fn id_str, acc ->
        compound_key = prefix <> id_str

        if stream_entry_exists?(store, key, compound_key) do
          case delete_stream_entry(store, key, compound_key) do
            :ok -> {:cont, acc + 1}
            {:error, _} = error -> {:halt, error}
          end
        else
          {:cont, acc}
        end
      end)

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

  defp stream_entry_exists?(store, stream_key, compound_key) do
    if Ops.has_compound?(store) do
      Ops.compound_get(store, stream_key, compound_key) != nil
    else
      Ops.exists?(store, compound_key)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XINFO STREAM
  # ---------------------------------------------------------------------------

  defp do_xinfo_stream(key, store) do
    ensure_meta_table()

    case :ets.lookup(@meta_table, key) do
      [] ->
        {:error, "ERR no such key"}

      [{^key, len, first, last, _ms, _seq}] ->
        prefix = "X:#{key}" <> @sep

        {first_entry, last_entry} =
          if len > 0 do
            last_key = prefix <> last

            {first_raw, last_raw} =
              if first != "0-0" do
                [first_raw, last_raw] = Ops.batch_get(store, [prefix <> first, last_key])
                {first_raw, last_raw}
              else
                [last_raw] = Ops.batch_get(store, [last_key])
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
        groups = count_groups(key)

        %{
          "length" => len,
          "first-entry" => first_entry,
          "last-entry" => last_entry,
          "last-generated-id" => last,
          "groups" => groups
        }
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

  defp do_xgroup_create(key, group, id_str, mkstream) do
    ensure_meta_table()

    stream_exists? = :ets.lookup(@meta_table, key) != []
    group_exists? = :ets.lookup(@groups_table, {key, group}) != []

    cond do
      not stream_exists? and not mkstream ->
        {:error,
         "ERR The XGROUP subcommand requires the key to exist. " <>
           "Note that for CREATE you may want to use the MKSTREAM option to create " <>
           "an empty stream automatically."}

      not stream_exists? and mkstream ->
        # Create an empty stream.
        :ets.insert(@meta_table, {key, 0, "0-0", "0-0", 0, 0})
        create_group(key, group, id_str)
        :ok

      group_exists? ->
        {:error, "BUSYGROUP Consumer Group name already exists"}

      true ->
        create_group(key, group, id_str)
        :ok
    end
  end

  defp create_group(key, group, id_str) do
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

    :ets.insert(@groups_table, {{key, group}, last_delivered, %{}, %{}})
  end

  # ---------------------------------------------------------------------------
  # Private: XREADGROUP
  # ---------------------------------------------------------------------------

  defp do_xreadgroup(group, consumer, stream_ids, count, store) do
    ensure_meta_table()

    results =
      Enum.map(stream_ids, fn {key, id_str} ->
        case :ets.lookup(@groups_table, {key, group}) do
          [] ->
            {:error, "NOGROUP No such consumer group '#{group}' for key name '#{key}'"}

          [{{^key, ^group}, last_delivered, consumers, pending}] ->
            case id_str do
              ">" ->
                # Deliver new messages after last_delivered_id.
                start_id = parse_exclusive_start(last_delivered)

                case start_id do
                  {:ok, excl_start} ->
                    entries = do_xrange(key, excl_start, :max, count, store)

                    if entries == [] do
                      nil
                    else
                      # Update last_delivered_id and pending entries.
                      last_entry = List.last(entries)
                      new_last_delivered = hd(last_entry)

                      new_pending =
                        Enum.reduce(entries, pending, fn [id | _], acc ->
                          Map.put(acc, id, {consumer, CommandTime.now_ms()})
                        end)

                      new_consumers = Map.put(consumers, consumer, CommandTime.now_ms())

                      :ets.insert(
                        @groups_table,
                        {{key, group}, new_last_delivered, new_consumers, new_pending}
                      )

                      [key, entries]
                    end

                  {:error, _} = err ->
                    err
                end

              _ ->
                # Return pending entries for this consumer with id >= id_str.
                pending_start =
                  if id_str == "0" or id_str == "0-0" do
                    :min
                  else
                    parse_id!(id_str)
                  end

                pending_entries =
                  pending
                  |> Enum.filter(fn {_id, {owner, _ts}} -> owner == consumer end)
                  |> Enum.filter(fn {id, _} ->
                    case pending_start do
                      :min -> true
                      start -> id_cmp(parse_id!(id), start) != :lt
                    end
                  end)
                  |> Enum.sort_by(fn {id, _} -> parse_id!(id) end)
                  |> maybe_take_tuples(count)
                  |> Enum.map(fn {id_str_inner, _} ->
                    prefix = "X:#{key}" <> @sep
                    raw = Ops.get(store, prefix <> id_str_inner)

                    case decode_stream_fields(raw) do
                      {:ok, fields} -> [id_str_inner | fields]
                      :error -> nil
                    end
                  end)
                  |> Enum.reject(&is_nil/1)

                if pending_entries == [] do
                  nil
                else
                  [key, pending_entries]
                end
            end
        end
      end)

    # Check for errors.
    case Enum.find(results, &match?({:error, _}, &1)) do
      {:error, _} = err -> err
      nil -> Enum.reject(results, &is_nil/1)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XACK
  # ---------------------------------------------------------------------------

  defp do_xack(key, group, ids) do
    ensure_meta_table()

    case :ets.lookup(@groups_table, {key, group}) do
      [] ->
        0

      [{{^key, ^group}, last_delivered, consumers, pending}] ->
        {new_pending, acked} =
          Enum.reduce(ids, {pending, 0}, fn id, {pend, count} ->
            if Map.has_key?(pend, id) do
              {Map.delete(pend, id), count + 1}
            else
              {pend, count}
            end
          end)

        :ets.insert(@groups_table, {{key, group}, last_delivered, consumers, new_pending})
        acked
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

  defp put_stream_entry(store, stream_key, compound_key, encoded) do
    if Ops.has_compound?(store) do
      Ops.compound_put(store, stream_key, compound_key, encoded, 0)
    else
      Ops.put(store, compound_key, encoded, 0)
    end
  end

  defp delete_stream_entry(store, stream_key, compound_key) do
    if Ops.has_compound?(store) do
      with :ok <- Ops.compound_delete(store, stream_key, compound_key) do
        delete_stream_index_entry(stream_key, compound_key)
        :ok
      end
    else
      Ops.delete(store, compound_key)
    end
  end

  defp stream_entries_for(store, stream_key) do
    Ops.compound_scan(store, stream_key, stream_entry_prefix(stream_key))
  end

  defp stream_ids_for(store, stream_key) do
    store
    |> stream_entries_for(stream_key)
    |> Enum.map(fn {id_str, _raw} -> id_str end)
  end

  # Count compound entries with prefix `X:<stream_key>\0` — used by XLEN as
  # the fallback path on nodes where the local `@meta_table` doesn't have
  # the stream registered (notably Raft followers).
  defp count_stream_entries(store, stream_key) do
    Ops.compound_count(store, stream_key, stream_entry_prefix(stream_key))
  end

  defp stream_entry_prefix(stream_key) do
    "X:#{stream_key}" <> @sep
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
    |> stream_entries_for(stream_key)
    |> Enum.each(fn {id_str, _raw} ->
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

  defp insert_stream_index_entry(stream_key, id_str, compound_key) do
    {ms, seq} = parse_id!(id_str)
    :ets.insert(@index_table, {{stream_key, ms, seq}, id_str, compound_key})
  end

  defp delete_stream_index_entry(stream_key, compound_key) do
    prefix = stream_entry_prefix(stream_key)

    if String.starts_with?(compound_key, prefix) do
      id_str = String.replace_prefix(compound_key, prefix, "")
      {ms, seq} = parse_id!(id_str)
      :ets.delete(@index_table, {stream_key, ms, seq})
    end
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
      :ets.insert(@meta_table, {key, 0, "0-0", old_last, old_ms, old_seq})
    else
      update_meta_after_trim(key, [])
    end
  end

  defp update_meta_after_index_mutation(key, remaining_len, _old_last, _old_ms, _old_seq, store) do
    if Ops.has_compound?(store) do
      case stream_index_first_last(key) do
        {first_str, last_str} ->
          {last_ms, last_seq} = parse_id!(last_str)
          :ets.insert(@meta_table, {key, remaining_len, first_str, last_str, last_ms, last_seq})

        nil ->
          remaining_ids =
            store
            |> stream_ids_for(key)
            |> Enum.sort_by(&parse_id!/1)

          update_meta_after_trim(key, remaining_ids)
      end
    else
      remaining_ids =
        store
        |> stream_ids_for(key)
        |> Enum.sort_by(&parse_id!/1)

      update_meta_after_trim(key, remaining_ids)
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

        update_meta_after_trim(key, remaining_ids)
    end
  end

  defp exclusive_upper_bound({ms, seq}), do: {ms, seq - 1}

  defp decode_indexed_stream_entries([], _stream_key, _store), do: []

  defp decode_indexed_stream_entries(index_entries, stream_key, store) do
    compound_keys = Enum.map(index_entries, fn {_id_str, compound_key} -> compound_key end)
    raw_values = Ops.compound_batch_get(store, stream_key, compound_keys)

    index_entries
    |> Enum.zip(raw_values)
    |> Enum.flat_map(fn
      {{id_str, _compound_key}, raw} when is_binary(raw) ->
        case decode_stream_fields(raw) do
          {:ok, fields} -> [[id_str | fields]]
          :error -> []
        end

      _missing ->
        []
    end)
  end

  defp decode_stream_fields(raw) when is_binary(raw) do
    case Ferricstore.Flow.decode_history_fields(raw) do
      [_ | _] = fields -> {:ok, fields}
      _ -> :error
    end
  end

  defp decode_stream_fields(_), do: :error

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

  defp count_groups(key) do
    # Count groups by scanning the groups table.
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
