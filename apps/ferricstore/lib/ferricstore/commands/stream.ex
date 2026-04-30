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
  @stream_waiters_table :ferricstore_stream_waiters

  # Null byte separator between stream key and entry ID in compound keys.
  @sep <<0>>

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

  def handle("XLEN", [key], store) do
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
      entries = do_xrange(key, range_start, range_end, :infinity, store)
      entries = Enum.reverse(entries)

      case count do
        :infinity -> entries
        n -> Enum.take(entries, n)
      end
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
        Ops.put(store, compound_key, encoded, 0)

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

      {:error, _} = err ->
        err
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XRANGE / XREVRANGE
  # ---------------------------------------------------------------------------

  defp do_xrange(key, range_start, range_end, count, store) do
    ensure_meta_table()

    case :ets.lookup(@meta_table, key) do
      [] ->
        []

      [{^key, _len, _first, _last, _ms, _seq}] ->
        # Scan all entries for this stream. In a production system this would
        # use a Bitcask range scan or an ordered index. For v1, we retrieve
        # all keys with the stream prefix and filter.
        prefix = "X:#{key}" <> @sep

        all_entries =
          store
          |> stream_keys_for(prefix)
          |> Enum.map(fn compound_key ->
            id_str = String.replace_prefix(compound_key, prefix, "")
            {id_str, parse_id!(id_str)}
          end)
          |> Enum.filter(fn {_id_str, id} ->
            id_in_range?(id, range_start, range_end)
          end)
          |> Enum.sort_by(fn {_id_str, id} -> id end)
          |> maybe_take(count)
          |> Enum.map(fn {id_str, _id} ->
            compound_key = prefix <> id_str
            raw = Ops.get(store, compound_key)

            if raw do
              fields = :erlang.binary_to_term(raw)
              [id_str | fields]
            end
          end)
          |> Enum.reject(&is_nil/1)

        all_entries
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
    prefix = "X:#{key}" <> @sep

    all_ids =
      store
      |> stream_keys_for(prefix)
      |> Enum.map(fn ck -> String.replace_prefix(ck, prefix, "") end)
      |> Enum.sort_by(&parse_id!/1)

    current_len = length(all_ids)

    if current_len > max_len do
      to_remove = Enum.take(all_ids, current_len - max_len)

      Enum.each(to_remove, fn id_str ->
        Ops.delete(store, prefix <> id_str)
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

  defp apply_trim(key, {:minid, _approx, min_id_str}, store) do
    prefix = "X:#{key}" <> @sep

    case parse_full_id(min_id_str) do
      {:error, _} = err -> err
      {:ok, min_id} -> do_apply_trim_minid(key, prefix, min_id, store)
    end
  end

  defp do_apply_trim_minid(key, prefix, min_id, store) do
    all_ids =
      store
      |> stream_keys_for(prefix)
      |> Enum.map(fn ck -> String.replace_prefix(ck, prefix, "") end)
      |> Enum.sort_by(&parse_id!/1)

    {to_remove, _keep} =
      Enum.split_with(all_ids, fn id_str ->
        id_cmp(parse_id!(id_str), min_id) == :lt
      end)

    Enum.each(to_remove, fn id_str ->
      Ops.delete(store, prefix <> id_str)
    end)

    deleted_count = length(to_remove)

    if deleted_count > 0 do
      remaining = all_ids -- to_remove
      update_meta_after_trim(key, remaining)
    end

    deleted_count
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

    deleted =
      Enum.reduce(ids, 0, fn id_str, acc ->
        compound_key = prefix <> id_str

        if Ops.exists?(store, compound_key) do
          Ops.delete(store, compound_key)
          acc + 1
        else
          acc
        end
      end)

    if deleted > 0 do
      # Rebuild metadata from remaining entries.
      remaining_ids =
        store
        |> stream_keys_for(prefix)
        |> Enum.map(fn ck -> String.replace_prefix(ck, prefix, "") end)
        |> Enum.sort_by(&parse_id!/1)

      if remaining_ids == [] do
        # Stream is now empty. Keep the stream in metadata with length 0
        # but preserve last_id for future XADD ordering.
        case :ets.lookup(@meta_table, key) do
          [{^key, _len, _first, last, ms, seq}] ->
            :ets.insert(@meta_table, {key, 0, "0-0", last, ms, seq})

          [] ->
            :ok
        end
      else
        first_str = List.first(remaining_ids)
        last_str = List.last(remaining_ids)
        {last_ms, last_seq} = parse_id!(last_str)

        :ets.insert(
          @meta_table,
          {key, length(remaining_ids), first_str, last_str, last_ms, last_seq}
        )
      end
    end

    deleted
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

        first_entry =
          if len > 0 and first != "0-0" do
            raw = Ops.get(store, prefix <> first)

            if raw do
              fields = :erlang.binary_to_term(raw)
              [first | fields]
            end
          end

        last_entry =
          if len > 0 do
            raw = Ops.get(store, prefix <> last)

            if raw do
              fields = :erlang.binary_to_term(raw)
              [last | fields]
            end
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

                    if raw do
                      fields = :erlang.binary_to_term(raw)
                      [id_str_inner | fields]
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

  defp stream_keys_for(store, prefix) do
    Ops.keys(store)
    |> Enum.filter(&String.starts_with?(&1, prefix))
  end

  # Count compound entries with prefix `X:<stream_key>\0` — used by XLEN as
  # the fallback path on nodes where the local `@meta_table` doesn't have
  # the stream registered (notably Raft followers).
  defp count_stream_entries(store, stream_key) do
    prefix = "X:#{stream_key}" <> @sep
    stream_keys_for(store, prefix) |> length()
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
