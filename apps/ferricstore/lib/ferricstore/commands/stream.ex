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
  alias Ferricstore.Commands.Stream.Args
  alias Ferricstore.Commands.Stream.Entries
  alias Ferricstore.Commands.Stream.Groups
  alias Ferricstore.Commands.Stream.ID
  alias Ferricstore.Commands.Stream.Info
  alias Ferricstore.Commands.Stream.Index
  alias Ferricstore.Commands.Stream.Meta
  alias Ferricstore.Commands.Stream.Mutations
  alias Ferricstore.Commands.Stream.Tables
  alias Ferricstore.Commands.Stream.Waiters
  alias Ferricstore.Stream.ActivityLog
  alias Ferricstore.Store.{Ops, ReadResult, TypeRegistry}

  @typedoc "A parsed stream ID as `{milliseconds, sequence}`."
  @type stream_id :: {non_neg_integer(), non_neg_integer()}

  @typedoc "A stream entry: `{id_string, [field, value, ...]}` flat list."
  @type entry :: {binary(), [binary()]}

  @max_timeout_ms 0xFFFFFFFF
  @max_u64 18_446_744_073_709_551_615
  @invalid_stream_id "ERR Invalid stream ID specified as stream command argument"

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
    case Args.parse_xadd_args(args) do
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
    with {:ok, count} <- Args.parse_count_opt(rest),
         {:ok, range_start} <- ID.parse_range_id(start_str, :min),
         {:ok, range_end} <- ID.parse_range_id(end_str, :max) do
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
    with {:ok, count} <- Args.parse_count_opt(rest),
         {:ok, range_start} <- ID.parse_range_id(start_str, :min),
         {:ok, range_end} <- ID.parse_range_id(end_str, :max) do
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
    case Args.parse_xread_args(args) do
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
    case Args.parse_trim_opts(rest) do
      {:ok, trim_opts} ->
        record_xtrim_result(key, trim_opts, Mutations.trim(key, trim_opts, store))

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
    record_xdel_result(key, Mutations.xdel(key, ids, store))
  end

  def handle("XDEL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xdel' command"}
  end

  # -------------------------------------------------------------------------
  # XINFO STREAM key [FULL [COUNT count]]
  # -------------------------------------------------------------------------

  def handle("XINFO", [subcommand, key], store) when is_binary(subcommand) do
    if String.upcase(subcommand) == "STREAM" do
      Info.stream(key, store)
    else
      {:error, "ERR wrong number of arguments for 'xinfo' command"}
    end
  end

  def handle("XINFO", [subcommand, _key | _rest], _store) when is_binary(subcommand) do
    if String.upcase(subcommand) == "STREAM" do
      {:error, "ERR syntax error"}
    else
      {:error, "ERR wrong number of arguments for 'xinfo' command"}
    end
  end

  def handle("XINFO", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xinfo' command"}
  end

  # -------------------------------------------------------------------------
  # XGROUP CREATE key groupname id [MKSTREAM]
  # -------------------------------------------------------------------------

  def handle("XGROUP", [subcommand, key, group, id_str | rest], store)
      when is_binary(subcommand) do
    if String.upcase(subcommand) == "CREATE" do
      with {:ok, mkstream} <- parse_xgroup_create_options(rest) do
        do_xgroup_create(key, group, id_str, mkstream, store)
      end
    else
      {:error, "ERR syntax error"}
    end
  end

  def handle("XGROUP", _args, _store) do
    {:error, "ERR wrong number of arguments for 'xgroup' command"}
  end

  # -------------------------------------------------------------------------
  # XREADGROUP GROUP group consumer [COUNT count] STREAMS key [key ...] id [id ...]
  # -------------------------------------------------------------------------

  def handle("XREADGROUP", args, store) do
    case Args.parse_xreadgroup_args(args) do
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
    with :ok <- validate_ast_xadd(key, id_spec, fields, trim_opts) do
      do_xadd(key, id_spec, fields, trim_opts, nomkstream, store)
    end
  end

  def handle_ast({:xlen, key}, store), do: xlen_key(key, store)
  def handle_ast({:xrange, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:xrevrange, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xrange, key, range_start, range_end, count}, store) do
    with {:ok, range_start} <- ID.normalize_ast_range_id(range_start, :min),
         {:ok, range_end} <- ID.normalize_ast_range_id(range_end, :max),
         {:ok, count} <- normalize_ast_count(count) do
      do_xrange(key, range_start, range_end, count, store)
    end
  end

  def handle_ast({:xrevrange, key, range_start, range_end, count}, store) do
    with {:ok, range_start} <- ID.normalize_ast_range_id(range_start, :min),
         {:ok, range_end} <- ID.normalize_ast_range_id(range_end, :max),
         {:ok, count} <- normalize_ast_count(count) do
      do_xrevrange(key, range_start, range_end, count, store)
    end
  end

  def handle_ast({:xread, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xread, count, :no_block, stream_ids}, store) do
    with {:ok, count} <- normalize_ast_count(count) do
      do_xread(stream_ids, count, store)
    end
  end

  def handle_ast({:xread, count, {:block, timeout_ms}, stream_ids}, store) do
    with {:ok, count} <- normalize_ast_count(count),
         :ok <- validate_ast_timeout(timeout_ms) do
      result = do_xread(stream_ids, count, store)

      if result == [] do
        {:block, timeout_ms, stream_ids, count}
      else
        result
      end
    end
  end

  def handle_ast({:xtrim, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xtrim, key, trim_opts}, store),
    do: record_xtrim_result(key, trim_opts, Mutations.trim(key, trim_opts, store))

  def handle_ast({:xdel, key, ids}, store) when is_list(ids) and ids != [],
    do: record_xdel_result(key, Mutations.xdel(key, ids, store))

  def handle_ast({:xinfo_stream, key}, store), do: Info.stream(key, store)
  def handle_ast({:xinfo, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:xgroup, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xgroup_create, key, group, id_str, mkstream}, store)
      when is_boolean(mkstream) do
    do_xgroup_create(key, group, id_str, mkstream, store)
  end

  def handle_ast({:xreadgroup, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:xreadgroup, group, consumer, {count, :no_block, stream_ids}}, store) do
    with {:ok, count} <- normalize_ast_count(count) do
      do_xreadgroup(group, consumer, stream_ids, count, store)
    end
  end

  def handle_ast({:xreadgroup, group, consumer, {count, {:block, timeout_ms}, stream_ids}}, store) do
    with {:ok, count} <- normalize_ast_count(count),
         :ok <- validate_ast_timeout(timeout_ms) do
      result = do_xreadgroup(group, consumer, stream_ids, count, store)

      if result == [] do
        {:block, timeout_ms, stream_ids, count}
      else
        result
      end
    end
  end

  def handle_ast({:xack, key, group, ids}, store) when is_list(ids) and ids != [] do
    do_xack(key, group, ids, store)
  end

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported stream command AST"}

  defp xlen_key(key, store) do
    # The metadata cache is local-only (not Raft-replicated), so on a follower we
    # don't have it. Fall back to counting the stream's compound entries —
    # those go through Router.compound_put and are present on every node.
    # On the originating node we still consult the meta table for O(1) speed
    # when populated; followers and post-migration always count via prefix.
    ensure_meta_table()

    with :ok <- Meta.ensure_read_type(key, store) do
      case Meta.entries(key, store) do
        [{^key, len, _first, _last, _ms, _seq}] -> len
        [] -> store |> Entries.count(key) |> ReadResult.command_result()
        {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
        {:error, _reason} = error -> error
      end
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

  @spec clear_local_state(term()) :: :ok
  def clear_local_state(store), do: Ferricstore.Stream.LocalState.clear(store)

  @doc """
  Ensures the stream metadata ETS tables exist.

  Called lazily on first use. The tables are `:public` and `:named_table`
  so any process can read and write them. When `init_tables/0` has been
  called at application startup, this is a cheap no-op.
  """
  @spec ensure_meta_table() :: :ok
  def ensure_meta_table, do: Tables.ensure_all()

  # ---------------------------------------------------------------------------
  # Stream waiter management (for XREAD BLOCK)
  # ---------------------------------------------------------------------------

  @spec register_stream_waiter(binary(), pid(), binary()) :: :ok
  defdelegate register_stream_waiter(stream_key, pid, last_seen_id), to: Waiters, as: :register

  @spec register_stream_waiter(binary(), pid(), binary(), term()) :: :ok
  defdelegate register_stream_waiter(stream_key, pid, last_seen_id, store),
    to: Waiters,
    as: :register

  @spec unregister_stream_waiter(binary(), pid()) :: :ok
  defdelegate unregister_stream_waiter(stream_key, pid), to: Waiters, as: :unregister

  @spec unregister_stream_waiter(binary(), pid(), term()) :: :ok
  defdelegate unregister_stream_waiter(stream_key, pid, store), to: Waiters, as: :unregister

  @spec cleanup_stream_waiters(pid()) :: :ok
  defdelegate cleanup_stream_waiters(pid), to: Waiters, as: :cleanup

  @spec stream_waiter_count(binary()) :: non_neg_integer()
  defdelegate stream_waiter_count(stream_key), to: Waiters, as: :count

  @spec stream_waiter_count(binary(), term()) :: non_neg_integer()
  defdelegate stream_waiter_count(stream_key, store), to: Waiters, as: :count

  @doc false
  @spec notify_stream_waiters(binary()) :: :ok
  defdelegate notify_stream_waiters(stream_key), to: Waiters, as: :notify

  @doc false
  @spec notify_stream_waiters(binary(), term()) :: :ok
  defdelegate notify_stream_waiters(stream_key, store), to: Waiters, as: :notify

  # ---------------------------------------------------------------------------
  # Private: XADD
  # ---------------------------------------------------------------------------

  defp do_xadd(key, id_spec, fields, trim_opts, nomkstream, store) do
    ensure_meta_table()
    meta_entries = Meta.xadd_entries(key, store)

    # Check if stream exists when NOMKSTREAM is set
    case meta_entries do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error

      [] when nomkstream ->
        nil

      meta_entries ->
        do_xadd_insert(key, id_spec, fields, trim_opts, meta_entries, nomkstream, store)
    end
  end

  defp do_xadd_insert(key, id_spec, fields, trim_opts, meta_entries, nomkstream, store) do
    with type_status when type_status in [:ok, {:ok, :created}, :no_marker] <-
           stream_type_status(key, store) do
      {last_ms, last_seq} =
        case meta_entries do
          [{^key, _len, _first, _last, ms, seq}] -> {ms, seq}
          [] -> {0, 0}
        end

      case ID.resolve(id_spec, last_ms, last_seq) do
        {:ok, {ms, seq}} ->
          id_str = "#{ms}-#{seq}"
          compound_key = Entries.entry_key(key, id_str)

          case Mutations.prepare_trim(key, id_str, trim_opts, store) do
            {:ok, trim_plan} ->
              encoded = Ferricstore.TermCodec.encode(fields)

              case Entries.put(store, key, compound_key, encoded) do
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

                  case Meta.put(key, new_len, new_first, id_str, ms, seq, store) do
                    :ok ->
                      case Mutations.maybe_trim(key, trim_plan, store) do
                        :ok ->
                          # Notify any XREAD BLOCK waiters watching this stream.
                          notify_stream_waiters(key, store)

                          ActivityLog.record_xadd(
                            key,
                            id_str,
                            div(length(fields), 2),
                            trim_opts,
                            nomkstream
                          )

                          id_str

                        {:error, _reason} = error ->
                          error
                      end

                    {:error, _reason} = error ->
                      rollback_xadd_metadata_failure(
                        key,
                        id_str,
                        compound_key,
                        type_status,
                        store,
                        error
                      )
                  end

                {:error, _} = error ->
                  rollback_new_stream_type_marker(key, store, type_status, error)
              end

            {:error, _reason} = error ->
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

  defp rollback_xadd_metadata_failure(
         key,
         id_str,
         compound_key,
         type_status,
         store,
         write_error
       ) do
    entry_result = Entries.delete(store, key, [compound_key])
    Index.delete_ids(key, [id_str], store)

    type_result =
      case type_status do
        {:ok, :created} -> TypeRegistry.delete_type(key, store)
        _existing_or_untyped -> :ok
      end

    if type_status == {:ok, :created} do
      Meta.cleanup_local(key, store)
    end

    case {entry_result, type_result} do
      {:ok, :ok} ->
        write_error

      {entry_error, type_error} ->
        {:error, {:stream_metadata_rollback_failed, write_error, entry_error, type_error}}
    end
  end

  defp record_xtrim_result(key, trim_opts, result) when is_integer(result) do
    ActivityLog.record_xtrim(key, result, trim_opts)
    result
  end

  defp record_xtrim_result(_key, _trim_opts, result), do: result

  defp record_xdel_result(key, result) when is_integer(result) do
    ActivityLog.record_xdel(key, result)
    result
  end

  defp record_xdel_result(_key, result), do: result

  defp stream_type_status(key, store) do
    if Ops.has_compound?(store) do
      TypeRegistry.command_check_or_set_status(key, :stream, store)
    else
      :no_marker
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XRANGE / XREVRANGE
  # ---------------------------------------------------------------------------

  defp do_xrange(key, range_start, range_end, count, store) do
    ensure_meta_table()

    with :ok <- Meta.ensure_read_type(key, store) do
      if Ops.has_compound?(store) do
        indexed_stream_range(key, range_start, range_end, count, false, store)
      else
        scanned_stream_range(key, range_start, range_end, count, store)
      end
    end
  end

  defp do_xrevrange(key, range_start, range_end, count, store) do
    ensure_meta_table()

    with :ok <- Meta.ensure_read_type(key, store) do
      if Ops.has_compound?(store) do
        indexed_stream_range(key, range_start, range_end, count, true, store)
      else
        case scanned_stream_range(key, range_start, range_end, :infinity, store) do
          {:error, _} = error -> error
          entries -> entries |> Enum.reverse() |> maybe_take(count)
        end
      end
    end
  end

  defp scanned_stream_range(key, range_start, range_end, count, store) do
    case Meta.entries(key, store) do
      [] ->
        []

      [{^key, _len, _first, _last, _ms, _seq}] ->
        case Entries.scan(store, key) do
          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          scanned when is_list(scanned) ->
            selected_entries =
              scanned
              |> Enum.map(fn {id_str, raw} -> {id_str, parse_id!(id_str), raw} end)
              |> Enum.filter(fn {_id_str, id, _raw} ->
                ID.in_range?(id, range_start, range_end)
              end)
              |> Enum.sort_by(fn {_id_str, id, _raw} -> id end)
              |> maybe_take(count)

            Enum.flat_map(selected_entries, fn {id_str, _id, raw} ->
              case Entries.decode_fields(raw) do
                {:ok, fields} -> [[id_str | fields]]
                :error -> []
              end
            end)
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp indexed_stream_range(key, range_start, range_end, count, reverse?, store) do
    with :ok <- Index.ensure(key, store) do
      key
      |> Index.slice(range_start, range_end, count, reverse?, store)
      |> Entries.decode_indexed(key, store)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XREAD
  # ---------------------------------------------------------------------------

  defp do_xread(stream_ids, count, store) do
    ensure_meta_table()

    case xread_results(stream_ids, count, store, []) do
      {:error, _} = err -> err
      results -> record_xread_result(stream_ids, Enum.reverse(results))
    end
  end

  defp xread_results([], _count, _store, acc), do: acc

  defp xread_results([{key, id_str} | rest], count, store, acc) do
    # Handle "$" -- resolve to current last ID of the stream.
    resolved_id =
      if id_str == "$" do
        case Meta.entries(key, store) do
          [{^key, _len, _first, last, _ms, _seq}] -> last
          [] -> "0-0"
          {:error, _} = error -> error
        end
      else
        id_str
      end

    # For XREAD, the start is exclusive (entries > id).
    case resolved_id do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _} = error ->
        error

      resolved_id ->
        xread_from_resolved_id(rest, key, resolved_id, count, store, acc)
    end
  end

  defp xread_from_resolved_id(rest, key, resolved_id, count, store, acc) do
    case ID.parse_exclusive_start(resolved_id) do
      {:ok, excl_start} ->
        case do_xrange(key, excl_start, :max, count, store) do
          [] -> xread_results(rest, count, store, acc)
          {:error, _} = error -> error
          entries -> xread_results(rest, count, store, [[key, entries] | acc])
        end

      {:error, _} = err ->
        err
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XGROUP CREATE
  # ---------------------------------------------------------------------------

  defp do_xgroup_create(key, group, id_str, mkstream, store) do
    with {:ok, id_str} <- normalize_group_start_id(id_str) do
      ensure_meta_table()

      Groups.with_lock(store, key, group, fn ->
        do_xgroup_create_locked(key, group, id_str, mkstream, store)
      end)
    end
  end

  defp do_xgroup_create_locked(key, group, id_str, mkstream, store) do
    case stream_exists_for_group_create(key, store) do
      {:ok, stream_exists?} ->
        do_xgroup_create_with_existence(key, group, id_str, mkstream, stream_exists?, store)

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp do_xgroup_create_with_existence(key, group, id_str, mkstream, stream_exists?, store) do
    with {:ok, group_exists?} <- stream_group_exists?(store, key, group) do
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
                  case Meta.put(key, 0, "0-0", "0-0", 0, 0, store) do
                    :ok ->
                      :ok

                    {:error, _reason} = error ->
                      rollback_xgroup_mkstream(key, group, type_status, store, error)
                  end

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
  end

  defp stream_group_exists?(store, key, group) do
    case Groups.lookup(store, key, group) do
      :missing -> {:ok, false}
      {:ok, _last_delivered, _consumers, _pending} -> {:ok, true}
      {:error, _reason} = error -> error
    end
  end

  defp stream_exists_for_group_create(key, store) do
    case Meta.entries(key, store) do
      [] -> Meta.type_marker_status(key, store)
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      {:error, _reason} = error -> error
      _entries -> {:ok, true}
    end
  end

  defp create_group(key, group, id_str, store) do
    # last_delivered_id: the ID from which new messages will be delivered.
    # "0" means deliver all messages from the beginning.
    # "$" means deliver only new messages from now on.
    last_delivered =
      case id_str do
        "$" ->
          case Meta.entries(key, store) do
            [{^key, _len, _first, last, _ms, _seq}] -> last
            [] -> "0-0"
          end

        other ->
          other
      end

    Groups.persist(store, key, group, last_delivered, %{}, %{})
  end

  defp rollback_xgroup_mkstream(key, group, type_status, store, write_error) do
    group_result = Groups.delete(store, key, group)

    type_result =
      case type_status do
        {:ok, :created} -> TypeRegistry.delete_type(key, store)
        _existing_or_untyped -> :ok
      end

    case {group_result, type_result} do
      {:ok, :ok} ->
        write_error

      {group_error, type_error} ->
        {:error, {:stream_group_metadata_rollback_failed, write_error, group_error, type_error}}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: XREADGROUP
  # ---------------------------------------------------------------------------

  defp do_xreadgroup(group, consumer, stream_ids, count, store) do
    ensure_meta_table()

    case xreadgroup_results(group, consumer, stream_ids, count, store, []) do
      {:error, _} = err -> err
      results -> record_xreadgroup_result(group, consumer, stream_ids, Enum.reverse(results))
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
    Groups.with_lock(store, key, group, fn ->
      case Groups.lookup(store, key, group) do
        :missing ->
          {:error, "NOGROUP No such consumer group '#{group}' for key name '#{key}'"}

        {:error, _reason} = error ->
          error

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
    case ID.parse_exclusive_start(last_delivered) do
      {:ok, excl_start} ->
        case do_xrange(key, excl_start, :max, count, store) do
          {:error, _reason} = error ->
            error

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

            case Groups.persist(
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
    with {:ok, pending_start} <- normalize_pending_start(id_str) do
      pending_ids = xreadgroup_pending_ids(pending, consumer, pending_start, count)

      pending_compound_keys = Enum.map(pending_ids, &Entries.entry_key(key, &1))

      raw_values = Entries.batch_get(store, key, pending_compound_keys)

      case ReadResult.first_failure(raw_values) do
        nil ->
          pending_entries =
            raw_values
            |> Enum.zip(pending_ids)
            |> Enum.reduce([], fn {raw, id_str_inner}, acc ->
              case Entries.decode_fields(raw) do
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

        failure ->
          ReadResult.command_error(failure)
      end
    end
  end

  defp xreadgroup_pending_ids(pending, consumer, pending_start, count) do
    pending
    |> Enum.reduce([], fn
      {id, {^consumer, _ts}}, acc ->
        parsed_id = parse_id!(id)

        if pending_start == :min or ID.compare(parsed_id, pending_start) != :lt do
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

    Groups.with_lock(store, key, group, fn ->
      do_xack_locked(key, group, ids, store)
    end)
  end

  defp do_xack_locked(key, group, ids, store) do
    case Groups.lookup(store, key, group) do
      :missing ->
        0

      {:error, _reason} = error ->
        error

      {:ok, last_delivered, consumers, pending} ->
        {new_pending, acked} =
          Enum.reduce(ids, {pending, 0}, fn id, {pend, count} ->
            if Map.has_key?(pend, id) do
              {Map.delete(pend, id), count + 1}
            else
              {pend, count}
            end
          end)

        case Groups.persist(store, key, group, last_delivered, consumers, new_pending) do
          :ok -> record_xack_result(key, group, acked)
          {:error, _reason} = error -> error
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: ID generation and parsing
  # ---------------------------------------------------------------------------

  @doc false
  @spec parse_id!(binary()) :: stream_id()
  defdelegate parse_id!(id_str), to: ID

  defp normalize_ast_count(nil), do: {:ok, :infinity}
  defp normalize_ast_count(:infinity), do: {:ok, :infinity}
  defp normalize_ast_count(count) when is_integer(count) and count >= 0, do: {:ok, count}

  defp normalize_ast_count(_count),
    do: {:error, "ERR value is not an integer or out of range"}

  defp validate_ast_timeout(timeout_ms)
       when is_integer(timeout_ms) and timeout_ms >= 0 and timeout_ms <= @max_timeout_ms,
       do: :ok

  defp validate_ast_timeout(_timeout_ms),
    do: {:error, "ERR value is not an integer or out of range"}

  defp validate_ast_xadd(key, id_spec, fields, trim_opts) when is_binary(key) do
    with :ok <- validate_ast_xadd_fields(fields),
         :ok <- validate_ast_xadd_id(id_spec),
         :ok <- validate_ast_trim(trim_opts) do
      :ok
    end
  end

  defp validate_ast_xadd(_key, _id_spec, _fields, _trim_opts),
    do: {:error, "ERR protocol error"}

  defp validate_ast_xadd_fields(fields) do
    if fields != [] and rem(length(fields), 2) == 0 and Enum.all?(fields, &is_binary/1) do
      :ok
    else
      {:error, "ERR wrong number of arguments for 'xadd' command"}
    end
  end

  defp validate_ast_xadd_id(:auto), do: :ok

  defp validate_ast_xadd_id({:explicit, ms, seq})
       when is_integer(ms) and ms in 0..@max_u64 and is_integer(seq) and seq in 0..@max_u64,
       do: :ok

  defp validate_ast_xadd_id({:partial, ms})
       when is_integer(ms) and ms in 0..@max_u64,
       do: :ok

  defp validate_ast_xadd_id(_id_spec), do: {:error, @invalid_stream_id}

  defp validate_ast_trim(nil), do: :ok

  defp validate_ast_trim({:maxlen, approximate?, max_len})
       when is_boolean(approximate?) and is_integer(max_len) and max_len >= 0,
       do: :ok

  defp validate_ast_trim({:maxlen, _approximate?, _max_len}),
    do: {:error, "ERR value is not an integer or out of range"}

  defp validate_ast_trim({:minid, approximate?, id_str})
       when is_boolean(approximate?) and is_binary(id_str) do
    case ID.parse_full_id(id_str) do
      {:ok, _id} -> :ok
      {:error, _message} = error -> error
    end
  end

  defp validate_ast_trim(_trim_opts), do: {:error, "ERR syntax error"}

  defp parse_xgroup_create_options([]), do: {:ok, false}

  defp parse_xgroup_create_options([option]) when is_binary(option) do
    if String.upcase(option) == "MKSTREAM", do: {:ok, true}, else: {:error, "ERR syntax error"}
  end

  defp parse_xgroup_create_options(_options), do: {:error, "ERR syntax error"}

  defp normalize_group_start_id("$"), do: {:ok, "$"}

  defp normalize_group_start_id(id_str) when is_binary(id_str) do
    case ID.parse_full_id(id_str) do
      {:ok, {ms, seq}} -> {:ok, "#{ms}-#{seq}"}
      {:error, _message} = error -> error
    end
  end

  defp normalize_group_start_id(_id_str), do: {:error, @invalid_stream_id}

  defp normalize_pending_start(id_str) when id_str in ["0", "0-0"], do: {:ok, :min}
  defp normalize_pending_start(id_str), do: ID.parse_full_id(id_str)

  # ---------------------------------------------------------------------------
  # Private: compound key helpers
  # ---------------------------------------------------------------------------

  defp maybe_index_stream_put(store, stream_key, id_str, compound_key, meta_entries) do
    if Ops.has_compound?(store) do
      Index.insert_entry(stream_key, id_str, compound_key, store)

      if Index.ready?(stream_key, store) or meta_entries == [] do
        Index.mark_ready(stream_key, store)
      end
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private: helpers
  # ---------------------------------------------------------------------------

  defp maybe_take(entries, :infinity), do: entries
  defp maybe_take(entries, n), do: Enum.take(entries, n)

  defp maybe_take_tuples(entries, :infinity), do: entries
  defp maybe_take_tuples(entries, n), do: Enum.take(entries, n)

  defp record_xread_result(_stream_ids, []), do: []

  defp record_xread_result(stream_ids, results) do
    ActivityLog.record_xread(stream_ids, results)
    results
  end

  defp record_xreadgroup_result(_group, _consumer, _stream_ids, []), do: []

  defp record_xreadgroup_result(group, consumer, stream_ids, results) do
    ActivityLog.record_xreadgroup(group, consumer, stream_ids, results)
    results
  end

  defp record_xack_result(key, group, acked) when is_integer(acked) do
    ActivityLog.record_xack(key, group, acked)
    acked
  end
end
