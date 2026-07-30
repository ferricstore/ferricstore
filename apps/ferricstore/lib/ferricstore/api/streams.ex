defmodule FerricStore.API.Streams do
  @moduledoc false

  import FerricStore.API.Store

  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Store.Router
  alias Ferricstore.Stream.ActivityLog, as: StreamActivityLog

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Appends an entry to the stream at `key` with an auto-generated ID.

  `fields` is a flat list of field-value pairs: `["field1", "val1", "field2", "val2"]`.
  Streams are append-only logs ideal for event sourcing, activity feeds, and audit trails.

  ## Returns

    * `{:ok, entry_id}` where `entry_id` is a `"timestamp-seq"` string.
    * `{:error, reason}` on failure.

  ## Examples

      iex> FerricStore.xadd("events:user:42", ["action", "login", "ip", "10.0.0.1"])
      {:ok, "1711234567890-0"}

      iex> FerricStore.xadd("activity:feed", ["type", "comment", "body", "looks great!"])
      {:ok, "1711234567891-0"}

  """
  @spec xadd(key(), [binary()]) :: {:ok, binary()} | {:error, binary()}
  def xadd(key, fields) when is_list(fields) do
    store = build_stream_store(key)

    result =
      Ferricstore.Commands.Stream.handle_ast({:xadd, key, {:auto, fields, nil, false}}, store)

    wrap_result(result)
  end

  @doc """
  Appends many entries using one ordered replicated batch per shard.

  Each item is a `{stream_key, fields}` tuple. IDs are generated atomically on
  the owning shard, and results retain input order. Commands on one shard share
  one Raft/WAL entry; commands spanning shards are committed independently and
  concurrently.

  This is a group-commit API, not an all-or-nothing cross-shard transaction.
  A per-item error does not roll back successful appends in the same batch.

  ## Examples

      FerricStore.xadd_many([
        {"events:{tenant-1}", ["type", "created"]},
        {"events:{tenant-1}", ["type", "updated"]}
      ])
      #=> [{:ok, "1711234567890-0"}, {:ok, "1711234567890-1"}]

  """
  @spec xadd_many([{key(), [binary()]}]) ::
          [{:ok, binary()} | {:error, term()}] | {:error, binary()}
  def xadd_many([]), do: []

  def xadd_many(items) when is_list(items) do
    with {:ok, plan} <- validate_and_plan_xadd_many(items) do
      ctx = default_ctx()

      ctx
      |> submit_xadd_many(plan)
      |> record_xadd_many_activity(items)
      |> Enum.map(&wrap_xadd_many_result/1)
    end
  end

  def xadd_many(_items), do: {:error, "ERR XADD_MANY items must be a list"}

  defp validate_and_plan_xadd_many([{key, _fields} | _rest] = items),
    do: validate_and_build_xadd_many_plan(items, key, [], nil, false)

  defp validate_and_plan_xadd_many(_items), do: invalid_xadd_many_items()

  defp submit_xadd_many(ctx, {:same_stream, key, fields_lists}),
    do: Router.stream_append_many_auto(ctx, key, fields_lists)

  defp submit_xadd_many(ctx, {:mixed_streams, items}),
    do: Router.stream_append_many_auto_mixed(ctx, items)

  defp validate_and_build_xadd_many_plan(
         [],
         first_key,
         reversed_fields,
         reversed_items,
         reserved?
       ) do
    if reserved? do
      {:error, InternalKey.error_message()}
    else
      plan =
        case reversed_items do
          nil -> {:same_stream, first_key, Enum.reverse(reversed_fields)}
          items -> {:mixed_streams, Enum.reverse(items)}
        end

      {:ok, plan}
    end
  end

  defp validate_and_build_xadd_many_plan(
         [{key, fields} = item | rest],
         first_key,
         reversed_fields,
         reversed_items,
         reserved?
       ) do
    if valid_xadd_many_item?(item) do
      {next_reversed_fields, next_reversed_items} =
        case reversed_items do
          nil when key == first_key ->
            {[fields | reversed_fields], nil}

          nil ->
            prefix_items = Enum.map(reversed_fields, &{first_key, &1})

            {[], [{key, fields} | prefix_items]}

          items ->
            {reversed_fields, [{key, fields} | items]}
        end

      validate_and_build_xadd_many_plan(
        rest,
        first_key,
        next_reversed_fields,
        next_reversed_items,
        reserved? or InternalKey.reserved?(key)
      )
    else
      invalid_xadd_many_items()
    end
  end

  defp validate_and_build_xadd_many_plan(
         _invalid,
         _first_key,
         _reversed_fields,
         _reversed_commands,
         _reserved?
       ),
       do: invalid_xadd_many_items()

  defp valid_xadd_many_item?({key, [field, value | rest]})
       when is_binary(key) and is_binary(field) and is_binary(value),
       do: valid_xadd_many_field_pairs?(rest)

  defp valid_xadd_many_item?(_item), do: false

  defp valid_xadd_many_field_pairs?([]), do: true

  defp valid_xadd_many_field_pairs?([field, value | rest])
       when is_binary(field) and is_binary(value),
       do: valid_xadd_many_field_pairs?(rest)

  defp valid_xadd_many_field_pairs?(_invalid), do: false

  defp invalid_xadd_many_items,
    do: {:error, "ERR XADD_MANY requires {binary_key, non-empty_binary_field_pairs} items"}

  defp record_xadd_many_activity(results, items) do
    StreamActivityLog.record_xadd_results(results, items)
    results
  end

  defp wrap_xadd_many_result({:error, _reason} = error), do: error
  defp wrap_xadd_many_result({:ok, id}) when is_binary(id), do: {:ok, id}
  defp wrap_xadd_many_result(id) when is_binary(id), do: {:ok, id}
  defp wrap_xadd_many_result(other), do: {:error, other}

  @doc """
  Returns the number of entries in the stream at `key`.

  ## Returns

    * `{:ok, length}` on success.

  ## Examples

      iex> FerricStore.xlen("events:user:42")
      {:ok, 5}

  """
  @spec xlen(key()) :: {:ok, non_neg_integer()}
  def xlen(key) do
    store = build_stream_store(key)
    result = Ferricstore.Commands.Stream.handle_ast({:xlen, key}, store)
    wrap_result(result)
  end

  @doc """
  Returns entries from the stream at `key` in forward (oldest-first) order between `start` and `stop`.

  Use `"-"` for the minimum and `"+"` for the maximum stream IDs.

  ## Options

    * `:count` - Maximum number of entries to return.

  ## Returns

    * `{:ok, entries}` where each entry is `[id, field, value, ...]`.

  ## Examples

      iex> FerricStore.xrange("events:user:42", "-", "+", count: 10)
      {:ok, [["1711234567890-0", "action", "login", "ip", "10.0.0.1"]]}

      iex> FerricStore.xrange("activity:feed", "-", "+")
      {:ok, [["1711234567891-0", "type", "comment", "body", "looks great!"]]}

  """
  @spec xrange(key(), binary(), binary(), keyword()) :: {:ok, [[binary()]]} | {:error, binary()}
  def xrange(key, start, stop, opts \\ []) do
    store = build_stream_store(key)
    count = Keyword.get(opts, :count)
    count = if count, do: count, else: :infinity

    result =
      Ferricstore.Commands.Stream.handle_ast(
        {:xrange, key, parse_stream_range_id(start, true), parse_stream_range_id(stop, false),
         count},
        store
      )

    wrap_result(result)
  end

  @doc """
  Returns entries from the stream at `key` in reverse (newest-first) order between `stop` and `start`.

  ## Options

    * `:count` - Maximum number of entries to return.

  ## Returns

    * `{:ok, entries}` where each entry is `[id, field, value, ...]`.

  ## Examples

      iex> FerricStore.xrevrange("events:user:42", "+", "-", count: 5)
      {:ok, [["1711234567890-0", "action", "login", "ip", "10.0.0.1"]]}

  """
  @spec xrevrange(key(), binary(), binary(), keyword()) ::
          {:ok, [[binary()]]} | {:error, binary()}
  def xrevrange(key, stop, start, opts \\ []) do
    store = build_stream_store(key)
    count = Keyword.get(opts, :count)
    count = if count, do: count, else: :infinity

    result =
      Ferricstore.Commands.Stream.handle_ast(
        {:xrevrange, key, parse_stream_range_id(start, true), parse_stream_range_id(stop, false),
         count},
        store
      )

    wrap_result(result)
  end

  @doc """
  Trims the stream at `key` to a maximum number of entries, evicting the oldest.

  Useful for capping event logs and activity feeds to prevent unbounded growth.

  ## Options

    * `:maxlen` (required) - Maximum number of entries to keep.

  ## Returns

    * `{:ok, trimmed_count}` - the number of entries removed.

  ## Examples

      iex> FerricStore.xtrim("events:user:42", maxlen: 1000)
      {:ok, 5}

  """
  @spec xtrim(key(), keyword()) :: {:ok, non_neg_integer()}
  def xtrim(key, opts) do
    store = build_stream_store(key)
    maxlen = Keyword.fetch!(opts, :maxlen)

    result =
      Ferricstore.Commands.Stream.handle_ast({:xtrim, key, {:maxlen, false, maxlen}}, store)

    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Bloom Filter operations
  # ---------------------------------------------------------------------------
end
