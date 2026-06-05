defmodule FerricStore.API.Streams do
  @moduledoc false

  import FerricStore.API.Store

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

    * `{:ok, entries}` where entries is a list of `{id, [field, value, ...]}` tuples.

  ## Examples

      iex> FerricStore.xrange("events:user:42", "-", "+", count: 10)
      {:ok, [{"1711234567890-0", ["action", "login", "ip", "10.0.0.1"]}]}

      iex> FerricStore.xrange("activity:feed", "-", "+")
      {:ok, [{"1711234567891-0", ["type", "comment", "body", "looks great!"]}]}

  """
  @spec xrange(key(), binary(), binary(), keyword()) :: {:ok, [tuple()]}
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

    * `{:ok, entries}` where entries is a list of `{id, [field, value, ...]}` tuples.

  ## Examples

      iex> FerricStore.xrevrange("events:user:42", "+", "-", count: 5)
      {:ok, [{"1711234567890-0", ["action", "login", "ip", "10.0.0.1"]}]}

  """
  @spec xrevrange(key(), binary(), binary(), keyword()) :: {:ok, [tuple()]}
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
