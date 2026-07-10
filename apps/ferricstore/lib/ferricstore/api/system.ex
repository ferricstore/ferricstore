defmodule FerricStore.API.System do
  @moduledoc false

  alias FerricStore.API.Generic, as: GenericAPI

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Executes a sequence of commands atomically as a transaction.

  The provided function receives a `FerricStore.Tx` accumulator and should
  pipe commands into it. All commands execute in order and results are returned.

  ## Examples

      {:ok, [:ok, {:ok, "v1"}]} = FerricStore.multi(fn tx ->
        tx
        |> FerricStore.Tx.set("k1", "v1")
        |> FerricStore.Tx.get("k1")
      end)

  """
  @spec multi((FerricStore.Tx.t() -> FerricStore.Tx.t())) :: {:ok, [term()]} | {:error, binary()}
  def multi(fun) when is_function(fun, 1) do
    tx = fun.(FerricStore.Tx.new())

    case FerricStore.Tx.execute(tx) do
      {:error, _} = err -> err
      results when is_list(results) -> {:ok, results}
    end
  end

  # ---------------------------------------------------------------------------
  # Server: ping, echo, flushall
  # ---------------------------------------------------------------------------

  @doc """
  Health check that returns `{:ok, "PONG"}`.

  ## Examples

      iex> FerricStore.ping()
      {:ok, "PONG"}

  """
  @spec ping() :: {:ok, binary()}
  def ping, do: {:ok, "PONG"}

  @doc """
  Echoes back the given message, useful for connection testing.

  ## Examples

      iex> FerricStore.echo("hello")
      {:ok, "hello"}

  """
  @spec echo(binary()) :: {:ok, binary()}
  def echo(message) when is_binary(message), do: {:ok, message}

  @doc """
  Deletes all keys from the store.

  Alias for `flushdb/0`.

  ## Examples

      iex> FerricStore.flushall()
      :ok

  Returns `{:error, reason}` when delegated cleanup cannot make filesystem
  namespace changes durable.
  """
  @spec flushall() :: :ok | {:error, term()}
  def flushall, do: GenericAPI.flushdb()

  # ---------------------------------------------------------------------------
  # Pipeline
  # ---------------------------------------------------------------------------

  @doc """
  Batches multiple commands into a single group-commit entry.

  The provided function receives a `FerricStore.Pipe` accumulator and should
  pipe commands into it. All commands are executed atomically on completion.

  ## Examples

      results = FerricStore.pipeline(fn pipe ->
        pipe
        |> FerricStore.Pipe.set("key1", "val1")
        |> FerricStore.Pipe.set("key2", "val2")
        |> FerricStore.Pipe.incr("counter")
      end)

  ## Returns

    * `{:ok, results}` - a list of results for each piped command, in order.

  """
  @spec pipeline((FerricStore.Pipe.t() -> FerricStore.Pipe.t())) ::
          {:ok, [term()]} | write_error()
  def pipeline(fun) when is_function(fun, 1) do
    pipe = fun.(FerricStore.Pipe.new())

    case FerricStore.Pipe.execute(pipe) do
      {:error, _reason} = error -> error
      results -> {:ok, results}
    end
  end

  @doc """
  Batch GET: takes a list of keys, returns a list of values (nil for missing).

  Goes directly to `Router.batch_get` — single HLC timestamp, zero GenServer,
  zero Pipe struct overhead. Designed for erpc callers.
  """
  @spec batch_get([binary()]) :: [binary() | nil] | write_error()
  def batch_get(keys) when is_list(keys) do
    with :ok <- Ferricstore.Flow.InternalKey.authorize_public(keys) do
      ctx = FerricStore.Instance.get(:default)
      Ferricstore.Store.Router.batch_get(ctx, keys)
    end
  end

  @doc """
  Packed binary batch GET — minimal distribution overhead.

  Input: single binary with packed keys: `<<count::32, key_len::16, key::binary, ...>>`
  Output: single binary with packed values: `<<val_len::32, val::binary, ...>>`
  where val_len=0xFFFFFFFF means nil.

  One flat binary over distribution instead of a list of N binaries —
  eliminates per-element external term format encoding.
  """
  @spec packed_batch_get(binary()) :: binary() | write_error()
  def packed_batch_get(packed_keys) when is_binary(packed_keys) do
    ctx = FerricStore.Instance.get(:default)
    <<count::32, rest::binary>> = packed_keys
    keys = unpack_keys(rest, count, [])

    with :ok <- Ferricstore.Flow.InternalKey.authorize_public(keys) do
      values = Ferricstore.Store.Router.batch_get(ctx, keys)
      pack_values(values, [])
    end
  end

  defp unpack_keys(_rest, 0, acc), do: Enum.reverse(acc)

  defp unpack_keys(<<len::16, key::binary-size(len), rest::binary>>, n, acc) do
    unpack_keys(rest, n - 1, [key | acc])
  end

  defp pack_values([], acc), do: IO.iodata_to_binary(Enum.reverse(acc))

  defp pack_values([nil | rest], acc) do
    pack_values(rest, [<<0xFFFFFFFF::32>> | acc])
  end

  defp pack_values([value | rest], acc) when is_binary(value) do
    pack_values(rest, [<<byte_size(value)::32, value::binary>> | acc])
  end

  @doc """
  Batch SET: takes a list of `{key, value}` pairs, returns a list of results.

  Routes through `Router.batch_quorum_put`. Designed for erpc callers.
  """
  @spec batch_set([{binary(), binary()}]) :: [:ok | write_error()] | write_error()
  def batch_set(kv_pairs) when is_list(kv_pairs) do
    with :ok <-
           kv_pairs
           |> FerricStore.API.PublicAccess.pair_keys()
           |> Ferricstore.Flow.InternalKey.authorize_public() do
      ctx = FerricStore.Instance.get(:default)
      Ferricstore.Store.Router.batch_quorum_put(ctx, kv_pairs)
    end
  end
end
