defmodule FerricStore.API.Bitmap do
  @moduledoc false

  import FerricStore.API.Store
  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.Bitmap

  @type key :: FerricStore.key()
  @type value :: FerricStore.value()
  @type write_error :: FerricStore.write_error()
  @type set_opts :: FerricStore.set_opts()
  @type get_opts :: FerricStore.get_opts()
  @type cas_opts :: FerricStore.cas_opts()
  @type fetch_or_compute_opts :: FerricStore.fetch_or_compute_opts()
  @type zrange_opts :: FerricStore.zrange_opts()

  @doc """
  Sets or clears the bit at `offset` in the string value stored at `key`.

  Returns the original bit value at that position.

  ## Examples

      {:ok, 0} = FerricStore.setbit("key", 7, 1)

  """
  @spec setbit(key(), non_neg_integer(), 0 | 1) :: {:ok, 0 | 1} | {:error, binary()}
  def setbit(key, offset, bit_value) when bit_value in [0, 1] do
    cond do
      offset < 0 ->
        {:error, "ERR bit offset is not an integer or out of range"}

      offset > 4_294_967_295 ->
        {:error, "ERR bit offset is not an integer or out of range"}

      true ->
        wrap_result(Router.setbit(default_ctx(), key, offset, bit_value))
    end
  end

  @doc """
  Returns the bit value at `offset` in the string value stored at `key`.

  Returns `{:ok, 0}` for nonexistent keys or out-of-range offsets.

  ## Examples

      {:ok, 1} = FerricStore.getbit("key", 7)

  """
  @spec getbit(key(), non_neg_integer()) :: {:ok, 0 | 1}
  def getbit(key, offset) do
    store = build_string_store(key)
    result = Bitmap.handle_ast({:getbit, key, offset}, store)
    wrap_result(result)
  end

  @doc """
  Counts the number of set bits (1s) in the string value stored at `key`.

  ## Options

    * `:start` - Start byte offset (default: 0).
    * `:stop` - Stop byte offset (default: -1, meaning end of string).

  ## Returns

    * `{:ok, count}` on success.

  ## Examples

      {:ok, count} = FerricStore.bitcount("key")

  """
  @spec bitcount(key(), keyword()) :: {:ok, non_neg_integer()}
  def bitcount(key, opts \\ []) do
    store = build_string_store(key)
    start = Keyword.get(opts, :start)
    stop = Keyword.get(opts, :stop)

    ast =
      if start != nil and stop != nil do
        {:bitcount, key, {start, stop, :byte}}
      else
        {:bitcount, key}
      end

    result = Bitmap.handle_ast(ast, store)
    wrap_result(result)
  end

  @doc """
  Performs a bitwise operation between strings stored at `source_keys` and
  stores the result in `dest_key`.

  ## Parameters

    * `op` - `:and`, `:or`, `:xor`, or `:not`
    * `dest_key` - Destination key.
    * `source_keys` - List of source keys.

  ## Returns

    * `{:ok, byte_length}` - Length of the result string.

  ## Examples

      {:ok, 3} = FerricStore.bitop(:and, "dest", ["key1", "key2"])

  """
  @spec bitop(atom(), key(), [key()]) :: {:ok, non_neg_integer()}
  def bitop(op, dest_key, source_keys) when is_atom(op) and is_list(source_keys) do
    ast_op =
      case op do
        :and -> :band
        :or -> :bor
        :xor -> :bxor
        :not -> :bnot
        _ -> op
      end

    wrap_result(
      Bitmap.handle_ast({:bitop, ast_op, dest_key, source_keys}, build_string_store(dest_key))
    )
  end

  @doc """
  Finds the first bit set to `bit_value` (0 or 1) in the string at `key`.

  ## Options

    * `:start` - Start byte offset.
    * `:stop` - Stop byte offset.

  ## Returns

    * `{:ok, position}` - Bit position, or -1 if not found within a bounded range.

  ## Examples

      {:ok, 8} = FerricStore.bitpos("key", 1)

  """
  @spec bitpos(key(), 0 | 1, keyword()) :: {:ok, integer()}
  def bitpos(key, bit_value, opts \\ []) when bit_value in [0, 1] do
    store = build_string_store(key)
    start = Keyword.get(opts, :start)
    stop = Keyword.get(opts, :stop)

    ast =
      cond do
        start != nil and stop != nil ->
          {:bitpos, key, bit_value, {start, stop, :byte}}

        start != nil ->
          {:bitpos, key, bit_value, {:start, start}}

        true ->
          {:bitpos, key, bit_value, :all}
      end

    result = Bitmap.handle_ast(ast, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------------------
  # Hash extended operations
  # ---------------------------------------------------------------------------
end
