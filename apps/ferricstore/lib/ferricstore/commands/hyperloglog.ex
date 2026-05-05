defmodule Ferricstore.Commands.HyperLogLog do
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @moduledoc """
  Handles Redis HyperLogLog commands: PFADD, PFCOUNT, PFMERGE.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms -- the connection layer
  handles RESP encoding.

  HyperLogLog sketches are stored as plain 16,384-byte binary values in the
  store with no expiry (expire_at_ms = 0). They are transparent to the store
  layer -- just another binary blob keyed by a string.

  ## Supported commands

    * `PFADD key element [element ...]` -- adds elements to the HLL sketch at
      `key`, creating it if absent. Returns 1 if the sketch was modified, 0 if
      not.
    * `PFCOUNT key [key ...]` -- returns the estimated cardinality. For a
      single key, returns the estimate from that sketch. For multiple keys,
      merges sketches in memory (without writing) and returns the combined
      estimate. Non-existent keys are treated as empty sketches.
    * `PFMERGE destkey sourcekey [sourcekey ...]` -- merges all source sketches
      into `destkey` (which may or may not already exist). For each register
      position, takes the maximum value across all sources and the existing
      dest. Returns `:ok`.
  """

  alias Ferricstore.HyperLogLog, as: HLL

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
  @sketch_size HLL.num_registers()

  @doc """
  Handles typed HyperLogLog command AST terms produced by the Rust RESP parser.
  """
  @spec handle_ast(term(), map()) :: term()
  def handle_ast({tag, {:error, msg}}, _store) when is_atom(tag), do: {:error, msg}
  def handle_ast({:pfadd, args}, store), do: pfadd_args(args, store)
  def handle_ast({:pfcount, args}, store), do: pfcount_args(args, store)
  def handle_ast({:pfmerge, args}, store), do: pfmerge_args(args, store)

  @doc """
  Handles a HyperLogLog command.

  ## Parameters

    - `cmd` - Uppercased command name (`"PFADD"`, `"PFCOUNT"`, or `"PFMERGE"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `exists?` callbacks

  ## Returns

  Plain Elixir term: integer, `:ok`, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # PFADD key element [element ...]
  # ---------------------------------------------------------------------------

  def handle("PFADD", args, store), do: pfadd_args(args, store)

  # ---------------------------------------------------------------------------
  # PFCOUNT key [key ...]
  # ---------------------------------------------------------------------------

  def handle("PFCOUNT", args, store), do: pfcount_args(args, store)

  # ---------------------------------------------------------------------------
  # PFMERGE destkey sourcekey [sourcekey ...]
  # ---------------------------------------------------------------------------

  def handle("PFMERGE", args, store), do: pfmerge_args(args, store)

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp pfadd_args([key], store) do
    case ensure_not_compound_key(key, store) do
      :ok ->
        case hll_size_state(key, store) do
          :missing ->
            Ops.put(store, key, HLL.new(), 0)
            1

          :valid_size ->
            0

          :invalid_size ->
            @wrongtype_error

          :unknown ->
            pfadd_no_elements_from_value(key, store)
        end

      @wrongtype_error ->
        @wrongtype_error
    end
  end

  defp pfadd_args([key | elements], store) when elements != [] do
    sketch = get_or_new(key, store)

    case validate_sketch(sketch) do
      :ok ->
        {updated, modified?} =
          Enum.reduce(elements, {sketch, false}, fn elem, {sk, changed?} ->
            {new_sk, did_change?} = HLL.add(sk, elem)
            {new_sk, changed? or did_change?}
          end)

        if modified? do
          Ops.put(store, key, updated, 0)
          1
        else
          0
        end

      {:error, _} = err ->
        err
    end
  end

  defp pfadd_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'pfadd' command"}
  end

  defp pfadd_no_elements_from_value(key, store) do
    case Ops.get(store, key) do
      nil ->
        Ops.put(store, key, HLL.new(), 0)
        1

      sketch ->
        case validate_sketch(sketch) do
          :ok -> 0
          {:error, _} = err -> err
        end
    end
  end

  defp pfcount_args(keys, store) when keys != [] do
    case read_sketches(keys, store) do
      {:ok, [single]} ->
        HLL.count(single)

      {:ok, collected} ->
        collected
        |> Enum.reduce(&HLL.merge/2)
        |> HLL.count()

      {:error, _} = err ->
        err
    end
  end

  defp pfcount_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'pfcount' command"}
  end

  defp pfmerge_args([destkey | source_keys], store) when source_keys != [] do
    case read_sketches([destkey | source_keys], store) do
      {:ok, sketches} ->
        merged = Enum.reduce(sketches, &HLL.merge/2)
        Ops.put(store, destkey, merged, 0)
        :ok

      {:error, _} = err ->
        err
    end
  end

  defp pfmerge_args(_args, _store) do
    {:error, "ERR wrong number of arguments for 'pfmerge' command"}
  end

  # Returns the existing sketch for `key`, or a new empty sketch if the key
  # does not exist.
  @spec get_or_new(binary(), map()) :: binary() | {:error, binary()}
  defp get_or_new(key, store) do
    case ensure_not_compound_key(key, store) do
      :ok ->
        case hll_size_state(key, store) do
          :missing ->
            HLL.new()

          :invalid_size ->
            @wrongtype_error

          :valid_size ->
            Ops.get(store, key) || HLL.new()

          :unknown ->
            Ops.get(store, key) || HLL.new()
        end

      @wrongtype_error ->
        @wrongtype_error
    end
  end

  defp read_sketches(keys, store) do
    with :ok <- ensure_not_compound_keys(keys, store),
         :ok <- ensure_hll_candidate_sizes(keys, store) do
      store
      |> Ops.batch_get(keys)
      |> Enum.map(fn
        nil ->
          HLL.new()

        value ->
          value
      end)
      |> validate_sketches()
    end
  end

  defp ensure_hll_candidate_sizes(keys, store) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case hll_size_state(key, store) do
        :invalid_size -> {:halt, @wrongtype_error}
        _ -> {:cont, :ok}
      end
    end)
  end

  defp hll_size_state(key, store) do
    case metadata_value_size(store, key) do
      nil -> :missing
      @sketch_size -> :valid_size
      size when is_integer(size) -> :invalid_size
      :unknown -> :unknown
    end
  end

  defp metadata_value_size(%FerricStore.Instance{} = store, key), do: Ops.value_size(store, key)

  defp metadata_value_size(%Ferricstore.Store.LocalTxStore{} = store, key),
    do: Ops.value_size(store, key)

  defp metadata_value_size(%{value_size: value_size}, key) when is_function(value_size, 1),
    do: value_size.(key)

  defp metadata_value_size(_store, _key), do: :unknown

  defp ensure_not_compound_keys(keys, store) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case ensure_not_compound_key(key, store) do
        :ok -> {:cont, :ok}
        @wrongtype_error -> {:halt, @wrongtype_error}
      end
    end)
  end

  defp ensure_not_compound_key(key, store) do
    if compound_data_structure_key?(key, store) do
      @wrongtype_error
    else
      :ok
    end
  end

  defp compound_data_structure_key?(key, store) do
    Ops.has_compound?(store) and
      compound_type_marker?(key, store) and
      TypeRegistry.get_type(key, store) != "none"
  end

  defp compound_type_marker?(key, store) do
    Ops.compound_get(store, key, CompoundKey.type_key(key)) != nil
  end

  # Validates that a binary is the right size for an HLL sketch.
  # Protects against corrupted or non-HLL values being used with HLL commands.
  @spec validate_sketch(binary() | {:error, binary()}) :: :ok | {:error, binary()}
  defp validate_sketch({:error, _} = err), do: err

  defp validate_sketch(sketch) do
    if HLL.valid_sketch?(sketch) do
      :ok
    else
      @wrongtype_error
    end
  end

  defp validate_sketches(sketches) do
    Enum.reduce_while(sketches, {:ok, []}, fn sketch, {:ok, acc} ->
      case validate_sketch(sketch) do
        :ok -> {:cont, {:ok, [sketch | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = err -> err
    end
  end
end
