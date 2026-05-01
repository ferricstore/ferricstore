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

  def handle("PFADD", [key], store) do
    case ensure_not_compound_key(key, store) do
      :ok ->
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

      @wrongtype_error ->
        @wrongtype_error
    end
  end

  def handle("PFADD", [key | elements], store) when elements != [] do
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

  def handle("PFADD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pfadd' command"}
  end

  # ---------------------------------------------------------------------------
  # PFCOUNT key [key ...]
  # ---------------------------------------------------------------------------

  def handle("PFCOUNT", keys, store) when keys != [] do
    sketches =
      Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
        sketch = get_or_new(key, store)

        case validate_sketch(sketch) do
          :ok -> {:cont, {:ok, [sketch | acc]}}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case sketches do
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

  def handle("PFCOUNT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pfcount' command"}
  end

  # ---------------------------------------------------------------------------
  # PFMERGE destkey sourcekey [sourcekey ...]
  # ---------------------------------------------------------------------------

  def handle("PFMERGE", [destkey | source_keys], store) when source_keys != [] do
    # Start with the destination's existing sketch (or empty if absent)
    dest_sketch = get_or_new(destkey, store)

    result =
      Enum.reduce_while(
        [dest_sketch | Enum.map(source_keys, &get_or_new(&1, store))],
        {:ok, nil},
        fn
          sketch, {:ok, nil} ->
            case validate_sketch(sketch) do
              :ok -> {:cont, {:ok, sketch}}
              {:error, _} = err -> {:halt, err}
            end

          sketch, {:ok, acc} ->
            case validate_sketch(sketch) do
              :ok -> {:cont, {:ok, HLL.merge(acc, sketch)}}
              {:error, _} = err -> {:halt, err}
            end
        end
      )

    case result do
      {:ok, merged} ->
        Ops.put(store, destkey, merged, 0)
        :ok

      {:error, _} = err ->
        err
    end
  end

  def handle("PFMERGE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pfmerge' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Returns the existing sketch for `key`, or a new empty sketch if the key
  # does not exist.
  @spec get_or_new(binary(), map()) :: binary()
  defp get_or_new(key, store) do
    case ensure_not_compound_key(key, store) do
      :ok ->
        case Ops.get(store, key) do
          nil -> HLL.new()
          value -> value
        end

      @wrongtype_error ->
        @wrongtype_error
    end
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
end
