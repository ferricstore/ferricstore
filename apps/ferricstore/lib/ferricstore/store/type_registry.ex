defmodule Ferricstore.Store.TypeRegistry do
  @moduledoc """
  Manages type metadata for Redis keys that hold data structures.

  When a key is first used as a hash, list, set, or sorted set, a type
  metadata entry is written to the same Bitcask shard:

      T:keyname -> "hash" | "list" | "set" | "zset"

  Subsequent commands check this metadata and return a WRONGTYPE error if
  the command's expected type does not match. String keys do NOT get a type
  entry -- only data structure keys do. This matches Redis behavior where
  string operations on a data structure key return WRONGTYPE.

  ## Type Enforcement

  The `check_or_set/3` function either:
  1. Sets the type if the key has no type yet (first use)
  2. Returns `:ok` if the key already has the expected type
  3. Returns `{:error, wrongtype_message}` if the type mismatches
  """

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops

  @wrongtype_msg "WRONGTYPE Operation against a key holding the wrong kind of value"

  @doc """
  Checks that `redis_key` has the expected `type`, or sets it if the key
  has no type metadata yet.

  ## Parameters

    - `redis_key` - the Redis key to check
    - `type` - the expected data type (`:hash`, `:list`, `:set`, `:zset`)
    - `store` - the store (Instance, LocalTxStore, or closure map)

  ## Returns

    - `:ok` if the type matches or was newly set
    - `{:error, wrongtype_message}` if the type mismatches
  """
  @spec check_or_set(binary(), CompoundKey.data_type(), map()) :: :ok | {:error, binary()}
  def check_or_set(redis_key, type, store) do
    type_key = CompoundKey.type_key(redis_key)
    expected = CompoundKey.encode_type(type)

    case Ops.compound_get(store, redis_key, type_key) do
      nil ->
        # No type metadata. If the key exists as a plain string, reject.
        if has_exists?(store) and Ops.exists?(store, redis_key) do
          {:error, @wrongtype_msg}
        else
          Ops.compound_put(store, redis_key, type_key, expected, 0)
          :ok
        end

      ^expected ->
        :ok

      _other_type ->
        case get_type(redis_key, store) do
          "none" ->
            Ops.compound_put(store, redis_key, type_key, expected, 0)
            :ok

          _live_type ->
            {:error, @wrongtype_msg}
        end
    end
  end

  @doc """
  Returns the type of a Redis key, or `nil` if no type metadata exists.

  Used by the TYPE command.

  ## Parameters

    - `redis_key` - the Redis key to look up
    - `store` - the store (Instance, LocalTxStore, or closure map)

  ## Returns

    - `"hash"`, `"list"`, `"set"`, `"zset"`, `"string"`, or `"none"`
  """
  @spec get_type(binary(), map()) :: binary()
  def get_type(redis_key, store) do
    # Check compound key type registry first (for hash/set/zset)
    type_key = CompoundKey.type_key(redis_key)

    case Ops.compound_get(store, redis_key, type_key) do
      nil ->
        case Ops.get(store, redis_key) do
          nil -> "none"
          value when is_binary(value) -> "string"
          _ -> "string"
        end

      type_str ->
        live_type_or_none(redis_key, type_str, store)
    end
  end

  defp live_type_or_none(redis_key, type_str, store) do
    if live_compound_type?(redis_key, type_str, store) or Ops.exists?(store, redis_key) do
      type_str
    else
      delete_type(redis_key, store)
      "none"
    end
  end

  defp live_compound_type?(redis_key, "hash", store),
    do: Ops.compound_count(store, redis_key, CompoundKey.hash_prefix(redis_key)) > 0

  defp live_compound_type?(redis_key, "list", store) do
    Ops.compound_get(store, redis_key, CompoundKey.list_meta_key(redis_key)) != nil and
      Ops.compound_count(store, redis_key, CompoundKey.list_prefix(redis_key)) > 0
  end

  defp live_compound_type?(redis_key, "set", store),
    do: Ops.compound_count(store, redis_key, CompoundKey.set_prefix(redis_key)) > 0

  defp live_compound_type?(redis_key, "zset", store),
    do: Ops.compound_count(store, redis_key, CompoundKey.zset_prefix(redis_key)) > 0

  defp live_compound_type?(_redis_key, _type_str, _store), do: true

  @doc """
  Removes the type metadata for a Redis key.

  Called when DEL removes a data structure key.

  ## Parameters

    - `redis_key` - the Redis key whose type to remove
    - `store` - the store (Instance, LocalTxStore, or closure map)
  """
  @spec delete_type(binary(), map()) :: :ok
  def delete_type(redis_key, store) do
    type_key = CompoundKey.type_key(redis_key)
    Ops.compound_delete(store, redis_key, type_key)
  end

  @doc """
  Checks that a key either does not exist or has the expected type,
  WITHOUT setting the type if it doesn't exist. Used for read-only
  operations that should not create keys.

  ## Returns

    - `:ok` if the key doesn't exist or has the expected type
    - `{:error, wrongtype_message}` if the type mismatches
  """
  @spec check_type(binary(), CompoundKey.data_type(), map()) :: :ok | {:error, binary()}
  def check_type(redis_key, type, store) do
    type_key = CompoundKey.type_key(redis_key)
    expected = CompoundKey.encode_type(type)

    case Ops.compound_get(store, redis_key, type_key) do
      nil ->
        # No type metadata. Check if the key exists as a plain string --
        # if so, it is a type mismatch for data structure commands.
        if has_exists?(store) do
          if Ops.exists?(store, redis_key), do: {:error, @wrongtype_msg}, else: :ok
        else
          :ok
        end

      ^expected ->
        :ok

      _other_type ->
        case get_type(redis_key, store) do
          "none" -> :ok
          _live_type -> {:error, @wrongtype_msg}
        end
    end
  end

  # Check if the store supports `exists?` — closure maps may omit it.
  defp has_exists?(%FerricStore.Instance{}), do: true
  defp has_exists?(%Ferricstore.Store.LocalTxStore{}), do: true
  defp has_exists?(store) when is_map(store), do: is_map_key(store, :exists?)
end
