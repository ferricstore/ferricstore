defmodule Ferricstore.Store.PipelinePlanner do
  @moduledoc """
  Core-owned pipeline routing precompute.

  Server/protocol code may know command shape, but routing decisions belong in
  the store layer. The planner keeps namespace expansion, shard ownership, and
  keydir lookup together so batch read/write paths can avoid recomputing them
  per Router pass without moving store semantics into the RESP parser.
  """

  alias Ferricstore.Store.SlotMap

  @type key_plan :: {binary(), binary(), non_neg_integer(), atom() | reference()}

  @doc """
  Plans keys that are already in their stored lookup form.
  """
  @spec plan_lookup_keys(FerricStore.Instance.t(), [binary()]) :: [key_plan()]
  def plan_lookup_keys(ctx, keys) when is_list(keys) do
    slot_map = ctx.slot_map
    keydir_refs = ctx.keydir_refs

    Enum.map(keys, fn key ->
      plan_entry(slot_map, keydir_refs, key, key)
    end)
  end

  @doc """
  Plans client-visible keys and optional sandbox namespace expansion.

  The original key is retained for tracking/replies; the lookup key is the
  stored key used for routing and ETS/Bitcask access.
  """
  @spec plan_keys(FerricStore.Instance.t(), [binary()], binary() | nil) :: [key_plan()]
  def plan_keys(ctx, keys, nil), do: plan_lookup_keys(ctx, keys)

  def plan_keys(ctx, keys, namespace) when is_list(keys) and is_binary(namespace) do
    slot_map = ctx.slot_map
    keydir_refs = ctx.keydir_refs

    Enum.map(keys, fn key ->
      plan_entry(slot_map, keydir_refs, key, namespace <> key)
    end)
  end

  @doc false
  @spec original_key(key_plan()) :: binary()
  def original_key({key, _lookup_key, _shard_index, _keydir}), do: key

  @doc false
  @spec lookup_key(key_plan()) :: binary()
  def lookup_key({_key, lookup_key, _shard_index, _keydir}), do: lookup_key

  @doc false
  @spec shard_index(key_plan()) :: non_neg_integer()
  def shard_index({_key, _lookup_key, shard_index, _keydir}), do: shard_index

  @doc false
  @spec keydir(key_plan()) :: atom() | reference()
  def keydir({_key, _lookup_key, _shard_index, keydir}), do: keydir

  @doc false
  @spec original_keys([key_plan()]) :: [binary()]
  def original_keys(plan), do: Enum.map(plan, &original_key/1)

  @doc false
  @spec lookup_keys([key_plan()]) :: [binary()]
  def lookup_keys(plan), do: Enum.map(plan, &lookup_key/1)

  defp plan_entry(slot_map, keydir_refs, original_key, lookup_key) do
    slot = SlotMap.slot_for_key(lookup_key)
    shard_index = SlotMap.shard_for_slot(slot_map, slot)

    {original_key, lookup_key, shard_index, elem(keydir_refs, shard_index)}
  end
end
