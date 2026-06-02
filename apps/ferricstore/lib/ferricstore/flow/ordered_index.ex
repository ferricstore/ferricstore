defmodule Ferricstore.Flow.OrderedIndex do
  @moduledoc """
  Compatibility facade for the Flow ordered index.

  Flow no longer keeps an ETS mirror for secondary indexes. This module keeps
  the older call shape alive for tests and any internal callers that have not
  been renamed yet, but all data lives in `Ferricstore.Flow.NativeOrderedIndex`.
  """

  alias Ferricstore.Flow.NativeOrderedIndex

  @type score_input :: NativeOrderedIndex.score_input()
  @type table_ref :: atom() | reference()

  @spec table_names(atom(), non_neg_integer()) :: {atom(), atom()}
  def table_names(instance_name, shard_index),
    do: NativeOrderedIndex.table_names(instance_name, shard_index)

  @spec put_member(table_ref(), table_ref(), binary(), binary(), score_input()) :: :ok
  def put_member(index_table, lookup_table, key, member, score_input) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.put_member(key, member, score_input)
  end

  @spec put_new_member(table_ref(), table_ref(), binary(), binary(), score_input()) :: :ok
  def put_new_member(index_table, lookup_table, key, member, score_input) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.put_new_member(key, member, score_input)
  end

  @spec put_members(table_ref(), table_ref(), binary(), [{binary(), score_input()}]) :: :ok
  def put_members(index_table, lookup_table, key, member_score_pairs) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.put_members(key, member_score_pairs)
  end

  @spec put_entries(table_ref(), table_ref(), [{binary(), binary(), score_input()}]) :: :ok
  def put_entries(index_table, lookup_table, key_member_score_triples) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.put_entries(key_member_score_triples)
  end

  @spec put_new_entries(table_ref(), table_ref(), [{binary(), binary(), score_input()}]) :: :ok
  def put_new_entries(index_table, lookup_table, key_member_score_triples) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.put_new_entries(key_member_score_triples)
  end

  @spec put_new_members(table_ref(), table_ref(), binary(), [{binary(), score_input()}]) :: :ok
  def put_new_members(index_table, lookup_table, key, member_score_pairs) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.put_new_members(key, member_score_pairs)
  end

  @spec move_entries(table_ref(), table_ref(), [{binary(), binary(), binary(), score_input()}]) ::
          :ok
  def move_entries(index_table, lookup_table, key_key_member_score_quads) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.move_entries(key_key_member_score_quads)
  end

  @spec delete_member(table_ref(), table_ref(), binary(), binary()) :: :ok
  def delete_member(index_table, lookup_table, key, member) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.delete_member(key, member)
  end

  @spec delete_members(table_ref(), table_ref(), binary(), [binary()]) :: :ok
  def delete_members(index_table, lookup_table, key, members) do
    index_table
    |> resource!(lookup_table)
    |> NativeOrderedIndex.delete_members(key, members)
  end

  @spec score_of(table_ref(), binary(), binary()) :: {:ok, float()} | :miss
  def score_of(lookup_table, key, member) do
    lookup_table
    |> resource_from_lookup!()
    |> NativeOrderedIndex.score_of(key, member)
  end

  @spec range_slice(
          table_ref(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) :: [{binary(), float()}]
  def range_slice(index_table, key, min_bound, max_bound, reverse?, offset, count) do
    index_table
    |> resource_from_index!()
    |> NativeOrderedIndex.range_slice(key, min_bound, max_bound, reverse?, offset, count)
  end

  @spec rank_range(table_ref(), binary(), non_neg_integer(), non_neg_integer(), boolean()) :: [
          {binary(), float()}
        ]
  def rank_range(index_table, key, start_idx, stop_idx, reverse?) do
    index_table
    |> resource_from_index!()
    |> NativeOrderedIndex.rank_range(key, start_idx, stop_idx, reverse?)
  end

  @spec count_all(table_ref(), binary()) :: non_neg_integer()
  def count_all(lookup_table, key) do
    lookup_table
    |> resource_from_lookup!()
    |> NativeOrderedIndex.count_all(key)
  end

  @spec count_keys(table_ref()) :: [binary()]
  def count_keys(lookup_table) do
    lookup_table
    |> resource_from_lookup!()
    |> NativeOrderedIndex.count_keys()
  end

  @spec due_count_keys(table_ref()) :: [binary()]
  def due_count_keys(lookup_table) do
    lookup_table
    |> resource_from_lookup!()
    |> NativeOrderedIndex.due_count_keys()
  end

  @spec restore_count(table_ref(), binary(), integer()) :: :ok
  def restore_count(lookup_table, key, count) do
    lookup_table
    |> resource_from_lookup!()
    |> NativeOrderedIndex.restore_count(key, count)
  end

  @spec delete_count(table_ref(), binary()) :: :ok
  def delete_count(lookup_table, key) do
    lookup_table
    |> resource_from_lookup!()
    |> NativeOrderedIndex.delete_count(key)
  end

  defp resource_from_index!(index_table),
    do: resource!(index_table, lookup_table_name(index_table))

  defp resource_from_lookup!(lookup_table),
    do: resource!(index_table_name(lookup_table), lookup_table)

  defp resource!(index_table, lookup_table) do
    case NativeOrderedIndex.get(index_table, lookup_table) do
      nil ->
        resource = NativeOrderedIndex.new()
        NativeOrderedIndex.register(index_table, lookup_table, resource)
        resource

      resource ->
        resource
    end
  end

  defp lookup_table_name(index_table) when is_atom(index_table) do
    index_table
    |> Atom.to_string()
    |> String.replace("ferricstore_flow_index_", "ferricstore_flow_lookup_", global: false)
    |> String.to_atom()
  end

  defp lookup_table_name(index_table), do: index_table

  defp index_table_name(lookup_table) when is_atom(lookup_table) do
    lookup_table
    |> Atom.to_string()
    |> String.replace("ferricstore_flow_lookup_", "ferricstore_flow_index_", global: false)
    |> String.to_atom()
  end

  defp index_table_name(lookup_table), do: lookup_table
end
