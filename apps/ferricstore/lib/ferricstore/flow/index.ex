defmodule Ferricstore.Flow.Index do
  @moduledoc false

  @type score_input :: Ferricstore.Flow.OrderedIndex.score_input()

  defdelegate table_names(instance_name, shard_index), to: Ferricstore.Flow.OrderedIndex

  defdelegate put_member(index_table, lookup_table, key, member, score_input),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate put_new_member(index_table, lookup_table, key, member, score_input),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate put_members(index_table, lookup_table, key, member_score_pairs),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate put_new_members(index_table, lookup_table, key, member_score_pairs),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate put_entries(index_table, lookup_table, key_member_score_triples),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate put_new_entries(index_table, lookup_table, key_member_score_triples),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate move_entries(index_table, lookup_table, key_key_member_score_quads),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate delete_member(index_table, lookup_table, key, member),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate delete_members(index_table, lookup_table, key, members),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate score_of(lookup_table, key, member), to: Ferricstore.Flow.OrderedIndex

  defdelegate range_slice(index_table, key, min_bound, max_bound, reverse?, offset, count),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate rank_range(index_table, key, start_idx, stop_idx, reverse?),
    to: Ferricstore.Flow.OrderedIndex

  defdelegate count_all(lookup_table, key), to: Ferricstore.Flow.OrderedIndex
end
