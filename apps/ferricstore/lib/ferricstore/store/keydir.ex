defmodule Ferricstore.Store.Keydir do
  @moduledoc false

  @type entry :: {binary(), term(), term(), term(), term(), term(), term()}

  @spec delete_exact(:ets.tid() | atom(), entry()) :: boolean()
  def delete_exact(table, {key, value, expire_at_ms, lfu, file_id, offset, value_size}) do
    :ets.select_delete(table, [
      {
        {key, :"$1", :"$2", :"$3", :"$4", :"$5", :"$6"},
        exact_entry_guards(value, expire_at_ms, lfu, file_id, offset, value_size),
        [true]
      }
    ]) == 1
  rescue
    ArgumentError -> false
  end

  @spec delete_unchanged_source(:ets.tid() | atom(), entry()) :: boolean()
  # LFU's packed counter is mutable; tagged flow-state version and all other
  # source fields remain part of the identity checked by this delete.
  def delete_unchanged_source(table, row) do
    :ets.select_delete(table, unchanged_source_match_spec(row)) == 1
  rescue
    ArgumentError -> false
  end

  @spec replace_exact(:ets.tid() | atom(), entry(), entry()) :: boolean()
  def replace_exact(
        table,
        {key, value, expire_at_ms, lfu, file_id, offset, value_size},
        {key, _new_value, _new_expire_at_ms, _new_lfu, _new_file_id, _new_offset, _new_value_size} =
          replacement
      ) do
    :ets.select_replace(table, [
      {
        {key, :"$1", :"$2", :"$3", :"$4", :"$5", :"$6"},
        exact_entry_guards(value, expire_at_ms, lfu, file_id, offset, value_size),
        [{:const, replacement}]
      }
    ]) == 1
  rescue
    ArgumentError -> false
  end

  @spec relocate_exact(
          :ets.tid() | atom(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: boolean()
  def relocate_exact(table, key, file_id, old_offset, new_offset) do
    :ets.select_replace(table, [
      {
        {key, :"$1", :"$2", :"$3", :"$4", :"$5", :"$6"},
        [
          {:"=:=", :"$4", {:const, file_id}},
          {:"=:=", :"$5", {:const, old_offset}}
        ],
        [{{key, :"$1", :"$2", :"$3", :"$4", new_offset, :"$6"}}]
      }
    ]) == 1
  rescue
    ArgumentError -> false
  end

  defp exact_entry_guards(value, expire_at_ms, lfu, file_id, offset, value_size) do
    [
      {:"=:=", :"$1", {:const, value}},
      {:"=:=", :"$2", {:const, expire_at_ms}},
      {:"=:=", :"$3", {:const, lfu}},
      {:"=:=", :"$4", {:const, file_id}},
      {:"=:=", :"$5", {:const, offset}},
      {:"=:=", :"$6", {:const, value_size}}
    ]
  end

  defp unchanged_source_match_spec({key, value, expire_at_ms, lfu, file_id, offset, value_size})
       when is_integer(lfu) and lfu >= 0 do
    [
      {
        {key, :"$1", :"$2", :"$3", :"$4", :"$5", :"$6"},
        unchanged_source_guards(value, expire_at_ms, file_id, offset, value_size) ++
          [{:is_integer, :"$3"}, {:>=, :"$3", 0}],
        [true]
      }
    ]
  end

  defp unchanged_source_match_spec(
         {key, value, expire_at_ms, {:flow_state_version, version, packed}, file_id, offset,
          value_size}
       )
       when is_integer(version) and is_integer(packed) and packed >= 0 do
    [
      {
        {key, :"$1", :"$2", {:flow_state_version, version, :"$3"}, :"$4", :"$5", :"$6"},
        unchanged_source_guards(value, expire_at_ms, file_id, offset, value_size) ++
          [{:is_integer, :"$3"}, {:>=, :"$3", 0}],
        [true]
      }
    ]
  end

  defp unchanged_source_match_spec({key, value, expire_at_ms, lfu, file_id, offset, value_size}) do
    [
      {
        {key, :"$1", :"$2", :"$3", :"$4", :"$5", :"$6"},
        unchanged_source_guards(value, expire_at_ms, file_id, offset, value_size) ++
          [{:"=:=", :"$3", {:const, lfu}}],
        [true]
      }
    ]
  end

  defp unchanged_source_guards(value, expire_at_ms, file_id, offset, value_size) do
    [
      {:"=:=", :"$1", {:const, value}},
      {:"=:=", :"$2", {:const, expire_at_ms}},
      {:"=:=", :"$4", {:const, file_id}},
      {:"=:=", :"$5", {:const, offset}},
      {:"=:=", :"$6", {:const, value_size}}
    ]
  end
end
