defmodule Ferricstore.Store.Shard.CompoundMemberIndex do
  @moduledoc false

  alias Ferricstore.HLC
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @separator <<0>>

  @type table_ref :: atom() | :ets.tid() | nil

  @spec table_name(atom() | binary(), non_neg_integer()) :: atom()
  def table_name(instance_name, shard_index),
    do: :"ferricstore_compound_member_index_#{instance_name}_#{shard_index}"

  @spec ensure_table!(atom()) :: atom()
  def ensure_table!(table_name) when is_atom(table_name) do
    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [
          :ordered_set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto}
        ])

      _tid ->
        table_name
    end
  end

  @spec reset(table_ref()) :: :ok
  def reset(nil), do: :ok

  def reset(table) do
    case table_ref(table) do
      :undefined -> :ok
      tid -> :ets.delete_all_objects(tid)
    end
  end

  @spec rebuild(table_ref(), :ets.tid() | atom()) :: :ok
  def rebuild(table, keydir) do
    case table_ref(table) do
      :undefined ->
        :ok

      index ->
        :ets.delete_all_objects(index)
        now = HLC.now_ms()

        :ets.foldl(
          fn
            {key, value, expire_at_ms, _lfu, file_id, _offset, _value_size}, :ok
            when is_binary(key) ->
              if live_keydir_entry?(value, expire_at_ms, file_id, now) do
                put(index, key)
              end

              :ok

            _row, :ok ->
              :ok
          end,
          :ok,
          keydir
        )
    end
  end

  @spec put(table_ref(), binary()) :: :ok
  def put(nil, _compound_key), do: :ok

  def put(table, <<"H:", _rest::binary>> = compound_key),
    do: put_compound_member(table, compound_key)

  def put(table, <<"L:", _rest::binary>> = compound_key),
    do: put_compound_member(table, compound_key)

  def put(table, <<"S:", _rest::binary>> = compound_key),
    do: put_compound_member(table, compound_key)

  def put(table, <<"Z:", _rest::binary>> = compound_key),
    do: put_compound_member(table, compound_key)

  def put(table, <<"XG:", _rest::binary>> = compound_key),
    do: put_compound_member(table, compound_key)

  def put(table, <<"XM:", _rest::binary>> = compound_key),
    do: put_compound_member(table, compound_key)

  def put(table, <<"X:", _rest::binary>> = compound_key),
    do: put_compound_member(table, compound_key)

  def put(_table, _compound_key), do: :ok

  defp put_compound_member(table, compound_key) do
    with tid when tid != :undefined <- writable_table_ref(table),
         {:ok, prefix, member} <- split_at_separator(compound_key) do
      :ets.insert(tid, {{prefix, member}, compound_key})
    end

    :ok
  end

  @spec delete(table_ref(), binary()) :: :ok
  def delete(nil, _compound_key), do: :ok

  def delete(table, <<"H:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"L:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"S:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"Z:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"XG:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"XM:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"X:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(_table, _compound_key), do: :ok

  defp delete_compound_member(table, compound_key) do
    with tid when tid != :undefined <- table_ref(table),
         {:ok, prefix, member} <- split_at_separator(compound_key) do
      :ets.delete(tid, {prefix, member})
    end

    :ok
  end

  @spec delete_prefix(table_ref(), binary()) :: :ok
  def delete_prefix(nil, _prefix), do: :ok

  def delete_prefix(table, prefix) when is_binary(prefix) do
    with tid when tid != :undefined <- table_ref(table) do
      tid
      |> scan_index_keys(prefix)
      |> Enum.each(&:ets.delete(tid, &1))
    end

    :ok
  end

  def delete_prefix(_table, _prefix), do: :ok

  @spec any_live?(table_ref(), map(), binary()) :: boolean() | :unavailable
  def any_live?(table, state, prefix) when is_binary(prefix) do
    any_live?(table, state, prefix, %{})
  end

  def any_live?(_table, _state, _prefix), do: :unavailable

  @spec any_live?(table_ref(), map(), binary(), map() | MapSet.t()) :: boolean() | :unavailable
  def any_live?(table, state, prefix, ignored_keys) when is_binary(prefix) do
    case table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        do_any_live?(tid, lookup_state(state), prefix, first_key(tid, prefix), ignored_keys)
    end
  end

  def any_live?(_table, _state, _prefix, _ignored_keys), do: :unavailable

  @spec scan_entries(table_ref(), map(), binary()) :: {:ok, [{binary(), binary()}]} | :unavailable
  def scan_entries(table, state, prefix) when is_binary(prefix) do
    case table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        lookup_state = lookup_state(state)

        entries =
          tid
          |> scan_keys(prefix)
          |> Enum.reduce_while([], fn compound_key, acc ->
            case ShardETS.ets_lookup(lookup_state, compound_key) do
              {:hit, value, _expire_at_ms} when is_binary(value) ->
                {:cont, [{member_from_key(compound_key, prefix), value} | acc]}

              {:hit, value, _expire_at_ms} ->
                {:cont, [{member_from_key(compound_key, prefix), to_string(value)} | acc]}

              {:cold, _fid, _off, _vsize, _expire_at_ms} ->
                {:halt, :unavailable}

              :expired ->
                delete(tid, compound_key)
                {:cont, acc}

              :miss ->
                delete(tid, compound_key)
                {:cont, acc}
            end
          end)

        case entries do
          :unavailable -> :unavailable
          entries -> {:ok, Enum.reverse(entries)}
        end
    end
  end

  def scan_entries(_table, _state, _prefix), do: :unavailable

  defp table_ref(nil), do: :undefined
  defp table_ref(table) when is_reference(table), do: table

  defp table_ref(table) when is_atom(table) do
    case :ets.whereis(table) do
      :undefined -> :undefined
      tid -> tid
    end
  end

  defp writable_table_ref(table) when is_atom(table) do
    ensure_table!(table)
    table_ref(table)
  end

  defp writable_table_ref(table), do: table_ref(table)

  defp live_keydir_entry?(_value, expire_at_ms, _file_id, now)
       when is_integer(expire_at_ms) and expire_at_ms != 0 and expire_at_ms <= now,
       do: false

  defp live_keydir_entry?(nil, _expire_at_ms, :pending, _now), do: false
  defp live_keydir_entry?(_value, _expire_at_ms, _file_id, _now), do: true

  defp split_at_separator(key) do
    case :binary.match(key, @separator) do
      {pos, 1} ->
        prefix = binary_part(key, 0, pos + 1)
        member = binary_part(key, pos + 1, byte_size(key) - pos - 1)
        {:ok, prefix, member}

      :nomatch ->
        :ignore
    end
  end

  defp scan_keys(table, prefix) do
    first = first_key(table, prefix)
    do_scan_keys(table, prefix, first, [])
  end

  defp scan_index_keys(table, prefix) do
    first = first_key(table, prefix)
    do_scan_index_keys(table, prefix, first, [])
  end

  defp first_key(table, prefix) do
    case :ets.lookup(table, {prefix, <<>>}) do
      [{{^prefix, <<>>}, _compound_key}] -> {prefix, <<>>}
      _ -> :ets.next(table, {prefix, <<>>})
    end
  end

  defp do_scan_keys(_table, _prefix, :"$end_of_table", acc), do: Enum.reverse(acc)

  defp do_scan_keys(table, prefix, {prefix, _member} = index_key, acc) do
    acc =
      case :ets.lookup(table, index_key) do
        [{^index_key, compound_key}] -> [compound_key | acc]
        _missing -> acc
      end

    do_scan_keys(table, prefix, :ets.next(table, index_key), acc)
  end

  defp do_scan_keys(_table, _prefix, _other_key, acc), do: Enum.reverse(acc)

  defp do_scan_index_keys(_table, _prefix, :"$end_of_table", acc), do: Enum.reverse(acc)

  defp do_scan_index_keys(table, prefix, {prefix, _member} = index_key, acc),
    do: do_scan_index_keys(table, prefix, :ets.next(table, index_key), [index_key | acc])

  defp do_scan_index_keys(_table, _prefix, _other_key, acc), do: Enum.reverse(acc)

  defp do_any_live?(_table, _state, _prefix, :"$end_of_table", _ignored_keys), do: false

  defp do_any_live?(table, state, prefix, {prefix, _member} = index_key, ignored_keys) do
    next_key = :ets.next(table, index_key)

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        if ignored_key?(ignored_keys, compound_key) do
          do_any_live?(table, state, prefix, next_key, ignored_keys)
        else
          case ShardETS.ets_lookup_warm(state, compound_key) do
            {:hit, _value, _expire_at_ms} ->
              true

            :expired ->
              delete(table, compound_key)
              do_any_live?(table, state, prefix, next_key, ignored_keys)

            :miss ->
              delete(table, compound_key)
              do_any_live?(table, state, prefix, next_key, ignored_keys)
          end
        end

      _missing ->
        do_any_live?(table, state, prefix, next_key, ignored_keys)
    end
  end

  defp do_any_live?(_table, _state, _prefix, _other_key, _ignored_keys), do: false

  defp ignored_key?(%MapSet{} = ignored_keys, compound_key),
    do: MapSet.member?(ignored_keys, compound_key)

  defp ignored_key?(ignored_keys, compound_key) when is_map(ignored_keys),
    do: Map.get(ignored_keys, compound_key) == :deleted

  defp ignored_key?(_ignored_keys, _compound_key), do: false

  defp member_from_key(compound_key, prefix) do
    prefix_len = byte_size(prefix)
    binary_part(compound_key, prefix_len, byte_size(compound_key) - prefix_len)
  end

  defp lookup_state(%{keydir: _keydir} = state), do: state
  defp lookup_state(%{ets: ets} = state), do: Map.put(state, :keydir, ets)
end
