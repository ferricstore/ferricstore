defmodule Ferricstore.Flow.NativeOrderedIndex do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @registry_key {__MODULE__, :resource}

  @type resource :: reference()
  @type score_input :: binary() | integer() | float()

  @spec table_names(atom(), non_neg_integer()) :: {atom(), atom()}
  def table_names(instance_name, shard_index) do
    {
      :"ferricstore_flow_index_#{instance_name}_#{shard_index}",
      :"ferricstore_flow_lookup_#{instance_name}_#{shard_index}"
    }
  end

  @spec new() :: resource()
  def new, do: NIF.flow_index_new()

  @type index_name :: atom() | reference()

  @spec register(index_name(), index_name(), resource()) :: :ok
  def register(index_table, lookup_table, resource) do
    :persistent_term.put(registry_key(index_table, lookup_table), resource)
    :ok
  end

  @spec reset(index_name(), index_name()) :: resource()
  def reset(index_table, lookup_table) do
    resource = new()
    register(index_table, lookup_table, resource)
    resource
  end

  @spec reset_shard(atom(), non_neg_integer()) :: resource()
  def reset_shard(instance_name, shard_index) do
    {index_table, lookup_table} = table_names(instance_name, shard_index)
    reset(index_table, lookup_table)
  end

  @spec reset_all(atom(), non_neg_integer()) :: :ok
  def reset_all(instance_name, shard_count) when is_integer(shard_count) and shard_count >= 0 do
    if shard_count > 0 do
      Enum.each(0..(shard_count - 1), &reset_shard(instance_name, &1))
    end

    :ok
  end

  @spec get(index_name(), index_name()) :: resource() | nil
  def get(index_table, lookup_table) do
    :persistent_term.get(registry_key(index_table, lookup_table), nil)
  end

  @spec put_member(resource(), binary(), binary(), score_input()) :: :ok
  def put_member(resource, key, member, score_input) do
    put_members(resource, key, [{member, score_input}])
  end

  @spec put_new_member(resource(), binary(), binary(), score_input()) :: :ok
  def put_new_member(resource, key, member, score_input) do
    put_new_members(resource, key, [{member, score_input}])
  end

  @spec put_members(resource(), binary(), [{binary(), score_input()}]) :: :ok
  def put_members(resource, key, member_score_pairs) do
    entries =
      Enum.flat_map(member_score_pairs, fn {member, score_input} ->
        case parse_score(score_input) do
          {:ok, score} -> [{key, member, score}]
          :error -> []
        end
      end)

    put_entries(resource, entries)
  end

  @spec put_new_members(resource(), binary(), [{binary(), score_input()}]) :: :ok
  def put_new_members(resource, key, member_score_pairs) do
    entries =
      Enum.flat_map(member_score_pairs, fn {member, score_input} ->
        case parse_score(score_input) do
          {:ok, score} -> [{key, member, score}]
          :error -> []
        end
      end)

    put_new_entries(resource, entries)
  end

  @spec put_entries(resource(), [{binary(), binary(), score_input()}]) :: :ok
  def put_entries(_resource, []), do: :ok

  def put_entries(resource, entries) do
    NIF.flow_index_put_entries(resource, parse_entries(entries))
  end

  @spec put_new_entries(resource(), [{binary(), binary(), score_input()}]) :: :ok
  def put_new_entries(_resource, []), do: :ok

  def put_new_entries(resource, entries) do
    NIF.flow_index_put_new_entries(resource, parse_entries(entries))
  end

  @spec move_entries(resource(), [{binary(), binary(), binary(), score_input()}]) :: :ok
  def move_entries(_resource, []), do: :ok

  def move_entries(resource, entries) do
    native_entries =
      Enum.flat_map(entries, fn {from_key, to_key, member, score_input} ->
        case parse_score(score_input) do
          {:ok, score} -> [{from_key, to_key, member, score}]
          :error -> []
        end
      end)

    NIF.flow_index_move_entries(resource, native_entries)
  end

  @spec delete_member(resource(), binary(), binary()) :: :ok
  def delete_member(resource, key, member), do: delete_members(resource, key, [member])

  @spec delete_members(resource(), binary(), [binary()]) :: :ok
  def delete_members(_resource, _key, []), do: :ok

  def delete_members(resource, key, members) do
    NIF.flow_index_delete_members(resource, key, members)
  end

  @spec delete_entries(resource(), [{binary(), binary()}]) :: :ok
  def delete_entries(_resource, []), do: :ok

  def delete_entries(resource, entries) do
    NIF.flow_index_delete_entries(resource, entries)
  end

  @spec score_of(resource(), binary(), binary()) :: {:ok, float()} | :miss
  def score_of(resource, key, member), do: NIF.flow_index_score_of(resource, key, member)

  @spec range_slice(
          resource(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) :: [{binary(), float()}]
  def range_slice(_resource, _key, _min_bound, _max_bound, _reverse?, _offset, 0), do: []

  def range_slice(resource, key, min_bound, max_bound, reverse?, offset, count) do
    {min_kind, min_score} = encode_min_bound(min_bound)
    {max_kind, max_score} = encode_max_bound(max_bound)
    count_arg = if count == :all, do: -1, else: count

    NIF.flow_index_range_slice(
      resource,
      key,
      min_kind,
      min_score,
      max_kind,
      max_score,
      reverse?,
      offset,
      count_arg
    )
  end

  @spec take_due(resource(), binary(), score_input(), non_neg_integer()) :: [{binary(), float()}]
  def take_due(_resource, _key, _now_score, 0), do: []

  def take_due(resource, key, now_score, count) do
    case parse_score(now_score) do
      {:ok, score} -> NIF.flow_index_take_due(resource, key, score, count)
      :error -> []
    end
  end

  @spec rank_range(resource(), binary(), non_neg_integer(), non_neg_integer(), boolean()) :: [
          {binary(), float()}
        ]
  def rank_range(_resource, _key, start_idx, stop_idx, _reverse?) when start_idx > stop_idx,
    do: []

  def rank_range(resource, key, start_idx, stop_idx, reverse?) do
    range_slice(resource, key, :neg_inf, :inf, reverse?, start_idx, stop_idx - start_idx + 1)
  end

  @spec count_all(resource(), binary()) :: non_neg_integer()
  def count_all(resource, key), do: max(NIF.flow_index_count_all(resource, key), 0)

  @spec count_many(resource(), [binary()]) :: [non_neg_integer()]
  def count_many(_resource, []), do: []

  def count_many(resource, keys) when is_list(keys) do
    resource
    |> NIF.flow_index_count_many(keys)
    |> Enum.map(&max(&1, 0))
  end

  @spec count_keys(resource()) :: [binary()]
  def count_keys(resource), do: NIF.flow_index_count_keys(resource)

  @spec due_count_keys(resource()) :: [binary()]
  def due_count_keys(resource), do: NIF.flow_index_due_count_keys(resource)

  @spec restore_count(resource(), binary(), integer()) :: :ok
  def restore_count(resource, key, count), do: NIF.flow_index_restore_count(resource, key, count)

  @spec delete_count(resource(), binary()) :: :ok
  def delete_count(resource, key), do: NIF.flow_index_delete_count(resource, key)

  @spec apply_batch(resource(), [tuple()]) :: :ok
  def apply_batch(_resource, []), do: :ok

  def apply_batch(resource, ops) do
    {put_entries, put_new_entries, move_entries, delete_entries, claim_entries} =
      Enum.reduce(ops, {[], [], [], [], []}, fn
        {:put_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {[entries | puts], put_news, moves, deletes, claims}

        {:put_new_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {puts, [entries | put_news], moves, deletes, claims}

        {:move_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {puts, put_news, [entries | moves], deletes, claims}

        {:delete_members, key, members}, {puts, put_news, moves, deletes, claims} ->
          {puts, put_news, moves, [{key, members} | deletes], claims}

        {:apply_claim_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {puts, put_news, moves, deletes, [entries | claims]}

        _other, acc ->
          acc
      end)

    NIF.flow_index_apply_batch(
      resource,
      parse_reversed_entry_groups(put_entries),
      parse_reversed_entry_groups(put_new_entries),
      parse_reversed_move_groups(move_entries),
      Enum.reverse(delete_entries),
      prepend_reversed_groups(claim_entries)
    )
  end

  @spec claim_due_candidates(
          resource(),
          [binary()],
          score_input(),
          non_neg_integer(),
          non_neg_integer()
        ) :: [{binary(), [{binary(), float()}]}]
  def claim_due_candidates(_resource, _keys, _now_score, 0, _max_scan), do: []
  def claim_due_candidates(_resource, _keys, _now_score, _limit, 0), do: []

  def claim_due_candidates(resource, keys, now_score, limit, max_scan) when is_list(keys) do
    case parse_score(now_score) do
      {:ok, score} -> NIF.flow_index_claim_due_candidates(resource, keys, score, limit, max_scan)
      :error -> []
    end
  end

  @spec due_keys_present(resource(), [binary()], score_input()) :: [binary()]
  def due_keys_present(_resource, [], _now_score), do: []

  def due_keys_present(resource, keys, now_score) when is_list(keys) do
    case parse_score(now_score) do
      {:ok, score} -> NIF.flow_index_due_keys_present(resource, keys, score)
      :error -> []
    end
  end

  @spec apply_claim_entries(resource(), [tuple()]) :: :ok
  def apply_claim_entries(_resource, []), do: :ok

  def apply_claim_entries(resource, entries) do
    NIF.flow_index_apply_claim_entries(resource, entries)
  end

  @spec rollback_claim_entries(resource(), [tuple()]) :: :ok
  def rollback_claim_entries(_resource, []), do: :ok

  def rollback_claim_entries(resource, entries) do
    NIF.flow_index_rollback_claim_entries(resource, entries)
  end

  @spec plan_claims(
          [{binary(), float()}],
          [binary() | nil],
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary()
        ) :: {:ok, [tuple()], [binary()], non_neg_integer()} | :fallback
  def plan_claims(
        candidates,
        values,
        type,
        expected_state,
        worker,
        lease_ms,
        now_ms,
        remaining,
        from_due_key,
        to_due_key,
        from_state_key,
        to_state_key,
        inflight_key,
        worker_key,
        state_key_prefix
      ) do
    NIF.flow_record_plan_claims(
      candidates,
      values,
      type,
      expected_state,
      worker,
      lease_ms,
      now_ms,
      remaining,
      from_due_key,
      to_due_key,
      from_state_key,
      to_state_key,
      inflight_key,
      worker_key,
      state_key_prefix
    )
  end

  @spec plan_claims_with_history(
          [{binary(), float()}],
          [binary() | nil],
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary()
        ) :: {:ok, [tuple()], [binary()], non_neg_integer()} | :fallback
  def plan_claims_with_history(
        candidates,
        values,
        type,
        expected_state,
        worker,
        lease_ms,
        now_ms,
        remaining,
        from_due_key,
        to_due_key,
        from_state_key,
        to_state_key,
        inflight_key,
        worker_key,
        state_key_prefix,
        history_key_prefix
      ) do
    NIF.flow_record_plan_claims_with_history(
      candidates,
      values,
      type,
      expected_state,
      worker,
      lease_ms,
      now_ms,
      remaining,
      from_due_key,
      to_due_key,
      from_state_key,
      to_state_key,
      inflight_key,
      worker_key,
      state_key_prefix,
      history_key_prefix
    )
  end

  defp registry_key(index_table, lookup_table), do: {@registry_key, index_table, lookup_table}

  defp parse_entries(entries) do
    Enum.flat_map(entries, fn {key, member, score_input} ->
      case parse_score(score_input) do
        {:ok, score} -> [{key, member, score}]
        :error -> []
      end
    end)
  end

  defp parse_reversed_entry_groups(groups) do
    Enum.reduce(groups, [], &prepend_parsed_entries/2)
  end

  defp prepend_parsed_entries([], acc), do: acc

  defp prepend_parsed_entries([{key, member, score_input} | rest], acc) do
    case parse_score(score_input) do
      {:ok, score} -> [{key, member, score} | prepend_parsed_entries(rest, acc)]
      :error -> prepend_parsed_entries(rest, acc)
    end
  end

  defp parse_reversed_move_groups(groups) do
    Enum.reduce(groups, [], &prepend_parsed_move_entries/2)
  end

  defp prepend_parsed_move_entries([], acc), do: acc

  defp prepend_parsed_move_entries([{from_key, to_key, member, score_input} | rest], acc) do
    case parse_score(score_input) do
      {:ok, score} ->
        [{from_key, to_key, member, score} | prepend_parsed_move_entries(rest, acc)]

      :error ->
        prepend_parsed_move_entries(rest, acc)
    end
  end

  defp prepend_reversed_groups(groups) do
    Enum.reduce(groups, [], &prepend_group_entries/2)
  end

  defp prepend_group_entries([], acc), do: acc
  defp prepend_group_entries([entry | rest], acc), do: [entry | prepend_group_entries(rest, acc)]

  defp encode_min_bound(:neg_inf), do: {0, 0.0}
  defp encode_min_bound({:inclusive, score}), do: {1, score * 1.0}
  defp encode_min_bound({:exclusive, score}), do: {2, score * 1.0}
  defp encode_min_bound(:inf), do: {3, 0.0}
  defp encode_min_bound(:pos_inf), do: {3, 0.0}
  defp encode_min_bound(_other), do: {3, 0.0}

  defp encode_max_bound(:inf), do: {0, 0.0}
  defp encode_max_bound(:pos_inf), do: {0, 0.0}
  defp encode_max_bound({:inclusive, score}), do: {1, score * 1.0}
  defp encode_max_bound({:exclusive, score}), do: {2, score * 1.0}
  defp encode_max_bound(:neg_inf), do: {3, 0.0}
  defp encode_max_bound(_other), do: {3, 0.0}

  defp parse_score(score) when is_float(score), do: {:ok, score}
  defp parse_score(score) when is_integer(score), do: {:ok, score * 1.0}

  defp parse_score(score) when is_binary(score) do
    case Float.parse(score) do
      {score, ""} -> {:ok, score}
      _ -> :error
    end
  end

  defp parse_score(_score), do: :error
end
