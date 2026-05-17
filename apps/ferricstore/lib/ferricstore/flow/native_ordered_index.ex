defmodule Ferricstore.Flow.NativeOrderedIndex do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @registry_key {__MODULE__, :resource}

  @type resource :: reference()
  @type score_input :: binary() | integer() | float()

  @spec new() :: resource()
  def new, do: NIF.flow_index_new()

  @spec register(:ets.tid() | atom(), :ets.tid() | atom(), resource()) :: :ok
  def register(index_table, lookup_table, resource) do
    :persistent_term.put(registry_key(index_table, lookup_table), resource)
    :ok
  end

  @spec get(:ets.tid() | atom(), :ets.tid() | atom()) :: resource() | nil
  def get(index_table, lookup_table) do
    :persistent_term.get(registry_key(index_table, lookup_table), nil)
  end

  @spec rebuild_from_ets(:ets.tid() | atom(), :ets.tid() | atom()) :: :ok
  def rebuild_from_ets(index_table, lookup_table) do
    resource = new()

    merge_ets_into_resource(resource, index_table, lookup_table)
    register(index_table, lookup_table, resource)
  end

  @spec merge_from_ets(:ets.tid() | atom(), :ets.tid() | atom()) :: :ok
  def merge_from_ets(index_table, lookup_table) do
    resource =
      case get(index_table, lookup_table) do
        nil ->
          resource = new()
          register(index_table, lookup_table, resource)
          resource

        resource ->
          resource
      end

    merge_ets_into_resource(resource, index_table, lookup_table)
  end

  defp merge_ets_into_resource(resource, index_table, lookup_table) do
    index_table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{key, score, member}, true}
      when is_binary(key) and is_binary(member) and (is_integer(score) or is_float(score)) ->
        [{key, member, score * 1.0}]

      _other ->
        []
    end)
    |> then(&put_new_entries(resource, &1))

    lookup_table
    |> :ets.tab2list()
    |> Enum.each(fn
      {{:count, key}, count} when is_binary(key) and is_integer(count) ->
        restore_count(resource, key, count)

      _other ->
        :ok
    end)
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
      put_entries |> reverse_flatten() |> parse_entries(),
      put_new_entries |> reverse_flatten() |> parse_entries(),
      move_entries |> reverse_flatten() |> parse_move_entries(),
      Enum.reverse(delete_entries),
      reverse_flatten(claim_entries)
    )
  end

  @spec claim_due_candidates(
          resource(),
          [binary()],
          score_input(),
          non_neg_integer(),
          non_neg_integer()
        ) :: [{binary(), binary(), float()}]
  def claim_due_candidates(_resource, _keys, _now_score, 0, _max_scan), do: []
  def claim_due_candidates(_resource, _keys, _now_score, _limit, 0), do: []

  def claim_due_candidates(resource, keys, now_score, limit, max_scan) when is_list(keys) do
    case parse_score(now_score) do
      {:ok, score} -> NIF.flow_index_claim_due_candidates(resource, keys, score, limit, max_scan)
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
          [{binary(), score_input()}],
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
    native_candidates =
      Enum.flat_map(candidates, fn {id, score_input} ->
        case parse_score(score_input) do
          {:ok, score} -> [{id, score}]
          :error -> []
        end
      end)

    NIF.flow_record_plan_claims(
      native_candidates,
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

  defp registry_key(index_table, lookup_table), do: {@registry_key, index_table, lookup_table}

  defp parse_entries(entries) do
    Enum.flat_map(entries, fn {key, member, score_input} ->
      case parse_score(score_input) do
        {:ok, score} -> [{key, member, score}]
        :error -> []
      end
    end)
  end

  defp parse_move_entries(entries) do
    Enum.flat_map(entries, fn {from_key, to_key, member, score_input} ->
      case parse_score(score_input) do
        {:ok, score} -> [{from_key, to_key, member, score}]
        :error -> []
      end
    end)
  end

  defp reverse_flatten(lists) do
    lists
    |> Enum.reverse()
    |> List.flatten()
  end

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
