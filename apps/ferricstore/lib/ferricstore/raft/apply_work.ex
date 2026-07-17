defmodule Ferricstore.Raft.ApplyWork do
  @moduledoc false

  alias Ferricstore.Raft.ApplyContext

  @compound_batch_tags [
    :compound_batch_delete,
    :compound_batch_put,
    :compound_blob_batch_put
  ]

  @single_compound_tags [
    :compound_delete,
    :compound_put,
    :compound_put_blob_ref,
    :hincrby,
    :hincrbyfloat,
    :hset_single,
    :lpush_single,
    :rpush_single,
    :sadd_single,
    :srem_single,
    :zadd_single,
    :zincrby,
    :zrem_single
  ]

  @list_push_tags [:lpush, :lpushx, :rpush, :rpushx]

  @spec admit_items(ApplyContext.t(), term()) ::
          {:ok, non_neg_integer()} | {:error, atom()}
  def admit_items(%ApplyContext{batch_command_apply_budget: budget}, items)
      when is_integer(budget) and budget > 0 and is_list(items) do
    count_items_up_to(
      items,
      budget,
      0,
      :batch_command_apply_budget_exceeded,
      :invalid_batch_command_list
    )
  end

  def admit_items(%ApplyContext{batch_command_apply_budget: budget}, _invalid)
      when is_integer(budget) and budget > 0,
      do: {:error, :invalid_batch_command_list}

  def admit_items(_context, _items), do: {:error, :invalid_batch_apply_budget}

  @spec admit_command(ApplyContext.t(), term()) :: :ok | {:error, atom()}
  def admit_command(
        %ApplyContext{
          batch_command_apply_budget: command_budget,
          compound_member_apply_budget: compound_budget
        },
        command
      )
      when is_integer(command_budget) and command_budget > 0 and
             is_integer(compound_budget) and compound_budget > 0 do
    case consume_command(command, command_budget, compound_budget, command_budget * 2) do
      {:ok, _command_remaining, _compound_remaining, _visit_remaining} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def admit_command(_context, _command), do: {:error, :invalid_batch_apply_budget}

  @spec normalize_batch(ApplyContext.t(), term()) ::
          {:ok, [term()], non_neg_integer()} | {:error, atom()}
  def normalize_batch(
        %ApplyContext{
          batch_command_apply_budget: command_budget,
          compound_member_apply_budget: compound_budget
        },
        commands
      )
      when is_integer(command_budget) and command_budget > 0 and
             is_integer(compound_budget) and compound_budget > 0 and is_list(commands) do
    normalize_frames(
      [{:commands, commands}],
      [],
      0,
      command_budget,
      command_budget * 2,
      compound_budget
    )
  end

  def normalize_batch(_context, _commands), do: {:error, :invalid_batch_apply_budget}

  defp normalize_frames(
         [],
         acc,
         count,
         _command_remaining,
         _visit_remaining,
         _compound_remaining
       ),
       do: {:ok, Enum.reverse(acc), count}

  defp normalize_frames(
         [{_kind, []} | frames],
         acc,
         count,
         command_remaining,
         visit_remaining,
         compound_remaining
       ) do
    normalize_frames(
      frames,
      acc,
      count,
      command_remaining,
      visit_remaining,
      compound_remaining
    )
  end

  defp normalize_frames(
         [{_kind, [_item | _rest]} | _frames],
         _acc,
         _count,
         _command_remaining,
         0,
         _compound_remaining
       ),
       do: {:error, :batch_command_apply_budget_exceeded}

  defp normalize_frames(
         [{:commands, [command | rest]} | frames],
         acc,
         count,
         command_remaining,
         visit_remaining,
         compound_remaining
       ) do
    next_frames = [{:commands, rest} | frames]
    visit_remaining = visit_remaining - 1

    case command do
      {:batch, nested} when is_list(nested) ->
        normalize_frames(
          [{:commands, nested} | next_frames],
          acc,
          count,
          command_remaining,
          visit_remaining,
          compound_remaining
        )

      {:put_batch, entries} when is_list(entries) ->
        normalize_frames(
          [{:put_entries, entries} | next_frames],
          acc,
          count,
          command_remaining,
          visit_remaining,
          compound_remaining
        )

      {:put_blob_batch, entries} when is_list(entries) ->
        normalize_frames(
          [{:put_blob_entries, entries} | next_frames],
          acc,
          count,
          command_remaining,
          visit_remaining,
          compound_remaining
        )

      {:delete_batch, keys} when is_list(keys) ->
        normalize_frames(
          [{:delete_keys, keys} | next_frames],
          acc,
          count,
          command_remaining,
          visit_remaining,
          compound_remaining
        )

      leaf ->
        normalize_leaf(
          leaf,
          next_frames,
          acc,
          count,
          command_remaining,
          visit_remaining,
          compound_remaining
        )
    end
  end

  defp normalize_frames(
         [{:delete_keys, [key | rest]} | frames],
         acc,
         count,
         command_remaining,
         visit_remaining,
         compound_remaining
       )
       when is_binary(key) do
    normalize_leaf(
      {:delete, key},
      [{:delete_keys, rest} | frames],
      acc,
      count,
      command_remaining,
      visit_remaining - 1,
      compound_remaining
    )
  end

  defp normalize_frames(
         [{:delete_keys, [_invalid | _rest]} | _frames],
         _acc,
         _count,
         _command_remaining,
         _visit_remaining,
         _compound_remaining
       ),
       do: {:error, :invalid_delete_batch_key}

  defp normalize_frames(
         [{kind, [entry | rest]} | frames],
         acc,
         count,
         command_remaining,
         visit_remaining,
         compound_remaining
       )
       when kind in [:put_entries, :put_blob_entries] do
    normalize_leaf(
      normalize_entry(kind, entry),
      [{kind, rest} | frames],
      acc,
      count,
      command_remaining,
      visit_remaining - 1,
      compound_remaining
    )
  end

  defp normalize_frames(
         [{_kind, _improper_tail} | _frames],
         _acc,
         _count,
         _command_remaining,
         _visit_remaining,
         _compound_remaining
       ),
       do: {:error, :invalid_batch_command_list}

  defp normalize_leaf(
         _leaf,
         _frames,
         _acc,
         _count,
         0,
         _visit_remaining,
         _compound_remaining
       ),
       do: {:error, :batch_command_apply_budget_exceeded}

  defp normalize_leaf(
         leaf,
         frames,
         acc,
         count,
         command_remaining,
         visit_remaining,
         compound_remaining
       ) do
    with {:ok, next_command_remaining, next_compound_remaining, next_visit_remaining} <-
           consume_command(
             leaf,
             command_remaining,
             compound_remaining,
             visit_remaining
           ) do
      normalize_frames(
        frames,
        [leaf | acc],
        count + 1,
        next_command_remaining,
        next_visit_remaining,
        next_compound_remaining
      )
    end
  end

  defp normalize_entry(:put_entries, {key, value, expire_at_ms}),
    do: {:put, key, value, expire_at_ms}

  defp normalize_entry(:put_entries, invalid), do: {:invalid_put_batch_entry, invalid}

  defp normalize_entry(:put_blob_entries, {key, value, expire_at_ms, :value}),
    do: {:put, key, value, expire_at_ms}

  defp normalize_entry(:put_blob_entries, {key, encoded_ref, expire_at_ms, :blob_ref}),
    do: {:put_blob_ref, key, encoded_ref, expire_at_ms}

  defp normalize_entry(:put_blob_entries, invalid),
    do: {:invalid_put_blob_batch_entry, invalid}

  defp consume_command(
         {:async, _origin, inner},
         command_remaining,
         compound_remaining,
         visit_remaining
       ),
       do: consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining)

  defp consume_command(
         {:ferricstore_latency_trace, inner},
         command_remaining,
         compound_remaining,
         visit_remaining
       ),
       do: consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining)

  defp consume_command(
         {:ferricstore_apply_context, _encoded, inner},
         command_remaining,
         compound_remaining,
         visit_remaining
       ),
       do: consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining)

  defp consume_command(
         {:flow_shared_ref_write, _shard_index, inner},
         command_remaining,
         compound_remaining,
         visit_remaining
       ),
       do: consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining)

  defp consume_command(
         {:flow_policy_fence, installs, inner},
         command_remaining,
         compound_remaining,
         visit_remaining
       ) do
    with {:ok, next_command_remaining} <- consume_optional_items(installs, command_remaining) do
      consume_wrapped(inner, next_command_remaining, compound_remaining, visit_remaining)
    end
  end

  defp consume_command(
         {:origin_checked, _key, inner, _before_value, _before_expire_at_ms, _expected_value,
          _expire_at_ms},
         command_remaining,
         compound_remaining,
         visit_remaining
       ),
       do: consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining)

  defp consume_command(
         {:origin_checked, _key, inner, _expected_value, _expire_at_ms},
         command_remaining,
         compound_remaining,
         visit_remaining
       ),
       do: consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining)

  defp consume_command(
         {inner, metadata},
         command_remaining,
         compound_remaining,
         visit_remaining
       )
       when is_tuple(inner) and is_map(metadata),
       do: consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining)

  defp consume_command(command, command_remaining, compound_remaining, visit_remaining) do
    with {:ok, next_command_remaining} <- consume_command_work(command, command_remaining),
         {:ok, next_compound_remaining} <-
           consume_compound_work(command, compound_remaining) do
      {:ok, next_command_remaining, next_compound_remaining, visit_remaining}
    end
  end

  defp consume_wrapped(_inner, _command_remaining, _compound_remaining, 0),
    do: {:error, :batch_command_apply_budget_exceeded}

  defp consume_wrapped(inner, command_remaining, compound_remaining, visit_remaining),
    do:
      consume_command(
        inner,
        command_remaining,
        compound_remaining,
        visit_remaining - 1
      )

  defp consume_command_work({tag, entries}, remaining)
       when tag in [
              :mset,
              :mset_blob_batch,
              :msetnx,
              :msetnx_blob_batch,
              :watch_tokens,
              :zadd_many_single
            ],
       do: consume_items(entries, remaining)

  defp consume_command_work({tag, _redis_key, entries}, remaining)
       when tag in @compound_batch_tags,
       do: consume_items(entries, remaining)

  defp consume_command_work({:tx_execute, queue, _namespace}, remaining),
    do: consume_items(queue, remaining)

  defp consume_command_work({:tx_execute, queue, _namespace, _watched_keys}, remaining),
    do: consume_items(queue, remaining)

  defp consume_command_work({:pfadd, _key, elements}, remaining),
    do: consume_items(elements, remaining)

  defp consume_command_work({:pfmerge, _key, sketches}, remaining),
    do: consume_items(sketches, remaining)

  defp consume_command_work({:pfmerge, _key, source_keys, sketches}, remaining),
    do: consume_paired_items(source_keys, sketches, remaining)

  defp consume_command_work({:list_op, _key, {tag, elements}}, remaining)
       when tag in @list_push_tags,
       do: consume_items(elements, remaining)

  defp consume_command_work({:bloom_madd, _key, elements, _auto_create}, remaining),
    do: consume_items(elements, remaining)

  defp consume_command_work({:cms_incrby, _key, items}, remaining),
    do: consume_items(items, remaining)

  defp consume_command_work(
         {:cms_merge, _key, source_keys, weights, _create_params},
         remaining
       ),
       do: consume_paired_items(source_keys, weights, remaining)

  defp consume_command_work({:topk_add, _key, elements}, remaining),
    do: consume_items(elements, remaining)

  defp consume_command_work({:topk_incrby, _key, pairs}, remaining),
    do: consume_items(pairs, remaining)

  defp consume_command_work(_command, remaining), do: consume_unit(remaining)

  defp consume_items(items, remaining) do
    case count_items_up_to(
           items,
           remaining,
           0,
           :batch_command_apply_budget_exceeded,
           :invalid_batch_command_list
         ) do
      {:ok, 0} -> consume_unit(remaining)
      {:ok, count} -> {:ok, remaining - count}
      {:error, _reason} = error -> error
    end
  end

  defp consume_optional_items(items, remaining) do
    case count_items_up_to(
           items,
           remaining,
           0,
           :batch_command_apply_budget_exceeded,
           :invalid_batch_command_list
         ) do
      {:ok, count} -> {:ok, remaining - count}
      {:error, _reason} = error -> error
    end
  end

  defp consume_paired_items(first, second, remaining),
    do: count_paired_items(first, second, remaining, 0)

  defp count_paired_items([], [], remaining, 0), do: consume_unit(remaining)
  defp count_paired_items([], [], remaining, count), do: {:ok, remaining - count}

  defp count_paired_items([_first | first_rest], [_second | second_rest], remaining, count)
       when remaining - count >= 2,
       do: count_paired_items(first_rest, second_rest, remaining, count + 2)

  defp count_paired_items([_first | _], [_second | _], _remaining, _count),
    do: {:error, :batch_command_apply_budget_exceeded}

  defp count_paired_items(first, second, _remaining, _count)
       when first == [] or second == [],
       do: {:error, :batch_pair_cardinality_mismatch}

  defp count_paired_items(_first, _second, _remaining, _count),
    do: {:error, :invalid_batch_command_list}

  defp consume_unit(remaining) when remaining > 0, do: {:ok, remaining - 1}
  defp consume_unit(0), do: {:error, :batch_command_apply_budget_exceeded}

  defp consume_compound_work({tag, _redis_key, entries}, remaining)
       when tag in @compound_batch_tags,
       do: consume_compound_items(entries, remaining)

  defp consume_compound_work({:zadd_many_single, entries}, remaining),
    do: consume_compound_items(entries, remaining)

  defp consume_compound_work({:list_op, _key, {tag, elements}}, remaining)
       when tag in @list_push_tags,
       do: consume_compound_items(elements, remaining)

  defp consume_compound_work(command, remaining)
       when is_tuple(command) and tuple_size(command) > 0 and
              elem(command, 0) in @single_compound_tags,
       do: consume_compound_unit(remaining)

  defp consume_compound_work(_command, remaining), do: {:ok, remaining}

  defp consume_compound_items(items, remaining) do
    case count_items_up_to(
           items,
           remaining,
           0,
           :compound_member_apply_budget_exceeded,
           :invalid_compound_batch_entry
         ) do
      {:ok, count} -> {:ok, remaining - count}
      {:error, _reason} = error -> error
    end
  end

  defp consume_compound_unit(remaining) when remaining > 0,
    do: {:ok, remaining - 1}

  defp consume_compound_unit(0),
    do: {:error, :compound_member_apply_budget_exceeded}

  defp count_items_up_to(
         [],
         _remaining,
         count,
         _exceeded_error,
         _invalid_error
       ),
       do: {:ok, count}

  defp count_items_up_to(
         [_item | _rest],
         0,
         _count,
         exceeded_error,
         _invalid_error
       ),
       do: {:error, exceeded_error}

  defp count_items_up_to(
         [_item | rest],
         remaining,
         count,
         exceeded_error,
         invalid_error
       ),
       do:
         count_items_up_to(
           rest,
           remaining - 1,
           count + 1,
           exceeded_error,
           invalid_error
         )

  defp count_items_up_to(
         _improper_tail,
         _remaining,
         _count,
         _exceeded_error,
         invalid_error
       ),
       do: {:error, invalid_error}
end
