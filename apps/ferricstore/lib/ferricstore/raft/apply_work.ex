defmodule Ferricstore.Raft.ApplyWork do
  @moduledoc false

  alias Ferricstore.Raft.ApplyContext
  alias Ferricstore.Store.CompoundKey

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
  @visit_budget_multiplier 2

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
  def admit_command(%ApplyContext{} = context, command) do
    case batch_usage(context, [command]) do
      {:ok, _usage} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def admit_command(_context, _command), do: {:error, :invalid_batch_apply_budget}

  @spec visit_budget(ApplyContext.t()) :: non_neg_integer()
  def visit_budget(%ApplyContext{batch_command_apply_budget: budget})
      when is_integer(budget) and budget > 0,
      do: budget * @visit_budget_multiplier

  def visit_budget(_context), do: 0

  @spec normalize_batch(ApplyContext.t(), term()) ::
          {:ok, [term()], non_neg_integer()} | {:error, atom()}
  def normalize_batch(%ApplyContext{} = context, commands) do
    case normalize_batch_with_usage(context, commands, []) do
      {:ok, normalized, count, _usage} -> {:ok, normalized, count}
      {:error, _reason} = error -> error
    end
  end

  def normalize_batch(_context, _commands), do: {:error, :invalid_batch_apply_budget}

  @type usage :: %{
          command_items: non_neg_integer(),
          compound_members: non_neg_integer(),
          visits: non_neg_integer(),
          replies: non_neg_integer()
        }

  @spec batch_usage(ApplyContext.t(), term()) :: {:ok, usage()} | {:error, atom()}
  def batch_usage(%ApplyContext{} = context, commands) do
    case normalize_batch_with_usage(context, commands, nil) do
      {:ok, _normalized, _count, usage} -> {:ok, usage}
      {:error, _reason} = error -> error
    end
  end

  def batch_usage(_context, _commands), do: {:error, :invalid_batch_apply_budget}

  defp normalize_batch_with_usage(
         %ApplyContext{
           batch_command_apply_budget: command_budget,
           compound_member_apply_budget: compound_budget
         } = context,
         commands,
         normalized_acc
       )
       when is_integer(command_budget) and command_budget > 0 and
              is_integer(compound_budget) and compound_budget > 0 and is_list(commands) do
    case normalize_frames(
           [{:commands, commands}],
           normalized_acc,
           0,
           command_budget,
           visit_budget(context),
           compound_budget
         ) do
      {:ok, normalized, count, command_remaining, visit_remaining, compound_remaining} ->
        {:ok, normalized, count,
         %{
           command_items: command_budget - command_remaining,
           compound_members: compound_budget - compound_remaining,
           visits: visit_budget(context) - visit_remaining,
           replies: count
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_batch_with_usage(_context, _commands, _normalized_acc),
    do: {:error, :invalid_batch_apply_budget}

  defp normalize_frames(
         [],
         acc,
         count,
         command_remaining,
         visit_remaining,
         compound_remaining
       ),
       do:
         {:ok, reverse_normalized(acc), count, command_remaining, visit_remaining,
          compound_remaining}

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
        prepend_normalized(acc, leaf),
        count + 1,
        next_command_remaining,
        next_visit_remaining,
        next_compound_remaining
      )
    end
  end

  defp prepend_normalized(nil, _leaf), do: nil
  defp prepend_normalized(acc, leaf), do: [leaf | acc]

  defp reverse_normalized(nil), do: nil
  defp reverse_normalized(acc), do: Enum.reverse(acc)

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

  defp consume_command(
         {tag, entries} = command,
         command_remaining,
         compound_remaining,
         visit_remaining
       )
       when tag in [:batch, :put_batch, :put_blob_batch, :delete_batch] and is_list(entries),
       do:
         consume_expanded_command(
           command,
           command_remaining,
           compound_remaining,
           visit_remaining
         )

  defp consume_command(command, command_remaining, compound_remaining, visit_remaining) do
    with {:ok, next_command_remaining} <- consume_command_work(command, command_remaining),
         {:ok, next_compound_remaining} <-
           consume_compound_work(command, compound_remaining) do
      {:ok, next_command_remaining, next_compound_remaining, visit_remaining}
    end
  end

  defp consume_expanded_command(
         command,
         command_remaining,
         compound_remaining,
         visit_remaining
       ) do
    frames =
      case command do
        {:batch, commands} -> [{:commands, commands}]
        {:put_batch, entries} -> [{:put_entries, entries}]
        {:put_blob_batch, entries} -> [{:put_blob_entries, entries}]
        {:delete_batch, keys} -> [{:delete_keys, keys}]
      end

    case normalize_frames(
           frames,
           nil,
           0,
           command_remaining,
           visit_remaining,
           compound_remaining
         ) do
      {:ok, _normalized, 0, next_command_remaining, next_visit_remaining, next_compound_remaining} ->
        with {:ok, charged_command_remaining} <- consume_unit(next_command_remaining) do
          {:ok, charged_command_remaining, next_compound_remaining, next_visit_remaining}
        end

      {:ok, _normalized, _count, next_command_remaining, next_visit_remaining,
       next_compound_remaining} ->
        {:ok, next_command_remaining, next_compound_remaining, next_visit_remaining}

      {:error, _reason} = error ->
        error
    end
  end

  defp consume_wrapped(
         {tag, entries} = inner,
         command_remaining,
         compound_remaining,
         visit_remaining
       )
       when tag in [:batch, :put_batch, :put_blob_batch, :delete_batch] and is_list(entries),
       do: consume_command(inner, command_remaining, compound_remaining, visit_remaining)

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
              :expire_if_batch,
              :put_batch,
              :put_blob_batch,
              :delete_batch,
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

  defp consume_compound_work({:expire_if_batch, entries}, remaining),
    do: consume_expire_if_compound_items(entries, remaining)

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

  defp consume_expire_if_compound_items([], remaining), do: {:ok, remaining}

  defp consume_expire_if_compound_items(
         [{key, expire_at_ms} | rest],
         remaining
       )
       when is_binary(key) and is_integer(expire_at_ms) and expire_at_ms > 0 do
    if CompoundKey.internal_key?(key) do
      with {:ok, next_remaining} <- consume_compound_unit(remaining) do
        consume_expire_if_compound_items(rest, next_remaining)
      end
    else
      consume_expire_if_compound_items(rest, remaining)
    end
  end

  defp consume_expire_if_compound_items(_invalid, _remaining),
    do: {:error, :invalid_expire_if_batch_entry}

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
