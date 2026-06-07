defmodule Ferricstore.Raft.StateMachine.Sections.FlowTerminal do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]
      import Bitwise

      require Logger

      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.CommandTime
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Commands.HyperLogLog
      alias Ferricstore.Commands.Json
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Flow
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.RetryPolicy
      alias Ferricstore.HLC

      alias Ferricstore.Store.{
        BitcaskWriter,
        BlobRef,
        BlobStore,
        BlobValue,
        ColdRead,
        CompoundKey,
        ExpiryTracker,
        LFU,
        ListOps,
        Promotion,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.ZSetIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

  defp do_flow_cross_terminal(state, :complete, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms),
         :ok <- flow_apply_complete_local(child_state, record, next, partition_key, now_ms, attrs),
         :ok <- flow_apply_child_terminal_chain(state, next, "completed", now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal(state, :fail, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms),
         :ok <- flow_apply_fail_local(child_state, record, next, partition_key, now_ms, attrs),
         :ok <- flow_apply_child_terminal_chain(state, next, "failed", now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal(state, :retry, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next, history_meta} <-
           flow_prepare_retry_existing_record(child_state, record, attrs, lease_token, now_ms),
         :ok <-
           flow_apply_retry_local(
             child_state,
             record,
             next,
             partition_key,
             now_ms,
             history_meta,
             attrs
           ),
         :ok <- flow_maybe_apply_cross_terminal_chain(state, next, now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal(state, :cancel, %{id: id} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <- flow_prepare_cancel_existing_record(record, attrs, now_ms),
         :ok <- flow_apply_cancel_local(child_state, record, next, attrs, partition_key, now_ms),
         :ok <- flow_apply_child_terminal_chain(state, next, "cancelled", now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal_many(state, op, %{records: [_ | _] = attrs_list})
       when op in [:complete, :retry, :fail, :cancel] do
    with :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans} <- flow_cross_terminal_many_prepare(state, op, attrs_list),
         :ok <- flow_cross_terminal_many_apply(state, op, plans) do
      :ok
    end
  end

  defp do_flow_cross_terminal_many(state, op, [_ | _] = attrs_list)
       when op in [:complete, :retry, :fail, :cancel] do
    with :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans} <- flow_cross_terminal_many_prepare(state, op, attrs_list),
         :ok <- flow_cross_terminal_many_apply(state, op, plans) do
      :ok
    end
  end

  defp do_flow_cross_terminal_many(_state, _op, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_cross_terminal_many_prepare(state, op, attrs_list) do
    attrs_list
    |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, acc} ->
      case flow_cross_terminal_prepare(state, op, attrs) do
        {:ok, plan} -> {:cont, {:ok, [plan | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, plans} -> {:ok, Enum.reverse(plans)}
      {:error, _reason} = error -> error
    end
  end

  defp flow_cross_terminal_prepare(state, :complete, %{id: id, lease_token: lease_token} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms}}
    end
  end

  defp flow_cross_terminal_prepare(state, :fail, %{id: id, lease_token: lease_token} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms}}
    end
  end

  defp flow_cross_terminal_prepare(state, :retry, %{id: id, lease_token: lease_token} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next, history_meta} <-
           flow_prepare_retry_existing_record(child_state, record, attrs, lease_token, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms, history_meta}}
    end
  end

  defp flow_cross_terminal_prepare(state, :cancel, %{id: id} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <- flow_prepare_cancel_existing_record(record, attrs, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms}}
    end
  end

  defp flow_cross_terminal_prepare(_state, _op, _attrs),
    do: {:error, "ERR flow id must be a non-empty string"}

  defp flow_cross_terminal_many_apply(state, op, plans) do
    Enum.reduce_while(plans, :ok, fn plan, :ok ->
      case flow_cross_terminal_apply_plan(state, op, plan) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :complete,
         {child_state, record, next, attrs, partition_key, now_ms}
       ) do
    with :ok <- flow_apply_complete_local(child_state, record, next, partition_key, now_ms, attrs) do
      flow_apply_child_terminal_chain(state, next, "completed", now_ms)
    end
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :fail,
         {child_state, record, next, attrs, partition_key, now_ms}
       ) do
    with :ok <- flow_apply_fail_local(child_state, record, next, partition_key, now_ms, attrs) do
      flow_apply_child_terminal_chain(state, next, "failed", now_ms)
    end
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :retry,
         {child_state, record, next, attrs, partition_key, now_ms, history_meta}
       ) do
    with :ok <-
           flow_apply_retry_local(
             child_state,
             record,
             next,
             partition_key,
             now_ms,
             history_meta,
             attrs
           ) do
      flow_maybe_apply_cross_terminal_chain(state, next, now_ms)
    end
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :cancel,
         {child_state, record, next, attrs, partition_key, now_ms}
       ) do
    with :ok <- flow_apply_cancel_local(child_state, record, next, attrs, partition_key, now_ms) do
      flow_apply_child_terminal_chain(state, next, "cancelled", now_ms)
    end
  end

  defp flow_apply_complete_local(state, record, next, partition_key, now_ms, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "completed", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_apply_fail_local(state, record, next, partition_key, now_ms, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "failed", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_apply_cancel_local(state, record, next, attrs, partition_key, now_ms) do
    plans = [{record, next}]
    refresh_attrs = flow_cancel_refresh_attrs(attrs)

    with :ok <- do_flow_put_record_values(state, next, attrs),
         :ok <- flow_refresh_terminal_value_expirations(state, next, refresh_attrs),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "cancelled", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_apply_retry_local(state, record, next, partition_key, now_ms, history_meta, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "retry", now_ms, history_meta) do
      flow_after_history_put(state, next)
    end
  end

  defp flow_maybe_apply_cross_terminal_chain(state, next, now_ms) do
    status = Map.get(next, :state)

    if Ferricstore.Flow.LMDB.terminal_state?(status) do
      with :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms) do
        flow_apply_child_terminal_chain(state, next, status, now_ms)
      end
    else
      :ok
    end
  end

  defp flow_apply_child_terminal_chain(state, child, status, now_ms) do
    parent_id = Map.get(child, :parent_flow_id)
    parent_partition = Map.get(child, :parent_partition_key) || Map.get(child, :partition_key)

    if is_binary(parent_id) and parent_id != "" and status in ["completed", "failed", "cancelled"] do
      parent_state =
        cross_shard_state_for_key(state, FlowKeys.state_key(parent_id, parent_partition))

      case flow_read_record(parent_state, parent_id, parent_partition) do
        nil ->
          :ok

        parent ->
          case flow_child_terminal_parent_next(parent, child, status, now_ms) do
            {:ok, nil} ->
              :ok

            {:ok, next_parent} ->
              with :ok <-
                     flow_apply_parent_update(
                       parent_state,
                       parent,
                       next_parent,
                       "child_#{status}",
                       now_ms
                     ),
                   :ok <-
                     flow_maybe_cancel_children_on_parent_closed(
                       parent_state,
                       next_parent,
                       now_ms
                     ) do
                flow_maybe_apply_resolved_parent_terminal_cross(state, next_parent, now_ms)
              end
          end
      end
    else
      :ok
    end
  end

  defp flow_maybe_apply_resolved_parent_terminal_cross(state, parent, now_ms) do
    case Map.get(parent, :state) do
      "completed" -> flow_apply_child_terminal_chain(state, parent, "completed", now_ms)
      "failed" -> flow_apply_child_terminal_chain(state, parent, "failed", now_ms)
      "cancelled" -> flow_apply_child_terminal_chain(state, parent, "cancelled", now_ms)
      _state -> :ok
    end
  end

  defp flow_maybe_cancel_children_on_parent_closed(state, parent, now_ms) do
    case flow_child_groups(parent) do
      groups when map_size(groups) == 0 ->
        :ok

      groups ->
        if Ferricstore.Flow.LMDB.terminal_state?(Map.get(parent, :state)) or
             flow_has_resolved_cancel_child_group?(groups) do
          flow_cancel_children_on_parent_closed(state, parent, now_ms, groups)
        else
          :ok
        end
    end
  end

  defp flow_has_resolved_cancel_child_group?(groups) do
    Enum.any?(groups, fn {_group_id, group} ->
      not is_nil(Map.get(group, "resolved")) and flow_group_should_cancel_children?(group)
    end)
  end

  defp flow_cancel_children_on_parent_closed(state, parent, now_ms, groups) do
    {updated_groups, child_refs} =
      Enum.reduce(groups, {groups, []}, fn {group_id, group}, {groups_acc, child_acc} ->
        if flow_group_should_cancel_children?(group) do
          running_refs = flow_group_running_child_refs(group, Map.get(parent, :partition_key))
          running_ids = Enum.map(running_refs, fn {child_id, _partition_key} -> child_id end)
          updated_group = flow_group_mark_children_cancelled(group, running_ids)
          {Map.put(groups_acc, group_id, updated_group), running_refs ++ child_acc}
        else
          {groups_acc, child_acc}
        end
      end)

    child_refs = Enum.uniq(child_refs)

    if child_refs == [] do
      :ok
    else
      with :ok <- flow_cancel_direct_children(state, child_refs, now_ms),
           {:ok, updated_parent} <-
             flow_parent_with_updated_child_groups(parent, updated_groups, now_ms) do
        flow_apply_parent_update(state, parent, updated_parent, "children_cancelled", now_ms)
      end
    end
  end

  defp flow_group_should_cancel_children?(%{"on_parent_closed" => "cancel_children"} = group) do
    group
    |> Map.get("children", %{})
    |> Enum.any?(fn {_child_id, status} -> status == "running" end)
  end

  defp flow_group_should_cancel_children?(_group), do: false

  defp flow_group_running_child_refs(group, default_partition_key) do
    child_partitions = Map.get(group, "child_partitions", %{})

    group
    |> Map.get("children", %{})
    |> Enum.flat_map(fn
      {child_id, "running"} ->
        [{child_id, Map.get(child_partitions, child_id, default_partition_key)}]

      _other ->
        []
    end)
  end

  defp flow_group_mark_children_cancelled(group, []), do: group

  defp flow_group_mark_children_cancelled(group, child_ids) do
    children =
      Enum.reduce(child_ids, Map.get(group, "children", %{}), fn child_id, acc ->
        Map.put(acc, child_id, "cancelled")
      end)

    summary = Map.get(group, "summary", %{})
    cancelled = Map.get(summary, "cancelled", 0) + length(child_ids)
    resolved = Map.get(group, "resolved") || "failure"

    group
    |> Map.put("children", children)
    |> Map.put("results", flow_child_group_cancelled_results(group, child_ids))
    |> Map.put("summary", Map.put(summary, "cancelled", cancelled))
    |> Map.put("resolved", resolved)
  end

  defp flow_child_group_cancelled_results(group, child_ids) do
    Enum.reduce(child_ids, Map.get(group, "results", %{}), fn child_id, acc ->
      Map.put(acc, child_id, %{"status" => "cancelled"})
    end)
  end

  defp flow_cancel_direct_children(state, child_refs, now_ms) do
    Enum.reduce_while(child_refs, :ok, fn {child_id, partition_key}, :ok ->
      child_state = flow_child_state_for_partition(state, child_id, partition_key)

      case flow_read_record(child_state, child_id, partition_key) do
        nil ->
          {:cont, :ok}

        child ->
          if Ferricstore.Flow.LMDB.terminal_state?(Map.get(child, :state)) do
            {:cont, :ok}
          else
            case flow_apply_internal_child_cancel(child_state, child, now_ms) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
          end
      end
    end)
  end

  defp flow_child_state_for_partition(state, child_id, partition_key) do
    if cross_shard_pending_active?() do
      cross_shard_state_for_key(state, FlowKeys.state_key(child_id, partition_key))
    else
      state
    end
  end

  defp flow_apply_internal_child_cancel(state, child, now_ms) do
    next =
      child
      |> Map.merge(%{
        state: "cancelled",
        version: Map.fetch!(child, :version) + 1,
        updated_at_ms: now_ms,
        ttl_ms: nil,
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        next_run_at_ms: nil
      })
      |> flow_stamp_terminal_retention(now_ms)

    with :ok <- flow_transition_move_indexes(state, [{child, next}]),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, Map.get(next, :partition_key)),
             next
           ),
         :ok <- flow_history_put_planned(state, child, next, "cancelled", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_parent_with_updated_child_groups(parent, updated_groups, now_ms) do
    next =
      parent
      |> Map.merge(%{
        version: Map.fetch!(parent, :version) + 1,
        updated_at_ms: now_ms,
        child_groups: updated_groups
      })

    with :ok <- flow_validate_record_keys(next) do
      {:ok, next}
    end
  end

  defp flow_maybe_apply_child_terminal(
         state,
         %{parent_flow_id: parent_id, partition_key: partition_key} = child,
         status,
         now_ms
       )
       when is_binary(parent_id) and parent_id != "" and
              status in ["completed", "failed", "cancelled"] do
    parent_partition_key = Map.get(child, :parent_partition_key) || partition_key

    if parent_partition_key != partition_key and not cross_shard_pending_active?() do
      :ok
    else
      parent_state =
        if cross_shard_pending_active?() do
          cross_shard_state_for_key(state, FlowKeys.state_key(parent_id, parent_partition_key))
        else
          state
        end

      case flow_read_record(parent_state, parent_id, parent_partition_key) do
        nil ->
          :ok

        parent ->
          case flow_child_terminal_parent_next(parent, child, status, now_ms) do
            {:ok, nil} ->
              :ok

            {:ok, next_parent} ->
              with :ok <-
                     flow_apply_parent_update(
                       parent_state,
                       parent,
                       next_parent,
                       "child_#{status}",
                       now_ms
                     ),
                   :ok <-
                     flow_maybe_cancel_children_on_parent_closed(
                       parent_state,
                       next_parent,
                       now_ms
                     ) do
                flow_maybe_apply_resolved_parent_terminal(parent_state, next_parent, now_ms)
              end
          end
      end
    end
  end

  defp flow_maybe_apply_child_terminal(_state, _child, _status, _now_ms), do: :ok

  defp flow_many_after_terminal(_state, _plans, _status, false), do: :ok

  defp flow_many_after_terminal(state, plans, status, true) do
    flow_many_after_terminal(state, plans, status)
  end

  defp flow_many_after_terminal(state, plans, status) do
    Enum.reduce_while(plans, :ok, fn plan, :ok ->
      {_record, next} = flow_claim_plan_pair(plan)

      if flow_terminal_after_noop?(next) do
        {:cont, :ok}
      else
        now_ms = flow_record_updated_at_ms(next)

        with :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
             :ok <- flow_maybe_apply_child_terminal(state, next, status, now_ms) do
          {:cont, :ok}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end
    end)
  end

  defp flow_terminal_after_required?(:retry, next) do
    Ferricstore.Flow.LMDB.terminal_state?(Map.get(next, :state)) and
      not flow_terminal_after_noop?(next)
  end

  defp flow_terminal_after_required?(_op, next), do: not flow_terminal_after_noop?(next)

  defp flow_terminal_after_noop?(record) do
    flow_blank_metadata?(Map.get(record, :parent_flow_id)) and
      flow_empty_child_groups?(Map.get(record, :child_groups))
  end

  defp flow_empty_child_groups?(groups) when is_map(groups), do: map_size(groups) == 0
  defp flow_empty_child_groups?(_groups), do: true

  defp flow_record_updated_at_ms(%{updated_at_ms: now_ms}) when is_integer(now_ms), do: now_ms
  defp flow_record_updated_at_ms(_record), do: apply_now_ms()

  defp flow_attrs_now_ms(%{now_ms: now_ms}), do: now_ms
  defp flow_attrs_now_ms(_attrs), do: apply_now_ms()

  defp flow_child_terminal_parent_next(parent, child, status, now_ms) do
    child_id = Map.fetch!(child, :id)
    groups = flow_child_groups(parent)

    case flow_find_open_child_group(groups, child_id) do
      nil ->
        {:ok, nil}

      {group_id, group} ->
        updated_group = flow_child_group_count_terminal(group, child, status)
        resolved_group = flow_child_group_resolve(updated_group, status)
        updated_groups = Map.put(groups, group_id, resolved_group)
        next_state = flow_child_group_parent_state(parent, resolved_group)

        next =
          parent
          |> Map.merge(%{
            state: next_state,
            version: Map.fetch!(parent, :version) + 1,
            updated_at_ms: now_ms,
            child_groups: updated_groups
          })
          |> flow_clear_parent_if_resolved(resolved_group)
          |> flow_stamp_terminal_retention(now_ms)

        {:ok, next}
    end
  end

  defp flow_find_open_child_group(groups, child_id) do
    Enum.find(groups, fn {_group_id, group} ->
      is_nil(Map.get(group, "resolved")) and
        Map.get(group, "children", %{})[child_id] == "running"
    end)
  end

  defp flow_child_group_count_terminal(group, child, status) do
    child_id = Map.fetch!(child, :id)
    summary_key = status
    result = flow_child_terminal_result(child, status)

    group
    |> update_in(["children"], &Map.put(&1, child_id, status))
    |> update_in(["results"], &Map.put(&1 || %{}, child_id, result))
    |> update_in(["summary", summary_key], fn count -> (count || 0) + 1 end)
  end

  defp flow_child_terminal_result(child, status) do
    %{"status" => status}
    |> maybe_put_group_result_ref("result_ref", Map.get(child, :result_ref))
    |> maybe_put_group_result_ref("error_ref", Map.get(child, :error_ref))
  end

  defp maybe_put_group_result_ref(result, _key, nil), do: result
  defp maybe_put_group_result_ref(result, key, value), do: Map.put(result, key, value)

  defp flow_child_group_resolve(%{"wait" => "any"} = group, "completed") do
    Map.put(group, "resolved", "success")
  end

  defp flow_child_group_resolve(%{"wait" => "any"} = group, status)
       when status in ["failed", "cancelled"] do
    if Map.get(group, "on_child_failed") == "fail_parent" do
      Map.put(group, "resolved", "failure")
    else
      flow_child_group_resolve_any_terminal(group)
    end
  end

  defp flow_child_group_resolve(group, status) when status in ["failed", "cancelled"] do
    if Map.get(group, "on_child_failed") == "fail_parent" do
      Map.put(group, "resolved", "failure")
    else
      flow_child_group_resolve_all_terminal(group)
    end
  end

  defp flow_child_group_resolve(group, _status), do: flow_child_group_resolve_all_terminal(group)

  defp flow_child_group_resolve_any_terminal(group) do
    summary = Map.get(group, "summary", %{})
    total = Map.get(summary, "total", 0)
    completed = Map.get(summary, "completed", 0)

    terminal_count =
      completed + Map.get(summary, "failed", 0) + Map.get(summary, "cancelled", 0)

    cond do
      completed > 0 -> Map.put(group, "resolved", "success")
      terminal_count >= total -> Map.put(group, "resolved", "failure")
      true -> group
    end
  end

  defp flow_child_group_resolve_all_terminal(group) do
    summary = Map.get(group, "summary", %{})
    total = Map.get(summary, "total", 0)

    terminal_count =
      Map.get(summary, "completed", 0) + Map.get(summary, "failed", 0) +
        Map.get(summary, "cancelled", 0)

    if terminal_count >= total do
      Map.put(group, "resolved", "success")
    else
      group
    end
  end

  defp flow_child_group_parent_state(_parent, %{
         "resolved" => resolved,
         "exhaust_to" => exhaust_to
       })
       when resolved in ["success", "failure"] and is_map(exhaust_to) do
    Map.fetch!(exhaust_to, resolved)
  end

  defp flow_child_group_parent_state(parent, _group), do: Map.fetch!(parent, :state)

  defp flow_clear_parent_if_resolved(next, %{"resolved" => resolved})
       when resolved in ["success", "failure"] do
    next
    |> Map.put(:next_run_at_ms, nil)
    |> Map.put(:ttl_ms, nil)
    |> Map.put(:lease_owner, nil)
    |> Map.put(:lease_token, nil)
    |> Map.put(:lease_deadline_ms, 0)
  end

  defp flow_clear_parent_if_resolved(next, _group), do: next

  defp flow_maybe_apply_resolved_parent_terminal(state, parent, now_ms) do
    case Map.get(parent, :state) do
      "completed" -> flow_maybe_apply_child_terminal(state, parent, "completed", now_ms)
      "failed" -> flow_maybe_apply_child_terminal(state, parent, "failed", now_ms)
      "cancelled" -> flow_maybe_apply_child_terminal(state, parent, "cancelled", now_ms)
      _state -> :ok
    end
  end

  defp flow_cancel_refresh_attrs(attrs) do
    if Map.has_key?(attrs, :error) do
      attrs
    else
      Map.put(attrs, :error, true)
    end
  end

  defp flow_cancel_many_prepare(state, attrs_list) do
    existing_records = flow_read_records(state, attrs_list)

    attrs_list
    |> Enum.zip(existing_records)
    |> Enum.reduce_while({:ok, [], false, false}, fn
      {%{id: _id} = attrs, existing}, {:ok, acc, has_values?, has_after_terminal?} ->
        now_ms = flow_attrs_now_ms(attrs)

        case existing do
          nil ->
            {:halt, {:error, "ERR flow not found"}}

          record ->
            case flow_prepare_cancel_existing_record(record, attrs, now_ms) do
              {:ok, record, next} ->
                {:cont,
                 {:ok, [{record, next, attrs} | acc],
                  has_values? or flow_attrs_have_record_values?(attrs),
                  has_after_terminal? or flow_terminal_after_required?(:cancel, next)}}

              {:error, _reason} = error ->
                {:halt, error}
            end
        end

      {_bad, _existing}, {:ok, _acc, _has_values?, _has_after_terminal?} ->
        {:halt, {:error, "ERR flow id must be a non-empty string"}}
    end)
    |> case do
      {:ok, plans, has_record_values?, has_after_terminal?} ->
        {:ok, Enum.reverse(plans), has_record_values?, has_after_terminal?}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_cancel_many_apply(state, plans, has_record_values?, has_after_terminal?) do
    with :ok <- flow_cancel_many_put_record_values(state, plans, has_record_values?),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_claim_put_state_records(state, plans),
         :ok <- flow_many_put_history(state, plans, "cancelled"),
         :ok <- flow_many_after_terminal(state, plans, "cancelled", has_after_terminal?) do
      :ok
    end
  end

  defp flow_cancel_many_put_record_values(_state, _plans, false), do: :ok

  defp flow_cancel_many_put_record_values(state, plans, :unknown) do
    if flow_many_plans_have_record_values?(plans) do
      flow_cancel_many_put_record_values(state, plans, true)
    else
      :ok
    end
  end

  defp flow_cancel_many_put_record_values(state, plans, true) do
    Enum.reduce_while(plans, :ok, fn {_record, next, attrs}, :ok ->
      refresh_attrs = flow_cancel_refresh_attrs(attrs)

      case do_flow_put_record_values(state, next, attrs) do
        :ok ->
          :ok = flow_refresh_terminal_value_expirations(state, next, refresh_attrs)
          {:cont, :ok}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp do_flow_retention_cleanup(state, attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    limit = Map.get(attrs, :limit, 100)

    if flow_retention_history_projection_pending?(state) do
      flow_retention_zero_counts()
    else
      ets_entries = flow_retention_expired_state_entries(state, now_ms, limit)

      ets_result =
        Enum.reduce_while(ets_entries, flow_retention_zero_counts(), fn entry, {:ok, acc} ->
          case flow_retention_cleanup_entry(state, entry) do
            {:ok, counts} -> {:cont, {:ok, flow_retention_merge_counts(acc, counts)}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)

      with {:ok, acc} <- ets_result do
        seen =
          ets_entries
          |> Enum.map(fn {state_key, _value, _expire_at_ms, _fid, _offset, _value_size} ->
            state_key
          end)
          |> MapSet.new()

        state
        |> flow_retention_expired_lmdb_state_keys(now_ms, max(limit - MapSet.size(seen), 0), seen)
        |> Enum.reduce_while({:ok, acc}, fn state_key, {:ok, acc} ->
          case flow_retention_cleanup_lmdb_state_key(state, state_key, now_ms) do
            {:ok, counts} -> {:cont, {:ok, flow_retention_merge_counts(acc, counts)}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end
    end
  end

  defp flow_retention_history_projection_pending?(state) do
    case HistoryProjector.pending_count(instance_ctx_for_state(state), state.shard_index, 500) do
      {:ok, 0} -> false
      {:ok, count} when is_integer(count) and count > 0 -> true
      {:error, :not_started} -> false
      {:error, {:noproc, _reason}} -> false
      {:error, _reason} -> true
      _other -> true
    end
  end

  defp flow_retention_lmdb_projection_pending?(state) do
    ctx = instance_ctx_for_state(state)
    shard_index = Map.get(state, :shard_index, 0)

    case Map.get(ctx || %{}, :flow_lmdb_writer_pending_ops) do
      ref when is_reference(ref) ->
        shard_index < :atomics.info(ref).size and :atomics.get(ref, shard_index + 1) > 0

      _other ->
        false
    end
  rescue
    _ -> true
  end

  defp flow_retention_expired_lmdb_state_keys(_state, _now_ms, remaining, _seen)
       when remaining <= 0,
       do: []

  defp flow_retention_expired_lmdb_state_keys(state, now_ms, remaining, seen) do
    case Ferricstore.Flow.LMDB.expired_terminal_state_keys(
           flow_lmdb_record_path(state),
           now_ms,
           remaining
         ) do
      {:ok, state_keys} ->
        state_keys
        |> Enum.reject(&MapSet.member?(seen, &1))
        |> Enum.filter(&flow_retention_state_key_owned_by_shard?(state, &1))

      {:error, _reason} ->
        []
    end
  end

    end
  end
end
