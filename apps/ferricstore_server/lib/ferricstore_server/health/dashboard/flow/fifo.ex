defmodule FerricstoreServer.Health.Dashboard.Flow.Fifo do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Flow.Calls

  @flow_terminal_states ~w(completed failed cancelled)
  @member_preview_limit 8

  @spec annotate_state_summaries([map()]) :: [map()]
  def annotate_state_summaries(states) when is_list(states) do
    annotate_state_summaries(states, policy_cache_from_type_states(states))
  end

  def annotate_state_summaries(_states), do: []

  def annotate_state_summaries(states, policies) do
    Enum.map(states, fn state ->
      modes =
        state
        |> Map.get(:logical_states, [Map.get(state, :state)])
        |> Enum.map(&effective_state_mode(policies, Map.get(state, :type), &1))
        |> Enum.uniq()

      mode =
        case modes do
          [mode] -> mode
          _ -> :mixed
        end

      Map.put(state, :mode, mode)
    end)
  end

  @spec lane_summaries([map()]) :: [map()]
  def lane_summaries(records) when is_list(records) do
    lane_snapshot(records).lanes
  end

  def lane_summaries(_records), do: []

  def lane_snapshot(records, policy_records \\ nil) when is_list(records) do
    policies = policy_cache_from_records(policy_records || records)

    %{
      lanes: summarize_lanes(records, policies),
      coverage: policy_coverage(policies),
      policies: policies
    }
  end

  defp summarize_lanes(records, policies) do
    groups =
      records
      |> Enum.filter(&fifo_lane_record?(&1, policies))
      |> Enum.group_by(fn record ->
        {flow_record_type(record), flow_record_logical_state(record),
         flow_record_partition_key(record)}
      end)

    groups
    |> Enum.map(fn {{type, state, partition_key}, lane_records} ->
      summarize_lane(type, state, partition_key, lane_records, map_size(groups) == 1)
    end)
    |> Enum.sort_by(fn lane ->
      {-lane.running, -lane.due, -lane.waiting, lane.type, lane.state, lane.partition_key}
    end)
  end

  @spec effective_state_mode(binary(), binary()) :: :parallel | :fifo | :unknown
  def effective_state_mode(type, state) when is_binary(type) and is_binary(state) do
    %{type => safe_flow_policy(type)}
    |> effective_state_mode(type, state)
  end

  def effective_state_mode(_type, _state), do: :unknown

  defp fifo_lane_record?(record, policies) when is_map(record) do
    state = flow_record_logical_state(record)

    state not in @flow_terminal_states and
      is_binary(flow_record_partition_key(record)) and
      effective_state_mode(policies, flow_record_type(record), state) == :fifo
  end

  defp fifo_lane_record?(_record, _policies), do: false

  defp summarize_lane(type, state, partition_key, records, preview?) do
    running = Enum.filter(records, &(flow_record_state(&1) == "running"))
    live_running = Enum.reject(running, &flow_expired_lease?/1)
    expired_running = Enum.filter(running, &flow_expired_lease?/1)
    waiting_records = Enum.reject(records, &(flow_record_state(&1) == "running"))
    head_waiting = Enum.min_by(waiting_records, &fifo_record_order/1, fn -> nil end)
    blocker = Enum.min_by(live_running, &fifo_record_order/1, fn -> nil end)
    expired_blocker = Enum.min_by(expired_running, &fifo_record_order/1, fn -> nil end)
    head = blocker || expired_blocker || head_waiting
    members = if preview?, do: member_previews(records), else: []

    %{
      type: type,
      state: state,
      partition_key: partition_key,
      mode: :fifo,
      coverage: :sampled,
      members: members,
      members_omitted: max(length(records) - length(members), 0),
      order_known: preview? and Enum.all?(records, &is_integer(flow_record_state_enter_seq(&1))),
      count: length(records),
      running: length(running),
      waiting: length(waiting_records),
      due: Enum.count(waiting_records, &flow_due_now?/1),
      scheduled: Enum.count(waiting_records, &flow_scheduled_future?/1),
      head_id: flow_id_or_nil(head),
      waiting_head_id: flow_id_or_nil(head_waiting),
      head_status: lane_head_status(blocker, expired_blocker, head_waiting),
      blocked_by_id: flow_id_or_nil(blocker || expired_blocker),
      blocked_by_worker: flow_record_worker(blocker || expired_blocker),
      lease_expires_at_ms: flow_record_lease_expires_at_ms(blocker || expired_blocker)
    }
  end

  # Select a fixed-size preview without sorting or copying the entire lane.
  defp member_previews(records) do
    now = System.system_time(:millisecond)

    records
    |> Enum.reduce(:gb_trees.empty(), fn record, selected ->
      role = if flow_record_state(record) == "running", do: 0, else: 1
      key = {role, fifo_record_order(record)}

      if :gb_trees.size(selected) < @member_preview_limit or
           key < elem(:gb_trees.largest(selected), 0) do
        selected = :gb_trees.enter(key, record, selected)

        if :gb_trees.size(selected) > @member_preview_limit do
          {_key, _record, selected} = :gb_trees.take_largest(selected)
          selected
        else
          selected
        end
      else
        selected
      end
    end)
    |> :gb_trees.values()
    |> Enum.map(fn record ->
      %{
        id: flow_record_id(record),
        state_enter_seq: flow_record_state_enter_seq(record),
        worker: flow_record_worker(record),
        lease_expires_at_ms: flow_record_lease_expires_at_ms(record),
        run_at_ms: flow_record_run_at_ms(record),
        status: member_status(record, now)
      }
    end)
  end

  defp member_status(record, now) do
    running? = flow_record_state(record) == "running"
    lease = flow_record_lease_expires_at_ms(record)
    run_at = flow_record_run_at_ms(record)

    cond do
      running? and is_integer(lease) and lease > 0 and lease <= now -> :expired
      running? -> :leased
      is_integer(run_at) and run_at > now -> :scheduled
      is_integer(run_at) -> :due
      true -> :waiting
    end
  end

  defp lane_head_status(%{} = _blocker, _expired_blocker, _head_waiting),
    do: "blocked by active flow"

  defp lane_head_status(nil, %{} = _expired_blocker, _head_waiting),
    do: "blocked by expired lease"

  defp lane_head_status(nil, nil, %{} = head_waiting) do
    if flow_due_now?(head_waiting), do: "head claimable", else: "waiting for schedule"
  end

  defp lane_head_status(nil, nil, nil), do: "idle"

  defp fifo_record_order(record) when is_map(record) do
    case flow_record_state_enter_seq(record) do
      seq when is_integer(seq) -> {0, seq, flow_record_id(record)}
      _ -> {1, flow_record_created_at_ms(record), flow_record_id(record)}
    end
  end

  defp fifo_record_order(_record), do: {2, 0, ""}

  defp flow_id_or_nil(%{} = record), do: flow_record_id(record)
  defp flow_id_or_nil(_record), do: nil

  defp policy_cache_from_records(records) do
    records
    |> Enum.map(&flow_record_type/1)
    |> Enum.reject(&(&1 in ["", "unknown"]))
    |> MapSet.new()
    |> policy_cache()
  end

  defp policy_cache_from_type_states(states) do
    states
    |> Enum.map(&Map.get(&1, :type))
    |> Enum.reject(&(&1 in [nil, "", "unknown"]))
    |> MapSet.new()
    |> policy_cache()
  end

  defp policy_cache(types) do
    Map.new(types, fn type -> {type, safe_flow_policy(type)} end)
  end

  defp safe_flow_policy(type) when is_binary(type) and type != "" do
    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_policy_get(type, []) end,
           flow_dashboard_list_fetch_timeout_ms(),
           :flow_policy
         ) do
      {:ok, {:ok, policy}} when is_map(policy) -> {:ok, policy}
      _ -> {:error, :unavailable}
    end
  rescue
    _ -> {:error, :unavailable}
  catch
    :exit, _ -> {:error, :unavailable}
  end

  defp safe_flow_policy(_type), do: {:error, :unavailable}

  defp policy_coverage(policies) do
    unavailable_types =
      policies
      |> Enum.flat_map(fn {type, result} -> if match?({:ok, _}, result), do: [], else: [type] end)
      |> Enum.sort()

    status =
      cond do
        unavailable_types == [] -> :ok
        length(unavailable_types) == map_size(policies) -> :unavailable
        true -> :partial
      end

    %{status: status, unavailable_types: unavailable_types}
  end

  defp effective_state_mode(policies, type, state)
       when is_map(policies) and is_binary(type) and is_binary(state) do
    case Map.get(policies, type) do
      {:ok, policy} ->
        policy
        |> policy_field(:states, %{})
        |> Map.get(state, %{})
        |> policy_field(:mode, :parallel)
        |> normalize_mode()

      _ ->
        :unknown
    end
  end

  defp effective_state_mode(_policies, _type, _state), do: :unknown

  defp policy_field(map, key, default) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp policy_field(_map, _key, default), do: default

  defp normalize_mode(mode) when mode in [:fifo, "fifo", "FIFO"], do: :fifo
  defp normalize_mode(_mode), do: :parallel
end
