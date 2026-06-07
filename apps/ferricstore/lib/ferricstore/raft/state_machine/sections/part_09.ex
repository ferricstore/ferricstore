defmodule Ferricstore.Raft.StateMachine.Sections.Part09 do
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

  defp flow_create_existing_state(state, %{idempotent: true}, state_key) do
    flow_read_record_by_key(state, state_key)
  end

  defp flow_create_existing_state(state, _attrs, state_key) do
    if flow_state_key_present?(state, state_key), do: :present, else: nil
  end

  defp do_flow_spawn_children(
         state,
         %{id: parent_id, partition_key: partition_key, children: [_ | _] = children} = attrs
       ) do
    now_ms = flow_attrs_now_ms(attrs)

    with {:ok, parent} <- flow_require_record(state, parent_id, partition_key),
         :ok <- flow_require_parent_partition(parent, partition_key),
         child_attrs = flow_spawn_child_attrs(parent, children),
         :ok <- flow_many_partitions_valid?(state, child_attrs),
         :ok <- flow_create_many_unique?(child_attrs),
         {:ok, group_state} <- flow_child_group_spawn_state(parent, attrs, child_attrs) do
      case group_state do
        :idempotent ->
          :ok

        :new ->
          with :ok <- flow_require_expected_state(parent, Map.get(attrs, :from_state)),
               :ok <- flow_require_fencing_token(parent, Map.fetch!(attrs, :fencing_token)),
               :ok <- flow_require_transition_lease(parent, Map.get(attrs, :lease_token)),
               :ok <- flow_require_active_parent(parent),
               :ok <- flow_require_spawn_wait_state(parent, attrs),
               {:ok, _child_records, child_plans} <- flow_create_many_prepare(state, child_attrs),
               {:ok, next_parent} <- flow_prepare_spawn_parent(parent, attrs, child_attrs, now_ms),
               :ok <- flow_validate_record_keys(next_parent),
               :ok <-
                 flow_apply_parent_update(state, parent, next_parent, "children_spawned", now_ms),
               :ok <- flow_create_many_apply(state, child_plans) do
            :ok
          end
      end
    end
  end

  defp do_flow_spawn_children(_state, _attrs),
    do: {:error, "ERR flow children must be a non-empty list"}

  defp do_flow_cross_spawn_children(
         state,
         %{id: parent_id, partition_key: partition_key, children: [_ | _] = children} = attrs
       ) do
    parent_state = cross_shard_state_for_key(state, FlowKeys.state_key(parent_id, partition_key))
    now_ms = flow_attrs_now_ms(attrs)

    with {:ok, parent} <- flow_require_record(parent_state, parent_id, partition_key),
         :ok <- flow_require_parent_partition(parent, partition_key),
         child_attrs = flow_spawn_child_attrs(parent, children),
         :ok <- flow_many_partition_keys_present?(child_attrs),
         :ok <- flow_create_many_unique?(child_attrs),
         {:ok, group_state} <- flow_child_group_spawn_state(parent, attrs, child_attrs) do
      case group_state do
        :idempotent ->
          :ok

        :new ->
          with :ok <- flow_require_expected_state(parent, Map.get(attrs, :from_state)),
               :ok <- flow_require_fencing_token(parent, Map.fetch!(attrs, :fencing_token)),
               :ok <- flow_require_transition_lease(parent, Map.get(attrs, :lease_token)),
               :ok <- flow_require_active_parent(parent),
               :ok <- flow_require_spawn_wait_state(parent, attrs),
               {:ok, child_apply_groups} <- flow_cross_create_many_prepare(state, child_attrs),
               {:ok, next_parent} <- flow_prepare_spawn_parent(parent, attrs, child_attrs, now_ms),
               :ok <- flow_validate_record_keys(next_parent),
               :ok <-
                 flow_apply_parent_update(
                   parent_state,
                   parent,
                   next_parent,
                   "children_spawned",
                   now_ms
                 ),
               :ok <- flow_cross_create_many_apply(child_apply_groups) do
            :ok
          end
      end
    end
  end

  defp do_flow_cross_spawn_children(_state, _attrs),
    do: {:error, "ERR flow children must be a non-empty list"}

  defp flow_cross_create_many_prepare(state, attrs_list) do
    attrs_list
    |> Enum.group_by(fn attrs ->
      key = FlowKeys.state_key(Map.fetch!(attrs, :id), Map.fetch!(attrs, :partition_key))
      cross_shard_state_for_key(state, key)
    end)
    |> Enum.reduce_while({:ok, []}, fn {child_state, shard_attrs}, {:ok, acc} ->
      with :ok <- flow_many_partitions_valid?(child_state, shard_attrs),
           {:ok, _records, plans} <- flow_create_many_prepare(child_state, shard_attrs) do
        {:cont, {:ok, [{child_state, plans} | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, groups} -> {:ok, Enum.reverse(groups)}
      {:error, _reason} = error -> error
    end
  end

  defp flow_cross_create_many_apply(groups) do
    Enum.reduce_while(groups, :ok, fn {child_state, plans}, :ok ->
      case flow_create_many_apply(child_state, plans) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_create_record(state, %{id: id, type: type, state: flow_state} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    run_at_ms = Map.get(attrs, :run_at_ms, now_ms)
    priority = Map.get(attrs, :priority, 0)
    retention = flow_retention_for_create(state, attrs)

    flow_create_record_with_retention(
      attrs,
      id,
      type,
      flow_state,
      now_ms,
      run_at_ms,
      priority,
      retention
    )
  end

  defp flow_create_record_cached_retention(state, attrs, retention_cache) do
    key = flow_create_retention_cache_key(attrs)

    case Map.fetch(retention_cache, key) do
      {:ok, retention} ->
        {flow_create_record_with_resolved_retention(attrs, retention), retention_cache}

      :error ->
        retention = flow_retention_for_create(state, attrs)

        {flow_create_record_with_resolved_retention(attrs, retention),
         Map.put(retention_cache, key, retention)}
    end
  end

  defp flow_create_record_with_resolved_retention(
         %{id: id, type: type, state: flow_state} = attrs,
         retention
       ) do
    now_ms = flow_attrs_now_ms(attrs)
    run_at_ms = Map.get(attrs, :run_at_ms, now_ms)
    priority = Map.get(attrs, :priority, 0)

    flow_create_record_with_retention(
      attrs,
      id,
      type,
      flow_state,
      now_ms,
      run_at_ms,
      priority,
      retention
    )
  end

  defp flow_create_record_with_retention(
         attrs,
         id,
         type,
         flow_state,
         now_ms,
         run_at_ms,
         priority,
         retention
       ) do
    partition_key = Map.get(attrs, :partition_key)

    %{
      id: id,
      type: type,
      state: flow_state,
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: now_ms,
      updated_at_ms: now_ms,
      next_run_at_ms: run_at_ms,
      priority: priority,
      ttl_ms: nil,
      retention_ttl_ms: Map.fetch!(retention, :ttl_ms),
      terminal_retention_until_ms: nil,
      history_hot_max_events: Map.fetch!(retention, :history_hot_max_events),
      history_max_events: Map.fetch!(retention, :history_max_events),
      partition_key: partition_key,
      payload_ref: flow_value_ref(attrs, :payload, id, 1, partition_key),
      value_refs: flow_new_named_value_refs(attrs, id, 1, partition_key),
      parent_flow_id: Map.get(attrs, :parent_flow_id),
      parent_partition_key: Map.get(attrs, :parent_partition_key),
      root_flow_id: Map.get(attrs, :root_flow_id) || id,
      correlation_id: Map.get(attrs, :correlation_id),
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      child_groups: %{}
    }
    |> flow_stamp_terminal_retention(now_ms)
  end

  defp flow_create_retention_cache_key(attrs) do
    {
      Map.get(attrs, :type),
      Map.get(attrs, :state),
      Map.get(attrs, :retention_ttl_ms),
      Map.get(attrs, :history_hot_max_events),
      Map.get(attrs, :history_max_events)
    }
  end

  defp flow_require_parent_partition(parent, partition_key) do
    if Map.get(parent, :partition_key) == partition_key do
      :ok
    else
      {:error, "ERR flow parent partition mismatch"}
    end
  end

  defp flow_require_active_parent(%{state: state}) do
    if Ferricstore.Flow.LMDB.terminal_state?(state) do
      {:error, "ERR flow parent is terminal"}
    else
      :ok
    end
  end

  defp flow_require_spawn_wait_state(_parent, %{wait: wait, wait_state: wait_state})
       when wait in [:all, :any] do
    if is_binary(wait_state) and wait_state != "" do
      :ok
    else
      {:error, "ERR flow wait_state is required when waiting for children"}
    end
  end

  defp flow_require_spawn_wait_state(_parent, _attrs), do: :ok

  defp flow_child_group_spawn_state(parent, attrs, child_attrs) do
    group_id = Map.fetch!(attrs, :group_id)
    requested_hash = flow_child_group_request_hash(attrs, child_attrs)

    case Map.get(flow_child_groups(parent), group_id) do
      nil ->
        {:ok, :new}

      %{"request_hash" => ^requested_hash} ->
        {:ok, :idempotent}

      _existing ->
        {:error, "ERR flow child group idempotency conflict"}
    end
  end

  defp flow_spawn_child_attrs(parent, children) do
    root_flow_id = Map.get(parent, :root_flow_id) || Map.fetch!(parent, :id)
    parent_id = Map.fetch!(parent, :id)
    partition_key = Map.get(parent, :partition_key)

    Enum.map(children, fn attrs ->
      attrs
      |> Map.put(:parent_flow_id, parent_id)
      |> Map.put(:parent_partition_key, partition_key)
      |> Map.put(:root_flow_id, root_flow_id)
      |> Map.put_new(:partition_key, partition_key)
    end)
  end

  defp flow_prepare_spawn_parent(parent, attrs, child_attrs, now_ms) do
    group = flow_new_child_group(attrs, child_attrs)
    groups = Map.put(flow_child_groups(parent), Map.fetch!(attrs, :group_id), group)
    state = flow_spawn_parent_state(parent, attrs)

    next =
      parent
      |> Map.merge(%{
        state: state,
        version: Map.fetch!(parent, :version) + 1,
        updated_at_ms: now_ms,
        next_run_at_ms: nil,
        ttl_ms: nil,
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        child_groups: groups
      })
      |> flow_stamp_terminal_retention(now_ms)

    {:ok, next}
  end

  defp flow_spawn_parent_state(_parent, %{wait: :none, exhaust_to: %{"success" => state}}),
    do: state

  defp flow_spawn_parent_state(_parent, %{wait: wait, wait_state: wait_state})
       when wait in [:all, :any] and is_binary(wait_state) and wait_state != "",
       do: wait_state

  defp flow_spawn_parent_state(parent, _attrs), do: Map.fetch!(parent, :state)

  defp flow_new_child_group(attrs, child_attrs) do
    children =
      child_attrs
      |> Enum.map(fn %{id: id} -> {id, "running"} end)
      |> Map.new()

    child_partitions =
      child_attrs
      |> Enum.map(fn %{id: id, partition_key: child_partition} -> {id, child_partition} end)
      |> Map.new()

    resolved =
      case Map.fetch!(attrs, :wait) do
        :none -> "success"
        :all -> nil
        :any -> nil
      end

    %{
      "wait" => Atom.to_string(Map.fetch!(attrs, :wait)),
      "on_child_failed" => Atom.to_string(Map.fetch!(attrs, :on_child_failed)),
      "on_parent_closed" => Atom.to_string(Map.fetch!(attrs, :on_parent_closed)),
      "exhaust_to" => Map.fetch!(attrs, :exhaust_to),
      "request_hash" => flow_child_group_request_hash(attrs, child_attrs),
      "children" => children,
      "child_partitions" => child_partitions,
      "summary" => %{
        "total" => map_size(children),
        "completed" => 0,
        "failed" => 0,
        "cancelled" => 0
      },
      "results" => %{},
      "resolved" => resolved
    }
  end

  defp flow_child_group_request_hash(attrs, child_attrs) do
    request = %{
      wait: Map.fetch!(attrs, :wait),
      wait_state: Map.get(attrs, :wait_state),
      on_child_failed: Map.fetch!(attrs, :on_child_failed),
      on_parent_closed: Map.fetch!(attrs, :on_parent_closed),
      exhaust_to: Map.fetch!(attrs, :exhaust_to),
      children:
        child_attrs
        |> Enum.map(&flow_child_group_request_child/1)
        |> Enum.sort_by(fn child ->
          {Map.fetch!(child, :id), Map.fetch!(child, :partition_key)}
        end)
    }

    :crypto.hash(:sha256, :erlang.term_to_binary(request))
    |> Base.encode16(case: :lower)
  end

  defp flow_child_group_request_child(attrs) do
    attrs
    |> Map.take([
      :id,
      :type,
      :state,
      :partition_key,
      :run_at_ms,
      :priority,
      :retention_ttl_ms,
      :history_hot_max_events,
      :history_max_events,
      :correlation_id,
      :payload_ref,
      :payload
    ])
    |> Map.put(:payload_hash, flow_child_group_payload_hash(Map.get(attrs, :payload)))
    |> Map.delete(:payload)
  end

  defp flow_child_group_payload_hash(nil), do: nil

  defp flow_child_group_payload_hash(payload) do
    :crypto.hash(:sha256, :erlang.term_to_binary(payload))
    |> Base.encode16(case: :lower)
  end

  defp flow_child_groups(record) do
    case Map.get(record, :child_groups) do
      groups when is_map(groups) -> groups
      _ -> %{}
    end
  end

  defp flow_apply_parent_update(state, record, next, event, now_ms) do
    plans = [{record, next}]

    with :ok <- flow_transition_move_indexes(state, plans),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, Map.get(next, :partition_key)),
             next
           ),
         :ok <- flow_history_put_planned(state, record, next, event, now_ms),
         :ok <- flow_after_history_put(state, next) do
      :ok
    end
  end

  defp flow_retention_for_create(state, attrs) do
    flow_policy = flow_read_policy(state, Map.get(attrs, :type))

    override =
      %{}
      |> maybe_put_retention_override(:ttl_ms, Map.get(attrs, :retention_ttl_ms))
      |> maybe_put_retention_override(
        :history_hot_max_events,
        Map.get(attrs, :history_hot_max_events)
      )
      |> maybe_put_retention_override(
        :history_max_events,
        Map.get(attrs, :history_max_events)
      )

    RetryPolicy.resolve_retention(flow_policy, Map.get(attrs, :state), override)
  end

  defp maybe_put_retention_override(map, _key, nil), do: map
  defp maybe_put_retention_override(map, key, value), do: Map.put(map, key, value)

  defp flow_stamp_terminal_retention(
         %{state: state, terminal_retention_until_ms: nil} = record,
         _now_ms
       )
       when state != "completed" and state != "failed" and state != "cancelled" do
    record
  end

  defp flow_stamp_terminal_retention(record, now_ms) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      retention_ttl_ms = Map.get(record, :retention_ttl_ms)

      if is_integer(retention_ttl_ms) and retention_ttl_ms > 0 do
        retention_start_ms = max(now_ms, apply_now_ms())
        Map.put(record, :terminal_retention_until_ms, retention_start_ms + retention_ttl_ms)
      else
        Map.put(record, :terminal_retention_until_ms, nil)
      end
    else
      Map.put(record, :terminal_retention_until_ms, nil)
    end
  end

  defp flow_create_duplicate_result(state, existing, %{idempotent: true} = attrs) do
    if flow_create_idempotent_match?(state, existing, attrs) do
      {:ok, existing}
    else
      {:error, "ERR flow idempotency conflict"}
    end
  end

  defp flow_create_duplicate_result(_state, _existing, _attrs),
    do: {:error, "ERR flow already exists"}

  defp flow_value_ref(attrs, kind, id, version, partition_key, existing_ref \\ nil) do
    cond do
      Map.has_key?(attrs, kind) ->
        FlowKeys.value_key(id, kind, version, partition_key)

      ref = Map.get(attrs, flow_value_ref_field(kind)) ->
        ref

      true ->
        existing_ref
    end
  end

  defp flow_value_ref_field(:payload), do: :payload_ref
  defp flow_value_ref_field(:result), do: :result_ref
  defp flow_value_ref_field(:error), do: :error_ref

  defp flow_new_named_value_refs(attrs, id, version, partition_key) do
    if flow_attrs_named_value_refs_empty?(attrs) do
      %{}
    else
      case flow_named_value_refs(%{}, attrs, id, version, partition_key) do
        {:ok, refs} -> refs
        {:error, _reason} -> %{}
      end
    end
  end

  defp flow_attrs_named_value_refs_empty?(attrs) do
    flow_empty_named_ref_input?(Map.get(attrs, :values)) and
      flow_empty_named_ref_input?(Map.get(attrs, :value_refs)) and
      flow_empty_named_ref_input?(Map.get(attrs, :drop_values)) and
      flow_empty_named_ref_input?(Map.get(attrs, :override_values))
  end

  defp flow_empty_named_ref_input?(nil), do: true
  defp flow_empty_named_ref_input?(map) when is_map(map), do: map_size(map) == 0
  defp flow_empty_named_ref_input?([]), do: true
  defp flow_empty_named_ref_input?(""), do: true
  defp flow_empty_named_ref_input?(_value), do: false

  defp flow_named_value_refs(record_or_refs, attrs, id, _version, partition_key) do
    if flow_attrs_named_value_refs_empty?(attrs) do
      flow_named_value_refs_empty_fast_path(record_or_refs)
    else
      values = flow_named_values(Map.get(attrs, :values))

      refs =
        record_or_refs
        |> flow_record_value_refs()
        |> flow_drop_named_value_refs(Map.get(attrs, :drop_values))
        |> flow_merge_external_value_refs(Map.get(attrs, :value_refs))

      value_names = flow_named_value_names(values)
      overrides = flow_named_value_name_set(Map.get(attrs, :override_values))

      Enum.reduce_while(value_names, {:ok, refs}, fn name, {:ok, acc} ->
        value = Map.fetch!(values, name)
        digest = flow_value_digest(value)
        existing = Map.get(acc, name)

        cond do
          flow_named_value_same_digest?(existing, digest) ->
            {:cont, {:ok, acc}}

          not is_nil(existing) and not MapSet.member?(overrides, name) ->
            {:halt,
             {:error,
              "ERR flow value #{name} already exists with different digest; use OVERRIDE true"}}

          true ->
            next_version = flow_named_value_next_version(existing)
            ref = FlowKeys.value_key(id <> ":" <> name, :shared, next_version, partition_key)

            {:cont, {:ok, Map.put(acc, name, %{ref: ref, version: next_version, digest: digest})}}
        end
      end)
    end
  end

  defp flow_named_value_refs_empty_fast_path(%{value_refs: refs}) do
    {:ok, flow_normalize_value_refs(refs)}
  end

  defp flow_named_value_refs_empty_fast_path(_record_or_refs), do: {:ok, %{}}

  defp flow_record_value_refs(%{value_refs: refs}) do
    flow_normalize_value_refs(refs)
  end

  defp flow_record_value_refs(_record), do: %{}

  defp flow_put_record_value_refs(record, refs) when is_map(refs) and map_size(refs) > 0,
    do: Map.put(record, :value_refs, refs)

  defp flow_put_record_value_refs(record, _refs), do: Map.delete(record, :value_refs)

  defp flow_normalize_value_refs(refs) when is_map(refs) do
    Enum.reduce(refs, %{}, fn
      {name, %{ref: ref} = entry}, acc when is_binary(name) and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{
          ref: ref,
          version: flow_named_value_version(Map.get(entry, :version)),
          digest: flow_named_value_digest_value(Map.get(entry, :digest))
        })

      {name, %{"ref" => ref} = entry}, acc
      when is_binary(name) and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{
          ref: ref,
          version: flow_named_value_version(Map.get(entry, "version")),
          digest: flow_named_value_digest_value(Map.get(entry, "digest"))
        })

      {name, ref}, acc when is_binary(name) and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{ref: ref, version: nil, digest: nil})

      _entry, acc ->
        acc
    end)
  end

  defp flow_normalize_value_refs(refs) when is_binary(refs) do
    case Jason.decode(refs) do
      {:ok, decoded} -> flow_normalize_value_refs(decoded)
      _ -> %{}
    end
  end

  defp flow_normalize_value_refs(_refs), do: %{}

  defp flow_merge_external_value_refs(refs, external_refs) when is_map(external_refs) do
    Map.merge(refs, flow_normalize_value_refs(external_refs))
  end

  defp flow_merge_external_value_refs(refs, external_refs) when is_list(external_refs) do
    Map.merge(refs, flow_normalize_value_refs(Map.new(external_refs)))
  rescue
    _ -> refs
  end

  defp flow_merge_external_value_refs(refs, _external_refs), do: refs

  defp flow_drop_named_value_refs(refs, drops) do
    drops
    |> flow_named_value_name_set()
    |> Enum.reduce(refs, &Map.delete(&2, &1))
  end

  defp flow_named_value_names(values) when is_map(values) do
    values
    |> Map.keys()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  defp flow_named_value_names(values) when is_list(values) do
    values
    |> Enum.flat_map(fn
      {name, _value} when is_binary(name) and name != "" -> [name]
      _other -> []
    end)
    |> Enum.uniq()
  end

  defp flow_named_value_names(_values), do: []

  defp flow_named_values(values) when is_map(values) do
    values
    |> Enum.reduce(%{}, fn
      {name, value}, acc when is_binary(name) and name != "" -> Map.put(acc, name, value)
      _other, acc -> acc
    end)
  end

  defp flow_named_values(values) when is_list(values) do
    values
    |> Enum.reduce(%{}, fn
      {name, value}, acc when is_binary(name) and name != "" -> Map.put(acc, name, value)
      _other, acc -> acc
    end)
  end

  defp flow_named_values(_values), do: %{}

  defp flow_named_value_name_set(nil), do: MapSet.new()

  defp flow_named_value_name_set(value) when is_binary(value) and value != "",
    do: MapSet.new([value])

  defp flow_named_value_name_set(values) when is_list(values) do
    values
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> MapSet.new()
  end

  defp flow_named_value_name_set(_values), do: MapSet.new()

  defp flow_named_value_same_digest?(%{digest: digest}, digest) when is_binary(digest), do: true
  defp flow_named_value_same_digest?(_entry, _digest), do: false

  defp flow_named_value_next_version(%{version: version})
       when is_integer(version) and version > 0,
       do: version + 1

  defp flow_named_value_next_version(_entry), do: 1

  defp flow_named_value_version(version) when is_integer(version), do: version

  defp flow_named_value_version(version) when is_binary(version) do
    case Integer.parse(version) do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  defp flow_named_value_version(_version), do: nil

  defp flow_named_value_digest_value(value) when is_binary(value) and value != "", do: value
  defp flow_named_value_digest_value(_value), do: nil

  defp flow_value_digest(value) do
    :crypto.hash(:sha256, Flow.encode_value(value))
    |> Base.encode16(case: :lower)
  end

  defp flow_create_idempotent_match?(state, existing, attrs) do
    id = Map.fetch!(attrs, :id)
    partition_key = Map.get(attrs, :partition_key)
    retention = flow_retention_for_create(state, attrs)

    comparable_attrs = %{
      id: id,
      type: Map.get(attrs, :type),
      state: Map.get(attrs, :state),
      partition_key: partition_key,
      payload_ref: flow_value_ref(attrs, :payload, id, 1, partition_key),
      parent_flow_id: Map.get(attrs, :parent_flow_id),
      root_flow_id: Map.get(attrs, :root_flow_id) || id,
      correlation_id: Map.get(attrs, :correlation_id),
      priority: Map.get(attrs, :priority, 0),
      ttl_ms: nil,
      retention_ttl_ms: Map.fetch!(retention, :ttl_ms),
      history_hot_max_events: Map.fetch!(retention, :history_hot_max_events),
      history_max_events: Map.fetch!(retention, :history_max_events)
    }

    Enum.all?(comparable_attrs, fn {key, value} -> Map.get(existing, key) == value end) and
      flow_create_idempotent_payload_match?(state, existing, attrs)
  end

  defp flow_create_idempotent_payload_match?(state, existing, %{payload: payload}) do
    with ref when is_binary(ref) and ref != "" <- Map.get(existing, :payload_ref),
         {:ok, expected} <- flow_idempotent_expected_encoded_value(state, payload),
         [stored] when is_binary(stored) <- sm_store_batch_get(state, [ref], &sm_file_path/2) do
      stored == expected
    else
      _ -> false
    end
  end

  defp flow_create_idempotent_payload_match?(_state, _existing, _attrs), do: true

  defp flow_idempotent_expected_encoded_value(state, payload) do
    case BlobCommand.flow_blob_value_ref(payload) do
      {:ok, encoded_ref} -> materialize_blob_ref(state, encoded_ref)
      :error -> {:ok, Flow.encode_value(payload)}
    end
  end

  defp flow_many_partitions_valid?(state, attrs_list),
    do: flow_many_partitions_valid?(state, attrs_list, nil)

  defp flow_many_partitions_valid?(state, attrs_list, stamped_shard) do
    with :ok <- flow_many_partition_keys_present?(attrs_list) do
      flow_many_same_state_machine_shard?(state, attrs_list, stamped_shard)
    end
  end

  defp flow_many_partition_keys_present?(attrs_list) do
    if Enum.all?(attrs_list, fn attrs ->
         partition_key = Map.get(attrs, :partition_key)
         is_binary(partition_key) and partition_key != ""
       end) do
      :ok
    else
      {:error, "ERR flow partition_key is required"}
    end
  end

  defp flow_many_same_state_machine_shard?(
         %{instance_ctx: ctx, shard_index: shard_index},
         attrs_list,
         stamped_shard
       )
       when is_map(ctx) do
    cond do
      stamped_shard == shard_index ->
        :ok

      is_integer(stamped_shard) ->
        {:error, "ERR flow batch crosses shards"}

      flow_attrs_same_stamped_shard?(attrs_list, shard_index) ->
        :ok

      Enum.all?(attrs_list, fn %{id: id, partition_key: partition_key} ->
        key = FlowKeys.state_key(id, partition_key)
        Router.shard_for(ctx, key) == shard_index
      end) ->
        :ok

      true ->
        {:error, "ERR flow batch crosses shards"}
    end
  rescue
    _ -> :ok
  end

  defp flow_many_same_state_machine_shard?(_state, _attrs_list, _stamped_shard), do: :ok

  defp flow_attrs_same_stamped_shard?([_ | _] = attrs_list, shard_index) do
    Enum.all?(attrs_list, &(Map.get(&1, @flow_shard_marker) == shard_index))
  end

  defp flow_attrs_same_stamped_shard?(_attrs_list, _shard_index), do: false

  defp flow_create_many_unique?(attrs_list) do
    {_seen, result} =
      Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
        if MapSet.member?(seen, id) do
          {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
        else
          {:cont, {MapSet.put(seen, id), :ok}}
        end
      end)

    result
  end

  defp flow_validate_create_attrs_list(attrs_list) do
    Enum.reduce_while(attrs_list, :ok, fn attrs, :ok ->
      case flow_validate_create_attrs(attrs) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_validate_create_attrs(%{id: id} = attrs) when is_binary(id) and id != "" do
    cond do
      not flow_non_empty_binary?(Map.get(attrs, :type)) ->
        {:error, "ERR flow type must be a non-empty string"}

      not flow_non_empty_binary?(Map.get(attrs, :state)) ->
        {:error, "ERR flow state must be a non-empty string"}

      true ->
        :ok
    end
  end

  defp flow_validate_create_attrs(_attrs),
    do: {:error, "ERR flow id must be a non-empty string"}

  defp flow_non_empty_binary?(value), do: is_binary(value) and value != ""

  defp flow_create_many_prepare(state, attrs_list) do
    with :ok <- flow_validate_create_attrs_list(attrs_list) do
      existing_records = flow_create_many_existing_states(state, attrs_list)

      attrs_list
      |> Enum.zip(existing_records)
      |> Enum.reduce_while({:ok, [], [], %{}}, fn
        {%{id: _id} = attrs, existing}, {:ok, acc, new_acc, retention_cache} ->
          case existing do
            nil ->
              {record, retention_cache} =
                flow_create_record_cached_retention(state, attrs, retention_cache)

              case flow_validate_record_keys(record) do
                :ok ->
                  {:cont, {:ok, [record | acc], [{record, attrs} | new_acc], retention_cache}}

                {:error, _reason} = error ->
                  {:halt, error}
              end

            :present ->
              {:halt, {:error, "ERR flow already exists"}}

            existing ->
              case flow_create_duplicate_result(state, existing, attrs) do
                {:ok, existing} -> {:cont, {:ok, [existing | acc], new_acc, retention_cache}}
                {:error, _reason} = error -> {:halt, error}
              end
          end
      end)
      |> case do
        {:ok, records, new_records, _retention_cache} ->
          {:ok, Enum.reverse(records), Enum.reverse(new_records)}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp flow_create_many_existing_states(state, attrs_list) do
    keys = flow_state_keys_for_attrs(attrs_list)
    present = flow_state_keys_present(state, keys)

    attrs_list
    |> Enum.zip(Enum.zip(keys, present))
    |> Enum.map(fn
      {%{idempotent: true}, {key, true}} -> flow_read_record_by_key(state, key)
      {%{idempotent: true}, {_key, false}} -> nil
      {_attrs, {_key, true}} -> :present
      {_attrs, {_key, false}} -> nil
    end)
  end

    end
  end
end
