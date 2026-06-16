defmodule FerricstoreServer.Health.Dashboard.FlowRecord do
  @moduledoc false

  @flow_terminal_states ~w(completed failed cancelled)

  def flow_record_id(record), do: flow_field_string(record, :id, "")
  def flow_record_type(record), do: flow_field_string(record, :type, "")
  def flow_record_state(record), do: flow_field_string(record, :state, "unknown")

  def flow_record_worker(record) do
    case flow_first_non_empty_binary(record, [:worker, :lease_owner]) do
      worker when is_binary(worker) -> worker
      _ -> nil
    end
  end

  def flow_record_partition_key(record) do
    case flow_field(record, :partition_key, nil) do
      partition_key when is_binary(partition_key) and partition_key != "" -> partition_key
      _ -> nil
    end
  end

  def flow_record_parent_id(record),
    do: flow_first_non_empty_binary(record, [:parent_flow_id, :parent_id])

  def flow_record_root_id(record),
    do: flow_first_non_empty_binary(record, [:root_flow_id, :root_id])

  def flow_record_correlation_id(record),
    do: flow_first_non_empty_binary(record, [:correlation_id, :correlation])

  def flow_record_run_at_ms(record),
    do: flow_first_integer(record, [:run_at_ms, :next_run_at_ms, :due_at_ms])

  def flow_record_updated_at_ms(record),
    do: flow_first_integer(record, [:updated_at_ms, :created_at_ms, :run_at_ms]) || 0

  def flow_record_lease_expires_at_ms(record),
    do: flow_first_integer(record, [:lease_expires_at_ms, :lease_deadline_ms, :lease_until_ms])

  def flow_record_attempts(record) do
    case flow_first_integer(record, [:attempts, :attempt]) do
      attempts when is_integer(attempts) and attempts > 0 -> attempts
      _ -> 0
    end
  end

  def flow_record_max_attempts(record),
    do: flow_first_integer(record, [:max_attempts, :max_retries, :retry_max_retries])

  def flow_detail_url_partition_key(partition_key) when is_binary(partition_key) do
    if Ferricstore.Flow.Keys.auto_partition_key?(partition_key), do: nil, else: partition_key
  end

  def flow_detail_url_partition_key(_partition_key), do: nil

  def flow_value_ref_entries(record, source) do
    base_refs =
      [
        {"payload", flow_field(record, :payload_ref, nil)},
        {"result", flow_field(record, :result_ref, nil)},
        {"error", flow_field(record, :error_ref, nil)}
      ]
      |> Enum.flat_map(fn {label, ref} ->
        case ref do
          ref when is_binary(ref) and ref != "" -> [%{label: label, ref: ref, source: source}]
          _ -> []
        end
      end)

    named_refs =
      record
      |> flow_named_value_refs()
      |> Enum.map(fn {name, ref} -> %{label: to_string(name), ref: ref, source: source} end)
      |> Enum.sort_by(& &1.label)

    base_refs ++ named_refs
  end

  def flow_named_value_refs(record) do
    record
    |> flow_field(:value_refs, flow_field(record, :values_refs, %{}))
    |> normalize_flow_named_value_refs()
  end

  def flow_record_attributes(record) do
    record
    |> flow_field(:attributes, %{})
    |> normalize_flow_attributes()
  end

  def normalize_flow_attributes(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {to_string(key), value} end)
  end

  def normalize_flow_attributes(attrs) when is_binary(attrs) do
    case Jason.decode(attrs) do
      {:ok, decoded} -> normalize_flow_attributes(decoded)
      _ -> %{}
    end
  end

  def normalize_flow_attributes(_attrs), do: %{}

  def normalize_flow_named_value_refs(refs) when is_map(refs) do
    Enum.flat_map(refs, fn {name, ref} ->
      case normalize_flow_value_ref(ref) do
        ref when is_binary(ref) and ref != "" -> [{name, ref}]
        _ -> []
      end
    end)
  end

  def normalize_flow_named_value_refs(refs) when is_binary(refs) do
    case Jason.decode(refs) do
      {:ok, decoded} -> normalize_flow_named_value_refs(decoded)
      _ -> []
    end
  end

  def normalize_flow_named_value_refs(_refs), do: []

  def normalize_flow_value_ref(ref) when is_binary(ref), do: ref
  def normalize_flow_value_ref(ref) when is_map(ref), do: flow_field(ref, :ref, nil)
  def normalize_flow_value_ref(_ref), do: nil

  def flow_waiting_reason(nil), do: "flow not found"

  def flow_waiting_reason(record) do
    state = flow_record_state(record)
    now = System.system_time(:millisecond)
    run_at = flow_record_run_at_ms(record)
    worker = flow_record_worker(record)

    cond do
      state in @flow_terminal_states ->
        "terminal: #{state}"

      state == "running" and flow_expired_lease?(record) ->
        "lease expired; reclaimable by workers"

      state == "running" and is_binary(worker) and worker != "" ->
        "leased by #{worker}"

      state == "running" ->
        "running without worker metadata"

      is_integer(run_at) and run_at > now ->
        "scheduled for future"

      state == "queued" ->
        "due now, waiting for worker claim"

      true ->
        "waiting in #{state}"
    end
  end

  def flow_due_now?(record) do
    state = flow_record_state(record)
    run_at = flow_record_run_at_ms(record)

    state not in @flow_terminal_states and state != "running" and is_integer(run_at) and
      run_at <= System.system_time(:millisecond)
  end

  def flow_scheduled_future?(record) do
    state = flow_record_state(record)
    run_at = flow_record_run_at_ms(record)

    state not in @flow_terminal_states and state != "running" and is_integer(run_at) and
      run_at > System.system_time(:millisecond)
  end

  def flow_retrying?(record),
    do:
      flow_record_attempts(record) > 0 and flow_record_state(record) not in @flow_terminal_states

  def flow_failed?(record), do: flow_record_state(record) == "failed"

  def flow_max_attempts_reached?(record) do
    attempts = flow_record_attempts(record)

    case flow_record_max_attempts(record) do
      max_attempts when is_integer(max_attempts) and max_attempts >= 0 and attempts > 0 ->
        attempts >= max_attempts

      _ ->
        false
    end
  end

  def flow_expired_lease?(record) do
    flow_record_state(record) == "running" and
      case flow_record_lease_expires_at_ms(record) do
        n when is_integer(n) and n > 0 -> n <= System.system_time(:millisecond)
        _ -> false
      end
  end

  def flow_recovery_reason(record) do
    cond do
      flow_expired_lease?(record) -> "expired running lease"
      flow_failed?(record) -> "terminal failed"
      flow_max_attempts_reached?(record) -> "retry attempts exhausted"
      flow_retrying?(record) -> "retrying"
      true -> "needs attention"
    end
  end

  def flow_record_status_label(record) do
    state = flow_record_state(record)

    cond do
      state in @flow_terminal_states -> "terminal"
      flow_expired_lease?(record) -> "expired lease"
      state == "running" -> "running"
      flow_retrying?(record) -> "retrying"
      flow_scheduled_future?(record) -> "scheduled"
      flow_due_now?(record) -> "due"
      true -> "active"
    end
  end

  def flow_retention_until_ms(record) do
    flow_first_integer(record, [
      :terminal_retention_until_ms,
      :retention_until_ms,
      :expires_at_ms,
      :expire_at_ms
    ])
  end

  def flow_first_non_empty_binary(record, keys) do
    Enum.find_value(keys, fn key ->
      case flow_field(record, key, nil) do
        value when is_binary(value) and value != "" -> value
        _ -> nil
      end
    end)
  end

  def flow_first_integer(record, keys) do
    Enum.find_value(keys, fn key ->
      case flow_field(record, key, nil) do
        n when is_integer(n) -> n
        _ -> nil
      end
    end)
  end

  def flow_field(map, key, default) when is_map(map) and is_atom(key) do
    string_key = Atom.to_string(key)

    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, string_key, default)
    end
  end

  def flow_field(_map, _key, default), do: default

  def flow_field_string(map, key, default) do
    case flow_field(map, key, default) do
      value when is_binary(value) -> value
      value when is_atom(value) -> Atom.to_string(value)
      value when is_integer(value) -> Integer.to_string(value)
      _ -> default
    end
  end
end
