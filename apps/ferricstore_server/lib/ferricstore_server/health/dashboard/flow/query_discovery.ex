defmodule FerricstoreServer.Health.Dashboard.Flow.QueryDiscovery do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Access, as: DashboardAccess
  alias FerricstoreServer.Health.Dashboard.Flow.Calls

  @common_states ~w(queued running completed failed cancelled)
  @max_state_suggestions 64
  @max_type_suggestions 64
  @max_value_suggestions 20
  @max_suggestion_bytes 1_024
  @type_discovery_query "FROM runs WHERE partition_key = @partition ORDER BY updated_at_ms DESC LIMIT 65 RETURN RECORDS (type)"

  @type discovery :: %{
          status: :idle | :ready | :unavailable | :forbidden,
          type: binary() | nil,
          available_types: [binary()],
          types_truncated?: boolean(),
          generation: non_neg_integer() | nil,
          lifecycle_states: [binary()],
          lifecycle_states_truncated?: boolean(),
          workflow_steps: [binary()],
          workflow_steps_truncated?: boolean(),
          indexed_attributes: [binary()],
          indexed_state_meta: binary() | nil,
          attribute_values: [map()],
          state_meta_values: [map()],
          restricted_commands: [binary()],
          denied_scope: :partition | :type | nil
        }

  @type pending ::
          {:ready, discovery()}
          | {:pending, pid(), reference(), reference(), discovery()}

  @spec collect(map(), map(), keyword()) :: discovery()
  def collect(filters, result, opts \\ [])

  def collect(filters, result, opts)
      when is_map(filters) and is_map(result) and is_list(opts) do
    pending = start(filters, opts)
    finish(pending, filters, result)
  end

  def collect(_filters, _result, _opts), do: empty_discovery(nil, @common_states, [])

  @spec start(map(), keyword()) :: pending()
  def start(filters, opts) when is_map(filters) and is_list(opts) do
    type = normalized_name(Map.get(filters, :type))
    username = DashboardAccess.keyspace_acl_username(opts)

    base =
      empty_discovery(
        type,
        suggested_lifecycle_states(filters, []),
        suggested_workflow_steps(filters, [], [])
      )

    cond do
      is_nil(type) and type_discovery_requested?(filters) and
          not discovery_scope_allowed?(filters, opts) ->
        {:ready, forbidden_scope(base, :partition)}

      is_nil(type) and type_discovery_requested?(filters) and
          not DashboardAccess.flow_command_allowed_for_acl?("FLOW.QUERY", username) ->
        {:ready, forbidden_command(base, "FLOW.QUERY")}

      is_nil(type) and type_discovery_requested?(filters) ->
        start_type_discovery(base, filters)

      is_nil(type) ->
        {:ready, base}

      not discovery_scope_allowed?(filters, opts) ->
        {:ready, forbidden_scope(base, :partition)}

      not DashboardAccess.flow_command_allowed_for_acl?("FLOW.POLICY.GET", username) ->
        {:ready, forbidden_command(base, "FLOW.POLICY.GET")}

      not DashboardAccess.flow_scope_allowed_for_acl?(type, username) ->
        {:ready, forbidden_scope(base, :type)}

      true ->
        filters =
          Map.put(
            filters,
            :value_discovery_allowed?,
            DashboardAccess.flow_command_allowed_for_acl?("FLOW.ATTRIBUTE_VALUES", username)
          )

        start_policy_discovery(base, filters)
    end
  end

  def start(_filters, _opts), do: {:ready, empty_discovery(nil, @common_states, [])}

  @spec finish(pending(), map(), map()) :: discovery()
  def finish({:ready, discovery}, filters, result)
      when is_map(discovery) and is_map(filters) and is_map(result) do
    merge_observed_states(discovery, result)
  end

  def finish({:pending, worker, monitor_ref, reply_ref, base}, filters, result)
      when is_pid(worker) and is_reference(monitor_ref) and is_reference(reply_ref) and
             is_map(base) and is_map(filters) and is_map(result) do
    discovery = await_policy_discovery(worker, monitor_ref, reply_ref, base)
    merge_observed_states(discovery, result)
  end

  def finish(_pending, filters, result) when is_map(filters) and is_map(result) do
    type = normalized_name(Map.get(filters, :type))

    base =
      empty_discovery(
        type,
        suggested_lifecycle_states(filters, []),
        suggested_workflow_steps(filters, [], [])
      )

    merge_observed_states(%{base | status: :unavailable}, result)
  end

  def finish(_pending, _filters, _result), do: empty_discovery(nil, @common_states, [])

  defp start_policy_discovery(base, filters) do
    caller = self()
    reply_ref = make_ref()

    {worker, monitor_ref} =
      spawn_monitor(fn ->
        discovery = safe_load_policy_discovery(base, filters)
        send(caller, {reply_ref, discovery})
      end)

    {:pending, worker, monitor_ref, reply_ref, base}
  end

  defp start_type_discovery(base, filters) do
    caller = self()
    reply_ref = make_ref()

    {worker, monitor_ref} =
      spawn_monitor(fn ->
        discovery = safe_load_type_discovery(base, filters)
        send(caller, {reply_ref, discovery})
      end)

    {:pending, worker, monitor_ref, reply_ref, base}
  end

  defp safe_load_policy_discovery(base, filters) do
    load_policy_discovery(base, filters, [])
  rescue
    _error -> %{base | status: :unavailable}
  catch
    _kind, _reason -> %{base | status: :unavailable}
  end

  defp safe_load_type_discovery(base, filters) do
    load_type_discovery(base, filters)
  rescue
    _error -> %{base | status: :unavailable}
  catch
    _kind, _reason -> %{base | status: :unavailable}
  end

  defp await_policy_discovery(worker, monitor_ref, reply_ref, base) do
    receive do
      {^reply_ref, discovery} when is_map(discovery) ->
        Process.demonitor(monitor_ref, [:flush])
        discovery

      {:DOWN, ^monitor_ref, :process, ^worker, _reason} ->
        %{base | status: :unavailable}
    after
      pending_timeout_ms() ->
        Process.exit(worker, :kill)
        await_worker_down(monitor_ref, worker)
        flush_worker_reply(reply_ref)
        %{base | status: :unavailable}
    end
  end

  defp await_worker_down(monitor_ref, worker) do
    receive do
      {:DOWN, ^monitor_ref, :process, ^worker, _reason} -> :ok
    end
  end

  defp flush_worker_reply(reply_ref) do
    receive do
      {^reply_ref, _discovery} -> :ok
    after
      0 -> :ok
    end
  end

  defp pending_timeout_ms do
    case Calls.flow_dashboard_query_discovery_fetch_timeout_ms() do
      timeout when is_integer(timeout) and timeout > 0 -> timeout * 2 + 250
      _invalid -> 2_250
    end
  end

  defp load_type_discovery(base, filters) do
    partition_key = Map.fetch!(filters, :partition_key)

    result =
      Calls.bounded_dashboard_call(
        fn ->
          Calls.flow_dashboard_flow_query(@type_discovery_query, %{"partition" => partition_key})
        end,
        Calls.flow_dashboard_query_discovery_fetch_timeout_ms(),
        :query_discovery_types
      )

    case result do
      {:ok, {:ok, response}} ->
        types = response |> response_rows() |> observed_types()

        %{
          base
          | status: :ready,
            available_types: Enum.take(types, @max_type_suggestions),
            types_truncated?: length(types) > @max_type_suggestions
        }
        |> Map.put(:scope, partition_key)

      _unavailable ->
        %{base | status: :unavailable}
    end
  end

  defp load_policy_discovery(base, filters, observed_states) do
    type = base.type

    result =
      Calls.bounded_dashboard_call(
        fn -> Calls.flow_dashboard_flow_policy_get(type, []) end,
        Calls.flow_dashboard_query_discovery_fetch_timeout_ms(),
        :query_discovery_policy
      )

    case result do
      {:ok, {:ok, policy}} when is_map(policy) ->
        lifecycle_states = suggested_lifecycle_states(filters, [])

        workflow_steps =
          suggested_workflow_steps(filters, observed_states, policy_state_names(policy))

        discovery = %{
          status: :ready,
          type: type,
          available_types: [type],
          types_truncated?: false,
          generation: policy_generation(policy),
          lifecycle_states: Enum.take(lifecycle_states, @max_state_suggestions),
          lifecycle_states_truncated?: length(lifecycle_states) > @max_state_suggestions,
          workflow_steps: Enum.take(workflow_steps, @max_state_suggestions),
          workflow_steps_truncated?: length(workflow_steps) > @max_state_suggestions,
          indexed_attributes: indexed_attributes(policy),
          indexed_state_meta: normalized_name(Map.get(policy, :indexed_state_meta)),
          attribute_values: [],
          state_meta_values: [],
          restricted_commands: [],
          denied_scope: nil
        }

        load_value_discovery(discovery, filters)

      _unavailable ->
        %{base | status: :unavailable}
    end
  end

  defp empty_discovery(type, lifecycle_states, workflow_steps) do
    lifecycle_states_truncated? = length(lifecycle_states) > @max_state_suggestions
    workflow_steps_truncated? = length(workflow_steps) > @max_state_suggestions

    %{
      status: :idle,
      type: type,
      available_types: if(is_binary(type), do: [type], else: []),
      types_truncated?: false,
      generation: nil,
      lifecycle_states: Enum.take(lifecycle_states, @max_state_suggestions),
      lifecycle_states_truncated?: lifecycle_states_truncated?,
      workflow_steps: Enum.take(workflow_steps, @max_state_suggestions),
      workflow_steps_truncated?: workflow_steps_truncated?,
      indexed_attributes: [],
      indexed_state_meta: nil,
      attribute_values: [],
      state_meta_values: [],
      restricted_commands: [],
      denied_scope: nil
    }
  end

  defp merge_observed_states(discovery, result) do
    observed_lifecycle_states = observed_field_values(result, discovery.type, :state)
    observed_workflow_steps = observed_field_values(result, discovery.type, :run_state)

    lifecycle_states =
      Enum.uniq(discovery.lifecycle_states ++ Enum.sort(observed_lifecycle_states))

    workflow_steps = Enum.uniq(discovery.workflow_steps ++ Enum.sort(observed_workflow_steps))

    %{
      discovery
      | lifecycle_states: Enum.take(lifecycle_states, @max_state_suggestions),
        lifecycle_states_truncated?:
          discovery.lifecycle_states_truncated? or
            length(lifecycle_states) > @max_state_suggestions,
        workflow_steps: Enum.take(workflow_steps, @max_state_suggestions),
        workflow_steps_truncated?:
          discovery.workflow_steps_truncated? or length(workflow_steps) > @max_state_suggestions
    }
  end

  defp discovery_scope_allowed?(filters, opts) do
    username = DashboardAccess.keyspace_acl_username(opts)
    scope = Map.get(filters, :partition_key) || "*"
    DashboardAccess.flow_scope_allowed_for_acl?(scope, username)
  end

  defp type_discovery_requested?(filters) do
    Map.get(filters, :inspect) == true and is_binary(Map.get(filters, :partition_key))
  end

  defp load_value_discovery(discovery, %{inspect: true} = filters) do
    requests = value_requests(discovery, filters)

    if requests != [] and Map.get(filters, :value_discovery_allowed?) != true do
      %{discovery | restricted_commands: ["FLOW.ATTRIBUTE_VALUES"]}
    else
      requests
      |> Task.async_stream(&load_value_request/1,
        max_concurrency: 2,
        ordered: false,
        timeout: pending_timeout_ms(),
        on_timeout: :kill_task
      )
      |> Enum.reduce(discovery, fn
        {:ok, {key, values}}, acc -> Map.put(acc, key, values)
        _unavailable, acc -> acc
      end)
    end
  end

  defp load_value_discovery(discovery, _filters), do: discovery

  defp value_requests(discovery, filters) do
    base_opts = [
      count: @max_value_suggestions,
      partition_key: Map.get(filters, :partition_key),
      consistent_projection: true
    ]

    attribute =
      with type when is_binary(type) <- discovery.type,
           key when is_binary(key) <- Map.get(filters, :attribute_key),
           true <- key in discovery.indexed_attributes do
        [
          {:attribute_values, type, key,
           Keyword.put(base_opts, :state, Map.get(filters, :state) || "any"),
           Map.get(filters, :attribute_value_type, "string")}
        ]
      else
        _missing -> []
      end

    state_meta =
      with type when is_binary(type) <- discovery.type,
           state when is_binary(state) <- Map.get(filters, :state_meta_state),
           key when is_binary(key) <- Map.get(filters, :state_meta_key),
           true <- key == discovery.indexed_state_meta do
        [
          {:state_meta_values, type, state, key, base_opts,
           Map.get(filters, :state_meta_value_type, "string")}
        ]
      else
        _missing -> []
      end

    attribute ++ state_meta
  end

  defp load_value_request({:attribute_values, type, key, opts, value_type}) do
    result =
      Calls.bounded_dashboard_call(
        fn -> Calls.flow_dashboard_flow_attribute_values(type, key, opts) end,
        Calls.flow_dashboard_query_discovery_fetch_timeout_ms(),
        :query_discovery_attribute_values
      )

    {:attribute_values, normalize_value_result(result, value_type)}
  end

  defp load_value_request({:state_meta_values, type, state, key, opts, value_type}) do
    result =
      Calls.bounded_dashboard_call(
        fn -> Calls.flow_dashboard_flow_state_meta_values(type, state, key, opts) end,
        Calls.flow_dashboard_query_discovery_fetch_timeout_ms(),
        :query_discovery_state_meta_values
      )

    {:state_meta_values, normalize_value_result(result, value_type)}
  end

  defp normalize_value_result({:ok, {:ok, values}}, value_type) when is_list(values) do
    values
    |> Enum.filter(&valid_value_entry?/1)
    |> Enum.filter(&(scalar_type(Map.get(&1, :value)) == value_type))
    |> Enum.take(@max_value_suggestions)
  end

  defp normalize_value_result(_unavailable, _value_type), do: []

  defp valid_value_entry?(%{value: value, count: count})
       when is_integer(count) and count >= 0,
       do: scalar_type(value) != "invalid"

  defp valid_value_entry?(_entry), do: false

  defp scalar_type(value) when is_binary(value), do: "string"
  defp scalar_type(value) when is_integer(value), do: "integer"
  defp scalar_type(value) when is_float(value), do: "float"
  defp scalar_type(value) when is_boolean(value), do: "boolean"
  defp scalar_type(nil), do: "null"
  defp scalar_type(_invalid), do: "invalid"

  defp forbidden_command(discovery, command) do
    discovery
    |> Map.put(:status, :forbidden)
    |> Map.put(:required_command, command)
  end

  defp forbidden_scope(discovery, scope) when scope in [:partition, :type] do
    %{discovery | status: :forbidden, denied_scope: scope}
  end

  defp response_rows(%{records: rows}) when is_list(rows), do: rows
  defp response_rows(%{"records" => rows}) when is_list(rows), do: rows
  defp response_rows(rows) when is_list(rows), do: rows
  defp response_rows(_response), do: []

  defp observed_types(rows) do
    rows
    |> Enum.map(fn
      row when is_map(row) -> field(row, :type)
      _invalid -> nil
    end)
    |> normalized_names()
    |> Enum.sort()
  end

  defp suggested_lifecycle_states(filters, observed_states) do
    @common_states
    |> Kernel.++(normalized_names([Map.get(filters, :state)]))
    |> Kernel.++(Enum.sort(observed_states))
    |> Enum.uniq()
  end

  defp suggested_workflow_steps(filters, observed_states, policy_states) do
    [Map.get(filters, :run_state), Map.get(filters, :state_meta_state)]
    |> normalized_names()
    |> Kernel.++(Enum.sort(policy_states))
    |> Kernel.++(Enum.sort(observed_states))
    |> Enum.uniq()
  end

  defp observed_field_values(result, type, field_name) do
    rows =
      case Map.get(result, :rows) do
        rows when is_list(rows) -> rows
        _missing_or_invalid -> []
      end

    rows
    |> Enum.flat_map(fn
      row when is_map(row) ->
        if row_matches_type?(row, type), do: [field(row, field_name)], else: []

      _invalid ->
        []
    end)
    |> normalized_names()
  end

  defp row_matches_type?(_row, nil), do: true
  defp row_matches_type?(row, type), do: field(row, :type) == type

  defp policy_state_names(policy) do
    case Map.get(policy, :states) do
      states when is_map(states) -> states |> Map.keys() |> normalized_names()
      _missing -> []
    end
  end

  defp indexed_attributes(policy) do
    policy
    |> Map.get(:indexed_attributes, [])
    |> normalized_names()
    |> Enum.sort()
    |> Enum.uniq()
  end

  defp policy_generation(policy) do
    case Map.get(policy, :generation) do
      generation when is_integer(generation) and generation >= 0 -> generation
      _invalid -> nil
    end
  end

  defp normalized_names(values) when is_list(values) do
    values
    |> Enum.map(&normalized_name/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp normalized_names(_values), do: []

  defp normalized_name(value) when is_binary(value) do
    if String.valid?(value) and byte_size(value) <= @max_suggestion_bytes do
      case String.trim(value) do
        "" -> nil
        normalized -> normalized
      end
    else
      nil
    end
  end

  defp normalized_name(_value), do: nil

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
