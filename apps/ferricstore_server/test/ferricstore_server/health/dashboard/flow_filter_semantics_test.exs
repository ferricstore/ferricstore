defmodule FerricstoreServer.Health.Dashboard.FlowFilterSemanticsTest do
  use FerricstoreServer.Test.DashboardCase

  alias Ferricstore.Flow.Query.Limits
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous_query = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)
    previous_stats = Application.get_env(:ferricstore, :flow_dashboard_flow_stats_fun)

    previous_policy_get =
      Application.get_env(:ferricstore, :flow_dashboard_flow_policy_get_fun)

    previous_attribute_values =
      Application.get_env(:ferricstore, :flow_dashboard_flow_attribute_values_fun)

    previous_state_meta_values =
      Application.get_env(:ferricstore, :flow_dashboard_flow_state_meta_values_fun)

    on_exit(fn -> restore_env(:flow_dashboard_flow_query_fun, previous_query) end)
    on_exit(fn -> restore_env(:flow_dashboard_flow_stats_fun, previous_stats) end)

    on_exit(fn ->
      restore_env(:flow_dashboard_flow_policy_get_fun, previous_policy_get)
    end)

    on_exit(fn ->
      restore_env(:flow_dashboard_flow_attribute_values_fun, previous_attribute_values)
    end)

    on_exit(fn ->
      restore_env(:flow_dashboard_flow_state_meta_values_fun, previous_state_meta_values)
    end)

    :ok
  end

  test "guided query controls expose only predicates honored by each query kind" do
    stats_html = render_query_controls("stats")
    failures_html = render_query_controls("failures")
    history_html = render_query_controls("history")

    refute field_tag(stats_html, "type") =~ "hidden"
    refute input_tag(stats_html, "type") =~ "disabled"
    assert required_input?(input_tag(stats_html, "type"))
    assert required_input?(input_tag(stats_html, "state"))
    assert input_tag(stats_html, "state") =~ ~s(placeholder="required")
    assert field_tag(stats_html, "from") =~ "hidden"
    assert input_tag(stats_html, "from") =~ "disabled"
    assert field_tag(stats_html, "limit") =~ "hidden"
    assert input_tag(stats_html, "limit") =~ "disabled"
    refute required_input?(input_tag(stats_html, "partition_key"))
    assert input_tag(stats_html, "partition_key") =~ ~s(placeholder="optional")

    assert field_tag(failures_html, "state") =~ "hidden"
    assert input_tag(failures_html, "state") =~ "disabled"
    assert required_input?(input_tag(failures_html, "type"))
    assert required_input?(input_tag(failures_html, "partition_key"))
    assert input_tag(failures_html, "partition_key") =~ ~s(placeholder="required")

    assert required_input?(input_tag(history_html, "id"))
    refute required_input?(input_tag(history_html, "partition_key"))
    assert input_tag(history_html, "partition_key") =~ ~s(placeholder="optional")

    assert stats_html =~ ~s(max="#{Limits.max_results()}")
  end

  test "query modes and actions expose complete keyboard and contextual semantics" do
    guided_html = render_query_page("search", :guided)

    assert guided_html =~
             ~s(id="flow-query-tab-guided" aria-controls="flow-query-panel-guided" aria-selected="true" tabindex="0")

    assert guided_html =~
             ~s(id="flow-query-tab-advanced" aria-controls="flow-query-panel-advanced" aria-selected="false" tabindex="-1")

    assert guided_html =~
             ~s(id="flow-query-panel-guided" role="tabpanel" aria-labelledby="flow-query-tab-guided")

    assert guided_html =~
             ~s(id="flow-query-panel-advanced" role="tabpanel" aria-labelledby="flow-query-tab-advanced" hidden)

    assert guided_html =~ "<legend>Indexed attribute predicate</legend>"
    assert guided_html =~ "<legend>State metadata predicate</legend>"
    assert guided_html =~ "Workflow type filters records; Partition is the data ACL scope."
    assert guided_html =~ "Required query, routing, and data ACL scope."

    run_position = :binary.match(guided_html, ~s(data-flow-query-run-action)) |> elem(0)
    options_position = :binary.match(guided_html, ~s(data-flow-query-options-action)) |> elem(0)
    assert run_position < options_position

    history_html = render_query_page("history", :guided)
    assert history_html =~ ~r/data-flow-query-options-action[^>]*hidden/

    advanced_html = render_query_page("search", :advanced)

    assert advanced_html =~
             ~s(id="flow-query-panel-guided" role="tabpanel" aria-labelledby="flow-query-tab-guided" hidden)

    assert advanced_html =~
             ~s(id="flow-query-panel-advanced" role="tabpanel" aria-labelledby="flow-query-tab-advanced")
  end

  test "guided stats requires the state predicate it actually counts" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_stats_fun, fn type, opts ->
      send(parent, {:unexpected_stats, type, opts})
      {:ok, %{count: 0}}
    end)

    data = Dashboard.collect_flow_query_page(kind: "stats", type: "email")

    assert data.result.status == :idle
    assert data.result.message == "Enter a workflow state"
    refute_receive {:unexpected_stats, _type, _opts}
  end

  test "guided stats forwards only predicates supported by FLOW.STATS" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_stats_fun, fn type, opts ->
      send(parent, {:stats, type, opts})
      {:ok, %{count: 1}}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "stats",
        type: "email",
        state: "failed",
        partition_key: "tenant-a",
        attribute_key: "customer",
        attribute_value: "acme"
      )

    assert data.result.status == :ok
    assert_receive {:stats, "email", opts}
    assert opts[:state] == "failed"
    assert opts[:partition_key] == "tenant-a"
    assert opts[:attributes] == %{"customer" => "acme"}
    assert opts[:consistent_projection] == true

    assert Keyword.keys(opts) |> Enum.sort() ==
             [:attributes, :consistent_projection, :partition_key, :state]
  end

  test "guided queries reject predicates hidden for the selected kind" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:unexpected_query, query, params})
      {:ok, %{records: []}}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_stats_fun, fn type, opts ->
      send(parent, {:unexpected_stats, type, opts})
      {:ok, %{count: 0}}
    end)

    cases = [
      {[kind: "failures", type: "email", state: "queued", partition_key: "tenant-a"],
       "Lifecycle state is not supported by failures"},
      {[
         kind: "list",
         type: "email",
         partition_key: "tenant-a",
         state_meta_state: "review",
         state_meta_key: "risk_tier",
         state_meta_value: "high"
       ], "State metadata is not supported by list"},
      {[kind: "stats", type: "email", state: "failed", from_ms: 1_000],
       "Time range is not supported by stats"},
      {[kind: "history", id: "flow-1", rev: true],
       "Newest-first ordering is not supported by history"}
    ]

    for {opts, message} <- cases do
      data = Dashboard.collect_flow_query_page(opts)
      assert data.result.status == :idle
      assert data.result.message == message
    end

    refute_receive {:unexpected_query, _query, _params}
    refute_receive {:unexpected_stats, _type, _opts}
  end

  test "guided query rejects incomplete optional predicates instead of silently dropping them" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:unexpected_query, query, params})
      {:ok, %{records: []}}
    end)

    attribute =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "customer"
      )

    assert attribute.result.status == :idle
    assert attribute.result.message =~ "attribute key and value"

    state_meta =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "customer",
        attribute_value: "acme",
        state_meta_state: "review",
        state_meta_key: "",
        state_meta_value: "high"
      )

    assert state_meta.result.status == :idle
    assert state_meta.result.message =~ "state metadata state, key, and value"
    refute_receive {:unexpected_query, _query, _params}
  end

  test "guided query ignores empty optional controls submitted by the browser" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:blank_controls_query, query, params})
      {:ok, %{records: []}}
    end)

    list =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "",
        attribute_value: ""
      )

    assert list.result.status == :ok
    assert_receive {:blank_controls_query, list_query, list_params}
    refute list_query =~ "attribute."
    refute Map.has_key?(list_params, "attribute_value")

    search =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "customer",
        attribute_value: "acme",
        state_meta_state: "",
        state_meta_key: "",
        state_meta_value: ""
      )

    assert search.result.status == :ok
    assert_receive {:blank_controls_query, search_query, search_params}
    assert search_query =~ "attribute.customer = @attribute_value"
    refute search_query =~ "state_meta."
    refute Map.has_key?(search_params, "state_meta_value")
  end

  test "guided list applies its displayed attribute predicate" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:list_query, query, params})
      {:ok, %{records: []}}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "customer",
        attribute_value: "acme"
      )

    assert data.result.status == :ok
    assert_receive {:list_query, query, params}
    assert query =~ "attribute.customer = @attribute_value"
    assert params["attribute_value"] == "acme"
  end

  test "guided queries keep lifecycle state and workflow step independent" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:state_query, query, params})
      {:ok, %{records: []}}
    end)

    query =
      URI.encode_query(%{
        "kind" => "list",
        "type" => "email",
        "state" => "running",
        "run_state" => "review",
        "partition_key" => "tenant-a"
      })

    opts = Dashboard.flow_query_opts_from_query(query)
    assert opts[:state] == "running"
    assert opts[:run_state] == "review"

    data = Dashboard.collect_flow_query_page(opts)

    assert data.result.status == :ok
    assert_receive {:state_query, fql, params}
    assert fql =~ "state = @state"
    assert fql =~ "run_state = @run_state"
    assert params["state"] == "running"
    assert params["run_state"] == "review"
  end

  test "guided metadata values preserve scalar types and empty strings" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:typed_query, query, params})
      {:ok, %{records: []}}
    end)

    cases = [
      {[attribute_key: "priority", attribute_value_type: "integer", attribute_value: "42"],
       "attribute.priority = @attribute_value", "attribute_value", 42},
      {[attribute_key: "enabled", attribute_value_type: "boolean", attribute_value: "true"],
       "attribute.enabled = @attribute_value", "attribute_value", true},
      {[attribute_key: "empty", attribute_value_type: "string", attribute_value: ""],
       "attribute.empty = @attribute_value", "attribute_value", ""},
      {[
         state_meta_state: "review",
         state_meta_key: "score",
         state_meta_value_type: "float",
         state_meta_value: "1.25"
       ], "state_meta.review.score = @state_meta_value", "state_meta_value", 1.25}
    ]

    for {metadata_opts, expected_query, parameter, expected_value} <- cases do
      data =
        Dashboard.collect_flow_query_page(
          [kind: "search", type: "email", partition_key: "tenant-a"] ++ metadata_opts
        )

      assert data.result.status == :ok
      assert_receive {:typed_query, query, params}
      assert query =~ expected_query
      assert params[parameter] == expected_value
    end
  end

  test "guided metadata supports null and rejects malformed typed values before execution" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:typed_query, query, params})
      {:ok, %{records: []}}
    end)

    null =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "reviewed_at",
        attribute_value_type: "null"
      )

    assert null.result.status == :ok
    assert_receive {:typed_query, query, params}
    assert query =~ "attribute.reviewed_at IS NULL"
    refute Map.has_key?(params, "attribute_value")

    invalid =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "priority",
        attribute_value_type: "integer",
        attribute_value: "forty-two"
      )

    assert invalid.result.status == :idle
    assert invalid.result.message == "Attribute value must be an integer"
    refute_receive {:typed_query, _query, _params}
  end

  test "guided validation explains unsupported metadata names and reversed time ranges" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:unexpected_query, query, params})
      {:ok, %{records: []}}
    end)

    reserved_attribute =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        attribute_key: "__secret",
        attribute_value: "hidden"
      )

    assert reserved_attribute.result.status == :idle

    assert reserved_attribute.result.message ==
             "Attribute key is not queryable. Choose an indexed attribute from Show options"

    reserved_state_meta =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        state_meta_state: "review",
        state_meta_key: "__secret",
        state_meta_value: "hidden"
      )

    assert reserved_state_meta.result.status == :idle

    assert reserved_state_meta.result.message ==
             "State metadata state or key is not queryable. Choose the policy-indexed field from Show options"

    reversed_time =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        partition_key: "tenant-a",
        from_ms: 2_000,
        to_ms: 1_000
      )

    assert reversed_time.result.status == :idle
    assert reversed_time.result.message == "From UTC must not be later than To UTC"
    refute_receive {:unexpected_query, _query, _params}
  end

  test "type inspection loads queryable policy options without executing a query" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:unexpected_query, query, params})
      {:ok, %{records: []}}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, opts ->
      send(parent, {:policy_get, type, opts})

      {:ok,
       %{
         type: type,
         generation: 7,
         states: %{"review" => %{}, "retrying" => %{}},
         indexed_attributes: ["customer", "region"],
         indexed_state_meta: "risk_tier"
       }}
    end)

    opts =
      Dashboard.flow_query_opts_from_query(
        URI.encode_query(%{
          "kind" => "search",
          "type" => "email",
          "partition_key" => "tenant-a",
          "inspect" => "true"
        })
      )

    assert opts[:inspect] == true

    data = Dashboard.collect_flow_query_page(opts)

    assert_receive {:policy_get, "email", []}
    refute_receive {:unexpected_query, _query, _params}

    assert data.result.status == :idle
    assert data.result.message == "Options loaded for email"
    assert data.discovery.status == :ready
    assert data.discovery.type == "email"
    assert data.discovery.generation == 7
    assert "running" in data.discovery.lifecycle_states
    refute "review" in data.discovery.lifecycle_states
    assert "review" in data.discovery.workflow_steps
    assert "retrying" in data.discovery.workflow_steps
    assert data.discovery.indexed_attributes == ["customer", "region"]
    assert data.discovery.indexed_state_meta == "risk_tier"

    assert Jason.decode!(data.workbench.params_json) == %{
             "partition" => "tenant-a",
             "type" => "email"
           }

    html = Dashboard.render_flow_query_page(data)

    assert html =~ ~s(data-flow-query-discovery)
    assert html =~ "Query fields for <code>email</code>"
    assert html =~ ~s(<option value="review"></option>)
    assert html =~ ~s(<option value="customer"></option>)
    assert html =~ ~s(<option value="risk_tier"></option>)
    assert html =~ ~s(list="flow-query-lifecycle-state-options")
    assert html =~ ~s(list="flow-query-workflow-step-options")
    assert html =~ ~s(list="flow-query-attribute-options")
    assert html =~ ~s(list="flow-query-state-meta-key-options")
    assert html =~ ~s(name="inspect" value="true")
  end

  test "type inspection without a type samples only projected types in the authorized partition" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:type_discovery_query, query, params})

      {:ok,
       %{
         records: [
           %{"type" => "email"},
           %{"type" => "invoice"},
           %{"type" => "email"}
         ]
       }}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        partition_key: "tenant-a",
        inspect: true
      )

    assert_receive {:type_discovery_query, query, %{"partition" => "tenant-a"}}
    assert query =~ "partition_key = @partition"
    assert query =~ "LIMIT 65"
    assert query =~ "RETURN RECORDS (type)"
    refute query =~ "payload"
    assert data.discovery.available_types == ["email", "invoice"]
    assert data.discovery.types_truncated? == false
    assert data.result.message == "Type options loaded for tenant-a"

    html = Dashboard.render_flow_query_page(data)
    assert html =~ ~s(list="flow-query-type-options")
    assert html =~ ~s(<option value="email"></option>)
    assert html =~ "Observed workflow types"
  end

  test "type inspection enforces FLOW.QUERY without relying on the HTTP route" do
    username = "query-type-discovery-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "-@all"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:unexpected_type_discovery, query, params})
      {:ok, %{records: [%{"type" => "email"}]}}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        partition_key: "tenant-a",
        inspect: true,
        acl_username: username
      )

    assert data.discovery.status == :forbidden
    assert data.discovery.required_command == "FLOW.QUERY"
    assert data.result.message == "Type options require +FLOW.QUERY"
    refute_receive {:unexpected_type_discovery, _query, _params}
  end

  test "show options loads bounded typed attribute and state metadata values" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, _opts ->
      {:ok,
       %{
         type: type,
         generation: 2,
         states: %{"review" => %{}},
         indexed_attributes: ["priority"],
         indexed_state_meta: "risk_tier"
       }}
    end)

    Application.put_env(
      :ferricstore,
      :flow_dashboard_flow_attribute_values_fun,
      fn type, key, opts ->
        send(parent, {:attribute_values, type, key, opts})
        {:ok, [%{value: 42, count: 7, approximate: true}, %{value: "42", count: 3}]}
      end
    )

    Application.put_env(
      :ferricstore,
      :flow_dashboard_flow_state_meta_values_fun,
      fn type, state, key, opts ->
        send(parent, {:state_meta_values, type, state, key, opts})
        {:ok, [%{value: "high", count: 5}, %{value: nil, count: 1}]}
      end
    )

    data =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        state: "running",
        attribute_key: "priority",
        attribute_value_type: "integer",
        state_meta_state: "review",
        state_meta_key: "risk_tier",
        state_meta_value_type: "string",
        partition_key: "tenant-a",
        inspect: true
      )

    assert_receive {:attribute_values, "email", "priority", attribute_opts}
    assert attribute_opts[:count] == 20
    assert attribute_opts[:state] == "running"
    assert attribute_opts[:partition_key] == "tenant-a"
    assert attribute_opts[:consistent_projection] == true

    assert_receive {:state_meta_values, "email", "review", "risk_tier", state_meta_opts}
    assert state_meta_opts[:count] == 20
    assert state_meta_opts[:partition_key] == "tenant-a"
    assert state_meta_opts[:consistent_projection] == true

    assert data.discovery.attribute_values == [%{value: 42, count: 7, approximate: true}]
    assert data.discovery.state_meta_values == [%{value: "high", count: 5}]

    html = Dashboard.render_flow_query_page(data)
    assert html =~ ~s(list="flow-query-attribute-value-options")
    assert html =~ ~s(list="flow-query-state-meta-value-options")
    assert html =~ ~s(<option value="42"></option>)
    assert html =~ ~s(<option value="high"></option>)
    assert html =~ "Top attribute values"
    assert html =~ "Top state metadata values"
    assert html =~ ~s(title="Approximate count" aria-label="approximately 7">~7</small>)
  end

  test "query discovery performs no policy lookup until a workflow type is supplied" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, opts ->
      send(parent, {:unexpected_policy_get, type, opts})
      {:ok, %{}}
    end)

    data = Dashboard.collect_flow_query_page()

    assert data.discovery.status == :idle
    assert data.discovery.type == nil
    assert data.discovery.indexed_attributes == []
    refute_receive {:unexpected_policy_get, _type, _opts}
  end

  test "policy discovery overlaps normal query execution" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn _query, _params ->
      send(parent, {:query_started, self()})
      receive do: (:finish_query -> {:ok, %{records: []}})
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, _opts ->
      send(parent, {:policy_started, self()})

      receive do
        :finish_policy ->
          {:ok,
           %{
             type: type,
             generation: 1,
             states: %{},
             indexed_attributes: [],
             indexed_state_meta: nil
           }}
      end
    end)

    collector =
      Task.async(fn ->
        Dashboard.collect_flow_query_page(
          kind: "list",
          type: "email",
          partition_key: "tenant-a"
        )
      end)

    assert_receive {:query_started, query_pid}, 500

    policy_pid =
      receive do
        {:policy_started, pid} -> pid
      after
        100 -> nil
      end

    overlapped? = is_pid(policy_pid)

    send(query_pid, :finish_query)

    policy_pid =
      case policy_pid do
        nil ->
          assert_receive {:policy_started, pid}, 500
          pid

        pid ->
          pid
      end

    send(policy_pid, :finish_policy)
    data = Task.await(collector, 1_000)

    assert overlapped?
    assert data.result.status == :ok
    assert data.discovery.status == :ready
  end

  test "policy discovery failure does not replace a successful query result" do
    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn _query, _params ->
      {:ok, %{records: [%{id: "flow-1", type: "email", state: "manual_review"}]}}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn _type, _opts ->
      {:error, "ERR flow policy shard not available"}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "list",
        type: "email",
        partition_key: "tenant-a"
      )

    assert data.result.status == :ok
    assert [%{id: "flow-1"}] = data.result.rows
    assert data.discovery.status == :unavailable
    assert "manual_review" in data.discovery.lifecycle_states
    assert data.discovery.indexed_attributes == []
  end

  test "query discovery does not read policy metadata for a denied partition" do
    username = "query-discovery-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "-@all",
               "+FLOW.QUERY"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, opts ->
      send(parent, {:unexpected_policy_get, type, opts})
      {:ok, %{type: type, states: %{"review" => %{}}}}
    end)

    idle = Dashboard.collect_flow_query_page(acl_username: username)
    assert idle.discovery.status == :idle

    command_denied =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        inspect: true,
        acl_username: username
      )

    assert command_denied.discovery.status == :forbidden
    assert command_denied.discovery.required_command == "FLOW.POLICY.GET"
    assert command_denied.result.message == "Type options require +FLOW.POLICY.GET"
    refute_receive {:unexpected_policy_get, _type, _opts}

    data =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-b",
        inspect: true,
        acl_username: username
      )

    assert data.discovery.status == :forbidden
    assert data.result.message == "Type options are not available for this partition"
    refute_receive {:unexpected_policy_get, _type, _opts}
  end

  test "query discovery does not read policy metadata for a denied workflow type" do
    username = "query-discovery-type-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "-@all",
               "+FLOW.QUERY",
               "+FLOW.POLICY.GET"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, opts ->
      send(parent, {:unexpected_policy_get, type, opts})
      {:ok, %{type: type, states: %{"review" => %{}}}}
    end)

    data =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        partition_key: "tenant-a",
        inspect: true,
        acl_username: username
      )

    assert data.discovery.status == :forbidden
    assert data.discovery.denied_scope == :type
    assert data.result.message == "Type options are not available for this workflow type"
    refute_receive {:unexpected_policy_get, _type, _opts}

    html = Dashboard.render_flow_query_page(data)
    assert html =~ "Type options are not available for this workflow type."
  end

  test "value discovery does not bypass the FLOW.ATTRIBUTE_VALUES command ACL" do
    username = "query-value-discovery-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "%R~tenant-a",
               "%R~email",
               "-@all",
               "+FLOW.QUERY",
               "+FLOW.POLICY.GET"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, _opts ->
      {:ok,
       %{
         type: type,
         states: %{},
         indexed_attributes: ["customer"],
         indexed_state_meta: nil
       }}
    end)

    Application.put_env(
      :ferricstore,
      :flow_dashboard_flow_attribute_values_fun,
      fn type, key, opts ->
        send(parent, {:unexpected_attribute_values, type, key, opts})
        {:ok, [%{value: "acme", count: 1}]}
      end
    )

    data =
      Dashboard.collect_flow_query_page(
        kind: "search",
        type: "email",
        attribute_key: "customer",
        partition_key: "tenant-a",
        inspect: true,
        acl_username: username
      )

    assert data.discovery.status == :ready
    assert data.discovery.attribute_values == []
    assert data.discovery.restricted_commands == ["FLOW.ATTRIBUTE_VALUES"]
    refute_receive {:unexpected_attribute_values, _type, _key, _opts}

    html = Dashboard.render_flow_query_page(data)
    assert html =~ "Top values require <code>+FLOW.ATTRIBUTE_VALUES</code>"
  end

  test "query option identifiers are escaped before rendering" do
    Application.put_env(:ferricstore, :flow_dashboard_flow_policy_get_fun, fn type, _opts ->
      {:ok,
       %{
         type: type,
         generation: 1,
         states: %{"review\"><script>alert(1)</script>" => %{}},
         indexed_attributes: ["customer\"><script>alert(2)</script>"],
         indexed_state_meta: "risk\"><script>alert(3)</script>"
       }}
    end)

    html =
      Dashboard.collect_flow_query_page(type: "email", inspect: true)
      |> Dashboard.render_flow_query_page()

    refute html =~ "<script>alert(1)</script>"
    refute html =~ "<script>alert(2)</script>"
    refute html =~ "<script>alert(3)</script>"
    assert html =~ "review&quot;&gt;&lt;script&gt;alert(1)&lt;/script&gt;"
  end

  test "FQL-backed dashboard limits never exceed the engine result bound" do
    query_opts =
      Dashboard.flow_query_opts_from_query(
        URI.encode_query(%{"kind" => "list", "limit" => "200"})
      )

    lineage_opts =
      Dashboard.flow_lineage_opts_from_query(
        URI.encode_query(%{"id" => "root-1", "limit" => "200"})
      )

    assert query_opts[:limit] == Limits.max_results()
    assert lineage_opts[:limit] == Limits.max_results()
  end

  test "standalone lineage form marks its partition predicate as required" do
    html =
      Dashboard.collect_flow_lineage_page(mode: "root", target: "root-1", limit: 10)
      |> Dashboard.render_flow_lineage_page()

    form = form_html(html, "/dashboard/flow/lineage")
    partition = input_tag(form, "partition_key")
    assert required_input?(partition)
    assert partition =~ ~s(placeholder="required")
    refute partition =~ "optional"
  end

  test "cold terminal query preserves the state page time predicates and newest-first order" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:terminal_query, query, params})
      {:ok, %{records: []}}
    end)

    _data =
      Dashboard.collect_flow_states_page(
        type: "email",
        state: "completed",
        partition_key: "tenant-a",
        from_ms: 1_000,
        to_ms: 2_000,
        limit: 25
      )

    assert_receive {:terminal_query, query, params}
    assert query =~ "updated_at_ms BETWEEN @from_ms AND @to_ms"
    assert query =~ "ORDER BY updated_at_ms DESC"
    assert params["from_ms"] == 1_000
    assert params["to_ms"] == 2_000
  end

  test "empty state uses one merged cold query and keeps newer terminal records" do
    parent = self()
    type = "terminal-fairness-#{System.unique_integer([:positive])}"

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:terminal_query, query, params})

      records =
        for index <- 1..24 do
          terminal_record("completed-#{index}", type, "completed", index)
        end ++ [terminal_record("newest-failed", type, "failed", 10_000)]

      {:ok, %{records: records}}
    end)

    data =
      Dashboard.collect_flow_states_page(
        type: type,
        partition_key: "tenant-a",
        limit: 25
      )

    assert_received {:terminal_query, query, params}
    assert query =~ "state IN (@terminal_0, @terminal_1, @terminal_2)"

    assert Map.take(params, ~w(terminal_0 terminal_1 terminal_2)) == %{
             "terminal_0" => "completed",
             "terminal_1" => "failed",
             "terminal_2" => "cancelled"
           }

    refute_receive {:terminal_query, _query, _params}
    assert Enum.any?(data.records, &(Map.get(&1, :id) == "newest-failed"))
  end

  test "all-type state views do not run a partial cold-query fanout" do
    parent = self()
    suffix = System.unique_integer([:positive])
    type = "terminal-all-types-#{suffix}"
    partition_key = "terminal-all-types-partition-#{suffix}"

    assert :ok =
             FerricStore.flow_create("terminal-all-types-hot-#{suffix}",
               type: type,
               state: "queued",
               partition_key: partition_key,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:unexpected_terminal_fanout, query, params})
      {:ok, %{records: []}}
    end)

    data =
      Dashboard.collect_flow_states_page(
        state: "failed",
        partition_key: partition_key,
        limit: 25
      )

    refute_receive {:unexpected_terminal_fanout, _query, _params}
    assert data.filters.type == nil
    assert data.filters.state == "failed"
  end

  test "schedule query filters trim whitespace before deciding whether predicates are empty" do
    opts =
      Dashboard.flow_schedules_opts_from_query(
        URI.encode_query(%{
          "state" => " all ",
          "kind" => " cron ",
          "q" => "   ",
          "limit" => " 20 "
        })
      )

    assert opts[:state] == :all
    assert opts[:kind] == :cron
    assert opts[:limit] == 20
    refute Keyword.has_key?(opts, :q)
  end

  test "schedule filter controls have concise accessible names" do
    html =
      FerricstoreServer.Health.Dashboard.Render.FlowSchedules.render_flow_schedules_filters(%{
        filters: %{q: nil, state: :all, kind: nil, limit: 100}
      })

    assert html =~ ~s(name="q" aria-label="Schedule ID contains")
    assert html =~ ~s(name="state" aria-label="Schedule state")
    assert html =~ ~s(name="kind" aria-label="Schedule kind")
    assert html =~ ~s(name="limit" aria-label="Schedule limit")
  end

  test "governance filter controls match their bounded and required predicate contracts" do
    opts =
      Dashboard.flow_governance_opts_from_query(URI.encode_query(%{"status" => "expired"}))

    html =
      Dashboard.render_flow_governance_page(%{
        counts: %{},
        filters: %{limit: 100, status: "expired"},
        state_meta_result: %{
          status: :idle,
          command: "FLOW.QUERY",
          rows: [],
          message: "Enter query predicates"
        },
        circuits: [],
        approvals: [],
        budgets: [],
        limits: []
      })

    refute html =~ ~s(max="500")
    refute html =~ "scope / tenant"
    assert opts[:status] == "expired"
    assert html =~ ~s(<option value="expired" selected>expired</option>)

    for name <- ~w(meta_type meta_state meta_key meta_value meta_partition_key) do
      assert required_input?(input_tag(html, name))
    end
  end

  test "empty optional filters normalize to unfiltered semantics on sampled screens" do
    state_opts =
      Dashboard.flow_states_opts_from_query(
        URI.encode_query(%{"type" => "all", "state" => "", "partition_key" => " "})
      )

    failure_opts =
      Dashboard.flow_failures_opts_from_query(
        URI.encode_query(%{"type" => "all", "partition_key" => "", "q" => " "})
      )

    signal_opts =
      Dashboard.flow_signals_opts_from_query(
        URI.encode_query(%{"type" => "all", "signal" => "", "q" => " "})
      )

    refute Keyword.has_key?(state_opts, :type)
    refute Keyword.has_key?(state_opts, :state)
    refute Keyword.has_key?(state_opts, :partition_key)
    refute Keyword.has_key?(failure_opts, :type)
    refute Keyword.has_key?(failure_opts, :partition_key)
    refute Keyword.has_key?(failure_opts, :q)
    refute Keyword.has_key?(signal_opts, :type)
    refute Keyword.has_key?(signal_opts, :signal)
    refute Keyword.has_key?(signal_opts, :q)
  end

  test "sampled type selects preserve an explicit predicate missing from the sample" do
    state_html =
      FerricstoreServer.Health.Dashboard.Render.FlowFilters.render_flow_type_filter(%{
        available_types: [],
        available_states: [],
        filters: %{
          type: "email",
          state: nil,
          partition_key: nil,
          q: nil,
          range: nil,
          from_ms: nil,
          to_ms: nil,
          limit: 40
        }
      })

    signal_html =
      FerricstoreServer.Health.Dashboard.Render.FlowFilters.render_flow_signals_filter(%{
        available_types: [],
        filters: %{type: "email", signal: nil, q: nil, limit: 40, scan_history: false}
      })

    assert state_html =~
             ~s(<input id="flow-state-type-filter" class="flow-search-input mono" type="search" name="type" value="email" list="flow-state-type-options")

    assert state_html =~
             ~s(<datalist id="flow-state-type-options"><option value="email"></option></datalist>)

    refute state_html =~ ~s(<select id="flow-state-type-filter")
    assert signal_html =~ ~s(<option value="email" selected>email</option>)
  end

  test "state filters always expose exact terminal states for bounded cold lookup" do
    html =
      FerricstoreServer.Health.Dashboard.Render.FlowFilters.render_flow_type_filter(%{
        available_types: [],
        available_states: [],
        filters: %{
          type: nil,
          state: nil,
          partition_key: nil,
          q: nil,
          range: nil,
          from_ms: nil,
          to_ms: nil,
          limit: 40
        }
      })

    assert html =~ ~s(<option value="cancelled">cancelled</option>)
    assert html =~ ~s(<option value="completed">completed</option>)
    assert html =~ ~s(<option value="failed">failed</option>)
  end

  test "failure exact scan requires a type instead of silently scanning a type subset" do
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fn query, params ->
      send(parent, {:unexpected_exact_query, query, params})
      {:ok, %{records: []}}
    end)

    data =
      Dashboard.collect_flow_failures_page(
        partition_key: "tenant-a",
        scan_exact: true,
        limit: 20
      )

    assert data.exact_scan_status.failures == {:error, :query_type_required}
    assert data.exact_scan_status.stuck == {:error, :query_type_required}
    refute_receive {:unexpected_exact_query, _query, _params}

    html = Dashboard.render_flow_failures_page(data)
    assert html =~ "Exact scan requires a workflow type and partition"
    assert html =~ "FLOW.QUERY requires one workflow type"

    assert length(String.split(html, "Exact scan issue:", trim: true)) == 2

    assert html =~ ~r/<input[^>]+name="type"[^>]+list="flow-failure-type-options"/
    assert html =~ ~s(<datalist id="flow-failure-type-options">)
    refute html =~ ~s(<select class="flow-search-input mono" name="type")
  end

  test "failure exact scan reports both required fields when both are missing" do
    html =
      Dashboard.collect_flow_failures_page(scan_exact: true, limit: 20)
      |> Dashboard.render_flow_failures_page()

    assert html =~ "FLOW.QUERY requires a workflow type and partition key"
    assert length(String.split(html, "Exact scan issue:", trim: true)) == 2
  end

  defp render_query_controls(kind) do
    render_query_page(kind, :guided)
    |> form_html("/dashboard/flow/query")
  end

  defp render_query_page(kind, mode) do
    Dashboard.render_flow_query_page(%{
      filters: %{
        kind: kind,
        type: "email",
        state: nil,
        run_state: nil,
        attribute_key: nil,
        attribute_value_type: "string",
        attribute_value: nil,
        state_meta_state: nil,
        state_meta_key: nil,
        state_meta_value_type: "string",
        state_meta_value: nil,
        id: nil,
        partition_key: "tenant-a",
        limit: 40,
        from_ms: nil,
        to_ms: nil,
        rev: false
      },
      workbench: %{
        FerricstoreServer.Health.Dashboard.Flow.QueryWorkbench.default_form()
        | mode: mode
      },
      result: %{status: :idle, command: "FLOW.QUERY", rows: [], message: "idle"}
    })
  end

  defp field_tag(html, field) do
    [tag] = Regex.run(~r/<label[^>]*data-flow-query-field="#{field}"[^>]*>/, html)
    tag
  end

  defp input_tag(html, name) do
    [tag] = Regex.run(~r/<input[^>]*name="#{name}"[^>]*>/, html)
    tag
  end

  defp required_input?(tag), do: Regex.match?(~r/\srequired(?:\s|>)/, tag)

  defp form_html(html, action) do
    [_, form] = Regex.run(~r/<form[^>]*action="#{action}"[^>]*>(.*?)<\/form>/s, html)
    form
  end

  defp terminal_record(id, type, state, updated_at_ms) do
    %{
      id: id,
      type: type,
      state: state,
      partition_key: "tenant-a",
      updated_at_ms: updated_at_ms,
      run_at_ms: updated_at_ms,
      attempts: 0
    }
  end
end
