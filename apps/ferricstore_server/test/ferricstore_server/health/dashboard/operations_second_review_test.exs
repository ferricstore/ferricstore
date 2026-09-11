defmodule FerricstoreServer.Health.Dashboard.OperationsSecondReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Data.{KV, Security}

  alias FerricstoreServer.Health.Dashboard.Render.{
    Admin,
    DoctorPages,
    FlowGovernance,
    FlowIndexCatalog,
    FlowRetention,
    FlowSchedules,
    Prefixes
  }

  alias FerricstoreServer.Health.Dashboard.Render.Security, as: SecurityRender
  alias FerricstoreServer.Health.Dashboard.Flow.{Schedules, PolicyRetention, Governance}
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  test "cleanup and circuit mutation forms use the existing single-submit contract" do
    reviewed = %{
      circuit_review: %{
        status: :ok,
        scope: "effect:review",
        circuit: %{scope: "effect:review", status: :open},
        fingerprint: String.duplicate("x", 43)
      }
    }

    for html <- [
          FlowRetention.render_flow_retention_controls(%{}),
          FlowGovernance.render_flow_governance_circuit_actions(reviewed)
        ] do
      forms = Regex.scan(~r/<form[^>]*method="post"[^>]*>/, html) |> List.flatten()
      assert forms != []
      assert Enum.all?(forms, &String.contains?(&1, "data-dashboard-single-submit"))
    end
  end

  test "ACL tester retains and names the HTTP method and rejects unknown routes" do
    page =
      Security.collect_page(%{
        "route_path" => "/dashboard/security/users",
        "route_method" => "POST"
      })

    html = SecurityRender.render_acl_tester(page)
    assert html =~ ~s(name="route_method")
    assert html =~ ~s(value="POST" selected)
    assert html =~ "POST /dashboard/security/users"
    unknown = Security.collect_page(%{"route_path" => "/dashboard/not-a-real-page"})
    assert unknown.tester.route.status == :unsupported
  end

  test "sensitive config values are explicitly redacted rather than shown as unset" do
    html =
      Admin.render_config_parameters([
        %{
          parameter: "requirepass",
          value: "",
          source: "CONFIG GET",
          scope: "runtime",
          mutability: "read-write",
          notes: "Password"
        }
      ])

    assert html =~ "Redacted"
  end

  test "open mode explains authenticated setup without suggesting an ACL grant" do
    html =
      SecurityRender.render_account_management(%{
        protected_mode: false,
        current_user: nil,
        can_manage_users: false
      })

    assert html =~ "protected mode"
    assert html =~ "authenticated"
    assert html =~ "href="
    refute html =~ "Account mutations require"
  end

  test "Doctor checks use a named focusable bounded scroller" do
    html = DoctorPages.render_doctor_checks(%{"checks" => []})
    assert html =~ ~s(class="table-scroll" role="region" aria-label="Doctor checks" tabindex="0")
    assert html =~ ~s(<h2 class="section-title">Checks</h2>)
    assert DoctorPages.render_doctor_jobs([]) =~ ~s(aria-label="Doctor jobs" tabindex="0")

    assert DoctorPages.render_doctor_command_reference([]) =~
             ~s(aria-label="Doctor command reference" tabindex="0")
  end

  test "schedule metadata is inspectable without payload values and end time is formatted" do
    schedule = %{
      id: "schedule<&",
      state: "cancelled",
      kind: :interval,
      every_ms: 60_000,
      timezone: "Etc/UTC",
      start_at_ms: 1_788_948_000_001,
      end_at_ms: 1_789_207_761_000,
      target: %{type: "review", partition_key: " scoped ", payload: "secret-payload"},
      last_target_id: "flow<&"
    }

    html = FlowSchedules.render_flow_schedules_table([schedule])
    assert html =~ "Schedule definition"
    assert html =~ "60000"
    assert html =~ "Etc/UTC"
    assert html =~ " scoped "
    assert html =~ "partition_key=+scoped+"
    assert html =~ "2026-09-12"
    refute html =~ "until 1789207761000"
    refute html =~ "secret-payload"
  end

  test "prefix measurements are looked up for displayed prefixes outside the cold top 20" do
    prefixes = for i <- 1..25, do: "second-review-#{System.unique_integer([:positive])}-#{i}"

    for {prefix, i} <- Enum.with_index(prefixes, 1) do
      :ets.insert(:keydir_0, {prefix <> ":key", "v", 0, 0, 0, 0, 1})
      :ets.insert(:ferricstore_hotness, {prefix, i * 10, i})
    end

    on_exit(fn ->
      for prefix <- prefixes do
        :ets.delete(:keydir_0, prefix <> ":key")
        :ets.delete(:ferricstore_hotness, prefix)
      end
    end)

    [first | _] = prefixes
    row = KV.collect_prefixes_page().prefixes |> Enum.find(&(&1.prefix == first))
    assert row.hot_reads == 10
    assert row.cold_reads == 1
  end

  test "prefix summaries distinguish displayed and sampled user-KV keys" do
    data = %{prefixes: [], total_sampled: 0, scan_limited?: true}
    html = Prefixes.render_prefixes_summary(data) <> Prefixes.render_prefixes_table(data)
    assert html =~ "User KV"
    assert html =~ "Displayed keys"
    assert html =~ "Scan budget reached"
    refute html =~ "Indexed Keys"
    refute html =~ "No keys found"
  end

  test "index diagnosis uses the existing service and freshness snapshot" do
    html =
      FlowIndexCatalog.render(%{
        status: :ok,
        snapshot: %{
          "services" => %{"statistics_worker" => "unavailable", "statistics_store" => "ready"},
          "statistics_max_age_ms" => 300_000,
          "indexes" => [
            %{
              "id" => "test_index",
              "fields" => [],
              "statistics" => %{"status" => "stale", "oldest_age_ms" => 400_000},
              "validation" => %{"failure_reason" => "bad<&"}
            }
          ]
        }
      })

    assert html =~ "statistics_worker"
    assert html =~ "Freshness budget"
    assert html =~ "FLOW.QUERY.INDEXES"
    assert html =~ "bad&lt;&amp;"
    refute html =~ "Oldest sample: 400000 ms"
  end

  test "server advertises its installed ACL management without enabling enterprise flags" do
    assert FerricStore.Management.ACL.implementation() == FerricstoreServer.Management.ACL
    capabilities = FerricStore.ManagementCapabilities.capabilities()
    assert capabilities.acl_management
    refute capabilities.namespace_management
    refute capabilities.quota_management
    refute FerricStore.ManagementCapabilities.default().acl_management
  end

  test "schedule searches apply result limits after matching and provide exact lookup" do
    prefix = "ops-schedule-#{System.unique_integer([:positive])}"
    ids = Enum.map(1..3, &(prefix <> "-#{&1}"))

    for id <- ids do
      assert {:ok, _} =
               FerricStore.flow_schedule_create(id,
                 every_ms: 3_600_000,
                 target: [type: "ops-review"]
               )
    end

    on_exit(fn -> for id <- ids, do: FerricStore.flow_schedule_delete(id) end)
    wanted = List.last(ids)
    assert [row] = Schedules.collect_page(q: wanted, limit: 1).schedules
    assert row.id == wanted
    opts = Schedules.opts_from_query(URI.encode_query(%{"id" => wanted, "limit" => "1"}))
    assert opts[:id] == wanted
    assert [%{id: ^wanted}] = Schedules.collect_page(opts).schedules
  end

  test "original schedule timing survives fire and cancellation" do
    now = System.system_time(:millisecond)
    id = "ops-original-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             FerricStore.flow_schedule_create(id,
               now_ms: now,
               every_ms: 3_600_000,
               start_at_ms: now + 5000,
               target: [type: "ops-review"]
             )

    on_exit(fn -> FerricStore.flow_schedule_delete(id) end)
    assert {:ok, before} = FerricStore.flow_schedule_get(id)
    assert before.initial_run_at_ms == now + 5000
    assert before.start_at_ms == now + 5000
    flush_projection()
    assert {:ok, _} = FerricStore.flow_schedule_fire(id, now_ms: now + 5000)
    assert {:ok, fired} = FerricStore.flow_schedule_get(id)
    assert fired.initial_run_at_ms == before.initial_run_at_ms
    flush_projection()
    assert :ok = FerricStore.flow_schedule_delete(id, now_ms: now + 5001)
    assert {:ok, cancelled} = FerricStore.flow_schedule_get(id)
    assert cancelled.initial_run_at_ms == before.initial_run_at_ms
    assert cancelled.start_at_ms == before.start_at_ms
  end

  test "read-only management pages prepare denied mutations before rendering forms" do
    username = "ops-reader-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "-@all",
               "+FLOW.QUERY",
               "+FLOW.SCHEDULE.LIST",
               "+FLOW.POLICY.GET",
               "+FLOW.GOVERNANCE.OVERVIEW",
               "%R~*"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    opts = [acl_username: username]
    schedules = Schedules.collect_page(opts)
    assert schedules.action_capabilities.create == false
    refute Dashboard.render_flow_schedules_page(schedules) =~ "Create durable schedule"
    policies = PolicyRetention.collect_policies_page(opts)
    refute policies.action_capabilities.save
    refute Dashboard.render_flow_policies_page(policies) =~ ">Save Policy</button>"
    retention = PolicyRetention.collect_retention_page(opts)
    refute retention.action_capabilities.cleanup
    refute FlowRetention.render_flow_retention_controls(retention) =~ ">Run Cleanup</button>"
    governance = Governance.collect_page(opts)
    refute governance.action_capabilities.open_circuit

    refute Dashboard.render_flow_governance_page(governance) =~
             ~s(name="action" value="open_circuit")
  end

  test "visible stream snapshots filter before the retained row cap" do
    alias Ferricstore.Commands.Stream.{Groups, Waiters}
    Groups.snapshot(0)
    Waiters.snapshot(0)
    prefix = "ops-visible-#{System.unique_integer([:positive])}"
    visible = prefix <> ":visible"
    hidden = prefix <> ":hidden"
    now = System.monotonic_time(:microsecond)
    :ets.insert(Ferricstore.Stream.Groups, {{visible, "group"}, "0-0", %{}, %{}})
    :ets.insert(Ferricstore.Stream.Groups, {{hidden, "group"}, "0-0", %{}, %{1 => %{}}})
    :ets.insert(:ferricstore_stream_waiters, {visible, self(), "1-0", now})
    for i <- 1..2, do: :ets.insert(:ferricstore_stream_waiters, {hidden, self(), "#{i}-0", now})

    on_exit(fn ->
      for key <- [visible, hidden] do
        :ets.delete(Ferricstore.Stream.Groups, {key, "group"})
        :ets.delete(:ferricstore_stream_waiters, key)
      end
    end)

    assert [%{key: ^visible}] = Groups.snapshot(1, &(&1.key == visible))
    assert [%{key: ^visible}] = Waiters.snapshot(1, &(&1.key == visible))
  end

  test "policy filters use the shared matched-count and no-match contract" do
    html =
      FerricstoreServer.Health.Dashboard.Render.FlowPolicy.render_flow_policies_table([], %{})

    assert html =~ "data-dashboard-filter-status"
    assert html =~ "data-dashboard-table-filter"
  end

  test "management scope and authorization preserve literal identifiers" do
    alias FerricstoreServer.Health.Endpoint.RouteRequirements
    literal = " ops literal "
    assert PolicyRetention.clean_form_value(literal) == literal
    assert Governance.opts_from_query(URI.encode_query(%{"scope" => literal}))[:scope] == literal

    assert RouteRequirements.flow_policy_form_requirement(%{"type" => literal}) ==
             {"FLOW.POLICY.SET", key: {literal, :write}}

    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/schedules?id=known"
           ) == {"FLOW.SCHEDULE.GET", key: {"*", :read}}
  end

  test "static latency estimates are not presented as measured cache performance" do
    data = FerricstoreServer.Health.Dashboard.Data.Operational.collect_hotcold()
    html = FerricstoreServer.Health.Dashboard.Render.Overview.render_cache_performance(data)
    refute html =~ "~1-5us"
    refute html =~ "~50-200us"
    refute html =~ "Latency is usually"
  end

  test "observation scans stop exactly at their budget" do
    table = :ets.new(:ops_snapshot, [:set])
    for i <- 1..20, do: :ets.insert(table, {i, i})

    assert 3 ==
             Ferricstore.ObservabilitySnapshot.fold(fn _row, count -> count + 1 end, 0, table, 3)

    assert 0 ==
             Ferricstore.ObservabilitySnapshot.fold(
               fn _, _ -> flunk("zero budget must not read") end,
               0,
               table,
               0
             )

    :ets.delete(table)
  end

  test "pubsub visible top rows do not lose lower-ranked authorized channels or patterns" do
    visible = "ops-pubsub-#{System.unique_integer([:positive])}"
    hidden = visible <> "-hidden"
    test_pid = self()

    extra_pid =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    for {name, pid} <- [{visible, test_pid}, {hidden, test_pid}, {hidden, extra_pid}] do
      assert :ok = Ferricstore.PubSub.subscribe(name, pid)
      assert :ok = Ferricstore.PubSub.psubscribe(name, pid)
    end

    on_exit(fn ->
      for key <- [visible, hidden], pid <- [test_pid, extra_pid] do
        Ferricstore.PubSub.unsubscribe(key, pid)
        Ferricstore.PubSub.punsubscribe(key, pid)
      end

      send(extra_pid, :stop)
    end)

    snapshot =
      Ferricstore.PubSub.subscription_snapshot(1, fn row ->
        Map.get(row, :channel, Map.get(row, :pattern)) == visible
      end)

    assert [%{channel: ^visible, subscribers: 1}] = snapshot.channels
    assert [%{pattern: ^visible, subscribers: 1}] = snapshot.patterns
  end

  test "prepared permissions match write scope and remain denied for the other type" do
    alias FerricstoreServer.Health.Dashboard.Flow.ManagementActions
    username = "ops-writer-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "-@all",
               "+FLOW.POLICY.SET",
               "%W~allowed-type"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    assert ManagementActions.policy(username, "allowed-type").save
    refute ManagementActions.policy(username, "other-type").save
    refute ManagementActions.retention(username).cleanup
  end

  test "schedule action capabilities also require the POST entry command" do
    alias FerricstoreServer.Health.Dashboard.Flow.ManagementActions
    username = "ops-schedule-writer-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "-@all",
               "+FLOW.SCHEDULE.GET",
               "+FLOW.SCHEDULE.CREATE",
               "%R~*",
               "%W~*"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    refute ManagementActions.schedules(username).create
    assert :ok = Acl.set_user(username, ["+FLOW.SCHEDULE.LIST"])
    assert ManagementActions.schedules(username).create
  end

  test "retention and governance actions retain their POST entry requirements" do
    alias FerricstoreServer.Health.Dashboard.Flow.ManagementActions
    username = "ops-action-writer-#{System.unique_integer([:positive])}"

    assert :ok =
             Acl.set_user(username, [
               "on",
               "nopass",
               "-@all",
               "+FLOW.RETENTION_CLEANUP",
               "+FLOW.CIRCUIT.OPEN",
               "%W~*"
             ])

    on_exit(fn -> Acl.del_user(username) end)
    refute ManagementActions.retention(username).cleanup
    refute ManagementActions.governance(username, "effect:review").open_circuit
    assert :ok = Acl.set_user(username, ["+FLOW.QUERY", "+FLOW.GOVERNANCE.OVERVIEW"])
    assert ManagementActions.retention(username).cleanup
    refute ManagementActions.governance(username, "effect:review").open_circuit
    assert :ok = Acl.set_user(username, ["+FLOW.CIRCUIT.GET", "%R~effect:review"])
    assert ManagementActions.governance(username, "effect:review").open_circuit
  end

  test "one-shot and delay original timing remains available after cancellation" do
    now = System.system_time(:millisecond)

    for {kind, opts} <- [{:one_shot, [at_ms: now + 60_000]}, {:delay, [delay_ms: 60_000]}] do
      id = "ops-original-#{kind}-#{System.unique_integer([:positive])}"

      assert {:ok, _} =
               FerricStore.flow_schedule_create(
                 id,
                 opts ++ [now_ms: now, target: [type: "ops-review"]]
               )

      on_exit(fn -> FerricStore.flow_schedule_delete(id) end)
      assert {:ok, definition} = FerricStore.flow_schedule_get(id)
      assert definition.initial_run_at_ms == now + 60_000
      assert definition.delay_ms == if(kind == :delay, do: 60_000)
      flush_projection()
      assert :ok = FerricStore.flow_schedule_delete(id)
      assert {:ok, cancelled} = FerricStore.flow_schedule_get(id)
      assert cancelled.initial_run_at_ms == definition.initial_run_at_ms
      assert cancelled.delay_ms == definition.delay_ms
      flush_projection()
    end
  end

  test "untracked prefix counters remain unavailable while measured zero stays zero" do
    prefix = "ops-untracked-#{System.unique_integer([:positive])}"
    assert Ferricstore.Stats.hotness_for_prefix(prefix) == nil
    :ets.insert(:ferricstore_hotness, {prefix, 0, 0})
    on_exit(fn -> :ets.delete(:ferricstore_hotness, prefix) end)
    assert Ferricstore.Stats.hotness_for_prefix(prefix) == {0, 0}

    html =
      Prefixes.render_prefixes_table(%{
        prefixes: [%{prefix: prefix, keys: 1, pct: 100, hot_reads: nil, cold_reads: nil}],
        total_sampled: 1
      })

    assert html =~ "Unavailable"
  end

  defp flush_projection do
    ctx = FerricStore.Instance.get(:default)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, ctx.shard_count, 30_000)
  end
end
