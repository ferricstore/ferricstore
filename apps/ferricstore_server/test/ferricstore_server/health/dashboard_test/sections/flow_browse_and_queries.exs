defmodule FerricstoreServer.Health.DashboardTest.Sections.FlowBrowseAndQueries do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

      describe "render_flow_states_page/1" do
        test "renders state-centric Flow page" do
          data = Dashboard.collect_flow_states_page()
          html = Dashboard.render_flow_states_page(data)

          assert String.contains?(html, "Flow States")
          assert String.contains?(html, "Oldest Due")
          assert String.contains?(html, "Due Now")
        end

        test "filters state-centric Flow page by workflow type" do
          email_type = "dashboard-email-#{System.unique_integer([:positive])}"
          sms_type = "dashboard-sms-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(
                     "dashboard-email-id-#{System.unique_integer([:positive])}",
                     type: email_type,
                     state: "queued",
                     run_at_ms: 1_000
                   )

          assert :ok =
                   FerricStore.flow_create(
                     "dashboard-sms-id-#{System.unique_integer([:positive])}",
                     type: sms_type,
                     state: "queued",
                     run_at_ms: 1_000
                   )

          data = Dashboard.collect_flow_states_page(type: email_type)
          html = Dashboard.render_flow_states_page(data)

          assert data.type_filter == email_type
          assert Enum.all?(data.states, &(&1.type == email_type))
          assert String.contains?(html, ~s(name="type"))
          assert String.contains?(html, ~s(value="#{email_type}" selected))
          assert String.contains?(html, email_type)

          assert String.contains?(
                   html,
                   ~s(data-dashboard-live-url="/dashboard/api/flow/states?type=)
                 )
        end

        test "filters state-centric Flow page by type, state, name, updated range, and limit" do
          flow_type = "dashboard-filter-#{System.unique_integer([:positive])}"
          keep_old_id = "dashboard-filter-keep-old-#{System.unique_integer([:positive])}"
          keep_new_id = "dashboard-filter-keep-new-#{System.unique_integer([:positive])}"
          drop_id = "dashboard-filter-drop-#{System.unique_integer([:positive])}"
          other_state_id = "dashboard-filter-other-#{System.unique_integer([:positive])}"
          now_ms = System.system_time(:millisecond)

          assert :ok =
                   FerricStore.flow_create(keep_old_id,
                     type: flow_type,
                     state: "queued",
                     run_at_ms: now_ms,
                     now_ms: now_ms
                   )

          assert :ok =
                   FerricStore.flow_create(keep_new_id,
                     type: flow_type,
                     state: "queued",
                     run_at_ms: now_ms + 10,
                     now_ms: now_ms + 10
                   )

          assert :ok =
                   FerricStore.flow_create(drop_id,
                     type: flow_type,
                     state: "queued",
                     run_at_ms: now_ms,
                     now_ms: now_ms
                   )

          assert :ok =
                   FerricStore.flow_create(other_state_id,
                     type: flow_type,
                     state: "waiting",
                     run_at_ms: now_ms,
                     now_ms: now_ms
                   )

          data =
            Dashboard.collect_flow_states_page(
              type: flow_type,
              state: "queued",
              q: "keep",
              from_ms: now_ms - 1_000,
              to_ms: now_ms + 1_000,
              limit: 1
            )

          html = Dashboard.render_flow_states_page(data)

          assert data.type_filter == flow_type
          assert data.state_filter == "queued"
          assert data.name_filter == "keep"
          assert data.limit == 1
          assert data.filtered_sampled == 2
          assert Enum.map(data.records, &Map.fetch!(&1, :id)) == [keep_new_id]
          assert Enum.all?(data.states, &(&1.type == flow_type and &1.state == "queued"))
          assert Enum.map(data.states, & &1.count) == [2]
          assert String.contains?(html, ~s(name="state"))
          assert String.contains?(html, ~s(name="q"))
          assert String.contains?(html, ~s(name="from"))
          assert String.contains?(html, ~s(name="to"))
          assert String.contains?(html, ~s(name="limit"))
          assert String.contains?(html, "Showing #{flow_type} / queued / id contains keep")
          assert String.contains?(html, "recent limit 1")
          assert String.contains?(html, "Recent Flow Records")
          assert String.contains?(html, "limit 1")
          assert String.contains?(html, keep_new_id)
          refute String.contains?(html, keep_old_id)
          refute String.contains?(html, drop_id)
          refute String.contains?(html, other_state_id)
        end

        test "terminal state filter reads cold projected list records when hot sample misses them" do
          flow_type = "dashboard-cold-terminal-#{System.unique_integer([:positive])}"
          hot_id = "dashboard-cold-terminal-hot-#{System.unique_integer([:positive])}"
          terminal_id = "dashboard-cold-terminal-done-#{System.unique_integer([:positive])}"
          previous_list = Application.get_env(:ferricstore, :flow_dashboard_flow_list_fun)
          test_pid = self()

          assert :ok =
                   FerricStore.flow_create(hot_id,
                     type: flow_type,
                     state: "queued",
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )

          Application.put_env(:ferricstore, :flow_dashboard_flow_list_fun, fn ^flow_type, opts ->
            send(test_pid, {:terminal_list_dashboard_opts, opts})

            {:ok,
             [
               %{
                 id: terminal_id,
                 type: flow_type,
                 state: "completed",
                 partition_key: "tenant-cold-terminal",
                 run_at_ms: 2_000,
                 updated_at_ms: 2_000,
                 attempts: 0
               }
             ]}
          end)

          on_exit(fn -> restore_env(:flow_dashboard_flow_list_fun, previous_list) end)

          data = Dashboard.collect_flow_states_page(state: "completed", limit: 25)

          assert_receive {:terminal_list_dashboard_opts, opts}
          assert opts[:state] == "completed"
          assert opts[:include_cold] == true
          assert opts[:consistent_projection] == true
          assert opts[:count] >= 25

          assert Enum.any?(data.records, &(Map.get(&1, :id) == terminal_id))
          assert Enum.any?(data.states, &(&1.state == "completed" and &1.count >= 1))
          assert "completed" in data.available_states
          assert data.filtered_sampled <= data.total_sampled
        end

        test "terminal state filter tolerates slower cold projection list reads" do
          flow_type = "dashboard-slow-terminal-#{System.unique_integer([:positive])}"
          hot_id = "dashboard-slow-terminal-hot-#{System.unique_integer([:positive])}"
          terminal_id = "dashboard-slow-terminal-done-#{System.unique_integer([:positive])}"
          previous_list = Application.get_env(:ferricstore, :flow_dashboard_flow_list_fun)

          previous_detail_timeout =
            Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

          assert :ok =
                   FerricStore.flow_create(hot_id,
                     type: flow_type,
                     state: "queued",
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )

          Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

          Application.put_env(:ferricstore, :flow_dashboard_flow_list_fun, fn ^flow_type, _opts ->
            Process.sleep(50)

            {:ok,
             [
               %{
                 id: terminal_id,
                 type: flow_type,
                 state: "completed",
                 partition_key: "tenant-slow-terminal",
                 run_at_ms: 2_000,
                 updated_at_ms: 2_000,
                 attempts: 0
               }
             ]}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_list_fun, previous_list)
            restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_detail_timeout)
          end)

          data = Dashboard.collect_flow_states_page(state: "completed", limit: 25)

          assert Enum.any?(data.records, &(Map.get(&1, :id) == terminal_id))
        end

        test "type-filtered state page includes terminal records projected out of hot keydir" do
          flow_type = "dashboard-terminal-#{System.unique_integer([:positive])}"
          partition_key = "tenant-terminal-#{System.unique_integer([:positive])}"
          complete_id = "dashboard-terminal-complete-#{System.unique_integer([:positive])}"
          fail_id = "dashboard-terminal-fail-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(complete_id,
                     type: flow_type,
                     partition_key: partition_key,
                     state: "queued",
                     run_at_ms: 1_000,
                     retention_ttl_ms: 60_000,
                     now_ms: 1_000
                   )

          assert :ok =
                   FerricStore.flow_create(fail_id,
                     type: flow_type,
                     partition_key: partition_key,
                     state: "queued",
                     run_at_ms: 1_000,
                     retention_ttl_ms: 60_000,
                     now_ms: 1_000
                   )

          assert {:ok, [first_claim, second_claim]} =
                   FerricStore.flow_claim_due(flow_type,
                     partition_key: partition_key,
                     worker: "dashboard-worker",
                     lease_ms: 30_000,
                     limit: 2,
                     now_ms: 1_000
                   )

          claims_by_id = Map.new([first_claim, second_claim], &{Map.fetch!(&1, :id), &1})
          complete_claim = Map.fetch!(claims_by_id, complete_id)
          fail_claim = Map.fetch!(claims_by_id, fail_id)

          assert :ok =
                   FerricStore.flow_complete(complete_id, complete_claim.lease_token,
                     partition_key: partition_key,
                     fencing_token: complete_claim.fencing_token,
                     result: "ok",
                     now_ms: 1_100
                   )

          assert :ok =
                   FerricStore.flow_fail(fail_id, fail_claim.lease_token,
                     partition_key: partition_key,
                     fencing_token: fail_claim.fencing_token,
                     error: "bad",
                     now_ms: 1_100
                   )

          assert {:ok, %{state: "completed"}} =
                   FerricStore.flow_get(complete_id, partition_key: partition_key)

          assert {:ok, %{state: "failed"}} =
                   FerricStore.flow_get(fail_id, partition_key: partition_key)

          data = Dashboard.collect_flow_states_page(type: flow_type)

          assert Enum.any?(data.records, &(Map.get(&1, :id) == complete_id))
          assert Enum.any?(data.records, &(Map.get(&1, :id) == fail_id))
          assert Enum.any?(data.states, &(&1.state == "completed" and &1.count >= 1))
          assert Enum.any?(data.states, &(&1.state == "failed" and &1.count >= 1))
        end

        test "Flow states filter renders friendly time controls and parses datetime-local input" do
          from_ms =
            DateTime.to_unix(
              DateTime.from_naive!(~N[2026-05-25 12:30:00], "Etc/UTC"),
              :millisecond
            )

          to_ms =
            DateTime.to_unix(
              DateTime.from_naive!(~N[2026-05-25 13:00:00], "Etc/UTC"),
              :millisecond
            )

          opts =
            Dashboard.flow_states_opts_from_query(
              URI.encode_query(%{"from" => "2026-05-25T12:30", "to" => "2026-05-25T13:00"})
            )

          assert opts[:from_ms] == from_ms
          assert opts[:to_ms] == to_ms

          html =
            Dashboard.collect_flow_states_page(from_ms: from_ms, to_ms: to_ms)
            |> Dashboard.render_flow_states_page()

          assert String.contains?(html, ~s(name="range"))
          assert String.contains?(html, ~s(type="datetime-local"))
          assert String.contains?(html, ~s(title="Filter by workflow type"))

          assert String.contains?(
                   html,
                   ~s(title="Use a quick sliding window or Custom for From/To")
                 )

          assert String.contains?(html, ~s(title="Apply Flow state filters"))
          assert String.contains?(html, ~s(value="2026-05-25T12:30"))
          assert String.contains?(html, ~s(value="2026-05-25T13:00"))
          refute String.contains?(html, ~s(value="#{from_ms}"))
        end

        test "Flow states quick range keeps a sliding live query" do
          before_ms = System.system_time(:millisecond)
          data = Dashboard.collect_flow_states_page(range: "1h", limit: 1)
          after_ms = System.system_time(:millisecond)
          html = Dashboard.render_flow_states_page(data)

          assert data.filters.range == "1h"
          assert data.from_ms >= before_ms - 3_600_000
          assert data.from_ms <= after_ms - 3_600_000
          assert data.to_ms == nil
          assert String.contains?(html, ~s(name="range"))
          assert String.contains?(html, ~s(value="1h" selected))
          assert Regex.match?(~r/id="flow-state-from-filter"[^>]*value=""/, html)
          assert Regex.match?(~r/id="flow-state-to-filter"[^>]*value=""/, html)
          assert String.contains?(html, "updated last 1 hour")
          assert String.contains?(html, "range=1h")
        end

        test "renders retry, failed, and expired lease help in state rows" do
          data = %{
            states: [
              %{
                type: "email",
                state: "queued",
                mode: :fifo,
                count: 3,
                due_now: 2,
                running: 0,
                retrying: 1,
                failed: 0,
                expired_leases: 0,
                max_attempts_reached: 0,
                oldest_due_ms: 1_000
              },
              %{
                type: "email",
                state: "running",
                count: 1,
                due_now: 0,
                running: 1,
                retrying: 0,
                failed: 0,
                expired_leases: 1,
                max_attempts_reached: 0,
                oldest_due_ms: 0
              },
              %{
                type: "email",
                state: "failed",
                count: 1,
                due_now: 0,
                running: 0,
                retrying: 0,
                failed: 1,
                expired_leases: 0,
                max_attempts_reached: 1,
                oldest_due_ms: 0
              }
            ],
            records: [],
            available_types: ["email"],
            type_filter: "email",
            total_sampled: 5,
            filtered_sampled: 5,
            sample_limit: 400
          }

          html = Dashboard.render_flow_states_page(data)

          assert String.contains?(html, "Retrying")
          assert String.contains?(html, "Failed")
          assert String.contains?(html, "Maxed")
          assert String.contains?(html, "Mode")
          assert String.contains?(html, "FIFO")
          assert String.contains?(html, "lease deadline passed")
          assert String.contains?(html, "terminal failed")
          assert String.contains?(html, "attempts")
          assert String.contains?(html, ~s(role="img"))

          assert String.contains?(
                   html,
                   ~s(data-tooltip="Running flows whose lease deadline passed)
                 )

          assert String.contains?(
                   html,
                   ~s(data-tooltip="Updated quick ranges are sliding windows)
                 )
        end

        test "renders FIFO lane observability from policy and sampled records" do
          suffix = System.unique_integer([:positive])
          type = "dashboard-fifo-lane-#{suffix}"
          partition_key = "tenant:dashboard-fifo-lane:#{suffix}"
          first_id = "dashboard-fifo-lane-first-#{suffix}"
          second_id = "dashboard-fifo-lane-second-#{suffix}"
          now_ms = System.system_time(:millisecond)

          assert {:ok, _policy} =
                   FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})

          assert :ok =
                   FerricStore.flow_create(first_id,
                     type: type,
                     state: "queued",
                     partition_key: partition_key,
                     now_ms: now_ms - 2,
                     run_at_ms: now_ms - 2
                   )

          assert :ok =
                   FerricStore.flow_create(second_id,
                     type: type,
                     state: "queued",
                     partition_key: partition_key,
                     now_ms: now_ms - 1,
                     run_at_ms: now_ms - 1
                   )

          assert {:ok, [claimed]} =
                   FerricStore.flow_claim_due(type,
                     state: "queued",
                     partition_key: partition_key,
                     worker: "dashboard-worker",
                     limit: 10,
                     lease_ms: 60_000,
                     now_ms: now_ms
                   )

          assert claimed.id == first_id

          data = Dashboard.collect_flow_states_page(type: type)
          html = Dashboard.render_flow_states_page(data)

          assert Enum.any?(
                   data.states,
                   &(&1.type == type and &1.state == "queued" and &1.mode == :fifo)
                 )

          assert Enum.any?(
                   data.fifo_lanes,
                   &(&1.type == type and &1.partition_key == partition_key)
                 )

          assert String.contains?(html, "FIFO Lanes")
          assert String.contains?(html, "blocked by active flow")
          assert String.contains?(html, first_id)
          assert String.contains?(html, second_id)
          assert String.contains?(html, "dashboard-worker")
        end
      end

      describe "render_flow_workers_page/1" do
        test "renders workers and lease health" do
          data = Dashboard.collect_flow_workers_page()
          html = Dashboard.render_flow_workers_page(data)

          assert String.contains?(html, "Flow Workers")
          assert String.contains?(html, "Running")
          assert String.contains?(html, "Expired")
        end
      end

      describe "render_flow_due_page/1" do
        test "renders due and scheduled work" do
          data = Dashboard.collect_flow_due_page()
          html = Dashboard.render_flow_due_page(data)

          assert String.contains?(html, "Due / Scheduled")
          assert String.contains?(html, "Why Waiting")
          assert String.contains?(html, "Run At")
        end
      end

      describe "Flow failures, lineage, and query pages" do
        test "renders failures page with recovery controls and bounded records" do
          now_ms = System.system_time(:millisecond)

          html =
            Dashboard.render_flow_failures_page(%{
              filters: %{type: "email", partition_key: nil, q: nil, limit: 40, scan_exact: false},
              available_types: ["email"],
              flash: nil,
              summary: %{total: 1, failed: 1, expired_leases: 0, maxed: 1},
              total_sampled: 1,
              filtered_sampled: 1,
              sample_limit: 400,
              candidates: [
                %{
                  id: "failed-flow",
                  type: "email",
                  state: "failed",
                  attempts: 3,
                  max_attempts: 3,
                  updated_at_ms: now_ms
                }
              ]
            })

          assert String.contains?(html, "Flow Failures")
          assert String.contains?(html, "Recovery Actions")
          assert String.contains?(html, "FLOW.RECLAIM")
          assert String.contains?(html, "failed-flow")
          assert String.contains?(html, "terminal failed")
        end

        test "failures page does not run exact failure/stuck queries unless requested" do
          previous_failures = Application.get_env(:ferricstore, :flow_dashboard_flow_failures_fun)
          previous_stuck = Application.get_env(:ferricstore, :flow_dashboard_flow_stuck_fun)
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_flow_failures_fun, fn type, _opts ->
            send(test_pid, {:exact_failure_scan, type})
            {:ok, []}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_stuck_fun, fn type, _opts ->
            send(test_pid, {:exact_stuck_scan, type})
            {:ok, []}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_failures_fun, previous_failures)
            restore_env(:flow_dashboard_flow_stuck_fun, previous_stuck)
          end)

          _data = Dashboard.collect_flow_failures_page(type: "email")
          refute_received {:exact_failure_scan, "email"}
          refute_received {:exact_stuck_scan, "email"}

          _data = Dashboard.collect_flow_failures_page(type: "email", scan_exact: true)
          assert_received {:exact_failure_scan, "email"}
          assert_received {:exact_stuck_scan, "email"}
        end

        test "exact failure scan errors are visible and not collapsed to authoritative zero" do
          previous_failures = Application.get_env(:ferricstore, :flow_dashboard_flow_failures_fun)
          previous_stuck = Application.get_env(:ferricstore, :flow_dashboard_flow_stuck_fun)

          Application.put_env(:ferricstore, :flow_dashboard_flow_failures_fun, fn _type, _opts ->
            {:error, :backend_down}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_stuck_fun, fn _type, _opts ->
            {:ok, []}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_failures_fun, previous_failures)
            restore_env(:flow_dashboard_flow_stuck_fun, previous_stuck)
          end)

          data = Dashboard.collect_flow_failures_page(type: "email", scan_exact: true)
          html = Dashboard.render_flow_failures_page(data)

          assert data.exact_scan_status.failures == {:error, :backend_down}
          assert data.exact_scan_status.stuck == :ok
          assert String.contains?(html, "Exact scan issue")
          assert String.contains?(html, "FLOW.FAILURES")
          assert String.contains?(html, "backend_down")
        end

        test "signals page does not read histories unless scan is requested" do
          flow_type = "dashboard-signal-scan-#{System.unique_integer([:positive])}"
          id = "dashboard-signal-scan-flow-#{System.unique_integer([:positive])}"
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
          test_pid = self()

          assert :ok =
                   FerricStore.flow_create(id,
                     type: flow_type,
                     state: "queued",
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
            send(test_pid, {:signal_history_scan, id})
            {:ok, []}
          end)

          on_exit(fn -> restore_env(:flow_dashboard_flow_history_fun, previous_history) end)

          _data = Dashboard.collect_flow_signals_page(type: flow_type)
          refute_received {:signal_history_scan, ^id}

          _data = Dashboard.collect_flow_signals_page(type: flow_type, scan_history: true)
          assert_received {:signal_history_scan, ^id}
        end

        test "lineage page uses injected root query and renders graph/table components" do
          previous = Application.get_env(:ferricstore, :flow_dashboard_flow_by_root_fun)
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_flow_by_root_fun, fn "root-1", opts ->
            send(test_pid, {:lineage_opts, opts})

            {:ok,
             [
               %{
                 id: "child-1",
                 type: "order",
                 state: "queued",
                 root_flow_id: "root-1",
                 parent_flow_id: "root-1",
                 correlation_id: "order-1",
                 updated_at_ms: 1_000
               }
             ]}
          end)

          on_exit(fn -> restore_env(:flow_dashboard_flow_by_root_fun, previous) end)

          data = Dashboard.collect_flow_lineage_page(mode: "root", target: "root-1", limit: 10)
          html = Dashboard.render_flow_lineage_page(data)

          assert_receive {:lineage_opts, opts}
          assert opts[:include_cold] == true
          assert opts[:consistent_projection] == true
          assert opts[:count] == 10
          assert data.result.command == "FLOW.BY_ROOT"
          assert String.contains?(html, "Flow Lineage")
          assert String.contains?(html, "Lineage Map")
          assert String.contains?(html, "child-1")
          assert String.contains?(html, "order-1")
        end

        test "query explorer renders idle and result states without payload hydration" do
          idle =
            Dashboard.collect_flow_query_page(kind: "history")
            |> Dashboard.render_flow_query_page()

          assert String.contains?(idle, "Flow Query Explorer")
          assert String.contains?(idle, "Enter an id")
          assert String.contains?(idle, "FLOW.HISTORY")
          assert String.contains?(idle, "Flow ID")
          assert String.contains?(idle, "Attribute key")
          assert String.contains?(idle, "Attribute value")
          assert String.contains?(idle, "FLOW.STATS")
          assert String.contains?(idle, ~s(data-flow-query-form))
          assert String.contains?(idle, ~s(data-flow-query-kind))
          assert idle =~ ~r/data-flow-query-field="type"[^>]*hidden/
          assert idle =~ ~r/name="type"[^>]*disabled/
          assert idle =~ ~r/data-flow-query-field="from"[^>]*hidden/
          assert idle =~ ~r/name="from"[^>]*disabled/

          html =
            Dashboard.render_flow_query_page(%{
              filters: %{
                kind: "list",
                type: "email",
                state: nil,
                id: nil,
                partition_key: nil,
                limit: 40,
                from_ms: nil,
                to_ms: nil,
                rev: false
              },
              available_types: ["email"],
              total_sampled: 1,
              sample_limit: 400,
              result: %{
                status: :ok,
                command: "FLOW.LIST",
                message: "1 row",
                rows: [
                  %{
                    id: "query-flow",
                    type: "email",
                    state: "queued",
                    updated_at_ms: 1_000
                  }
                ]
              }
            })

          assert String.contains?(html, "Safe Query Explorer")
          assert String.contains?(html, "FLOW.LIST")
          assert String.contains?(html, "Workflow Type")
          assert String.contains?(html, "State")
          assert String.contains?(html, "From UTC")
          assert html =~ ~r/data-flow-query-field="id"[^>]*hidden/
          assert html =~ ~r/name="id"[^>]*disabled/
          assert String.contains?(html, "query-flow")
        end

        test "query explorer renders stats with attribute filters" do
          html =
            Dashboard.render_flow_query_page(%{
              filters: %{
                kind: "stats",
                type: "email",
                state: "queued",
                id: nil,
                partition_key: "tenant-a",
                attribute_key: "tenant",
                attribute_value: "acme",
                limit: 40,
                from_ms: nil,
                to_ms: nil,
                rev: false
              },
              available_types: ["email"],
              total_sampled: 1,
              sample_limit: 400,
              result: %{
                status: :ok,
                command: "FLOW.STATS",
                message: "1 matching row",
                rows: %{
                  type: "email",
                  state: "queued",
                  attributes: %{"tenant" => "acme"},
                  count: 1
                }
              }
            })

          assert String.contains?(html, "FLOW.STATS")
          assert String.contains?(html, "Attribute key")
          assert String.contains?(html, "tenant")
          assert String.contains?(html, "acme")
          assert String.contains?(html, "count")
        end

        test "query explorer runs FLOW.SEARCH with indexed attributes and state metadata" do
          previous_search = Application.get_env(:ferricstore, :flow_dashboard_flow_search_fun)
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_flow_search_fun, fn opts ->
            send(test_pid, {:flow_search_opts, opts})

            {:ok,
             [
               %{
                 id: "search-flow",
                 type: "email",
                 state: "queued",
                 partition_key: "tenant-a",
                 updated_at_ms: 1_000
               }
             ]}
          end)

          on_exit(fn -> restore_env(:flow_dashboard_flow_search_fun, previous_search) end)

          data =
            Dashboard.collect_flow_query_page(
              kind: "search",
              type: "email",
              partition_key: "tenant-a",
              attribute_key: "tenant",
              attribute_value: "acme",
              state_meta_state: "review",
              state_meta_key: "risk_tier",
              state_meta_value: "high",
              limit: 7
            )

          assert_receive {:flow_search_opts, opts}
          assert Keyword.fetch!(opts, :type) == "email"
          assert Keyword.fetch!(opts, :partition_key) == "tenant-a"
          assert Keyword.fetch!(opts, :count) == 7
          assert Keyword.fetch!(opts, :consistent_projection) == true
          assert Keyword.fetch!(opts, :attributes) == %{"tenant" => "acme"}
          assert Keyword.fetch!(opts, :state_meta) == %{"review" => %{"risk_tier" => "high"}}

          html = Dashboard.render_flow_query_page(data)

          assert String.contains?(html, "FLOW.SEARCH")
          assert String.contains?(html, "State meta state")
          assert String.contains?(html, "search-flow")
        end

        test "query explorer only renders fields relevant to lineage query kind" do
          html =
            Dashboard.collect_flow_query_page(kind: "by_correlation", id: "corr-1")
            |> Dashboard.render_flow_query_page()

          assert String.contains?(html, "FLOW.BY_CORRELATION")
          assert String.contains?(html, "Correlation ID")
          assert String.contains?(html, "Partition")
          assert String.contains?(html, "From UTC")
          assert html =~ ~r/data-flow-query-field="type"[^>]*hidden/
          assert html =~ ~r/name="type"[^>]*disabled/
          assert html =~ ~r/data-flow-query-field="state"[^>]*hidden/
          assert html =~ ~r/name="state"[^>]*disabled/
        end
      end

      describe "Flow overview projection health" do
        test "renders projection health inside overview instead of a separate page" do
          data = Dashboard.collect_flow_page()
          html = Dashboard.render_flow_page(data)

          assert String.contains?(html, "Projection Health")
          assert String.contains?(html, "LMDB")
          assert String.contains?(html, "lagged")
          assert String.contains?(html, "Pending")
          refute String.contains?(html, "LMDB Mode")
          refute String.contains?(String.downcase(html), "mirror")
          refute String.contains?(html, ~s(href="/dashboard/flow/projections"))
          assert String.contains?(html, ~s(href="/dashboard/flow/governance"))
        end

        test "renders grouped projection health instead of raw metric names" do
          html =
            Dashboard.render_flow_page(%{
              summary: %{},
              types: [],
              workers: [],
              records: [],
              total_sampled: 0,
              sample_limit: 400,
              projection: %{
                lmdb_projection: :lagged,
                lmdb_flush_interval_ms: 1_000,
                history_flush_interval_ms: 0,
                metrics: [
                  %{name: ~s(ferricstore_flow_lmdb_replay_safe_lag{shard_index="0"}), value: "7"},
                  %{
                    name: ~s(ferricstore_flow_lmdb_writer_pending_ops{shard_index="0"}),
                    value: "3"
                  },
                  %{
                    name:
                      ~s(ferricstore_flow_lmdb_projection_enqueue_failures_total{shard_index="0"}),
                    value: "1"
                  }
                ]
              }
            })

          assert String.contains?(html, "Health")
          assert String.contains?(html, "failures")
          assert String.contains?(html, ">7<")
          refute String.contains?(html, "ferricstore_flow_lmdb_replay_safe_lag")
        end
      end
    end
  end
end
