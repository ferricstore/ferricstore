defmodule FerricstoreServer.Health.Dashboard.FlowUxRedesignTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.Query
  alias FerricstoreServer.Health.Dashboard.Render.FlowDetail
  alias FerricstoreServer.Health.Dashboard.Render.FlowComponents
  alias FerricstoreServer.Health.Dashboard.Render.FlowCharts
  alias FerricstoreServer.Health.Dashboard.Render.FlowHistory
  alias FerricstoreServer.Health.Dashboard.Render.FlowOverview
  alias FerricstoreServer.Health.Dashboard.Render.FlowQueryControls
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Lineage
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Projection
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Signals
  alias FerricstoreServer.Health.Dashboard.Layout.Styles

  describe "Diagnostic Hero Banner" do
    test "renders active running state with worker and lease deadline" do
      now = System.system_time(:millisecond)

      data = %{
        record: %{
          id: "flow-run-1",
          type: "order_workflow",
          state: "running",
          partition_key: "tenant-a",
          worker: "worker-prod-1",
          run_at_ms: now - 5_000,
          lease_expires_at_ms: now + 25_000,
          fencing_token: 3
        },
        waiting_reason: "leased by worker-prod-1",
        state_mode: :parallel,
        fifo_lane: nil
      }

      html = FlowDetail.render_flow_diagnostic_hero(data)

      assert html =~ "flow-diagnostic-hero hero-running"
      assert html =~ "Leased to worker-prod-1"
      assert html =~ "order_workflow" or html =~ "worker-prod-1"
      assert html =~ "Lease expires in"
      assert html =~ ~s(<span class="flow-status-indicator status-dot dot-green")
      refute html =~ "&lt;span"
    end

    test "renders expired lease warning" do
      now = System.system_time(:millisecond)

      data = %{
        record: %{
          id: "flow-expired-1",
          type: "order_workflow",
          state: "running",
          partition_key: "tenant-a",
          worker: "worker-dead",
          run_at_ms: now - 60_000,
          lease_expires_at_ms: now - 10_000,
          fencing_token: 2
        },
        waiting_reason: "lease expired",
        state_mode: :parallel,
        fifo_lane: nil
      }

      html = FlowDetail.render_flow_diagnostic_hero(data)

      assert html =~ "flow-diagnostic-hero hero-blocked"
      assert html =~ "Lease Expired (Worker: worker-dead)"
      assert html =~ "Work is reclaimable"
    end

    test "does not infer signal suspension from explanatory text" do
      data = %{
        record: %{
          id: "flow-signal-wait-1",
          type: "payment_flow",
          state: "awaiting_callback",
          partition_key: "tenant-b",
          run_at_ms: 1_000,
          updated_at_ms: 1_000
        },
        waiting_reason: "waiting for signal payment_hook",
        state_mode: :parallel,
        fifo_lane: nil
      }

      html = FlowDetail.render_flow_diagnostic_hero(data)

      assert html =~ "Ready for worker claim"
      refute html =~ "Waiting for Signal"
      refute html =~ "Signal action"
    end

    test "renders FIFO blocked diagnostic when blocked behind lane head" do
      data = %{
        record: %{
          id: "flow-fifo-blocked-2",
          type: "invoice_flow",
          state: "ready",
          partition_key: "customer-99",
          run_at_ms: 1_000,
          updated_at_ms: 1_000
        },
        waiting_reason: "waiting for fifo head",
        state_mode: :fifo,
        fifo_lane: %{
          head_id: "flow-fifo-head-1",
          head_status: "blocked by active flow",
          count: 5
        }
      }

      html = FlowDetail.render_flow_diagnostic_hero(data)

      assert html =~ "flow-diagnostic-hero hero-idle"
      assert html =~ "Waiting behind FIFO head"
      assert html =~ "Partition: customer-99"
      assert html =~ "flow-fifo-head-1"
    end

    test "renders terminal failed status" do
      data = %{
        record: %{
          id: "flow-failed-1",
          type: "settlement_flow",
          state: "failed",
          partition_key: "tenant-c",
          attempts: 5,
          max_attempts: 5,
          run_at_ms: 1_000,
          updated_at_ms: 1_000
        },
        waiting_reason: "terminal failure: max attempts exceeded",
        state_mode: :parallel,
        fifo_lane: nil
      }

      html = FlowDetail.render_flow_diagnostic_hero(data)

      assert html =~ "flow-diagnostic-hero hero-failed"
      assert html =~ "Terminal Failed"
      assert html =~ "5 attempt(s)"
    end

    test "renders completed status" do
      data = %{
        record: %{
          id: "flow-completed-1",
          type: "settlement_flow",
          state: "completed",
          partition_key: "tenant-c",
          run_at_ms: 1_000,
          updated_at_ms: 2_000
        },
        waiting_reason: "terminal",
        state_mode: :parallel,
        fifo_lane: nil
      }

      html = FlowDetail.render_flow_diagnostic_hero(data)

      assert html =~ "flow-diagnostic-hero hero-completed"
      assert html =~ "Completed"
      assert html =~ "Workflow completed successfully"
    end
  end

  describe "Execution Step Journal & Timeline View Toggle" do
    test "renders visual step journal and raw table with toggle" do
      history = [
        {"1000-1", %{"event" => "created", "state" => "queued"}},
        {"1100-2",
         %{
           "event" => "claim",
           "worker" => "worker-1",
           "state" => "running",
           "from_state" => "queued"
         }},
        {"1200-3",
         %{
           "event" => "retry",
           "state" => "queued",
           "error" => "timeout",
           "attempt" => "2"
         }},
        {"1250-4", %{"event" => "failed", "state" => "failed", "error" => "exhausted"}},
        {"1300-4", %{"event" => "completed", "state" => "completed", "result" => "ok"}}
      ]

      html = FlowHistory.render_flow_history_timeline(history, :ok, nil)

      assert html =~ ~s(class="flow-journal-card")
      assert html =~ ~s(data-journal-view-toggle="tree")
      assert html =~ ~s(data-journal-view-toggle="table")
      assert html =~ ~s(data-journal-view="tree")
      assert html =~ ~s(data-journal-view="table")
      assert html =~ ~s(class="flow-journal-tree")
      assert html =~ ~s(class="journal-step")
      assert html =~ ~s(role="button")
      assert html =~ ~s(aria-expanded="false")
      assert html =~ ~s(aria-controls="journal-inspector-flow-event-MTEwMC0y")
      assert html =~ ~s(id="journal-inspector-flow-event-MTEwMC0y")
      assert html =~ ~s(class="journal-event-inspector" hidden)
      assert html =~ ~s(role="region")
      assert html =~ "Event details"
      assert html =~ "timeout"
      refute html =~ "Input/output diff"
      assert html =~ ~s(class="journal-step-node node-warn")
      assert html =~ ~s(class="journal-step-node node-error")
      assert html =~ ~s(class="journal-step-node node-ok")
      assert html =~ "Created"
      assert html =~ "Retry"
      assert html =~ "Completed"
      assert html =~ "worker-1"
      assert html =~ "Execution Journal"
      refute html =~ "📜"
    end

    test "signal and waterfall jumps target the visible journal" do
      signal_row = %{
        event_id: "1200-3",
        id: "flow-signal-1",
        partition_key: "tenant-a"
      }

      assert Signals.flow_signal_event_href(signal_row, :detail) =~ "#journal-flow-event-"
      assert Signals.flow_signal_event_href(signal_row, :page) =~ "#journal-flow-event-"

      html =
        FlowCharts.render_flow_timeline_chart([
          {"1000-1", %{"event" => "created", "state" => "queued"}},
          {"1200-3", %{"event" => "failed", "state" => "failed"}}
        ])

      assert html =~ ~s(href="#journal-flow-event-)
      assert html =~ ~s(aria-label="Step timing")
    end

    test "waterfall axis marks both edges for contained label alignment" do
      html = FlowCharts.render_flow_step_waterfall_axis(%{total_ms: 2_000})

      assert html =~ ~s(data-axis-edge="start")
      assert html =~ ~s(data-axis-edge="end")
    end
  end

  describe "Guarded recovery actions" do
    test "requires browser confirmation before submitting a lease reclaim" do
      html = FlowComponents.render_flow_recovery_actions(%{})

      assert html =~ ~s(name="confirm_reclaim" value="true" required)
    end
  end

  describe "Failure triage toolbar" do
    test "renders mutually exclusive sample and exact scan modes" do
      data = %{
        filters: %{
          type: "payment",
          partition_key: "tenant-a",
          q: "failed-",
          limit: 40,
          scan_exact: false
        },
        available_types: ["payment"]
      }

      html = FlowComponents.render_flow_failures_controls(data)

      assert html =~ ~s(class="flow-filter-panel flow-failure-filter-panel")
      assert html =~ ~s(role="radiogroup" aria-label="Scan precision")
      assert html =~ ~r/<input[^>]+type="radio"[^>]+name="exact"[^>]+value="false"[^>]+checked/
      assert html =~ ~r/<input[^>]+type="radio"[^>]+name="exact"[^>]+value="true"/
      refute html =~ ~s(type="checkbox" name="exact")

      exact_html =
        FlowComponents.render_flow_failures_controls(%{
          data
          | filters: %{data.filters | scan_exact: true}
        })

      assert exact_html =~
               ~r/<input[^>]+type="radio"[^>]+name="exact"[^>]+value="true"[^>]+checked/
    end

    test "renders failure counts as a compact semantic summary" do
      html =
        FlowComponents.render_flow_failures_summary(%{
          summary: %{total: 12, failed: 7, expired_leases: 3, maxed: 2}
        })

      assert html =~ ~s(<dl class="flow-failure-summary-ribbon")
      assert html =~ "Failure candidates"
      assert html =~ "Expired leases"
      assert html =~ "Maxed retries"
      assert html =~ ">12<"
      assert html =~ ">7<"
      refute html =~ ~s(class="flow-card")
      refute html =~ ~s(class="flow-card-grid")
    end
  end

  describe "Workflow Explorer Status Summary" do
    test "renders one summary without navigation disguised as filters" do
      summary = %{
        types: 8,
        active: 42,
        queued: 20,
        running: 22,
        failed: 3,
        due_now_sampled: 12,
        inflight: 22
      }

      html = FlowOverview.render_flow_overview(summary, 50, 400)

      refute html =~ ~s(class="flow-facets")
      assert html =~ "50 matching records"
      assert html =~ "Ready now"
      assert html =~ "42"
      assert html =~ "12"
      assert html =~ "22"
      assert html =~ "3"
      assert html =~ ~s(<dl class="flow-overview-ribbon")
      refute html =~ ~s(class="flow-card-grid")
    end

    test "labels nonterminal activity and bounded samples without implying totals" do
      html =
        FlowOverview.render_flow_overview(
          %{
            types: 4,
            active: 17,
            queued: 3,
            running: 2,
            failed: 1,
            due_now_sampled: 5,
            inflight: 2
          },
          50,
          400
        )

      assert html =~ "all nonterminal states"
      assert html =~ "50 matching records"
      assert html =~ "scan cap 400"
      refute html =~ "queued + running"
      refute html =~ "sampled 50 / 400"
    end

    test "puts active task issues before the general overview ledger" do
      html =
        Dashboard.render_flow_page(%{
          summary: %{
            types: 1,
            active: 2,
            queued: 1,
            running: 1,
            failed: 1,
            due_now_sampled: 1,
            expired_leases_sampled: 0,
            inflight: 1
          },
          types: [],
          workers: [],
          records: [],
          total_sampled: 2,
          filtered_sampled: 2,
          sample_limit: 400,
          projection: Projection.default_flow_projection_health()
        })

      {issues_position, _length} = :binary.match(html, ">Needs attention</h2>")
      {overview_position, _length} = :binary.match(html, ">Flow Overview ")

      assert issues_position < overview_position
    end
  end

  describe "Workflow pressure and projection ledgers" do
    test "state pressure matrix scales independent columns and preserves exact zeroes" do
      html =
        FlowCharts.render_flow_states_chart([
          %{
            type: "orders",
            state: "ready",
            count: 120,
            due_now: 100,
            running: 0,
            retrying: 10,
            failed: 0,
            expired_leases: 2
          },
          %{
            type: "orders",
            state: "review",
            count: 20,
            due_now: 10,
            running: 5,
            retrying: 1,
            failed: 4,
            expired_leases: 0
          }
        ])

      assert html =~ ~s(class="flow-state-pressure-matrix")
      assert html =~ ~s(data-state="orders:ready")
      assert html =~ ~s(data-state="orders:review")

      assert html =~
               ~r/data-state="orders:review"[\s\S]+?data-metric="due"[\s\S]+?style="width: 10%"/

      assert html =~
               ~r/data-state="orders:ready"[\s\S]+?data-metric="running"[\s\S]+?style="width: 0%"/

      assert html =~ ">120<"
      assert html =~ ">100<"
      assert html =~ ">0<"
      refute html =~ ~s(style="width: 2%")
    end

    test "projection health is a compact labeled ledger" do
      html =
        Projection.render_flow_projection_health(%{
          lmdb_projection: :lagged,
          lmdb_flush_interval_ms: 1_000,
          history_flush_interval_ms: 0,
          metrics: [
            %{name: ~s(ferricstore_flow_lmdb_replay_safe_lag{shard_index="0"}), value: "7"},
            %{
              name: ~s(ferricstore_flow_lmdb_writer_pending_ops{shard_index="0"}),
              value: "3"
            }
          ]
        })

      assert html =~ ~s(<dl class="flow-projection-ledger")
      assert html =~ "Projection Health"
      assert html =~ "requested index minus durable projected index"
      assert html =~ ">7<"
      refute html =~ ~s(class="flow-card-grid")
      refute html =~ ~s(class="flow-card")
    end
  end

  describe "Operator-safe dashboard surfaces" do
    test "Flow tables contain horizontal scrolling inside an accessible region" do
      html =
        FerricstoreServer.Health.Dashboard.Render.FlowTables.Records.render_flow_states_table(
          [],
          100,
          20,
          400,
          %{}
        )

      assert html =~ ~s(class="table-scroll")
      assert html =~ ~s(role="region")
      assert html =~ ~s(aria-label="Flow states")
      assert html =~ ~s(tabindex="0")
      assert html =~ "20 matching of 100 sampled"
      assert html =~ "scan cap 400"
    end

    test "Doctor repair requires an explicit confirmation and prevents duplicate submission" do
      html =
        FerricstoreServer.Health.Dashboard.Render.DoctorPages.render_doctor_actions()

      assert html =~ ~s(class="flow-action-confirm")
      assert html =~ ~s(name="confirm_action" value="true")
      assert html =~ ~s(name="expected_action" value="repair_flow_lmdb")
      assert html =~ ~s(data-dashboard-single-submit)

      refute html =~
               ~s(<button type="submit" class="flow-action-button">Repair Flow LMDB</button>)

      assert {:error, "repair confirmation is required"} =
               FerricstoreServer.Health.Dashboard.apply_doctor_form(%{
                 "action" => "repair_flow_lmdb"
               })

      assert {:error, "repair action changed; review it again"} =
               FerricstoreServer.Health.Dashboard.apply_doctor_form(%{
                 "action" => "repair_flow_lmdb",
                 "confirm_action" => "true",
                 "expected_action" => "different_action"
               })
    end

    test "workflow mutation panel is explicitly guarded against duplicate submits" do
      data = %{
        record: %{
          id: "payment-1",
          type: "payment",
          state: "waiting",
          partition_key: "tenant-a"
        },
        history: [{"1-0", %{"event" => "created", "state" => "waiting"}}]
      }

      html = FlowDetail.render_flow_actions(data)

      assert html =~ ~s(class="flow-operations-panel")
      assert html =~ "Workflow Actions"
      assert html =~ ~s(data-dashboard-single-submit)
      assert html =~ "Rewind"
      assert html =~ "Send Signal"
    end

    test "initial Keyspace state asks for a query instead of reporting no matches" do
      html =
        FerricstoreServer.Health.Dashboard.Render.KVPages.render_keyspace_table(%{
          rows: [],
          total_sampled: 0,
          searched?: false
        })

      assert html =~ "Enter an exact key or prefix"
      refute html =~ "No key metadata matched this query"

      page =
        FerricstoreServer.Health.Dashboard.render_keyspace_page(%{
          filters: %{key: "", prefix: "", include_internal: false, limit: 50},
          rows: [],
          inspected: nil,
          total_sampled: 0,
          searched?: false
        })

      assert page =~ ~s(data-dashboard-live-url="/dashboard/api/keyspace")
      refute page =~ ~s(data-dashboard-live-url="/dashboard/api/keyspace?)
    end

    test "page purpose is compact and does not repeat its title" do
      html =
        FerricstoreServer.Health.Dashboard.Layout.render_page_intro(
          "Flow States",
          "Inspect workflow state."
        )

      assert html =~ "Inspect workflow state."
      refute html =~ ~s(class="page-intro-title")
      refute html =~ ">Flow States<"
    end

    test "empty operational sections use collapsed progressive disclosure" do
      html =
        FerricstoreServer.Health.Dashboard.Layout.render_dashboard_disclosure(
          "Blocked Readers",
          "0",
          "<table><tbody></tbody></table>",
          open: false
        )

      assert html =~ ~s(<details class="dashboard-disclosure">)

      assert html =~
               ~s(<summary><span>Blocked Readers</span><span class="badge badge-idle">0</span></summary>)

      refute html =~ ~s(<details class="dashboard-disclosure" open>)
    end

    test "sidebar uses collapsible subject groups and opens the active workflow group" do
      html = FerricstoreServer.Health.Dashboard.Layout.render_sidebar_static("flow_states")

      assert html =~ ~s(data-dashboard-nav-group="Workflows")
      assert html =~ ~r/<details[^>]*data-dashboard-nav-group="Workflows"[^>]*open/
      assert html =~ ~s(<summary>Workflows</summary>)
      assert html =~ ~s(href="/dashboard/flow/states")

      assert html =~
               ~s(<div class="nav-subgroup" role="group" aria-labelledby="workflow-nav-operate">)

      assert html =~ ~s(id="workflow-nav-operate">Operate</div>)
      assert html =~ ~s(id="workflow-nav-investigate">Investigate</div>)
      assert html =~ ~s(id="workflow-nav-configure">Configure</div>)
      assert html =~ ~s(href="/dashboard/flow/query")
      assert html =~ ">Query Studio<"

      assert :binary.match(html, ~s(id="workflow-nav-operate")) <
               :binary.match(html, ~s(href="/dashboard/flow/states"))

      assert :binary.match(html, ~s(id="workflow-nav-investigate")) <
               :binary.match(html, ~s(href="/dashboard/flow/failures"))

      assert :binary.match(html, ~s(id="workflow-nav-configure")) <
               :binary.match(html, ~s(href="/dashboard/flow/policies"))
    end

    test "workflow context links preserve only safe compatible filters" do
      html =
        FlowOverview.render_flow_context_tools(
          %{
            filters: %{
              type: "payment/retry",
              partition_key: "customer north",
              state: "awaiting review",
              range: "1h",
              attribute_key: "secret_key",
              attribute_value: "do-not-leak",
              q: "private-flow-id"
            }
          },
          "flow_states"
        )

      assert html =~ ~s(aria-label="Workflow investigation context")
      assert html =~ "payment%2Fretry"
      assert html =~ "customer+north"
      assert html =~ "awaiting+review"
      assert html =~ "range=1h"
      assert html =~ ~s(aria-current="page")
      refute html =~ "secret_key"
      refute html =~ "do-not-leak"
      refute html =~ "private-flow-id"
    end

    test "workflow detail context keeps investigation links without repeating live scope" do
      html =
        FlowOverview.render_flow_context_tools(
          %{
            record: %{
              id: "payment-42",
              type: "payment",
              partition_key: "tenant-a",
              state: "running",
              root_flow_id: "checkout-7"
            }
          },
          "flow_detail",
          show_scope: false
        )

      assert html =~ "Investigate"
      assert html =~ "/dashboard/flow/states?"
      assert html =~ "type=payment"
      assert html =~ "partition_key=tenant-a"
      assert html =~ "/dashboard/flow/signals?"
      assert html =~ "/dashboard/flow/lineage?"
      assert html =~ "id=checkout-7"
      refute html =~ ~s(class="flow-context-chip)
      refute html =~ ">Scope<"
    end

    test "operator attention exposes subsystem failures and sampled workflow risks" do
      data = %{
        subsystem_health: %{
          policy_migration: %{
            status: :warning,
            issues: [
              %{
                shard: 0,
                reason: :policy_catalog_state_projection_pending,
                occurrences: 3
              }
            ]
          }
        },
        shards: [%{index: 0, status: "ok"}],
        memory: %{pressure_level: :normal},
        flow_summary: %{failed: 2, expired_leases_sampled: 1}
      }

      html =
        FerricstoreServer.Health.Dashboard.Render.Overview.render_operator_attention(data)

      assert html =~ "Operator Attention"
      assert html =~ "Policy migration waiting on its state projection"
      assert html =~ "2 sampled failed workflows"
      assert html =~ "1 sampled expired lease"
      assert html =~ ~s(href="/dashboard/flow/failures")
    end

    test "top status cannot report healthy while an operational subsystem is warning" do
      data = %{
        overview: %{status: :ok, total_keys: 1},
        hotcold: %{total_lookups: 0, hit_ratio: 0.0, ops_per_sec: 0.0, sample_rate: 100},
        memory: %{
          pressure_level: :normal,
          max_bytes: 1_000,
          total_bytes: 100,
          ratio: 0.1
        },
        connections: %{active: 1},
        cluster: %{cluster_mode: :standalone, node_name: :test@localhost},
        subsystem_health: %{policy_migration: %{status: :warning, issues: []}}
      }

      html = FerricstoreServer.Health.Dashboard.Render.Overview.render_top_bar(data)

      assert html =~ "dot-yellow"
      assert html =~ ">warning<"
      refute html =~ ">healthy<"
    end
  end

  describe "Enhanced Lineage Nodes" do
    test "recent lineage hints exclude internal schedule control flows" do
      now_ms = System.system_time(:millisecond)
      schedule_id = "lineage-hidden-schedule-#{System.unique_integer([:positive])}"

      assert {:ok, _schedule} =
               FerricStore.flow_schedule_create(schedule_id,
                 kind: :interval,
                 every_ms: 60_000,
                 start_at_ms: now_ms + 60_000,
                 now_ms: now_ms,
                 target: [type: "lineage-hint-target"]
               )

      data = Query.collect_lineage_page()

      refute Enum.any?(data.hints, &String.contains?(&1.id, schedule_id))
    end

    test "recent lineage hints preserve their required partition scope" do
      html =
        Lineage.render_flow_lineage_hints([
          %{mode: "root", label: "root", id: "order/root", partition_key: "tenant north"}
        ])

      assert html =~
               ~s(href="/dashboard/flow/lineage?id=order%2Froot&amp;mode=root&amp;partition_key=tenant+north")

      assert html =~ "tenant north"
    end

    test "lineage empty state distinguishes missing partition from no matches" do
      missing_partition =
        Lineage.render_flow_lineage_nodes([], %{target: "root-1", partition_key: nil})

      no_matches =
        Lineage.render_flow_lineage_nodes([], %{
          target: "root-1",
          partition_key: "tenant-a"
        })

      assert missing_partition =~ "Enter a partition key"
      refute missing_partition =~ "No lineage records matched"
      assert no_matches =~ "No lineage records matched"
    end

    test "renders lineage nodes with status badge and parent info" do
      records = [
        %{
          id: "child-flow-1",
          type: "subtask",
          state: "running",
          parent_flow_id: "root-flow-0",
          partition_key: "tenant-x"
        },
        %{
          id: "child-flow-2",
          type: "subtask",
          state: "completed",
          parent_flow_id: "root-flow-0",
          partition_key: "tenant-x"
        }
      ]

      html = Lineage.render_flow_lineage_nodes(records, %{})

      assert html =~ "child-flow-1"
      assert html =~ "child-flow-2"
      assert html =~ ~s(class="badge badge-merging">running</span>)
      assert html =~ ~s(class="badge badge-ok">completed</span>)
      assert html =~ "parent: root-flow-0"
    end
  end

  describe "Breadcrumb Navigation & Keyboard Shortcuts" do
    test "renders flow breadcrumb with copy buttons" do
      record = %{
        id: "order-step-999",
        type: "order_fulfillment",
        partition_key: "tenant-globex"
      }

      html = FlowDetail.render_flow_breadcrumb(record)

      assert html =~ ~s(class="flow-breadcrumb")
      assert html =~ "order_fulfillment"
      assert html =~ "order-step-999"
      assert html =~ ~s(data-copy-text="order-step-999")
      assert html =~ ~s(data-copy-text="tenant-globex")
      assert html =~ ~s(aria-label="Copy workflow ID")
      assert html =~ "Copy ID"
      refute html =~ "📋"
      refute html =~ "🏷️"
    end

    test "workflow detail exposes one section navigator while preserving durable fields" do
      data = %{
        record: %{
          id: "order-step-999",
          type: "order_fulfillment",
          state: "running",
          run_state: "charge_card",
          partition_key: "tenant-globex",
          worker: "worker-7",
          fencing_token: 11,
          attempts: 2,
          priority: 4,
          run_at_ms: 1_000,
          lease_expires_at_ms: 2_000,
          updated_at_ms: 1_500,
          root_flow_id: "root-1"
        },
        waiting_reason: "leased by worker-7",
        state_mode: :parallel,
        fifo_lane: nil,
        history: []
      }

      html = FlowDetail.render_flow_detail(data)

      assert html =~ ~s(class="flow-entity-header")
      assert html =~ ~s(class="flow-entity-identity")
      assert html =~ ~s(aria-label="Workflow detail sections")
      assert html =~ ~s(href="#workflow-timeline")
      assert html =~ ~s(href="#workflow-data")
      assert html =~ ~s(href="#workflow-relationships")
      assert html =~ ~s(href="#workflow-actions")
      assert html =~ ~s(id="workflow-summary")
      assert html =~ "worker-7"
      assert html =~ "tenant-globex"
      metadata = FlowDetail.render_flow_detail_metadata(data)
      assert metadata =~ "root-1"
      assert metadata =~ "Fencing token"
      assert metadata =~ ">11<"
      assert html =~ "charge_card"
      assert html =~ ~s(<h2 class="sr-only" id="workflow-summary-title">Execution summary</h2>)
      refute html =~ ~s(<td class="c-muted">Logical State</td>)
      refute html =~ ~s(<td class="c-muted">Worker</td>)
    end

    test "renders keyboard shortcuts modal" do
      html = FerricstoreServer.Health.Dashboard.Layout.render_keyboard_shortcuts_modal()

      assert html =~ ~s(id="keyboard-shortcuts-modal")
      assert html =~ "Keyboard Shortcuts"
      assert html =~ "<kbd>/</kbd>"
      assert html =~ "<kbd>G</kbd> <kbd>F</kbd>"
    end

    test "renders recent flow records with actions column and status chips" do
      records = [
        %{
          id: "flow-rec-1",
          type: "stripe_sync",
          state: "running",
          partition_key: "tenant-acme",
          worker: "worker-1",
          run_at_ms: 1_000,
          updated_at_ms: 2_000,
          attempts: 1
        }
      ]

      html =
        FerricstoreServer.Health.Dashboard.Render.FlowTables.Records.render_flow_recent_records(
          records
        )

      assert html =~ "<th>Actions</th>"
      assert html =~ ">Inspect<"
      refute html =~ "Canvas ↗"
      assert html =~ ~s(data-copy-text="flow-rec-1")
      assert html =~ "badge-running"
      refute html =~ "pulse-dot-green"
    end

    test "renders query discovery datalists for types and partitions" do
      discovery = %{
        available_types: ["order_fulfillment", "stripe_sync"],
        available_partitions: ["tenant-acme", "tenant-stripe"]
      }

      html =
        FerricstoreServer.Health.Dashboard.Render.FlowQueryControls.render_flow_query_discovery_datalists(
          discovery
        )

      assert html =~ ~s(id="flow-query-type-options")
      assert html =~ ~s(<option value="order_fulfillment"></option>)
      assert html =~ ~s(id="flow-query-partition-options")
      assert html =~ ~s(<option value="tenant-acme"></option>)
      assert html =~ ~s(<option value="tenant-stripe"></option>)
    end

    test "query operation labels describe operator tasks without changing command values" do
      html = FlowQueryControls.render_flow_query_kind_options("failures")

      assert html =~ ~s(<option value="list">List workflow runs</option>)
      assert html =~ ~s(<option value="search">Search indexed metadata</option>)
      assert html =~ ~s(<option value="stats">Count workflow runs</option>)
      assert html =~ ~s(<option value="failures" selected>Find failed workflows</option>)
      assert html =~ ~s(<option value="stuck">Find expired leases</option>)
      refute html =~ "FLOW.QUERY: list"
    end

    test "renders flow schedule create form" do
      html =
        FerricstoreServer.Health.Dashboard.Render.FlowSchedules.render_flow_schedule_create_form(
          %{}
        )

      assert html =~ "Create Durable Schedule"
      assert html =~ ~s(name="action" value="create")
      assert html =~ ~s(name="schedule_kind")
      assert html =~ ~s(name="cron")
      assert html =~ ~s(name="every_ms")
      assert html =~ ~s(name="target_type")
      assert html =~ ~s(name="overlap_policy")
      assert html =~ ~s(<option value="allow")
      assert html =~ ~s(<option value="skip" selected>)
      assert html =~ ~s(<option value="queue_after_previous")
      assert html =~ ~s(<option value="fail_schedule")
      refute html =~ ~s(value="cancel_previous")
      refute html =~ ~s(value="replace")
      assert html =~ "➕ Create Schedule"
    end

    test "Schedules.apply_form creates cron schedule" do
      id = "test-sched-#{System.unique_integer([:positive])}"

      params = %{
        "action" => "create",
        "id" => id,
        "schedule_kind" => "cron",
        "cron" => "0 9 * * *",
        "target_type" => "daily_summary",
        "target_partition" => "tenant-alpha",
        "overlap_policy" => "skip",
        "timezone" => "Etc/UTC",
        "overwrite" => "true"
      }

      assert {:ok, message} = FerricstoreServer.Health.Dashboard.Flow.Schedules.apply_form(params)
      assert message =~ "created schedule #{id}"

      assert FerricstoreServer.Health.Dashboard.Flow.Schedules.form_command(params) ==
               "FLOW.SCHEDULE.CREATE"

      # verify schedule exists
      assert {:ok, schedule} = FerricStore.flow_schedule_get(id)
      assert schedule.id == id
      assert schedule.target.type == "daily_summary"
      assert schedule.cron == "0 9 * * *"
    end

    test "Schedules.apply_form creates interval schedule" do
      id = "test-sched-interval-#{System.unique_integer([:positive])}"

      params = %{
        "action" => "create",
        "id" => id,
        "schedule_kind" => "interval",
        "every_ms" => "45000",
        "target_type" => "heartbeat",
        "overlap_policy" => "queue_after_previous"
      }

      assert {:ok, message} = FerricstoreServer.Health.Dashboard.Flow.Schedules.apply_form(params)
      assert message =~ "created schedule #{id}"

      assert {:ok, schedule} = FerricStore.flow_schedule_get(id)
      assert schedule.every_ms == 45_000
      assert schedule.overlap_policy == :queue_after_previous
    end

    test "Schedules.apply_form rejects unsupported overlap policies without creating a schedule" do
      id = "test-sched-overlap-invalid-#{System.unique_integer([:positive])}"

      assert {:error, message} =
               FerricstoreServer.Health.Dashboard.Flow.Schedules.apply_form(%{
                 "action" => "create",
                 "id" => id,
                 "schedule_kind" => "interval",
                 "every_ms" => "45000",
                 "target_type" => "heartbeat",
                 "overlap_policy" => "cancel_previous"
               })

      assert message =~ "overlap policy must be one of"
      assert {:ok, nil} = FerricStore.flow_schedule_get(id)
    end

    test "Schedules.apply_form rejects malformed max fires without creating a schedule" do
      id = "test-sched-max-fires-invalid-#{System.unique_integer([:positive])}"

      assert {:error, "max fires must be a positive integer"} =
               FerricstoreServer.Health.Dashboard.Flow.Schedules.apply_form(%{
                 "action" => "create",
                 "id" => id,
                 "schedule_kind" => "interval",
                 "every_ms" => "45000",
                 "target_type" => "heartbeat",
                 "overlap_policy" => "skip",
                 "max_fires" => "ten"
               })

      assert {:ok, nil} = FerricStore.flow_schedule_get(id)
    end

    test "Schedules.apply_form validates required fields" do
      assert {:error, "schedule id is required"} =
               FerricstoreServer.Health.Dashboard.Flow.Schedules.apply_form(%{
                 "action" => "create",
                 "id" => ""
               })

      assert {:error, "target workflow type is required"} =
               FerricstoreServer.Health.Dashboard.Flow.Schedules.apply_form(%{
                 "action" => "create",
                 "id" => "sched-1",
                 "target_type" => ""
               })

      assert {:error, "cron expression is required"} =
               FerricstoreServer.Health.Dashboard.Flow.Schedules.apply_form(%{
                 "action" => "create",
                 "id" => "sched-1",
                 "target_type" => "my_type",
                 "schedule_kind" => "cron",
                 "cron" => ""
               })
    end
  end

  describe "Styles and CSS Validation" do
    test "stylesheet includes master-detail canvas, journal, and live pulse rules" do
      css = Styles.stylesheet()

      assert css =~ ".flow-canvas"
      assert css =~ ".flow-diagnostic-hero"
      assert css =~ ".flow-journal-tree"
      assert css =~ ".journal-step"
      assert css =~ ".journal-step:target"
      assert css =~ ".journal-step-node"
      assert css =~ ~s(.flow-step-waterfall-axis-label[data-axis-edge="start"])
      assert css =~ ~s(.flow-step-waterfall-axis-label[data-axis-edge="end"])
      assert css =~ ".flow-facets"
      assert css =~ ".view-toggle"
      assert css =~ ".flow-breadcrumb"
      assert css =~ ".copy-btn-inline"
      assert css =~ ".pulse-dot-green"
      assert css =~ ".pulse-dot-amber"
      assert css =~ ".keyboard-modal"
      assert css =~ ".dashboard-disclosure"
      assert css =~ ".operator-attention"
      assert css =~ ".nav-subgroup"
      assert css =~ ".flow-investigation-context"
      assert css =~ ".flow-detail-sections"
      assert css =~ ".flow-query-workspace"
      assert css =~ ".flow-filter-limit { flex: none; width: 78px; max-width: 78px; }"
      refute css =~ ".flow-filter-limit { flex: 0 0 78px"
      assert css =~ "position: sticky"
    end
  end
end
