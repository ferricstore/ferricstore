defmodule FerricstoreServer.Health.DashboardTest.Sections.OperationalPages do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

      describe "non-Flow operational sub-pages" do
        test "slowlog page renders at-a-glance latency summary" do
          html =
            Dashboard.render_slowlog_page(%{
              slowlog: [
                %{
                  id: 2,
                  timestamp_us: 1_700_000_000_100_000,
                  duration_us: 12_000,
                  command: ["SET", "a", "b"]
                },
                %{
                  id: 1,
                  timestamp_us: 1_700_000_000_000_000,
                  duration_us: 4_000,
                  command: ["GET", "a"]
                }
              ]
            })

          assert String.contains?(html, "Slow Log Summary")
          assert String.contains?(html, "Worst")
          assert String.contains?(html, "Avg")
          assert String.contains?(html, "Total Time")
          assert String.contains?(html, "12.0 ms")
        end

        test "merge page renders compact merge pressure summary" do
          html =
            Dashboard.render_merge_page(%{
              merge: [
                %{
                  shard_index: 0,
                  mode: :idle,
                  merging: false,
                  last_merge_at: nil,
                  merge_count: 2,
                  total_bytes_reclaimed: 1_024
                },
                %{
                  shard_index: 1,
                  mode: :compact,
                  merging: true,
                  last_merge_at: 1_700_000_000_000,
                  merge_count: 3,
                  total_bytes_reclaimed: 2_048
                }
              ]
            })

          assert String.contains?(html, "Merge Summary")
          assert String.contains?(html, "Active Shards")
          assert String.contains?(html, "Total Reclaimed")
          assert String.contains?(html, "Total Merges")
          assert String.contains?(html, "1 / 2")
        end

        test "consensus page renders leader and apply-lag summary" do
          html =
            Dashboard.render_raft_page(%{
              cluster: %{
                node_name: :nonode@nohost,
                cluster_mode: :standalone,
                cluster_size: 1,
                nodes: [:nonode@nohost]
              },
              raft_shards: [
                %{
                  shard: 0,
                  status: :ok,
                  leader: {:raft_server_0, :nonode@nohost},
                  current_term: 4,
                  commit_index: 1_100,
                  last_applied: 1_000,
                  log_size: 64,
                  members: []
                },
                %{
                  shard: 1,
                  status: :down,
                  leader: nil,
                  current_term: 0,
                  commit_index: 0,
                  last_applied: 0,
                  log_size: 0,
                  members: []
                }
              ]
            })

          assert String.contains?(html, "Consensus Summary")
          assert String.contains?(html, "Healthy Shards")
          assert String.contains?(html, "Max Apply Lag")
          assert String.contains?(html, "single-member WARaft")
        end

        test "clients page summarizes flags and oldest client age" do
          html =
            Dashboard.render_clients_page(%{
              connections: %{active: 3, blocked: 1, tracking: 1},
              clients: [
                %{pid: self(), peer: "127.0.0.1:6379", age_seconds: 65, flags: "ST"},
                %{pid: self(), peer: "127.0.0.1:6380", age_seconds: 2, flags: "M"}
              ]
            })

          assert String.contains?(html, "Oldest")
          assert String.contains?(html, "Pub/Sub")
          assert String.contains?(html, "Transactions")
          assert String.contains?(html, "Tracking")
        end

        test "overview renders idle and unlimited memory as neutral states" do
          data =
            Dashboard.collect()
            |> put_in([:hotcold, :total_lookups], 0)
            |> put_in([:hotcold, :total_hits], 0)
            |> put_in([:hotcold, :total_misses], 0)
            |> put_in([:hotcold, :hit_ratio], 0.0)
            |> put_in([:memory, :max_bytes], 0)
            |> put_in([:memory, :ratio], 0.0)

          html = Dashboard.render(data)

          assert String.contains?(html, "No read samples")
          assert String.contains?(html, "unlimited")
          refute String.contains?(html, "/ 0 B")
        end

        test "consensus page uses WARaft-ready labels and omits unavailable zero log metrics" do
          html =
            Dashboard.render_raft_page(%{
              cluster: %{
                node_name: :nonode@nohost,
                cluster_mode: :standalone,
                cluster_size: 1,
                nodes: [:nonode@nohost]
              },
              raft_shards: [
                %{
                  shard: 16,
                  status: :ok,
                  leader: {:raft_server_ferricstore_waraft_backend_16, :nonode@nohost},
                  current_term: 4,
                  commit_index: 16_000,
                  last_applied: 15_900,
                  log_size: 0,
                  members: [{:raft_server_ferricstore_waraft_backend_16, :nonode@nohost}]
                }
              ]
            })

          assert String.contains?(html, "Consensus")
          assert String.contains?(html, "WARaft")
          assert String.contains?(html, "shard-16")
          refute String.contains?(html, "Raft Consensus - FerricStore")
          refute String.contains?(html, ">Raft Consensus<")
          refute String.contains?(html, "Per-Shard Raft State")
          refute String.contains?(html, "Total Log Entries")
          refute String.contains?(html, "<td>0</td>")
        end

        test "storage page shows total files and largest shard summary" do
          html =
            Dashboard.render_storage_page(%{
              total_disk_bytes: 10_240,
              total_files: 6,
              shards: [
                %{index: 0, disk_bytes: 2_048, data_file_count: 1, hint_file_count: 0},
                %{index: 1, disk_bytes: 8_192, data_file_count: 3, hint_file_count: 2}
              ]
            })

          assert String.contains?(html, "Largest Shard")
          assert String.contains?(html, "Shard 1")
          assert String.contains?(html, "Data Files")
          assert String.contains?(html, "Hint Files")
        end

        test "keyspace page renders bounded key metadata and inspector purpose" do
          html =
            Dashboard.render_keyspace_page(%{
              filters: %{key: "user:1", prefix: "", include_internal: false, limit: 50},
              rows: [
                %{
                  key: "user:1",
                  physical_key: "user:1",
                  shard: 0,
                  type: "string",
                  ttl: "none",
                  size: "5 B",
                  location: "hot",
                  internal?: false
                }
              ],
              inspected: %{
                key: "user:1",
                found?: true,
                type: "string",
                ttl: "none",
                size: "5 B",
                location: "hot",
                shard: 0
              },
              total_sampled: 1
            })

          assert String.contains?(html, "Keyspace")
          assert String.contains?(html, "Find keys, inspect metadata")
          assert String.contains?(html, "user:1")
          assert String.contains?(html, "Requires +SCAN")
          assert String.contains?(html, "Requires +GET")
        end

        test "commands page explains KV command health from counters and slowlog" do
          html =
            Dashboard.render_commands_page(%{
              summary: %{
                total_commands: 120,
                ops_per_sec: 12.5,
                slowlog_entries: 2,
                slowest_us: 15_000
              },
              slow_by_command: [
                %{command: "SET", count: 1, worst_us: 15_000, avg_us: 15_000},
                %{command: "GET", count: 1, worst_us: 4_000, avg_us: 4_000}
              ],
              command_groups: []
            })

          assert String.contains?(html, "Commands")
          assert String.contains?(html, "Understand command traffic")
          assert String.contains?(html, "Slow Log By Command")
          assert String.contains?(html, "15.0 ms")
        end

        test "read path page explains hot/cold and prefix read pressure" do
          html =
            Dashboard.render_reads_page(%{
              hotcold: %{
                hit_ratio: 98.5,
                total_hits: 985,
                total_misses: 15,
                hot_reads: 900,
                cold_reads: 85,
                hot_read_pct: 91.4,
                cold_reads_per_sec: 3.2,
                sample_rate: 100,
                total_lookups: 1_000,
                top_prefixes: []
              },
              prefixes: [
                %{prefix: "tenant", hot_reads: 900, cold_reads: 85, cold_pct: 8.6}
              ]
            })

          assert String.contains?(html, "Read Path")
          assert String.contains?(html, "Track hot-cache efficiency")
          assert String.contains?(html, "Prefix Read Pressure")
          assert String.contains?(html, "tenant")
          assert String.contains?(html, ~s(<span class="sampled-tag"))
          refute String.contains?(html, "&lt;span")
        end

        test "prefixes page summarizes sample size and read totals" do
          html =
            Dashboard.render_prefixes_page(%{
              total_sampled: 100,
              prefixes: [
                %{prefix: "tenant-a", keys: 70, pct: 70.0, hot_reads: 12, cold_reads: 3},
                %{prefix: "tenant-b", keys: 30, pct: 30.0, hot_reads: 2, cold_reads: 5}
              ]
            })

          assert String.contains?(html, "Prefix Summary")
          assert String.contains?(html, "Sampled Keys")
          assert String.contains?(html, "Indexed Keys")
          assert String.contains?(html, "Hot Reads")
          assert String.contains?(html, "Cold Reads")
        end

        test "non-Flow sub-pages include purpose text" do
          pages = [
            Dashboard.render_slowlog_page(%{slowlog: []}),
            Dashboard.render_merge_page(%{merge: []}),
            Dashboard.render_config_page(%{
              namespace_config: [],
              config_commands: [],
              config_parameters: []
            }),
            Dashboard.render_raft_page(%{
              cluster: %{
                node_name: :nonode@nohost,
                cluster_mode: :standalone,
                cluster_size: 1,
                nodes: []
              },
              raft_shards: []
            }),
            Dashboard.render_clients_page(%{
              connections: %{active: 0, blocked: 0, tracking: 0},
              clients: []
            }),
            Dashboard.render_storage_page(%{total_disk_bytes: 0, total_files: 0, shards: []}),
            Dashboard.render_doctor_page(%{
              check: %{"status" => "ok", "checks" => []},
              jobs: [],
              flash: %{},
              command_reference: []
            }),
            Dashboard.render_prefixes_page(%{total_sampled: 0, prefixes: []})
          ]

          for html <- pages do
            assert String.contains?(html, ~s(class="page-intro"))
          end
        end
      end

      describe "GET /dashboard namespace config sidebar link" do
        test "HTTP response body contains Config sidebar link" do
          port = HealthEndpoint.port()
          response = http_get(port, "/dashboard")
          body = extract_body(response)

          assert String.contains?(body, "Config")
          assert String.contains?(body, "/dashboard/config")
        end

        test "HTTP response sidebar contains config link regardless of overrides" do
          NamespaceConfig.reset_all()
          port = HealthEndpoint.port()
          response = http_get(port, "/dashboard")
          body = extract_body(response)

          assert String.contains?(body, "/dashboard/config")
        end
      end

      describe "dashboard UX hardening" do
        test "/dashboard/consensus redirects to the consensus page" do
          response = http_get(HealthEndpoint.port(), "/dashboard/consensus")

          assert extract_status_code(response) == 302
          assert extract_header(response, "location") == "/dashboard/raft"
        end

        test "/dashboard/raft hides internal collection errors" do
          previous = Application.get_env(:ferricstore, :dashboard_raft_page_fun)

          on_exit(fn ->
            restore_env(:dashboard_raft_page_fun, previous)
          end)

          Application.put_env(:ferricstore, :dashboard_raft_page_fun, fn ->
            raise RuntimeError, "secret raft page failure"
          end)

          response = http_get(HealthEndpoint.port(), "/dashboard/raft")
          body = extract_body(response)

          assert extract_status_code(response) == 200
          assert body =~ "Internal dashboard error"
          refute body =~ "secret raft page failure"
          refute body =~ "RuntimeError"
        end

        test "subpage shell exposes semantic heading and active navigation state" do
          html = Dashboard.collect_flow_page() |> Dashboard.render_flow_page()

          assert String.contains?(html, ~s(<h1 class="subpage-title">FerricFlow</h1>))
          assert String.contains?(html, ~s(<nav class="sidebar" aria-label="Dashboard sections">))
          assert String.contains?(html, ~s(aria-current="page"))
        end

        test "clients page uses explicit connection registry metadata" do
          client_id = System.unique_integer([:positive])
          parent = self()

          pid =
            spawn(fn ->
              send(parent, {:client_fixture_ready, self()})

              receive do
                :stop -> :ok
              end
            end)

          assert_receive {:client_fixture_ready, ^pid}

          FerricstoreServer.Connection.Registry.register(client_id, pid, %{
            client_name: "dashboard-client",
            username: "alice",
            peer: "127.0.0.1:6380",
            created_at_ms: System.monotonic_time(:millisecond) - 65_000,
            flags: "ST"
          })

          on_exit(fn ->
            FerricstoreServer.Connection.Registry.unregister(client_id, pid)
            send(pid, :stop)
          end)

          html = Dashboard.collect_clients_page() |> Dashboard.render_clients_page()

          assert String.contains?(html, Integer.to_string(client_id))
          assert String.contains?(html, "dashboard-client")
          assert String.contains?(html, "alice")
          assert String.contains?(html, "127.0.0.1:6380")
          refute String.contains?(html, "unknown:0")
        end

        test "live Flow payloads update charts and tables together" do
          assert {:ok, states_payload} = Dashboard.live_payload("flow/states")
          assert Map.has_key?(states_payload.components, "flow_states_chart")
          assert Map.has_key?(states_payload.components, "flow_states_table")

          assert {:ok, workers_payload} = Dashboard.live_payload("flow/workers")
          assert Map.has_key?(workers_payload.components, "flow_workers_chart")
          assert Map.has_key?(workers_payload.components, "flow_workers")

          assert {:ok, due_payload} = Dashboard.live_payload("flow/due")
          assert Map.has_key?(due_payload.components, "flow_due_chart")
          assert Map.has_key?(due_payload.components, "flow_due_now")
        end

        test "signals live refresh preserves scan history mode" do
          html =
            Dashboard.render_flow_signals_page(%{
              signals: [],
              filters: %{type: "email", signal: nil, q: nil, limit: 40, scan_history: true},
              available_types: ["email"],
              total_sampled: 0,
              filtered_sampled: 0,
              sample_limit: 400
            })

          assert String.contains?(html, ~s(data-dashboard-live-url="/dashboard/api/flow/signals?))
          assert String.contains?(html, "scan=true")
        end

        test "retention cleanup requires explicit confirmation" do
          assert {:error, reason} =
                   Dashboard.apply_flow_retention_form(%{"action" => "cleanup", "limit" => "1"})

          assert reason =~ "confirm"

          html =
            Dashboard.collect_flow_retention_page(limit: 1)
            |> Dashboard.render_flow_retention_page()

          assert String.contains?(html, ~s(name="confirm_cleanup"))
          assert String.contains?(html, "Requires +FLOW.RETENTION_CLEANUP")
          assert String.contains?(html, "Sample Preview")
        end

        test "failure recovery requires explicit reclaim confirmation" do
          assert {:error, reason} =
                   Dashboard.apply_flow_failures_form(%{
                     "action" => "reclaim",
                     "type" => "email",
                     "limit" => "1",
                     "lease_ms" => "30000"
                   })

          assert reason =~ "confirm"

          html =
            Dashboard.collect_flow_failures_page(type: "email")
            |> Dashboard.render_flow_failures_page()

          assert String.contains?(html, ~s(name="confirm_reclaim"))
          assert String.contains?(html, "Requires +FLOW.RECLAIM")
        end

        test "detail rewind requires explicit confirmation" do
          assert {:error, reason} =
                   Dashboard.apply_flow_rewind_form(%{"id" => "flow-1", "to_event" => "event-1"})

          assert reason =~ "confirm"
        end
      end

      # ---------------------------------------------------------------------------
      # FerricFlow dashboard pages
      # ---------------------------------------------------------------------------

      describe "collect_flow_page/0" do
        test "discovers Flow types from durable state records" do
          id = "dashboard-flow-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-order",
                     state: "queued",
                     run_at_ms: 1_000
                   )

          data = Dashboard.collect_flow_page()

          assert data.total_sampled >= 1
          assert Enum.any?(data.types, fn type -> type.type == "dashboard-order" end)

          type = Enum.find(data.types, fn type -> type.type == "dashboard-order" end)
          assert type.active >= 1
          assert type.queued >= 1
        end

        test "uses sampled counts when global Flow info misses partitioned records" do
          flow_type = "dashboard-partitioned-counts-#{System.unique_integer([:positive])}"
          partition_key = "tenant-counts-#{System.unique_integer([:positive])}"
          queued_id = "dashboard-queued-#{System.unique_integer([:positive])}"
          running_id = "dashboard-running-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(queued_id,
                     type: flow_type,
                     state: "queued",
                     partition_key: partition_key,
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )

          assert :ok =
                   FerricStore.flow_create(running_id,
                     type: flow_type,
                     state: "queued",
                     partition_key: partition_key,
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )

          assert {:ok, [%{lease_owner: "dashboard-worker"}]} =
                   FerricStore.flow_claim_due(flow_type,
                     partition_key: partition_key,
                     worker: "dashboard-worker",
                     limit: 1,
                     now_ms: 1_000,
                     payload: false
                   )

          data = Dashboard.collect_flow_page()
          type = Enum.find(data.types, &(&1.type == flow_type))

          assert type
          assert type.exact == false
          assert type.total >= 2
          assert type.active >= 2
          assert type.queued >= 1
          assert type.running >= 1
          assert data.summary.active >= 2
        end
      end

      describe "render_flow_page/1" do
        test "renders overview, state breakdown, and explorer links" do
          data = Dashboard.collect_flow_page()
          html = Dashboard.render_flow_page(data)

          assert String.contains?(html, "FerricFlow")
          assert String.contains?(html, "Flow Overview")
          assert String.contains?(html, "Projection Health")
          assert String.contains?(html, "State Breakdown")
          assert String.contains?(html, "Recent Flow Records")
          assert String.contains?(html, ~s(href="/dashboard/flow/states"))
          assert String.contains?(html, ~s(href="/dashboard/flow/workers"))
          assert String.contains?(html, ~s(href="/dashboard/flow/due"))
          assert String.contains?(html, ~s(href="/dashboard/flow/failures"))
          assert String.contains?(html, ~s(href="/dashboard/flow/lineage"))
          assert String.contains?(html, ~s(href="/dashboard/flow/query"))
          assert String.contains?(html, ~s(href="/dashboard/flow/policies"))
          assert String.contains?(html, ~s(href="/dashboard/flow/retention"))
          refute String.contains?(html, ~s(href="/dashboard/flow/config"))
          refute String.contains?(html, ~s(href="/dashboard/flow/projections"))
        end

        test "renders lease_owner as the running worker" do
          html =
            Dashboard.render_flow_page(%{
              summary: %{},
              types: [],
              workers: [],
              records: [
                %{
                  id: "dashboard-running-worker",
                  type: "email",
                  state: "running",
                  partition_key: "tenant-a",
                  lease_owner: "worker-a",
                  run_at_ms: 1_000,
                  lease_expires_at_ms: System.system_time(:millisecond) + 30_000,
                  updated_at_ms: 1_000
                }
              ],
              total_sampled: 1,
              sample_limit: 400
            })

          assert String.contains?(html, "worker-a")
          assert String.contains?(html, "leased by worker-a")
          refute String.contains?(html, "running without worker metadata")
        end

        test "Flow overview value refs link to the detail inspector" do
          html =
            Dashboard.render_flow_page(%{
              summary: %{},
              types: [],
              workers: [],
              records: [
                %{
                  id: "dashboard-value-link",
                  type: "email",
                  state: "queued",
                  partition_key: "tenant-a",
                  payload_ref: "flow-value:payload",
                  run_at_ms: 1_000,
                  updated_at_ms: 1_000
                }
              ],
              total_sampled: 1,
              sample_limit: 400
            })

          assert String.contains?(
                   html,
                   ~s(href="/dashboard/flow/dashboard-value-link?partition_key=tenant-a#flow-value-)
                 )

          assert String.contains?(html, ~s(title="Open payload value"))
          assert String.contains?(html, ~s(aria-label="Open payload value"))
        end

        test "Flow signals page renders signal history rows" do
          html =
            Dashboard.render_flow_signals_page(%{
              signals: [
                %{
                  id: "dashboard-signal-row",
                  partition_key: "tenant-a",
                  type: "email",
                  event_id: "1100-2",
                  time_ms: 1_100,
                  signal: "payment_received",
                  from_state: "waiting_payment",
                  to_state: "verify_payment",
                  fields: %{
                    "value_refs" => ~s({"payment_event":{"ref":"ref:payment","version":1}})
                  },
                  record: %{
                    id: "dashboard-signal-row",
                    partition_key: "tenant-a",
                    type: "email"
                  }
                }
              ],
              filters: %{type: nil, signal: nil, q: nil, limit: 40},
              available_types: ["email"],
              total_sampled: 1,
              filtered_sampled: 1,
              sample_limit: 400
            })

          assert String.contains?(html, "Flow Signals")
          assert String.contains?(html, "payment_received")
          assert String.contains?(html, "waiting_payment -> verify_payment")
          assert String.contains?(html, "payment_event")
          assert String.contains?(html, ~s(title="Filter by signal name substring"))
          assert String.contains?(html, ~s(title="Apply Flow signal filters"))

          assert String.contains?(
                   html,
                   ~s(href="/dashboard/flow/dashboard-signal-row?partition_key=tenant-a)
                 )
        end

        test "does not depend on external chart scripts" do
          pages = [
            Dashboard.collect_flow_page() |> Dashboard.render_flow_page(),
            Dashboard.collect_flow_states_page() |> Dashboard.render_flow_states_page(),
            Dashboard.collect_flow_workers_page() |> Dashboard.render_flow_workers_page(),
            Dashboard.collect_flow_due_page() |> Dashboard.render_flow_due_page(),
            "dashboard-flow-chart-#{System.unique_integer([:positive])}"
            |> Dashboard.collect_flow_detail_page()
            |> Dashboard.render_flow_detail_page()
          ]

          for html <- pages do
            refute String.contains?(html, "cdn.jsdelivr")
            refute String.contains?(html, "https://")
            refute String.contains?(html, "new Chart")
            refute String.contains?(html, "<canvas")
          end
        end
      end
    end
  end
end
