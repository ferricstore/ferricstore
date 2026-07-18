defmodule FerricstoreServer.Health.DashboardTest.Sections.FlowDetailPoliciesRetention do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

      describe "collect_flow_detail_page/1 live history and policies" do
        test "Flow detail page is live-watchable without full-page refresh" do
          id = "dashboard-flow-watch/#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-watch",
                     state: "queued",
                     run_at_ms: 1_000
                   )

          encoded_id = URI.encode(id, &URI.char_unreserved?/1)
          html = id |> Dashboard.collect_flow_detail_page() |> Dashboard.render_flow_detail_page()

          assert String.contains?(html, ~s(data-dashboard-live-page="flow-detail"))

          assert String.contains?(
                   html,
                   ~s(data-dashboard-live-url="/dashboard/api/flow/#{encoded_id}")
                 )

          assert String.contains?(html, ~s(data-live-component="flow_detail"))
          assert String.contains?(html, ~s(data-live-component="flow_debug"))
          assert String.contains?(html, ~s(data-live-component="flow_history"))
          refute String.contains?(html, ~s(http-equiv="refresh"))
        end

        test "Flow detail uses partition key and consistent HISTORY projection" do
          id = "dashboard-flow-detail-partitioned/#{System.unique_integer([:positive])}"
          partition_key = "tenant-detail-#{System.unique_integer([:positive])}"
          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, opts ->
            send(test_pid, {:dashboard_get_opts, opts})

            {:ok,
             %{
               id: id,
               type: "dashboard-partitioned",
               state: "queued",
               partition_key: partition_key,
               run_at_ms: 1_000,
               updated_at_ms: 1_000
             }}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, opts ->
            send(test_pid, {:dashboard_history_opts, opts})
            {:ok, [{"1000-1", %{"event" => "created", "to_state" => "queued"}}]}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
            restore_env(:flow_dashboard_flow_history_fun, previous_history)
          end)

          data = Dashboard.collect_flow_detail_page(id, partition_key: partition_key)
          html = Dashboard.render_flow_detail_page(data)

          assert data.record.id == id
          assert data.partition_key == partition_key
          assert String.contains?(html, partition_key)

          assert_receive {:dashboard_get_opts, get_opts}
          assert get_opts[:payload] == false
          assert get_opts[:partition_key] == partition_key

          assert_receive {:dashboard_history_opts, history_opts}
          assert history_opts[:partition_key] == partition_key
          assert history_opts[:consistent_projection] == true
        end

        test "Flow detail paginates older history with an inclusive event cursor" do
          id = "dashboard-flow-history-before/#{System.unique_integer([:positive])}"
          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, _opts ->
            {:ok,
             %{
               id: id,
               type: "dashboard-history-page",
               state: "s3",
               partition_key: "tenant-history-page",
               run_at_ms: 1_000,
               updated_at_ms: 4_000
             }}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, opts ->
            send(test_pid, {:dashboard_history_opts, opts})

            {:ok,
             [
               {"3000-3", %{"event" => "transitioned", "state" => "s3", "version" => "3"}},
               {"2000-2", %{"event" => "transitioned", "state" => "s2", "version" => "2"}},
               {"1000-1", %{"event" => "created", "state" => "s1", "version" => "1"}}
             ]}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
            restore_env(:flow_dashboard_flow_history_fun, previous_history)
          end)

          data =
            Dashboard.collect_flow_detail_page(id,
              history_before: "3000-3",
              history_count: 1
            )

          assert_receive {:dashboard_history_opts, history_opts}
          assert history_opts[:to_event] == "3000-3"
          assert history_opts[:rev] == true
          assert history_opts[:count] == 3

          assert Enum.map(data.history, &elem(&1, 0)) == ["2000-2"]
          assert data.history_page.has_older == true
          assert data.history_page.has_newer == true
          assert data.history_page.oldest_event_id == "2000-2"
          assert data.history_page.newest_event_id == "2000-2"
        end

        test "Flow detail renders cursor pagination controls for history" do
          html =
            Dashboard.render_flow_detail_page(%{
              id: "dashboard-flow-history-links",
              partition_key: "tenant history links",
              record: %{
                id: "dashboard-flow-history-links",
                type: "debug",
                state: "s2",
                partition_key: "tenant history links",
                run_at_ms: 1_000,
                updated_at_ms: 2_000
              },
              record_status: :ok,
              history: [
                {"1000-1", %{"event" => "created", "state" => "s1", "version" => "1"}},
                {"2000-2", %{"event" => "transitioned", "state" => "s2", "version" => "2"}}
              ],
              history_status: :ok,
              history_page: %{
                id: "dashboard-flow-history-links",
                partition_key: "tenant history links",
                count: 2,
                has_older: true,
                has_newer: true,
                oldest_event_id: "1000-1",
                newest_event_id: "2000-2",
                before: nil,
                after: nil,
                older_url:
                  "/dashboard/flow/dashboard-flow-history-links?history_before=1000-1&history_count=2&partition_key=tenant+history+links",
                newer_url:
                  "/dashboard/flow/dashboard-flow-history-links?history_after=2000-2&history_count=2&partition_key=tenant+history+links"
              },
              value_refs: [],
              values_by_ref: %{},
              values_status: :skipped,
              waiting_reason: "ready"
            })

          assert String.contains?(html, "History page")
          assert String.contains?(html, "Older")
          assert String.contains?(html, "Newer")
          assert String.contains?(html, "history_before=1000-1")
          assert String.contains?(html, "history_after=2000-2")
          assert String.contains?(html, "history_count=2")
        end

        test "Flow detail renders FIFO state mode and lane blocker" do
          html =
            Dashboard.render_flow_detail_page(%{
              id: "dashboard-flow-fifo-detail",
              partition_key: "tenant-fifo-detail",
              record: %{
                id: "dashboard-flow-fifo-detail",
                type: "dashboard-fifo-detail",
                state: "running",
                run_state: "queued",
                partition_key: "tenant-fifo-detail",
                worker: "detail-worker",
                run_at_ms: 1_000,
                lease_expires_at_ms: 60_000,
                updated_at_ms: 2_000
              },
              record_status: :ok,
              history: [],
              history_status: :ok,
              history_page: %{
                id: "dashboard-flow-fifo-detail",
                partition_key: "tenant-fifo-detail",
                count: 0,
                has_older: false,
                has_newer: false,
                oldest_event_id: nil,
                newest_event_id: nil,
                before: nil,
                after: nil,
                older_url: nil,
                newer_url: nil
              },
              value_refs: [],
              values_by_ref: %{},
              values_status: :skipped,
              waiting_reason: "running until 1970-01-01 00:01:00.000Z",
              state_mode: :fifo,
              fifo_lane: %{
                type: "dashboard-fifo-detail",
                state: "queued",
                partition_key: "tenant-fifo-detail",
                count: 2,
                running: 1,
                waiting: 1,
                due: 1,
                scheduled: 0,
                head_id: "dashboard-flow-fifo-detail",
                waiting_head_id: "dashboard-flow-fifo-next",
                head_status: "blocked by active flow",
                blocked_by_id: "dashboard-flow-fifo-detail",
                blocked_by_worker: "detail-worker",
                lease_expires_at_ms: 60_000
              }
            })

          assert String.contains?(html, "State mode")
          assert String.contains?(html, "FIFO")
          assert String.contains?(html, "Logical State")
          assert String.contains?(html, "blocked by active flow")
          assert String.contains?(html, "dashboard-flow-fifo-next")
          assert String.contains?(html, "detail-worker")
        end

        test "Flow detail lookup is bounded when durable lookup stalls" do
          previous_timeout =
            Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)

          Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn _id ->
            Process.sleep(1_000)
            {:ok, %{id: "too-slow"}}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_timeout)
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
          end)

          {elapsed_us, data} =
            :timer.tc(fn -> Dashboard.collect_flow_detail_page("slow-flow") end)

          html = Dashboard.render_flow_detail_page(data)

          assert elapsed_us < 300_000
          assert data.record == nil
          assert data.record_status == :timeout
          assert String.contains?(html, "lookup timed out")
        end

        test "Flow detail history is bounded when history lookup stalls" do
          previous_timeout =
            Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)

          Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn id ->
            {:ok,
             %{
               id: id,
               type: "debug",
               state: "queued",
               partition_key: "tenant-1",
               run_at_ms: 1_000,
               updated_at_ms: 1_000
             }}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _id, _opts ->
            Process.sleep(1_000)
            {:ok, [{1, %{event: "create", to_state: "queued"}}]}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_timeout)
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
            restore_env(:flow_dashboard_flow_history_fun, previous_history)
          end)

          {elapsed_us, data} =
            :timer.tc(fn -> Dashboard.collect_flow_detail_page("slow-history-flow") end)

          html = Dashboard.render_flow_detail_page(data)

          assert elapsed_us < 300_000
          assert data.record.id == "slow-history-flow"
          assert data.history == []
          assert data.history_status == :timeout
          assert String.contains?(html, "History temporarily unavailable")
        end

        test "Flow detail emits telemetry for bounded dashboard lookups" do
          previous_timeout =
            Application.get_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms)

          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
          test_pid = self()

          :telemetry.attach(
            {__MODULE__, :dashboard_flow_lookup, test_pid},
            [:ferricstore, :dashboard, :flow, :lookup],
            &__MODULE__.handle_dashboard_flow_lookup_event/4,
            test_pid
          )

          Application.put_env(:ferricstore, :flow_dashboard_detail_fetch_timeout_ms, 10)

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn id ->
            {:ok,
             %{
               id: id,
               type: "debug",
               state: "queued",
               partition_key: "tenant-1",
               run_at_ms: 1_000,
               updated_at_ms: 1_000
             }}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _id, _opts ->
            Process.sleep(1_000)
            {:ok, [{1, %{event: "create", to_state: "queued"}}]}
          end)

          on_exit(fn ->
            :telemetry.detach({__MODULE__, :dashboard_flow_lookup, test_pid})
            restore_env(:flow_dashboard_detail_fetch_timeout_ms, previous_timeout)
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
            restore_env(:flow_dashboard_flow_history_fun, previous_history)
          end)

          data = Dashboard.collect_flow_detail_page("profiled-history-flow")

          assert data.history_status == :timeout

          assert_receive {:dashboard_flow_lookup, [:ferricstore, :dashboard, :flow, :lookup],
                          %{duration_us: duration_us, timeout_ms: 10},
                          %{operation: :record, result: :ok}},
                         500

          assert is_integer(duration_us)
          assert duration_us >= 0

          assert_receive {:dashboard_flow_lookup, [:ferricstore, :dashboard, :flow, :lookup],
                          %{duration_us: history_duration_us, timeout_ms: 10},
                          %{operation: :history, result: :timeout}},
                         500

          assert is_integer(history_duration_us)
          assert history_duration_us >= 10_000
        end
      end

      # ---------------------------------------------------------------------------
      # Sub-page HTTP endpoints
      # ---------------------------------------------------------------------------
      describe "GET /dashboard/config sub-page" do
        test "returns 200 with config page HTML" do
          port = HealthEndpoint.port()
          response = http_get(port, "/dashboard/config")

          assert response =~ "HTTP/1.1 200 OK"
          assert response =~ "text/html"

          body = extract_body(response)
          assert String.contains?(body, "Namespace Config")
          assert String.contains?(body, "Configuration Commands")
          assert String.contains?(body, "Runtime Parameters")
        end

        test "shows built-in defaults when no overrides exist" do
          NamespaceConfig.reset_all()
          port = HealthEndpoint.port()
          response = http_get(port, "/dashboard/config")
          body = extract_body(response)

          assert String.contains?(body, "built-in default window")
        end
      end

      describe "collect_flow_policies_page/0 and render_flow_policies_page/1" do
        test "renders a create and update policy form" do
          html =
            Dashboard.collect_flow_policies_page()
            |> Dashboard.render_flow_policies_page()

          assert String.contains?(html, ~s(id="flow-policy-editor"))
          assert String.contains?(html, ~s(method="post"))
          assert String.contains?(html, ~s(action="/dashboard/flow/policies"))
          assert String.contains?(html, ~s(name="type"))
          assert String.contains?(html, ~s(name="mode"))
          assert String.contains?(html, ~s(name="indexed_attributes"))
          assert String.contains?(html, ~s(name="indexed_state_meta"))
          assert String.contains?(html, ~s(name="max_retries"))
          assert String.contains?(html, ~s(name="max_active_ms"))
          assert String.contains?(html, ~s(name="retention_ttl_ms"))
          refute String.contains?(html, ~s(name="history_hot_max_events"))
          refute String.contains?(html, "Hot history")
          assert String.contains?(html, "Create / Update Policy")
          assert String.contains?(html, "Policies affect new Flow work and retry scheduling")
          assert String.contains?(html, ~s(title="Save this Flow policy"))

          assert String.contains?(
                   html,
                   ~s(title="Terminal state used when retry attempts are exhausted")
                 )
        end

        test "shows configured policy-only Flow types and command reference" do
          type = "dashboard-policy-only-#{System.unique_integer([:positive])}"

          assert {:ok, _policy} =
                   FerricStore.flow_policy_set(type,
                     max_active_ms: 45_000,
                     retry: [
                       max_retries: 5,
                       backoff: [kind: :fixed, base_ms: 100, max_ms: 500, jitter_pct: 0],
                       exhausted_to: "dead"
                     ],
                     retention: [
                       ttl_ms: 60_000,
                       history_max_events: 50
                     ],
                     indexed_attributes: ["tenant", "priority_tier"],
                     indexed_state_meta: "ai.model",
                     states: [
                       {"review",
                        [
                          mode: :fifo,
                          retry: [max_retries: 1, exhausted_to: "review_failed"],
                          retention: [
                            ttl_ms: 30_000,
                            history_max_events: 10
                          ]
                        ]}
                     ]
                   )

          data = Dashboard.collect_flow_policies_page()

          assert Enum.any?(data.policies, &(&1.type == type))

          html = Dashboard.render_flow_policies_page(data)

          assert String.contains?(html, "FerricFlow Policies")
          assert String.contains?(html, "FLOW.POLICY.SET &lt;type&gt;")
          assert String.contains?(html, "FLOW.POLICY.GET &lt;type&gt;")
          assert String.contains?(html, "MAX_RETRIES")
          assert String.contains?(html, "RETENTION_TTL_MS")
          assert String.contains?(html, "MODE FIFO|PARALLEL")
          assert String.contains?(html, "INDEXED_ATTRIBUTES")
          assert String.contains?(html, "tenant")
          assert String.contains?(html, "priority_tier")
          assert String.contains?(html, "ai.model")
          assert String.contains?(html, type)
          assert String.contains?(html, "configured")
          assert String.contains?(html, "fixed 100ms")
          assert String.contains?(html, "dead")
          assert String.contains?(html, "review")
        end

        test "policy form updates global policy without dropping state overrides" do
          type = "dashboard-policy-form-#{System.unique_integer([:positive])}"

          assert {:ok, _policy} =
                   FerricStore.flow_policy_set(type,
                     max_active_ms: 45_000,
                     retry: [
                       max_retries: 2,
                       backoff: [kind: :fixed, base_ms: 100, max_ms: 200, jitter_pct: 0],
                       exhausted_to: "failed"
                     ],
                     retention: [
                       ttl_ms: 60_000,
                       history_max_events: 10
                     ],
                     indexed_attributes: ["tenant"],
                     indexed_state_meta: "risk_tier",
                     states: [
                       {"review",
                        [
                          retry: [max_retries: 7, exhausted_to: "review_failed"],
                          retention: [
                            ttl_ms: 30_000,
                            history_max_events: 5
                          ]
                        ]}
                     ]
                   )

          assert {:ok, ^type} =
                   Dashboard.apply_flow_policy_form(%{
                     "type" => type,
                     "max_retries" => "4",
                     "backoff_kind" => "linear",
                     "base_ms" => "250",
                     "max_ms" => "2000",
                     "jitter_pct" => "10",
                     "exhausted_to" => "dead",
                     "retention_ttl_ms" => "120000",
                     "history_max_events" => "20"
                   })

          assert {:ok, policy} = FerricStore.flow_policy_get(type)
          assert policy.retry.max_retries == 4
          assert policy.max_active_ms == 45_000
          assert policy.retry.backoff.kind == :linear
          assert policy.retry.exhausted_to == "dead"
          assert policy.retention.ttl_ms == 120_000
          refute Map.has_key?(policy.retention, :history_hot_max_events)
          assert policy.indexed_attributes == ["tenant"]
          assert policy.indexed_state_meta == "risk_tier"
          assert policy.states["review"].retry.max_retries == 7
          assert policy.states["review"].retry.exhausted_to == "review_failed"
          assert policy.states["review"].retention.history_max_events == 5
        end

        test "policy form updates the type-level max active runtime" do
          type = "dashboard-policy-active-limit-#{System.unique_integer([:positive])}"

          assert {:ok, ^type} =
                   Dashboard.apply_flow_policy_form(%{
                     "type" => type,
                     "max_active_ms" => "90000",
                     "max_retries" => "4",
                     "backoff_kind" => "linear",
                     "base_ms" => "250",
                     "max_ms" => "2000",
                     "jitter_pct" => "10",
                     "exhausted_to" => "dead",
                     "retention_ttl_ms" => "120000",
                     "history_max_events" => "20"
                   })

          assert {:ok, %{max_active_ms: 90_000}} = FerricStore.flow_policy_get(type)

          html =
            Dashboard.collect_flow_policies_page(edit_type: type)
            |> Dashboard.render_flow_policies_page()

          assert String.contains?(html, ~s(name="max_active_ms"))
          assert String.contains?(html, ~s(value="90000"))
          assert String.contains?(html, "Max Active")
        end

        test "policy form writes state-level FIFO mode without dropping existing overrides" do
          type = "dashboard-policy-fifo-#{System.unique_integer([:positive])}"

          assert {:ok, _policy} =
                   FerricStore.flow_policy_set(type,
                     retry: [
                       max_retries: 2,
                       backoff: [kind: :fixed, base_ms: 100, max_ms: 200, jitter_pct: 0],
                       exhausted_to: "failed"
                     ],
                     retention: [
                       ttl_ms: 60_000,
                       history_max_events: 10
                     ],
                     states: [
                       {"ready",
                        [
                          mode: :parallel,
                          retry: [max_retries: 8, exhausted_to: "ready_failed"],
                          retention: [
                            ttl_ms: 90_000,
                            history_max_events: 11
                          ]
                        ]}
                     ]
                   )

          assert {:ok, ^type} =
                   Dashboard.apply_flow_policy_form(%{
                     "type" => type,
                     "state" => "queued",
                     "mode" => "fifo",
                     "max_retries" => "4",
                     "backoff_kind" => "linear",
                     "base_ms" => "250",
                     "max_ms" => "2000",
                     "jitter_pct" => "10",
                     "exhausted_to" => "dead",
                     "retention_ttl_ms" => "120000",
                     "history_max_events" => "20"
                   })

          assert {:ok, policy} = FerricStore.flow_policy_get(type)
          assert policy.states["queued"].mode == :fifo
          assert policy.states["queued"].retry.max_retries == 4
          assert policy.states["ready"].mode == :parallel
          assert policy.states["ready"].retry.max_retries == 8

          html =
            Dashboard.collect_flow_policies_page(edit_type: type)
            |> Dashboard.render_flow_policies_page()

          assert String.contains?(html, "FIFO")

          assert String.contains?(
                   html,
                   "FIFO requires every entering Flow to carry a partition key"
                 )

          assert String.contains?(html, "queued")
          assert String.contains?(html, "ready")
        end
      end

      describe "collect_flow_retention_page/1 and render_flow_retention_page/1" do
        test "collects storage bytes from nested Flow/WARaft storage" do
          old_data_dir = Application.get_env(:ferricstore, :data_dir)
          old_shard_count = Application.get_env(:ferricstore, :shard_count)

          data_dir =
            Path.join(
              System.tmp_dir!(),
              "dashboard-storage-#{System.unique_integer([:positive])}"
            )

          shard_dir = Ferricstore.DataDir.shard_data_path(data_dir, 0)
          history_dir = Path.join(shard_dir, "history")
          waraft_dir = Path.join([data_dir, "waraft", "shard_0"])

          File.mkdir_p!(history_dir)
          File.mkdir_p!(waraft_dir)
          File.write!(Path.join(shard_dir, "00000.log"), String.duplicate("a", 7))
          File.write!(Path.join(history_dir, "history.segment"), String.duplicate("b", 11))
          File.write!(Path.join(waraft_dir, "raft.segment"), String.duplicate("c", 13))

          Application.put_env(:ferricstore, :data_dir, data_dir)
          Application.put_env(:ferricstore, :shard_count, 1)

          on_exit(fn ->
            File.rm_rf!(data_dir)
            restore_env(:data_dir, old_data_dir)
            restore_env(:shard_count, old_shard_count)
          end)

          storage = Dashboard.collect_storage_page()
          retention = Dashboard.collect_flow_retention_page()
          shard = Enum.find(storage.shards, &(&1.index == 0))

          assert storage.total_disk_bytes >= 31
          assert %{disk_bytes: shard_bytes, data_file_count: 1, hint_file_count: 0} = shard
          assert shard_bytes >= 18
          assert retention.storage.total_disk_bytes >= 31
        end

        test "renders retention controls, command reference, and sampled candidates" do
          now_ms = System.system_time(:millisecond)

          html =
            Dashboard.render_flow_retention_page(%{
              now_ms: now_ms,
              limit: 25,
              sample_limit: 400,
              total_sampled: 2,
              terminal_sampled: 1,
              active_sampled: 1,
              eligible_sampled: 2,
              terminal_eligible_sampled: 1,
              active_timeout_eligible_sampled: 1,
              storage: %{total_disk_bytes: 1_234_567},
              projection: Dashboard.default_flow_projection_health(),
              flash: nil,
              active_timeout_candidates: [
                %{
                  id: "active-timeout-flow-1",
                  type: "email",
                  state: "running",
                  partition_key: "tenant-a",
                  created_at_ms: now_ms - 10_000,
                  max_active_ms: 5_000,
                  updated_at_ms: now_ms - 2_000,
                  attempts: 1
                }
              ],
              candidates: [
                %{
                  id: "retention-flow-1",
                  type: "email",
                  state: "completed",
                  partition_key: "tenant-a",
                  terminal_retention_until_ms: now_ms - 1_000,
                  updated_at_ms: now_ms - 2_000,
                  attempts: 2
                }
              ]
            })

          assert String.contains?(html, "FerricFlow Retention")
          assert String.contains?(html, ~s(action="/dashboard/flow/retention"))
          assert String.contains?(html, "Dry Run")
          assert String.contains?(html, "Run Cleanup")
          assert String.contains?(html, "FLOW.RETENTION_CLEANUP")
          assert String.contains?(html, "retention-flow-1")
          assert String.contains?(html, "active-timeout-flow-1")
          assert String.contains?(html, "Active Timeouts")
          assert String.contains?(html, "fails overdue active Flow records")
          assert String.contains?(html, "terminal Flow records")
          refute String.contains?(html, "Active Flow records are not touched")
        end

        test "collects overdue active flows in the sampled retention preview" do
          now_ms = System.system_time(:millisecond)
          id = "dashboard-active-timeout-preview-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-active-timeout",
                     state: "queued",
                     now_ms: now_ms - 10_000,
                     max_active_ms: 1_000
                   )

          data = Dashboard.collect_flow_retention_page(limit: 25)

          assert data.active_timeout_eligible_sampled == 1
          assert Enum.any?(data.active_timeout_candidates, &(&1.id == id))
        end

        test "dry-run form does not execute cleanup and cleanup form delegates with limit" do
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_retention_cleanup_fun, fn opts ->
            send(test_pid, {:retention_cleanup, opts})
            {:ok, %{active_timeouts: 4, flows: 1, history: 2, values: 3}}
          end)

          on_exit(fn ->
            Application.delete_env(:ferricstore, :flow_dashboard_retention_cleanup_fun)
          end)

          assert {:ok, :dry_run, %{limit: 50}} =
                   Dashboard.apply_flow_retention_form(%{
                     "action" => "dry_run",
                     "limit" => "50"
                   })

          refute_received {:retention_cleanup, _opts}

          assert {:ok, :cleanup, %{active_timeouts: 4, flows: 1, history: 2, values: 3, limit: 7}} =
                   Dashboard.apply_flow_retention_form(%{
                     "action" => "cleanup",
                     "limit" => "7",
                     "confirm_cleanup" => "true"
                   })

          assert_received {:retention_cleanup, [limit: 7]}
        end
      end
    end
  end
end
