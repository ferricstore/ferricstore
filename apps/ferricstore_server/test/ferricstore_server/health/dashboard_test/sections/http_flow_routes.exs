defmodule FerricstoreServer.Health.DashboardTest.Sections.HttpFlowRoutes do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

      describe "GET /dashboard/flow sub-pages" do
        test "returns 200 with Flow page HTML" do
          port = HealthEndpoint.port()
          response = http_get(port, "/dashboard/flow")

          assert response =~ "HTTP/1.1 200 OK"
          assert response =~ "text/html"

          body = extract_body(response)
          assert String.contains?(body, "FerricFlow")
          assert String.contains?(body, "Flow Overview")
        end

        test "redirects removed Flow config page to global config" do
          port = HealthEndpoint.port()

          for path <- [
                "/dashboard/flow/config",
                "/dashboard/flow/config?source=flow-sidebar"
              ] do
            response = http_get(port, path)

            assert response =~ "HTTP/1.1 302 Found"
            assert response =~ "Location: /dashboard/config"
          end
        end

        test "returns 200 with Flow detail page HTML" do
          id = "dashboard-flow-http-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-http",
                     state: "queued",
                     run_at_ms: 1_000
                   )

          port = HealthEndpoint.port()
          response = http_get(port, "/dashboard/flow/#{id}")

          assert response =~ "HTTP/1.1 200 OK"

          body = extract_body(response)
          assert String.contains?(body, id)
          assert String.contains?(body, "Flow Detail")
        end

        @tag :workflow_widgets
        test "detail page and refresh defer value reads until the scoped value endpoint is requested" do
          id = "dashboard-lazy-value-#{System.unique_integer([:positive])}"
          partition = "dashboard-lazy-partition"
          ref = "payload:#{id}"
          test_pid = self()
          previous = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)

          Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
            send(test_pid, {:lazy_value_read, refs})
            {:ok, Enum.map(refs, fn _ -> "payload-only-after-opening" end)}
          end)

          on_exit(fn -> restore_env(:flow_dashboard_flow_value_mget_fun, previous) end)

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-lazy-values",
                     partition_key: partition,
                     state: "queued",
                     payload_ref: ref,
                     run_at_ms: 1_000
                   )

          port = HealthEndpoint.port()
          scope = URI.encode_query(%{"partition_key" => partition})

          for path <- ["/dashboard/flow/#{id}?#{scope}", "/dashboard/api/flow/#{id}?#{scope}"] do
            response = http_get(port, path)
            assert response =~ "HTTP/1.1 200 OK"
            refute response =~ "payload-only-after-opening"
            refute_receive {:lazy_value_read, _}
          end

          value_params =
            URI.encode_query(%{"flow" => id, "partition_key" => partition, "ref" => ref})

          response = http_get(port, "/dashboard/api/flow/value?#{value_params}")
          assert response =~ "HTTP/1.1 200 OK"
          assert response =~ "payload-only-after-opening"
          assert_receive {:lazy_value_read, [^ref]}
        end

        test "redirects Flow lookup searches to encoded detail URLs" do
          id = "dashboard-flow-lookup/#{System.unique_integer([:positive])} with space"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-lookup",
                     state: "queued",
                     run_at_ms: 1_000
                   )

          port = HealthEndpoint.port()
          query = URI.encode_query(%{"id" => id})
          encoded_id = URI.encode(id, &URI.char_unreserved?/1)
          response = http_get(port, "/dashboard/flow/lookup?#{query}")

          assert response =~ "HTTP/1.1 302 Found"
          assert response =~ "Location: /dashboard/flow/#{encoded_id}"
        end

        test "redirects Flow lookup searches with partition key" do
          id = "dashboard-flow-lookup-partitioned/#{System.unique_integer([:positive])}"
          partition_key = "tenant lookup #{System.unique_integer([:positive])}"

          port = HealthEndpoint.port()
          query = URI.encode_query(%{"id" => id, "partition_key" => partition_key})
          encoded_id = URI.encode(id, &URI.char_unreserved?/1)
          encoded_partition = URI.encode_query(%{"partition_key" => partition_key})
          response = http_get(port, "/dashboard/flow/lookup?#{query}")

          assert response =~ "HTTP/1.1 302 Found"
          assert response =~ "Location: /dashboard/flow/#{encoded_id}?#{encoded_partition}"
        end

        test "redirects partition-only Flow lookup searches to scoped overview" do
          partition_key = "tenant only #{System.unique_integer([:positive])}"

          port = HealthEndpoint.port()
          query = URI.encode_query(%{"partition_key" => partition_key})
          encoded_partition = URI.encode_query(%{"partition_key" => partition_key})
          response = http_get(port, "/dashboard/flow/lookup?#{query}")

          assert response =~ "HTTP/1.1 302 Found"
          assert response =~ "Location: /dashboard/flow?#{encoded_partition}"
        end

        test "returns Flow detail page for explicitly partitioned records" do
          id = "dashboard-flow-http-partitioned/#{System.unique_integer([:positive])}"
          partition_key = "tenant-http-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-http-partitioned",
                     partition_key: partition_key,
                     state: "queued",
                     run_at_ms: 1_000
                   )

          port = HealthEndpoint.port()
          encoded_id = URI.encode(id, &URI.char_unreserved?/1)
          encoded_partition = URI.encode_query(%{"partition_key" => partition_key})
          response = http_get(port, "/dashboard/flow/#{encoded_id}?#{encoded_partition}")

          assert response =~ "HTTP/1.1 200 OK"

          body = extract_body(response)
          assert String.contains?(body, id)
          assert String.contains?(body, partition_key)
          refute String.contains?(body, "was not found")
        end

        test "returns Flow detail live component JSON" do
          id = "dashboard-flow-api/#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-api",
                     state: "queued",
                     run_at_ms: 1_000
                   )

          port = HealthEndpoint.port()
          encoded_id = URI.encode(id, &URI.char_unreserved?/1)
          response = http_get(port, "/dashboard/api/flow/#{encoded_id}")

          assert response =~ "HTTP/1.1 200 OK"
          assert response =~ "application/json"

          {:ok, decoded} = response |> extract_body() |> Jason.decode()
          assert is_integer(decoded["generated_at_ms"])
          assert is_binary(decoded["components"]["flow_detail"])
          assert is_binary(decoded["components"]["flow_debug"])
          assert is_binary(decoded["components"]["flow_history"])
          assert decoded["components"]["flow_detail"] =~ id
          assert decoded["components"]["flow_debug"] =~ "Debug Inspector"
        end

        test "returns 200 for Flow states, workers, due, and policies pages" do
          port = HealthEndpoint.port()

          for {path, title} <- [
                {"/dashboard/flow/states", "Flow States"},
                {"/dashboard/flow/workers", "Flow Workers"},
                {"/dashboard/flow/workers?refresh=1", "Flow Workers"},
                {"/dashboard/flow/due", "Due / Scheduled"},
                {"/dashboard/flow/due?refresh=1", "Due / Scheduled"},
                {"/dashboard/flow/failures", "Flow Failures"},
                {"/dashboard/flow/lineage", "Flow Lineage"},
                {"/dashboard/flow/query", "Flow Query Studio"},
                {"/dashboard/flow/signals", "Flow Signals"},
                {"/dashboard/flow/policies", "FerricFlow Policies"},
                {"/dashboard/flow/governance", "FerricFlow Governance"},
                {"/dashboard/flow/retention", "FerricFlow Retention"}
              ] do
            response = http_get(port, path)

            assert response =~ "HTTP/1.1 200 OK"
            assert response =~ "text/html"
            assert response |> extract_body() |> String.contains?(title)
          end
        end

        test "Flow workers and due live endpoints preserve their route with query strings" do
          port = HealthEndpoint.port()

          for {path, component} <- [
                {"/dashboard/api/flow/workers?refresh=1", "flow_workers"},
                {"/dashboard/api/flow/due?refresh=1", "flow_due_now"}
              ] do
            response = http_get(port, path)

            assert response =~ "HTTP/1.1 200 OK"
            assert response =~ "application/json"

            decoded = response |> extract_body() |> Jason.decode!()
            assert is_binary(decoded["components"][component])
            refute Map.has_key?(decoded["components"], "flow_detail")
          end
        end

        test "returns 200 for KV dashboard pages" do
          port = HealthEndpoint.port()

          for {path, title} <- [
                {"/dashboard/keyspace", "Keyspace"},
                {"/dashboard/keyspace?key=missing-key", "Keyspace"},
                {"/dashboard/commands", "Commands"},
                {"/dashboard/reads", "Read Path"},
                {"/dashboard/doctor", "Doctor"}
              ] do
            response = http_get(port, path)

            assert response =~ "HTTP/1.1 200 OK"
            assert response =~ "text/html"
            assert response |> extract_body() |> String.contains?(title)
          end
        end

        test "redirects removed Flow projections page back to overview" do
          port = HealthEndpoint.port()

          for path <- [
                "/dashboard/flow/projections",
                "/dashboard/flow/projections?partition_key=tenant-a"
              ] do
            response = http_get(port, path)

            assert response =~ "HTTP/1.1 302 Found"
            assert response =~ "Location: /dashboard/flow"
          end
        end

        test "POST /dashboard/flow/policies creates policy and redirects with status" do
          port = HealthEndpoint.port()
          type = "dashboard-policy-http-#{System.unique_integer([:positive])}"

          response =
            http_post_form(port, "/dashboard/flow/policies", %{
              "type" => type,
              "max_retries" => "6",
              "backoff_kind" => "fixed",
              "base_ms" => "50",
              "max_ms" => "500",
              "jitter_pct" => "5",
              "exhausted_to" => "dead",
              "max_active_ms" => "30000",
              "retention_ttl_ms" => "60000",
              "history_max_events" => "25"
            })

          assert response =~ "HTTP/1.1 302 Found"
          assert response =~ "Location: /dashboard/flow/policies?"
          assert response =~ "status=ok"

          assert {:ok, policy} = FerricStore.flow_policy_get(type)
          assert policy.retry.max_retries == 6
          assert policy.retry.backoff.kind == :fixed
          assert policy.retry.exhausted_to == "dead"
          assert policy.max_active_ms == 30_000
          assert policy.retention.ttl_ms == 60_000
          refute Map.has_key?(policy.retention, :history_hot_max_events)
          assert policy.retention.history_max_events == 25

          get_response = http_get(port, "/dashboard/flow/policies?status=ok&type=#{type}")
          assert get_response =~ "HTTP/1.1 200 OK"
          assert get_response |> extract_body() |> String.contains?("Policy saved")
        end

        test "POST /dashboard/flow/retention runs cleanup and redirects with counts" do
          port = HealthEndpoint.port()
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_retention_cleanup_fun, fn opts ->
            send(test_pid, {:retention_cleanup, opts})
            {:ok, %{active_timeouts: 4, flows: 1, history: 2, values: 3}}
          end)

          on_exit(fn ->
            Application.delete_env(:ferricstore, :flow_dashboard_retention_cleanup_fun)
          end)

          response =
            http_post_form(port, "/dashboard/flow/retention", %{
              "action" => "cleanup",
              "limit" => "3",
              "confirm_cleanup" => "true"
            })

          assert extract_status_code(response) == 302
          location = extract_header(response, "location")
          assert location =~ "/dashboard/flow/retention?"
          assert location =~ "status=ok"
          assert location =~ "active_timeouts=4"
          assert location =~ "flows=1"
          assert location =~ "history=2"
          assert location =~ "values=3"
          assert_received {:retention_cleanup, [limit: 3]}

          get_response = http_get(port, location)
          assert get_response =~ "HTTP/1.1 200 OK"
          assert get_response |> extract_body() |> String.contains?("Cleanup completed")
          assert get_response |> extract_body() |> String.contains?("4 active flows timed out")
        end

        test "POST /dashboard/flow/failures preserves the investigation scope" do
          port = HealthEndpoint.port()
          test_pid = self()

          Application.put_env(:ferricstore, :flow_dashboard_flow_reclaim_fun, fn type, opts ->
            send(test_pid, {:flow_reclaim, type, opts})
            {:ok, [%{id: "reclaimed-flow"}]}
          end)

          on_exit(fn ->
            Application.delete_env(:ferricstore, :flow_dashboard_flow_reclaim_fun)
          end)

          response =
            http_post_form(port, "/dashboard/flow/failures", %{
              "action" => "reclaim",
              "type" => "email jobs",
              "partition_key" => "tenant/a",
              "worker" => "recovery-worker",
              "limit" => "25",
              "lease_ms" => "30000",
              "confirm_reclaim" => "true",
              "return_q" => "checkout failed",
              "return_limit" => "80",
              "return_exact" => "true"
            })

          assert extract_status_code(response) == 302

          location = extract_header(response, "location")
          %URI{path: path, query: query} = URI.parse(location)

          assert path == "/dashboard/flow/failures"

          assert URI.decode_query(query) == %{
                   "count" => "1",
                   "exact" => "true",
                   "limit" => "80",
                   "partition_key" => "tenant/a",
                   "q" => "checkout failed",
                   "status" => "reclaimed",
                   "type" => "email jobs"
                 }

          assert_received {:flow_reclaim, "email jobs", opts}
          assert opts[:partition_key] == "tenant/a"
          assert opts[:worker] == "recovery-worker"
          assert opts[:limit] == 25
          assert opts[:lease_ms] == 30_000
        end

        test "POST /dashboard/flow/policies redirects invalid forms to a visible error" do
          port = HealthEndpoint.port()

          response =
            http_post_form(port, "/dashboard/flow/policies", %{
              "type" => "",
              "max_retries" => "1",
              "backoff_kind" => "fixed",
              "base_ms" => "50",
              "max_ms" => "500",
              "jitter_pct" => "0",
              "exhausted_to" => "failed",
              "retention_ttl_ms" => "60000",
              "history_max_events" => "25"
            })

          assert extract_status_code(response) == 302
          location = extract_header(response, "location")
          assert location =~ "/dashboard/flow/policies?"
          assert location =~ "status=error"

          get_response = http_get(port, location)
          assert get_response =~ "HTTP/1.1 200 OK"
          assert get_response |> extract_body() |> String.contains?("ERR flow type is required")
          assert get_response |> extract_body() |> String.contains?("flow-alert-error")
        end

        test "Flow detail page renders rewind success and error flash messages" do
          port = HealthEndpoint.port()
          id = "dashboard-rewind-flash-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: "dashboard-rewind-flash",
                     state: "queued",
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )

          encoded_id = URI.encode(id, &URI.char_unreserved?/1)

          success = http_get(port, "/dashboard/flow/#{encoded_id}?status=rewound")
          success_body = extract_body(success)

          assert success =~ "HTTP/1.1 200 OK"
          assert success_body =~ "flow-alert-ok"
          assert success_body =~ "Flow rewound"

          error =
            http_get(
              port,
              "/dashboard/flow/#{encoded_id}?status=error&message=ERR+rewind+target+event+is+required"
            )

          error_body = extract_body(error)

          assert error =~ "HTTP/1.1 200 OK"
          assert error_body =~ "flow-alert-error"
          assert error_body =~ "ERR rewind target event is required"
        end

        test "POST /dashboard/flow/:id/rewind restores selected history event" do
          port = HealthEndpoint.port()
          flow_type = "dashboard-rewind-http-#{System.unique_integer([:positive])}"
          partition_key = "tenant-rewind-http-#{System.unique_integer([:positive])}"
          id = "dashboard-rewind-http-flow-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(id,
                     type: flow_type,
                     partition_key: partition_key,
                     state: "queued",
                     run_at_ms: 1_000,
                     now_ms: 1_000
                   )

          assert {:ok, [{created_event_id, _fields} | _]} =
                   FerricStore.flow_history(id, partition_key: partition_key, count: 10)

          assert :ok =
                   FerricStore.flow_transition(id, "queued", "ready",
                     partition_key: partition_key,
                     fencing_token: 0,
                     run_at_ms: 2_000,
                     now_ms: 2_000
                   )

          encoded_id = URI.encode(id, &URI.char_unreserved?/1)

          response =
            http_post_form(port, "/dashboard/flow/#{encoded_id}/rewind", %{
              "partition_key" => partition_key,
              "to_event" => created_event_id,
              "confirm_rewind" => "true"
            })

          assert response =~ "HTTP/1.1 302 Found"
          assert response =~ "Location: /dashboard/flow/#{encoded_id}?"
          assert response =~ "status=rewound"

          assert {:ok, rewound} = FerricStore.flow_get(id, partition_key: partition_key)
          assert rewound.state == "queued"
          assert rewound.rewound_to_event_id == created_event_id
        end

        test "Flow states HTTP and live API preserve type filter" do
          flow_type = "dashboard-http-filter-#{System.unique_integer([:positive])}"
          other_type = "dashboard-http-other-#{System.unique_integer([:positive])}"

          assert :ok =
                   FerricStore.flow_create(
                     "dashboard-http-filter-id-#{System.unique_integer([:positive])}",
                     type: flow_type,
                     state: "queued",
                     run_at_ms: 1_000
                   )

          assert :ok =
                   FerricStore.flow_create(
                     "dashboard-http-other-id-#{System.unique_integer([:positive])}",
                     type: other_type,
                     state: "queued",
                     run_at_ms: 1_000
                   )

          port = HealthEndpoint.port()

          response =
            http_get(port, "/dashboard/flow/states?type=#{URI.encode_www_form(flow_type)}")

          body = extract_body(response)

          assert response =~ "HTTP/1.1 200 OK"
          assert body =~ flow_type

          api_response =
            http_get(port, "/dashboard/api/flow/states?type=#{URI.encode_www_form(flow_type)}")

          assert api_response =~ "HTTP/1.1 200 OK"
          {:ok, decoded} = api_response |> extract_body() |> Jason.decode()
          table = decoded["components"]["flow_states_table"]
          assert table =~ flow_type
          refute table =~ other_type
        end
      end
    end
  end
end
