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
          response = http_get(port, "/dashboard/flow/config")

          assert response =~ "HTTP/1.1 302 Found"
          assert response =~ "Location: /dashboard/config"
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
                {"/dashboard/flow/due", "Due / Scheduled"},
                {"/dashboard/flow/failures", "Flow Failures"},
                {"/dashboard/flow/lineage", "Flow Lineage"},
                {"/dashboard/flow/query", "Flow Query Explorer"},
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
          response = http_get(port, "/dashboard/flow/projections")

          assert response =~ "HTTP/1.1 302 Found"
          assert response =~ "Location: /dashboard/flow"
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
            {:ok, %{flows: 1, history: 2, values: 3}}
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
          assert location =~ "flows=1"
          assert location =~ "history=2"
          assert location =~ "values=3"
          assert_received {:retention_cleanup, [limit: 3]}

          get_response = http_get(port, location)
          assert get_response =~ "HTTP/1.1 200 OK"
          assert get_response |> extract_body() |> String.contains?("Cleanup completed")
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
