defmodule FerricstoreServer.Health.DashboardTest.Sections.AclFilteringAndConfig do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

      describe "dashboard ACL filtering and config" do
        setup do
          FerricstoreServer.Acl.reset!()
          :ok
        end

        test "keyspace inspector reports GET key permission when protected" do
          Application.put_env(:ferricstore, :protected_mode, true)

          :ok =
            FerricstoreServer.Acl.set_user("scan-only", [
              "on",
              ">secret",
              "~tenant-a:*",
              "-@all",
              "+SCAN"
            ])

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "scan-only",
              "password" => "secret"
            })

          response =
            http_get(HealthEndpoint.port(), "/dashboard/keyspace?key=tenant-b%3A1", [
              {"Cookie", dashboard_session_cookie(login)}
            ])

          assert extract_status_code(response) == 403
          assert extract_body(response) =~ "GET"
          assert extract_body(response) =~ "+GET"
          assert extract_body(response) =~ "%R~tenant-b:1"
        end

        test "keyspace sampled rows are filtered by dashboard ACL key patterns" do
          Application.put_env(:ferricstore, :protected_mode, true)

          suffix = System.unique_integer([:positive])
          allowed_key = "tenant-a:dash-keyspace:#{suffix}"
          denied_key = "tenant-b:dash-keyspace:#{suffix}"

          assert :ok = FerricStore.set(allowed_key, "visible")
          assert :ok = FerricStore.set(denied_key, "hidden")

          ShardHelpers.eventually(
            fn ->
              keys =
                %{"limit" => "200"}
                |> Dashboard.collect_keyspace_page()
                |> Map.fetch!(:rows)
                |> Enum.map(& &1.key)

              allowed_key in keys and denied_key in keys
            end,
            "expected keyspace dashboard sample to include setup keys",
            50,
            20
          )

          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-keyspace", [
              "on",
              ">secret",
              "~tenant-a:*",
              "-@all",
              "+SCAN"
            ])

          keys =
            %{"limit" => "200", "acl_username" => "tenant-a-keyspace"}
            |> Dashboard.collect_keyspace_page()
            |> Map.fetch!(:rows)
            |> Enum.map(& &1.key)

          assert allowed_key in keys
          refute denied_key in keys

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "tenant-a-keyspace",
              "password" => "secret"
            })

          cookie = dashboard_session_cookie(login)

          page =
            http_get(HealthEndpoint.port(), "/dashboard/keyspace?limit=200", [
              {"Cookie", cookie}
            ])

          assert extract_status_code(page) == 200
          assert extract_body(page) =~ allowed_key
          refute extract_body(page) =~ denied_key

          live =
            http_get(HealthEndpoint.port(), "/dashboard/api/keyspace?limit=200", [
              {"Cookie", cookie}
            ])

          assert extract_status_code(live) == 200
          assert extract_body(live) =~ allowed_key
          refute extract_body(live) =~ denied_key
        end

        test "Flow overview sampled rows are filtered by dashboard ACL key patterns" do
          Application.put_env(:ferricstore, :protected_mode, true)

          suffix = System.unique_integer([:positive])
          allowed_partition = "tenant-a:dash-flow:#{suffix}"
          denied_partition = "tenant-b:dash-flow:#{suffix}"
          allowed_id = "dash-flow-allowed-#{suffix}"
          denied_id = "dash-flow-denied-#{suffix}"

          assert :ok =
                   FerricStore.flow_create(allowed_id,
                     type: "dash-acl-flow",
                     state: "queued",
                     partition_key: allowed_partition,
                     run_at_ms: 1_000
                   )

          assert :ok =
                   FerricStore.flow_create(denied_id,
                     type: "dash-acl-flow",
                     state: "queued",
                     partition_key: denied_partition,
                     run_at_ms: 1_000
                   )

          ShardHelpers.eventually(
            fn ->
              records = Dashboard.collect_flow_page(partition_key: denied_partition).records
              Enum.any?(records, &(&1.id == denied_id))
            end,
            "expected Flow dashboard sample to include setup record",
            50,
            20
          )

          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-flow-dashboard", [
              "on",
              ">secret",
              "~tenant-a:*",
              "-@all",
              "+FLOW.LIST"
            ])

          denied_records =
            Dashboard.collect_flow_page(
              partition_key: denied_partition,
              acl_username: "tenant-a-flow-dashboard"
            ).records

          refute Enum.any?(denied_records, &(&1.id == denied_id))

          allowed_records =
            Dashboard.collect_flow_page(
              partition_key: allowed_partition,
              acl_username: "tenant-a-flow-dashboard"
            ).records

          assert Enum.any?(allowed_records, &(&1.id == allowed_id))

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "tenant-a-flow-dashboard",
              "password" => "secret"
            })

          cookie = dashboard_session_cookie(login)
          query = URI.encode_query(%{"partition_key" => denied_partition})

          page =
            http_get(HealthEndpoint.port(), "/dashboard/flow?#{query}", [
              {"Cookie", cookie}
            ])

          assert extract_status_code(page) == 200
          refute extract_body(page) =~ denied_id
        end

        test "Flow retention page filters sampled candidates by dashboard ACL key patterns" do
          Application.put_env(:ferricstore, :protected_mode, true)

          suffix = System.unique_integer([:positive])
          now_ms = System.system_time(:millisecond)
          allowed_partition = "tenant-a:dash-retention:#{suffix}"
          denied_partition = "tenant-b:dash-retention:#{suffix}"
          allowed_id = "dash-retention-allowed-#{suffix}"
          denied_id = "dash-retention-denied-#{suffix}"

          for {id, partition} <- [{allowed_id, allowed_partition}, {denied_id, denied_partition}] do
            key = Ferricstore.Flow.Keys.state_key(id, partition)

            record = %{
              id: id,
              type: "dash-retention-acl",
              state: "completed",
              version: 1,
              attempts: 1,
              fencing_token: 1,
              created_at_ms: now_ms - 10_000,
              updated_at_ms: now_ms - 5_000,
              next_run_at_ms: now_ms - 5_000,
              priority: 0,
              retention_ttl_ms: 1_000,
              terminal_retention_until_ms: now_ms - 1_000,
              partition_key: partition,
              run_state: "terminal"
            }

            assert :ok = FerricStore.set(key, Ferricstore.Flow.encode_record(record))
          end

          ShardHelpers.eventually(
            fn ->
              ids =
                Dashboard.collect_flow_retention_page(limit: 200).candidates
                |> Enum.map(& &1.id)

              allowed_id in ids and denied_id in ids
            end,
            "expected Flow retention dashboard sample to include setup candidates",
            50,
            20
          )

          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-retention-dashboard", [
              "on",
              ">secret",
              "~tenant-a:*",
              "-@all",
              "+FLOW.LIST"
            ])

          filtered_ids =
            Dashboard.collect_flow_retention_page(
              limit: 200,
              acl_username: "tenant-a-retention-dashboard"
            ).candidates
            |> Enum.map(& &1.id)

          assert allowed_id in filtered_ids
          refute denied_id in filtered_ids

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "tenant-a-retention-dashboard",
              "password" => "secret"
            })

          page =
            http_get(HealthEndpoint.port(), "/dashboard/flow/retention?limit=200", [
              {"Cookie", dashboard_session_cookie(login)}
            ])

          assert extract_status_code(page) == 200
          assert extract_body(page) =~ allowed_id
          refute extract_body(page) =~ denied_id
        end

        test "removed Flow projections live endpoint is not authorized as a Flow id" do
          Application.put_env(:ferricstore, :protected_mode, true)

          :ok =
            FerricstoreServer.Acl.set_user("flow-list", [
              "on",
              ">secret",
              "~*",
              "-@all",
              "+FLOW.LIST"
            ])

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "flow-list",
              "password" => "secret"
            })

          response =
            http_get(HealthEndpoint.port(), "/dashboard/api/flow/projections", [
              {"Cookie", dashboard_session_cookie(login)}
            ])

          assert extract_status_code(response) == 404
          assert Jason.decode!(extract_body(response)) == %{"error" => "not found"}

          :ok =
            FerricstoreServer.Acl.set_user("no-flow-list", [
              "on",
              ">secret",
              "~*",
              "-@all",
              "+FLOW.GET"
            ])

          denied_login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "no-flow-list",
              "password" => "secret"
            })

          denied =
            http_get(HealthEndpoint.port(), "/dashboard/api/flow/projections", [
              {"Cookie", dashboard_session_cookie(denied_login)}
            ])

          assert extract_status_code(denied) == 403
          assert Jason.decode!(extract_body(denied))["required_acl_rule"] == "+FLOW.LIST"
        end
      end

      # ---------------------------------------------------------------------------
      # Existing endpoints still work after dashboard addition
      # ---------------------------------------------------------------------------
      describe "existing endpoints unaffected" do
        test "/health/ready still returns JSON" do
          port = HealthEndpoint.port()
          response = http_get(port, "/health/ready")

          assert response =~ "HTTP/1.1 200 OK"
          assert response =~ "application/json"
          assert response =~ ~s("status":"ok")
        end

        test "unknown paths still return 404" do
          port = HealthEndpoint.port()
          response = http_get(port, "/nonexistent")

          assert response =~ "HTTP/1.1 404 Not Found"
        end
      end

      # ---------------------------------------------------------------------------
      # Dashboard reflects live data changes
      # ---------------------------------------------------------------------------

      describe "dashboard reflects live data" do
        test "shows shard status as ok when shards are running" do
          data = Dashboard.collect()

          # At least one shard should be ok since the app is running
          assert Enum.any?(data.shards, fn s -> s.status == "ok" end)
        end

        test "memory data shows non-zero max_bytes" do
          data = Dashboard.collect()

          assert data.memory.max_bytes > 0
        end

        test "eviction policy matches configuration" do
          data = Dashboard.collect()

          # Read the authoritative value directly from MemoryGuard (the same source
          # Dashboard.collect/0 uses) instead of Application.get_env, which can be
          # stale when other tests modify it via CONFIG SET without updating the
          # MemoryGuard GenServer state.
          configured = Ferricstore.MemoryGuard.eviction_policy()

          assert data.memory.eviction_policy == configured
        end
      end

      # ---------------------------------------------------------------------------
      # Namespace Config section (Dashboard Page 9)
      # ---------------------------------------------------------------------------

      describe "collect/0 namespace_config" do
        test "includes namespace_config key in collected data" do
          data = Dashboard.collect()

          assert Map.has_key?(data, :namespace_config)
          assert is_list(data.namespace_config)
        end

        test "returns empty list when no overrides are configured" do
          NamespaceConfig.reset_all()
          data = Dashboard.collect()

          assert data.namespace_config == []
        end

        test "returns configured namespaces after set" do
          NamespaceConfig.reset_all()
          :ok = NamespaceConfig.set("rate", "window_ms", "10")

          data = Dashboard.collect()

          assert length(data.namespace_config) == 1
          [entry] = data.namespace_config
          assert entry.prefix == "rate"
          assert entry.window_ms == 10
          assert is_integer(entry.changed_at)
          assert is_binary(entry.changed_by)

          NamespaceConfig.reset_all()
        end

        test "returns multiple configured namespaces sorted by prefix" do
          NamespaceConfig.reset_all()
          :ok = NamespaceConfig.set("zeta", "window_ms", "50")
          :ok = NamespaceConfig.set("alpha", "window_ms", "20")

          data = Dashboard.collect()

          assert length(data.namespace_config) == 2
          prefixes = Enum.map(data.namespace_config, & &1.prefix)
          assert prefixes == ["alpha", "zeta"]

          NamespaceConfig.reset_all()
        end
      end

      describe "render_config_page/1 namespace config sub-page" do
        test "config sub-page contains Namespace Config heading" do
          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          assert String.contains?(html, "Namespace Config")
        end

        test "config sub-page shows supported configuration commands" do
          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          assert String.contains?(html, "Configuration Commands")
          assert String.contains?(html, "CONFIG GET &lt;pattern&gt;")
          assert String.contains?(html, "CONFIG SET &lt;key&gt; &lt;value&gt;")
          assert String.contains?(html, "CONFIG SET LOCAL log_level &lt;level&gt;")
          assert String.contains?(html, "CONFIG RESETSTAT")
          assert String.contains?(html, "CONFIG REWRITE")

          assert String.contains?(
                   html,
                   "FERRICSTORE.CONFIG SET &lt;prefix&gt; window_ms &lt;ms&gt;"
                 )

          assert String.contains?(html, "FERRICSTORE.CONFIG RESET [prefix]")
        end

        test "config sub-page shows supported config parameters by scope" do
          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          assert String.contains?(html, "Runtime Parameters")
          assert String.contains?(html, "maxmemory-policy")
          assert String.contains?(html, "keydir-max-ram")
          assert String.contains?(html, "hot-cache-max-value-size")
          assert String.contains?(html, "maxmemory")
          assert String.contains?(html, "data-dir")
          assert String.contains?(html, "log_level")
          assert String.contains?(html, "read-write")
          assert String.contains?(html, "read-only")
          assert String.contains?(html, "node-local")
        end

        test "shows built-in defaults message when no namespaces configured" do
          NamespaceConfig.reset_all()
          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          assert String.contains?(html, "All namespaces using built-in default window (1ms)")
        end

        test "shows table with prefix and window when namespaces configured" do
          NamespaceConfig.reset_all()
          :ok = NamespaceConfig.set("session", "window_ms", "5")

          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          assert String.contains?(html, "session")
          assert String.contains?(html, "5")
          refute String.contains?(html, "quorum")
          # Table headers
          assert String.contains?(html, "Prefix")
          assert String.contains?(html, "Window (ms)")
          refute String.contains?(html, "Durability")
          assert String.contains?(html, "Changed At")
          assert String.contains?(html, "Changed By")

          NamespaceConfig.reset_all()
        end

        test "does not render removed namespace durability state" do
          NamespaceConfig.reset_all()
          assert {:error, _} = NamespaceConfig.set("ephemeral", "durability", "async")

          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          [_before, ns_section] = String.split(html, "Namespace Config", parts: 2)

          ns_html =
            case String.split(ns_section, "section-title", parts: 2) do
              [section, _rest] -> section
              [section] -> section
            end

          refute String.contains?(ns_html, "ephemeral")
          refute String.contains?(ns_html, ">async<")

          NamespaceConfig.reset_all()
        end

        test "does not render removed durability column state" do
          NamespaceConfig.reset_all()
          :ok = NamespaceConfig.set("safe", "window_ms", "2")

          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          # Extract the config table area to check
          [_before, ns_section] = String.split(html, "Namespace Config", parts: 2)
          # Take until the next section or end of body
          ns_html =
            case String.split(ns_section, "section-title", parts: 2) do
              [section, _rest] -> section
              [section] -> section
            end

          refute String.contains?(ns_html, "Durability")
          refute String.contains?(ns_html, "quorum")
          refute String.contains?(ns_html, "c-yellow")

          NamespaceConfig.reset_all()
        end

        test "shows overrides count badge when namespaces are configured" do
          NamespaceConfig.reset_all()
          :ok = NamespaceConfig.set("metrics", "window_ms", "100")

          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          assert String.contains?(html, "1 overrides")

          NamespaceConfig.reset_all()
        end

        test "shows defaults badge when all defaults" do
          NamespaceConfig.reset_all()

          data = Dashboard.collect_config_page()
          html = Dashboard.render_config_page(data)

          assert String.contains?(html, "defaults")

          NamespaceConfig.reset_all()
        end

        test "config sidebar link appears after merge status in sidebar" do
          data = Dashboard.collect()
          html = Dashboard.render(data)

          merge_pos = :binary.match(html, "Merge Status") |> elem(0)
          config_pos = :binary.match(html, "Config") |> elem(0)

          assert config_pos > merge_pos
        end
      end
    end
  end
end
