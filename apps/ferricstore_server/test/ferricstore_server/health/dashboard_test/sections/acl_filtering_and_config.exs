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

        @tag :dashboard_keyspace_acl_summary
        test "keyspace sampled rows and counts are filtered by dashboard ACL key patterns" do
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

          data =
            Dashboard.collect_keyspace_page(%{
              "limit" => "200",
              "acl_username" => "tenant-a-keyspace"
            })

          keys = Enum.map(data.rows, & &1.key)

          assert allowed_key in keys
          refute denied_key in keys
          assert data.total_sampled == length(data.rows)

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

        test "Flow record ACL filtering never treats the Flow type as its key scope" do
          records = [
            %{id: "flow-a", partition_key: "tenant-a:partition", type: "shared-type"},
            %{id: "flow-b", partition_key: "tenant-b:partition", type: "shared-type"}
          ]

          :ok =
            FerricstoreServer.Acl.set_user("flow-type-only", [
              "on",
              ">secret",
              "%R~shared-type",
              "-@all",
              "+FLOW.LIST"
            ])

          assert FerricstoreServer.Health.Dashboard.Access.filter_flow_records_for_acl(
                   records,
                   "flow-type-only"
                 ) == []
        end

        @tag :flow_detail_acl_partition
        test "Flow detail rechecks the fetched record partition instead of trusting its id" do
          suffix = System.unique_integer([:positive])
          id = "visible-flow:#{suffix}"
          username = "flow-detail-partition-#{suffix}"
          test_pid = self()
          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)

          :ok =
            FerricstoreServer.Acl.set_user(username, [
              "on",
              ">secret",
              "%R~visible-flow:*",
              "-@all",
              "+FLOW.GET",
              "+FLOW.HISTORY"
            ])

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, _opts ->
            {:ok,
             %{
               id: id,
               type: "sensitive",
               state: "queued",
               partition_key: "denied-tenant:partition"
             }}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
            send(test_pid, :unauthorized_history_read)
            {:ok, []}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
            restore_env(:flow_dashboard_flow_history_fun, previous_history)
          end)

          data = Dashboard.collect_flow_detail_page(id, acl_username: username, values: false)

          assert data.record == nil
          assert data.record_status == :not_found
          assert data.history_status == :skipped

          Application.put_env(:ferricstore, :protected_mode, true)

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => username,
              "password" => "secret"
            })

          cookie = dashboard_session_cookie(login)
          encoded_id = URI.encode(id, &URI.char_unreserved?/1)

          for path <- ["/dashboard/flow/#{encoded_id}", "/dashboard/api/flow/#{encoded_id}"] do
            response = http_get(HealthEndpoint.port(), path, [{"Cookie", cookie}])
            body = extract_body(response)

            assert extract_status_code(response) == 200
            refute body =~ "denied-tenant:partition"
            refute body =~ "sensitive"
          end

          refute_receive :unauthorized_history_read
        end

        @tag :flow_query_history_acl_partition
        test "Flow query history rechecks the persisted partition before reading history" do
          suffix = System.unique_integer([:positive])
          id = "visible-flow:#{suffix}"
          username = "flow-query-history-partition-#{suffix}"
          test_pid = self()
          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)

          :ok =
            FerricstoreServer.Acl.set_user(username, [
              "on",
              ">secret",
              "%R~visible-flow:*",
              "-@all",
              "+FLOW.HISTORY"
            ])

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, opts ->
            send(test_pid, {:flow_query_history_get_opts, opts})

            {:ok,
             %{
               id: id,
               type: "sensitive",
               state: "queued",
               partition_key: "denied-tenant:partition"
             }}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
            send(test_pid, :unauthorized_query_history_read)
            {:ok, [{"1-0", %{"secret" => "value"}}]}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
            restore_env(:flow_dashboard_flow_history_fun, previous_history)
          end)

          data =
            Dashboard.collect_flow_query_page(
              kind: "history",
              id: id,
              acl_username: username
            )

          assert data.result.status == :ok
          assert data.result.rows == []
          assert data.result.message == "0 visible row(s)"
          assert_receive {:flow_query_history_get_opts, opts}
          assert Keyword.get(opts, :payload) == false
          refute_receive :unauthorized_query_history_read
        end

        @tag :flow_query_history_acl_partition
        test "Flow query history routes authorized reads with the persisted partition" do
          suffix = System.unique_integer([:positive])
          id = "tenant-a:flow:#{suffix}"
          username = "flow-query-history-allowed-#{suffix}"
          test_pid = self()
          previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
          previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)

          :ok =
            FerricstoreServer.Acl.set_user(username, [
              "on",
              ">secret",
              "%R~tenant-a:*",
              "-@all",
              "+FLOW.HISTORY"
            ])

          ShardHelpers.eventually(
            fn ->
              FerricstoreServer.Acl.check_key_access(
                username,
                "tenant-a:partition",
                :read
              ) == :ok
            end,
            "expected Flow history ACL projection to become visible",
            50,
            20
          )

          Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, _opts ->
            {:ok,
             %{
               id: id,
               type: "email",
               state: "queued",
               partition_key: "tenant-a:partition"
             }}
          end)

          Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, opts ->
            send(test_pid, {:authorized_query_history_opts, opts})
            {:ok, [{"1-0", %{"signal" => "queued"}}]}
          end)

          on_exit(fn ->
            restore_env(:flow_dashboard_flow_get_fun, previous_get)
            restore_env(:flow_dashboard_flow_history_fun, previous_history)
          end)

          data =
            Dashboard.collect_flow_query_page(
              kind: "history",
              id: id,
              acl_username: username
            )

          assert data.result.status == :ok
          assert data.result.rows == [{"1-0", %{"signal" => "queued"}}]
          assert_receive {:authorized_query_history_opts, opts}
          assert Keyword.get(opts, :partition_key) == "tenant-a:partition"
          assert Keyword.get(opts, :values) == false
          assert Keyword.get(opts, :consistent_projection) == true
        end

        test "Flow query aggregate rows require their canonical request scope" do
          row = %{type: "shared-type", state: "completed", count: 2}
          result = %{rows: [row], message: "1 row"}

          :ok =
            FerricstoreServer.Acl.set_user("flow-query-type-only", [
              "on",
              ">secret",
              "%R~shared-type",
              "-@all",
              "+FLOW.STATS"
            ])

          assert %{rows: []} =
                   FerricstoreServer.Health.Dashboard.Access.flow_query_filter_result_for_acl(
                     result,
                     "flow-query-type-only",
                     "*"
                   )

          :ok =
            FerricstoreServer.Acl.set_user("flow-query-tenant", [
              "on",
              ">secret",
              "%R~tenant-a:*",
              "-@all",
              "+FLOW.STATS"
            ])

          assert %{rows: [^row]} =
                   FerricstoreServer.Health.Dashboard.Access.flow_query_filter_result_for_acl(
                     result,
                     "flow-query-tenant",
                     "tenant-a:partition"
                   )
        end

        test "stream activity rows are filtered by dashboard ACL key patterns" do
          Ferricstore.Stream.ActivityLog.reset()

          suffix = System.unique_integer([:positive])
          allowed_key = "tenant-a:dash-stream:#{suffix}"
          denied_key = "tenant-b:dash-stream:#{suffix}"

          assert {:ok, _} = FerricStore.xadd(allowed_key, ["field", "visible"])
          assert {:ok, _} = FerricStore.xadd(denied_key, ["field", "hidden"])

          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-stream-dashboard", [
              "on",
              ">secret",
              "~tenant-a:*",
              "-@all",
              "+XINFO"
            ])

          keys =
            %{"acl_username" => "tenant-a-stream-dashboard"}
            |> Dashboard.collect_streams_page()
            |> Map.fetch!(:entries)
            |> Enum.map(& &1.key)

          assert allowed_key in keys
          refute denied_key in keys
        end

        @tag :dashboard_pubsub_acl_summary
        test "pubsub activity and summary are filtered by dashboard ACL channel patterns" do
          Ferricstore.PubSub.ActivityLog.reset()

          suffix = System.unique_integer([:positive])
          allowed_channel = "tenant-a:dash-pubsub:#{suffix}"
          denied_channel = "tenant-b:dash-pubsub:#{suffix}"

          :ok = Ferricstore.PubSub.subscribe(allowed_channel, self())
          :ok = Ferricstore.PubSub.subscribe(denied_channel, self())

          on_exit(fn ->
            Ferricstore.PubSub.unsubscribe(allowed_channel, self())
            Ferricstore.PubSub.unsubscribe(denied_channel, self())
          end)

          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-pubsub-dashboard", [
              "on",
              ">secret",
              "&tenant-a:*",
              "-@all",
              "+PUBSUB"
            ])

          data =
            Dashboard.collect_pubsub_page(%{
              "acl_username" => "tenant-a-pubsub-dashboard"
            })

          channels = Enum.map(data.channels, & &1.channel)
          targets = Enum.map(data.activity, & &1.target)

          assert allowed_channel in channels
          refute denied_channel in channels
          assert Enum.any?(targets, &String.contains?(&1, allowed_channel))
          refute Enum.any?(targets, &String.contains?(&1, denied_channel))

          assert data.summary.exact_subscriptions ==
                   Enum.sum(Enum.map(data.channels, & &1.subscribers))

          assert data.summary.pattern_subscriptions ==
                   Enum.sum(Enum.map(data.patterns, & &1.subscribers))

          assert data.summary.active_subscribers == nil
          assert Dashboard.render_pubsub_page(data) =~ "Pub/Sub Activity"
        end

        test "security page is OSS-only diagnostics and redacts password material" do
          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-security", [
              "on",
              ">very-secret-password",
              "~tenant-a:*",
              "&tenant-a:*",
              "-@all",
              "+GET",
              "+SET",
              "+PUBSUB"
            ])

          data =
            Dashboard.collect_security_page(%{
              "user" => "tenant-a-security",
              "command" => "GET",
              "key" => "tenant-a:visible",
              "key_access" => "read",
              "channel" => "tenant-a:events"
            })

          html = Dashboard.render_security_page(data)

          assert html =~ "Security"
          assert html =~ "tenant-a-security"
          assert html =~ "Command allowed"
          assert html =~ "Key allowed"
          assert html =~ "Channel allowed"
          assert html =~ "ACL.LIST"
          refute html =~ "very-secret-password"
          refute html =~ "ACL SETUSER"
          refute html =~ ~s(method="post")
        end

        @tag :dashboard_security_acl_identity
        test "security diagnostics preserve an empty or whitespace session identity" do
          for username <- ["", " "] do
            data = Dashboard.collect_security_page(%{"acl_username" => username})

            assert data.current_user == username
            assert data.tester.input.user == username
          end
        end

        @tag :dashboard_security_acl_encoded_identity
        test "security diagnostics decode unambiguous ACL list usernames" do
          username = "tenant security"

          :ok =
            FerricstoreServer.Acl.set_user(username, [
              "on",
              "nopass",
              "~*",
              "&*",
              "+@all"
            ])

          data = Dashboard.collect_security_page()

          assert %{username: ^username, state: "on"} =
                   Enum.find(data.acl_users, &(&1.username == username))
        end

        @tag :dashboard_security_acl_exact_values
        test "security diagnostics preserve exact usernames keys and channels" do
          username = " exact user "
          key = " exact key "
          channel = " exact channel "

          :ok =
            FerricstoreServer.Acl.set_user(username, [
              "on",
              "nopass",
              "%R~#{key}",
              "&#{channel}",
              "-@all"
            ])

          data =
            Dashboard.collect_security_page(%{
              "user" => username,
              "key" => key,
              "key_access" => "read",
              "channel" => channel
            })

          assert data.tester.input.user == username
          assert data.tester.input.key == key
          assert data.tester.input.channel == channel
          assert data.tester.key.status == :allowed
          assert data.tester.channel.status == :allowed
        end

        @tag :dashboard_security_acl_malformed_channels
        test "security channel diagnostics fail closed for malformed ACL state" do
          username = "malformed-channel-user"

          true =
            :ets.insert(
              FerricstoreServer.Acl.Tables.active_table(),
              {username, %{enabled: true, channels: :invalid}}
            )

          data =
            Dashboard.collect_security_page(%{
              "user" => username,
              "channel" => "events"
            })

          assert data.tester.channel.status == :denied
        end

        test "security page tester shows denied command key and channel checks" do
          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-readonly", [
              "on",
              ">secret",
              "%R~tenant-a:*",
              "&tenant-a:*",
              "-@all",
              "+GET"
            ])

          data =
            Dashboard.collect_security_page(%{
              "user" => "tenant-a-readonly",
              "command" => "SET",
              "key" => "tenant-a:write",
              "key_access" => "write",
              "channel" => "tenant-b:events"
            })

          html = Dashboard.render_security_page(data)

          assert html =~ "Command denied"
          assert html =~ "Key denied"
          assert html =~ "Channel denied"
        end

        test "security page requires ACL LIST when dashboard is protected" do
          Application.put_env(:ferricstore, :protected_mode, true)

          :ok =
            FerricstoreServer.Acl.set_user("security-no-acl-list", [
              "on",
              ">secret",
              "~*",
              "-@all",
              "+INFO"
            ])

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "security-no-acl-list",
              "password" => "secret"
            })

          response =
            http_get(HealthEndpoint.port(), "/dashboard/security", [
              {"Cookie", dashboard_session_cookie(login)}
            ])

          assert extract_status_code(response) == 403
          assert extract_body(response) =~ "ACL.LIST"
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

          allowed_data =
            Dashboard.collect_flow_page(
              partition_key: allowed_partition,
              acl_username: "tenant-a-flow-dashboard"
            )

          allowed_records = allowed_data.records

          assert Enum.any?(allowed_records, &(&1.id == allowed_id))
          assert allowed_data.total_sampled == allowed_data.filtered_sampled
          assert allowed_data.projection == %{restricted: true}

          refute Dashboard.render_flow_page(allowed_data) =~
                   "cold/query projection runs after durable Flow writes"

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

            assert :ok =
                     Ferricstore.Store.Router.put(
                       FerricStore.Instance.get(:default),
                       key,
                       Ferricstore.Flow.encode_record(record),
                       0
                     )
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

          restricted_data =
            Dashboard.collect_flow_retention_page(
              limit: 200,
              acl_username: "tenant-a-retention-dashboard"
            )

          filtered_ids = Enum.map(restricted_data.candidates, & &1.id)

          assert allowed_id in filtered_ids
          refute denied_id in filtered_ids
          assert restricted_data.total_sampled == restricted_data.filtered_sampled
          assert restricted_data.storage == %{restricted: true}
          assert restricted_data.projection == %{restricted: true}

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
          refute extract_body(page) =~ "current data directory footprint"
          refute extract_body(page) =~ "cold query-index work pending"
        end

        @tag :dashboard_policy_acl_summary
        test "Flow policy page filters configured types editor data and sample counts by type ACL" do
          Application.put_env(:ferricstore, :protected_mode, true)

          suffix = System.unique_integer([:positive])
          allowed_type = "tenant-a:policy:#{suffix}"
          denied_type = "tenant-b:policy:#{suffix}"
          now_ms = System.system_time(:millisecond)

          assert {:ok, _policy} =
                   FerricStore.flow_policy_set(allowed_type, max_active_ms: 11_000)

          assert {:ok, _policy} =
                   FerricStore.flow_policy_set(denied_type, max_active_ms: 22_000)

          for {id, type} <- [
                {"policy-allowed-#{suffix}", allowed_type},
                {"policy-denied-#{suffix}", denied_type}
              ] do
            record = %{
              id: id,
              type: type,
              state: "running",
              version: 1,
              attempts: 1,
              fencing_token: 1,
              created_at_ms: now_ms,
              updated_at_ms: now_ms,
              next_run_at_ms: now_ms,
              priority: 0,
              partition_key: type,
              run_state: "running"
            }

            assert :ok =
                     Ferricstore.Store.Router.put(
                       FerricStore.Instance.get(:default),
                       Ferricstore.Flow.Keys.state_key(id, type),
                       Ferricstore.Flow.encode_record(record),
                       0
                     )
          end

          ShardHelpers.eventually(
            fn -> Dashboard.collect_flow_policies_page().total_sampled == 2 end,
            "expected Flow policy dashboard sample to include setup records",
            50,
            20
          )

          :ok =
            FerricstoreServer.Acl.set_user("tenant-a-policy-dashboard", [
              "on",
              ">secret",
              "%R~tenant-a:policy:*",
              "-@all",
              "+FLOW.POLICY.GET"
            ])

          ShardHelpers.eventually(
            fn ->
              Dashboard.collect_flow_policies_page(acl_username: "tenant-a-policy-dashboard").policies
              |> Enum.any?(&(&1.type == allowed_type))
            end,
            "expected allowed Flow policy type to become visible",
            50,
            20
          )

          data =
            Dashboard.collect_flow_policies_page(
              acl_username: "tenant-a-policy-dashboard",
              edit_type: denied_type
            )

          types = Enum.map(data.policies, & &1.type)
          assert allowed_type in types
          refute denied_type in types
          assert data.editor.type == ""
          assert data.policy_scan == %{restricted: true}
          assert data.total_sampled == 1

          login =
            http_post_form(HealthEndpoint.port(), "/dashboard/login", %{
              "username" => "tenant-a-policy-dashboard",
              "password" => "secret"
            })

          query = URI.encode_query(%{"edit" => denied_type})

          page =
            http_get(HealthEndpoint.port(), "/dashboard/flow/policies?#{query}", [
              {"Cookie", dashboard_session_cookie(login)}
            ])

          body = extract_body(page)
          assert extract_status_code(page) == 200
          assert body =~ allowed_type
          refute body =~ denied_type
          assert body =~ "limited to authorized Flow types"
          refute body =~ "entries inspected"
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

          for path <- [
                "/dashboard/api/flow/projections",
                "/dashboard/api/flow/projections?partition_key=tenant-a"
              ] do
            response =
              http_get(HealthEndpoint.port(), path, [
                {"Cookie", dashboard_session_cookie(login)}
              ])

            assert extract_status_code(response) == 404
            assert Jason.decode!(extract_body(response)) == %{"error" => "not found"}
          end

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
          assert String.contains?(html, "Redis-compatible")

          assert Enum.all?(
                   data.config_parameters,
                   &(&1.scope in ["runtime", "Redis-compatible", "current node"])
                 )
        end

        test "config sub-page does not advertise unsupported runtime parameters" do
          data = Dashboard.collect_config_page()

          runtime_parameters =
            Enum.reject(data.config_parameters, &(&1.scope == "current node"))

          for %{parameter: parameter} <- runtime_parameters do
            assert [{^parameter, _value}] = Ferricstore.Config.get(parameter)
          end

          refute Enum.any?(runtime_parameters, &(&1.parameter == "port"))
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
