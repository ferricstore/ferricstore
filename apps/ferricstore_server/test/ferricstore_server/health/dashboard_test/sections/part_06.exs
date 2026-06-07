defmodule FerricstoreServer.Health.DashboardTest.Sections.Part06 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Test.ShardHelpers

  describe "collect_flow_detail_page/1 part 1" do
    test "renders Flow detail with waiting reason and history timeline" do
      id = "dashboard-flow-detail-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: "dashboard-detail",
                 state: "queued",
                 run_at_ms: System.system_time(:millisecond) + 60_000
               )

      data = Dashboard.collect_flow_detail_page(id)
      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, id)
      assert String.contains?(html, "Why waiting")
      assert String.contains?(html, "Timeline")
      assert String.contains?(html, "scheduled for future")
    end

    test "Flow detail timeline renders transition, retry, and terminal events" do
      flow_type = "dashboard-history-#{System.unique_integer([:positive])}"
      partition_key = "tenant-history-#{System.unique_integer([:positive])}"
      id = "dashboard-history-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_transition(id, "queued", "ready",
                 partition_key: partition_key,
                 fencing_token: 0,
                 run_at_ms: 1_100,
                 now_ms: 1_100
               )

      assert {:ok, [first_claim]} =
               FerricStore.flow_claim_due(flow_type,
                 state: "ready",
                 partition_key: partition_key,
                 worker: "worker-a",
                 lease_ms: 30_000,
                 limit: 1,
                 now_ms: 1_200
               )

      assert :ok =
               FerricStore.flow_retry(id, first_claim.lease_token,
                 partition_key: partition_key,
                 fencing_token: first_claim.fencing_token,
                 error: "temporary failure",
                 run_at_ms: 1_300,
                 now_ms: 1_250
               )

      assert {:ok, retried} = FerricStore.flow_get(id, partition_key: partition_key)

      assert {:ok, [second_claim]} =
               FerricStore.flow_claim_due(flow_type,
                 state: retried.state,
                 partition_key: partition_key,
                 worker: "worker-a",
                 lease_ms: 30_000,
                 limit: 1,
                 now_ms: 1_400
               )

      assert :ok =
               FerricStore.flow_complete(id, second_claim.lease_token,
                 partition_key: partition_key,
                 fencing_token: second_claim.fencing_token,
                 result: "ok",
                 now_ms: 1_500
               )

      html =
        id
        |> Dashboard.collect_flow_detail_page(partition_key: partition_key)
        |> Dashboard.render_flow_detail_page()

      assert String.contains?(html, "Created")
      assert String.contains?(html, "Transitioned")
      assert String.contains?(html, "Retry")
      assert String.contains?(html, "Completed")
      assert String.contains?(html, "queued -&gt; ready")
      assert String.contains?(html, "terminal")
      assert String.contains?(html, "worker-a")
      assert String.contains?(html, "result_ref")
      assert String.contains?(html, "error_ref")
      assert String.contains?(html, ~s(href="#flow-event-))
      assert String.contains?(html, ~s(id="flow-event-))
      assert String.contains?(html, "ready")
      assert String.contains?(html, "State graph")
      assert String.contains?(html, ~s(class="flow-timeline-graph"))
      assert String.contains?(html, "<svg")
      assert String.contains?(html, ~s(class="flow-timeline-path"))
      assert String.contains?(html, ~s(class="flow-timeline-duration-segment))
      assert String.contains?(html, ~s(class="flow-timeline-node))
      assert String.contains?(html, ~s(class="flow-timeline-lane-label"))
      assert String.contains?(html, ~s(class="flow-timeline-node flow-timeline-node-retry"))
      assert String.contains?(html, ~s(class="flow-timeline-duration-segment bar-red"))
      assert Regex.match?(~r/class="flow-timeline-node-label"[^>]*>ready<\/text>/, html)
      assert String.contains?(html, "running")
      assert String.contains?(html, "100ms")

      refute Regex.match?(
               ~r/class="flow-timeline-lane-label"[^>]*>Retry<\/text>/,
               html
             )

      refute Regex.match?(
               ~r/class="flow-timeline-node-label"[^>]*>Transitioned<\/text>/,
               html
             )

      refute String.contains?(
               html,
               ~s(class="timeline-chip"><span class="timeline-chip-index">1</span>Transitioned)
             )
    end

    test "Flow detail renders rewind targets from existing history states only" do
      data = %{
        id: "dashboard-rewind-flow",
        partition_key: "tenant-rewind",
        record: %{
          id: "dashboard-rewind-flow",
          type: "dashboard-rewind",
          state: "completed",
          partition_key: "tenant-rewind",
          run_at_ms: 1_000,
          updated_at_ms: 3_000,
          fencing_token: 3
        },
        record_status: :ok,
        history: [
          {"1000-1", %{"event" => "created", "state" => "queued"}},
          {"2000-2", %{"event" => "transitioned", "state" => "ready"}},
          {"3000-3", %{"event" => "completed", "state" => "completed"}},
          {"4000-4", %{"event" => "note"}}
        ],
        history_status: :ok,
        history_page: nil,
        value_refs: [],
        values_by_ref: %{},
        values_status: :skipped,
        waiting_reason: "terminal"
      }

      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, "Rewind")
      assert String.contains?(html, ~s(action="/dashboard/flow/dashboard-rewind-flow/rewind"))
      assert String.contains?(html, ~s(name="to_event"))
      assert String.contains?(html, ~s(value="1000-1"))
      assert String.contains?(html, "queued")
      assert String.contains?(html, ~s(value="2000-2"))
      assert String.contains?(html, "ready")
      refute String.contains?(html, ~s(value="4000-4"))
      refute String.contains?(html, ~s(name="state"))
      assert String.contains?(html, ~s(name="partition_key" value="tenant-rewind"))
      assert String.contains?(html, "Rewind creates a durable FLOW.REWIND")

      assert String.contains?(
               html,
               ~s(title="Choose one of this flow&#39;s loaded history events")
             )

      assert String.contains?(html, ~s(title="Create a durable rewind to the selected event"))
    end

    test "Flow detail renders signal events as a focused section" do
      data = %{
        id: "dashboard-signal-flow",
        partition_key: "tenant-signal",
        record: %{
          id: "dashboard-signal-flow",
          type: "dashboard-signal",
          state: "verify_payment",
          partition_key: "tenant-signal",
          run_at_ms: 1_250,
          updated_at_ms: 1_100,
          fencing_token: 1
        },
        record_status: :ok,
        history: [
          {"1000-1", %{"event" => "created", "state" => "waiting_payment"}},
          {"1100-2",
           %{
             "event" => "signaled",
             "signal" => "payment_received",
             "from_state" => "waiting_payment",
             "state" => "verify_payment",
             "value_refs" => ~s({"payment_event":{"ref":"ref:payment","version":1}})
           }}
        ],
        history_status: :ok,
        history_page: nil,
        value_refs: [],
        values_by_ref: %{},
        values_status: :skipped,
        waiting_reason: "due"
      }

      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, "Signals")
      assert String.contains?(html, "payment_received")
      assert String.contains?(html, "waiting_payment -> verify_payment")
      assert String.contains?(html, "payment_event")
      assert String.contains?(html, ~s(href="#flow-event-))
    end

    test "dashboard rewind form restores a selected existing history state" do
      flow_type = "dashboard-rewind-apply-#{System.unique_integer([:positive])}"
      partition_key = "tenant-rewind-#{System.unique_integer([:positive])}"
      id = "dashboard-rewind-apply-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 partition_key: partition_key,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert {:ok, [{created_event_id, %{"state" => "queued"}} | _]} =
               FerricStore.flow_history(id, partition_key: partition_key, count: 10)

      assert :ok =
               FerricStore.flow_transition(id, "queued", "ready",
                 partition_key: partition_key,
                 fencing_token: 0,
                 run_at_ms: 2_000,
                 now_ms: 2_000
               )

      assert {:ok, ready} = FerricStore.flow_get(id, partition_key: partition_key)
      assert ready.state == "ready"

      assert {:ok, ^id, ^partition_key} =
               Dashboard.apply_flow_rewind_form(%{
                 "id" => id,
                 "partition_key" => partition_key,
                 "to_event" => created_event_id,
                 "confirm_rewind" => "true"
               })

      assert {:ok, rewound} = FerricStore.flow_get(id, partition_key: partition_key)
      assert rewound.state == "queued"
      assert rewound.rewound_to_event_id == created_event_id
    end

    test "dashboard rewind form rejects target events outside existing history" do
      flow_type = "dashboard-rewind-reject-#{System.unique_integer([:positive])}"
      id = "dashboard-rewind-reject-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert {:error, reason} =
               Dashboard.apply_flow_rewind_form(%{
                 "id" => id,
                 "to_event" => "999999-9",
                 "confirm_rewind" => "true"
               })

      assert reason =~ "target event"

      assert {:ok, record} = FerricStore.flow_get(id)
      assert record.state == "queued"
      assert Map.get(record, :rewound_to_event_id) in [nil, ""]
    end

    test "dashboard rewind form rejects same-type events from another flow" do
      flow_type = "dashboard-rewind-same-type-#{System.unique_integer([:positive])}"
      this_id = "dashboard-rewind-this-flow-#{System.unique_integer([:positive])}"
      other_id = "dashboard-rewind-other-flow-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(this_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(other_id,
                 type: flow_type,
                 state: "queued",
                 run_at_ms: 2_000,
                 now_ms: 2_000
               )

      assert :ok =
               FerricStore.flow_transition(other_id, "queued", "approval",
                 fencing_token: 0,
                 run_at_ms: 3_000,
                 now_ms: 3_000
               )

      assert {:ok, other_history} = FerricStore.flow_history(other_id, count: 10)

      {other_transition_event_id, _fields} =
        Enum.find(other_history, fn {_event_id, fields} ->
          fields["event"] == "transitioned" and fields["state"] == "approval"
        end)

      assert {:error, reason} =
               Dashboard.apply_flow_rewind_form(%{
                 "id" => this_id,
                 "to_event" => other_transition_event_id,
                 "confirm_rewind" => "true"
               })

      assert reason =~ "target event"

      assert {:ok, record} = FerricStore.flow_get(this_id)
      assert record.state == "queued"
      assert Map.get(record, :rewound_to_event_id) in [nil, ""]
    end

    test "Flow detail value refs open modal without rendering a visible inspector section" do
      id = "dashboard-flow-values/#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      previous_values = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, opts ->
        send(test_pid, {:dashboard_get_opts, opts})

        {:ok,
         %{
           id: id,
           type: "dashboard-values",
           state: "queued",
           partition_key: "tenant-values",
           run_at_ms: 1_000,
           updated_at_ms: 1_000,
           payload_ref: "flow-value:payload",
           value_refs: %{"invoice" => "flow-value:invoice"}
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        {:ok,
         [
           {"1000-1",
            %{
              "event" => "created",
              "to_state" => "queued",
              "payload_ref" => "flow-value:payload"
            }}
         ]}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
        send(test_pid, {:dashboard_value_refs, refs})
        {:ok, [%{"message" => "payload-value"}, %{"invoice_id" => "invoice-42"}]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
        restore_env(:flow_dashboard_flow_value_mget_fun, previous_values)
      end)

      data = Dashboard.collect_flow_detail_page(id)
      html = Dashboard.render_flow_detail_page(data)

      assert_receive {:dashboard_get_opts, get_opts}
      assert get_opts[:payload] == false

      assert_receive {:dashboard_value_refs, ["flow-value:payload", "flow-value:invoice"]}
      assert String.contains?(html, "Value Inspector")
      refute String.contains?(html, ~s(data-live-component="flow_values"))
      assert String.contains?(html, ~s(data-live-component="flow_value_store"))
      assert String.contains?(html, ~s(href="#flow-value-))
      assert String.contains?(html, ~s(id="flow-value-))
      assert String.contains?(html, ~s(id="flow-value-store" hidden aria-hidden="true"))
      assert String.contains?(html, ~s(data-flow-value-ref="flow-value:payload"))
      assert String.contains?(html, ~s(data-flow-value-preview))
      refute String.contains?(html, "Value Preview")
      refute String.contains?(html, "Payload/result/error refs are clickable.")
      assert String.contains?(html, ~s(id="flow-value-modal"))
      assert String.contains?(html, ~s(id="flow-value-modal-copy"))
      assert String.contains?(html, "openFromHash")
      assert String.contains?(html, "flowValueRequestUrl")
      assert String.contains?(html, "flowValueAnchorFromHref")
      assert String.contains?(html, "openFromRef")
      assert String.contains?(html, "payload-value")
      assert String.contains?(html, "invoice-42")
      assert String.contains?(html, "flow-value:payload")
    end

    test "Flow value live payload fetches one authorized value on demand" do
      id = "dashboard-flow-value-api/#{System.unique_integer([:positive])}"
      partition_key = "tenant-value-#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      previous_values = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, opts ->
        send(test_pid, {:dashboard_value_api_get_opts, opts})

        {:ok,
         %{
           id: id,
           type: "email",
           state: "queued",
           partition_key: partition_key,
           payload_ref: "flow-value:payload",
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        {:ok, [{"1000-1", %{"event" => "created", "payload_ref" => "flow-value:payload"}}]}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
        send(test_pid, {:dashboard_value_api_refs, refs})
        {:ok, [%{"message" => "payload-value"}]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
        restore_env(:flow_dashboard_flow_value_mget_fun, previous_values)
      end)

      query =
        URI.encode_query(%{
          "flow" => id,
          "partition_key" => partition_key,
          "ref" => "flow-value:payload"
        })

      assert {:ok, payload} = Dashboard.live_payload("flow/value?" <> query)
      assert payload.status == "ok"
      assert payload.ref == "flow-value:payload"
      assert payload.value =~ "payload-value"

      assert_receive {:dashboard_value_api_get_opts, opts}
      assert opts[:partition_key] == partition_key
      assert_receive {:dashboard_value_api_refs, ["flow-value:payload"]}
    end

    test "Flow value live payload refuses refs outside the visible flow detail" do
      id = "dashboard-flow-value-forbidden/#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      previous_values = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)
      test_pid = self()

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, _opts ->
        {:ok,
         %{
           id: id,
           type: "email",
           state: "queued",
           payload_ref: "flow-value:payload",
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        {:ok, []}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
        send(test_pid, {:unexpected_dashboard_value_api_refs, refs})
        {:ok, ["must-not-load"]}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
        restore_env(:flow_dashboard_flow_value_mget_fun, previous_values)
      end)

      query = URI.encode_query(%{"flow" => id, "ref" => "flow-value:other"})

      assert {:ok, payload} = Dashboard.live_payload("flow/value?" <> query)
      assert payload.status == "error"
      assert payload.error =~ "not visible"
      refute_receive {:unexpected_dashboard_value_api_refs, _}
    end

    test "Flow value live payload hides internal lookup error details" do
      id = "dashboard-flow-value-error/#{System.unique_integer([:positive])}"
      previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
      previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
      previous_values = Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun)

      Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn ^id, _opts ->
        {:ok,
         %{
           id: id,
           type: "email",
           state: "queued",
           payload_ref: "flow-value:payload",
           run_at_ms: 1_000,
           updated_at_ms: 1_000
         }}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn ^id, _opts ->
        {:ok, []}
      end)

      Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn _refs ->
        {:error, {:lmdb_corruption, "/secret/data/file.mdb"}}
      end)

      on_exit(fn ->
        restore_env(:flow_dashboard_flow_get_fun, previous_get)
        restore_env(:flow_dashboard_flow_history_fun, previous_history)
        restore_env(:flow_dashboard_flow_value_mget_fun, previous_values)
      end)

      query = URI.encode_query(%{"flow" => id, "ref" => "flow-value:payload"})

      assert {:ok, payload} = Dashboard.live_payload("flow/value?" <> query)
      assert payload.status == "error"
      assert payload.error =~ "value lookup failed"
      refute payload.error =~ "lmdb_corruption"
      refute payload.error =~ "/secret/data/file.mdb"
    end

    test "renders Flow debug inspector with lease, storage, values, and history hints" do
      data = %{
        id: "debug-flow-1",
        waiting_reason: "leased by worker-1",
        record: %{
          id: "debug-flow-1",
          type: "email",
          state: "running",
          partition_key: "tenant-7",
          worker: "worker-1",
          priority: 9,
          attempts: 2,
          max_attempts: 5,
          lease_token: "lease-abc",
          fencing_token: 12,
          run_at_ms: System.system_time(:millisecond) - 1_000,
          lease_expires_at_ms: System.system_time(:millisecond) + 30_000,
          updated_at_ms: System.system_time(:millisecond),
          payload_ref: "flow-value:payload",
          result_ref: "flow-value:result",
          value_refs: %{"invoice" => "flow-value:invoice"}
        },
        history: [
          {1, %{event: "create", to_state: "queued", payload_ref: "flow-value:payload"}},
          {2, %{event: "claim", from_state: "queued", to_state: "running"}}
        ]
      }

      html = Dashboard.render_flow_detail_page(data)

      assert String.contains?(html, "Debug Inspector")
      assert String.contains?(html, "Lease")
      assert String.contains?(html, "running until")
      assert String.contains?(html, "Storage")
      assert String.contains?(html, "tenant-7")
      assert String.contains?(html, "Values")
      assert String.contains?(html, "payload_ref")
      assert String.contains?(html, "invoice")
      assert String.contains?(html, "History")
      assert String.contains?(html, "2 events")
    end

    test "Flow detail treats lease_deadline_ms as lease expiry" do
      deadline = System.system_time(:millisecond) + 30_000

      html =
        Dashboard.render_flow_detail_page(%{
          id: "debug-flow-lease-deadline",
          waiting_reason: "leased by worker-1",
          record: %{
            id: "debug-flow-lease-deadline",
            type: "email",
            state: "running",
            partition_key: "tenant-7",
            lease_owner: "worker-1",
            lease_token: "lease-abc",
            fencing_token: 12,
            run_at_ms: System.system_time(:millisecond) - 1_000,
            lease_deadline_ms: deadline,
            updated_at_ms: System.system_time(:millisecond)
          },
          history: []
        })

      assert String.contains?(html, "running until")
      refute String.contains?(html, "running without lease expiry")
    end

    test "Flow overview exposes a search form for direct flow debugging" do
      html = Dashboard.collect_flow_page() |> Dashboard.render_flow_page()

      assert String.contains?(html, ~s(action="/dashboard/flow/lookup"))
      assert String.contains?(html, ~s(name="id"))
      assert String.contains?(html, ~s(name="partition_key"))
      assert String.contains?(html, "Search flow ID")
      assert String.contains?(html, ~s(title="Open a flow by ID."))

      assert String.contains?(
               html,
               ~s(title="With a Flow ID, scopes the detail lookup. Without a Flow ID, filters the overview to this partition.")
             )

      assert String.contains?(html, ~s(title="Open a flow by ID or filter overview by partition"))
    end

    test "Flow overview can be scoped to a searched partition key" do
      partition_key = "tenant-search-#{System.unique_integer([:positive])}"
      other_partition_key = "tenant-other-#{System.unique_integer([:positive])}"
      matching_id = "dashboard-partition-search-match-#{System.unique_integer([:positive])}"
      hidden_id = "dashboard-partition-search-hidden-#{System.unique_integer([:positive])}"

      assert :ok =
               FerricStore.flow_create(matching_id,
                 type: "dashboard-partition-search",
                 state: "queued",
                 partition_key: partition_key,
                 run_at_ms: 1_000
               )

      assert :ok =
               FerricStore.flow_create(hidden_id,
                 type: "dashboard-partition-search",
                 state: "queued",
                 partition_key: other_partition_key,
                 run_at_ms: 1_000
               )

      data = Dashboard.collect_flow_page(partition_key: partition_key)
      html = Dashboard.render_flow_page(data)

      assert Enum.all?(data.records, &(Map.get(&1, :partition_key) == partition_key))
      assert String.contains?(html, partition_key)
      assert String.contains?(html, matching_id)
      refute String.contains?(html, hidden_id)

      assert String.contains?(
               html,
               "/dashboard/api/flow?#{URI.encode_query(%{"partition_key" => partition_key})}"
             )
    end

    test "Flow overview detail links preserve partition key" do
      id = "dashboard-flow-partitioned/#{System.unique_integer([:positive])}"
      partition_key = "tenant/debug #{System.unique_integer([:positive])}"

      data =
        Dashboard.collect_flow_page()
        |> Map.merge(%{
          summary: %{},
          types: [],
          workers: [],
          records: [
            %{
              id: id,
              type: "dashboard-partitioned",
              state: "queued",
              partition_key: partition_key,
              run_at_ms: 1_000,
              updated_at_ms: 1_000
            }
          ],
          total_sampled: 1,
          sample_limit: 400
        })

      html = Dashboard.render_flow_page(data)
      encoded_id = URI.encode(id, &URI.char_unreserved?/1)
      encoded_partition = URI.encode_query(%{"partition_key" => partition_key})

      assert String.contains?(html, ~s(href="/dashboard/flow/#{encoded_id}?#{encoded_partition}"))
    end

  end
    end
  end
end
