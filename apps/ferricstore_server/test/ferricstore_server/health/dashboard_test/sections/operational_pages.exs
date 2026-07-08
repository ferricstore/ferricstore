defmodule FerricstoreServer.Health.DashboardTest.Sections.OperationalPages do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Health.Dashboard
      alias FerricstoreServer.Health.Endpoint, as: HealthEndpoint

      describe "operational dashboard pages" do
        test "renders operational collection pages" do
          port = HealthEndpoint.port()

          for {path, title} <- [
                {"/dashboard/slowlog", "Slow Log"},
                {"/dashboard/merge", "Merge Status"},
                {"/dashboard/config", "Config"},
                {"/dashboard/clients", "Clients"},
                {"/dashboard/storage", "Storage"},
                {"/dashboard/raft", "Raft"},
                {"/dashboard/security", "Security"},
                {"/dashboard/capabilities", "Capabilities"},
                {"/dashboard/streams", "Streams"},
                {"/dashboard/pubsub", "Pub/Sub"},
                {"/dashboard/prefixes", "Prefixes"}
              ] do
            response = http_get(port, path)

            assert response =~ "HTTP/1.1 200 OK"
            assert response =~ "text/html"
            assert response |> extract_body() |> String.contains?(title)
          end
        end

        test "collects client data from native protocol listeners only" do
          data = Dashboard.collect_clients_page()

          assert is_map(data)
          assert is_list(data.clients)
          assert is_integer(data.connections.active)
          assert is_integer(data.connections.blocked)
          assert is_integer(data.connections.tracking)
        end

        test "renders clients page with native protocol connection language" do
          html =
            Dashboard.collect_clients_page()
            |> Dashboard.render_clients_page()

          assert String.contains?(html, "Clients")
          assert String.contains?(html, "Connections")
          refute String.contains?(html, "RE" <> "SP")
        end

        test "renders raft and storage pages from current operational collectors" do
          raft_html =
            Dashboard.collect_raft_page()
            |> Dashboard.render_raft_page()

          storage_html =
            Dashboard.collect_storage_page()
            |> Dashboard.render_storage_page()

          assert String.contains?(raft_html, "Raft")
          assert String.contains?(storage_html, "Storage")
        end

        test "renders management capabilities without mutation forms" do
          html =
            Dashboard.collect_capabilities_page()
            |> Dashboard.render_capabilities_page()

          assert String.contains?(html, "Management Capabilities")
          assert String.contains?(html, "FERRICSTORE.CAPABILITIES")
          assert String.contains?(html, "flow_observability")
          assert String.contains?(html, "unsupported")
          refute String.contains?(html, ~s(method="post"))
          refute String.contains?(html, "COMMAND_EXEC")
        end

        test "command catalog exposes messaging anchors" do
          html =
            Dashboard.collect_commands_page()
            |> Dashboard.render_commands_page()

          assert String.contains?(html, ~s(id="streams"))
          assert String.contains?(html, "XREADGROUP")
          assert String.contains?(html, ~s(id="pubsub"))
          assert String.contains?(html, "PUBLISH")
        end

        test "stream activity page shows append metadata without payload fields" do
          Ferricstore.Stream.ActivityLog.reset()

          {:ok, entry_id} =
            FerricStore.xadd("dashboard:stream:activity", [
              "secret_field",
              "secret-value"
            ])

          html =
            Dashboard.collect_streams_page()
            |> Dashboard.render_streams_page()

          assert String.contains?(html, "Stream Activity")
          assert String.contains?(html, "dashboard:stream:activity")
          assert String.contains?(html, entry_id)
          assert String.contains?(html, "1 field pairs")
          refute String.contains?(html, "secret_field")
          refute String.contains?(html, "secret-value")
        end

        test "stream activity page shows consumer metadata without payload fields" do
          Ferricstore.Stream.ActivityLog.reset()

          key = "dashboard:stream:consumer:#{System.unique_integer([:positive])}"
          store = FerricStore.API.Store.build_stream_store(key)

          assert {:ok, entry_id} = FerricStore.xadd(key, ["payload_field", "payload-value"])

          assert :ok =
                   Ferricstore.Commands.Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

          assert [[^key, [[^entry_id | _fields]]]] =
                   Ferricstore.Commands.Stream.handle(
                     "XREADGROUP",
                     ["GROUP", "g1", "c1", "STREAMS", key, ">"],
                     store
                   )

          assert 1 == Ferricstore.Commands.Stream.handle("XACK", [key, "g1", entry_id], store)

          html =
            Dashboard.collect_streams_page()
            |> Dashboard.render_streams_page()

          assert String.contains?(html, "Stream Consumers")
          assert String.contains?(html, "XREADGROUP")
          assert String.contains?(html, "XACK")
          assert String.contains?(html, "g1 / c1")
          assert String.contains?(html, "consumer")
          refute String.contains?(html, "payload_field")
          refute String.contains?(html, "payload-value")
        end

        test "stream activity live payload renders recent append components" do
          Ferricstore.Stream.ActivityLog.reset()

          {:ok, _entry_id} = FerricStore.xadd("dashboard:stream:live", ["kind", "created"])

          assert {:ok, payload} = Dashboard.live_payload("streams")
          assert is_integer(payload.generated_at_ms)
          assert Map.has_key?(payload.components, "streams_summary")
          assert payload.components["streams_log"] =~ "dashboard:stream:live"
          refute payload.components["streams_log"] =~ "created"
        end

        test "pubsub page shows subscriptions and publish metadata without payload bodies" do
          Ferricstore.PubSub.ActivityLog.reset()

          channel = "dashboard:pubsub:#{System.unique_integer([:positive])}"
          pattern = "dashboard:pubsub:*"

          :ok = Ferricstore.PubSub.subscribe(channel, self())
          :ok = Ferricstore.PubSub.psubscribe(pattern, self())

          on_exit(fn ->
            Ferricstore.PubSub.unsubscribe(channel, self())
            Ferricstore.PubSub.punsubscribe(pattern, self())
          end)

          assert 2 == Ferricstore.Commands.PubSub.handle("PUBLISH", [channel, "secret-message"])

          html =
            Dashboard.collect_pubsub_page()
            |> Dashboard.render_pubsub_page()

          assert String.contains?(html, "Pub/Sub Activity")
          assert String.contains?(html, channel)
          assert String.contains?(html, pattern)
          assert String.contains?(html, "PUBLISH")
          assert String.contains?(html, "SUBSCRIBE")
          assert String.contains?(html, "receiver")
          refute String.contains?(html, "secret-message")
        end
      end
    end
  end
end
