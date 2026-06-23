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
      end
    end
  end
end
