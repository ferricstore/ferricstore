defmodule FerricstoreServer.Health.Dashboard.LineageReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Access
  alias FerricstoreServer.Health.Dashboard.Render.FlowTables.Lineage

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :flow_dashboard_flow_query_fun)
    on_exit(fn -> restore_env(:flow_dashboard_flow_query_fun, previous) end)
    :ok
  end

  test "lineage retains allowlisted page and quality metadata without additional queries" do
    parent = self()

    stub_query(fn query, params ->
      send(parent, {:query, query, params})

      {:ok,
       %{
         records: [record(1)],
         page: %{has_more: true, cursor: "next&<page>", private: "secret"},
         quality: %{coverage: "partial", freshness: "lagging", private: "secret"}
       }}
    end)

    data = collect(limit: 1)
    assert data.result.page == %{has_more: true, cursor: "next&<page>"}
    assert data.result.quality == %{coverage: "partial", freshness: "lagging"}
    assert_receive {:query, query, _params}
    assert query =~ "LIMIT 1"
    refute_receive {:query, _, _}

    html = Dashboard.render_flow_lineage_page(data)
    assert html =~ "partial"
    assert html =~ "lagging"
    assert html =~ "Next page"
    refute html =~ "secret"
  end

  test "lineage accepts the string-keyed query envelope" do
    stub_query(fn _, _ ->
      {:ok,
       %{
         "records" => [record(1)],
         "page" => %{"has_more" => false},
         "quality" => %{"coverage" => "complete"}
       }}
    end)

    data = collect()
    assert data.result.status == :ok
    assert data.records == [record(1)]
    assert data.result.page == %{has_more: false}
    refute Dashboard.render_flow_lineage_page(data) =~ "Next page"
  end

  test "continuation keeps scope and passes an opaque cursor as a bound parameter" do
    parent = self()
    cursor = "signed+token&\"<next>"

    query =
      URI.encode_query(%{
        id: "root/1",
        mode: "parent",
        partition_key: "p & north",
        limit: 2,
        cursor: cursor
      })

    opts = Dashboard.flow_lineage_opts_from_query(query)
    assert opts[:cursor] == cursor

    stub_query(fn fql, params ->
      send(parent, {:continued, fql, params})
      {:ok, %{records: [], page: %{has_more: false}}}
    end)

    data = Dashboard.collect_flow_lineage_page(opts)
    assert_receive {:continued, fql, params}
    assert fql =~ "CURSOR @cursor"
    refute fql =~ cursor
    assert params["cursor"] == cursor
    assert cursor == data.filters.cursor
    html = Dashboard.render_flow_lineage_page(data)
    assert html =~ "First page"
    refute html =~ "Next page"
  end

  test "ACL filtering preserves idle, error and timeout messages" do
    for status <- [:idle, :error, :timeout] do
      result = %{status: status, records: [], command: "FLOW.QUERY", message: "specific reason"}
      assert Access.flow_lineage_filter_result_for_acl(result, "missing-dashboard-user") == result
    end
  end

  test "failure is actionable and cannot render an empty successful query" do
    stub_query(fn _, _ -> {:error, "index <unavailable>"} end)
    data = collect(acl_username: "missing-dashboard-user")
    html = Dashboard.render_flow_lineage_page(data)

    assert data.result.status == :error
    assert html =~ ~s(role="alert")
    assert html =~ "Lineage query failed"
    assert html =~ "Retry query"
    refute html =~ "No lineage records matched"
    refute html =~ "0 visible record(s)"
    refute html =~ ~s(aria-label="Loaded lineage summary")
    refute html =~ "index <unavailable>"
  end

  test "idle and timeout do not render result statistics or a relationship preview" do
    idle = Dashboard.collect_flow_lineage_page()

    for data <- [
          idle,
          %{idle | result: %{status: :timeout, records: [], message: "query timed out"}}
        ] do
      html = Dashboard.render_flow_lineage_page(data)
      refute html =~ ~s(aria-label="Loaded lineage summary")
      refute html =~ "Relationship preview"
      refute html =~ "No lineage records matched"
    end
  end

  test "an ACL-empty page with continuation does not claim no matches" do
    stub_query(fn _, _ ->
      {:ok, %{records: [record(1)], page: %{has_more: true, cursor: "next"}}}
    end)

    data = collect(acl_username: "missing-dashboard-user")
    assert data.records == []
    html = Dashboard.render_flow_lineage_page(data)
    assert html =~ "No visible records on this page"
    assert html =~ "Next page"
    refute html =~ "No lineage records matched"
  end

  test "lineage values omitted by projection are not reported as absent" do
    html = Lineage.render_flow_lineage_rows([record(1)])
    assert html =~ "Inspect values"
    assert html =~ "/dashboard/flow/child-1?partition_key=p"
    refute html =~ ">none<"
  end

  test "available value references still link to the explicit detail value control" do
    html = Lineage.render_flow_lineage_rows([Map.put(record(1), :payload_ref, "payload:1")])
    assert html =~ "Open payload value"
    assert html =~ "#flow-value-"
  end

  test "relationship preview discloses its cap while the table keeps all loaded records" do
    records = Enum.map(1..45, &record/1)
    nodes = Lineage.render_flow_lineage_nodes(records, %{})
    assert length(Regex.scan(~r/class="flow-lineage-node /, nodes)) == 40
    assert nodes =~ "40 of 45 loaded records"
    assert nodes =~ ~s(href="#flow-lineage-records")
    assert Lineage.render_flow_lineage_rows(records) =~ "child-45"
  end

  test "successful lineage uses a compact summary and puts the table before the preview" do
    stub_query(fn _, _ -> {:ok, %{records: [record(1)], page: %{has_more: false}}} end)
    html = collect() |> Dashboard.render_flow_lineage_page()
    assert html =~ ~s(aria-label="Loaded lineage summary")
    refute html =~ ~s(class="flow-card-grid")
    {table, _} = :binary.match(html, "Lineage Records")
    {preview, _} = :binary.match(html, "Relationship preview")
    assert table < preview
    assert html =~ ~s(class="flow-lineage-id-field")
  end

  test "recovery retains per-source query metadata and warns without chasing pages" do
    parent = self()

    stub_query(fn query, _ ->
      send(parent, {:recovery_query, query})

      {:ok,
       %{
         records: [],
         page: %{has_more: true, cursor: "next"},
         quality: %{coverage: "partial", freshness: "lagging"}
       }}
    end)

    data =
      Dashboard.collect_flow_failures_page(type: "test", partition_key: "p", scan_exact: true)

    assert data.exact_scan_results.failures.page.has_more
    assert data.exact_scan_results.stuck.quality.freshness == "lagging"
    assert data.exact_scan_status == %{failures: :ok, stuck: :ok}
    assert_receive {:recovery_query, _}
    assert_receive {:recovery_query, _}
    refute_receive {:recovery_query, _}
    html = Dashboard.render_flow_failures_page(data)
    assert html =~ "More matching records exist"
    assert html =~ "partial"
    assert html =~ "lagging"
    assert html =~ "Query Studio"
  end

  test "manual signals scan shows its captured UTC time and stays unpolled" do
    data = Dashboard.collect_flow_signals_page(type: "no-such-type", scan_history: true)
    html = Dashboard.render_flow_signals_page(%{data | generated_at_ms: 1_700_000_000_123})
    assert html =~ "Scan captured"
    assert html =~ "2023-11-14 22:13:20.123 UTC"
    assert html =~ ~s(data-dashboard-live-url="")
  end

  test "HTTP lineage pages traverse a real query cursor without duplicates or payload reads" do
    Application.delete_env(:ferricstore, :flow_dashboard_flow_query_fun)
    previous_protected = Application.get_env(:ferricstore, :protected_mode)
    previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
    Application.put_env(:ferricstore, :protected_mode, false)
    parent = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn _, _ ->
      send(parent, :unexpected_record_read)
      {:error, :unexpected}
    end)

    on_exit(fn ->
      restore_env(:protected_mode, previous_protected)
      restore_env(:flow_dashboard_flow_get_fun, previous_get)
    end)

    suffix = System.unique_integer([:positive])
    root = "lineage-http-root-#{suffix}"
    partition = "lineage-http-partition-#{suffix}"
    ids = Enum.map(1..3, &"lineage-http-child-#{suffix}-#{&1}")

    for id <- ids do
      assert :ok =
               FerricStore.flow_create(id,
                 type: "lineage-http",
                 state: "queued",
                 partition_key: partition,
                 root_flow_id: root,
                 parent_flow_id: root,
                 payload_ref: "unloaded:#{id}"
               )
    end

    path =
      "/dashboard/flow/lineage?" <>
        URI.encode_query(%{id: root, mode: "root", partition_key: partition, limit: 1})

    port = FerricstoreServer.Health.Endpoint.port()

    {seen, _path} =
      Enum.reduce(1..3, {[], path}, fn page_number, {seen, path} ->
        response = http_get(port, path)
        assert extract_status_code(response) == 200
        body = extract_body(response)

        [_, table] =
          Regex.run(~r/aria-label="Workflow lineage records".*?<tbody>(.*?)<\/tbody>/s, body)

        shown = Enum.filter(ids, &String.contains?(table, &1))
        assert length(shown) == 1

        next = next_page(body)

        if page_number < 3 do
          assert is_binary(next)
          params = URI.decode_query(URI.parse(next).query)
          assert params["id"] == root
          assert params["partition_key"] == partition
          assert params["limit"] == "1"
          assert params["cursor"] != ""
        else
          assert next == nil
        end

        {seen ++ shown, next}
      end)

    assert Enum.sort(seen) == Enum.sort(ids)
    refute_receive :unexpected_record_read
  end

  test "pagination links escape opaque tokens and search forms reset continuation" do
    stub_query(fn _, _ -> {:ok, %{records: [], page: %{has_more: true, cursor: "a&\"<b>+c"}}} end)

    data =
      collect(target: "root & north", partition_key: "p & east", mode: "correlation", limit: 2)

    html = Dashboard.render_flow_lineage_page(data)
    query = next_page(html) |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()

    assert query == %{
             "id" => "root & north",
             "partition_key" => "p & east",
             "mode" => "correlation",
             "limit" => "2",
             "cursor" => "a&\"<b>+c"
           }

    refute html =~ ~s(name="cursor")
    refute html =~ "a&\"<b>+c"
  end

  test "query response without a usable cursor discloses the bounded result" do
    for cursor <- [nil, ""] do
      stub_query(fn _, _ ->
        {:ok, %{records: [record(1)], page: %{has_more: true, cursor: cursor}}}
      end)

      html = collect() |> Dashboard.render_flow_lineage_page()
      assert html =~ "no continuation is available"
      refute html =~ "Next page"
    end
  end

  test "lineage errors preserve actionable diagnostics without exposing internal paths" do
    stub_query(fn _, _ -> {:error, {:lmdb_corruption, "/private/data/file.mdb"}} end)
    html = collect() |> Dashboard.render_flow_lineage_page()
    refute html =~ "/private/data"
    assert html =~ "Query service unavailable"

    for reason <- [:query_cursor_invalid, :query_cursor_expired] do
      stub_query(fn _, _ -> {:error, reason} end)
      html = collect(cursor: "invalid") |> Dashboard.render_flow_lineage_page()
      assert html =~ "restart from the first page"
      assert html =~ "First page"
      refute html =~ "No lineage records matched"
    end
  end

  test "ACL pagination keeps authorized records only and preserves the continuation" do
    username = "lineage-reader-#{System.unique_integer([:positive])}"

    :ok =
      FerricstoreServer.Acl.set_user(username, ["on", "nopass", "%R~p", "-@all", "+FLOW.QUERY"])

    on_exit(fn -> FerricstoreServer.Acl.del_user(username) end)

    hidden = %{record(2) | partition_key: "denied-partition", type: "hidden-type"}

    stub_query(fn _, _ ->
      {:ok, %{records: [record(1), hidden], page: %{has_more: true, cursor: "next"}}}
    end)

    data = collect(acl_username: username)
    assert data.records == [record(1)]
    assert data.result.message == "1 visible record(s)"
    assert data.result.page.cursor == "next"
    assert data.summary.total == 1
    html = Dashboard.render_flow_lineage_page(data)
    refute html =~ "hidden-type"
    refute html =~ "denied-partition"
    assert html =~ "Next page"
  end

  defp next_page(html) do
    case Regex.run(~r/rel="next" href="([^"]+)"/, html) do
      [_, href] -> String.replace(href, "&amp;", "&")
      _ -> nil
    end
  end

  defp collect(opts \\ []) do
    Dashboard.collect_flow_lineage_page(
      Keyword.merge([target: "root", partition_key: "p", limit: 40], opts)
    )
  end

  defp stub_query(fun), do: Application.put_env(:ferricstore, :flow_dashboard_flow_query_fun, fun)

  defp record(n) do
    %{
      id: "child-#{n}",
      type: "test",
      state: "queued",
      partition_key: "p",
      parent_flow_id: "root",
      root_flow_id: "root",
      updated_at_ms: 1_700_000_000_000
    }
  end
end
