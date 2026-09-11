defmodule FerricstoreServer.Health.Dashboard.SystemSeventhReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Data.{Clients, KV, Operational}
  alias FerricstoreServer.Health.Dashboard.DoctorSupport
  alias FerricstoreServer.Health.Dashboard.LivePayload
  alias FerricstoreServer.Health.Dashboard.Render.{Admin, DoctorPages, KVPages, RecentRates}

  test "1 consensus uses independent server commit and storage applied indices without integer loss" do
    base = 9_007_199_254_740_993

    shard =
      Operational.raft_snapshot(
        0,
        {:ok, [{:raft_0, node()}], {:raft_0, node()}},
        [current_term: 9, commit_index: base + 211, last_applied: base + 200],
        {:ok, {:raft_log_pos, base, 3}}
      )

    assert shard.current_term == 9
    assert shard.commit_index == base + 211
    assert shard.last_applied == base
    assert shard.status == :ok
    html = Admin.render_consensus_summary([shard]) <> Admin.render_raft_table([shard])
    assert html =~ ">211</div>"
    assert html =~ Integer.to_string(base)
    assert html =~ Integer.to_string(base + 211)
  end

  test "1 partial consensus never substitutes zero or advertises a complete maximum" do
    shard =
      Operational.raft_snapshot(
        0,
        {:ok, [], nil},
        [current_term: 9, commit_index: 211],
        {:error, :timeout}
      )

    assert shard.status == :partial
    assert shard.last_applied == nil
    assert shard.commit_index == 211
    html = Admin.render_consensus_summary([shard]) <> Admin.render_raft_table([shard])
    assert html =~ "Unavailable"
    refute html =~ ~r/Max Apply Lag<\/div>\s*<div[^>]*c-green/

    missing =
      Operational.raft_snapshot(1, {:error, :timeout}, {:error, :noproc}, {:error, :timeout})

    assert missing.status == :unavailable
    assert missing.current_term == nil
    assert missing.commit_index == nil
  end

  test "1 collection deadlines bound stalled backends while retaining independent completed reads" do
    owner = self()

    shard =
      Operational.collect_waraft_overview(0,
        members: fn 0 ->
          send(owner, :members_read)
          {:ok, [], nil}
        end,
        status: fn 0 ->
          send(owner, :status_read)
          [current_term: 2, commit_index: 10]
        end,
        position: fn 0 ->
          send(owner, :position_read)

          receive do
            :never -> :ok
          end
        end,
        timeout: 20
      )

    assert shard.status == :partial
    assert shard.commit_index == 10
    assert shard.last_applied == nil
    assert_received :members_read
    assert_received :status_read
    assert_received :position_read
    refute_received :members_read
    refute_received :status_read
    refute_received :position_read
  end

  test "1 consensus leader uses the complete server name and node identity" do
    for {leader_name, expected} <- [
          {:right_member, {:right_member, node()}},
          {:not_a_member, nil}
        ] do
      shard =
        Operational.collect_waraft_overview(0,
          status: fn 0 ->
            [
              current_term: 9,
              commit_index: 10,
              leader_name: leader_name,
              leader_id: node(),
              config: %{
                version: 1,
                membership: [{:wrong_member, node()}, {:right_member, node()}]
              }
            ]
          end,
          position: fn 0 -> {:ok, {:raft_log_pos, 8, 9}} end
        )

      assert shard.leader == expected
      assert shard.status == :ok
    end
  end

  test "10 Doctor CHECK and LIST preserve independent failures and successful evidence" do
    command = fn
      ["CHECK"] -> %{"status" => "error", "error" => "<check timeout>", "checks" => []}
      ["LIST"] -> %{"status" => "error", "error" => "<jobs timeout>"}
    end

    data = DoctorSupport.collect_page(%{}, command)
    html = Dashboard.render_doctor_page(data)
    assert html =~ "&lt;check timeout&gt;"
    assert html =~ "&lt;jobs timeout&gt;"
    assert html =~ "Retry diagnostics"
    refute html =~ "No doctor jobs yet"
    refute html =~ "No doctor checks returned"
    refute DoctorPages.render_doctor_summary(data.check) =~ ">0.0 ms</div>"

    partial =
      DoctorSupport.collect_page(%{}, fn
        ["CHECK"] ->
          %{
            "status" => "ok",
            "checks" => [
              %{"scope" => "bitcask", "status" => "ok", "message" => "evidence survives"}
            ]
          }

        ["LIST"] ->
          %{"error" => "jobs unavailable", "status" => "error"}
      end)

    assert Dashboard.render_doctor_page(partial) =~ "evidence survives"
  end

  test "11 unavailable exact key metadata is unknown, not absent; prefix retains partial rows" do
    table = :ets.new(:keydir_seventh, [:set])
    unavailable = :ets.new(:missing_keydir_seventh, [:set])
    :ets.delete(unavailable)
    :ets.insert(table, {"visible:key", "value", 0, 0, 0, 0, 5})
    unknown = KV.collect_keyspace_page(%{"key" => "missing:key"}, [{0, table}, {1, unavailable}])
    assert unknown.inspected.found? == nil
    assert unknown.collection_status == :partial
    html = Dashboard.render_keyspace_page(unknown)
    assert html =~ "Key metadata unavailable"
    refute html =~ "No live key metadata found"
    refute html =~ "No key metadata matched"
    partial = KV.collect_keyspace_page(%{"prefix" => "visible:"}, [{0, table}, {1, unavailable}])
    assert [%{key: "visible:key"}] = partial.rows
    assert partial.collection_status == :partial
    assert Dashboard.render_keyspace_page(partial) =~ "Partial key metadata"
    missing = KV.collect_keyspace_page(%{"key" => "missing:key"}, [{0, table}])
    assert missing.inspected.found? == false
    assert missing.collection_status == :ok
  end

  test "12 Slow Log failure stays distinct from successful empty and is bounded" do
    assert %{status: :unavailable, entries: [], error: error} =
             Operational.slowlog_snapshot(fn 128 -> exit(:timeout) end)

    assert error =~ "timeout"
    assert %{status: :ok, entries: []} = Operational.slowlog_snapshot(fn 128 -> [] end)
    failure = %{status: :unavailable, entries: [], error: "<slow log unavailable>"}
    html = Admin.render_slowlog_summary(failure) <> Admin.render_slowlog_table(failure)
    assert html =~ "Slow Log unavailable"
    assert html =~ "&lt;slow log unavailable&gt;"
    refute html =~ "No slow commands"
    refute html =~ "No samples"

    assert {:error, :invalid_filters, message} =
             LivePayload.slowlog_payload(%{
               slowlog_status: :unavailable,
               slowlog: [],
               slowlog_error: "timeout"
             })

    assert message =~ "Last successful samples retained"

    assert {:ok, %{components: components}} =
             LivePayload.slowlog_payload(%{slowlog_status: :ok, slowlog: []})

    assert components["slowlog_table"] =~ "No slow commands recorded"
  end

  test "17 unmeasured command latency is not measured zero, including failed collection" do
    empty = KVPages.render_commands_summary(%{summary: %{slowlog_entries: 0, slowest_us: nil}})
    assert empty =~ "No samples"
    refute empty =~ "0.0 ms"
    measured = KVPages.render_commands_summary(%{summary: %{slowlog_entries: 1, slowest_us: 0}})
    assert measured =~ "0.0 ms"

    failed =
      KVPages.render_commands_summary(%{
        summary: %{slowlog_status: :unavailable, slowlog_entries: nil, slowest_us: nil}
      })

    assert failed =~ "Unavailable"
    refute failed =~ "No samples"
  end

  test "27 server-side search reaches clients beyond the old 500-row sample" do
    table = :ets.new(:clients_seventh, [:set])

    for id <- 1..700 do
      :ets.insert(
        table,
        {id, self(),
         %{
           client_id: id,
           client_name: "client-#{id}",
           username: "observer",
           peer: "127.0.0.1",
           created_at_ms: id,
           flags: ""
         }}
      )
    end

    result = Clients.snapshot(%{"q" => "client-699"}, table)
    assert [%{client_id: 699}] = result.clients
    assert result.total_registered == 700
    assert result.complete?
    assert result.scanned_count <= 10_000

    html =
      Admin.render_clients_page_table(%{
        clients: result.clients,
        client_coverage: result,
        client_filters: result.filters
      })

    assert html =~ "1 shown"
    assert html =~ "700 registered"
    assert html =~ ~r/<input[^>]*name="q"[^>]*value="client-699"/
    assert html =~ "Search connections"
  end

  test "27 bounded client pages continue without materializing all rows or sending process calls" do
    table = :ets.new(:clients_paging_seventh, [:set])

    for id <- 1..10_501 do
      :ets.insert(
        table,
        {id, self(), %{client_id: id, client_name: "client-#{id}", created_at_ms: id, flags: ""}}
      )
    end

    first = Clients.snapshot(%{}, table)
    assert length(first.clients) == 500
    assert first.scanned_count <= 10_000
    assert first.next_cursor != nil
    next = Clients.snapshot(%{"cursor" => first.next_cursor}, table)

    assert MapSet.disjoint?(
             MapSet.new(first.clients, & &1.client_id),
             MapSet.new(next.clients, & &1.client_id)
           )

    exhausted = Clients.snapshot(%{"q" => "not-present"}, table)
    assert exhausted.scanned_count == 10_000
    refute exhausted.complete?
    assert exhausted.next_cursor != nil
    refute_received {:"$gen_call", _, _}
    :ets.delete(table)
    assert Clients.snapshot(%{}, table).status == :unavailable
  end

  test "27 a disconnected continuation on the actual set registry offers explicit restart" do
    table = :ets.new(:clients_expired_seventh, [:set])
    for id <- 1..501, do: :ets.insert(table, {id, self(), %{client_name: "client-#{id}"}})
    first = Clients.snapshot(%{}, table)
    cursor = first.next_cursor
    :ets.delete(table, String.to_integer(cursor))
    expired = Clients.snapshot(%{"q" => "client", "cursor" => cursor}, table)
    assert expired.status == :expired_cursor
    assert expired.filters.q == "client"
    refute expired.complete?

    html =
      Admin.render_clients_page_table(%{
        clients: expired.clients,
        client_coverage: expired,
        client_filters: expired.filters
      })

    assert html =~ "Continuation expired"
    assert html =~ "Restart search"
    refute html =~ "No matching connections"
    assert :ets.info(table, :safe_fixed) == false
  end

  test "27 concurrent deletion during bounded set traversal retains collected rows and always unfixes" do
    table = :ets.new(:clients_deletion_seventh, [:set])
    for id <- 1..12, do: :ets.insert(table, {id, self(), %{client_name: "client-#{id}"}})
    observer = fn {:before_next, id} -> :ets.delete(table, id) end
    result = Clients.snapshot(%{}, table, observer)
    assert result.status == :ok
    assert length(result.clients) == 12
    assert result.complete?
    assert :ets.info(table, :safe_fixed) == false
    assert :ets.info(table, :size) == 0
    :ets.insert(table, {99, self(), %{client_name: "failure-release"}})
    failed = Clients.snapshot(%{}, table, fn _event -> exit(:injected_failure) end)
    assert failed.status == :unavailable
    assert :ets.info(table, :safe_fixed) == false
  end

  test "27 oversized search is rejected before registry reads without silently changing its draft" do
    query = String.duplicate("x", 257)
    result = Clients.snapshot(%{"q" => query}, :nonexistent_seventh_registry)
    assert result.status == :invalid_filters
    assert result.filters.q == query
    assert result.scanned_count == 0

    html =
      Admin.render_clients_page_table(%{
        clients: [],
        client_coverage: result,
        client_filters: result.filters
      })

    assert html =~ "Search must be 256 characters or fewer"
    assert html =~ ~s(value="#{query}")
    refute html =~ "No matching connections"
  end

  test "27 the operational collector, live payload and HTML route apply the same search beyond 500" do
    alias FerricstoreServer.Connection.Registry
    seed = System.unique_integer([:positive, :monotonic]) + 8_000_000_000
    query = "seventh-client-#{seed}"
    now = System.monotonic_time(:millisecond)
    ids = Enum.to_list((seed + 1)..(seed + 501))
    owner = self()
    on_exit(fn -> Enum.each(ids, &Registry.unregister(&1, owner)) end)

    for {id, offset} <- Enum.with_index(ids) do
      Registry.register(id, self(), %{
        client_name: if(offset == 500, do: query, else: "other"),
        created_at_ms: now + offset,
        peer: "127.0.0.1",
        flags: ""
      })
    end

    data = Dashboard.collect_clients_page(%{"q" => query})
    assert [%{client_id: id}] = data.clients
    assert id == seed + 501
    assert data.client_filters.q == query
    assert {:ok, %{components: components}} = Dashboard.live_payload("clients?q=" <> query)
    assert components["clients_table"] =~ query
    assert Dashboard.render_clients_page(data) =~ "/dashboard/api/clients?q=#{query}"
    username = "seventh-client-observer-#{seed}"
    :ok = FerricstoreServer.Acl.set_user(username, ["on", "nopass", "+CLIENT.LIST"])
    on_exit(fn -> FerricstoreServer.Acl.del_user(username) end)

    cookie =
      FerricstoreServer.Health.Endpoint.Session.session_cookie(username)
      |> String.split(";", parts: 2)
      |> hd()

    response =
      http_get(FerricstoreServer.Health.Endpoint.port(), "/dashboard/clients?q=" <> query, [
        {"Cookie", cookie}
      ])

    assert extract_status_code(response) == 200
    assert extract_body(response) =~ query

    invalid =
      http_get(
        FerricstoreServer.Health.Endpoint.port(),
        "/dashboard/clients?q=" <> String.duplicate("x", 257),
        [{"Cookie", cookie}]
      )

    assert extract_status_code(invalid) == 422
    assert extract_body(invalid) =~ "Search must be 256 characters or fewer"
    refute extract_body(invalid) =~ ~s(data-dashboard-live-page="clients")
    assert Map.has_key?(Dashboard.collect_doctor_page(), :jobs_result)
    assert Map.has_key?(Dashboard.collect_slowlog_page(), :slowlog_status)
  end

  test "30 recent metrics ship exact counters and immutable browser-local window metadata" do
    html =
      RecentRates.render(%{
        overview: %{run_id: "run-<a>", total_commands: 9_007_199_254_740_993},
        hotcold: %{total_hits: 10, total_misses: 2, total_cold: 1, sample_rate: 100},
        lifecycle: %{expired_total: 0, evicted_total: 0},
        generated_at_ms: 1000
      })

    assert html =~ "Recent activity"
    assert html =~ "Waiting for a second sample"
    assert html =~ ~s(data-commands="9007199254740993")
    assert html =~ "run-&lt;a&gt;"
    assert html =~ "data-dashboard-recent-rates"
    assert html =~ ~s(data-dashboard-disclosure-key="recent-rate-observations")
    script = RecentRates.script()
    refute script =~ "setInterval"
    refute script =~ "fetch("
    refute script =~ "localStorage"
    refute script =~ "sessionStorage"
  end
end
