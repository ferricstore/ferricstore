defmodule FerricstoreServer.Health.Dashboard.FlowSummaryScopeTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.Sample
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    %{suffix: System.unique_integer([:positive])}
  end

  test "restricted overview counts only authorized records on initial and live responses", %{
    suffix: suffix
  } do
    type = "summary-acl-#{suffix}"

    records =
      for n <- 1..12 do
        id = "#{type}-#{n}"
        assert :ok = FerricStore.flow_create(id, type: type, state: "queued")
        assert {:ok, record} = FerricStore.flow_get(id)
        record
      end

    partition = hd(records).partition_key
    expected = Enum.count(records, &(&1.partition_key == partition))
    assert expected < length(records)
    username = "summary-reader-#{suffix}"

    assert :ok =
             Acl.set_user(username, ["on", "nopass", "%R~#{partition}", "-@all", "+FLOW.QUERY"])

    on_exit(fn -> Acl.del_user(username) end)
    Application.put_env(:ferricstore, :protected_mode, true)
    assert {:error, _} = Acl.check_command(username, "FLOW.INFO")
    assert {:error, _} = Acl.check_key_access(username, "*", :read)

    for opts <- [[acl_username: username], [acl_username: username, partition_key: partition]] do
      data = Dashboard.collect_flow_page(opts)
      summary = Enum.find(data.types, &(&1.type == type))
      assert summary.total == expected
      assert summary.queued == expected
      assert summary.active == expected
      assert summary.count_source == :sampled
      refute summary.exact
    end

    headers = [{"Cookie", Session.session_cookie(username)}]
    query = URI.encode_query(%{"partition_key" => partition})

    expected_active =
      Sample.collect_flow_records_sample_for_acl(400, username)
      |> Enum.count(&(&1.state not in ["completed", "failed", "cancelled"]))

    response = http_get(Endpoint.port(), "/dashboard/flow?" <> query, headers)
    assert extract_status_code(response) == 200
    assert active_count(extract_body(response)) == expected_active

    response = http_get(Endpoint.port(), "/dashboard/api/flow?" <> query, headers)
    assert extract_status_code(response) == 200
    payload = response |> extract_body() |> Jason.decode!()
    assert active_count(payload["components"]["flow_overview"]) == expected_active
  end

  test "explicit partition summary preserves custom states and ignores automatic partitions", %{
    suffix: suffix
  } do
    type = "summary-custom-#{suffix}"
    partition = "custom-#{suffix}"

    assert :ok =
             FerricStore.flow_create(type,
               type: type,
               state: "awaiting_payment",
               partition_key: partition
             )

    for n <- 1..4 do
      assert :ok = FerricStore.flow_create("#{type}-auto-#{n}", type: type, state: "queued")
    end

    data = Dashboard.collect_flow_page(partition_key: partition)

    assert [%{type: ^type, total: 1, active: 1, queued: 0, count_source: :sampled, exact: false}] =
             data.types

    assert hd(data.types).states == %{"awaiting_payment" => 1}
    assert data.summary.total == 1
  end

  test "type summaries are a pure reduction of their supplied records", %{suffix: suffix} do
    type = "summary-pure-#{suffix}"

    for n <- 1..3,
        do: assert(:ok = FerricStore.flow_create("#{type}-#{n}", type: type, state: "queued"))

    records = [%{id: "observed", type: type, state: "custom"}]

    assert [%{total: 1, active: 1, queued: 0, count_source: :sampled, exact: false}] =
             Sample.type_summaries(records)
  end

  defp active_count(html) do
    [_, count] = Regex.run(~r/<dt>Active<\/dt>\s*<dd>(\d+)<span>/, html)
    String.to_integer(count)
  end
end
