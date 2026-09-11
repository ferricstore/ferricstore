defmodule FerricstoreServer.Health.Dashboard.FlowValueInspectorTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Render.FlowHistory

  setup do
    keys = [
      :protected_mode,
      :flow_dashboard_flow_get_fun,
      :flow_dashboard_flow_history_fun,
      :flow_dashboard_flow_value_mget_fun
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:ferricstore, &1)})
    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_env(key, value) end) end)
    Application.put_env(:ferricstore, :protected_mode, false)

    Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn "value-flow", opts ->
      assert opts[:partition_key] == "scope-a"

      {:ok,
       %{
         id: "value-flow",
         type: "email",
         state: "queued",
         partition_key: "scope-a",
         payload_ref: "current-ref",
         updated_at_ms: 1_000
       }}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _, _ -> {:ok, []} end)
    :ok
  end

  test "missing values are not successful text previews" do
    value_result(nil)
    assert %{status: "missing", ref: "current-ref"} = payload()
    refute Map.has_key?(payload(), :value)
  end

  test "literal missing and empty values remain successful copyable data" do
    for value <- ["missing", "", "Loading value..."] do
      value_result(value)
      assert %{status: "ok", value: ^value, truncated: false} = payload()
    end
  end

  test "errors are not returned in the value field" do
    Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn _ ->
      {:error, :unavailable}
    end)

    assert %{status: "error", error: _} = result = payload()
    refute Map.has_key?(result, :value)
  end

  test "older history context authorizes only refs on that bounded page" do
    test_pid = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn "value-flow", opts ->
      send(test_pid, {:history_opts, opts})

      if opts[:to_event] == "5000-1" do
        {:ok, [{"1000-1", %{"event" => "created", "payload_ref" => "old-ref"}}]}
      else
        {:ok, []}
      end
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn refs ->
      send(test_pid, {:values, refs})
      {:ok, ["old data"]}
    end)

    assert %{status: "ok", value: "old data"} =
             payload(%{
               "ref" => "old-ref",
               "history_before" => "5000-1",
               "history_count" => "100"
             })

    assert_receive {:history_opts, opts}
    assert opts[:to_event] == "5000-1"
    assert opts[:count] == 102
    assert opts[:partition_key] == "scope-a"
    assert opts[:values] == false
    assert_receive {:values, ["old-ref"]}

    assert %{status: "error"} = payload(%{"ref" => "old-ref"})
    assert %{status: "error"} = payload(%{"ref" => "foreign-ref", "history_before" => "5000-1"})
    refute_receive {:values, _}
  end

  test "history context enforces count ceilings and blank cursor normalization" do
    test_pid = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _, opts ->
      send(test_pid, {:history_opts, opts})
      {:ok, []}
    end)

    value_result("data")
    assert %{status: "ok"} = payload(%{"history_after" => "1000-1", "history_count" => "999999"})
    assert_receive {:history_opts, opts}
    assert opts[:from_event] == "1000-1"
    assert opts[:count] <= 252
    assert %{status: "ok"} = payload(%{"history_before" => "  ", "history_count" => "oops"})
    assert_receive {:history_opts, opts}
    refute opts[:to_event]
    assert opts[:count] == 51
  end

  test "preview cap is bytes, including combining marks and truncation notice" do
    for value <- [
          String.duplicate("a", 20_000),
          "a" <> :binary.copy(<<204, 129>>, 50_000),
          String.duplicate("界", 4_000),
          :binary.copy(<<255>>, 100_000)
        ] do
      preview = FlowHistory.flow_value_preview(value)
      assert byte_size(preview) <= 8 * 1024
      assert String.valid?(preview)
      assert preview =~ "truncated"
      value_result(value)
      assert %{status: "ok", truncated: true, value: ^preview} = payload()
    end
  end

  test "preview preserves complete values and valid UTF8 at byte boundaries" do
    for value <- ["", "missing", String.duplicate("a", 8_192), String.duplicate("界", 2_730)] do
      assert FlowHistory.flow_value_preview(value) == value
    end

    value = String.duplicate("a", 8_191) <> "界"
    preview = FlowHistory.flow_value_preview(value)
    assert String.valid?(preview)
    assert byte_size(preview) <= 8_192
    assert preview =~ "truncated"
  end

  test "older-page context cannot bypass record ACL or load arbitrary references" do
    username = "value-inspector-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               "nopass",
               "%R~other-scope",
               "-@all",
               "+FLOW.QUERY"
             ])

    on_exit(fn -> FerricstoreServer.Acl.del_user(username) end)
    test_pid = self()

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _, _ ->
      send(test_pid, :unexpected_history_read)
      {:ok, [{"1000-1", %{"payload_ref" => "secret-ref"}}]}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn _ ->
      send(test_pid, :unexpected_value_read)
      {:ok, ["secret"]}
    end)

    query =
      URI.encode_query(%{
        "flow" => "value-flow",
        "partition_key" => "scope-a",
        "ref" => "secret-ref",
        "history_before" => "5000-1",
        "acl_username" => "default"
      })

    assert {:ok, %{status: "error"}} =
             Dashboard.live_payload("flow/value?" <> query, acl_username: username)

    refute_receive :unexpected_history_read
    refute_receive :unexpected_value_read
  end

  defp value_result(value) do
    Application.put_env(:ferricstore, :flow_dashboard_flow_value_mget_fun, fn ["current-ref"] ->
      {:ok, [value]}
    end)
  end

  defp payload(extra \\ %{}) do
    query =
      Map.merge(
        %{"flow" => "value-flow", "partition_key" => "scope-a", "ref" => "current-ref"},
        extra
      )

    assert {:ok, result} = Dashboard.live_payload("flow/value?" <> URI.encode_query(query))
    result
  end
end
