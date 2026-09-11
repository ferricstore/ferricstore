defmodule FerricstoreServer.Health.Dashboard.SecurityKVReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Dashboard.Data.{KV, Security}
  alias FerricstoreServer.Health.Dashboard.Render.KVPages
  alias FerricstoreServer.Health.Dashboard.Render.Security, as: SecurityRender
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.{RouteRequirements, Session}

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  test "account forms default to observer, not administrator" do
    html = SecurityRender.render_account_management(%{can_manage_users: true})
    assert html =~ ~s(value="observer" checked)
    refute html =~ ~s(value="admin" checked)
  end

  test "account recovery escapes drafts and never reflects credential modifiers" do
    page =
      Security.account_error_page(
        "actor",
        %{
          "username" => "<script>bad</script>",
          "role" => "custom",
          "modifiers" => "+GET\r>private-password\r #private-hash\r%R~review:*",
          "password" => "not-for-the-page",
          "extra" => "untrusted"
        },
        "Invalid modifiers"
      )

    html = SecurityRender.render_acl_security(page)
    assert html =~ "&lt;script&gt;bad&lt;/script&gt;"
    assert html =~ "+GET"
    assert html =~ "%R~review:*"
    refute html =~ "private-password"
    refute html =~ "private-hash"
    refute html =~ "not-for-the-page"
    refute Map.has_key?(page.account_draft, "extra")
    assert html =~ ~s(value="custom" checked)
  end

  test "account validation keeps nonsecret scope and clears passwords without listing users" do
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, true)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    actor = user(["-@all", "+ACL.SETUSER"])
    username = uid("draft")

    params = %{
      "username" => username,
      "role" => "observer",
      "key_pattern" => " review:* ",
      "channel_pattern" => "review:events",
      "password" => "review-password-123",
      "password_confirmation" => "different-password-123"
    }

    for _attempt <- 1..2 do
      response = post_account(actor, params)
      assert extract_status_code(response) == 422
      assert extract_header(response, "location") == nil
      html = extract_body(response)
      assert html =~ ~s(value="#{username}")
      assert html =~ ~s(value="observer" checked)
      assert html =~ ~s(value=" review:* ")
      assert html =~ ~s(value="review:events")
      refute html =~ "review-password-123"
      refute html =~ "different-password-123"
      refute html =~ "ACL account list"
      assert Acl.get_user(username) == nil
    end

    response = post_account(actor, Map.put(params, "password_confirmation", params["password"]))
    on_exit(fn -> Acl.del_user(username) end)
    assert extract_status_code(response) == 302
    assert Acl.check_command(username, "GET") == :ok
    assert {:error, _} = Acl.check_command(username, "SET")
    assert {:error, _} = Acl.check_key_access(username, "other:key", :read)
  end

  test "effective admin summaries recognize compiled wildcard grants and preserve exceptions" do
    full = user(["~*", "&*", "+@all"])
    split = user(["%R~*", "%W~*", "&*", "+@all"])
    denied = user(["~*", "&*", "+@all", "-SET"])
    scoped = user(["~review:*", "&*", "+@all"])
    channels = user(["~*", "resetchannels", "+@all"])
    summaries = Security.collect_page().acl_users |> Map.new(&{&1.username, &1.access})
    assert summaries[full] == "Full administrator"
    assert summaries[split] == "Full administrator"

    for username <- [denied, scoped, channels],
        do: refute(summaries[username] == "Full administrator")

    assert summaries[denied] =~ "exceptions"
  end

  test "authorized sample fills the visible limit instead of stopping on denied rows" do
    prefix = uid("sample") <> ":"
    keys = Enum.map(1..80, &(prefix <> Integer.to_string(&1)))
    insert_keys(keys)
    all = KV.collect_keyspace_page(%{"prefix" => prefix, "limit" => "80"})
    allowed = List.last(all.rows).key
    actor = user(["-@all", "+GET", "+SCAN", "%R~#{allowed}"])

    page =
      KV.collect_keyspace_page(%{"prefix" => prefix, "limit" => "1", "acl_username" => actor})

    assert Enum.map(page.rows, & &1.key) == [allowed]
    assert page.total_sampled == 1
  end

  test "exhausted scan budgets are explicit and do not expose hidden row counts" do
    prefix = uid("budget") <> ":"
    insert_keys(Enum.map(1..10_100, &(prefix <> Integer.to_string(&1))))
    actor = user(["-@all", "+SCAN", "%R~not-present:*"])

    page =
      KV.collect_keyspace_page(%{"prefix" => prefix, "limit" => "1", "acl_username" => actor})

    assert page.rows == []
    assert page.total_sampled == 0
    assert page.scan_limited?
    html = KVPages.render_keyspace_table(page)
    assert html =~ "Scan budget reached"
    refute html =~ "No key metadata matched this query."
  end

  test "literal key and prefix identity survives normalization, ACL requirements, and links" do
    key = " " <> uid("caf\u00e9<&") <> " "
    insert_keys([key])
    assert KV.keyspace_filters(%{"key" => key}).key == key
    assert KV.keyspace_filters(%{"prefix" => " "}).prefix == " "
    page = KV.collect_keyspace_page(%{"key" => key})
    assert page.inspected.found?
    assert page.inspected.key == key
    path = "/dashboard/keyspace?" <> URI.encode_query(%{"key" => key})

    assert RouteRequirements.dashboard_route_requirement("GET", path) ==
             {"GET", key: {key, :read}}

    html = KVPages.render_keyspace_table(page)
    assert html =~ "mode=exact"
    assert html =~ URI.encode_www_form(key)
    refute html =~ key
  end

  test "explicit prefix mode cannot accidentally inspect an inactive exact-key draft" do
    key = uid("exact")
    prefix = uid("prefix")
    insert_keys([key, prefix <> ":one"])
    opts = %{"mode" => "prefix", "key" => key, "prefix" => prefix}
    page = KV.collect_keyspace_page(opts)
    assert page.inspected == nil
    assert Enum.map(page.rows, & &1.key) == [prefix <> ":one"]
    path = "/dashboard/keyspace?" <> URI.encode_query(opts)
    assert RouteRequirements.dashboard_route_requirement("GET", path) == {"SCAN", []}
    html = KVPages.render_keyspace_controls(page)
    assert html =~ ~s(aria-label="Exact key inspection")
    assert html =~ ~s(aria-label="Prefix sample")
  end

  test "read summaries distinguish no samples from a measured zero hit rate" do
    empty =
      KVPages.render_reads_summary(%{hotcold: %{hit_ratio: 0.0, hot_reads: 0, cold_reads: 0}})

    assert empty =~ "No read samples"
    refute empty =~ "0.0%"

    misses =
      KVPages.render_reads_summary(%{hotcold: %{hit_ratio: 0.0, hot_reads: 0, cold_reads: 3}})

    assert misses =~ "0.0%"

    only_misses =
      KVPages.render_reads_summary(%{
        hotcold: %{hit_ratio: 0.0, total_lookups: 3, total_misses: 3}
      })

    assert only_misses =~ "0.0%"
  end

  test "storage reconciles shard data and other directories without another scan" do
    shard = %{index: 0, disk_bytes: 1024, data_file_count: 1, hint_file_count: 1}

    html =
      KVPages.render_storage_summary(%{shards: [shard], total_disk_bytes: 4096, total_files: 2})

    assert html =~ "Data directory"
    assert html =~ "Shard data directories"
    assert html =~ "Other directories"
    assert html =~ "3.0 KB"
    assert html =~ "Data + hint files"
    assert KVPages.render_storage_table([shard]) =~ "Shard directory size"
  end

  defp insert_keys(keys) do
    entries = Enum.map(keys, &{&1, "value", 0, 0, 0, 0, 5})
    :ets.insert(:keydir_0, entries)
    on_exit(fn -> Enum.each(keys, &:ets.delete(:keydir_0, &1)) end)
  end

  defp user(rules) do
    name = uid("user")
    :ok = Acl.set_user(name, ["on", "nopass" | rules])
    on_exit(fn -> Acl.del_user(name) end)
    name
  end

  defp uid(prefix), do: "security-kv-#{prefix}-#{System.unique_integer([:positive])}"

  defp post_account(username, params) do
    session = Session.session_cookie(username) |> String.split(";", parts: 2) |> hd()
    {token, csrf_cookie} = Session.csrf_pair()
    csrf = csrf_cookie |> String.split(";", parts: 2) |> hd()

    http_post_form(
      Endpoint.port(),
      "/dashboard/security/users",
      Map.put(params, "_csrf_token", token),
      [{"Cookie", session <> "; " <> csrf}]
    )
  end
end
