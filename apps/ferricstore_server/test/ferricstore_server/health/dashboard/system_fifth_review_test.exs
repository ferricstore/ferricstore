defmodule FerricstoreServer.Health.Dashboard.SystemFifthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias Ferricstore.Commands.Catalog.Entries
  alias Ferricstore.Store.CompoundKey
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Data.{KV, Security}
  alias FerricstoreServer.Health.Dashboard.Render.{Admin, KVPages, Overview, Prefixes}
  alias FerricstoreServer.Health.Dashboard.Render.Security, as: SecurityRender

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  test "14 internal sampling includes authorized compound metadata but never protected logical keys" do
    prefix = uid("metadata") <> ":"
    allowed = prefix <> "allowed"
    denied = prefix <> "denied"
    flow = Ferricstore.Flow.Keys.state_key(uid("flow"), prefix)
    catalog = Ferricstore.ServerCatalog.root_prefix() <> uid("catalog")

    physical = [
      CompoundKey.type_key(allowed),
      CompoundKey.hash_field(allowed, "field"),
      CompoundKey.list_meta_key(allowed),
      CompoundKey.list_element(allowed, 0),
      CompoundKey.set_member(allowed, "member"),
      CompoundKey.zset_member(allowed, "member"),
      CompoundKey.stream_prefix(allowed) <> "0-1",
      CompoundKey.stream_meta_key(allowed),
      CompoundKey.stream_group(allowed, "group"),
      CompoundKey.stream_pending(allowed, "group", "0-1"),
      CompoundKey.stream_consumer(allowed, "group", "consumer")
    ]

    hidden = [
      CompoundKey.type_key(denied),
      flow,
      CompoundKey.type_key(flow),
      catalog,
      CompoundKey.type_key(catalog)
    ]

    insert_keys(physical ++ hidden)
    actor = user(["-@all", "+SCAN", "+GET", "%R~#{allowed}"])

    sample =
      KV.collect_keyspace_page(%{
        "prefix" => prefix,
        "include_internal" => "true",
        "acl_username" => actor
      })

    assert MapSet.new(Enum.map(sample.rows, & &1.physical_key)) == MapSet.new(physical)
    assert Enum.all?(sample.rows, &(&1.key == allowed))

    kinds = [
      "type metadata",
      "hash field",
      "list metadata",
      "list element",
      "set member",
      "zset member",
      "stream record",
      "stream metadata",
      "stream group",
      "stream pending entry",
      "stream consumer"
    ]

    expected = Map.new(Enum.zip(physical, kinds))
    assert Map.new(sample.rows, &{&1.physical_key, Map.get(&1, :physical_kind)}) == expected

    for row <- sample.rows, String.starts_with?(row.physical_key, ["XM:", "XG:", "XP:", "XC:"]) do
      refute row.type == "string"
    end

    assert KV.collect_keyspace_page(%{"prefix" => prefix, "acl_username" => actor}).rows == []

    open = KV.collect_keyspace_page(%{"include_internal" => "true", "limit" => "500"})
    refute Enum.any?(open.rows, &(&1.physical_key in Enum.drop(hidden, 1)))

    assert KV.collect_keyspace_page(%{"key" => hd(physical), "include_internal" => "true"}).rows ==
             []
  end

  test "15 long operational tables have labeled keyboard-scroll regions and inspectable full values" do
    long = String.duplicate("long<&identifier", 30)

    htmls = [
      {Admin.render_slowlog_table([
         %{id: 1, timestamp_us: 1_000_000, duration_us: 100, command: ["GET", long]}
       ]), "Slow log entries"},
      {Admin.render_clients_table([
         %{
           client_id: 1,
           client_name: long,
           username: "default",
           peer: "127.0.0.1:1234",
           age_seconds: 2,
           flags: ""
         }
       ]), "Active connections"},
      {Prefixes.render_prefixes_table(%{
         prefixes: [%{prefix: long, keys: 1, pct: 100, hot_reads: 0, cold_reads: 0}],
         total_sampled: 1
       }), "Key prefixes"},
      {KVPages.render_read_prefix_table(%{
         prefixes: [%{prefix: long, hot_reads: 1, cold_reads: 0, cold_pct: 0.0}]
       }), "Prefix read pressure"},
      {Admin.render_raft_table([
         %{
           shard: 0,
           status: :ok,
           leader: {long, :node@localhost},
           current_term: 1,
           commit_index: 3,
           last_applied: 3,
           members: [{long, :node@localhost}]
         }
       ]), "Per-shard consensus state"}
    ]

    for {html, label} <- htmls do
      assert html =~ ~s(role="region" aria-label="#{label}" tabindex="0")
      assert html =~ ~s(class="dashboard-table-value")
      assert html =~ ~s(class="dashboard-table-value-full")
      assert html =~ "long&lt;&amp;identifier"
      refute html =~ "long<&identifier"
    end
  end

  test "14 physical key representations preserve control bytes, literal escapes, and invalid UTF-8" do
    utf8 = CompoundKey.hash_field("tenant:key\\literal", "field<&\n\"")
    invalid = <<"H:tenant:key", 0, 255>>
    long = CompoundKey.hash_field("tenant:key", String.duplicate("field", 40))

    rows =
      Enum.map([utf8, invalid, long], fn physical ->
        %{
          key: "tenant:key",
          physical_key: physical,
          physical_kind: "hash field",
          type: "hash field"
        }
      end)

    html =
      KVPages.render_keyspace_table(%{
        rows: rows,
        searched?: true,
        filters: %{include_internal: true}
      })

    assert html =~ "Physical kind"
    assert html =~ "Physical Key (encoded)"

    assert html =~
             "JSON " <> FerricstoreServer.Health.Dashboard.Format.escape(Jason.encode!(utf8))

    assert html =~ "\\u0000"
    assert html =~ "\\\\literal"
    assert html =~ "Base64 " <> Base.encode64(invalid)
    assert html =~ "invalid UTF-8"
    assert html =~ ~s(class="dashboard-table-value")
    assert html =~ ~s(class="dashboard-table-value-full")
    assert :binary.match(html, <<0>>) == :nomatch
    assert String.valid?(html)
    refute html =~ "field<&"
  end

  test "14 undecodable, nested, and fetch-or-compute metadata never becomes a public logical key" do
    suffix = uid("opaque")
    hidden = ["FC:" <> suffix, "H:" <> suffix, "T:T:" <> suffix]
    insert_keys(hidden)

    for key <- hidden do
      page =
        KV.collect_keyspace_page(%{
          "prefix" => CompoundKey.extract_redis_key(key),
          "include_internal" => "true"
        })

      assert page.rows == []
    end
  end

  test "16 unknown commands are unsupported even for wildcard administrators" do
    actor = user(["+@all", "~*", "&*"])
    page = Security.collect_page(%{"user" => actor, "command" => "NOT_A_COMMAND"})
    assert page.tester.command.status == :unsupported
    assert page.tester.errors.command =~ "not a supported command"
    html = SecurityRender.render_acl_tester(page)
    assert html =~ ~s(aria-describedby="acl-command-error")
    assert html =~ "NOT_A_COMMAND"
    refute html =~ "Command allowed"
  end

  test "16 catalog-backed command checks retain allowed and denied results" do
    actor = user(["-@all", "+GET", "~*"])

    assert Security.collect_page(%{"user" => actor, "command" => "get"}).tester.command.status ==
             :allowed

    assert Security.collect_page(%{"user" => actor, "command" => "SET"}).tester.command.status ==
             :denied

    assert Security.collect_page(%{"user" => actor, "command" => "ACL.LIST"}).tester.command.status !=
             :unsupported
  end

  test "17 unknown or disabled users stop all checks with an actionable identity error" do
    unknown = uid("missing")
    disabled = user(["off", "+@all", "~*", "&*"])

    for {actor, message} <- [{unknown, "does not exist"}, {disabled, "is disabled"}] do
      page = Security.collect_page(%{"user" => actor, "command" => "GET", "key" => "tenant:key"})
      assert page.tester.errors.user =~ message
      assert page.tester.command.status == :idle
      assert page.tester.key.status == :idle
      html = SecurityRender.render_acl_tester(page)
      assert html =~ ~s(id="acl-user-error")
      assert html =~ ~s(aria-describedby="acl-user-error")
      assert html =~ actor
      refute html =~ "NOPERM"
    end
  end

  test "18 initial ACL tester stays neutral but empty submissions request a target" do
    assert Map.get(Security.collect_page().tester, :errors, %{}) == %{}

    page =
      Security.collect_page(%{
        "user" => "default",
        "command" => "",
        "key" => "",
        "channel" => "",
        "route_path" => ""
      })

    assert page.tester.errors.targets =~ "Enter a command, key, channel, or route"
    html = SecurityRender.render_acl_tester(page)
    assert html =~ ~s(id="acl-target-error")
    assert html =~ ~s(aria-describedby="acl-target-help acl-target-error")
  end

  test "19 configuration formatting preserves wire values and explains units, zero, and empty" do
    rows =
      Enum.map(
        [
          {"keydir-max-ram", "268435456"},
          {"maxmemory", "0"},
          {"notify-keyspace-events", ""},
          {"native-tls-cert-file", ""},
          {"requirepass", "secret"},
          {"missing", nil}
        ],
        fn {key, value} ->
          %{
            parameter: key,
            value: value,
            source: "CONFIG GET",
            scope: "runtime",
            mutability: "read-only",
            notes: "parameter"
          }
        end
      )

    html = Admin.render_config_parameters(rows)
    assert html =~ "256.0 MB"
    assert html =~ "268435456 bytes"
    assert html =~ "No explicit process-memory ceiling"
    assert html =~ "Empty string"
    assert html =~ "Not configured"
    assert html =~ "Unavailable"
    assert html =~ "Redacted"
    refute html =~ "secret"
  end

  test "22 exact and prefix submissions are distinct named groups with a page-scoped clear action" do
    html = KVPages.render_keyspace_controls(%{filters: %{}})
    assert html =~ ~s(class="kv-query-modes")
    assert html =~ "<legend>Inspect an exact key</legend>"
    assert html =~ "<legend>Sample by prefix</legend>"
    assert length(Regex.scan(~r/>Compound metadata\s*</, html)) == 2
    assert html =~ ">Clear both searches</a>"
    refute html =~ "Flow values"
  end

  test "19 disabled settings have parameter-specific semantics while zero counts stay zero" do
    rows =
      Enum.map(
        [{"notify-keyspace-events", ""}, {"native-tls-port", "0"}, {"slowlog-max-len", "0"}],
        fn {key, value} ->
          %{
            parameter: key,
            value: value,
            source: "CONFIG GET",
            scope: "runtime",
            mutability: "read-only",
            notes: "parameter"
          }
        end
      )

    html = Admin.render_config_parameters(rows)
    assert html =~ "Empty string"
    assert html =~ "Notifications disabled"
    assert html =~ "TLS listener not configured"
    assert html =~ ">0</td>"
  end

  test "24 absent read samples do not render a measured source distribution" do
    data = %{
      total_lookups: 0,
      hit_ratio: 0.0,
      sample_rate: 100,
      ram_ratio: 0.0,
      disk_ratio: 0.0,
      hits_per_sec: 0.0,
      misses_per_sec: 0.0
    }

    html = Overview.render_cache_performance(data)
    assert length(Regex.scan(~r/>No read samples</, html)) == 3
    refute html =~ ">0.0%</div>"
    observed = Overview.render_cache_performance(%{data | total_lookups: 1})
    assert observed =~ ">0.0%</div>"
  end

  test "25 every catalog command links to a verified local command guide section" do
    html = KVPages.render_kv_command_reference(%{})

    assert html =~
             ~s(href="https://github.com/ferricstore/ferricstore/blob/main/guides/commands.md#string-commands")

    assert html =~ "Returns the string value of a key."
    assert html =~ "#stream-commands"
    assert html =~ "#ferricstore-native-commands"

    for group <- KVPages.kv_command_groups(), command <- group.commands do
      assert {:ok, _entry} = Entries.lookup_upper(command)
      assert html =~ ~r/<a[^>]*>#{Regex.escape(command)}<\/a>/
    end
  end

  test "24 all-miss reads retain measured hit rate without fabricating hit sources" do
    data = %{
      total_lookups: 5,
      total_hits: 0,
      hit_ratio: 0.0,
      sample_rate: 100,
      ram_ratio: 40.0,
      disk_ratio: 60.0,
      hits_per_sec: 0.0,
      misses_per_sec: 1.0
    }

    for sample <- [
          data,
          Map.delete(data, :total_hits),
          Map.merge(data, %{total_hot: nil, total_cold: nil}),
          Map.merge(data, %{total_hits: nil, total_hot: nil, total_cold: nil})
        ] do
      html = Overview.render_cache_performance(sample)
      assert length(Regex.scan(~r/>No hit samples</, html)) == 2
      assert html =~ ">0.0%</div>"
      assert length(Regex.scan(~r/width:0%;background:/, html)) == 2
      refute html =~ ">40.0%</div>"
    end

    observed =
      Overview.render_cache_performance(%{
        data
        | total_hits: 5,
          ram_ratio: 0.0,
          disk_ratio: 100.0
      })

    assert observed =~ ">100.0%</div>"
    refute observed =~ "No hit samples"
  end

  test "28 Streams disclosures own their headings without repeated nested titles" do
    html = Dashboard.render_streams_page(%{})
    assert length(Regex.scan(~r/<summary><span>Active Streams<\/span>/, html)) == 1
    refute html =~ ~r/<h2[^>]*>Active Streams/
    refute html =~ ~r/<h2[^>]*>Blocked Readers/
    refute html =~ "Stream Consumers"
    assert html =~ "Filter loaded streams and consumer groups"
  end

  defp insert_keys(keys) do
    :ets.insert(:keydir_0, Enum.map(keys, &{&1, "hash", 0, 0, 0, 0, 4}))
    on_exit(fn -> Enum.each(keys, &:ets.delete(:keydir_0, &1)) end)
  end

  defp user(rules) do
    name = uid("user")
    :ok = Acl.set_user(name, ["on", "nopass" | rules])
    on_exit(fn -> Acl.del_user(name) end)
    name
  end

  defp uid(label), do: "fifth-system-#{label}-#{System.unique_integer([:positive])}"
end
