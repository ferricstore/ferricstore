defmodule FerricstoreServer.Health.Dashboard.KeysRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.KVPages

  test "each search is one row with a labelled input and its button" do
    html = KVPages.render_keyspace_controls(%{filters: %{}})

    assert length(Regex.scan(~r/<div class="kv-query-row">/, html)) == 2
    assert html =~ ~s(<span class="sr-only">Exact key</span>)
    assert html =~ ~s(<span class="sr-only">Prefix</span>)
    assert html =~ "<legend>Inspect an exact key</legend>"
    assert html =~ "<legend>Sample by prefix</legend>"
  end

  test "before a search a short prompt replaces the empty table" do
    html = KVPages.render_keyspace_table(%{searched?: false, rows: [], filters: %{}})

    assert html =~ "Enter an exact key or prefix"
    refute html =~ "<table"
    refute html =~ "awaiting query"
  end

  test "after a search the metadata table is shown" do
    html =
      KVPages.render_keyspace_table(%{
        searched?: true,
        rows: [
          %{
            key: "user:1",
            physical_kind: "string",
            shard: 3,
            location: "hot",
            size: "10 B",
            ttl: "-",
            lfu: 1,
            physical_key: "user:1"
          }
        ],
        filters: %{include_internal: false}
      })

    assert html =~ "<table"
    assert html =~ "user:1"
    assert html =~ "1 keys returned"
  end
end
