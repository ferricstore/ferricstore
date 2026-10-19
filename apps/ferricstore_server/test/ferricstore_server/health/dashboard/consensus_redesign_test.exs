defmodule FerricstoreServer.Health.Dashboard.ConsensusRedesignTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Render.Admin

  test "a healthy cluster says so in one line" do
    html = Admin.render_consensus_status([shard(0), shard(1)])

    assert html =~ "is-healthy"
    assert html =~ "All 2 shards have a leader and are caught up"
  end

  test "a shard behind on applying is named with its gap" do
    html = Admin.render_consensus_status([shard(0), shard(1, applied: 786)])

    assert html =~ "is-warning"
    assert html =~ "Shard 1 is 214 entries behind"
    refute html =~ "caught up"
  end

  test "a shard that could not be read is reported, never counted as caught up" do
    partial = %{shard(1) | status: :partial, last_applied: nil}
    html = Admin.render_consensus_status([shard(0), partial])

    assert html =~ "1 of 2 shards could not be fully read"
    refute html =~ "caught up"
  end

  test "a shard without a leader is reported" do
    html = Admin.render_consensus_status([shard(0), %{shard(1) | leader: nil}])

    assert html =~ "is-degraded"
    assert html =~ "1 shard has no leader"
  end

  test "the grid has one tile per shard with its lag" do
    html =
      Admin.render_consensus_grid([
        shard(0),
        shard(1, applied: 786),
        %{shard(2) | status: :unavailable, commit_index: nil, last_applied: nil}
      ])

    assert length(Regex.scan(~r/<li class="consensus-shard is-ok"/, html)) == 1
    assert length(Regex.scan(~r/<li class="consensus-shard is-behind"/, html)) == 1
    assert length(Regex.scan(~r/<li class="consensus-shard is-down"/, html)) == 1
    assert html =~ "214 behind"
    assert html =~ "caught up"
    assert html =~ "unavailable"
    assert html =~ ~s(raft_1@#{node()})
  end

  test "summary cards keep exact values and say when a maximum is incomplete" do
    html = Admin.render_consensus_summary([shard(0), shard(1, applied: 786)])

    assert html =~ "Shards with a leader"
    assert html =~ ~s(<div class="consensus-stat-value">2 / 2</div>)
    assert html =~ ~s(<div class="consensus-stat-value is-warning">214</div>)
    assert html =~ "Highest term"

    partial = Admin.render_consensus_summary([shard(0), %{shard(1) | last_applied: nil}])
    assert partial =~ ~s(<div class="consensus-stat-value is-unknown">Unavailable</div>)
    assert partial =~ "1 / 2 shards measured"
  end

  test "the full per-shard table is folded under its own disclosure" do
    html = Dashboard.render_raft_page(Dashboard.collect_raft_page())

    assert html =~ ~s(data-live-component="consensus_status")
    assert html =~ ~s(data-live-component="consensus_grid")

    assert html =~
             ~r{<details class="dashboard-disclosure">\s*<summary>All shard details</summary>}

    assert html =~ "<th>Storage applied index</th>"

    {:ok, payload} = Dashboard.live_payload("raft")
    assert Map.has_key?(payload.components, "consensus_status")
    assert Map.has_key?(payload.components, "consensus_grid")
  end

  defp shard(index, opts \\ []) do
    commit = 1_000

    %{
      shard: index,
      status: :ok,
      leader: {:"raft_#{index}", node()},
      current_term: 3,
      commit_index: commit,
      last_applied: Keyword.get(opts, :applied, commit),
      members: [{:"raft_#{index}", node()}]
    }
  end
end
