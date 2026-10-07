# MIX_ENV=test ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start bench/waraft_timer_regression.exs
# BENCH_WARAFT_EBIN can select an isolated compiled dependency candidate.
case System.get_env("BENCH_WARAFT_EBIN") do
  nil ->
    :ok

  path ->
    # ClusterHelper passes repeated -pa arguments; the peer sees their reverse
    # precedence. Append here so the candidate is first on each fresh peer.
    true = Code.append_path(path)

    for file <- Path.wildcard(Path.join(path, "wa_raft*.beam")) do
      module = file |> Path.basename(".beam") |> String.to_atom()
      {:module, ^module} = :code.load_abs(file |> Path.rootname() |> String.to_charlist())
    end
end

ExUnit.start(
  exclude: if(System.get_env("BENCH_PROPAGATION") == "1", do: [], else: [:commit_propagation])
)

Code.require_file(
  "regressions/commit_batch_deadline_test.exs",
  __DIR__
)

if System.get_env("BENCH_CLUSTER_CHECKS") == "1" do
  Code.require_file("../apps/ferricstore/test/ferricstore/cluster/raft_cluster_test.exs", __DIR__)
end
