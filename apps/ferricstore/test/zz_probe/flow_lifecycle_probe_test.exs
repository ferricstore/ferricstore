defmodule Ferricstore.ZzProbe.FlowLifecycleProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  @n String.to_integer(System.get_env("PROBE_N", "2000"))
  @cycles String.to_integer(System.get_env("PROBE_CYCLES", "3"))

  test "flow lifecycle: time, disk and keydir per cycle" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 6}} = FerricStore.raise_cluster_feature_level(6)
    ctx = FerricStore.Instance.get(:default)
    dir = isolated.tmp_dir
    report("baseline", dir, ctx)

    for {label, opts} <- [
          {"no payload", []},
          {"1 KB payload", [payload: String.duplicate("x", 1024)]}
        ] do
      {us, _} =
        :timer.tc(fn ->
          for i <- 1..200,
              do: :ok = FerricStore.flow_create("t-#{label}-#{i}", [type: "timing"] ++ opts)
        end)

      IO.puts("create #{label}: #{Float.round(us / 200, 1)} us")
    end

    for cycle <- 1..@cycles do
      type = "probe-#{cycle}"
      payload = :crypto.strong_rand_bytes(768) |> Base.encode64()
      ids = for i <- 1..@n, do: "c#{cycle}-#{i}"

      {create_us, _} =
        :timer.tc(fn ->
          for id <- ids do
            :ok = FerricStore.flow_create(id, type: type, payload: payload)
          end
        end)

      {claim_us, claims} =
        :timer.tc(fn -> claim_all(type, []) end)

      {complete_us, _} =
        :timer.tc(fn ->
          for c <- claims do
            :ok =
              FerricStore.flow_complete(c.id, c.lease_token,
                fencing_token: c.fencing_token,
                result: %{"out" => payload}
              )
          end
        end)

      IO.puts(
        "cycle #{cycle}: create #{per(create_us)} us, claim #{per(claim_us)} us (#{length(claims)}), complete #{per(complete_us)} us"
      )

      flush(ctx)
      report("cycle #{cycle} completed", dir, ctx)

      cleanup_now = System.system_time(:millisecond) + 8 * 86_400_000
      {cleanup_us, totals} = :timer.tc(fn -> cleanup_all(cleanup_now, nil, %{}) end)
      IO.puts("cycle #{cycle}: cleanup #{div(cleanup_us, 1000)} ms #{inspect(totals)}")
      flush(ctx)
      for id <- ids, do: {:ok, nil} = FerricStore.flow_get(id)
      report("cycle #{cycle} cleaned", dir, ctx)
    end
  end

  defp per(us), do: Float.round(us / @n, 1)

  defp claim_all(type, acc) do
    case FerricStore.flow_claim_due(type, worker: "w", limit: 200, lease_ms: 600_000) do
      {:ok, []} -> acc
      {:ok, claims} -> claim_all(type, claims ++ acc)
    end
  end

  defp cleanup_all(now, continuation, totals) do
    opts =
      [limit: 500, now_ms: now] ++ if(continuation, do: [continuation: continuation], else: [])

    {:ok, result} = FerricStore.flow_retention_cleanup(opts)

    totals =
      Enum.reduce([:flows, :history, :values], totals, fn k, acc ->
        Map.update(acc, k, Map.get(result, k, 0), &(&1 + Map.get(result, k, 0)))
      end)

    if result.more?, do: cleanup_all(now, result.continuation, totals), else: totals
  end

  defp flush(ctx) do
    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 120_000)

    for i <- 0..(ctx.shard_count - 1),
        do: Ferricstore.Flow.HistoryProjector.flush(ctx, i, 120_000)

    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 120_000)
  end

  defp report(label, dir, ctx) do
    {du, 0} = System.cmd("du", ["-sk", dir])
    total = du |> String.split() |> hd()

    {lines, 0} =
      System.cmd("sh", [
        "-c",
        "cd #{dir} && find . -mindepth 1 -maxdepth 3 -type d -exec du -sk {} + | sort -rn | head -12"
      ])

    keydir =
      ctx.keydir_refs
      |> Tuple.to_list()
      |> Enum.map(&:ets.info(&1, :size))
      |> Enum.sum()

    IO.puts("== #{label}: #{total} KB total, keydir rows #{keydir}\n#{lines}")
  end
end
