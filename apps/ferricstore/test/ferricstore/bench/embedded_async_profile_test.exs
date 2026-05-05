defmodule Ferricstore.Bench.EmbeddedAsyncProfileTest do
  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"
  @moduletag :bench
  @moduletag timeout: 120_000

  @iterations 1_000

  test "profile async SET - every layer timed" do
    alias Ferricstore.Store.Router

    # Set up async namespace
    Ferricstore.NamespaceConfig.set("aprof", "durability", "async")

    value = String.duplicate("v", 100)

    IO.puts("\n=== Async SET Profile (#{@iterations} iterations, sequential) ===\n")

    # 1. Full FerricStore.set on async namespace
    {full_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations, do: FerricStore.set("aprof:full:#{i}", value)
    end)

    # 2. Router.put on async namespace
    {router_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations, do: Router.put(FerricStore.Instance.get(:default), "aprof:rtr:#{i}", value, 0)
    end)

    # 3. quorum_bypass? check timing
    {bypass_us, _} = :timer.tc(fn ->
      for _ <- 1..@iterations do
        # Replicate the durability check
        prefix = case :binary.split("aprof:test", ":") do
          ["aprof:test"] -> "_root"
          [p | _] -> p
        end
        Ferricstore.NamespaceConfig.durability_for(prefix)
      end
    end)

    # 4. GenServer.call to shard (what async path does)
    key = "aprof:gs_test"
    idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    shard_name = Router.shard_name(FerricStore.Instance.get(:default), idx)
    {gs_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations do
        GenServer.call(shard_name, {:put, "aprof:gs:#{i}", value, 0})
      end
    end)

    # 5. ra:pipeline_command (what quorum path does)
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(idx)
    {ra_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations do
        corr = make_ref()
        :ra.pipeline_command(shard_id, {:put, "aprof:ra:#{i}", value, 0}, corr, :normal)
        receive do
          {:ra_event, _leader, {:applied, applied}} ->
            List.keyfind(applied, corr, 0)
          {:ra_event, _, _} ->
            :ok
        after
          10_000 -> :timeout
        end
      end
    end)

    full_ns = div(full_us * 1000, @iterations)
    router_ns = div(router_us * 1000, @iterations)
    bypass_ns = div(bypass_us * 1000, @iterations)
    gs_ns = div(gs_us * 1000, @iterations)
    ra_ns = div(ra_us * 1000, @iterations)

    IO.puts("  #{String.pad_trailing("Layer", 45)} #{String.pad_leading("Time/op", 12)}")
    IO.puts("  #{String.duplicate("-", 60)}")

    layers = [
      {"durability_for check", bypass_ns},
      {"GenServer.call(shard, {:put})", gs_ns},
      {"ra:pipeline_command + wait", ra_ns},
      {"--- Router.put (async ns) ---", router_ns},
      {"=== FerricStore.set (async ns) ===", full_ns},
      {"GenServer overhead vs ra", gs_ns - ra_ns}
    ]

    for {label, ns} <- layers do
      IO.puts("  #{String.pad_trailing(label, 45)} #{String.pad_leading("#{ns}ns", 12)}")
    end

    # Compare quorum namespace
    IO.puts("")
    {quorum_us, _} = :timer.tc(fn ->
      for i <- 1..@iterations, do: FerricStore.set("qprof:full:#{i}", value)
    end)
    quorum_ns = div(quorum_us * 1000, @iterations)
    IO.puts("  #{String.pad_trailing("=== FerricStore.set (quorum ns) ===", 45)} #{String.pad_leading("#{quorum_ns}ns", 12)}")
    IO.puts("  #{String.pad_trailing("Quorum vs Async ratio", 45)} #{String.pad_leading("#{Float.round(full_ns / max(quorum_ns, 1), 1)}x", 12)}")

    Ferricstore.NamespaceConfig.set("aprof", "durability", "quorum")
  end
end
