defmodule Ferricstore.ZzProbe.LmdbScaleProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers

  test "flush after 10k creates" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    ctx = FerricStore.Instance.get(:default)
    val = String.duplicate("v", 1024)
    IO.puts("cache type #{:ets.info(:ferricstore_waraft_apply_projection_cache, :type)}")

    for round <- 1..2 do
      {us, _} =
        :timer.tc(fn ->
          for(
            t <- 1..16,
            do:
              Task.async(fn ->
                for i <- 1..625,
                    do:
                      :ok =
                        FerricStore.flow_create("r#{round}-#{t}-#{i}", type: "sc", payload: val)
              end)
          )
          |> Task.await_many(:infinity)
        end)

      {fus, r} =
        :timer.tc(fn ->
          Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 600_000)
        end)

      IO.puts(
        "round #{round}: 10k creates #{div(us, 1000)} ms, flush #{div(fus, 1000)} ms #{inspect(r)}, cache rows #{:ets.info(:ferricstore_waraft_apply_projection_cache, :size)}"
      )
    end
  end
end
