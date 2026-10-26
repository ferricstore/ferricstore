defmodule Ferricstore.ZzProbe.ClaimScaleProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers

  test "claim latency vs queue size" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)

    for {target, batch} <- [{1_000, 1_000}, {10_000, 9_000}, {50_000, 40_000}] do
      base = System.unique_integer([:positive])

      for chunk <- Enum.chunk_every(1..batch, 500) do
        items = for i <- chunk, do: %{id: "q#{base}-#{i}"}
        :ok = FerricStore.flow_create_many(nil, items, type: "qscale")
      end

      times =
        for _ <- 1..20 do
          {us, {:ok, claims}} =
            :timer.tc(fn ->
              FerricStore.flow_claim_due("qscale", worker: "w", limit: 10, lease_ms: 600_000)
            end)

          10 = length(claims)
          us
        end

      IO.puts("queue ~#{target}: claim(10) median #{Enum.at(Enum.sort(times), 10)} us")
    end
  end
end
