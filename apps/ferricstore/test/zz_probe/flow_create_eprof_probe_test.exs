defmodule Ferricstore.ZzProbe.FlowCreateEprofProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers

  test "trace syncs during flow_create" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 6}} = FerricStore.raise_cluster_feature_level(6)
    for i <- 1..20, do: :ok = FerricStore.flow_create("warm-#{i}", type: "eprof")
    parent = self()

    tracer =
      spawn(fn ->
        loop = fn loop, acc ->
          receive do
            {:trace, pid, :call, {m, f, a}, caller} ->
              name = Process.info(pid, :registered_name) |> elem(1)
              loop.(loop, [{m, f, length(a), name, caller} | acc])

            {:done, from} ->
              send(from, {:traces, acc})
          end
        end

        loop.(loop, [])
      end)

    for mfa <- [
          {:prim_file, :sync_nif, 1},
          {:prim_file, :write_nif, 2},
          {:prim_file, :sync_nif, 2},
          {Ferricstore.Bitcask.NIF, :fs_atomic_replace_nofollow, 3},
          {Ferricstore.Bitcask.NIF, :v2_fsync, 1},
          {Ferricstore.Bitcask.NIF, :lmdb_write_batch, 3}
        ] do
      :erlang.trace_pattern(mfa, [{:_, [], [{:message, {:caller}}]}], [:local])
    end

    :erlang.trace(:all, true, [:call, {:tracer, tracer}])

    {us, _} =
      :timer.tc(fn ->
        for i <- 1..20, do: :ok = FerricStore.flow_create("p-#{i}", type: "eprof")
      end)

    :erlang.trace(:all, false, [:call])
    send(tracer, {:done, parent})
    assert_receive {:traces, traces}, 10_000
    IO.puts("create: #{div(us, 20)} us each")

    traces
    |> Enum.frequencies()
    |> Enum.sort_by(fn {_k, v} -> -v end)
    |> Enum.each(fn {k, v} -> IO.puts("TRACE #{v}x #{inspect(k)}") end)
  end
end
