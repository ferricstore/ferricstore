defmodule Ferricstore.ZzProbe.LmdbWriterEprofProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers

  defp table_name(t) when is_atom(t), do: t
  defp table_name(t), do: :ets.info(t, :name)

  test "eprof LMDB writers during a create burst" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    ctx = FerricStore.Instance.get(:default)
    val = String.duplicate("v", 1024)
    for i <- 1..50, do: :ok = FerricStore.flow_create("warm-#{i}", type: "wp", payload: val)
    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 300_000)

    writers =
      for i <- 0..(ctx.shard_count - 1),
          pid = Process.whereis(:"Ferricstore.Flow.LMDBWriter.#{i}"),
          do: pid

    red0 = Enum.sum(for p <- writers, do: elem(Process.info(p, :reductions), 2 - 1))
    n = 1000

    tasks =
      for t <- 1..8 do
        Task.async(fn ->
          for i <- 1..div(n, 8),
              do: :ok = FerricStore.flow_create("p#{t}-#{i}", type: "wp", payload: val)
        end)
      end

    parent = self()

    tracer =
      spawn(fn ->
        loop = fn loop, acc ->
          receive do
            {:trace, _pid, :call, {_m, _f, [table, _ms]}, caller} ->
              loop.(loop, Map.update(acc, {caller, table_name(table)}, 1, &(&1 + 1)))

            {:done, from} ->
              send(from, {:traces, acc})
          end
        end

        loop.(loop, %{})
      end)

    :erlang.trace_pattern({:ets, :select, 2}, [{:_, [], [{:message, {:caller}}]}], [:local])
    for w <- writers, do: :erlang.trace(w, true, [:call, {:tracer, tracer}])
    :eprof.start()
    :eprof.start_profiling(writers)
    Task.await_many(tasks, :infinity)

    {us, :ok} =
      :timer.tc(fn ->
        Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 300_000)
      end)

    :eprof.stop_profiling()
    for w <- writers, do: :erlang.trace(w, false, [:call])
    send(tracer, {:done, self()})

    receive do
      {:traces, t} ->
        for {k, v} <- Enum.sort_by(t, &(-elem(&1, 1))), do: IO.puts("SELECT #{v}x #{inspect(k)}")
    end

    red1 = Enum.sum(for p <- writers, do: elem(Process.info(p, :reductions), 1))

    IO.puts(
      "flush after burst: #{div(us, 1000)} ms; writer reductions per create: #{div(red1 - red0, n)}"
    )

    :eprof.log(
      ~c"/private/tmp/claude-501/-Users-yoavgea-repos/af457c08-e4d0-4151-9c38-37d8c74cf685/scratchpad/eprof_writer.txt"
    )

    :eprof.analyze(:total, sort: :time)
    :eprof.stop()
  end
end
