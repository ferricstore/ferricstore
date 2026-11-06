# bench/flow_memory_bench.exs
#
# Memory cost of Flow workflow commands: what each live Flow keeps in RAM, and
# how far memory peaks while the commands run.
#
# Creates FLOW_MEM_COUNT flows with FLOW.CREATE_MANY, claims them all with
# FLOW.CLAIM_DUE and completes them all with FLOW.COMPLETE_MANY. After each phase
# it garbage-collects every process and reports :erlang.memory/0 by category,
# the ETS tables and processes that grew most, and the per-flow cost. A sampler
# records the peak total memory during each phase.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/flow_memory_bench.exs
#
# Options:
#   FLOW_MEM_COUNT=100000
#   FLOW_MEM_BATCH=500
#   FLOW_MEM_PAYLOAD_BYTES=200
#   FLOW_MEM_SHARDS=1
#   FLOW_MEM_SETTLE_MS=3000   (wait for background projections before measuring)

count = System.get_env("FLOW_MEM_COUNT", "100000") |> String.to_integer()
batch = System.get_env("FLOW_MEM_BATCH", "500") |> String.to_integer()
payload_bytes = System.get_env("FLOW_MEM_PAYLOAD_BYTES", "200") |> String.to_integer()
shard_count = System.get_env("FLOW_MEM_SHARDS", "1") |> String.to_integer()
settle_ms = System.get_env("FLOW_MEM_SETTLE_MS", "3000") |> String.to_integer()

data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_mem_#{System.unique_integer([:positive])}")

File.mkdir_p!(data_dir)
Logger.configure(level: :warning)
Application.put_env(:ferricstore, :data_dir, data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)
{:ok, _} = Application.ensure_all_started(:ferricstore)

defmodule FlowMemoryBench do
  @partition "mem-bench"
  @type_name "mem-job"

  def run(count, batch, payload_bytes, settle_ms) do
    payload = String.duplicate("p", payload_bytes)
    ids = Enum.map(1..count, &("flow-" <> Integer.to_string(&1)))

    # One flow first, so the policy row, type tables and workers exist and the
    # baseline does not charge them to the measured flows.
    ok!(
      FerricStore.flow_create("warmup",
        type: @type_name,
        state: "queued",
        payload: payload,
        run_at_ms: 1_000,
        partition_key: @partition,
        now_ms: 1_000
      )
    )

    base = snapshot(settle_ms)

    created =
      phase("create", base, count, settle_ms, fn ->
        ids
        |> Enum.chunk_every(batch)
        |> Enum.each(fn chunk ->
          items =
            Enum.map(chunk, fn id ->
              {id, type: @type_name, state: "queued", payload: payload, run_at_ms: 1_000}
            end)

          ok!(FerricStore.flow_create_many(@partition, items, now_ms: 1_000))
        end)
      end)

    {claimed_snap, claimed} =
      phase_with_result("claim", created, count, settle_ms, fn ->
        claim_all(count + 1, batch, [])
      end)

    _completed =
      phase("complete", claimed_snap, count, settle_ms, fn ->
        claimed
        |> Enum.chunk_every(batch)
        |> Enum.each(fn chunk ->
          items =
            Enum.map(chunk, fn {id, lease_token, fencing_token} ->
              {id, lease_token, fencing_token: fencing_token, now_ms: 2_000}
            end)

          ok!(FerricStore.flow_complete_many(@partition, items))
        end)
      end)

    IO.puts("\n== whole run: create + claim + complete versus baseline")
    report(base, snapshot(settle_ms), count)
  end

  defp ok!(:ok), do: :ok
  defp ok!({:ok, _}), do: :ok

  defp claim_all(remaining, _batch, acc) when remaining <= 0, do: acc

  defp claim_all(remaining, batch, acc) do
    {:ok, claimed} =
      FerricStore.flow_claim_due(@type_name,
        state: "queued",
        worker: "w",
        lease_ms: 600_000,
        limit: min(batch, remaining),
        partition_key: @partition,
        now_ms: 1_500
      )

    # Keep only what completing needs, so the bench's own list stays small
    # next to the server memory it measures.
    case claimed do
      [] ->
        acc

      _ ->
        leases = Enum.map(claimed, &{&1.id, &1.lease_token, &1.fencing_token})
        claim_all(remaining - length(claimed), batch, leases ++ acc)
    end
  end

  defp phase(name, before, count, settle_ms, fun) do
    {snap, _result} = phase_with_result(name, before, count, settle_ms, fun)
    snap
  end

  defp phase_with_result(name, before, count, settle_ms, fun) do
    sampler = start_sampler()
    started = System.monotonic_time(:millisecond)
    result = fun.()
    elapsed = System.monotonic_time(:millisecond) - started
    {peak, per_process} = stop_sampler(sampler)
    snap = snapshot(settle_ms)

    IO.puts("\n== #{name}: #{count} flows in #{elapsed} ms")

    IO.puts(
      "peak total during phase: #{mb(peak)} MB (#{mb(peak - before.memory[:total])} MB above the phase start)"
    )

    IO.puts("largest per-process peaks during phase (heap+stack+binaries MB):")

    per_process
    |> Enum.sort_by(fn {_, bytes} -> -bytes end)
    |> Enum.take(6)
    |> Enum.each(fn {label, bytes} ->
      IO.puts("  #{String.pad_trailing(label, 70)} #{pad(mb(bytes))}")
    end)

    report(before, snap, count)
    {snap, result}
  end

  defp snapshot(settle_ms) do
    Process.sleep(settle_ms)
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    Process.sleep(200)

    %{
      memory:
        :erlang.memory()
        |> Map.new()
        |> Map.put(:rss, rss_bytes())
        |> Map.put(:carriers, carrier_bytes()),
      ets: ets_tables(),
      processes: processes()
    }
  end

  # The native ordered index (a Rust NIF resource) and LMDB's memory map are
  # outside :erlang.memory/0; process RSS includes them.
  defp rss_bytes do
    case System.cmd("ps", ["-o", "rss=", "-p", System.pid()]) do
      {out, 0} -> out |> String.trim() |> String.to_integer() |> Kernel.*(1024)
      _ -> 0
    end
  end

  # Memory the BEAM's allocators hold from the OS (in use or cached). RSS minus
  # this is native memory: NIF allocations and mapped LMDB pages.
  defp carrier_bytes do
    Enum.reduce(:erlang.system_info(:alloc_util_allocators), 0, fn alloc, total ->
      case :erlang.system_info({:allocator_sizes, alloc}) do
        instances when is_list(instances) ->
          Enum.reduce(instances, total, fn {:instance, _, sections}, acc ->
            Enum.reduce(sections, acc, fn
              {kind, values}, acc when kind in [:mbcs, :sbcs, :mbcs_pool] ->
                case List.keyfind(values, :carriers_size, 0) do
                  {:carriers_size, current, _, _} -> acc + current
                  _ -> acc
                end

              _, acc ->
                acc
            end)
          end)

        _ ->
          total
      end
    end)
  end

  defp ets_tables do
    word = :erlang.system_info(:wordsize)

    Enum.reduce(:ets.all(), %{}, fn table, acc ->
      case {:ets.info(table, :name), :ets.info(table, :memory), :ets.info(table, :size)} do
        {:undefined, _, _} ->
          acc

        {name, words, size} when is_integer(words) ->
          Map.update(acc, name, {words * word, size}, fn {bytes, rows} ->
            {bytes + words * word, rows + size}
          end)

        _ ->
          acc
      end
    end)
  end

  defp processes do
    Enum.reduce(Process.list(), %{}, fn pid, acc ->
      case Process.info(pid, [:registered_name, :dictionary, :memory, :binary]) do
        nil ->
          acc

        info ->
          binary = Enum.reduce(info[:binary], 0, fn {_, size, _}, sum -> sum + size end)

          Map.update(acc, process_label(pid, info), {info[:memory], binary}, fn {m, b} ->
            {m + info[:memory], b + binary}
          end)
      end
    end)
  end

  defp process_label(pid, info) do
    case info[:registered_name] do
      name when is_atom(name) and name != [] ->
        inspect(name)

      _ ->
        case Keyword.get(info[:dictionary], :"$initial_call") do
          {m, f, a} -> "#{inspect(m)}.#{f}/#{a}"
          _ -> inspect(:erlang.process_info(pid, :initial_call) |> elem(1))
        end
    end
  end

  defp report(before, now, count) do
    IO.puts("memory (MB, delta, bytes per flow):")

    for key <- [:rss, :carriers, :total, :processes, :ets, :binary, :atom, :code, :system] do
      delta = now.memory[key] - before.memory[key]

      IO.puts(
        "  #{String.pad_trailing(Atom.to_string(key), 10)} #{pad(mb(now.memory[key]))} #{pad(signed_mb(delta))} #{pad(div(delta, max(count, 1)))}"
      )
    end

    IO.puts("ETS tables by growth (bytes per flow, rows now):")

    now.ets
    |> Enum.map(fn {name, {bytes, rows}} ->
      {before_bytes, before_rows} = Map.get(before.ets, name, {0, 0})
      {name, bytes - before_bytes, rows, rows - before_rows}
    end)
    |> Enum.sort_by(fn {_, delta, _, _} -> -abs(delta) end)
    |> Enum.take(15)
    |> Enum.each(fn {name, delta, rows, row_delta} ->
      IO.puts(
        "  #{String.pad_trailing(inspect(name), 58)} #{pad(signed_mb(delta))} MB #{pad(div(delta, max(count, 1)))} B/flow rows=#{rows} (#{signed(row_delta)})"
      )
    end)

    IO.puts("processes by growth (heap+stack MB delta, referenced binary MB delta):")

    now.processes
    |> Enum.map(fn {label, {mem, bin}} ->
      {before_mem, before_bin} = Map.get(before.processes, label, {0, 0})
      {label, mem - before_mem, bin - before_bin}
    end)
    |> Enum.sort_by(fn {_, mem, bin} -> -(abs(mem) + abs(bin)) end)
    |> Enum.take(10)
    |> Enum.each(fn {label, mem, bin} ->
      IO.puts("  #{String.pad_trailing(label, 70)} #{pad(signed_mb(mem))} #{pad(signed_mb(bin))}")
    end)
  end

  # Samples total memory every 5 ms and each process's memory every 50 ms, so a
  # phase reports which processes caused its peak.
  defp start_sampler do
    parent = self()
    me = self()

    spawn_link(fn ->
      loop = fn loop, peak, per_process, tick ->
        receive do
          :stop -> send(parent, {:peak, peak, per_process})
        after
          5 ->
            per_process =
              if rem(tick, 10) == 0, do: sample_processes(per_process, me), else: per_process

            loop.(loop, max(peak, :erlang.memory(:total)), per_process, tick + 1)
        end
      end

      loop.(loop, :erlang.memory(:total), %{}, 0)
    end)
  end

  defp sample_processes(per_process, skip) do
    Enum.reduce(Process.list(), per_process, fn pid, acc ->
      with true <- pid != skip and pid != self(),
           [registered_name: _, dictionary: _, memory: memory, binary: binary] = info <-
             Process.info(pid, [:registered_name, :dictionary, :memory, :binary]) do
        bin = Enum.reduce(binary, 0, fn {_, size, _}, sum -> sum + size end)
        Map.update(acc, process_label(pid, info), memory + bin, &max(&1, memory + bin))
      else
        _ -> acc
      end
    end)
  end

  defp stop_sampler(pid) do
    send(pid, :stop)

    receive do
      {:peak, peak, per_process} -> {peak, per_process}
    end
  end

  defp mb(bytes), do: Float.round(bytes / 1_048_576, 1)
  defp signed_mb(bytes), do: if(bytes >= 0, do: "+", else: "") <> Float.to_string(mb(bytes))
  defp signed(n), do: if(n >= 0, do: "+#{n}", else: "#{n}")
  defp pad(value), do: value |> to_string() |> String.pad_leading(9)
end

IO.puts("=== Flow memory bench ===")

IO.puts(
  "count=#{count} batch=#{batch} payload=#{payload_bytes}B shards=#{shard_count} data_dir=#{data_dir}"
)

FlowMemoryBench.run(count, batch, payload_bytes, settle_ms)
