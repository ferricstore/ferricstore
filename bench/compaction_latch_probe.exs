# Controlled page-work delay; timings are diagnostic, with durable appends intact.
Code.require_file("support/compaction_latch_variant.exs", __DIR__)
alias FerricStore.Impl
alias Ferricstore.Store.{Promotion, Router}
alias Ferricstore.Store.Shard.Compound, as: Compound

{mode, loaded_source, code_root} = FerricstoreBench.CompactionLatchVariant.prepare()
fields = System.get_env("BENCH_FIELDS", "4096") |> String.to_integer()
delay_ms = System.get_env("BENCH_PAGE_DELAY_MS", "20") |> String.to_integer()
root = Path.join([System.tmp_dir!(), "opencode", "compaction-latch-probe-#{System.pid()}"])
if File.exists?(root), do: raise("fixture exists")
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :node_name, nil)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore, :waraft_single_hset_coalescing, false)
Logger.configure(level: :error)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore)
  ctx = FerricStore.Instance.get(:default)
  key = "compaction-latch:hash"
  value = :binary.copy("s", 4096)

  for page <- Enum.chunk_every(1..fields, 64) do
    {:ok, _} = Impl.hset(ctx, key, Map.new(page, &{"seed-#{&1}", value}))
  end

  shard = Router.shard_name(ctx, 0)
  true = GenServer.call(shard, {:promoted?, key})
  state = :sys.get_state(shard)
  path = state.promoted_instances[key].path
  parent = self()
  started = System.monotonic_time(:microsecond)

  compactor =
    Task.async(fn ->
      Process.put(:probe_pages, 0)

      Process.put(:ferricstore_promoted_compaction_after_collect_hook, fn _, entries ->
        if Enum.any?(entries, fn {key, _, _, _} -> String.starts_with?(key, "H:") end) do
          count = Process.get(:probe_pages) + 1
          Process.put(:probe_pages, count)
          if count == 1, do: send(parent, :first_page)
          Process.sleep(delay_ms)
        end
      end)

      Process.put(:ferricstore_separated_compaction_hook, fn
        :before_copy_page, _job ->
          count = Process.get(:probe_pages) + 1
          Process.put(:probe_pages, count)
          if count == 1, do: send(parent, :first_page)
          Process.sleep(delay_ms)
          :ok

        _, _ ->
          :ok
      end)

      result =
        case mode do
          "whole" ->
            Compound.compact_dedicated_result(state, key, path)

          "separate" ->
            token = Promotion.acquire_compaction_latch(state, key)

            try do
              Ferricstore.Store.Shard.Compound.SeparatedCompaction.run(state, key, path, token)
            after
              {table, latch_key} = token
              :ets.delete_object(table, {latch_key, self()})
            end

          candidate when candidate in ["pages", "single_page"] ->
            token = Promotion.acquire_compaction_latch(state, key)

            try do
              Compound.compact_dedicated_result_latched(state, key, path, token)
            after
              {table, latch_key} = token
              :ets.delete_object(table, {latch_key, self()})
            end
        end

      {:ok, _} = result

      %{
        duration_us: System.monotonic_time(:microsecond) - started,
        pages: Process.get(:probe_pages),
        finished_us: System.monotonic_time(:microsecond)
      }
    end)

  receive do
    :first_page -> :ok
  after
    30_000 -> raise("compaction did not start")
  end

  write_started = System.monotonic_time(:microsecond)
  {:ok, 0} = Impl.hset(ctx, key, %{"seed-1" => "new-value"})
  write_finished = System.monotonic_time(:microsecond)
  result = Task.await(compactor, 30_000)
  {:ok, "new-value"} = Impl.hget(ctx, key, "seed-1")
  for field <- ["seed-2", "seed-#{fields}"], do: {:ok, ^value} = Impl.hget(ctx, key, field)

  report = %{
    mode: mode,
    diagnostic_only: true,
    page_delay_ms: delay_ms,
    fields: fields,
    compaction: result,
    write_us: write_finished - write_started,
    write_finished_before_compaction: write_finished < result.finished_us,
    errors: 0,
    promoted_source:
      if(is_map(loaded_source) and Map.has_key?(loaded_source, :promoted),
        do: loaded_source.promoted,
        else: File.read!("apps/ferricstore/lib/ferricstore/store/shard/compound/promoted.ex")
      ),
    shard_info_source: if(is_map(loaded_source), do: loaded_source.info, else: loaded_source),
    separated_source: Map.get(if(is_map(loaded_source), do: loaded_source, else: %{}), :separate)
  }

  File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))

  IO.inspect(Map.drop(report, [:promoted_source, :shard_info_source, :separated_source]),
    label: "LATCH_PROBE"
  )
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
