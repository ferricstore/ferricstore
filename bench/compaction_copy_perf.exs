Code.require_file("support/compaction_sync_variant.exs", __DIR__)
{mode, source, code_root} = FerricstoreBench.CompactionSyncVariant.prepare()
root = Path.join([System.tmp_dir!(), "opencode", "compaction-copy-#{System.pid()}"])
if File.exists?(root), do: raise("copy fixture exists")
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :node_name, nil)
Logger.configure(level: :error)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore)
  ctx = FerricStore.Instance.get(:default)
  key = "copy-component-hash"
  value = :binary.copy("v", 4_096)

  for fields <- Enum.chunk_every(1..4_096, 64) do
    {:ok, _} = FerricStore.Impl.hset(ctx, key, Map.new(fields, &{"field-#{&1}", value}))
  end

  shard = Ferricstore.Store.Router.shard_name(ctx, Ferricstore.Store.Router.shard_for(ctx, key))
  true = GenServer.call(shard, {:promoted?, key})

  reports =
    for round <- 0..3 do
      parent = self()

      :sys.replace_state(shard, fn state ->
        path = state.promoted_instances[key].path
        counter = make_ref()
        Process.put(counter, 0)

        Process.put(:ferricstore_promoted_compaction_fsync_file_hook, fn file ->
          Process.put(counter, Process.get(counter) + 1)
          Ferricstore.Bitcask.NIF.v2_fsync(file)
        end)

        started = System.monotonic_time(:microsecond)

        try do
          {:ok, state} =
            Ferricstore.Store.Shard.Compound.compact_dedicated_result(state, key, path)

          send(
            parent,
            {:copy_report,
             %{
               round: round,
               duration_us: System.monotonic_time(:microsecond) - started,
               explicit_file_syncs: Process.get(counter)
             }}
          )

          state
        after
          Process.delete(:ferricstore_promoted_compaction_fsync_file_hook)
          Process.delete(counter)
        end
      end)

      receive do
        {:copy_report, report} -> report
      end
    end

  for i <- [1, 2_048, 4_096], do: {:ok, ^value} = FerricStore.Impl.hget(ctx, key, "field-#{i}")

  report = %{
    component_only: true,
    mode: mode,
    source: source,
    rounds: reports,
    errors: 0,
    fields: 4_096,
    value_bytes: 4_096,
    beam_md5:
      Base.encode16(Ferricstore.Store.Shard.Compound.Promoted.module_info(:md5), case: :lower)
  }

  File.write!(
    System.get_env("BENCH_OUTPUT", "bench/results/compaction-copy-#{mode}.json"),
    Jason.encode!(report, pretty: true)
  )

  IO.inspect(Map.drop(report, [:source]))
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
