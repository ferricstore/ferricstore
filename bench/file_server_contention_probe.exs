# Controlled shared file-server contention inside a disposable VM and fixture.
Code.require_file("support/metadata_route_variant.exs", __DIR__)
{route, source, promotion_source, code_root} = FerricstoreBench.MetadataRouteVariant.prepare()
root = Path.join([System.tmp_dir!(), "opencode", "file-server-probe-#{System.pid()}"])
if File.exists?(root), do: raise("fixture exists")

for {key, value} <- [
      data_dir: root,
      node_name: nil,
      shard_count: 1,
      waraft_single_hset_coalescing: false
    ],
    do: Application.put_env(:ferricstore, key, value)

Logger.configure(level: :error)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore)
  ctx = FerricStore.Instance.get(:default)
  hash = "contention-hash"
  {:ok, _} = FerricStore.Impl.hset(ctx, hash, Map.new(1..128, &{"seed-#{&1}", "seed"}))
  shard = Ferricstore.Store.Router.shard_name(ctx, 0)
  true = GenServer.call(shard, {:promoted?, hash})
  {:ok, 1} = FerricStore.Impl.hset(ctx, hash, %{"field" => "before"})
  started = System.monotonic_time(:microsecond)
  :ok = :sys.suspend(:file_server_2)
  task = Task.async(fn -> FerricStore.Impl.hset(ctx, hash, %{"field" => "after"}) end)

  early =
    try do
      Task.yield(task, 300)
    after
      :sys.resume(:file_server_2)
    end

  result =
    case early do
      nil -> Task.await(task, 10_000)
      {:ok, result} -> result
    end

  elapsed = System.monotonic_time(:microsecond) - started
  {:ok, 0} = result
  {:ok, "after"} = FerricStore.Impl.hget(ctx, hash, "field")

  report = %{
    route: route,
    completed_while_file_server_suspended: early != nil,
    elapsed_us: elapsed,
    suspension_budget_ms: 300,
    errors: 0,
    source: source,
    promotion_source: promotion_source
  }

  File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))
  IO.inspect(Map.drop(report, [:source, :promotion_source]), label: "FILE_SERVER_CONTENTION")
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
