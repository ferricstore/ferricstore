# MIX_ENV=test ERL_FLAGS='+S 8:8' BENCH_PROMOTED_READ=cached \
#   mise exec -- mix run --no-start bench/regressions/promoted_cached_read_atomicity.exs
Code.require_file("../support/promoted_read_variant.exs", __DIR__)
{variant, _source, code_root} = FerricstoreBench.PromotedReadVariant.prepare()
root = Path.join([System.tmp_dir!(), "opencode", "promoted-atomicity-#{System.pid()}"])
if File.exists?(root), do: raise("fixture exists")
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :node_name, nil)
Logger.configure(level: :error)
{:ok, _} = Application.ensure_all_started(:ferricstore)
writer_scope = System.get_env("BENCH_WRITER_SCOPE", "direct")
updates = System.get_env("BENCH_UPDATES", "100") |> String.to_integer()
true = writer_scope in ["direct", "default", "grouped"]

ctx =
  if writer_scope == "direct",
    do: Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, promotion_threshold: 1),
    else: FerricStore.Instance.get(:default)

try do
  alias Ferricstore.Store.{CompoundKey, Router}
  key = "atomicity-hash"
  type = CompoundKey.type_key(key)
  fields = Enum.map(1..512, &CompoundKey.hash_field(key, "field-#{&1}"))
  first = hd(fields)
  last = List.last(fields)
  :ok = Router.compound_put(ctx, key, type, "hash", 0)
  :ok = Router.compound_batch_put(ctx, key, Enum.map(fields, &{&1, <<0::unsigned-64>>, 0}))
  shard = Router.shard_name(ctx, Router.shard_for(ctx, key))

  Ferricstore.Test.ShardHelpers.eventually(
    fn ->
      GenServer.call(shard, {:promoted?, key})
    end,
    "hash was not promoted"
  )

  :sys.replace_state(shard, fn state ->
    info =
      Map.update!(
        state.promoted_instances,
        key,
        &Map.put(&1, :last_compacted_at, System.monotonic_time(:millisecond) + 3_600_000)
      )

    %{state | promoted_instances: info}
  end)

  parent = self()

  reader =
    Task.async(fn ->
      read = fn read, count ->
        receive do
          :stop -> %{samples: count, regression: nil}
        after
          0 ->
            <<a::unsigned-64>> = Router.compound_get(ctx, key, first)
            <<b::unsigned-64>> = Router.compound_get(ctx, key, last)
            if a > b, do: %{samples: count + 1, regression: {a, b}}, else: read.(read, count + 1)
        end
      end

      send(parent, :ready)
      read.(read, 0)
    end)

  receive do
    :ready -> :ok
  end

  for version <- 1..updates do
    if writer_scope == "grouped" do
      commands = Enum.map(1..512, &{:hset_single, key, "field-#{&1}", <<version::unsigned-64>>})

      {:ok, replies} =
        Ferricstore.Raft.WARaftBackend.write_batch(Router.shard_for(ctx, key), commands)

      true = replies == List.duplicate(0, 512)
    else
      :ok =
        Router.compound_batch_put(ctx, key, Enum.map(fields, &{&1, <<version::unsigned-64>>, 0}))
    end
  end

  send(reader.pid, :stop)
  result = Task.await(reader, 30_000)

  IO.inspect(%{variant: variant, writer_scope: writer_scope, result: result},
    label: "PROMOTED_ATOMICITY"
  )

  report = %{
    variant: variant,
    writer_scope: writer_scope,
    updates: updates,
    publication_identity: FerricstoreBench.PromotedReadVariant.source_identity(),
    router_beam_md5: Base.encode16(Router.module_info(:md5), case: :lower),
    samples: result.samples,
    regression: if(result.regression, do: Tuple.to_list(result.regression), else: nil)
  }

  File.write!(
    System.get_env(
      "BENCH_OUTPUT",
      "bench/results/promoted-cached-atomicity-#{writer_scope}-#{variant}.json"
    ),
    Jason.encode!(report, pretty: true)
  )

  if result.regression != nil,
    do: raise("promoted batch publication allowed sequential field versions to go backwards")
after
  if writer_scope == "direct", do: Ferricstore.Test.IsolatedInstance.checkin(ctx)
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
