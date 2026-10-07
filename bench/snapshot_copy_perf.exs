Code.require_file("support/snapshot_copy_variant.exs", __DIR__)
Code.require_file("support/bootstrap_stall_probe.exs", __DIR__)

{variant, source, code_root} = FerricstoreBench.SnapshotCopyVariant.prepare()
root = Path.join([System.tmp_dir!(), "opencode", "snapshot-copy-data-#{System.pid()}"])
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
  :ok = FerricStore.set("snapshot-copy-seed", "value")
  shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
  payload = :binary.copy("x", 65_536)

  files =
    for branch <- 1..8, leaf <- 1..2 do
      relative = "nested/b#{branch}/a/b/c/file-#{leaf}"
      path = Path.join(shard_path, relative)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, payload)
      relative
    end

  started = System.monotonic_time(:microsecond)

  {:ok, {:raft_log_pos, index, term}} =
    if System.get_env("BENCH_BOOTSTRAP_OUTPUT") do
      FerricstoreBench.BootstrapStallProbe.observe_startup(root, fn ->
        Ferricstore.Raft.WARaftBackend.create_snapshot(0)
      end)
    else
      Ferricstore.Raft.WARaftBackend.create_snapshot(0)
    end

  us = System.monotonic_time(:microsecond) - started

  snapshot =
    Path.join([root, "waraft", "ferricstore_waraft_backend.1", "snapshot.#{index}.#{term}"])

  for relative <- files, do: ^payload = File.read!(Path.join([snapshot, "data", relative]))
  true = File.exists?(Path.join(snapshot, "ferricstore_snapshot.term"))

  report = %{
    variant: variant,
    elapsed_us: us,
    files: length(files),
    bytes: byte_size(payload) * length(files),
    source: source,
    storage_beam_md5:
      Base.encode16(Ferricstore.Raft.WARaftStorage.module_info(:md5), case: :lower),
    diagnostic: System.get_env("BENCH_BOOTSTRAP_OUTPUT") != nil,
    errors: 0,
    otp: System.otp_release(),
    elixir: System.version(),
    erl_flags: System.get_env("ERL_FLAGS")
  }

  File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))
  IO.inspect(Map.drop(report, [:source]), label: "SNAPSHOT_COPY")
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
