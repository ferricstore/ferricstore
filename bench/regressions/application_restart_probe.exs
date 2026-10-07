Code.require_file("../support/bootstrap_stall_probe.exs", __DIR__)
root = Path.join([System.tmp_dir!(), "opencode", "application-restart-probe-#{System.pid()}"])
if File.exists?(root), do: raise("fixture exists")

for {key, value} <- [data_dir: root, node_name: nil, shard_count: 4],
    do: Application.put_env(:ferricstore, key, value)

Logger.configure(level: :error)
{:ok, _} = Application.ensure_all_started(:telemetry)

result =
  FerricstoreBench.BootstrapStallProbe.observe_startup(root, fn ->
    task =
      Task.async(fn ->
        {:ok, _} = Application.ensure_all_started(:ferricstore)
        ctx = FerricStore.Instance.get(:default)
        :ok = Ferricstore.Raft.WARaftBackend.start(ctx)
        :ok = Application.stop(:ferricstore)
        Application.put_env(:ferricstore, :shard_count, :invalid)
        {:error, _} = Application.ensure_all_started(:ferricstore)
        Application.put_env(:ferricstore, :shard_count, 4)
        Application.ensure_all_started(:ferricstore)
      end)

    case Task.yield(task, 30_000) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)
        {:error, :restart_probe_timeout}
    end
  end)

if match?({:ok, _}, result) do
  Application.stop(:ferricstore)
  File.rm_rf!(root)
else
  System.halt(1)
end
