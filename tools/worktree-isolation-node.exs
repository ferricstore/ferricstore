# Private stdio worker for worktree-isolation-test.exs, not a network API.
[root] = System.argv()

Enum.each(
  ~w(FERRICSTORE_DATA_DIR FERRICSTORE_NATIVE_PORT FERRICSTORE_HEALTH_PORT FERRICSTORE_HEALTH_PROBE_PORT FERRICSTORE_HTTP_PORT FERRICSTORE_NODE_NAME),
  &System.delete_env/1
)

config = Config.Reader.read!(Path.join(root, "config/config.exs"), env: :dev, target: :host)
core = Keyword.fetch!(config, :ferricstore)

unless core[:data_dir] == Path.join(root, "data") and
         Enum.all?([:native_port, :health_port, :health_probe_port], &(core[&1] == 0)) and
         config[:ferricstore_http][:port] == 0 do
  raise "worktree configuration is not isolated; refusing to start"
end

Application.put_all_env(config)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore_http, :enabled, true)
{:ok, _} = Application.ensure_all_started(:ferricstore_server)
{:ok, _} = Application.ensure_all_started(:ferricstore_http)

info = fn ->
  %{
    data_dir: Application.fetch_env!(:ferricstore, :data_dir),
    ports: [
      FerricstoreServer.Native.Listener.port(),
      FerricstoreServer.Health.Endpoint.port(),
      FerricstoreServer.Health.ProbeEndpoint.port(),
      FerricstoreHttp.Listener.port()
    ]
  }
end

reply = fn value -> IO.puts("WORKTREE_REPLY " <> Base.encode64(:erlang.term_to_binary(value))) end
reply.({:ready, info.()})

IO.stream(:stdio, :line)
|> Enum.each(fn line ->
  result =
    case line |> String.trim() |> Base.decode64!() |> :erlang.binary_to_term([:safe]) do
      {:get, key} ->
        FerricStore.get(key)

      {:set, key, value} ->
        FerricStore.set(key, value)

      :info ->
        info.()

      :stop ->
        Enum.each([:ferricstore_http, :ferricstore_server, :ferricstore], &Application.stop/1)
        reply.(:ok)
        System.halt(0)
    end

  reply.(result)
end)
