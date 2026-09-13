# Run with: mix run --no-halt scripts/run_dev.exs
IO.puts("FerricStore development instance")
IO.puts("  Data:      #{Application.fetch_env!(:ferricstore, :data_dir)}")
IO.puts("  Node:      #{node()}")
IO.puts("  Native:    ferric://127.0.0.1:#{FerricstoreServer.Native.Listener.port()}")
IO.puts("  Dashboard: http://127.0.0.1:#{FerricstoreServer.Health.Endpoint.port()}/dashboard")

IO.puts(
  "  Probe:     http://127.0.0.1:#{FerricstoreServer.Health.ProbeEndpoint.port()}/health/ready"
)

if Process.whereis(FerricstoreHttp.Listener) do
  {:ok, config} = FerricstoreHttp.Config.load()
  scheme = if config.tls.enabled, do: "https", else: "http"
  IO.puts("  HTTP API:  #{scheme}://127.0.0.1:#{FerricstoreHttp.Listener.port()}")
end
