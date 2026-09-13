import Config

Code.require_file(Path.join(__DIR__, "dev_runtime.exs"))

dev_runtime = Ferricstore.DevRuntime.settings(__DIR__)

if is_binary(dev_runtime.isolation_warning) do
  IO.warn("FerricStore development runtime: #{dev_runtime.isolation_warning}")
end

config :ferricstore,
  native_port: dev_runtime.native_port,
  health_port: dev_runtime.health_port,
  health_probe_port: dev_runtime.health_probe_port,
  data_dir: dev_runtime.data_dir,
  # A linked worktree stays standalone: no Node.start/epmd or libcluster
  # discovery is enabled merely to make its VM identity unique.
  node_name: nil,
  cluster_nodes: [],
  cluster_auto_join: false,
  dev_worktree?: dev_runtime.linked_worktree?

config :ferricstore_http, :port, dev_runtime.http_port

# Development VMs are intentionally standalone. `:nonode@nohost` is local to
# each VM and cannot collide with another checkout's node identity.
config :libcluster, topologies: :disabled
