import Config

config :logger, level: :info

config :ferricstore, :native_port, 6388
config :ferricstore, :data_dir, "/var/lib/ferricstore/data"

# Node discovery via libcluster -- Kubernetes DNS strategy.
# Uncomment and configure for your Kubernetes deployment:
#
# config :libcluster,
#   topologies: [
#     k8s: [
#       strategy: Cluster.Strategy.Kubernetes.DNS,
#       config: [
#         service: "ferricstore-headless",
#         application_name: "ferricstore"
#       ]
#     ]
#   ]
