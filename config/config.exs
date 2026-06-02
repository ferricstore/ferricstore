import Config

# Repo checkouts build native crates from source by default. Packaged
# dependency users still get RustlerPrecompiled artifacts because dependency
# config files are not imported into the parent application.
config :rustler_precompiled, :force_build, ferricstore: true

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: false,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

config :ferricstore, :ferricstore_wal_nif,
  skip_compilation?: false,
  load_from: {:ferricstore, "priv/native/ferricstore_wal_nif"}

# TCP server port (default: 6379, matches Redis)
config :ferricstore, :port, 6379

# Data directory for Bitcask shards
config :ferricstore, :data_dir, "data"

# Number of shards (0 = auto-detect from CPU cores)
config :ferricstore, :shard_count, 0

# Production durability path. WARaft with async segment append is the only
# runtime backend. Keep this simple: no Ra/WARaft mode flag in normal config.
config :ferricstore,
  flow_async_history: true,
  wal_commit_delay_us: 6_000,
  waraft_commit_batch_max: 10_000,
  waraft_apply_log_batch_size: 4_096

# Flow retention cleanup is correctness-safe as a normal durable Flow command,
# but it can be storage-heavy. Run it on a maintenance cadence by default so
# hot claim/transition workloads are not interrupted every minute.
config :ferricstore,
  flow_retention_sweeper_initial_delay_ms: 600_000,
  flow_retention_sweeper_interval_ms: 600_000,
  flow_retention_sweeper_pressure_interval_ms: 1_000,
  flow_retention_sweeper_pressure_limit: 10_000,
  flow_retention_sweeper_pressure_compaction_interval_ms: 60_000

# Operational guardrails are derived from the actual node/container memory and
# the filesystem backing `:data_dir`. These ratios control when cleanup becomes
# aggressive and when new writes are rejected cleanly instead of allowing RSS or
# disk growth to collapse the node.
config :ferricstore,
  operational_guard_enabled: true,
  operational_guard_interval_ms: 1_000,
  operational_rss_warn_ratio: 0.70,
  operational_rss_pressure_ratio: 0.80,
  operational_rss_reject_ratio: 0.88,
  operational_rss_panic_ratio: 0.94,
  operational_disk_warn_ratio: 0.70,
  operational_disk_pressure_ratio: 0.80,
  operational_disk_reject_ratio: 0.90,
  operational_disk_panic_ratio: 0.95

# LFU decay: minutes per decay step (0 = no decay). Matches Redis lfu-decay-time.
config :ferricstore, :lfu_decay_time, 1
# LFU log factor: controls probabilistic increment curve. Matches Redis lfu-log-factor.
config :ferricstore, :lfu_log_factor, 10

# Sendfile zero-copy threshold for GET responses in standalone TCP mode.
# Values >= this size are served via :file.sendfile/5 (kernel zero-copy)
# instead of reading into BEAM memory. Only applies to cold (on-disk) keys
# over plain TCP (:ranch_tcp); TLS and hot keys always use the normal path.
config :ferricstore_server, :sendfile_threshold, 65_536

# Large values at or above this size are stored in per-shard append blob
# segments with a small Bitcask reference. A conservative sweeper cleans stale
# tmp/legacy blob files and whole append segments once no live ref points into
# them. Partial segment compaction is intentionally a separate maintenance step.
config :ferricstore,
  blob_side_channel_threshold_bytes: 256 * 1024,
  blob_segment_max_bytes: 256 * 1024 * 1024,
  blob_gc_sweeper_enabled: true,
  blob_gc_sweeper_initial_delay_ms: 60_000,
  blob_gc_sweeper_interval_ms: 600_000

# Node discovery via libcluster.
# Default: Gossip strategy for local/dev multi-node clusters.
# Override in prod.exs or runtime.exs for Kubernetes DNS or other strategies.
config :libcluster,
  topologies: [
    ferricstore: [
      strategy: Cluster.Strategy.Gossip,
      config: [
        port: 45892,
        if_addr: "0.0.0.0",
        multicast_if: "0.0.0.0",
        multicast_addr: "230.1.1.251",
        multicast_ttl: 1
      ]
    ]
  ]

import_config "#{config_env()}.exs"
