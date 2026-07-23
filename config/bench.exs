import Config

config :logger, level: :warning

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: false,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

config :ferricstore, :ferricstore_wal_nif,
  skip_compilation?: false,
  load_from: {:ferricstore, "priv/native/ferricstore_wal_nif"}

config :ferricstore,
       :data_dir,
       System.get_env("FERRICSTORE_DATA_DIR", System.tmp_dir!() <> "/ferricstore_bench")

config :ferricstore,
  native_protocol_enabled: true,
  native_port: String.to_integer(System.get_env("FERRICSTORE_NATIVE_PORT", "0"))

config :ferricstore,
  flow_async_history: true,
  wal_commit_delay_us: 6_000,
  waraft_commit_batch_adaptive: true,
  waraft_commit_batch_max: 10_000,
  waraft_commit_priority: :high,
  waraft_generic_batch_window_ms: 0,
  waraft_apply_log_batch_size: 4_096,
  flow_retention_sweeper_initial_delay_ms: 600_000,
  flow_retention_sweeper_interval_ms: 600_000,
  flow_retention_sweeper_catchup_delay_ms: 100,
  flow_retention_sweeper_catchup_burst_limit: 8,
  flow_retention_sweeper_catchup_pause_ms: 1_000,
  flow_retention_sweeper_pressure_interval_ms: 1_000,
  flow_retention_sweeper_pressure_limit: 10_000,
  flow_retention_sweeper_pressure_compaction_interval_ms: 60_000,
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
