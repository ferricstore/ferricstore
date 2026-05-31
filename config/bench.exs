import Config

config :logger, level: :warning

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: false,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

config :ferricstore, :ferricstore_wal_nif,
  skip_compilation?: false,
  load_from: {:ferricstore, "priv/native/ferricstore_wal_nif"}

config :ferricstore, :port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_bench"

config :ferricstore,
  flow_async_history: true,
  wal_commit_delay_us: 6_000,
  waraft_commit_batch_max: 10_000,
  waraft_apply_log_batch_size: 4_096,
  flow_retention_sweeper_initial_delay_ms: 600_000,
  flow_retention_sweeper_interval_ms: 600_000
