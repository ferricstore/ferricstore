import Config

config :logger, level: :warning

build_native? = System.get_env("FERRICSTORE_BUILD") in ["1", "true"]

config :ferricstore, Ferricstore.Bitcask.NIF,
  skip_compilation?: not build_native?,
  load_from: {:ferricstore, "priv/native/ferricstore_bitcask"}

config :ferricstore, :ferricstore_wal_nif,
  skip_compilation?: not build_native?,
  load_from: {:ferricstore, "priv/native/ferricstore_wal_nif"}

config :ferricstore, :port, 0
config :ferricstore, :data_dir, System.tmp_dir!() <> "/ferricstore_bench"

config :ferricstore,
  flow_async_history: true,
  wal_commit_delay_us: 6_000,
  waraft_commit_batch_max: 10_000
