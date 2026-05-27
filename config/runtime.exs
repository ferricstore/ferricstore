import Config

if config_env() == :prod do
  boolean_env = fn name, default ->
    case System.get_env(name) do
      nil ->
        default

      value ->
        case String.downcase(String.trim(value)) do
          value when value in ["1", "true", "yes", "y", "on"] ->
            true

          value when value in ["0", "false", "no", "n", "off"] ->
            false

          _other ->
            raise "#{name} must be a boolean: true/false, 1/0, yes/no, or on/off"
        end
    end
  end

  log_level =
    case String.downcase(System.get_env("FERRICSTORE_LOG_LEVEL", "info")) do
      "debug" -> :debug
      "info" -> :info
      "notice" -> :notice
      "warning" -> :warning
      "warn" -> :warning
      "error" -> :error
      _other -> :info
    end

  limit_env = fn name ->
    case System.get_env(name) do
      nil ->
        nil

      value ->
        case String.downcase(String.trim(value)) do
          "" ->
            nil

          value when value in ["false", "off", "infinity", "inf", "unlimited"] ->
            :infinity

          value ->
            case Integer.parse(value) do
              {parsed, ""} when parsed >= 0 -> parsed
              _other -> raise "#{name} must be a non-negative integer or infinity/off/false"
            end
        end
    end
  end

  config :logger, level: log_level

  # ---------------------------------------------------------------------------
  # Core
  # ---------------------------------------------------------------------------
  config :ferricstore,
    protected_mode: true,
    port: String.to_integer(System.get_env("FERRICSTORE_PORT", "6379")),
    health_port: String.to_integer(System.get_env("FERRICSTORE_HEALTH_PORT", "6380")),
    data_dir: System.get_env("FERRICSTORE_DATA_DIR", "/data"),
    shard_count:
      (
        count = System.get_env("FERRICSTORE_SHARD_COUNT", "0")
        count = String.to_integer(count)
        if count == 0, do: System.schedulers_online(), else: count
      )

  # ---------------------------------------------------------------------------
  # Memory & Eviction
  # ---------------------------------------------------------------------------
  config :ferricstore,
    max_memory_bytes: String.to_integer(System.get_env("FERRICSTORE_MAX_MEMORY", "0")),
    keydir_max_ram: String.to_integer(System.get_env("FERRICSTORE_KEYDIR_MAX_RAM", "268435456")),
    eviction_policy: System.get_env("FERRICSTORE_EVICTION_POLICY", "volatile_lru"),
    max_value_size: String.to_integer(System.get_env("FERRICSTORE_MAX_VALUE_SIZE", "1048576")),
    hot_cache_max_value_size:
      String.to_integer(System.get_env("FERRICSTORE_HOT_CACHE_MAX_VALUE_SIZE", "65536")),
    blob_side_channel_threshold_bytes:
      String.to_integer(System.get_env("FERRICSTORE_BLOB_SIDE_CHANNEL_THRESHOLD_BYTES", "262144")),
    blob_segment_max_bytes:
      String.to_integer(System.get_env("FERRICSTORE_BLOB_SEGMENT_MAX_BYTES", "268435456")),
    blob_gc_sweeper_enabled: boolean_env.("FERRICSTORE_BLOB_GC_SWEEPER_ENABLED", true),
    blob_gc_sweeper_initial_delay_ms:
      String.to_integer(System.get_env("FERRICSTORE_BLOB_GC_SWEEPER_INITIAL_DELAY_MS", "60000")),
    blob_gc_sweeper_interval_ms:
      String.to_integer(System.get_env("FERRICSTORE_BLOB_GC_SWEEPER_INTERVAL_MS", "600000")),
    max_active_file_size:
      String.to_integer(System.get_env("FERRICSTORE_MAX_ACTIVE_FILE_SIZE", "8589934592")),
    flow_lmdb_map_size:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_MAP_SIZE", "68719476736")),
    flow_lmdb_flush_interval_ms:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_FLUSH_INTERVAL_MS", "1000")),
    flow_lmdb_max_batch_ops:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_MAX_BATCH_OPS", "25000")),
    flow_lmdb_flush_chunk_ops:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_FLUSH_CHUNK_OPS", "10000")),
    flow_lmdb_flush_chunk_pause_ms:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_FLUSH_CHUNK_PAUSE_MS", "1")),
    flow_lmdb_flush_jitter_ms:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_FLUSH_JITTER_MS", "250")),
    flow_lmdb_max_concurrent_flushes:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_MAX_CONCURRENT_FLUSHES", "1")),
    flow_async_history: true,
    memory_guard_interval_ms:
      String.to_integer(System.get_env("FERRICSTORE_MEMORY_GUARD_INTERVAL_MS", "5000"))

  # These override adaptive memory-budget caps. The apply-projection cache is
  # the important Flow/LMDB-adjacent one: it buffers WARaft-applied Flow
  # state/value rows until lagged LMDB/history projection can consume them.
  # Too low a value forces synchronous spill/compaction during terminal-flow
  # bursts; tune it with the DBOS-style 1M Flow benchmark, not by lowering it
  # to reduce steady-state LMDB lag.
  memory_budget_overrides =
    [
      flow_history_projector_max_pending_entries:
        limit_env.("FERRICSTORE_FLOW_HISTORY_PROJECTOR_MAX_PENDING_ENTRIES"),
      flow_lmdb_writer_max_mailbox_messages:
        limit_env.("FERRICSTORE_FLOW_LMDB_WRITER_MAX_MAILBOX_MESSAGES"),
      flow_lmdb_writer_max_enqueue_ops:
        limit_env.("FERRICSTORE_FLOW_LMDB_WRITER_MAX_ENQUEUE_OPS"),
      waraft_segment_log_max_ets_bytes:
        limit_env.("FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES"),
      waraft_segment_log_max_ets_entries:
        limit_env.("FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_ENTRIES"),
      waraft_segment_log_min_ets_entries:
        limit_env.("FERRICSTORE_WARAFT_SEGMENT_LOG_MIN_ETS_ENTRIES"),
      waraft_apply_projection_cache_max_entries:
        limit_env.("FERRICSTORE_WARAFT_APPLY_PROJECTION_CACHE_MAX_ENTRIES")
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)

  if memory_budget_overrides != [] do
    config :ferricstore, memory_budget_overrides
  end

  # ---------------------------------------------------------------------------
  # LFU Scoring
  # ---------------------------------------------------------------------------
  config :ferricstore,
    lfu_decay_time: String.to_integer(System.get_env("FERRICSTORE_LFU_DECAY_TIME", "1")),
    lfu_log_factor: String.to_integer(System.get_env("FERRICSTORE_LFU_LOG_FACTOR", "10")),
    read_sample_rate: String.to_integer(System.get_env("FERRICSTORE_READ_SAMPLE_RATE", "100"))

  # ---------------------------------------------------------------------------
  # Expiry
  # ---------------------------------------------------------------------------
  config :ferricstore,
    expiry_sweep_interval_ms:
      String.to_integer(System.get_env("FERRICSTORE_EXPIRY_SWEEP_INTERVAL_MS", "5000")),
    expiry_max_keys_per_sweep:
      String.to_integer(System.get_env("FERRICSTORE_EXPIRY_MAX_KEYS_PER_SWEEP", "20"))

  # ---------------------------------------------------------------------------
  # Slow Log
  # ---------------------------------------------------------------------------
  config :ferricstore,
    slowlog_log_slower_than_us:
      String.to_integer(System.get_env("FERRICSTORE_SLOWLOG_SLOWER_THAN_US", "10000")),
    slowlog_max_len: String.to_integer(System.get_env("FERRICSTORE_SLOWLOG_MAX_LEN", "128"))

  # ---------------------------------------------------------------------------
  # Security
  # ---------------------------------------------------------------------------
  config :ferricstore,
    protected_mode: boolean_env.("FERRICSTORE_PROTECTED_MODE", true),
    maxclients: String.to_integer(System.get_env("FERRICSTORE_MAXCLIENTS", "10000")),
    audit_log_enabled: boolean_env.("FERRICSTORE_AUDIT_LOG", false),
    acl_auto_save: boolean_env.("FERRICSTORE_ACL_AUTO_SAVE", false)

  # ---------------------------------------------------------------------------
  # TLS (optional)
  # ---------------------------------------------------------------------------
  tls_cert = System.get_env("FERRICSTORE_TLS_CERT_FILE")

  if tls_cert do
    config :ferricstore,
      tls_port: String.to_integer(System.get_env("FERRICSTORE_TLS_PORT", "6380")),
      tls_cert_file: tls_cert,
      tls_key_file: System.get_env("FERRICSTORE_TLS_KEY_FILE"),
      tls_ca_cert_file: System.get_env("FERRICSTORE_TLS_CA_CERT_FILE"),
      require_tls: boolean_env.("FERRICSTORE_REQUIRE_TLS", false)
  end

  # ---------------------------------------------------------------------------
  # Connection
  # ---------------------------------------------------------------------------
  socket_mode = System.get_env("FERRICSTORE_SOCKET_ACTIVE_MODE", "true")

  config :ferricstore,
    socket_active_mode:
      (case socket_mode do
         "true" -> true
         "once" -> :once
         n -> String.to_integer(n)
       end),
    tcp_nodelay: boolean_env.("FERRICSTORE_TCP_NODELAY", true),
    tcp_recbuf: String.to_integer(System.get_env("FERRICSTORE_TCP_RECBUF", "131072")),
    tcp_sndbuf: String.to_integer(System.get_env("FERRICSTORE_TCP_SNDBUF", "131072"))

  # ---------------------------------------------------------------------------
  # Replication / Internals
  # ---------------------------------------------------------------------------
  config :ferricstore,
    release_cursor_interval:
      String.to_integer(System.get_env("FERRICSTORE_RELEASE_CURSOR_INTERVAL", "200000")),
    raft_batcher_max_pending:
      String.to_integer(System.get_env("FERRICSTORE_RAFT_BATCHER_MAX_PENDING", "256")),
    raft_batcher_max_pending_bytes:
      String.to_integer(System.get_env("FERRICSTORE_RAFT_BATCHER_MAX_PENDING_BYTES", "0")),
    raft_batcher_max_batch_bytes:
      String.to_integer(System.get_env("FERRICSTORE_RAFT_BATCHER_MAX_BATCH_BYTES", "4194304")),
    raft_pipeline_priority: System.get_env("FERRICSTORE_RAFT_PIPELINE_PRIORITY", "low"),
    raft_direct_batch_commands: System.get_env("FERRICSTORE_RAFT_DIRECT_BATCH_COMMANDS", "true"),
    raft_compact_hot_batches: System.get_env("FERRICSTORE_RAFT_COMPACT_HOT_BATCHES", "true"),
    raft_put_batch_apply_fast_path:
      System.get_env("FERRICSTORE_RAFT_PUT_BATCH_APPLY_FAST_PATH", "true"),
    raft_delete_batch_apply_fast_path:
      System.get_env("FERRICSTORE_RAFT_DELETE_BATCH_APPLY_FAST_PATH", "true"),
    waraft_log_rotation_interval:
      String.to_integer(System.get_env("FERRICSTORE_WARAFT_LOG_ROTATION_INTERVAL", "50000")),
    waraft_log_rotation_keep:
      String.to_integer(System.get_env("FERRICSTORE_WARAFT_LOG_ROTATION_KEEP", "100000")),
    waraft_max_retained_entries:
      String.to_integer(System.get_env("FERRICSTORE_WARAFT_MAX_RETAINED_ENTRIES", "100000")),
    waraft_commit_batch_max:
      String.to_integer(System.get_env("FERRICSTORE_WARAFT_COMMIT_BATCH_MAX", "10000")),
    promotion_threshold:
      String.to_integer(System.get_env("FERRICSTORE_PROMOTION_THRESHOLD", "100")),
    wal_commit_delay_us:
      String.to_integer(System.get_env("FERRICSTORE_WAL_COMMIT_DELAY_US", "6000"))

  # ---------------------------------------------------------------------------
  # Supervisor (test-only tuning, production defaults are fine)
  # ---------------------------------------------------------------------------
  config :ferricstore,
    supervisor_max_restarts: {
      String.to_integer(System.get_env("FERRICSTORE_MAX_RESTARTS", "20")),
      String.to_integer(System.get_env("FERRICSTORE_MAX_RESTARTS_SECONDS", "10"))
    }

  # ---------------------------------------------------------------------------
  # Clustering
  # ---------------------------------------------------------------------------

  node_name = System.get_env("FERRICSTORE_NODE_NAME")
  cookie = System.get_env("FERRICSTORE_COOKIE", "ferricstore")

  if node_name do
    config :ferricstore,
      node_name: String.to_atom(node_name),
      cookie: String.to_atom(cookie),
      cluster_role: String.to_atom(System.get_env("FERRICSTORE_CLUSTER_ROLE", "voter")),
      cluster_auto_join: boolean_env.("FERRICSTORE_CLUSTER_AUTO_JOIN", false),
      cluster_remove_delay_ms:
        String.to_integer(System.get_env("FERRICSTORE_CLUSTER_REMOVE_DELAY_MS", "60000"))

    # Static node list (alternative to libcluster auto-discovery)
    cluster_nodes = System.get_env("FERRICSTORE_CLUSTER_NODES", "")

    if cluster_nodes != "" do
      config :ferricstore,
             :cluster_nodes,
             cluster_nodes
             |> String.split(",", trim: true)
             |> Enum.map(&String.to_atom/1)
    end

    # Node discovery strategy — auto-configured from env vars.
    # Set FERRICSTORE_DISCOVERY to choose strategy:
    #   "gossip"  — multicast UDP (default, good for Docker Compose / LAN)
    #   "dns"     — DNS A-record polling (good for Kubernetes headless services)
    #   "epmd"    — static node list from FERRICSTORE_CLUSTER_NODES
    #   "consul"  — Consul service discovery (requires libcluster_consul dep)
    #   "etcd"    — etcd service discovery (requires libcluster_etcd dep)
    #   "none"    — disable libcluster (manual CLUSTER.JOIN only)
    discovery = System.get_env("FERRICSTORE_DISCOVERY", "gossip")

    case discovery do
      "gossip" ->
        config :libcluster,
          topologies: [
            ferricstore: [
              strategy: Cluster.Strategy.Gossip,
              config: [
                secret: cookie
              ]
            ]
          ]

      "dns" ->
        dns_name = System.get_env("FERRICSTORE_DNS_NAME", "ferricstore-headless")

        config :libcluster,
          topologies: [
            ferricstore: [
              strategy: Cluster.Strategy.DNSPoll,
              config: [
                polling_interval: 5_000,
                query: dns_name,
                node_basename: node_name |> to_string() |> String.split("@") |> hd()
              ]
            ]
          ]

      "epmd" ->
        nodes =
          System.get_env("FERRICSTORE_CLUSTER_NODES", "")
          |> String.split(",", trim: true)
          |> Enum.map(&String.to_atom/1)

        config :libcluster,
          topologies: [
            ferricstore: [
              strategy: Cluster.Strategy.Epmd,
              config: [hosts: nodes]
            ]
          ]

      "consul" ->
        config :libcluster,
          topologies: [
            ferricstore: [
              strategy: ClusterConsul.Strategy,
              config: [
                agent_url: System.get_env("FERRICSTORE_CONSUL_URL", "http://localhost:8500"),
                service_name: System.get_env("FERRICSTORE_CONSUL_SERVICE", "ferricstore"),
                polling_interval: 5_000
              ]
            ]
          ]

      "etcd" ->
        config :libcluster,
          topologies: [
            ferricstore: [
              strategy: Cluster.Strategy.Etcd,
              config: [
                endpoints: System.get_env("FERRICSTORE_ETCD_ENDPOINTS", "http://localhost:2379"),
                prefix: System.get_env("FERRICSTORE_ETCD_PREFIX", "/ferricstore/nodes"),
                node_basename: node_name |> to_string() |> String.split("@") |> hd()
              ]
            ]
          ]

      "none" ->
        config :libcluster, topologies: :disabled
    end
  end
end
