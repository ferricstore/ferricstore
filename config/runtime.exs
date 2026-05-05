import Config

if config_env() == :prod do
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
    eviction_policy: System.get_env("FERRICSTORE_EVICTION_POLICY", "volatile_lru"),
    max_value_size: String.to_integer(System.get_env("FERRICSTORE_MAX_VALUE_SIZE", "1048576")),
    hot_cache_max_value_size:
      String.to_integer(System.get_env("FERRICSTORE_HOT_CACHE_MAX_VALUE_SIZE", "65536")),
    flow_lmdb_enabled: System.get_env("FERRICSTORE_FLOW_LMDB", "false") in ["1", "true", "TRUE"],
    flow_lmdb_mode:
      System.get_env(
        "FERRICSTORE_FLOW_LMDB_MODE",
        if(System.get_env("FERRICSTORE_FLOW_LMDB", "false") in ["1", "true", "TRUE"],
          do: "mirror",
          else: "off"
        )
      ),
    flow_lmdb_map_size:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_MAP_SIZE", "68719476736")),
    flow_lmdb_flush_interval_ms:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_FLUSH_INTERVAL_MS", "10")),
    flow_lmdb_max_batch_ops:
      String.to_integer(System.get_env("FERRICSTORE_FLOW_LMDB_MAX_BATCH_OPS", "1000")),
    memory_guard_interval_ms:
      String.to_integer(System.get_env("FERRICSTORE_MEMORY_GUARD_INTERVAL_MS", "5000"))

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
      String.to_integer(System.get_env("FERRICSTORE_EXPIRY_SWEEP_INTERVAL_MS", "100")),
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
    protected_mode: System.get_env("FERRICSTORE_PROTECTED_MODE", "true") == "true",
    maxclients: String.to_integer(System.get_env("FERRICSTORE_MAXCLIENTS", "10000")),
    audit_log_enabled: System.get_env("FERRICSTORE_AUDIT_LOG", "false") == "true",
    acl_auto_save: System.get_env("FERRICSTORE_ACL_AUTO_SAVE", "false") == "true"

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
      require_tls: System.get_env("FERRICSTORE_REQUIRE_TLS", "false") == "true"
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
    tcp_nodelay: System.get_env("FERRICSTORE_TCP_NODELAY", "true") == "true",
    tcp_recbuf: String.to_integer(System.get_env("FERRICSTORE_TCP_RECBUF", "131072")),
    tcp_sndbuf: String.to_integer(System.get_env("FERRICSTORE_TCP_SNDBUF", "131072"))

  # ---------------------------------------------------------------------------
  # Raft / Internals
  # ---------------------------------------------------------------------------
  config :ferricstore,
    release_cursor_interval:
      String.to_integer(System.get_env("FERRICSTORE_RELEASE_CURSOR_INTERVAL", "10000")),
    promotion_threshold:
      String.to_integer(System.get_env("FERRICSTORE_PROMOTION_THRESHOLD", "100")),
    wal_commit_delay_us:
      String.to_integer(System.get_env("FERRICSTORE_WAL_COMMIT_DELAY_US", "200"))

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
