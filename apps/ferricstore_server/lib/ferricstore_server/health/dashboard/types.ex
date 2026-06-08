defmodule FerricstoreServer.Health.Dashboard.Types do
  @moduledoc """
  Shared dashboard data shapes.

  The dashboard boundary returns plain maps from collector modules and renders
  EEx templates from those maps. Keeping the map types here keeps the public
  dashboard facade focused on routing collector/render calls.
  """

  @typedoc "Dashboard data map containing all sections."
  @type dashboard_data :: %{
          overview: overview_data(),
          shards: [shard_data()],
          hotcold: hotcold_data(),
          memory: memory_data(),
          connections: connections_data(),
          slowlog: [slowlog_entry()],
          merge: [merge_status()],
          namespace_config: [Ferricstore.NamespaceConfig.ns_entry()],
          cluster: cluster_data()
        }

  @typedoc "Overview section data."
  @type overview_data :: %{
          status: :ok | :starting,
          uptime_seconds: non_neg_integer(),
          total_keys: non_neg_integer(),
          total_commands: non_neg_integer(),
          total_connections: non_neg_integer(),
          memory_bytes: non_neg_integer(),
          run_id: binary()
        }

  @typedoc "Per-shard status data."
  @type shard_data :: %{
          index: non_neg_integer(),
          status: String.t(),
          keys: non_neg_integer(),
          ets_memory_bytes: non_neg_integer()
        }

  @typedoc "Hot/cold read metrics."
  @type hotcold_data :: %{
          hot_read_pct: float(),
          cold_reads_per_sec: float(),
          total_hot: non_neg_integer(),
          total_cold: non_neg_integer(),
          top_prefixes: [Ferricstore.Stats.hotness_entry()]
        }

  @typedoc "Memory pressure data."
  @type memory_data :: %{
          total_bytes: non_neg_integer(),
          max_bytes: non_neg_integer(),
          ratio: float(),
          pressure_level: Ferricstore.MemoryGuard.pressure_level(),
          eviction_policy: atom(),
          shards: %{non_neg_integer() => %{bytes: non_neg_integer(), ratio: float()}}
        }

  @typedoc "Connection metrics."
  @type connections_data :: %{
          active: non_neg_integer(),
          blocked: non_neg_integer(),
          tracking: non_neg_integer()
        }

  @typedoc "A single slowlog entry for display."
  @type slowlog_entry :: %{
          id: non_neg_integer(),
          timestamp_us: integer(),
          duration_us: non_neg_integer(),
          command: [binary()]
        }

  @typedoc "Merge scheduler status for one shard."
  @type merge_status :: %{
          shard_index: non_neg_integer(),
          mode: atom(),
          merging: boolean(),
          last_merge_at: integer() | nil,
          merge_count: non_neg_integer(),
          total_bytes_reclaimed: non_neg_integer()
        }

  @typedoc "Cluster topology data."
  @type cluster_data :: %{
          node_name: atom(),
          cluster_mode: :standalone | :cluster,
          cluster_size: non_neg_integer(),
          nodes: [atom()]
        }

  @typedoc "Per-shard Raft consensus data."
  @type raft_shard_data :: %{
          shard: non_neg_integer(),
          status: :ok | :unavailable,
          leader: tuple() | nil,
          current_term: non_neg_integer(),
          commit_index: non_neg_integer(),
          last_applied: non_neg_integer(),
          log_size: non_neg_integer(),
          members: [tuple()]
        }

  @typedoc "Active client connection data."
  @type client_data :: %{
          optional(:client_id) => pos_integer(),
          optional(:client_name) => binary() | nil,
          optional(:username) => binary() | nil,
          pid: pid(),
          peer: String.t(),
          age_seconds: non_neg_integer(),
          flags: String.t()
        }

  @typedoc "Configuration command reference row."
  @type config_command_entry :: %{
          command: binary(),
          scope: binary(),
          mutability: binary(),
          notes: binary()
        }

  @typedoc "Configuration parameter reference row."
  @type config_parameter_entry :: %{
          parameter: binary(),
          scope: binary(),
          mutability: binary(),
          notes: binary()
        }

  @typedoc "Configuration dashboard page data."
  @type config_page_data :: %{
          namespace_config: [Ferricstore.NamespaceConfig.ns_entry()],
          config_commands: [config_command_entry()],
          config_parameters: [config_parameter_entry()]
        }
end
