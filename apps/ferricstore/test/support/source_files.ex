defmodule Ferricstore.Test.SourceFiles do
  @moduledoc false

  @router_sources [
    "../../lib/ferricstore/store/router.ex",
    "../../lib/ferricstore/store/router/part_01.ex",
    "../../lib/ferricstore/store/router/part_02.ex",
    "../../lib/ferricstore/store/router/part_03.ex",
    "../../lib/ferricstore/store/router/part_04.ex",
    "../../lib/ferricstore/store/router/part_05.ex",
    "../../lib/ferricstore/store/router/part_06.ex",
    "../../lib/ferricstore/store/router/part_07.ex",
    "../../lib/ferricstore/store/router/part_08.ex",
    "../../lib/ferricstore/store/router/part_09.ex",
    "../../lib/ferricstore/store/router/part_10.ex",
    "../../lib/ferricstore/store/router/part_11.ex",
    "../../lib/ferricstore/store/router/blob_gc.ex"
  ]

  @shard_sources [
    "../../lib/ferricstore/store/shard.ex",
    "../../lib/ferricstore/store/shard/startup.ex",
    "../../lib/ferricstore/store/shard/calls.ex",
    "../../lib/ferricstore/store/shard/routing.ex",
    "../../lib/ferricstore/store/shard/compaction.ex",
    "../../lib/ferricstore/store/shard/info.ex"
  ]

  @flow_sources [
    "../../lib/ferricstore/flow.ex"
  ]

  @public_api_sources [
    "../../lib/ferricstore.ex",
    "../../lib/ferricstore/impl.ex"
  ]

  @commands_server_sources [
    "../../lib/ferricstore/commands/server.ex",
    "../../lib/ferricstore/commands/server/info.ex"
  ]

  @shard_ets_sources [
    "../../lib/ferricstore/store/shard/ets.ex",
    "../../lib/ferricstore/store/shard/ets/prefix_scan.ex"
  ]

  @waraft_backend_sources [
    "../../lib/ferricstore/raft/waraft_backend.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/public_api.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/commit_path.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/startup.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/leader_wait.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/telemetry.ex"
  ]

  @waraft_segment_log_sources [
    "../../src/ferricstore_waraft_spike_segment_log.erl",
    "../../src/ferricstore_waraft_spike_segment_log/sections/part_01.hrl",
    "../../src/ferricstore_waraft_spike_segment_log/sections/part_02.hrl",
    "../../src/ferricstore_waraft_spike_segment_log/sections/part_03.hrl",
    "../../src/ferricstore_waraft_spike_segment_log/sections/part_04.hrl",
    "../../src/ferricstore_waraft_spike_segment_log/sections/part_05.hrl",
    "../../src/ferricstore_waraft_spike_segment_log/sections/part_06.hrl"
  ]

  @waraft_storage_sources [
    "../../lib/ferricstore/raft/waraft_storage.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/lifecycle.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/segment_project_commands.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/segment_projection.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/apply_result.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/recovery.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/metadata.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/projection_snapshot.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/snapshot_metadata.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/snapshot_install.ex"
  ]

  @shard_compound_sources [
    "../../lib/ferricstore/store/shard/compound.ex",
    "../../lib/ferricstore/store/shard/compound/ops.ex",
    "../../lib/ferricstore/store/shard/compound/promoted.ex",
    "../../lib/ferricstore/store/shard/compound/read.ex",
    "../../lib/ferricstore/store/shard/compound/support.ex"
  ]

  @store_ops_sources [
    "../../lib/ferricstore/store/ops.ex",
    "../../lib/ferricstore/store/ops/compound.ex",
    "../../lib/ferricstore/store/ops/flush.ex",
    "../../lib/ferricstore/store/ops/local_read.ex",
    "../../lib/ferricstore/store/ops/map_store.ex"
  ]

  @state_machine_sources [
    "../../lib/ferricstore/raft/state_machine.ex",
    "../../lib/ferricstore/raft/state_machine/sections/init.ex",
    "../../lib/ferricstore/raft/state_machine/sections/apply_dispatch.ex",
    "../../lib/ferricstore/raft/state_machine/sections/raft_callbacks.ex",
    "../../lib/ferricstore/raft/state_machine/sections/cross_shard_dispatch.ex",
    "../../lib/ferricstore/raft/state_machine/sections/cross_shard_reads.ex",
    "../../lib/ferricstore/raft/state_machine/sections/async_apply.ex",
    "../../lib/ferricstore/raft/state_machine/sections/compound_apply.ex",
    "../../lib/ferricstore/raft/state_machine/sections/cross_shard_pending.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_create.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_claim_due.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_claim_scan.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_claim_native_plan.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_transition.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_terminal.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_retention_state.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_retention_values.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_claim_indexes.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_claim_state_writes.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_history_writes.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_history_reads.ex",
    "../../lib/ferricstore/raft/state_machine/sections/flow_values.ex",
    "../../lib/ferricstore/raft/state_machine/sections/pending_writes.ex",
    "../../lib/ferricstore/raft/state_machine/sections/pending_locations.ex",
    "../../lib/ferricstore/raft/state_machine/sections/lmdb_projection.ex",
    "../../lib/ferricstore/raft/state_machine/sections/data_mutations.ex",
    "../../lib/ferricstore/raft/state_machine/sections/read_warm.ex",
    "../../lib/ferricstore/raft/state_machine/sections/compound_indexes.ex"
  ]

  def router_source do
    joined_source(@router_sources)
  end

  def shard_source do
    joined_source_with_wildcards(@shard_sources, [
      "../../lib/ferricstore/store/shard/*.ex",
      "../../lib/ferricstore/store/shard/**/*.ex"
    ])
  end

  def shard_compound_source do
    joined_source(@shard_compound_sources)
  end

  def shard_ets_source do
    joined_source(@shard_ets_sources)
  end

  def store_ops_source do
    joined_source(@store_ops_sources)
  end

  def public_api_source do
    joined_source_with_wildcards(@public_api_sources, [
      "../../lib/ferricstore/api/*.ex"
    ])
  end

  def commands_server_source do
    joined_source(@commands_server_sources)
  end

  def flow_source do
    flow_paths =
      __DIR__
      |> Path.expand()
      |> Path.join("../../lib/ferricstore/flow/**/*.ex")
      |> Path.wildcard()
      |> Enum.sort()

    @flow_sources
    |> Enum.map(&Path.expand(&1, __DIR__))
    |> Kernel.++(flow_paths)
    |> Enum.uniq()
    |> Enum.map_join("\n", &File.read!/1)
  end

  def flow_state_lmdb_soak_source do
    joined_source_with_wildcards(["../../../../bench/flow_state_lmdb_soak.exs"], [
      "../../../../bench/flow_state_lmdb_soak/**/*.exs"
    ])
  end

  def bitcask_native_source do
    joined_source_with_wildcards(["../../native/ferricstore_bitcask/src/lib.rs"], [
      "../../native/ferricstore_bitcask/src/sections/*.rs"
    ])
  end

  def resp_parser_source do
    joined_source_with_wildcards(["../../native/resp_parser_nif/src/lib.rs"], [
      "../../native/resp_parser_nif/src/sections/part_*.rs"
    ])
  end

  def waraft_backend_source do
    joined_source(@waraft_backend_sources)
  end

  def waraft_storage_source do
    joined_source(@waraft_storage_sources)
  end

  def waraft_segment_log_source do
    joined_source(@waraft_segment_log_sources)
  end

  def state_machine_source do
    joined_source(@state_machine_sources)
  end

  def private_function_sources(source, function) do
    pattern = ~r/^\s*defp\s+#{Regex.escape(function)}(?=[\s(]).*?(?=^\s*defp\s+|\z)/ms

    pattern
    |> Regex.scan(source)
    |> List.flatten()
  end

  def private_function_source!(source, function, contains \\ nil) do
    source
    |> private_function_sources(function)
    |> Enum.find(fn body -> is_nil(contains) or String.contains?(body, contains) end)
    |> case do
      nil -> raise "could not find private function #{function} containing #{inspect(contains)}"
      body -> body
    end
  end

  defp joined_source(paths) do
    Enum.map_join(paths, "\n", fn path ->
      path
      |> Path.expand(__DIR__)
      |> File.read!()
    end)
  end

  defp joined_source_with_wildcards(paths, globs) do
    explicit_paths = Enum.map(paths, &Path.expand(&1, __DIR__))

    wildcard_paths =
      globs
      |> Enum.flat_map(fn glob ->
        glob
        |> Path.expand(__DIR__)
        |> Path.wildcard()
      end)
      |> Enum.sort()

    (explicit_paths ++ wildcard_paths)
    |> Enum.uniq()
    |> Enum.map_join("\n", &File.read!/1)
  end
end
