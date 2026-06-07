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

  @waraft_backend_sources [
    "../../lib/ferricstore/raft/waraft_backend.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/part_01.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/part_02.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/part_03.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/part_04.ex",
    "../../lib/ferricstore/raft/waraft_backend/sections/part_05.ex"
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
    "../../lib/ferricstore/raft/waraft_storage/sections/part_01.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_02.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_03.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_04.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_05.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_06.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_07.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_08.ex",
    "../../lib/ferricstore/raft/waraft_storage/sections/part_09.ex"
  ]

  @shard_compound_sources [
    "../../lib/ferricstore/store/shard/compound.ex",
    "../../lib/ferricstore/store/shard/compound/ops.ex",
    "../../lib/ferricstore/store/shard/compound/promoted.ex",
    "../../lib/ferricstore/store/shard/compound/read.ex",
    "../../lib/ferricstore/store/shard/compound/support.ex"
  ]

  @state_machine_sources [
    "../../lib/ferricstore/raft/state_machine.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_01.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_02.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_03.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_04.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_05.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_06.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_07.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_08.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_09.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_10.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_11.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_12.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_13.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_14.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_15.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_16.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_17.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_18.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_19.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_20.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_21.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_22.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_23.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_24.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_25.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_26.ex",
    "../../lib/ferricstore/raft/state_machine/sections/part_27.ex"
  ]

  def router_source do
    joined_source(@router_sources)
  end

  def shard_source do
    joined_source(@shard_sources)
  end

  def shard_compound_source do
    joined_source(@shard_compound_sources)
  end

  def flow_source do
    flow_paths =
      (__DIR__
       |> Path.expand()
       |> Path.join("../../lib/ferricstore/flow/**/*.ex")
       |> Path.wildcard())
      |> Enum.sort()

    @flow_sources
    |> Enum.map(&Path.expand(&1, __DIR__))
    |> Kernel.++(flow_paths)
    |> Enum.uniq()
    |> Enum.map_join("\n", &File.read!/1)
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

  defp joined_source(paths) do
    Enum.map_join(paths, "\n", fn path ->
      path
      |> Path.expand(__DIR__)
      |> File.read!()
    end)
  end
end
