defmodule Ferricstore.Flow.LMDBRebuilder.ColdStateTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDBRebuilder, Locator}
  alias Ferricstore.Flow.LMDBRebuilder.ColdState
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.LFU

  test "malformed source state is counted as a rebuild read failure" do
    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)

    on_exit(fn -> Process.delete(:flow_lmdb_rebuild_cold_read_errors) end)

    assert [] = ColdState.decode_state_record("flow-state", "corrupt", 0, nil, nil)
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 1
  end

  test "active index rebuild rejects a partial decode" do
    keydir = :ets.new(:flow_active_index_partial_decode, [:set])
    state_key = Keys.state_key("partial-active-index")

    :ets.insert(
      keydir,
      {state_key, "corrupt", 0, LFU.initial(), :memory, 0, byte_size("corrupt")}
    )

    assert {:error, {:cold_read_errors, 1}} =
             LMDBRebuilder.rebuild_active_indexes_from_keydir(
               System.tmp_dir!(),
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil
             )
  end

  test "cached WARaft state is pinned and physicalized before rebuilding query rows" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "flow-rebuilder-hot-waraft-#{System.unique_integer([:positive])}"
      )

    index = System.unique_integer([:positive, :monotonic])
    file_id = {:waraft_apply_projection, index}
    state_key = Keys.state_key("hot-waraft-source")

    record = %{
      id: "hot-waraft-source",
      type: "job",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 2,
      next_run_at_ms: 3,
      priority: 0,
      partition_key: nil,
      root_flow_id: "hot-waraft-source"
    }

    encoded = Ferricstore.Flow.encode_record(record)

    on_exit(fn ->
      WARaftSegmentReader.clear_apply_projection_cache(data_dir, 0)
      File.rm_rf!(data_dir)
    end)

    assert :ok =
             WARaftSegmentReader.put_apply_projection(data_dir, 0, index, [
               {state_key, encoded, 0}
             ])

    entry =
      {state_key, encoded, 0, LFU.initial(), file_id, 0, byte_size(encoded)}

    assert [{^state_key, ^encoded, 0, rebuilt, %Locator{} = locator}] =
             ColdState.read_and_decode([entry], data_dir, 0, %{data_dir: data_dir})

    assert rebuilt.id == record.id
    assert rebuilt.version == record.version
    assert Locator.hydration_ready?(locator)

    assert {:ok, physical_location} =
             WARaftSegmentReader.physical_location(%{data_dir: data_dir}, 0, file_id)

    assert physical_location ==
             {locator.segment_generation, locator.offset, locator.frame_size}

    assert WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0
  end

  test "metadata-only cleanup decodes a hot WARaft row without physical context" do
    state_key = Keys.state_key("hot-waraft-metadata-only")

    record = %{
      id: "hot-waraft-metadata-only",
      type: "job",
      state: "completed",
      version: 2,
      attempts: 1,
      fencing_token: 1,
      created_at_ms: 1,
      updated_at_ms: 2,
      next_run_at_ms: 0,
      priority: 0,
      partition_key: nil,
      root_flow_id: "hot-waraft-metadata-only"
    }

    encoded = Ferricstore.Flow.encode_record(record)

    entry =
      {state_key, encoded, 0, LFU.initial(), {:waraft_apply_projection, 17}, 0,
       byte_size(encoded)}

    Process.put(:flow_lmdb_rebuild_cold_read_errors, 0)
    on_exit(fn -> Process.delete(:flow_lmdb_rebuild_cold_read_errors) end)

    assert [{^state_key, ^encoded, 0, rebuilt, %Locator{}}] =
             ColdState.read_and_decode([entry], System.tmp_dir!())

    assert rebuilt.id == record.id
    assert Process.get(:flow_lmdb_rebuild_cold_read_errors) == 0
  end
end
