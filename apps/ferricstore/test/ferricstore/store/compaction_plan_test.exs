defmodule Ferricstore.Store.CompactionPlanTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, Locator}
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.Store.CompactionPlan

  setup do
    shard_path =
      Path.join(
        System.tmp_dir!(),
        "compaction_plan_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(shard_path)
    lmdb_path = LMDB.path(shard_path)

    on_exit(fn ->
      _ = LMDB.release(lmdb_path, 1_000)
      File.rm_rf(shard_path)
    end)

    %{shard_path: shard_path, lmdb_path: lmdb_path}
  end

  test "plan records are consumed in bounded pages", %{shard_path: shard_path} do
    entries =
      for offset <- 0..24 do
        {:hot, "key-#{offset}", offset, offset + 100, 10}
      end

    assert {:ok, writer} = CompactionPlan.create(shard_path, 7)
    assert :ok = CompactionPlan.append(writer, entries)
    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert {:ok, {pages, restored}} =
             CompactionPlan.reduce_pages(plan_path, 7, {[], []}, fn page, {sizes, acc} ->
               {[length(page) | sizes], Enum.reverse(page, acc)}
             end)

    assert Enum.reverse(pages) == [7, 7, 7, 4]
    assert Enum.reverse(restored) == entries
  end

  test "truncated plans fail closed instead of publishing partial mappings", %{
    shard_path: shard_path
  } do
    assert {:ok, writer} = CompactionPlan.create(shard_path, 3)
    assert :ok = CompactionPlan.append(writer, [{:hot, "key", 1, 2, 3}])
    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    size = File.stat!(plan_path).size
    {:ok, file} = :file.open(plan_path, [:read, :write, :binary, :raw])
    target_size = size - 1
    assert {:ok, ^target_size} = :file.position(file, target_size)
    assert :ok = :file.truncate(file)
    assert :ok = :file.close(file)

    assert {:error, :truncated_record} =
             CompactionPlan.reduce_pages(plan_path, 8, :ok, fn _page, acc -> acc end)
  end

  test "the replaced beta plan format is rejected", %{shard_path: shard_path} do
    plan_path = Path.join(shard_path, "old-format.txn")
    File.write!(plan_path, <<"FSCPLAN1", 0::unsigned-big-64>>)

    assert {:error, :invalid_plan_header} =
             CompactionPlan.reduce_pages(plan_path, 8, :ok, fn _page, acc -> acc end)
  end

  test "plan reads reject a file swapped after lstat", %{shard_path: shard_path} do
    assert {:ok, writer} = CompactionPlan.create(shard_path, 4)
    assert :ok = CompactionPlan.append(writer, [{:hot, "key", 1, 2, 3}])
    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    original_path = plan_path <> ".original"
    replacement_path = plan_path <> ".replacement"
    File.cp!(plan_path, replacement_path)

    Process.put(:ferricstore_compaction_plan_open_read_hook, fn path, modes ->
      File.rename!(path, original_path)
      File.ln_s!(replacement_path, path)
      File.open(path, modes)
    end)

    on_exit(fn -> Process.delete(:ferricstore_compaction_plan_open_read_hook) end)

    assert {:error, {:plan_identity_changed, ^plan_path}} =
             CompactionPlan.reduce_pages(plan_path, 8, :ok, fn _page, acc -> acc end)
  end

  test "plan records reject compressed or trailing external terms even with a valid checksum", %{
    shard_path: shard_path
  } do
    entry = {:hot, String.duplicate("key", 1_024), 1, 2, 3}

    compressed = :erlang.term_to_binary(entry, compressed: 9)
    assert <<131, 80, _::binary>> = compressed

    for {name, payload} <- [
          compressed: compressed,
          trailing: :erlang.term_to_binary(entry) <> <<0>>
        ] do
      plan_path = Path.join(shard_path, "#{name}.txn")
      frame = <<byte_size(payload)::unsigned-big-32, :erlang.crc32(payload)::unsigned-big-32>>
      File.write!(plan_path, [<<"FSCPLAN2", 7::unsigned-big-64>>, frame, payload])

      assert {:error, :invalid_plan_record} =
               CompactionPlan.reduce_pages(plan_path, 8, :ok, fn _page, acc -> acc end)
    end
  end

  test "Flow plan records reject non-hydratable or size-changing relocations", %{
    shard_path: shard_path
  } do
    state_key = Keys.state_key("flow-1", "tenant-a")
    park_key = LMDB.cold_park_key_for_state_key(state_key)
    locator = locator(offset: 10, value_size: 50)

    park = %{
      locator: locator,
      state_key: state_key,
      partition_key: "tenant-a",
      type: "job",
      state: "waiting",
      due_at_ms: 900_000
    }

    invalid_entries = [
      {:flow, state_key, 0, 10, 0, 110, 0},
      {:flow, state_key, 0, 10, 50, 110, 60},
      {:cold, state_key, 10, 110, 0, park_key, park},
      {:cold, state_key, 10, 110, 60, park_key, park},
      {:cold, state_key, 10, 110, 50, park_key, %{park | locator: %{locator | checksum: <<1>>}}}
    ]

    for entry <- invalid_entries do
      assert {:ok, writer} = CompactionPlan.create(shard_path, 0)
      assert {:error, :invalid_plan_record} = CompactionPlan.append(writer, [entry])
      assert :ok = CompactionPlan.abort(writer)
    end
  end

  test "cold relocation replay is bounded and idempotent in both directions", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    old_locator = locator(offset: 10, value_size: 50)
    state_key = Keys.state_key("flow-1", "tenant-a")
    park_key = LMDB.cold_park_key_for_state_key(state_key)

    park = %{
      locator: old_locator,
      state_key: state_key,
      partition_key: "tenant-a",
      type: "job",
      state: "waiting",
      due_at_ms: 900_000
    }

    record = %{
      id: "flow-1",
      version: 1,
      type: "job",
      state: "waiting",
      partition_key: "tenant-a",
      created_at_ms: 100,
      updated_at_ms: 200,
      next_run_at_ms: 900_000,
      priority: 0,
      attempts: 0
    }

    old_blob = LMDB.encode_cold_park(old_locator, Map.delete(park, :locator))
    old_reverse = LMDB.cold_by_segment_key(old_locator)
    assert {:ok, old_query_row} = QueryRowCodec.encode(state_key, record, old_locator, 0)

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, park_key, old_blob},
               {:put, old_reverse, park_key},
               {:put, state_key, old_query_row}
             ])

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)

    assert :ok =
             CompactionPlan.append(writer, [
               {:cold, state_key, 10, 110, 50, park_key, park}
             ])

    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward, page_size: 1)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward, page_size: 1)

    new_locator = Locator.relocate!(old_locator, offset: 110, value_size: 50)
    new_reverse = LMDB.cold_by_segment_key(new_locator)
    new_blob = LMDB.encode_cold_park(new_locator, Map.delete(park, :locator))

    assert {:ok, ^new_blob} = LMDB.get(lmdb_path, park_key)
    assert :not_found = LMDB.get(lmdb_path, old_reverse)
    assert {:ok, ^park_key} = LMDB.get(lmdb_path, new_reverse)
    assert {:ok, new_query_row} = LMDB.get(lmdb_path, state_key)

    assert {:ok, %{locator: ^new_locator, record: ^record}} =
             QueryRowCodec.decode(new_query_row, state_key)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :reverse, page_size: 1)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :reverse, page_size: 1)

    assert {:ok, ^old_blob} = LMDB.get(lmdb_path, park_key)
    assert {:ok, ^park_key} = LMDB.get(lmdb_path, old_reverse)
    assert :not_found = LMDB.get(lmdb_path, new_reverse)
    assert {:ok, ^old_query_row} = LMDB.get(lmdb_path, state_key)
  end

  test "hot Flow relocation updates only the centralized query row locator", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    old_locator = locator(offset: 10, value_size: 50)
    state_key = Keys.state_key("flow-1", "tenant-a")

    record = %{
      id: "flow-1",
      version: 1,
      type: "job",
      state: "waiting",
      partition_key: "tenant-a",
      created_at_ms: 100,
      updated_at_ms: 200,
      next_run_at_ms: 900_000,
      priority: 0,
      attempts: 0
    }

    assert {:ok, old_query_row} = QueryRowCodec.encode(state_key, record, old_locator, 0)
    composite_key = "flow-composite:sentinel"
    composite_value = "must-not-be-rewritten"

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, state_key, old_query_row},
               {:put, composite_key, composite_value}
             ])

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)
    assert :ok = CompactionPlan.append(writer, [{:flow, state_key, 0, 10, 50, 110, 50}])
    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward, page_size: 1)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward, page_size: 1)

    new_locator = Locator.relocate!(old_locator, offset: 110, value_size: 50)
    assert {:ok, ^new_locator} = query_locator(lmdb_path, state_key)
    assert {:ok, ^composite_value} = LMDB.get(lmdb_path, composite_key)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :reverse, page_size: 1)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :reverse, page_size: 1)

    assert {:ok, ^old_locator} = query_locator(lmdb_path, state_key)
    assert {:ok, ^composite_value} = LMDB.get(lmdb_path, composite_key)
  end

  test "ordinary hot compaction pages do not open an LMDB transaction", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)
    assert :ok = CompactionPlan.append(writer, [{:hot, "ordinary-key", 10, 110, 60}])
    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward,
               get_many_fun: fn _path, _keys -> flunk("ordinary pages must not read LMDB") end,
               write_fun: fn _path, _ops -> flunk("ordinary pages must not write LMDB") end
             )
  end

  test "hot Flow relocation does not overwrite a newer query row", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    state_key = Keys.state_key("flow-1", "tenant-a")

    newer_record = %{
      id: "flow-1",
      version: 2,
      type: "job",
      state: "running",
      partition_key: "tenant-a",
      created_at_ms: 100,
      updated_at_ms: 300,
      next_run_at_ms: 900_000,
      priority: 0,
      attempts: 1
    }

    newer_locator = locator(version: 2, raft_index: 20, offset: 250, value_size: 70)

    assert {:ok, newer_query_row} =
             QueryRowCodec.encode(state_key, newer_record, newer_locator, 0)

    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, newer_query_row}])

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)
    assert :ok = CompactionPlan.append(writer, [{:flow, state_key, 0, 10, 50, 110, 50}])
    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert :ok =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward,
               write_fun: fn _path, _ops ->
                 flunk("stale plans must not open a write transaction")
               end
             )

    assert {:ok, ^newer_query_row} = LMDB.get(lmdb_path, state_key)
  end

  test "hot Flow relocation fails closed on a corrupted query row", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    state_key = Keys.state_key("flow-1", "tenant-a")
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, state_key, <<0, 1, 2, 3>>}])

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)
    assert :ok = CompactionPlan.append(writer, [{:flow, state_key, 0, 10, 50, 110, 50}])
    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert {:error, :invalid_query_row} =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward)

    assert {:ok, <<0, 1, 2, 3>>} = LMDB.get(lmdb_path, state_key)
  end

  test "cold relocation returns a structured error for malformed park metadata", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    locator = locator(offset: 10, value_size: 50)
    state_key = Keys.state_key("flow-1", "tenant-a")
    park_key = LMDB.cold_park_key_for_state_key(state_key)

    park = %{
      locator: locator,
      state_key: state_key,
      partition_key: "tenant-a",
      due_at_ms: "invalid"
    }

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)

    assert :ok =
             CompactionPlan.append(writer, [
               {:cold, state_key, 10, 110, 50, park_key, park}
             ])

    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert {:error, :invalid_cold_row} =
             CompactionPlan.relocate_flow_locators(plan_path, lmdb_path, :forward)
  end

  defp locator(overrides) do
    defaults = [
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 10,
      file_id: 0,
      offset: 0,
      value_size: 0,
      checksum: :binary.copy(<<1>>, 32)
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end

  defp query_locator(lmdb_path, state_key) do
    with {:ok, query_row} <- LMDB.get(lmdb_path, state_key),
         {:ok, %{locator: locator}} <- QueryRowCodec.decode(query_row, state_key) do
      {:ok, locator}
    end
  end
end
