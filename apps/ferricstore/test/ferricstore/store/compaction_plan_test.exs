defmodule Ferricstore.Store.CompactionPlanTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{LMDB, Locator}
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
      File.write!(plan_path, [<<"FSCPLAN1", 7::unsigned-big-64>>, frame, payload])

      assert {:error, :invalid_plan_record} =
               CompactionPlan.reduce_pages(plan_path, 8, :ok, fn _page, acc -> acc end)
    end
  end

  test "cold relocation replay is bounded and idempotent in both directions", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    old_locator = locator(offset: 10, value_size: 50)
    park_key = LMDB.cold_park_key_for_state_key("flow/state/flow-1")

    park = %{
      locator: old_locator,
      state_key: "flow/state/flow-1",
      type: "job",
      state: "waiting"
    }

    old_blob = LMDB.encode_cold_park(old_locator, Map.delete(park, :locator))
    old_reverse = LMDB.cold_by_segment_key(old_locator)

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, park_key, old_blob},
               {:put, old_reverse, park_key}
             ])

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)

    assert :ok =
             CompactionPlan.append(writer, [
               {:cold, "flow/state/flow-1", 10, 110, 60, park_key, park}
             ])

    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert :ok = CompactionPlan.relocate_cold(plan_path, lmdb_path, :forward, page_size: 1)
    assert :ok = CompactionPlan.relocate_cold(plan_path, lmdb_path, :forward, page_size: 1)

    new_locator = Locator.relocate!(old_locator, offset: 110, value_size: 60)
    new_reverse = LMDB.cold_by_segment_key(new_locator)
    new_blob = LMDB.encode_cold_park(new_locator, Map.delete(park, :locator))

    assert {:ok, ^new_blob} = LMDB.get(lmdb_path, park_key)
    assert :not_found = LMDB.get(lmdb_path, old_reverse)
    assert {:ok, ^park_key} = LMDB.get(lmdb_path, new_reverse)

    assert :ok = CompactionPlan.relocate_cold(plan_path, lmdb_path, :reverse, page_size: 1)
    assert :ok = CompactionPlan.relocate_cold(plan_path, lmdb_path, :reverse, page_size: 1)

    assert {:ok, ^old_blob} = LMDB.get(lmdb_path, park_key)
    assert {:ok, ^park_key} = LMDB.get(lmdb_path, old_reverse)
    assert :not_found = LMDB.get(lmdb_path, new_reverse)
  end

  test "cold relocation returns a structured error for malformed park metadata", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    locator = locator(offset: 10, value_size: 50)
    park_key = LMDB.cold_park_key_for_state_key("flow/state/malformed")

    park = %{
      locator: locator,
      state_key: "flow/state/malformed",
      due_at_ms: "invalid"
    }

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)

    assert :ok =
             CompactionPlan.append(writer, [
               {:cold, "flow/state/malformed", 10, 110, 60, park_key, park}
             ])

    assert {:ok, plan_path} = CompactionPlan.finish(writer)

    assert {:error, :invalid_cold_row} =
             CompactionPlan.relocate_cold(plan_path, lmdb_path, :forward)
  end

  defp locator(overrides) do
    defaults = [
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 10,
      file_id: 0,
      offset: 0,
      value_size: 0
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end
end
