defmodule Ferricstore.Store.CompactionJournalTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{LMDB, Locator}
  alias Ferricstore.Store.{CompactionJournal, CompactionPlan, CompactionTombstoneCatalog}
  alias Ferricstore.Store.Shard.Lifecycle

  setup do
    shard_path =
      Path.join(
        System.tmp_dir!(),
        "compaction_journal_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(shard_path)
    lmdb_path = LMDB.path(shard_path)

    on_exit(fn ->
      _ = LMDB.release(lmdb_path, 1_000)
      File.rm_rf(shard_path)
    end)

    %{shard_path: shard_path, lmdb_path: lmdb_path}
  end

  test "startup restores the backup when the locator transaction did not commit", %{
    shard_path: shard_path
  } do
    source = Path.join(shard_path, "00000.log")
    File.write!(source, "old-segment")
    plan_path = empty_plan!(shard_path, 0)
    assert {:ok, transaction} = CompactionJournal.begin(shard_path, 0, plan_path)

    File.rename!(source, transaction.backup)
    File.write!(source, "new-segment")
    assert :ok = CompactionJournal.sync_swap(transaction)

    expected_size = byte_size("old-segment")
    assert {0, ^expected_size} = Lifecycle.discover_active_file(shard_path)
    assert File.read!(source) == "old-segment"
    refute File.exists?(transaction.backup)
    refute File.exists?(transaction.journal)
    refute File.exists?(transaction.plan)
  end

  test "startup keeps the new segment when the locator transaction committed", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    source = Path.join(shard_path, "00000.log")
    File.write!(source, "old-segment")
    plan_path = empty_plan!(shard_path, 0)
    assert {:ok, transaction} = CompactionJournal.begin(shard_path, 0, plan_path)

    File.rename!(source, transaction.backup)
    File.write!(source, "new-segment")
    assert :ok = CompactionJournal.sync_swap(transaction)
    assert :ok = LMDB.write_batch(lmdb_path, [CompactionJournal.marker_op(transaction)])

    expected_size = byte_size("new-segment")
    assert {0, ^expected_size} = Lifecycle.discover_active_file(shard_path)
    assert File.read!(source) == "new-segment"
    assert :not_found = LMDB.get(lmdb_path, transaction.marker_key)
    refute File.exists?(transaction.backup)
    refute File.exists?(transaction.journal)
    refute File.exists?(transaction.plan)
  end

  test "recovery fails closed when a compaction backup is replaced by a symlink", %{
    shard_path: shard_path
  } do
    source = Path.join(shard_path, "00000.log")
    victim = Path.join(shard_path, "external.log")
    File.write!(source, "source")
    File.write!(victim, "external")
    plan_path = empty_plan!(shard_path, 0)
    assert {:ok, transaction} = CompactionJournal.begin(shard_path, 0, plan_path)
    File.ln_s!(victim, transaction.backup)

    assert {:error, {"compaction_swap_0.txn", _reason}} =
             CompactionJournal.recover_all(shard_path)

    assert File.read!(source) == "source"
    assert File.read!(victim) == "external"
    assert File.lstat!(transaction.backup).type == :symlink
    assert File.regular?(transaction.journal)
  end

  test "startup reverses partially applied cold relocations before rolling back the segment", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    {plan_path, park_key, old_locator, new_locator} = cold_plan!(shard_path, lmdb_path)
    source = Path.join(shard_path, "00000.log")
    File.write!(source, "old-segment")
    assert {:ok, transaction} = CompactionJournal.begin(shard_path, 0, plan_path)

    File.rename!(source, transaction.backup)
    File.write!(source, "new-segment")
    assert :ok = CompactionJournal.sync_swap(transaction)
    assert :ok = CompactionPlan.relocate_cold(plan_path, lmdb_path, :forward)
    assert {:ok, %{locator: ^new_locator}} = cold_park(lmdb_path, park_key)

    expected_size = byte_size("old-segment")
    assert {0, ^expected_size} = Lifecycle.discover_active_file(shard_path)
    assert File.read!(source) == "old-segment"
    assert {:ok, %{locator: ^old_locator}} = cold_park(lmdb_path, park_key)
    refute File.exists?(transaction.plan)
  end

  test "startup finishes cold relocation after the commit marker is durable", %{
    shard_path: shard_path,
    lmdb_path: lmdb_path
  } do
    {plan_path, park_key, _old_locator, new_locator} = cold_plan!(shard_path, lmdb_path)
    source = Path.join(shard_path, "00000.log")
    File.write!(source, "old-segment")
    assert {:ok, transaction} = CompactionJournal.begin(shard_path, 0, plan_path)

    File.rename!(source, transaction.backup)
    File.write!(source, "new-segment")
    assert :ok = CompactionJournal.sync_swap(transaction)
    assert :ok = LMDB.write_batch(lmdb_path, [CompactionJournal.marker_op(transaction)])

    expected_size = byte_size("new-segment")
    assert {0, ^expected_size} = Lifecycle.discover_active_file(shard_path)
    assert File.read!(source) == "new-segment"
    assert {:ok, %{locator: ^new_locator}} = cold_park(lmdb_path, park_key)
    refute File.exists?(transaction.plan)
  end

  test "startup removes orphaned pre-journal planning artifacts", %{shard_path: shard_path} do
    source = Path.join(shard_path, "00000.log")
    compact = Path.join(shard_path, "compact_0.log")
    plan = CompactionPlan.path(shard_path, 0)
    plan_temp = plan <> ".tmp"
    catalog = Path.join(shard_path, "compaction_tombstones_0")

    File.write!(source, "source")
    File.write!(compact, "partial")
    File.write!(plan, "orphan")
    File.write!(plan_temp, "partial-plan")
    File.mkdir_p!(catalog)
    File.write!(Path.join(catalog, "data.mdb"), "orphan")

    expected_size = byte_size("source")
    assert {0, ^expected_size} = Lifecycle.discover_active_file(shard_path)
    refute File.exists?(compact)
    refute File.exists?(plan)
    refute File.exists?(plan_temp)
    refute File.exists?(catalog)
  end

  test "startup releases an orphaned tombstone catalog before removing it", %{
    shard_path: shard_path
  } do
    source = Path.join(shard_path, "00000.log")
    File.write!(source, "source")

    assert {:ok, catalog} = CompactionTombstoneCatalog.open(shard_path, 0)
    assert :ok = CompactionTombstoneCatalog.record_source_page(catalog, [{"key", 1, 0, 0, true}])

    expected_size = byte_size("source")
    assert {0, ^expected_size} = Lifecycle.discover_active_file(shard_path)
    refute File.exists?(catalog.path)

    replacement = [{"replacement", 2, 0, 0, true}]
    assert :ok = CompactionTombstoneCatalog.record_source_page(catalog, replacement)

    assert :ok =
             CompactionTombstoneCatalog.observe_lower_page(catalog, [
               {"replacement", 1, 0, 0, false}
             ])

    assert :ok = LMDB.release(catalog.path, 1_000)
    assert {:ok, [2]} = CompactionTombstoneCatalog.needed_offsets(catalog, replacement)
  end

  test "recovery rejects compressed or trailing journal terms", %{shard_path: shard_path} do
    name = "compaction_swap_0.txn"
    path = Path.join(shard_path, name)
    term = {:ferricstore_compaction_swap, 1, 0, String.duplicate("tx", 2_048)}
    compressed = :erlang.term_to_binary(term, compressed: 9)
    assert <<131, 80, _::binary>> = compressed

    for payload <- [compressed, :erlang.term_to_binary(term) <> <<0>>] do
      File.write!(path, payload)
      assert {:error, {^name, :invalid_journal}} = CompactionJournal.recover_all(shard_path)
    end
  end

  defp empty_plan!(shard_path, fid) do
    assert {:ok, writer} = CompactionPlan.create(shard_path, fid)
    assert {:ok, plan_path} = CompactionPlan.finish(writer)
    plan_path
  end

  defp cold_plan!(shard_path, lmdb_path) do
    state_key = "flow/state/flow-1"
    park_key = LMDB.cold_park_key_for_state_key(state_key)

    old_locator =
      Locator.new!(
        flow_id: "flow-1",
        kind: :state,
        version: 1,
        raft_index: 10,
        file_id: 0,
        offset: 10,
        value_size: 50
      )

    new_locator = Locator.relocate!(old_locator, offset: 110, value_size: 60)
    park = %{locator: old_locator, state_key: state_key, type: "job", state: "waiting"}

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, park_key, LMDB.encode_cold_park(old_locator, Map.delete(park, :locator))},
               {:put, LMDB.cold_by_segment_key(old_locator), park_key}
             ])

    assert {:ok, writer} = CompactionPlan.create(shard_path, 0)
    assert :ok = CompactionPlan.append(writer, [{:cold, state_key, 10, 110, 60, park_key, park}])
    assert {:ok, plan_path} = CompactionPlan.finish(writer)
    {plan_path, park_key, old_locator, new_locator}
  end

  defp cold_park(lmdb_path, park_key) do
    with {:ok, blob} <- LMDB.get(lmdb_path, park_key) do
      LMDB.decode_cold_park(blob)
    end
  end
end
