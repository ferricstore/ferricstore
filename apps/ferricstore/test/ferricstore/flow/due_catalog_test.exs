defmodule Ferricstore.Flow.DueCatalogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{DueCatalog, Keys, NativeOrderedIndex}
  alias Ferricstore.Raft.StateMachine

  test "excluded states cannot hide an eligible state behind ten thousand due indexes" do
    type = "catalog-fairness"

    catalog =
      Enum.reduce(0..10_000, DueCatalog.new(), fn partition, acc ->
        DueCatalog.put(
          acc,
          Keys.due_key(type, "running", 0, "partition-#{partition}"),
          partition
        )
      end)

    eligible_key = Keys.due_key(type, "waiting", 0, "eligible-partition")
    catalog = DueCatalog.put(catalog, eligible_key, 20_000)

    assert {:ok,
            %{
              keys: [^eligible_key],
              inspected_entries: 1,
              inspected_heads: inspected_heads
            }} =
             DueCatalog.select(
               catalog,
               type,
               0,
               :any,
               {:exclude, :any, ["running"]},
               1
             )

    assert inspected_heads <= 2
  end

  test "selection preserves global earliest-due order across states and partitions" do
    type = "catalog-order"
    waiting_later = Keys.due_key(type, "waiting", 1, "partition-a")
    queued_earliest = Keys.due_key(type, "queued", 1, "partition-b")
    waiting_middle = Keys.due_key(type, "waiting", 1, "partition-c")

    catalog =
      DueCatalog.new()
      |> DueCatalog.put(waiting_later, 30)
      |> DueCatalog.put(queued_earliest, 10)
      |> DueCatalog.put(waiting_middle, 20)

    assert {:ok,
            %{
              keys: [^queued_earliest, ^waiting_middle, ^waiting_later],
              inspected_entries: 3
            }} =
             DueCatalog.select(catalog, type, 1, :any, :any, 3)
  end

  test "catalog head changes are exact after score updates and deletion" do
    type = "catalog-updates"
    first = Keys.due_key(type, "waiting", 2, "partition-a")
    second = Keys.due_key(type, "waiting", 2, "partition-b")

    catalog =
      DueCatalog.new()
      |> DueCatalog.put(first, 10)
      |> DueCatalog.put(second, 20)
      |> DueCatalog.put(first, 30)

    assert {:ok, %{keys: [^second, ^first]}} =
             DueCatalog.select(catalog, type, 2, :any, :any, 2)

    catalog = DueCatalog.delete(catalog, second)

    assert {:ok, %{keys: [^first]}} =
             DueCatalog.select(catalog, type, 2, :any, :any, 2)
  end

  test "paged selection reaches an eligible key after a full stale-key page" do
    type = "catalog-page"

    {catalog, ordered_keys} =
      Enum.reduce(0..256, {DueCatalog.new(), []}, fn number, {catalog, keys} ->
        key =
          Keys.due_key(
            type,
            "waiting",
            0,
            "partition-#{String.pad_leading(Integer.to_string(number), 3, "0")}"
          )

        {DueCatalog.put(catalog, key, number), [key | keys]}
      end)

    ordered_keys = Enum.reverse(ordered_keys)

    assert {:ok, selection} =
             DueCatalog.start_selection(catalog, type, 0, :any, :any)

    assert {:ok,
            %{
              keys: first_page,
              continuation: continuation,
              done?: false
            }} = DueCatalog.take_page(selection, 256)

    assert first_page == Enum.take(ordered_keys, 256)

    assert {:ok,
            %{
              keys: [last_key],
              done?: true
            }} = DueCatalog.take_page(continuation, 256)

    assert last_key == List.last(ordered_keys)
  end

  test "due paging stops before future catalog heads" do
    type = "catalog-due-horizon"
    due_key = Keys.due_key(type, "waiting", 0, "due")
    future_key = Keys.due_key(type, "waiting", 0, "future")

    catalog =
      DueCatalog.new()
      |> DueCatalog.put(due_key, 10)
      |> DueCatalog.put(future_key, 20)

    assert {:ok, selection} =
             DueCatalog.start_selection(catalog, type, 0, :any, :any)

    assert {:ok,
            %{
              keys: [^due_key],
              continuation: continuation,
              done?: true
            }} = DueCatalog.take_due_page(selection, 10, 256)

    assert {:ok, %{keys: [^future_key], done?: true}} =
             DueCatalog.take_page(continuation, 256)
  end

  test "deep validation rejects corrupt entries and ordered trees without crashing selection" do
    type = "catalog-corrupt"
    key = Keys.due_key(type, "waiting", 0, "partition")
    catalog = DueCatalog.put(DueCatalog.new(), key, 10)

    assert DueCatalog.deep_valid?(catalog)

    metadata_corrupt =
      put_in(catalog, [:entries, key, :state], "running")

    refute DueCatalog.deep_valid?(metadata_corrupt)

    assert {:error, :invalid_due_catalog} =
             DueCatalog.put_checked(metadata_corrupt, key, 20)

    {tree_key, _tree} = Enum.at(catalog.state_trees, 0)
    tree_corrupt = put_in(catalog, [:state_trees, tree_key], :not_a_gb_set)

    refute DueCatalog.deep_valid?(tree_corrupt)

    assert {:error, :invalid_due_catalog_query} =
             DueCatalog.start_selection(tree_corrupt, type, 0, :any, :any)
  end

  test "native lifecycle transitions and recovery keep catalog heads exact" do
    native = NativeOrderedIndex.new()
    type = "catalog-native-sync"
    waiting = Keys.due_key(type, "waiting", 0, "partition")
    running = Keys.due_key(type, "running", 0, "partition")

    assert :ok = NativeOrderedIndex.put_new_entries(native, [{waiting, "flow-1", 10}])

    catalog =
      StateMachine.__flow_sync_due_catalog_for_test__(
        DueCatalog.new(),
        native,
        [waiting]
      )

    assert {:ok, %{keys: [^waiting]}} =
             DueCatalog.select(catalog, type, 0, :any, :any, 1)

    assert :ok =
             NativeOrderedIndex.move_entries(native, [
               {waiting, running, "flow-1", 30}
             ])

    catalog =
      StateMachine.__flow_sync_due_catalog_for_test__(
        catalog,
        native,
        [waiting, running]
      )

    assert {:ok, %{keys: [^running]}} =
             DueCatalog.select(catalog, type, 0, :any, :any, 2)

    assert :ok = NativeOrderedIndex.delete_members(native, running, ["flow-1"])

    catalog =
      StateMachine.__flow_sync_due_catalog_for_test__(
        catalog,
        native,
        [running]
      )

    assert {:ok, %{keys: []}} =
             DueCatalog.select(catalog, type, 0, :any, :any, 1)

    assert :ok = NativeOrderedIndex.put_new_entries(native, [{waiting, "flow-2", 5}])

    catalog = DueCatalog.put(catalog, waiting, 5)
    corrupt_catalog = put_in(catalog, [:entries, waiting, :state], "running")

    rebuilt =
      StateMachine.__flow_sync_due_catalog_for_test__(
        corrupt_catalog,
        native,
        [waiting]
      )

    assert DueCatalog.deep_valid?(rebuilt)

    assert {:ok, %{keys: [^waiting]}} =
             DueCatalog.select(rebuilt, type, 0, :any, :any, 1)

    index = make_ref()
    lookup = make_ref()
    assert :ok = NativeOrderedIndex.register(index, lookup, native)

    recovered =
      StateMachine.__flow_due_catalog_from_native_for_recovery__(%{
        flow_index_name: index,
        flow_lookup_name: lookup,
        flow_due_catalog: metadata_corrupt_catalog(catalog)
      })

    assert DueCatalog.deep_valid?(recovered.flow_due_catalog)

    assert {:ok, %{keys: [^waiting]}} =
             DueCatalog.select(recovered.flow_due_catalog, type, 0, :any, :any, 1)
  end

  test "Raft claim uses the replicated bounded catalog instead of enumerating native count keys" do
    sections =
      Path.expand(
        "../../../lib/ferricstore/raft/state_machine/sections",
        __DIR__
      )

    scan_source = File.read!(Path.join(sections, "flow_claim_scan.ex"))
    claim_source = File.read!(Path.join(sections, "flow_claim_due.ex"))
    history_source = File.read!(Path.join(sections, "flow_history_reads.ex"))
    init_source = File.read!(Path.join(sections, "init.ex"))
    callbacks_source = File.read!(Path.join(sections, "raft_callbacks.ex"))

    refute scan_source =~ "flow_claim_index_count_keys()"
    refute scan_source =~ "NativeFlowIndex.due_count_keys("
    refute history_source =~ "NativeFlowIndex.due_count_keys("
    assert scan_source =~ "DueCatalog.start_selection("
    assert claim_source =~ "DueCatalog.take_due_page("
    assert claim_source =~ "@flow_due_catalog_max_candidate_budget"
    assert claim_source =~ "scanned_candidates"
    assert history_source =~ "NativeFlowIndex.reduce_due_count_key_pages("
    assert history_source =~ "flow_sync_due_catalog_keys("
    assert init_source =~ "flow_due_catalog:"
    assert callbacks_source =~ "Map.put(state, :flow_due_catalog, catalog)"
  end

  defp metadata_corrupt_catalog(catalog) do
    %{catalog | entries: %{"invalid" => %{state: "invalid"}}}
  end
end
