defmodule Ferricstore.Flow.ReadAPITest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.ReadAPI
  alias Ferricstore.Flow.LMDB

  @max_exact_integer 9_007_199_254_740_991

  defmodule EmptyIndexShard do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)
    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call(
          {:flow_index_score_range_slice, index_key, _min, _max, _reverse?, _offset, _count},
          _from,
          test_pid
        ) do
      send(test_pid, {:index_read, index_key})
      {:reply, {:ok, []}, test_pid}
    end
  end

  test "terminal query timestamps reject values above the exact integer ceiling" do
    assert ReadAPI.terminals(%{}, "email", from_ms: @max_exact_integer + 1) ==
             {:error, "ERR flow from_ms exceeds maximum #{@max_exact_integer}"}
  end

  test "stuck query timestamps reject values above the exact integer ceiling" do
    assert ReadAPI.stuck(%{}, "email", now_ms: @max_exact_integer + 1) ==
             {:error, "ERR flow now_ms exceeds maximum #{@max_exact_integer}"}
  end

  test "candidate scan counts cover disjoint RAM and LMDB index entries" do
    assert ReadAPI.__candidate_scan_count_for_test__(7, 11) == 18
  end

  test "candidate selection never prefers a failed count over a successful count" do
    scored = [
      {{:error, :index_unavailable}, :failed_candidate, {"a", "1"}},
      {{:ok, 20_000}, :available_candidate, {"z", "9"}}
    ]

    assert ReadAPI.__select_scored_candidate_for_test__(scored) == :available_candidate
  end

  test "query callers may lower but cannot raise the bounded candidate scan ceiling" do
    assert {:error, "ERR flow query scan limit is invalid"} =
             ReadAPI.list(%{}, "jobs", query_scan_limit: 10_001)
  end

  test "attribute discovery fails closed on corrupt query-index values" do
    {raw_prefix, key, _index_key} = discovery_query_key("blue")

    assert {:error, {:invalid_query_index_value, ^key}} =
             ReadAPI.__attribute_value_counts_from_chunks_for_test__(
               [{"unused", [{key, "corrupt"}]}],
               raw_prefix,
               10,
               fn _path, _ops -> flunk("corrupt values must not be deleted as expired") end
             )
  end

  test "attribute discovery rejects query-index key and value mismatches" do
    {raw_prefix, key, index_key} = discovery_query_key("blue")
    value = LMDB.encode_query_index_value(index_key, "other-flow", 1, 0)

    assert {:error, {:invalid_query_index_value, ^key}} =
             ReadAPI.__attribute_value_counts_from_chunks_for_test__(
               [{"unused", [{key, value}]}],
               raw_prefix,
               10,
               fn _path, _ops -> flunk("mismatched live rows must not be deleted") end
             )
  end

  test "attribute discovery decodes typed values inside query-index key components" do
    attribute_value = "blue\0north"
    {raw_prefix, key, index_key} = discovery_query_key(attribute_value)
    value = LMDB.encode_query_index_value(index_key, "flow-1", 1, 0)

    assert {:ok, %{^attribute_value => 1}} =
             ReadAPI.__attribute_value_counts_from_chunks_for_test__(
               [{"unused", [{key, value}]}],
               raw_prefix,
               10,
               fn _path, _ops -> flunk("live values must not be deleted") end
             )
  end

  test "attribute discovery retains maximum-size string values outside the LMDB key" do
    attribute_value = :binary.copy("v", 256)
    {raw_prefix, key, index_key} = discovery_query_key(attribute_value)
    value = LMDB.encode_query_index_value(index_key, "flow-1", 1, 0)

    assert byte_size(key) <= 511

    assert {:ok, %{^attribute_value => 1}} =
             ReadAPI.__attribute_value_counts_from_chunks_for_test__(
               [{"unused", [{key, value}]}],
               raw_prefix,
               10,
               fn _path, _ops -> flunk("live values must not be deleted") end
             )
  end

  test "attribute discovery propagates expired query-index deletion failures" do
    {raw_prefix, key, index_key} = discovery_query_key("blue")
    value = LMDB.encode_query_index_value(index_key, "flow-1", 1, 1)

    assert {:error, :disk_full} =
             ReadAPI.__attribute_value_counts_from_chunks_for_test__(
               [{"path", [{key, value}]}],
               raw_prefix,
               10,
               fn "path", [{:delete, ^key}] -> {:error, :disk_full} end
             )
  end

  test "an omitted count respects a configured maximum below the built-in default" do
    previous = Application.get_env(:ferricstore, :flow_max_count)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:ferricstore, :flow_max_count)
      else
        Application.put_env(:ferricstore, :flow_max_count, previous)
      end
    end)

    Application.put_env(:ferricstore, :flow_max_count, 7)

    assert ReadAPI.__flow_count_for_test__([]) == {:ok, 7}

    assert ReadAPI.__flow_count_for_test__(count: 8) ==
             {:error, "ERR flow count exceeds maximum 7"}
  end

  test "relationship queries scan auto partitions instead of passing atoms into key builders" do
    shard = start_supervised!({EmptyIndexShard, self()})

    assert {:ok, metadata_snapshot} =
             FerricStore.Flow.MetadataExtension.configure(
               FerricStore.Flow.MetadataExtension.Disabled,
               []
             )

    ctx = %FerricStore.Instance{
      name: :read_api_auto_relationship_test,
      data_dir: System.tmp_dir!(),
      shard_count: 1,
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple(),
      shard_names: {shard},
      flow_metadata_snapshot: metadata_snapshot
    }

    assert {:ok, []} = ReadAPI.by_parent(ctx, "parent-1", partition_key: :auto, count: 1)
    assert_received {:index_read, _index_key}
  end

  test "list applies terminal-only filtering before truncating active state indexes" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    id = "active-list-#{System.unique_integer([:positive])}"
    partition_key = Ferricstore.Flow.Keys.auto_partition_key(id)

    assert :ok =
             Ferricstore.Flow.create(
               ctx,
               id,
               type: "jobs",
               state: "queued",
               partition_key: partition_key
             )

    assert {:ok, []} =
             ReadAPI.list(
               ctx,
               "jobs",
               state: "queued",
               partition_key: partition_key,
               terminal_only: true,
               count: 1
             )

    assert {:ok, [%{id: ^id}]} =
             ReadAPI.by_root(ctx, id, partition_key: :auto, count: 1)
  end

  defp discovery_query_key(value) do
    index_key_prefix =
      Ferricstore.Flow.Keys.attribute_index_prefix("jobs", "queued", "color", "partition-1")

    index_key =
      Ferricstore.Flow.Keys.attribute_index_key(
        "jobs",
        "queued",
        "color",
        Ferricstore.Flow.Attributes.index_value(value),
        "partition-1"
      )

    {
      LMDB.query_index_raw_prefix(index_key_prefix),
      LMDB.query_index_key(index_key, "flow-1", 1),
      index_key
    }
  end
end
