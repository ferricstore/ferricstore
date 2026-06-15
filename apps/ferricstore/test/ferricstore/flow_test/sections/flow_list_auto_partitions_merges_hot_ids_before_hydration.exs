defmodule Ferricstore.FlowTest.Sections.FlowListAutoPartitionsMergesHotIdsBeforeHydration do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      test "Flow auto partition key list exposes all hidden buckets in order" do
        partitions = Ferricstore.Flow.Keys.auto_partition_keys()

        assert length(partitions) == 256
        assert hd(partitions) == "__flow_auto__:0"
        assert List.last(partitions) == "__flow_auto__:255"
        assert Enum.uniq(partitions) == partitions
      end

      test "Flow list without partition merges hot auto partitions by update order" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("auto-list-hot")
        partitions = Ferricstore.Flow.Keys.auto_partition_keys() |> Enum.take(3)

        ids =
          partitions
          |> Enum.with_index()
          |> Enum.map(fn {partition, index} ->
            id = uid("auto-list-hot-#{index}")
            now_ms = 1_000 + index

            assert {:ok, %{id: ^id}} =
                     flow_create_and_get(id,
                       type: type,
                       partition_key: partition,
                       now_ms: now_ms,
                       run_at_ms: now_ms
                     )

            id
          end)

        assert {:ok, records} = Ferricstore.Flow.list(ctx, type, state: "queued", count: 2)

        assert Enum.map(records, & &1.id) == Enum.take(ids, 2)
      end

      test "Flow list without partition preserves sparse auto partition ordering" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("auto-list-sparse")

        sparse_partitions =
          Ferricstore.Flow.Keys.auto_partition_keys()
          |> Enum.take_every(64)
          |> Enum.take(3)

        ids =
          sparse_partitions
          |> Enum.with_index()
          |> Enum.map(fn {partition, index} ->
            id = uid("auto-list-sparse-#{index}")
            now_ms = 2_000 + index

            assert {:ok, %{id: ^id}} =
                     flow_create_and_get(id,
                       type: type,
                       partition_key: partition,
                       now_ms: now_ms,
                       run_at_ms: now_ms
                     )

            id
          end)

        assert {:ok, records} = Ferricstore.Flow.list(ctx, type, state: "queued", count: 10)

        assert Enum.map(records, & &1.id) == ids
      end

      test "Flow list return meta trims heavy fields through embedded API" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("auto-list-meta")
        id = uid("auto-list-meta-flow")
        partition = Ferricstore.Flow.Keys.auto_partition_keys() |> hd()

        assert {:ok, %{id: ^id}} =
                 flow_create_and_get(id,
                   type: type,
                   partition_key: partition,
                   now_ms: 2_500,
                   run_at_ms: 2_500,
                   parent_flow_id: uid("auto-list-meta-parent"),
                   root_flow_id: uid("auto-list-meta-root"),
                   correlation_id: uid("auto-list-meta-correlation"),
                   payload: String.duplicate("x", 1024)
                 )

        assert {:ok, [record]} =
                 Ferricstore.Flow.list(ctx, type,
                   state: "queued",
                   count: 10,
                   return: :meta
                 )

        assert record.id == id
        assert record.type == type
        assert record.state == "queued"
        refute Map.has_key?(record, :payload)
        refute Map.has_key?(record, :parent_flow_id)
        refute Map.has_key?(record, :root_flow_id)
        refute Map.has_key?(record, :correlation_id)
      end

      test "Flow list without partition fetches more hot records from a skewed auto partition" do
        ctx = FerricStore.Instance.get(:default)
        type = uid("auto-list-skew")
        [early_partition, late_partition | _] = Ferricstore.Flow.Keys.auto_partition_keys()

        early_ids =
          0..5
          |> Enum.map(fn index ->
            id = uid("auto-list-skew-early-#{index}")
            now_ms = 3_000 + index

            assert {:ok, %{id: ^id}} =
                     flow_create_and_get(id,
                       type: type,
                       partition_key: early_partition,
                       now_ms: now_ms,
                       run_at_ms: now_ms
                     )

            id
          end)

        for index <- 0..5 do
          id = uid("auto-list-skew-late-#{index}")
          now_ms = 4_000 + index

          assert {:ok, %{id: ^id}} =
                   flow_create_and_get(id,
                     type: type,
                     partition_key: late_partition,
                     now_ms: now_ms,
                     run_at_ms: now_ms
                   )
        end

        assert {:ok, records} = Ferricstore.Flow.list(ctx, type, state: "queued", count: 5)

        assert Enum.map(records, & &1.id) == Enum.take(early_ids, 5)
      end
    end
  end
end
