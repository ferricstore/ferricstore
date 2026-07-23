defmodule Ferricstore.Flow.Query.LineageExecutorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{MandatoryScope, Request}
  alias Ferricstore.TermCodec

  alias Ferricstore.Flow.Query.{
    Budget,
    Cursor,
    ExecutionResult,
    LineageExecutor,
    Planner
  }

  @cursor_key :binary.copy(<<0x52>>, 32)
  @now_ms 1_800_000_000_000

  test "issues an authenticated tuple cursor and resumes from its exclusive boundary" do
    request = lineage_request(:parent_flow_id, "parent-1", :asc, 2)
    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)
    parent = self()

    first_read = fn _ctx, ^request, ^scope, nil ->
      {:ok,
       %{
         records: [
           record("child-a", 1_000, :parent_flow_id, "parent-1"),
           record("child-b", 1_000, :parent_flow_id, "parent-1")
         ],
         has_more: true,
         continuation: {1_000, "child-b"},
         scanned_entries: 6,
         hydrated_records: 3,
         duplicate_entries: 2,
         memory_high_water_bytes: 1_000
       }}
    end

    assert {:ok,
            %ExecutionResult{
              records: [%{id: "child-a"}, %{id: "child-b"}],
              has_more: true,
              continuation: token,
              usage: %{
                scanned_entries: 6,
                hydrated_records: 3,
                duplicate_entries: 2,
                memory_high_water_bytes: 1_000
              }
            }} =
             LineageExecutor.execute(context(), request, plan, nil,
               page_read: first_read,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )

    assert {:ok, scope_keys} = MandatoryScope.derive_keys(scope, "tenant-a")

    request_binding = %{
      instance: context().name,
      scope: scope_keys.query_binding,
      query_fingerprint: Planner.query_fingerprint(request),
      query_digest: Planner.query_digest(request),
      order_by: request.order_by
    }

    assert {:ok, claim} = Cursor.open(request_binding, token, key: @cursor_key, now_ms: @now_ms)

    assert {:ok, {:ferric_flow_lineage_seek, 1, 1_000, "child-b"}} =
             TermCodec.decode(claim.continuation)

    continued = %{request | cursor: {:literal, :keyword, token}}

    second_read = fn _ctx, ^continued, ^scope, boundary ->
      send(parent, {:lineage_boundary, boundary})

      {:ok,
       %{
         records: [record("child-c", 1_001, :parent_flow_id, "parent-1")],
         has_more: false,
         continuation: nil,
         scanned_entries: 2,
         hydrated_records: 1,
         duplicate_entries: 1,
         memory_high_water_bytes: 500
       }}
    end

    assert {:ok, %ExecutionResult{has_more: false, continuation: nil}} =
             LineageExecutor.execute(
               context(),
               continued,
               plan,
               %{claim: claim, key: @cursor_key},
               page_read: second_read,
               now_ms: @now_ms
             )

    assert_received {:lineage_boundary, {1_000, "child-b"}}
  end

  test "rejects forged page accounting and malformed continuation tuples" do
    request = lineage_request(:root_flow_id, "root-1", :desc, 2)
    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

    oversized = fn _ctx, _request, _scope, nil ->
      {:ok,
       %{
         records: [record("root-1", 1_000, :root_flow_id, "root-1")],
         has_more: false,
         continuation: nil,
         scanned_entries: 8,
         hydrated_records: 1,
         duplicate_entries: 0,
         memory_high_water_bytes: 100
       }}
    end

    assert {:error, :query_storage_inconsistent} =
             LineageExecutor.execute(context(), request, plan, nil,
               page_read: oversized,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )

    bad_claim = %Cursor.Claim{
      request_digest: :binary.copy(<<0>>, 32),
      index_id: plan.index_id,
      index_version: plan.index_version,
      index_build_id: plan.index_build_id,
      continuation: TermCodec.encode({:ferric_flow_lineage_seek, 1, -1, "root-1"}),
      token_digest: :binary.copy(<<1>>, 32)
    }

    assert {:error, :query_cursor_invalid} =
             LineageExecutor.execute(
               context(),
               %{request | cursor: {:literal, :keyword, "ignored"}},
               plan,
               %{claim: bad_claim, key: @cursor_key},
               page_read: oversized,
               now_ms: @now_ms
             )
  end

  test "rejects a page whose decoded-record peak exceeds the executor budget" do
    request = lineage_request(:parent_flow_id, "parent-memory", :asc, 2)
    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

    page_read = fn _ctx, _request, _scope, nil ->
      {:ok,
       %{
         records: [record("child", 1_000, :parent_flow_id, "parent-memory")],
         has_more: false,
         continuation: nil,
         scanned_entries: 1,
         hydrated_records: 1,
         duplicate_entries: 0,
         memory_high_water_bytes: Budget.default().executor_memory_bytes + 1
       }}
    end

    assert {:error, :query_memory_budget_exceeded} =
             LineageExecutor.execute(context(), request, plan, nil,
               page_read: page_read,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )
  end

  test "rejects resumed pages that do not advance past the authenticated lineage boundary" do
    scope = MandatoryScope.dedicated()

    for {direction, boundary, returned_boundary} <- [
          {:asc, {1_000, "child-b"}, {1_000, "child-b"}},
          {:desc, {1_000, "child-b"}, {1_001, "child-c"}}
        ] do
      request = lineage_request(:parent_flow_id, "parent-boundary", direction, 2)
      {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)
      {boundary_ms, boundary_id} = boundary
      {returned_ms, returned_id} = returned_boundary

      claim = %Cursor.Claim{
        request_digest: :binary.copy(<<0>>, 32),
        index_id: plan.index_id,
        index_version: plan.index_version,
        index_build_id: plan.index_build_id,
        continuation: TermCodec.encode({:ferric_flow_lineage_seek, 1, boundary_ms, boundary_id}),
        token_digest: :binary.copy(<<1>>, 32)
      }

      page_read = fn _ctx, _request, _scope, ^boundary ->
        {:ok,
         %{
           records: [
             record(returned_id, returned_ms, :parent_flow_id, "parent-boundary")
           ],
           has_more: false,
           continuation: nil,
           scanned_entries: 1,
           hydrated_records: 1,
           duplicate_entries: 0,
           memory_high_water_bytes: 1_000
         }}
      end

      assert {:error, :query_storage_inconsistent} =
               LineageExecutor.execute(
                 context(),
                 %{request | cursor: {:literal, :keyword, String.duplicate("x", 16)}},
                 plan,
                 %{claim: claim, key: @cursor_key},
                 page_read: page_read,
                 now_ms: @now_ms
               )
    end
  end

  defp lineage_request(field, value, direction, limit) do
    Request.collection(
      :execute,
      [
        {:eq, field, {:literal, :keyword, value}},
        {:eq, :partition_key, {:literal, :keyword, "tenant-a"}}
      ],
      [{:updated_at_ms, direction}],
      limit,
      :record
    )
  end

  defp record(id, updated_at_ms, field, value) do
    %{id: id, updated_at_ms: updated_at_ms, partition_key: "tenant-a"}
    |> Map.put(field, value)
  end

  defp context do
    %{
      name: :lineage_executor_test,
      data_dir: "/tmp/lineage-executor-test",
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024))
    }
  end
end
