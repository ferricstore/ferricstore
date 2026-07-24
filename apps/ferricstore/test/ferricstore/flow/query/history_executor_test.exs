defmodule Ferricstore.Flow.Query.HistoryExecutorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.Query.{MandatoryScope, Request}
  alias Ferricstore.TermCodec

  alias Ferricstore.Flow.Query.{
    Budget,
    Cursor,
    ExecutionResult,
    HistoryExecutor,
    MemoryBudget,
    Planner
  }

  @cursor_key :binary.copy(<<0x42>>, 32)
  @now_ms 1_800_000_000_000

  test "issues an authenticated cursor and resumes from its exclusive event boundary" do
    request = history_request("history-run", 2)
    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)
    parent = self()

    first_read = fn _ctx, ^request, ^scope, nil ->
      {:ok,
       %{
         records: [event("1000-1"), event("1001-2")],
         has_more: true,
         continuation: "1001-2",
         scanned_entries: 6,
         hydrated_records: 3,
         duplicate_entries: 2,
         memory_high_water_bytes: 1_000
       }}
    end

    assert {:ok,
            %ExecutionResult{
              records: [%{event_id: "1000-1"}, %{event_id: "1001-2"}],
              has_more: true,
              continuation: token,
              usage: %{
                scanned_entries: 6,
                hydrated_records: 3,
                duplicate_entries: 2,
                memory_high_water_bytes: 1_000
              }
            }} =
             HistoryExecutor.execute(context(), request, plan, nil,
               page_read: first_read,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )

    assert {:ok, scope_keys} =
             MandatoryScope.derive_keys(scope, Keys.auto_partition_key("history-run"))

    request_binding = %{
      instance: context().name,
      scope: scope_keys.query_binding,
      query_fingerprint: Planner.query_fingerprint(request),
      query_digest: Planner.query_digest(request),
      order_by: request.order_by
    }

    assert {:ok, claim} = Cursor.open(request_binding, token, key: @cursor_key, now_ms: @now_ms)
    assert {:ok, {:ferric_flow_history_seek, 1, "1001-2"}} = TermCodec.decode(claim.continuation)

    continued = %{request | cursor: {:literal, :keyword, token}}

    second_read = fn _ctx, ^continued, ^scope, before_event ->
      send(parent, {:history_before, before_event})

      {:ok,
       %{
         records: [event("1002-3"), event("1003-4")],
         has_more: false,
         continuation: nil,
         scanned_entries: 4,
         hydrated_records: 2,
         duplicate_entries: 2,
         memory_high_water_bytes: 500
       }}
    end

    assert {:ok, %ExecutionResult{has_more: false, continuation: nil}} =
             HistoryExecutor.execute(
               context(),
               continued,
               plan,
               %{claim: claim, key: @cursor_key},
               page_read: second_read,
               now_ms: @now_ms
             )

    assert_received {:history_before, "1001-2"}
  end

  test "projects history fields after validating the full page and continuation" do
    request =
      history_request("history-projection", 2)
      |> Map.put(:projection, [:event_id, {:event_field, "event"}])

    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

    page_read = fn _ctx, ^request, ^scope, nil ->
      {:ok,
       %{
         records: [
           %{event_id: "1000-1", fields: %{"event" => "created", "secret" => "hidden"}}
         ],
         has_more: false,
         continuation: nil,
         scanned_entries: 1,
         hydrated_records: 1,
         duplicate_entries: 0,
         memory_high_water_bytes: 1_000
       }}
    end

    assert {:ok, %ExecutionResult{records: records}} =
             HistoryExecutor.execute(context(), request, plan, nil,
               page_read: page_read,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )

    assert records == [%{event_id: "1000-1", fields: %{"event" => "created"}}]
  end

  test "bare history returns do not reserve a duplicate output page" do
    request = history_request("history-full-return", 1)
    scope = MandatoryScope.dedicated()
    {:ok, original_plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

    records = [
      %{event_id: "1000-1", fields: %{"payload" => String.duplicate("x", 2_048)}}
    ]

    page_bytes = MemoryBudget.term_bytes(records)
    plan = %{original_plan | budget: %{original_plan.budget | executor_memory_bytes: page_bytes}}

    page_read = fn _ctx, ^request, ^scope, nil ->
      {:ok,
       %{
         records: records,
         has_more: false,
         continuation: nil,
         scanned_entries: 1,
         hydrated_records: 1,
         duplicate_entries: 0,
         memory_high_water_bytes: page_bytes
       }}
    end

    assert {:ok, %ExecutionResult{records: ^records, usage: usage}} =
             HistoryExecutor.execute(context(), request, plan, nil,
               page_read: page_read,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )

    assert usage.memory_high_water_bytes == page_bytes
  end

  test "rejects history pages containing fields outside the public result allowlist" do
    request = history_request("history-private-field", 1)
    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

    page_read = fn _ctx, ^request, ^scope, nil ->
      records = [Map.put(event("1000-1"), :lease_token, "must-not-leak")]

      {:ok,
       %{
         records: records,
         has_more: false,
         continuation: nil,
         scanned_entries: 1,
         hydrated_records: 1,
         duplicate_entries: 0,
         memory_high_water_bytes: Ferricstore.TermMemory.bytes(records)
       }}
    end

    assert {:error, :query_storage_inconsistent} =
             HistoryExecutor.execute(context(), request, plan, nil,
               page_read: page_read,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )
  end

  test "rejects forged page accounting and malformed cursor continuations" do
    request = history_request("history-budget", 2)
    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

    oversized = fn _ctx, _request, _scope, nil ->
      {:ok,
       %{
         records: [event("1000-1")],
         has_more: false,
         continuation: nil,
         scanned_entries: 7,
         hydrated_records: 1,
         duplicate_entries: 0,
         memory_high_water_bytes: 100
       }}
    end

    assert {:error, :query_storage_inconsistent} =
             HistoryExecutor.execute(context(), request, plan, nil,
               page_read: oversized,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )

    bad_claim = %Cursor.Claim{
      request_digest: :binary.copy(<<0>>, 32),
      index_id: plan.index_id,
      index_version: plan.index_version,
      index_build_id: plan.index_build_id,
      continuation: TermCodec.encode({:unexpected, "1000-1"}),
      token_digest: :binary.copy(<<1>>, 32)
    }

    assert {:error, :query_cursor_invalid} =
             HistoryExecutor.execute(
               context(),
               %{request | cursor: {:literal, :keyword, "ignored"}},
               plan,
               %{claim: bad_claim, key: @cursor_key},
               page_read: oversized,
               now_ms: @now_ms
             )
  end

  test "rejects a page whose decoded-record peak exceeds the executor budget" do
    request = history_request("history-memory", 2)
    scope = MandatoryScope.dedicated()
    {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

    page_read = fn _ctx, _request, _scope, nil ->
      {:ok,
       %{
         records: [event("1000-1")],
         has_more: false,
         continuation: nil,
         scanned_entries: 1,
         hydrated_records: 1,
         duplicate_entries: 0,
         memory_high_water_bytes: Budget.default().executor_memory_bytes + 1
       }}
    end

    assert {:error, :query_memory_budget_exceeded} =
             HistoryExecutor.execute(context(), request, plan, nil,
               page_read: page_read,
               cursor_key: @cursor_key,
               now_ms: @now_ms
             )
  end

  test "rejects resumed pages that do not advance past the authenticated event boundary" do
    scope = MandatoryScope.dedicated()

    for {direction, boundary, returned_event} <- [
          {:asc, "1001-2", "1001-2"},
          {:desc, "1001-2", "1002-3"}
        ] do
      request = %{history_request("history-boundary", 2) | order_by: [{:event_id, direction}]}
      {:ok, plan} = Planner.plan(request, [], mandatory_scope: scope, now_ms: @now_ms)

      claim = %Cursor.Claim{
        request_digest: :binary.copy(<<0>>, 32),
        index_id: plan.index_id,
        index_version: plan.index_version,
        index_build_id: plan.index_build_id,
        continuation: TermCodec.encode({:ferric_flow_history_seek, 1, boundary}),
        token_digest: :binary.copy(<<1>>, 32)
      }

      page_read = fn _ctx, _request, _scope, ^boundary ->
        {:ok,
         %{
           records: [event(returned_event)],
           has_more: false,
           continuation: nil,
           scanned_entries: 1,
           hydrated_records: 1,
           duplicate_entries: 0,
           memory_high_water_bytes: 1_000
         }}
      end

      assert {:error, :query_storage_inconsistent} =
               HistoryExecutor.execute(
                 context(),
                 %{request | cursor: {:literal, :keyword, String.duplicate("x", 16)}},
                 plan,
                 %{claim: claim, key: @cursor_key},
                 page_read: page_read,
                 now_ms: @now_ms
               )
    end
  end

  defp history_request(id, limit) do
    Request.history(
      :execute,
      [{:eq, :run_id, {:literal, :keyword, id}}],
      :asc,
      limit
    )
  end

  defp event(event_id), do: %{event_id: event_id, fields: %{}}

  defp context do
    %{
      name: :history_executor_test,
      data_dir: "/tmp/history-executor-test",
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024))
    }
  end
end
