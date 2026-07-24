defmodule Ferricstore.Flow.Query.FixedIndexExecutorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.Query.{Budget, FixedIndexExecutor, Limits, RecordProjection, Request}
  alias Ferricstore.Flow.Query
  alias Ferricstore.Test.IsolatedInstance

  test "explicit fixed-index projections skip the full allowlisted intermediate" do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    suffix = System.unique_integer([:positive, :monotonic])
    partition = "fixed-projection-#{suffix}"
    type = "invoice-#{suffix}"
    id = "run-#{suffix}"

    try do
      assert :ok =
               Ferricstore.Flow.create(ctx, id,
                 type: type,
                 state: "failed",
                 partition_key: partition,
                 attributes: %{"customer" => "acme", "large" => :binary.copy(<<7>>, 64)},
                 now_ms: 1_000
               )

      query =
        "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state " <>
          "ORDER BY updated_at_ms ASC LIMIT 10 " <>
          "RETURN RECORDS (run_id, state, attribute['customer'])"

      assert {:ok, request} =
               Query.prepare_reference("FQL1", query, %{
                 "partition" => partition,
                 "type" => type,
                 "state" => "failed"
               })

      {result, full_projection_calls} =
        execute_with_call_trace({RecordProjection, :project_result, 1}, fn ->
          Query.execute(ctx, request)
        end)

      assert {:ok,
              %{
                records: [
                  %{id: ^id, state: "failed", attributes: %{"customer" => "acme"}}
                ]
              }} = result

      assert full_projection_calls == 0
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "fixed fallback bounds each synchronous candidate window to one maximum page" do
    request =
      Request.collection(
        :execute,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
          {:eq, :type, {:literal, :keyword, "invoice"}},
          {:eq, :state, {:literal, :keyword, "failed"}}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:ok, %{range_seeks: range_seeks, scan_entries: scan_entries}} =
             FixedIndexExecutor.plan(request)

    assert div(scan_entries, range_seeks) == Limits.max_results() + 1
    assert (Limits.max_results() + 1) * 128 * 1_024 <= Budget.default().executor_memory_bytes
  end

  test "normalizes the indexed reader's candidate ceiling as a scan-budget error" do
    assert {:error, :query_scan_budget_exceeded} =
             FixedIndexExecutor.normalize_read_result_for_test(
               {:error, "ERR flow query candidate limit exceeded (10000)"}
             )
  end

  test "normalizes a type-less state-metadata lookup as a missing bounded plan" do
    assert {:error, :query_no_bounded_plan} =
             FixedIndexExecutor.normalize_read_result_for_test(
               {:error, "ERR flow state_meta search requires type"}
             )
  end

  test "EXPLAIN rejects a type-less state-metadata shape that execution cannot plan" do
    request =
      Request.collection(
        :explain,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
          {:eq, {:state_meta, "queued", "risk"}, {:literal, :keyword, "high"}}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )

    assert {:error, :query_no_bounded_plan} = FixedIndexExecutor.execute(%{}, request)
  end

  test "EXPLAIN rejects nonterminal multi-state filters that lack a fixed bounded fanout" do
    state_subset =
      {:in, :state,
       [
         {:literal, :keyword, "queued"},
         {:literal, :keyword, "running"}
       ]}

    requests = [
      Request.collection(
        :explain,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
          {:eq, :type, {:literal, :keyword, "invoice"}},
          state_subset,
          {:eq, {:attribute, "region"}, {:literal, :keyword, "eu"}}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      ),
      Request.collection(
        :explain,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
          {:eq, :parent_flow_id, {:literal, :keyword, "parent-1"}},
          state_subset
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )
    ]

    for request <- requests do
      assert {:error, :unsupported_query_shape} = FixedIndexExecutor.execute(%{}, request)
    end
  end

  test "EXPLAIN accounts for terminal-subset fanout and rejects repeated states" do
    request = fn states ->
      Request.collection(
        :explain,
        [
          {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
          {:eq, :type, {:literal, :keyword, "invoice"}},
          {:in, :state, Enum.map(states, &{:literal, :keyword, &1})}
        ],
        [{:updated_at_ms, :desc}],
        10,
        :record
      )
    end

    assert {:ok,
            %{
              bounds: %{scan_records: 202},
              query_fingerprint: subset_fingerprint
            }} =
             FixedIndexExecutor.execute(%{}, request.(~w(failed completed)))

    assert {:ok,
            %{
              bounds: %{scan_records: 101},
              query_fingerprint: all_terminals_fingerprint
            }} =
             FixedIndexExecutor.execute(%{}, request.(~w(failed completed cancelled)))

    refute subset_fingerprint == all_terminals_fingerprint

    assert {:error, :duplicate_predicate_value} =
             FixedIndexExecutor.execute(%{}, request.(~w(failed failed)))
  end

  defp execute_with_call_trace({module, function, arity} = mfa, execute_fun) do
    parent = self()
    reference = make_ref()

    executor =
      spawn(fn ->
        receive do
          :execute ->
            send(parent, {reference, execute_fun.()})
            receive do: (:stop -> :ok)
        end
      end)

    Code.ensure_loaded!(module)
    :erlang.trace(executor, true, [:call, {:tracer, self()}])
    :erlang.trace_pattern(mfa, true, [:local])

    try do
      send(executor, :execute)
      assert_receive {^reference, result}, 5_000
      trace_reference = :erlang.trace_delivered(executor)
      assert_receive {:trace_delivered, ^executor, ^trace_reference}, 1_000
      {result, traced_calls(module, function, arity)}
    after
      :erlang.trace_pattern(mfa, false, [:local])
      :erlang.trace(executor, false, [:call])
      send(executor, :stop)
    end
  end

  defp traced_calls(module, function, arity) do
    receive do
      {:trace, _pid, :call, {^module, ^function, args}} when length(args) == arity ->
        1 + traced_calls(module, function, arity)
    after
      0 -> 0
    end
  end
end
