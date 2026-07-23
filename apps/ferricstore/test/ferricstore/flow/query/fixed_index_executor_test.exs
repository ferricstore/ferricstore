defmodule Ferricstore.Flow.Query.FixedIndexExecutorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Budget, FixedIndexExecutor, Limits, Request}

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
end
