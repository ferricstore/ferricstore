defmodule Ferricstore.Flow.HibernationTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Flow.{Hibernation, Keys, LMDB, Locator}
  alias Ferricstore.Flow.LMDBWriter.AfterFlush
  alias Ferricstore.Raft.{ApplyContext, StateMachine}

  test "demotable requires far-future waiting unleased non-terminal flow" do
    now = 1_000

    record = %{
      id: "flow-1",
      type: "email",
      state: "waiting",
      run_state: "waiting",
      next_run_at_ms: now + 10 * 60 * 1_000,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0
    }

    assert Hibernation.demotable?(record, now, hot_window_ms: 60_000, safety_margin_ms: 60_000)
    refute Hibernation.demotable?(%{record | next_run_at_ms: now + 30_000}, now)
    refute Hibernation.demotable?(%{record | lease_owner: "worker"}, now)
    refute Hibernation.demotable?(Map.put(record, :terminal_retention_until_ms, now + 1_000), now)
  end

  test "default demotion threshold parks only flows due more than five minutes away" do
    now = 1_000

    record = %{
      id: "flow-1",
      type: "email",
      state: "waiting",
      run_state: "waiting",
      next_run_at_ms: now + 5 * 60 * 1_000,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0
    }

    assert Hibernation.enabled?()
    assert Hibernation.hot_window_ms() == 5 * 60 * 1_000
    assert Hibernation.safety_margin_ms() == 0
    refute Hibernation.demotable?(record, now)
    assert Hibernation.demotable?(%{record | next_run_at_ms: now + 5 * 60 * 1_000 + 1}, now)
  end

  test "demotion eligibility fails closed for invalid clocks and window limits" do
    record = candidate().record

    refute Hibernation.demotable?(record, -1, hot_window_ms: 1, safety_margin_ms: 0)
    refute Hibernation.demotable?(record, 1_000, hot_window_ms: -1, safety_margin_ms: 0)
    refute Hibernation.demotable?(record, 1_000, hot_window_ms: 1, safety_margin_ms: "0")
    refute Hibernation.demotable?(record, 1_000, %{hot_window_ms: 1})
  end

  test "enabled can be disabled from runtime config without recompiling" do
    previous = Application.get_env(:ferricstore, :flow_hibernation_enabled)

    try do
      Application.put_env(:ferricstore, :flow_hibernation_enabled, false)
      refute Hibernation.refresh_config!()
      refute Hibernation.enabled?()

      Application.put_env(:ferricstore, :flow_hibernation_enabled, true)
      assert Hibernation.refresh_config!()
      assert Hibernation.enabled?()
    after
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_hibernation_enabled)
        value -> Application.put_env(:ferricstore, :flow_hibernation_enabled, value)
      end

      Hibernation.refresh_config!()
    end
  end

  test "demotion writes cold rows before attempting hot eviction" do
    test_pid = self()
    candidate = candidate()

    result =
      Hibernation.demote_candidates([candidate],
        write_cold_fun: fn ops ->
          send(test_pid, {:cold_written, ops})
          :ok
        end,
        evict_hot_fun: fn locator ->
          assert_receive {:cold_written, _ops}
          send(test_pid, {:hot_evicted, locator})
          :ok
        end
      )

    assert {:ok, %{attempted: 1, cold_written: 1, hot_evicted: 1, hot_changed: 0}} = result
    assert_receive {:hot_evicted, %Locator{}}
  end

  test "demotion does not evict hot locator when cold write fails" do
    test_pid = self()

    result =
      Hibernation.demote_candidates([candidate()],
        write_cold_fun: fn _ops -> {:error, :lmdb_down} end,
        evict_hot_fun: fn _locator ->
          send(test_pid, :unexpected_evict)
          :ok
        end
      )

    assert {:error, :lmdb_down, %{attempted: 1, cold_written: 0, hot_evicted: 0}} = result
    refute_receive :unexpected_evict
  end

  test "demotion reports changed hot locator without treating it as evicted" do
    result =
      Hibernation.demote_candidates([candidate()],
        write_cold_fun: fn _ops -> :ok end,
        evict_hot_fun: fn _locator -> {:error, :changed} end
      )

    assert {:ok, %{attempted: 1, cold_written: 1, hot_evicted: 0, hot_changed: 1}} = result
  end

  test "demotion propagates hot eviction failures with partial durability counts" do
    result =
      Hibernation.demote_candidates([candidate()],
        write_cold_fun: fn _ops -> :ok end,
        evict_hot_fun: fn _locator -> {:error, :hot_store_unavailable} end
      )

    assert {:error, {:evict_hot_failed, :hot_store_unavailable},
            %{
              attempted: 1,
              cold_written: 1,
              hot_evicted: 0,
              hot_changed: 0,
              hot_failed: 1
            }} = result
  end

  test "demotion rejects missing callbacks without raising or encoding candidates" do
    assert {:error, {:missing_callback, :write_cold_fun},
            %{
              attempted: 0,
              cold_written: 0,
              hot_evicted: 0,
              hot_changed: 0,
              hot_failed: 0
            }} = Hibernation.demote_candidates([%{}], [])
  end

  test "demotion rejects an invalid cold writer result without evicting hot state" do
    test_pid = self()

    result =
      Hibernation.demote_candidates([candidate()],
        write_cold_fun: fn _ops -> :queued end,
        evict_hot_fun: fn _locator ->
          send(test_pid, :unexpected_evict)
          :ok
        end
      )

    assert {:error, {:invalid_write_cold_result, :queued},
            %{attempted: 1, cold_written: 0, hot_evicted: 0, hot_failed: 0}} = result

    refute_receive :unexpected_evict
  end

  test "empty demotion batches do not invoke storage callbacks" do
    test_pid = self()

    assert {:ok,
            %{
              attempted: 0,
              cold_written: 0,
              hot_evicted: 0,
              hot_changed: 0,
              hot_failed: 0
            }} =
             Hibernation.demote_candidates([],
               write_cold_fun: fn _ops ->
                 send(test_pid, :unexpected_write)
                 :ok
               end,
               evict_hot_fun: fn _locator ->
                 send(test_pid, :unexpected_evict)
                 :ok
               end
             )

    refute_receive :unexpected_write
    refute_receive :unexpected_evict
  end

  test "demotion converts cold writer exceptions into structured errors" do
    assert {:error, {:callback_failed, :write_cold_fun, {RuntimeError, "writer crashed"}},
            %{attempted: 1, cold_written: 0, hot_evicted: 0, hot_failed: 0}} =
             Hibernation.demote_candidates([candidate()],
               write_cold_fun: fn _ops -> raise "writer crashed" end,
               evict_hot_fun: fn _locator -> :ok end
             )
  end

  test "demotion rejects oversized and malformed batches before invoking callbacks" do
    test_pid = self()

    callbacks = [
      write_cold_fun: fn _ops ->
        send(test_pid, :unexpected_write)
        :ok
      end,
      evict_hot_fun: fn _locator ->
        send(test_pid, :unexpected_evict)
        :ok
      end
    ]

    assert {:error, {:batch_too_large, 1_000}, %{attempted: 0}} =
             Hibernation.demote_candidates(List.duplicate(candidate(), 1_001), callbacks)

    assert {:error, :invalid_candidate, %{attempted: 0}} =
             Hibernation.demote_candidates(
               [%{locator: candidate().locator, record: %{}}],
               callbacks
             )

    refute_receive :unexpected_write
    refute_receive :unexpected_evict
  end

  test "demotion ops include park row due row and reverse segment row" do
    ops = Hibernation.demotion_ops(candidate())
    park_key = Ferricstore.Flow.LMDB.cold_park_key_for_state_key("flow/state/tenant-1/flow-1")

    assert length(ops) >= 3
    assert Enum.any?(ops, &match?({:put, ^park_key, _value}, &1))

    assert Enum.any?(ops, fn
             {:put, key, ^park_key} -> String.starts_with?(key, "flow:due:v1:")
             _ -> false
           end)

    assert Enum.any?(ops, fn {:put, key, _value} ->
             String.starts_with?(key, "flow:cold:by-segment:v1:")
           end)

    assert Enum.any?(ops, &match?({:put, _reverse_key, ^park_key}, &1))

    refute Enum.any?(ops, fn
             {:put, "flow-active-index:" <> _rest, _value} -> true
             _other -> false
           end)
  end

  test "demotion ops reject malformed candidates at one schema boundary" do
    assert_raise ArgumentError, "invalid Flow hibernation demotion candidate", fn ->
      Hibernation.demotion_ops(%{locator: candidate().locator, record: %{}})
    end

    mismatched =
      candidate()
      |> put_in([:record, :id], "other-flow")

    assert_raise ArgumentError, "invalid Flow hibernation demotion candidate", fn ->
      Hibernation.demotion_ops(mismatched)
    end
  end

  test "demotion rejects generated cold keys above the store key limit" do
    oversized =
      candidate()
      |> put_in([:record, :type], String.duplicate("t", 40_000))
      |> put_in([:record, :state], String.duplicate("s", 40_000))

    assert {:error, :generated_key_too_large} =
             Hibernation.demotion_ops_result(oversized)
  end

  test "demotion persists only the max-active timeout row in the active projection" do
    state_key = Keys.state_key("flow-1", "tenant-1")

    candidate =
      candidate()
      |> put_in([:record, :state_key], state_key)
      |> put_in([:record, :created_at_ms], 1_000)
      |> put_in([:record, :max_active_ms], 5_000)

    ops = Hibernation.demotion_ops(candidate)
    timeout_index_key = Keys.active_timeout_index_key()

    assert [{:put, active_key, active_value}] =
             Enum.filter(ops, fn
               {:put, "flow-active-index:" <> _rest, _value} -> true
               _other -> false
             end)

    assert {:ok, {^timeout_index_key, ^state_key, 6_000, 0, ^state_key}} =
             LMDB.decode_active_index_value(active_value)

    reverse_key = LMDB.active_by_state_key_key(state_key)

    assert {:put, ^reverse_key, reverse_value} =
             Enum.find(ops, &match?({:put, ^reverse_key, _value}, &1))

    assert {:ok, [^active_key]} = LMDB.decode_active_index_reverse_value(reverse_value)

    state_active_key =
      LMDB.active_index_key(
        Keys.state_index_key("email", "waiting", "tenant-1"),
        "flow-1",
        1_000
      )

    due_active_key =
      LMDB.active_index_key(
        Keys.due_key("email", "waiting", 0, "tenant-1"),
        "flow-1",
        900_000
      )

    assert {:delete, state_active_key} in ops
    assert {:delete, due_active_key} in ops
  end

  test "demotion replaces a full durable active projection without orphaned rows" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_hibernation_timeout_projection_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(data_dir) end)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    lmdb_path =
      data_dir
      |> Ferricstore.DataDir.shard_data_path(0)
      |> LMDB.path()

    state_key = Keys.state_key("flow-1", "tenant-1")

    candidate =
      candidate()
      |> put_in([:record, :state_key], state_key)
      |> put_in([:record, :created_at_ms], 1_000)
      |> put_in([:record, :max_active_ms], 5_000)

    previous_record = candidate.record

    full_projection_ops =
      previous_record
      |> then(&LMDB.active_index_put_ops_with_reverse(state_key, &1, 0))
      |> elem(0)

    assert :ok = LMDB.write_batch(lmdb_path, full_projection_ops)

    assert {:ok, previous_reverse_value} =
             LMDB.get(lmdb_path, LMDB.active_by_state_key_key(state_key))

    candidate = Map.put(candidate, :active_index_reverse_value, previous_reverse_value)

    assert :ok = LMDB.write_batch(lmdb_path, Hibernation.demotion_ops(candidate))

    timeout_index_key = Keys.active_timeout_index_key()

    assert {:ok, [{active_key, active_value}]} =
             LMDB.prefix_entries(lmdb_path, LMDB.active_index_global_prefix(), 10)

    assert {:ok, {^timeout_index_key, ^state_key, 6_000, 0, ^state_key}} =
             LMDB.decode_active_index_value(active_value)

    assert {:ok, reverse_value} = LMDB.get(lmdb_path, LMDB.active_by_state_key_key(state_key))
    assert {:ok, [^active_key]} = LMDB.decode_active_index_reverse_value(reverse_value)
  end

  test "demotion rejects corrupt active reverse metadata before writing cold rows" do
    test_pid = self()
    candidate = Map.put(candidate(), :active_index_reverse_value, "corrupt-reverse")

    assert {:error, :invalid_active_index_reverse} =
             Hibernation.demotion_ops_result(candidate)

    assert {:error, :invalid_active_index_reverse,
            %{attempted: 1, cold_written: 0, hot_evicted: 0}} =
             Hibernation.demote_candidates([candidate],
               write_cold_fun: fn _ops ->
                 send(test_pid, :unexpected_write)
                 :ok
               end,
               evict_hot_fun: fn _locator -> :ok end
             )

    refute_receive :unexpected_write
  end

  test "demotion rejects an active reverse owned by another flow" do
    other_state_key = Keys.state_key("flow-2", "tenant-1")

    other_record =
      candidate().record
      |> Map.put(:id, "flow-2")
      |> Map.put(:state_key, other_state_key)

    {_ops, other_reverse_value} =
      LMDB.active_index_put_ops_with_reverse(other_state_key, other_record, 0)

    candidate = Map.put(candidate(), :active_index_reverse_value, other_reverse_value)

    assert {:error, :active_index_reverse_owner_mismatch} =
             Hibernation.demotion_ops_result(candidate)
  end

  test "Raft hibernation projection consumes the result planner with linear accumulation" do
    source =
      __DIR__
      |> Path.join("../../../lib/ferricstore/raft/state_machine/sections/lmdb_projection.ex")
      |> File.read!()

    assert source =~ "Hibernation.demotion_ops_result(%{"
    assert source =~ "Enum.reduce_while({:ok, {[], []}}"
    refute source =~ "Hibernation.demotion_ops(%{"
    refute source =~ "ops_acc ++ ops"
  end

  test "Raft claim promotion cursor is initialized, advanced, and checkpointed in state" do
    sections_root =
      Path.expand(
        "../../../lib/ferricstore/raft/state_machine/sections",
        __DIR__
      )

    init_source = File.read!(Path.join(sections_root, "init.ex"))
    claim_source = File.read!(Path.join(sections_root, "flow_claim_due.ex"))
    callbacks_source = File.read!(Path.join(sections_root, "raft_callbacks.ex"))

    assert init_source =~ "flow_hibernation_promotion_cursor: nil"
    assert claim_source =~ "Hibernation.reduce_promotion_buckets("
    assert claim_source =~ "Ferricstore.Flow.LMDB.prefix_entries_after("
    assert claim_source =~ "apply_state_put(\n              :flow_hibernation_promotion_cursor"

    assert callbacks_source =~
             "Map.put(state, :flow_hibernation_promotion_cursor, cursor)"

    refute claim_source =~ "Hibernation.promotion_bucket_prefixes("
  end

  test "Raft claim fills remaining capacity from cold rows after partial hot claims" do
    state = %{apply_context: ApplyContext.default()}

    assert StateMachine.__flow_should_promote_cold_due_for_claim_for_test__(
             state,
             %{cold_due_mode: :allow},
             [],
             [%{id: "hot-flow"}],
             1
           )

    refute StateMachine.__flow_should_promote_cold_due_for_claim_for_test__(
             state,
             %{cold_due_mode: :skip},
             [],
             [],
             1
           )
  end

  test "stale due cleanup deletes only an unchanged scanned row" do
    {root, path} = temporary_lmdb("stale_due_cleanup")
    due_key = LMDB.cold_due_key(cold_due_attrs("flow-1", 1))
    park_key = LMDB.cold_park_key_for_state_key("flow/state/tenant-1/flow-1")
    park_blob = "park-v1"

    on_exit(fn ->
      _ = LMDB.release(path, 1_000)
      File.rm_rf(root)
    end)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, due_key, park_key},
               {:put, park_key, park_blob}
             ])

    assert {:ok, cleanup_batch} =
             Hibernation.stale_due_cleanup_batch(due_key, park_key, park_blob)

    assert :ok =
             AfterFlush.apply_after_flush({:cleanup_stale_cold_due, path, [cleanup_batch]})

    assert :not_found = LMDB.get(path, due_key)
    assert {:ok, ^park_blob} = LMDB.get(path, park_key)
  end

  test "stale due cleanup preserves a concurrently refreshed cold row" do
    {root, path} = temporary_lmdb("stale_due_refresh")
    due_key = LMDB.cold_due_key(cold_due_attrs("flow-1", 1))
    park_key = LMDB.cold_park_key_for_state_key("flow/state/tenant-1/flow-1")
    stale_park_blob = "park-v1"
    fresh_park_blob = "park-v2"

    on_exit(fn ->
      _ = LMDB.release(path, 1_000)
      File.rm_rf(root)
    end)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, due_key, park_key},
               {:put, park_key, stale_park_blob}
             ])

    assert {:ok, cleanup_batch} =
             Hibernation.stale_due_cleanup_batch(due_key, park_key, stale_park_blob)

    assert :ok = LMDB.write_batch(path, [{:put, park_key, fresh_park_blob}])

    assert :ok =
             AfterFlush.apply_after_flush({:cleanup_stale_cold_due, path, [cleanup_batch]})

    assert {:ok, ^park_key} = LMDB.get(path, due_key)
    assert {:ok, ^fresh_park_blob} = LMDB.get(path, park_key)
  end

  test "a refreshed stale row does not block cleanup of other unchanged rows" do
    {root, path} = temporary_lmdb("stale_due_batch")
    due_key_a = LMDB.cold_due_key(cold_due_attrs("flow-1", 1))
    due_key_b = LMDB.cold_due_key(cold_due_attrs("flow-2", 1))
    park_key_a = LMDB.cold_park_key_for_state_key("flow/state/tenant-1/flow-1")
    park_key_b = LMDB.cold_park_key_for_state_key("flow/state/tenant-1/flow-2")

    on_exit(fn ->
      _ = LMDB.release(path, 1_000)
      File.rm_rf(root)
    end)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, due_key_a, park_key_a},
               {:put, park_key_a, "park-a-v1"},
               {:put, due_key_b, park_key_b},
               {:put, park_key_b, "park-b-v1"}
             ])

    assert {:ok, cleanup_a} =
             Hibernation.stale_due_cleanup_batch(due_key_a, park_key_a, "park-a-v1")

    assert {:ok, cleanup_b} =
             Hibernation.stale_due_cleanup_batch(due_key_b, park_key_b, "park-b-v1")

    assert :ok = LMDB.write_batch(path, [{:put, park_key_a, "park-a-v2"}])

    assert :ok =
             AfterFlush.apply_after_flush({:cleanup_stale_cold_due, path, [cleanup_a, cleanup_b]})

    assert {:ok, ^park_key_a} = LMDB.get(path, due_key_a)
    assert :not_found = LMDB.get(path, due_key_b)
    assert {:ok, "park-a-v2"} = LMDB.get(path, park_key_a)
    assert {:ok, "park-b-v1"} = LMDB.get(path, park_key_b)
  end

  test "Raft stale due cleanup is bounded and deferred until after mirror flush" do
    claim_source =
      __DIR__
      |> Path.join("../../../lib/ferricstore/raft/state_machine/sections/flow_claim_due.ex")
      |> File.read!()

    assert claim_source =~ "Hibernation.stale_due_cleanup_batch("
    assert claim_source =~ "queue_pending_lmdb_mirror_after_flush("
    assert claim_source =~ "{:cleanup_stale_cold_due,"
    refute claim_source =~ "Hibernation.cleanup_stale_due_batches("
  end

  test "rebuild cold ops reconstructs only demotable far-future flows from durable candidates" do
    now = 1_000
    far = candidate()
    soon = put_in(candidate(), [:record, :next_run_at_ms], now + 30_000)
    terminal = put_in(candidate(), [:record, :terminal_retention_until_ms], now + 60_000)

    ops =
      Hibernation.rebuild_cold_ops([far, soon, terminal], now,
        hot_window_ms: 60_000,
        safety_margin_ms: 60_000
      )

    assert length(ops) >= 3
    assert Enum.any?(ops, fn {:put, key, _value} -> String.starts_with?(key, "flow:park:v1:") end)
    assert Enum.any?(ops, fn {:put, key, _value} -> String.starts_with?(key, "flow:due:v1:") end)
  end

  test "cold rebuild rejects oversized candidate pages before operation expansion" do
    candidates = List.duplicate(candidate(), 1_001)

    assert {:error, {:batch_too_large, 1_000}} =
             Hibernation.rebuild_cold_ops_result(candidates, 1_000,
               hot_window_ms: 1,
               safety_margin_ms: 0
             )

    assert_raise ArgumentError, "invalid Flow hibernation rebuild candidates", fn ->
      Hibernation.rebuild_cold_ops(candidates, 1_000,
        hot_window_ms: 1,
        safety_margin_ms: 0
      )
    end
  end

  test "promotion scan cursor covers every bucket entry exactly across bounded pages" do
    bucket_ms = 60_000
    last_bucket = 2 * bucket_ms
    prefixes = Enum.map([0, bucket_ms, last_bucket], &LMDB.cold_due_bucket_prefix/1)

    entries = %{
      Enum.at(prefixes, 0) =>
        for(
          index <- 1..5,
          do: {Enum.at(prefixes, 0) <> ":#{index}", "park-#{index}"}
        ),
      Enum.at(prefixes, 1) => [],
      Enum.at(prefixes, 2) => [
        {Enum.at(prefixes, 2) <> ":1", "park-6"},
        {Enum.at(prefixes, 2) <> ":2", "park-7"}
      ]
    }

    scan_fun = promotion_scan_fun(entries)
    reduce_fun = fn {key, _park_key}, acc -> {:cont, [key | acc]} end

    assert {:ok,
            %{
              cursor: %{bucket_ms: 0, after_key: first_after},
              scanned_pages: 1,
              scanned_entries: 3,
              wrapped?: false,
              acc: first
            }} =
             Hibernation.reduce_promotion_buckets(
               0,
               2 * bucket_ms,
               nil,
               [bucket_ms: bucket_ms, max_pages: 2, max_entries: 3],
               [],
               scan_fun,
               reduce_fun
             )

    assert {:ok,
            %{
              cursor: %{bucket_ms: ^last_bucket, after_key: nil},
              scanned_pages: 2,
              scanned_entries: 2,
              wrapped?: false,
              acc: second
            }} =
             Hibernation.reduce_promotion_buckets(
               0,
               last_bucket,
               %{bucket_ms: 0, after_key: first_after},
               [bucket_ms: bucket_ms, max_pages: 2, max_entries: 3],
               [],
               scan_fun,
               reduce_fun
             )

    assert {:ok,
            %{
              cursor: %{bucket_ms: 0, after_key: nil},
              scanned_pages: 1,
              scanned_entries: 2,
              wrapped?: true,
              acc: third
            }} =
             Hibernation.reduce_promotion_buckets(
               0,
               2 * bucket_ms,
               %{bucket_ms: last_bucket, after_key: nil},
               [bucket_ms: bucket_ms, max_pages: 2, max_entries: 3],
               [],
               scan_fun,
               reduce_fun
             )

    assert (first ++ second ++ third)
           |> Enum.reverse()
           |> Enum.sort() ==
             entries
             |> Map.values()
             |> List.flatten()
             |> Enum.map(&elem(&1, 0))
             |> Enum.sort()
  end

  test "promotion scan resumes after the last entry processed before a reducer halt" do
    bucket_ms = 60_000
    prefix = LMDB.cold_due_bucket_prefix(0)

    entries = %{
      prefix => for(index <- 1..5, do: {prefix <> ":#{index}", "park-#{index}"})
    }

    reduce_fun = fn {key, _park_key}, acc ->
      next = [key | acc]
      if length(next) == 2, do: {:halt, next}, else: {:cont, next}
    end

    assert {:ok,
            %{
              cursor: %{bucket_ms: 0, after_key: after_key},
              halted?: true,
              scanned_entries: 2,
              acc: first
            }} =
             Hibernation.reduce_promotion_buckets(
               0,
               0,
               nil,
               [bucket_ms: bucket_ms, max_pages: 4, max_entries: 5],
               [],
               promotion_scan_fun(entries),
               reduce_fun
             )

    assert {:ok, %{wrapped?: true, acc: second}} =
             Hibernation.reduce_promotion_buckets(
               0,
               0,
               %{bucket_ms: 0, after_key: after_key},
               [bucket_ms: bucket_ms, max_pages: 4, max_entries: 5],
               [],
               promotion_scan_fun(entries),
               fn {key, _park_key}, acc -> {:cont, [key | acc]} end
             )

    assert length(first) == 2
    assert length(second) == 3
    assert MapSet.disjoint?(MapSet.new(first), MapSet.new(second))
  end

  test "promotion scan rejects invalid pages without advancing the durable cursor" do
    cursor = %{bucket_ms: 0, after_key: nil}

    assert {:error, :invalid_promotion_scan_page,
            %{cursor: ^cursor, scanned_pages: 0, scanned_entries: 0}} =
             Hibernation.reduce_promotion_buckets(
               0,
               60_000,
               cursor,
               [bucket_ms: 60_000, max_pages: 2, max_entries: 2],
               :acc,
               fn _prefix, _after_key, _limit -> {:ok, [{"wrong-prefix", "park"}]} end,
               fn _entry, acc -> {:cont, acc} end
             )
  end

  test "hot index keys cover lifecycle due any running and metadata indexes" do
    record =
      candidate().record
      |> Map.merge(%{
        state: "running",
        lease_owner: "worker-1",
        parent_flow_id: "parent-1",
        root_flow_id: "root-1",
        correlation_id: "corr-1"
      })

    keys = Hibernation.hot_index_keys(record)

    assert Enum.any?(keys, &String.contains?(&1, ":s:"))
    assert Enum.any?(keys, &String.contains?(&1, ":d:"))
    assert Enum.any?(keys, &String.contains?(&1, ":da:"))
    assert Enum.any?(keys, &String.contains?(&1, ":i:"))
    assert Enum.any?(keys, &String.contains?(&1, ":w:"))
    assert Enum.any?(keys, &String.contains?(&1, ":p:"))
    assert Enum.any?(keys, &String.contains?(&1, ":r:"))
    assert Enum.any?(keys, &String.contains?(&1, ":c:"))
  end

  test "hot index planning rejects malformed records and never indexes an empty worker" do
    malformed = put_in(candidate(), [:record, :priority], "high").record

    assert Hibernation.hot_index_keys(malformed) == []
    assert Hibernation.hot_index_keys(candidate().record, %{due_any?: true}) == []

    running =
      candidate().record
      |> Map.put(:state, "running")
      |> Map.put(:lease_owner, nil)

    keys = Hibernation.hot_index_keys(running)

    assert Keys.inflight_index_key("email", "tenant-1") in keys
    refute Keys.worker_index_key("", "tenant-1") in keys
  end

  test "hot index planning fails closed instead of returning a partial oversized projection" do
    oversized =
      candidate().record
      |> Map.put(:parent_flow_id, String.duplicate("x", 65_536))

    assert Hibernation.hot_index_keys(oversized) == []
  end

  test "promotion reads cold locator validates state installs hot then cleans cold rows" do
    test_pid = self()
    row = promotion_row()

    result =
      Hibernation.promote_candidates([row],
        read_state_fun: fn locator ->
          send(test_pid, {:read, locator})

          {:ok,
           %{
             id: "flow-1",
             version: 1,
             type: "email",
             state: "waiting",
             next_run_at_ms: 900_000,
             run_state: "waiting"
           }}
        end,
        install_hot_fun: fn locator, record ->
          assert_receive {:read, ^locator}
          send(test_pid, {:installed, locator, record})
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          assert_receive {:installed, _locator, _record}
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:ok, %{attempted: 1, read: 1, installed: 1, stale: 0, failed: 0}} = result
    assert_receive {:cleanup, cleanup_ops}
    assert Enum.any?(cleanup_ops, &match?({:delete, "flow:park:v1:key:abc"}, &1))
    assert Enum.any?(cleanup_ops, &match?({:delete, "due-key"}, &1))
  end

  test "promotion rejects missing callbacks without reading rows" do
    assert {:error, {:missing_callback, :read_state_fun},
            %{attempted: 0, read: 0, installed: 0, stale: 0, failed: 0}} =
             Hibernation.promote_candidates([%{}], [])
  end

  test "promotion rejects malformed cold park rows before custom validation" do
    test_pid = self()
    malformed = %{promotion_row() | park: %{}}

    assert {:error, :invalid_promotion_row,
            %{attempted: 0, read: 0, installed: 0, stale: 0, failed: 0}} =
             Hibernation.promote_candidates([malformed],
               read_state_fun: fn _locator ->
                 send(test_pid, :unexpected_read)
                 {:ok, %{}}
               end,
               install_hot_fun: fn _locator, _record -> :ok end,
               validate_fun: fn _record, _row -> true end
             )

    refute_receive :unexpected_read
  end

  test "promotion rejects invalid and oversized limits before reading state" do
    test_pid = self()

    callbacks = [
      read_state_fun: fn _locator ->
        send(test_pid, :unexpected_read)
        {:error, :unexpected}
      end,
      install_hot_fun: fn _locator, _record -> :ok end
    ]

    assert {:error, :invalid_limit, %{attempted: 0}} =
             Hibernation.promote_candidates([promotion_row()], [limit: -1] ++ callbacks)

    assert {:error, {:batch_too_large, 1_000}, %{attempted: 0}} =
             Hibernation.promote_candidates(List.duplicate(promotion_row(), 1_001), callbacks)

    refute_receive :unexpected_read
  end

  test "promotion propagates install and cleanup failures with accurate partial counts" do
    test_pid = self()

    record = %{
      id: "flow-1",
      version: 1,
      type: "email",
      state: "waiting",
      next_run_at_ms: 900_000
    }

    assert {:error, {:install_hot_failed, :hot_store_unavailable},
            %{attempted: 1, read: 1, installed: 0, stale: 0, failed: 1}} =
             Hibernation.promote_candidates([promotion_row()],
               read_state_fun: fn _locator -> {:ok, record} end,
               install_hot_fun: fn _locator, _record -> {:error, :hot_store_unavailable} end,
               cleanup_cold_fun: fn _ops ->
                 send(test_pid, :unexpected_cleanup)
                 :ok
               end
             )

    refute_receive :unexpected_cleanup

    assert {:error, :cold_cleanup_unavailable,
            %{attempted: 1, read: 1, installed: 1, stale: 0, failed: 0}} =
             Hibernation.promote_candidates([promotion_row()],
               read_state_fun: fn _locator -> {:ok, record} end,
               install_hot_fun: fn _locator, _record -> :ok end,
               cleanup_cold_fun: fn _ops -> {:error, :cold_cleanup_unavailable} end
             )
  end

  test "promotion skips stale durable record and cleans stale cold rows" do
    test_pid = self()

    result =
      Hibernation.promote_candidates([promotion_row()],
        read_state_fun: fn _locator ->
          {:ok, %{id: "flow-1", version: 2, next_run_at_ms: 901_000, run_state: "waiting"}}
        end,
        install_hot_fun: fn _locator, _record ->
          send(test_pid, :unexpected_install)
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:ok, %{attempted: 1, read: 1, installed: 0, stale: 1, failed: 0}} = result
    refute_receive :unexpected_install
    assert_receive {:cleanup, [_ | _]}
  end

  test "promotion treats missing versions and leased records as stale" do
    test_pid = self()

    for record <- [
          %{id: "flow-1", type: "email", state: "waiting", next_run_at_ms: 900_000},
          %{
            id: "flow-1",
            version: 1,
            type: "email",
            state: "waiting",
            next_run_at_ms: 900_000,
            lease_owner: "worker-1"
          }
        ] do
      assert {:ok, %{attempted: 1, read: 1, installed: 0, stale: 1, failed: 0}} =
               Hibernation.promote_candidates([promotion_row()],
                 read_state_fun: fn _locator -> {:ok, record} end,
                 install_hot_fun: fn _locator, _record ->
                   send(test_pid, :unexpected_install)
                   :ok
                 end,
                 cleanup_cold_fun: fn _ops -> :ok end
               )
    end

    refute_receive :unexpected_install
  end

  test "promotion propagates durable read failures without cleaning cold rows" do
    test_pid = self()

    result =
      Hibernation.promote_candidates([promotion_row()],
        read_state_fun: fn _locator -> {:error, :enoent} end,
        install_hot_fun: fn _locator, _record ->
          send(test_pid, :unexpected_install)
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:error, {:read_state_failed, :enoent},
            %{attempted: 1, read: 0, installed: 0, stale: 0, failed: 1}} = result

    refute_receive :unexpected_install
    refute_receive {:cleanup, _ops}
  end

  test "fetch_or_promote returns hot record without touching cold path" do
    test_pid = self()

    result =
      Hibernation.fetch_or_promote("flow-1",
        fetch_hot_fun: fn "flow-1" -> {:ok, %{id: "flow-1", hot: true}} end,
        fetch_cold_fun: fn _flow_id ->
          send(test_pid, :unexpected_cold_fetch)
          :not_found
        end,
        read_state_fun: fn _locator -> {:error, :unexpected} end,
        install_hot_fun: fn _locator, _record -> :ok end
      )

    assert {:ok, :hot, %{id: "flow-1", hot: true}} = result
    refute_receive :unexpected_cold_fetch
  end

  test "fetch_or_promote rejects invalid identifiers and missing callbacks without raising" do
    assert {:error, :invalid_flow_id} = Hibernation.fetch_or_promote("", [])

    assert {:error, {:missing_callback, :fetch_hot_fun}} =
             Hibernation.fetch_or_promote("flow-1", [])
  end

  test "fetch_or_promote promotes one cold parked flow on hot miss" do
    test_pid = self()
    row = promotion_row()

    result =
      Hibernation.fetch_or_promote("flow-1",
        fetch_hot_fun: fn "flow-1" -> :not_found end,
        fetch_cold_fun: fn "flow-1" -> {:ok, row} end,
        read_state_fun: fn locator ->
          {:ok,
           %{
             id: locator.flow_id,
             version: locator.version,
             type: "email",
             state: "waiting",
             next_run_at_ms: 900_000
           }}
        end,
        install_hot_fun: fn locator, record ->
          send(test_pid, {:installed, locator, record})
          :ok
        end,
        cleanup_cold_fun: fn ops ->
          send(test_pid, {:cleanup, ops})
          :ok
        end
      )

    assert {:ok, :cold_promoted, %{id: "flow-1", version: 1}} = result
    assert_receive {:installed, %Locator{}, %{id: "flow-1"}}
    assert_receive {:cleanup, [_ | _]}
  end

  test "fetch_or_promote single-flights concurrent cold promotion per flow" do
    row = promotion_row()

    record = %{
      id: "flow-1",
      version: 1,
      type: "email",
      state: "waiting",
      run_state: "waiting",
      next_run_at_ms: 900_000
    }

    {:ok, state} = Agent.start_link(fn -> %{hot: nil, installs: 0} end)

    opts = [
      fetch_hot_fun: fn "flow-1" ->
        case Agent.get(state, & &1.hot) do
          nil -> :not_found
          hot -> {:ok, hot}
        end
      end,
      fetch_cold_fun: fn "flow-1" -> {:ok, row} end,
      read_state_fun: fn _locator -> {:ok, record} end,
      install_hot_fun: fn _locator, installed ->
        Process.sleep(5)

        Agent.update(state, fn current ->
          %{current | hot: installed, installs: current.installs + 1}
        end)

        :ok
      end,
      cleanup_cold_fun: fn _ops -> :ok end
    ]

    results =
      1..16
      |> Task.async_stream(
        fn _ -> Hibernation.fetch_or_promote("flow-1", opts) end,
        max_concurrency: 16,
        ordered: false,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Agent.get(state, & &1.installs) == 1
    assert Enum.count(results, &match?({:ok, :cold_promoted, _record}, &1)) == 1
    assert Enum.count(results, &match?({:ok, :hot, _record}, &1)) == 15
  end

  test "fetch_or_promote rejects stale cold locator instead of returning false not_found" do
    test_pid = self()
    row = promotion_row()

    result =
      Hibernation.fetch_or_promote("flow-1",
        fetch_hot_fun: fn "flow-1" -> :not_found end,
        fetch_cold_fun: fn "flow-1" -> {:ok, row} end,
        read_state_fun: fn _locator ->
          {:ok,
           %{
             id: "flow-1",
             version: 2,
             type: "email",
             state: "waiting",
             next_run_at_ms: 901_000
           }}
        end,
        install_hot_fun: fn _locator, _record -> :ok end,
        cleanup_cold_fun: fn ops ->
          send(test_pid, {:stale_cleanup, ops})
          :ok
        end
      )

    assert {:error, :stale_cold_locator} = result
    assert_receive {:stale_cleanup, [_ | _]}
  end

  test "cold compaction relocation updates reverse segment row and park row" do
    old_row = promotion_row()

    assert {:ok, new_row} =
             Hibernation.relocate_cold_row(old_row,
               file_id: {:flow_state, 2},
               offset: 2_000,
               value_size: 300,
               segment_generation: 5
             )

    assert {:ok, ops} = Hibernation.cold_compaction_ops(old_row, new_row)

    assert {:compare, "flow:park:v1:key:abc", encoded_old_park} = Enum.at(ops, 0)
    assert {:compare, old_reverse_key, "flow:park:v1:key:abc"} = Enum.at(ops, 1)
    assert {:delete, ^old_reverse_key} = Enum.at(ops, 2)
    assert {:put, new_reverse_key, "flow:park:v1:key:abc"} = Enum.at(ops, 3)
    assert {:put, "flow:park:v1:key:abc", encoded_park} = Enum.at(ops, 4)
    assert old_reverse_key != new_reverse_key

    assert {:ok, old_park} = Ferricstore.Flow.LMDB.decode_cold_park(encoded_old_park)
    assert old_park.locator == old_row.locator
    assert {:ok, park} = Ferricstore.Flow.LMDB.decode_cold_park(encoded_park)
    assert park.locator == new_row.locator
  end

  test "cold compaction refuses to update locator across logical generation" do
    old_row = promotion_row()
    newer = %{old_row | locator: Locator.relocate!(old_row.locator, offset: 2_000)}
    newer = %{newer | locator: %{newer.locator | version: 2}}

    assert {:error, :logical_generation_mismatch} =
             Hibernation.cold_compaction_ops(old_row, newer)
  end

  test "cold relocation and compaction reject malformed park rows without raising" do
    malformed = %{promotion_row() | park: "not-a-park"}

    assert {:error, :invalid_cold_row} =
             Hibernation.relocate_cold_row(malformed, offset: 2_000)

    assert {:error, :invalid_cold_row} =
             Hibernation.cold_compaction_ops(malformed, promotion_row())
  end

  test "property model never makes a live flow neither hot nor cold" do
    seed = {101, 202, 303}
    :rand.seed(:exsss, seed)

    initial = %{
      truth: locator(version: 1, raft_index: 1, offset: 1),
      hot: locator(version: 1, raft_index: 1, offset: 1),
      cold: nil
    }

    final =
      Enum.reduce(1..1_000, initial, fn step, model ->
        model
        |> random_model_step(step)
        |> assert_visible_model!()
      end)

    assert {:ok, _source, resolved} = Locator.resolve(final.hot, final.cold)
    assert Locator.compare_generation(resolved, final.truth) in [:eq, :gt]
  end

  defp candidate do
    locator =
      Locator.new!(
        flow_id: "flow-1",
        kind: :state,
        version: 1,
        raft_index: 10,
        file_id: {:flow_state, 0},
        offset: 128,
        value_size: 256,
        checksum: <<1>>
      )

    %{
      locator: locator,
      record: %{
        id: "flow-1",
        state_key: "flow/state/tenant-1/flow-1",
        type: "email",
        state: "waiting",
        run_state: "waiting",
        version: 1,
        updated_at_ms: 1_000,
        next_run_at_ms: 900_000,
        priority: 0,
        partition_key: "tenant-1",
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        fencing_token: 1,
        value_refs: %{"payload" => "ref-1"}
      }
    }
  end

  defp locator(overrides) do
    defaults = [
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: {:flow_state, 0},
      offset: 0,
      value_size: 1,
      checksum: <<0>>
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end

  defp random_model_step(model, step) do
    case :rand.uniform(6) do
      1 ->
        next =
          Locator.relocate!(model.truth,
            file_id: {:flow_state, rem(step, 7)},
            offset: step * 10,
            value_size: model.truth.value_size + 1
          )

        next = %{next | version: model.truth.version + 1, raft_index: model.truth.raft_index + 1}
        %{model | truth: next, hot: next}

      2 ->
        if model.hot do
          %{model | cold: model.hot}
        else
          model
        end

      3 ->
        if model.hot && model.cold && Locator.same_physical_record?(model.hot, model.cold) do
          %{model | hot: nil}
        else
          model
        end

      4 ->
        if model.cold && not Locator.stale_for?(model.cold, model.truth) do
          %{model | hot: model.cold}
        else
          model
        end

      5 ->
        cond do
          model.cold && Locator.stale_for?(model.cold, model.truth) ->
            %{model | cold: nil}

          model.cold ->
            {:ok, relocated} =
              Locator.relocate(model.cold,
                file_id: {:flow_state, rem(step, 9)},
                offset: step * 100
              )

            if Locator.same_logical_record?(relocated, model.truth) do
              %{model | cold: relocated, truth: relocated}
            else
              %{model | cold: relocated}
            end

          true ->
            model
        end

      _ ->
        model
    end
  end

  defp assert_visible_model!(model) do
    assert {:ok, _source, resolved} = Locator.resolve(model.hot, model.cold)
    refute Locator.stale_for?(resolved, model.truth)
    model
  end

  defp promotion_row do
    locator = candidate().locator

    %{
      locator: locator,
      park: %{due_at_ms: 900_000},
      park_key: "flow:park:v1:key:abc",
      due_key: "due-key"
    }
  end

  defp promotion_scan_fun(entries_by_prefix) do
    fn prefix, after_key, limit ->
      entries =
        entries_by_prefix
        |> Map.get(prefix, [])
        |> Enum.filter(fn {key, _value} -> is_nil(after_key) or key > after_key end)
        |> Enum.take(limit)

      {:ok, entries}
    end
  end

  defp cold_due_attrs(flow_id, version) do
    [
      type: "email",
      state: "waiting",
      partition_key: "tenant-1",
      priority: 0,
      due_at_ms: 900_000,
      flow_id: flow_id,
      version: version
    ]
  end

  defp temporary_lmdb(suffix) do
    root =
      Path.join(
        System.tmp_dir!(),
        "flow_hibernation_#{suffix}_#{System.unique_integer([:positive])}"
      )

    {root, LMDB.path(root)}
  end
end
