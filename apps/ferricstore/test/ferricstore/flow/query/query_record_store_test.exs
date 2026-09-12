defmodule Ferricstore.Flow.Query.QueryRecordStoreTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, Locator}
  alias Ferricstore.Flow.Query.{MemoryBudget, QueryRecordStore, QueryRow, QueryRowReference}

  test "derives a bounded hydration input budget from the replicated value limit" do
    assert QueryRecordStore.max_input_bytes(%{max_value_size: 1_048_576}) == 1_114_112
    assert QueryRecordStore.max_input_bytes(%{}) == 1_114_112

    assert QueryRecordStore.max_input_bytes(%{max_value_size: 2_000_000_000}) ==
             1_073_741_824
  end

  test "hydrates present query rows in order while preserving missing positions" do
    first = record("run-1", 1)
    third = record("run-3", 3)
    rows = [row(first, 10), nil, row(third, 30)]
    keys = Enum.map([first, record("run-2", 2), third], &state_key/1)

    row_read = fn _path, ^keys, 1_000, _max_bytes ->
      {:ok, rows, 300, true}
    end

    hydrate = fn _ctx, 0, requests, opts ->
      assert Keyword.fetch!(opts, :max_bytes) > 0
      assert Enum.map(requests, &elem(&1, 0)) == [state_key(first), state_key(third)]
      {:ok, [first, third]}
    end

    assert {:ok, [^first, nil, ^third], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", keys, 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: fn _ctx, _shard, _path, _requests ->
                 flunk("healthy hydration must not enter locator repair")
               end
             )
  end

  test "recovery reads only hydrate query rows written before the replayed command" do
    future = record("run-from-future", 8)
    current = record("run-at-replay-index", 7)
    previous = record("run-before-replay-index", 3)
    keys = Enum.map([future, current, previous], &state_key/1)
    rows = [row(future, 80), row(current, 70), row(previous, 30)]

    assert {:ok, [nil, nil, ^previous], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", keys, 1_000, 10_000,
               before_raft_index: 7,
               query_row_read: fn _path, ^keys, 1_000, _max_bytes ->
                 {:ok, rows, 300, true}
               end,
               hydrate: fn _ctx, 0, requests, _opts ->
                 assert Enum.map(requests, &elem(&1, 0)) == [state_key(previous)]

                 {:ok, [previous]}
               end,
               repair_locators: fn _ctx, _shard, _path, _requests ->
                 flunk("a future query row must not reach locator repair")
               end
             )
  end

  test "replay ordering also filters compact query-row references" do
    previous = record("run-before-replay", 4)
    future = record("run-after-replay", 10)
    keys = [state_key(previous), state_key(future)]
    rows = [reference(previous, 40), reference(future, 100)]

    assert {:ok, [^previous, nil], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", keys, 1_000, 10_000,
               before_raft_index: 9,
               query_row_read: fn _path, ^keys, 1_000, _max_bytes ->
                 {:ok, rows, 200, true}
               end,
               hydrate: fn _ctx, 0, [{key, _locator}], _opts ->
                 assert key == state_key(previous)
                 {:ok, [previous]}
               end
             )
  end

  test "recovery hydration retries cannot observe rows after the replay index" do
    current = record("run-retry-current", 6)
    future = record("run-retry-future", 9)
    current_key = state_key(current)
    future_key = state_key(future)
    keys = [current_key, future_key]
    stale = row(current, 70)
    relocated = row(current, 71)
    future_row = row(future, 90)
    calls = :counters.new(2, [])

    row_read = fn _path, ^keys, 1_000, _max_bytes ->
      :counters.add(calls, 1, 1)
      current_row = if :counters.get(calls, 1) == 1, do: stale, else: relocated
      {:ok, [current_row, future_row], 200, true}
    end

    hydrate = fn _ctx, 0, requests, _opts ->
      :counters.add(calls, 2, 1)
      assert Enum.map(requests, &elem(&1, 0)) == [current_key]

      case :counters.get(calls, 2) do
        1 -> {:error, :hydrated_record_identity_mismatch}
        2 -> {:ok, [current]}
      end
    end

    assert {:ok, [^current, nil], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", keys, 1_000, 10_000,
               before_raft_index: 7,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: fn _ctx, _shard, _path, _requests ->
                 flunk("a relocated eligible row must not require repair")
               end
             )

    assert :counters.get(calls, 1) == 2
    assert :counters.get(calls, 2) == 2
  end

  test "recovery locator repair rereads cannot observe rows after the replay index" do
    current = record("run-repair-current", 6)
    future = record("run-repair-future", 9)
    current_key = state_key(current)
    future_key = state_key(future)
    keys = [current_key, future_key]
    stale = row(current, 70)
    relocated = row(current, 71)
    future_row = row(future, 90)
    calls = :counters.new(3, [])

    row_read = fn _path, ^keys, 1_000, _max_bytes ->
      :counters.add(calls, 1, 1)
      current_row = if :counters.get(calls, 3) == 0, do: stale, else: relocated
      {:ok, [current_row, future_row], 200, true}
    end

    hydrate = fn _ctx, 0, requests, _opts ->
      :counters.add(calls, 2, 1)
      assert Enum.map(requests, &elem(&1, 0)) == [current_key]

      case :counters.get(calls, 2) do
        1 -> {:ok, [nil]}
        2 -> {:ok, [current]}
      end
    end

    repair = fn _ctx, 0, "/lmdb", [{^current_key, locator}] ->
      assert locator == stale.locator
      :counters.add(calls, 3, 1)
      {:ok, 1}
    end

    assert {:ok, [^current, nil], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", keys, 1_000, 10_000,
               before_raft_index: 7,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: repair
             )

    assert :counters.get(calls, 1) == 3
    assert :counters.get(calls, 2) == 2
    assert :counters.get(calls, 3) == 1
  end

  test "rejects an invalid replay index boundary before storage IO" do
    key = state_key(record("invalid-replay-boundary", 1))

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               before_raft_index: 0,
               query_row_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("an invalid replay boundary reached query storage")
               end
             )
  end

  test "replay ordering does not hide non-WARaft storage locators" do
    record = record("bitcask-row", 12)
    key = state_key(record)
    query_row = row(record, 120)
    bitcask_row = %{query_row | locator: %{query_row.locator | file_id: 1}}

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               before_raft_index: 5,
               query_row_read: fn _path, [^key], 1_000, _max_bytes ->
                 {:ok, [bitcask_row], 100, true}
               end,
               hydrate: fn _ctx, 0, [{^key, locator}], _opts ->
                 assert locator.file_id == 1
                 {:ok, [record]}
               end
             )
  end

  test "replay file visibility is strict only for WARaft storage identities" do
    assert QueryRecordStore.visible_file_id_before_raft_index?({:waraft_segment, 4}, 5)
    refute QueryRecordStore.visible_file_id_before_raft_index?({:waraft_segment, 5}, 5)
    refute QueryRecordStore.visible_file_id_before_raft_index?({:waraft_projection, 6}, 5)

    refute QueryRecordStore.visible_file_id_before_raft_index?(
             {:waraft_apply_projection, 5},
             5
           )

    assert QueryRecordStore.visible_file_id_before_raft_index?(17, 5)
    assert QueryRecordStore.visible_file_id_before_raft_index?(:pending, 5)
    assert QueryRecordStore.visible_file_id_before_raft_index?({:flow_history, 99}, 5)
    assert QueryRecordStore.visible_file_id_before_raft_index?({:waraft_segment, 99}, nil)
  end

  test "hydrates strict query-row references without requiring metadata maps" do
    record = record("run-reference", 4)
    key = state_key(record)
    reference = reference(record, 20)

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      {:ok, [reference], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, locator}], _opts ->
      assert locator == reference.locator
      {:ok, [record]}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )

    mismatched = %{reference | version: reference.version + 1}

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: fn _path, [^key], 1_000, _max_bytes ->
                 {:ok, [mismatched], 100, true}
               end,
               hydrate: fn _ctx, _shard, _requests, _opts ->
                 flunk("an inconsistent hydration reference reached authoritative storage")
               end
             )
  end

  test "forwards the explicit include-expired mode to authoritative hydration" do
    record = record("run-expired", 2)
    key = state_key(record)
    expired_row = %{row(record, 10) | expire_at_ms: 500}

    row_read = fn _path, [^key], 0, _max_bytes ->
      {:ok, [expired_row], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], opts ->
      assert Keyword.fetch!(opts, :include_expired)
      {:ok, [record]}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               include_expired: true,
               query_row_read: row_read,
               hydrate: hydrate
             )
  end

  test "retention can fall back to validated metadata for an expired row with a retired source" do
    record = record("run-expired-retired", 2)
    key = state_key(record)
    expired_row = %{row(record, 10) | expire_at_ms: 500}
    reads = :counters.new(1, [])

    row_read = fn _path, [^key], 0, _max_bytes ->
      :counters.add(reads, 1, 1)
      {:ok, [expired_row], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], opts ->
      assert Keyword.fetch!(opts, :include_expired)
      {:ok, [nil]}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               include_expired: true,
               expired_query_row_fallback: true,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: fn _ctx, _shard, _path, _requests ->
                 flunk("an expired row with validated metadata must not require source repair")
               end
             )

    assert :counters.get(reads, 1) == 2
  end

  test "recovers expired QueryRow metadata after an authoritative identity mismatch" do
    record = record("run-expired-identity-fallback", 2)
    key = state_key(record)
    expired_row = %{row(record, 10) | expire_at_ms: 500}
    hydration_calls = :atomics.new(1, [])

    hydrate = fn _ctx, 0, [{^key, _locator}], opts ->
      assert Keyword.fetch!(opts, :include_expired)

      case :atomics.add_get(hydration_calls, 1, 1) do
        1 -> {:error, :hydrated_record_identity_mismatch}
        2 -> {:ok, [nil]}
      end
    end

    assert {:ok, [%{id: "run-expired-identity-fallback"}], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               include_expired: true,
               expired_query_row_fallback: true,
               query_row_read: fn _path, [^key], 0, _max_bytes ->
                 {:ok, [expired_row], 100, true}
               end,
               hydrate: hydrate,
               repair_locators: fn _ctx, _shard, _path, _requests ->
                 flunk("expired validated metadata must not require locator repair")
               end
             )

    assert :atomics.get(hydration_calls, 1) == 2
  end

  test "expired fallback preserves successfully hydrated references in the same batch" do
    expired = record("run-expired-mixed", 2)
    live = record("run-live-mixed", 3)
    expired_key = state_key(expired)
    live_key = state_key(live)
    expired_row = %{row(expired, 10) | expire_at_ms: 500}
    live_reference = reference(live, 20)

    row_read = fn _path, [^expired_key, ^live_key], 0, _max_bytes ->
      {:ok, [expired_row, live_reference], 200, true}
    end

    hydrate = fn _ctx, 0, [{^expired_key, _}, {^live_key, _}], _opts ->
      {:ok, [nil, live]}
    end

    assert {:ok, [^expired, ^live], true} =
             QueryRecordStore.read_many(
               context(),
               0,
               "/lmdb",
               [expired_key, live_key],
               1_000,
               10_000,
               include_expired: true,
               expired_query_row_fallback: true,
               query_row_read: row_read,
               hydrate: hydrate
             )
  end

  test "rejects a non-boolean include-expired mode before reading storage" do
    key = state_key(record("run-invalid-expiry-mode", 1))

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               include_expired: :yes,
               query_row_read: fn _path, _keys, _now_ms, _max_bytes ->
                 flunk("invalid expiry mode reached QueryRow storage")
               end,
               hydrate: fn _ctx, _shard, _requests, _opts ->
                 flunk("invalid expiry mode reached authoritative storage")
               end
             )
  end

  test "fails closed without relocation when hydrated bytes are invalid" do
    record = record("run-invalid-hydrated-record", 2)
    key = state_key(record)

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: fn _path, [^key], 1_000, _max_bytes ->
                 {:ok, [row(record, 20)], 100, true}
               end,
               hydrate: fn _ctx, 0, [{^key, _locator}], _opts ->
                 {:error, :invalid_hydrated_record}
               end,
               repair_locators: fn _ctx, _shard, _path, _requests ->
                 flunk("invalid hydrated bytes must not be treated as a relocation race")
               end
             )
  end

  test "rejects a query row with a non-durable locator before authoritative IO" do
    record = record("run-invalid-locator", 1)
    key = state_key(record)
    row = row(record, 10)
    malformed = %{row | locator: %{row.locator | file_id: {:flow_state, 0}}}

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      {:ok, [malformed], 100, true}
    end

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: fn _ctx, _shard, _requests, _opts ->
                 flunk("a non-durable locator reached authoritative storage")
               end
             )
  end

  test "re-resolves query rows once when compaction races hydration" do
    record = record("run-1", 1)
    old = row(record, 10)
    relocated = row(record, 20)
    key = state_key(record)
    counter = :counters.new(2, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(counter, 1, 1)
      call = :counters.get(counter, 1)
      {:ok, [if(call == 1, do: old, else: relocated)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, locator}], _opts ->
      :counters.add(counter, 2, 1)
      call = :counters.get(counter, 2)

      if call == 1 do
        assert locator.offset == 10
        {:error, :hydrated_record_identity_mismatch}
      else
        assert locator.offset == 20
        {:ok, [record]}
      end
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )

    assert :counters.get(counter, 1) == 2
    assert :counters.get(counter, 2) == 2
  end

  test "retries an unavailable old segment only after its locator is relocated" do
    record = record("run-1", 1)
    old = row(record, 10)
    relocated = row(record, 20)
    key = state_key(record)
    counter = :counters.new(2, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(counter, 1, 1)
      call = :counters.get(counter, 1)
      {:ok, [if(call == 1, do: old, else: relocated)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, locator}], _opts ->
      :counters.add(counter, 2, 1)

      case :counters.get(counter, 2) do
        1 ->
          assert locator.offset == 10
          {:error, :enoent}

        2 ->
          assert locator.offset == 20
          {:ok, [record]}
      end
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )

    assert :counters.get(counter, 1) == 2
    assert :counters.get(counter, 2) == 2
  end

  test "repairs an unchanged stale physical locator after a completed compaction rewrite" do
    record = record("run-crash-repair", 1)
    old = row(record, 10)
    relocated = row(record, 20)
    key = state_key(record)
    calls = :counters.new(3, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(calls, 1, 1)
      current = if :counters.get(calls, 3) == 0, do: old, else: relocated
      {:ok, [current], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, locator}], _opts ->
      :counters.add(calls, 2, 1)

      case :counters.get(calls, 2) do
        1 ->
          assert locator == old.locator
          {:error, :enoent}

        2 ->
          assert locator == relocated.locator
          {:ok, [record]}
      end
    end

    repair = fn _ctx, 0, "/lmdb", [{^key, locator}] ->
      assert locator == old.locator
      :counters.add(calls, 3, 1)
      {:ok, 1}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: repair
             )

    assert :counters.get(calls, 1) == 3
    assert :counters.get(calls, 2) == 2
    assert :counters.get(calls, 3) == 1
  end

  test "repairs an unchanged locator after hydration reads the wrong record" do
    record = record("run-identity-repair", 1)
    old = row(record, 10)
    relocated = row(record, 20)
    key = state_key(record)
    calls = :counters.new(3, [])

    row_read = fn _path, [^key], _visibility_ms, _max_bytes ->
      :counters.add(calls, 1, 1)
      current = if :counters.get(calls, 3) == 0, do: old, else: relocated
      {:ok, [current], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, locator}], _opts ->
      :counters.add(calls, 2, 1)

      case :counters.get(calls, 2) do
        1 ->
          assert locator == old.locator
          {:error, :hydrated_record_identity_mismatch}

        2 ->
          assert locator == relocated.locator
          {:ok, [record]}
      end
    end

    repair = fn _ctx, 0, "/lmdb", [{^key, locator}] ->
      assert locator == old.locator
      :counters.add(calls, 3, 1)
      {:ok, 1}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               include_expired: true,
               expired_query_row_fallback: true,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: repair
             )

    assert :counters.get(calls, 1) == 3
    assert :counters.get(calls, 2) == 2
    assert :counters.get(calls, 3) == 1
  end

  test "repairs an unchanged locator after hydration cannot find its record" do
    record = record("run-missing-repair", 1)
    old = row(record, 10)
    relocated = row(record, 20)
    key = state_key(record)
    calls = :counters.new(3, [])

    row_read = fn _path, [^key], _visibility_ms, _max_bytes ->
      :counters.add(calls, 1, 1)
      current = if :counters.get(calls, 3) == 0, do: old, else: relocated
      {:ok, [current], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, locator}], _opts ->
      :counters.add(calls, 2, 1)

      case :counters.get(calls, 2) do
        1 ->
          assert locator == old.locator
          {:ok, [nil]}

        2 ->
          assert locator == relocated.locator
          {:ok, [record]}
      end
    end

    repair = fn _ctx, 0, "/lmdb", [{^key, locator}] ->
      assert locator == old.locator
      :counters.add(calls, 3, 1)
      {:ok, 1}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               include_expired: true,
               expired_query_row_fallback: true,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: repair
             )

    assert :counters.get(calls, 1) == 3
    assert :counters.get(calls, 2) == 2
    assert :counters.get(calls, 3) == 1
  end

  test "repairs only missing rows from a partially hydrated batch" do
    missing = record("run-partial-missing", 1)
    healthy = record("run-partial-healthy", 1)
    missing_key = state_key(missing)
    healthy_key = state_key(healthy)
    keys = [missing_key, healthy_key]
    stale_missing = row(missing, 10)
    repaired_missing = row(missing, 20)
    healthy_row = row(healthy, 30)
    calls = :counters.new(3, [])

    row_read = fn _path, ^keys, 1_000, _max_bytes ->
      :counters.add(calls, 1, 1)

      rows =
        if :counters.get(calls, 3) == 0,
          do: [stale_missing, healthy_row],
          else: [repaired_missing, healthy_row]

      {:ok, rows, 200, true}
    end

    hydrate = fn _ctx, 0, requests, _opts ->
      :counters.add(calls, 2, 1)
      assert Enum.map(requests, &elem(&1, 0)) == keys

      case :counters.get(calls, 2) do
        1 -> {:ok, [nil, healthy]}
        2 -> {:ok, [missing, healthy]}
      end
    end

    repair = fn _ctx, 0, "/lmdb", [{^missing_key, locator}] ->
      assert locator == stale_missing.locator
      :counters.add(calls, 3, 1)
      {:ok, 1}
    end

    assert {:ok, [^missing, ^healthy], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", keys, 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: repair
             )

    assert :counters.get(calls, 1) == 3
    assert :counters.get(calls, 2) == 2
    assert :counters.get(calls, 3) == 1
  end

  test "fails closed when identity-mismatch locator repair makes no progress" do
    record = record("run-identity-no-repair", 1)
    unchanged = row(record, 10)
    key = state_key(record)
    calls = :counters.new(3, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(calls, 1, 1)
      {:ok, [unchanged], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], _opts ->
      :counters.add(calls, 2, 1)
      {:error, :hydrated_record_identity_mismatch}
    end

    repair = fn _ctx, 0, "/lmdb", [{^key, _locator}] ->
      :counters.add(calls, 3, 1)
      {:ok, 0}
    end

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: repair
             )

    assert :counters.get(calls, 1) == 2
    assert :counters.get(calls, 2) == 1
    assert :counters.get(calls, 3) == 1
  end

  test "reports storage unavailable when identity-mismatch repair cannot read storage" do
    record = record("run-identity-repair-error", 1)
    key = state_key(record)
    unchanged = row(record, 10)

    assert {:error, :query_storage_unavailable} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: fn _path, [^key], 1_000, _max_bytes ->
                 {:ok, [unchanged], 100, true}
               end,
               hydrate: fn _ctx, 0, [{^key, _locator}], _opts ->
                 {:error, :hydrated_record_identity_mismatch}
               end,
               repair_locators: fn _ctx, 0, "/lmdb", [{^key, _locator}] ->
                 {:error, :lmdb_busy}
               end
             )
  end

  test "fails closed when identity mismatch persists after a successful repair" do
    record = record("run-persistent-identity", 1)
    key = state_key(record)
    stale = row(record, 10)
    relocated = row(record, 20)
    repaired? = :atomics.new(1, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      row = if :atomics.get(repaired?, 1) == 0, do: stale, else: relocated
      {:ok, [row], 100, true}
    end

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: fn _ctx, 0, [{^key, _locator}], _opts ->
                 {:error, :hydrated_record_identity_mismatch}
               end,
               repair_locators: fn _ctx, 0, "/lmdb", [{^key, _locator}] ->
                 :atomics.put(repaired?, 1, 1)
                 {:ok, 1}
               end
             )
  end

  test "honors the shared deadline before identity-mismatch repair" do
    record = record("run-identity-repair-deadline", 1)
    key = state_key(record)
    unchanged = row(record, 10)
    clock_calls = :atomics.new(1, [])

    clock_ms = fn ->
      call = :atomics.add_get(clock_calls, 1, 1)
      if call < 3, do: 0, else: 10
    end

    assert {:error, :query_deadline_exceeded} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               timeout_ms: 10,
               clock_ms: clock_ms,
               query_row_read: fn _path, [^key], 1_000, _max_bytes ->
                 {:ok, [unchanged], 100, true}
               end,
               hydrate: fn _ctx, 0, [{^key, _locator}], _opts ->
                 {:error, :hydrated_record_identity_mismatch}
               end,
               repair_locators: fn _ctx, _shard, _path, _requests ->
                 flunk("repair must not start after the shared deadline")
               end
             )
  end

  test "shares one timeout budget across a relocated hydration retry" do
    record = record("run-deadline", 1)
    old = row(record, 10)
    relocated = row(record, 20)
    key = state_key(record)
    calls = :counters.new(2, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(calls, 1, 1)
      row = if :counters.get(calls, 1) == 1, do: old, else: relocated
      {:ok, [row], 100, true}
    end

    clock_ms = fn ->
      call = :counters.get(calls, 2)
      :counters.add(calls, 2, 1)

      case call do
        0 -> 0
        1 -> 0
        _retry -> 7
      end
    end

    hydrate = fn _ctx, 0, [{^key, locator}], opts ->
      case locator.offset do
        10 ->
          assert Keyword.fetch!(opts, :timeout_ms) == 10
          {:error, :enoent}

        20 ->
          assert Keyword.fetch!(opts, :timeout_ms) == 3
          {:ok, [record]}
      end
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               timeout_ms: 10,
               clock_ms: clock_ms,
               query_row_read: row_read,
               hydrate: hydrate
             )
  end

  test "does not repeat unavailable IO when re-resolution returns the same locator" do
    record = record("run-1", 1)
    unchanged = row(record, 10)
    key = state_key(record)
    counter = :counters.new(2, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(counter, 1, 1)
      {:ok, [unchanged], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], _opts ->
      :counters.add(counter, 2, 1)
      {:error, :enoent}
    end

    assert {:error, :query_storage_unavailable} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )

    assert :counters.get(counter, 1) == 2
    assert :counters.get(counter, 2) == 1
  end

  test "forwards the authoritative read timeout" do
    record = record("run-timeout", 1)
    key = state_key(record)

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      {:ok, [row(record, 10)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], opts ->
      assert Keyword.fetch!(opts, :timeout_ms) == 37
      {:ok, [record]}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               timeout_ms: 37,
               clock_ms: fn -> 0 end,
               query_row_read: row_read,
               hydrate: hydrate
             )
  end

  test "preserves hydration timeouts without re-resolving the locator" do
    record = record("run-timeout", 1)
    key = state_key(record)
    reads = :counters.new(1, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(reads, 1, 1)
      {:ok, [row(record, 10)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], _opts ->
      {:error, :hydration_timeout}
    end

    assert {:error, :query_deadline_exceeded} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )

    assert :counters.get(reads, 1) == 1
  end

  test "fails closed when a locator is still inconsistent after one retry" do
    record = record("run-1", 1)
    key = state_key(record)

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      {:ok, [row(record, 10)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], _opts ->
      {:error, :hydrated_record_identity_mismatch}
    end

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )
  end

  test "fails closed when a missing authoritative record cannot be relocated" do
    record = record("run-missing", 1)
    key = state_key(record)
    reads = :counters.new(2, [])

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      :counters.add(reads, 1, 1)
      {:ok, [row(record, 10)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], opts ->
      :counters.add(reads, 2, 1)
      assert Keyword.fetch!(opts, :now_ms) == 1_000
      {:ok, [nil]}
    end

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )

    assert :counters.get(reads, 1) == 2
    assert :counters.get(reads, 2) == 1
  end

  test "recovery fallback fails closed when a live missing record cannot be relocated" do
    record = record("run-live-recovery-missing", 1)
    key = state_key(record)
    reads = :counters.new(3, [])

    row_read = fn _path, [^key], 0, _max_bytes ->
      :counters.add(reads, 1, 1)
      {:ok, [row(record, 10)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], opts ->
      :counters.add(reads, 2, 1)
      assert Keyword.fetch!(opts, :include_expired)
      assert Keyword.fetch!(opts, :now_ms) == 1_000
      {:ok, [nil]}
    end

    repair = fn _ctx, 0, "/lmdb", [{^key, _locator}] ->
      :counters.add(reads, 3, 1)
      {:ok, 0}
    end

    assert {:error, :query_storage_inconsistent} =
             QueryRecordStore.read_many(context(), 0, "/lmdb", [key], 1_000, 10_000,
               include_expired: true,
               expired_query_row_fallback: true,
               query_row_read: row_read,
               hydrate: hydrate,
               repair_locators: repair
             )

    assert :counters.get(reads, 1) == 2
    assert :counters.get(reads, 2) == 2
    assert :counters.get(reads, 3) == 1
  end

  test "reserves decoded query-row memory before admitting authoritative bytes" do
    record = record("run-1", 1)
    row = row(record, 10)
    key = state_key(record)
    max_input_bytes = 10_000

    row_read = fn _path, [^key], 1_000, ^max_input_bytes ->
      {:ok, [row], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], opts ->
      available_memory = MemoryBudget.decoded_record_reservation(max_input_bytes)

      expected =
        available_memory
        |> Kernel.-(MemoryBudget.term_bytes([row]))
        |> MemoryBudget.encoded_record_input_bytes()

      assert Keyword.fetch!(opts, :max_bytes) == expected
      {:ok, [record]}
    end

    assert {:ok, [^record], true} =
             QueryRecordStore.read_many(
               context(),
               0,
               "/lmdb",
               [key],
               1_000,
               max_input_bytes,
               query_row_read: row_read,
               hydrate: hydrate
             )
  end

  test "supports validated encoded reads without re-encoding authoritative records" do
    record = record("run-1", 1)
    encoded = "authoritative-record-bytes"
    key = state_key(record)

    row_read = fn _path, [^key], 1_000, _max_bytes ->
      {:ok, [row(record, 10)], 100, true}
    end

    hydrate = fn _ctx, 0, [{^key, _locator}], _opts -> {:ok, [encoded]} end

    assert {:ok, [^encoded], true} =
             QueryRecordStore.read_encoded_many(
               context(),
               0,
               "/lmdb",
               [key],
               1_000,
               10_000,
               query_row_read: row_read,
               hydrate: hydrate
             )
  end

  defp row(record, offset) do
    %QueryRow{
      state_key: state_key(record),
      record: record,
      locator:
        Locator.new!(
          flow_id: record.id,
          kind: :state,
          version: record.version,
          raft_index: record.version,
          file_id: {:waraft_apply_projection, record.version},
          offset: offset,
          value_size: 100,
          frame_size: 180,
          segment_generation: 1,
          checksum: :binary.copy(<<1>>, 32)
        ),
      expire_at_ms: 0
    }
  end

  defp reference(record, offset) do
    %QueryRowReference{
      state_key: state_key(record),
      flow_id: record.id,
      version: record.version,
      locator: row(record, offset).locator,
      expire_at_ms: 0
    }
  end

  defp record(id, version) do
    %{
      id: id,
      version: version,
      type: "job",
      state: "queued",
      partition_key: "tenant-a",
      updated_at_ms: version
    }
  end

  defp state_key(record), do: Keys.state_key(record.id, record.partition_key)
  defp context, do: %{data_dir: "/unused"}
end
