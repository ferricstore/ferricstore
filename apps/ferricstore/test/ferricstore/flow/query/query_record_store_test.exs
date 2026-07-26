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
               hydrate: hydrate
             )
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

  test "fails closed when a located authoritative record remains missing after one retry" do
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
    assert :counters.get(reads, 2) == 2
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
