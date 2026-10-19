defmodule Ferricstore.Flow.Query.QueryRowRelocatorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, Locator}
  alias Ferricstore.Flow.Query.{QueryRowCodec, QueryRowRelocator}

  test "groups physical lookup and CAS-relocates only the central QueryRows" do
    first = row_fixture("run-1", 1, 11, 10)
    second = row_fixture("run-2", 2, 11, 10)
    calls = :counters.new(1, [])
    parent = self()

    physical_location = fn _ctx, 0, {:waraft_apply_projection, 11} ->
      :counters.add(calls, 1, 1)
      {:ok, {3, 900, 240}}
    end

    write_batch = fn "/lmdb", ops ->
      send(parent, {:ops, ops})
      :ok
    end

    assert :ok =
             QueryRowRelocator.relocate_many(
               %{},
               0,
               "/lmdb",
               [first, second],
               physical_location: physical_location,
               write_batch: write_batch,
               get: fn _path, _key -> flunk("successful CAS performed a scalar read") end
             )

    assert :counters.get(calls, 1) == 1
    assert_receive {:ops, ops}
    assert length(ops) == 4

    Enum.each([first, second], fn %{state_key: state_key, encoded: expected, locator: old} ->
      assert {:compare, state_key, expected} in ops

      assert {:put, ^state_key, replacement} =
               Enum.find(ops, &match?({:put, ^state_key, _replacement}, &1))

      assert {:ok, relocated} = QueryRowCodec.decode(replacement, state_key)
      assert Locator.same_logical_record?(old, relocated.locator)
      assert relocated.locator.segment_generation == 3
      assert relocated.locator.offset == 900
      assert relocated.locator.frame_size == 240
      assert relocated.locator.value_size == old.value_size
    end)
  end

  test "isolates a compare race, preserves the newer row, and relocates unaffected rows" do
    raced = row_fixture("run-raced", 1, 11, 10)
    unaffected = row_fixture("run-unaffected", 1, 12, 20)
    newer = row_fixture("run-raced", 2, 13, 700)
    writes = :counters.new(1, [])
    parent = self()

    physical_location = fn
      _ctx, 0, {:waraft_apply_projection, 11} ->
        {:ok, {2, 100, 200}}

      _ctx, 0, {:waraft_apply_projection, 12} ->
        {:ok, {2, 200, 200}}

      _ctx, 0, {:waraft_apply_projection, 13} ->
        {:ok, {newer.locator.segment_generation, newer.locator.offset, newer.locator.frame_size}}
    end

    write_batch = fn "/lmdb", ops ->
      :counters.add(writes, 1, 1)
      send(parent, {:write, ops})

      if :counters.get(writes, 1) == 1,
        do: {:error, {:compare_failed, raced.state_key}},
        else: :ok
    end

    get = fn "/lmdb", key ->
      assert key == raced.state_key
      {:ok, newer.encoded}
    end

    assert :ok =
             QueryRowRelocator.relocate_many(
               %{},
               0,
               "/lmdb",
               [raced, unaffected],
               physical_location: physical_location,
               write_batch: write_batch,
               get: get
             )

    assert_receive {:write, first_ops}
    assert_receive {:write, second_ops}
    assert Enum.any?(first_ops, &match?({:compare, key, _} when key == raced.state_key, &1))
    assert Enum.any?(first_ops, &match?({:compare, key, _} when key == unaffected.state_key, &1))
    refute Enum.any?(second_ops, &match?({:put, key, _} when key == raced.state_key, &1))
    assert Enum.any?(second_ops, &match?({:put, key, _} when key == unaffected.state_key, &1))
    refute_receive {:write, _ops}
  end

  test "repairs only QueryRows that still match the failed physical locator" do
    stale = row_fixture("run-stale", 1, 11, 10)
    requested_old = row_fixture("run-changed", 1, 12, 20)
    already_changed = row_fixture("run-changed", 2, 13, 30)
    parent = self()

    get_many = fn "/lmdb", keys ->
      assert keys == [stale.state_key, requested_old.state_key]
      {:ok, [{:ok, stale.encoded}, {:ok, already_changed.encoded}]}
    end

    physical_location = fn _ctx, 0, {:waraft_apply_projection, 11} ->
      {:ok, {4, 1_100, 260}}
    end

    write_batch = fn "/lmdb", ops ->
      send(parent, {:repair_ops, ops})
      :ok
    end

    assert {:ok, 1} =
             QueryRowRelocator.repair_many(
               %{},
               0,
               "/lmdb",
               [
                 {stale.state_key, stale.locator},
                 {requested_old.state_key, requested_old.locator}
               ],
               get_many: get_many,
               physical_location: physical_location,
               write_batch: write_batch,
               get: fn _path, _key -> flunk("successful repair performed a scalar read") end
             )

    assert_receive {:repair_ops,
                    [
                      {:compare, stale_key, stale_encoded},
                      {:put, stale_key, replacement}
                    ]}

    assert stale_key == stale.state_key
    assert stale_encoded == stale.encoded
    assert {:ok, repaired} = QueryRowCodec.decode(replacement, stale.state_key)
    assert repaired.locator.offset == 1_100
    assert repaired.locator.segment_generation == 4
    refute_receive {:repair_ops, _ops}
  end

  # Projection-log frames are renumbered on every rewrite, so the frame now at a
  # row's old number usually holds another key. The repair must not move the
  # row there.
  test "repair leaves a projection QueryRow alone when the frame now holds another key" do
    row = row_fixture("run-projection", 7, 37_548, 17_670_794, {:waraft_projection, 37_548})
    parent = self()

    assert {:ok, 0} =
             QueryRowRelocator.repair_many(
               %{},
               0,
               "/lmdb",
               [{row.state_key, row.locator}],
               get_many: fn "/lmdb", [_key] -> {:ok, [{:ok, row.encoded}]} end,
               physical_location: fn _ctx, 0, {:waraft_projection, 37_548} ->
                 {:ok, {0, 17_634_744, 635}}
               end,
               verify_projection_frame: fn _ctx, 0, state_key, locator ->
                 send(parent, {:verified, state_key, locator.offset})
                 false
               end,
               write_batch: fn "/lmdb", ops -> send(parent, {:ops, ops}) end,
               get: fn _path, _key -> flunk("no write, so no compare race") end
             )

    assert_receive {:verified, state_key, 17_634_744}
    assert state_key == row.state_key
    refute_receive {:ops, _ops}
  end

  test "repair moves a projection QueryRow when the frame holds its record" do
    row = row_fixture("run-projection", 7, 37_548, 17_670_794, {:waraft_projection, 37_548})
    parent = self()

    assert {:ok, 1} =
             QueryRowRelocator.repair_many(
               %{},
               0,
               "/lmdb",
               [{row.state_key, row.locator}],
               get_many: fn "/lmdb", [_key] -> {:ok, [{:ok, row.encoded}]} end,
               physical_location: fn _ctx, 0, {:waraft_projection, 37_548} ->
                 {:ok, {0, 17_634_744, 2_074}}
               end,
               verify_projection_frame: fn _ctx, 0, _state_key, _locator -> true end,
               write_batch: fn "/lmdb", ops ->
                 send(parent, {:ops, ops})
                 :ok
               end,
               get: fn _path, _key -> flunk("successful repair performed a scalar read") end
             )

    assert_receive {:ops, [{:compare, _key, _expected}, {:put, key, replacement}]}
    assert {:ok, repaired} = QueryRowCodec.decode(replacement, key)
    assert repaired.locator.offset == 17_634_744
    assert repaired.locator.frame_size == 2_074
  end

  # A Flow whose record sits on the projection log stays in the keydir, and the
  # keydir row follows the record when a rewrite renumbers the frames. The repair
  # moves the row there, and the frame number is the locator's raft_index too.
  test "repair follows the keydir to a projection QueryRow's renumbered frame" do
    row = row_fixture("run-projection", 7, 37_548, 17_670_794, {:waraft_projection, 37_548})
    keydir = :ets.new(:relocator_projection_keydir, [:set])

    true =
      :ets.insert(
        keydir,
        {row.state_key, nil, 0, 0, {:waraft_projection, 41_002}, 17_700_000,
         row.locator.value_size}
      )

    parent = self()

    assert {:ok, 1} =
             QueryRowRelocator.repair_many(
               %{keydir_refs: {keydir}},
               0,
               "/lmdb",
               [{row.state_key, row.locator}],
               get_many: fn "/lmdb", [_key] -> {:ok, [{:ok, row.encoded}]} end,
               physical_location: fn
                 _ctx, 0, {:waraft_projection, 41_002} -> {:ok, {2, 17_700_000, 2_074}}
                 _ctx, 0, {:waraft_projection, 37_548} -> {:ok, {0, 17_634_744, 635}}
               end,
               verify_projection_frame: fn _ctx, 0, _state_key, locator ->
                 locator.file_id == {:waraft_projection, 41_002}
               end,
               write_batch: fn "/lmdb", ops ->
                 send(parent, {:ops, ops})
                 :ok
               end,
               get: fn _path, _key -> flunk("successful repair performed a scalar read") end
             )

    assert_receive {:ops, [{:compare, key, expected}, {:put, key, replacement}]}
    assert expected == row.encoded
    assert {:ok, repaired} = QueryRowCodec.decode(replacement, key)
    assert repaired.locator.file_id == {:waraft_projection, 41_002}
    assert repaired.locator.raft_index == 41_002
    assert repaired.locator.segment_generation == 2
    assert repaired.locator.offset == 17_700_000
    assert repaired.locator.frame_size == 2_074
    assert repaired.locator.version == row.locator.version
    assert repaired.locator.checksum == row.locator.checksum
    assert {:ok, original} = QueryRowCodec.decode(row.encoded, key)
    assert repaired.record == original.record
  end

  defp row_fixture(id, version, index, offset, file_id \\ nil) do
    record = %{
      id: id,
      version: version,
      type: "job",
      state: "queued",
      partition_key: "tenant-a",
      created_at_ms: 1,
      updated_at_ms: version
    }

    state_key = Keys.state_key(id, record.partition_key)
    encoded_record = Ferricstore.Flow.encode_record(record)

    locator =
      Locator.new!(
        flow_id: id,
        kind: :state,
        version: version,
        raft_index: index,
        file_id: file_id || {:waraft_apply_projection, index},
        segment_generation: 1,
        offset: offset,
        frame_size: 180,
        value_size: byte_size(encoded_record),
        checksum: :crypto.hash(:sha256, encoded_record)
      )

    assert {:ok, encoded} = QueryRowCodec.encode(state_key, record, locator, 0)
    %{state_key: state_key, encoded: encoded, locator: locator}
  end
end
