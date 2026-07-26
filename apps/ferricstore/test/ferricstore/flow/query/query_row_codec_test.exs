defmodule Ferricstore.Flow.Query.QueryRowCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Codec, Keys, LMDB, Locator, StorageScope}
  alias Ferricstore.Flow.Query.{QueryRow, QueryRowCodec}

  @state_key Keys.state_key("run-1", "tenant-a")

  test "round trips all query metadata and a physical locator" do
    record = record()
    locator = locator(expire_at_ms: 9_000)

    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record, locator, 9_000)
    assert byte_size(encoded) <= QueryRowCodec.max_encoded_bytes()

    assert {:ok,
            %QueryRow{
              state_key: @state_key,
              record: decoded,
              locator: ^locator,
              expire_at_ms: 9_000
            }} = QueryRowCodec.decode(encoded, @state_key)

    assert decoded == record
  end

  test "round trips the worker identity required to rebuild running active indexes" do
    record =
      record()
      |> Map.merge(%{
        state: "running",
        run_state: "queued",
        lease_owner: "worker-1",
        lease_deadline_ms: 450
      })

    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record, locator(), 0)
    assert {:ok, %{record: decoded}} = QueryRowCodec.decode(encoded, @state_key)

    assert decoded.lease_owner == "worker-1"
    assert LMDB.active_projection_entries(decoded) == LMDB.active_projection_entries(record)
  end

  test "preserves missing, null, scalar, and multivalue metadata distinctly" do
    record =
      record()
      |> Map.put(:attributes, %{
        "nullable" => nil,
        "enabled" => true,
        "score" => 1.5,
        "labels" => ["urgent", "finance"]
      })
      |> Map.put(:state_meta, %{
        "queued" => %{"nullable" => nil, "attempt" => 2}
      })

    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record, locator(), 0)
    assert {:ok, %{record: decoded}} = QueryRowCodec.decode(encoded, @state_key)

    assert decoded.attributes["nullable"] == nil
    refute Map.has_key?(decoded.attributes, "missing")
    assert decoded.attributes["labels"] == ["urgent", "finance"]
    assert decoded.state_meta["queued"]["nullable"] == nil
  end

  test "round trips an unpartitioned run through its auto-partition state key" do
    state_key = Keys.state_key("run-1", nil)
    record = %{record() | partition_key: nil}

    assert {:ok, encoded} = QueryRowCodec.encode(state_key, record, locator(), 0)

    assert {:ok, %{state_key: ^state_key, record: ^record}} =
             QueryRowCodec.decode(encoded, state_key)
  end

  test "does not persist payload references or internal system metadata" do
    record =
      record()
      |> Map.put(:payload_ref, "payload-secret")
      |> Map.put(:result_ref, "result-secret")
      |> Map.put(:value_refs, %{"payload" => "value-secret"})
      |> Map.put(:system_metadata, %{})

    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record, locator(), 0)
    assert :nomatch = :binary.match(encoded, "payload-secret")
    assert :nomatch = :binary.match(encoded, "result-secret")
    assert :nomatch = :binary.match(encoded, "value-secret")

    assert {:ok, %{record: decoded}} = QueryRowCodec.decode(encoded, @state_key)
    refute Map.has_key?(decoded, :payload_ref)
    refute Map.has_key?(decoded, :result_ref)
    refute Map.has_key?(decoded, :value_refs)
    refute Map.has_key?(decoded, :system_metadata)
  end

  test "binds opaque shared routing scope while exposing only the logical partition" do
    scope_prefix = <<42::unsigned-big-64>>
    metadata = %{0x8001 => {1, :uint64, :isolation_scope, 42}}

    assert {:ok, physical_partition} =
             StorageScope.physical_partition_key("tenant-a", scope_prefix)

    state_key = Keys.state_key("run-1", physical_partition)

    authoritative =
      record()
      |> Map.put(:partition_key, physical_partition)
      |> Map.put(:system_metadata, metadata)

    assert {:ok, encoded} =
             QueryRowCodec.encode(state_key, authoritative, locator(expire_at_ms: 9_000), 9_000)

    assert {:ok,
            %QueryRow{
              state_key: ^state_key,
              scope_prefix: ^scope_prefix,
              record: %{partition_key: "tenant-a"} = public
            }} = QueryRowCodec.decode(encoded, state_key)

    refute Map.has_key?(public, :system_metadata)

    assert {:ok, foreign_partition} =
             StorageScope.physical_partition_key("tenant-a", <<99::unsigned-big-64>>)

    assert :error =
             QueryRowCodec.encode(
               Keys.state_key("run-1", foreign_partition),
               authoritative,
               locator(expire_at_ms: 9_000),
               9_000
             )
  end

  test "encoding is deterministic across nested map insertion order" do
    first = record()

    second =
      first
      |> Enum.reverse()
      |> Map.new()
      |> Map.update!(:attributes, &(Enum.reverse(&1) |> Map.new()))
      |> Map.update!(:state_meta, fn states ->
        states
        |> Enum.reverse()
        |> Map.new(fn {state, values} -> {state, values |> Enum.reverse() |> Map.new()} end)
      end)

    locator = locator(expire_at_ms: 9_000)
    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, first, locator, 9_000)
    assert {:ok, ^encoded} = QueryRowCodec.encode(@state_key, second, locator, 9_000)
  end

  test "supports every authoritative state locator source" do
    for file_id <- [
          7,
          {:waraft_segment, 8},
          {:waraft_projection, 9},
          {:waraft_apply_projection, 10}
        ] do
      locator =
        if is_tuple(file_id) do
          locator(
            file_id: file_id,
            raft_index: elem(file_id, 1),
            segment_generation: 4,
            frame_size: 1_024
          )
        else
          locator(file_id: file_id, segment_generation: nil, frame_size: nil)
        end

      assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), locator, 0)
      assert {:ok, %{locator: ^locator}} = QueryRowCodec.decode(encoded, @state_key)
    end
  end

  test "rejects logical WARaft locations and round trips the complete physical frame address" do
    logical =
      locator(
        file_id: {:waraft_apply_projection, 99},
        segment_generation: nil,
        frame_size: nil
      )

    assert :error = QueryRowCodec.encode(@state_key, record(), logical, 0)

    physical = %{logical | segment_generation: 7, offset: 4_096, frame_size: 2_048}
    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), physical, 0)
    assert {:ok, %{locator: ^physical}} = QueryRowCodec.decode(encoded, @state_key)
  end

  test "derives large run identities from the state key instead of duplicating them" do
    id = :binary.copy("r", 60_000)
    state_key = Keys.state_key(id, "tenant-a")
    record = %{record() | id: id, root_flow_id: id}
    locator = locator(flow_id: id)

    assert {:ok, encoded} = QueryRowCodec.encode(state_key, record, locator, 0)
    assert byte_size(encoded) < 1_024

    assert {:ok, %{record: %{id: ^id}, locator: %{flow_id: ^id}}} =
             QueryRowCodec.decode(encoded, state_key)
  end

  test "binds the row to its state key and logical generation" do
    assert :error =
             QueryRowCodec.encode(
               @state_key,
               record(),
               locator(flow_id: "other"),
               0
             )

    assert :error = QueryRowCodec.encode(@state_key, record(), locator(version: 8), 0)

    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), locator(), 0)
    refute QueryRowCodec.decode(encoded, Keys.state_key("run-1", "tenant-b")) == {:ok, encoded}
    assert :error = QueryRowCodec.decode(encoded, Keys.state_key("run-1", "tenant-b"))
  end

  test "rejects inconsistent row and locator expiry" do
    assert :error =
             QueryRowCodec.encode(@state_key, record(), locator(expire_at_ms: 8_000), 9_000)

    assert :error = QueryRowCodec.encode(@state_key, record(), locator(expire_at_ms: 1), 0)

    assert {:ok, encoded} =
             QueryRowCodec.encode(@state_key, record(), locator(expire_at_ms: nil), 9_000)

    assert {:ok, %{locator: %{expire_at_ms: nil}, expire_at_ms: 9_000}} =
             QueryRowCodec.decode(encoded, @state_key)

    assert {:ok, encoded} =
             QueryRowCodec.encode(@state_key, record(), locator(expire_at_ms: 10_000), 9_000)

    assert {:ok, %{locator: %{expire_at_ms: 10_000}, expire_at_ms: 9_000}} =
             QueryRowCodec.decode(encoded, @state_key)
  end

  test "query-rich rows are smaller than the removed full-record LMDB envelope" do
    record = %{record() | priority: 2}
    locator = locator(expire_at_ms: 9_000)
    authoritative = Codec.encode_record(record)
    previous_lmdb_value = LMDB.encode_value(authoritative, 9_000)

    assert {:ok, query_row} = QueryRowCodec.encode(@state_key, record, locator, 9_000)
    assert byte_size(query_row) < byte_size(previous_lmdb_value)
  end

  test "relocation compare-and-swaps only the physical locator" do
    old_locator = locator(expire_at_ms: 9_000)

    new_locator =
      Locator.relocate!(old_locator,
        file_id: 4,
        offset: 1_024,
        value_size: 600,
        segment_generation: 5
      )

    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), old_locator, 9_000)

    assert {:ok, relocated} =
             QueryRowCodec.relocate(encoded, @state_key, old_locator, new_locator)

    assert {:ok, %{record: decoded, locator: ^new_locator, expire_at_ms: 9_000}} =
             QueryRowCodec.decode(relocated, @state_key)

    assert decoded == record()

    stale = Locator.relocate!(old_locator, offset: old_locator.offset + 1)

    assert {:error, :locator_compare_failed} =
             QueryRowCodec.relocate(encoded, @state_key, stale, new_locator)

    newer = %{new_locator | version: new_locator.version + 1}

    assert {:error, :logical_generation_mismatch} =
             QueryRowCodec.relocate(encoded, @state_key, old_locator, newer)
  end

  test "applies expiry without decoding expired metadata" do
    assert {:ok, encoded} =
             QueryRowCodec.encode(@state_key, record(), locator(expire_at_ms: 2_000), 2_000)

    assert {:ok, %QueryRow{}} = QueryRowCodec.decode(encoded, @state_key, 1_999)
    assert :expired = QueryRowCodec.decode(encoded, @state_key, 2_000)
  end

  test "decodes a strict hydration reference without materializing query metadata" do
    locator = locator(expire_at_ms: 2_000)
    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), locator, 2_000)

    assert {:ok,
            %{
              state_key: @state_key,
              flow_id: "run-1",
              version: 7,
              locator: ^locator,
              expire_at_ms: 2_000
            } = reference} = QueryRowCodec.decode_reference(encoded, @state_key, 1_999)

    refute Map.has_key?(reference, :record)
    assert :expired = QueryRowCodec.decode_reference(encoded, @state_key, 2_000)
    assert :error = QueryRowCodec.decode_reference(encoded <> <<0>>, @state_key, 0)

    for offset <- 0..(byte_size(encoded) - 1) do
      <<prefix::binary-size(offset), byte, suffix::binary>> = encoded
      corrupted = <<prefix::binary, Bitwise.bxor(byte, 1), suffix::binary>>
      assert :error = QueryRowCodec.decode_reference(corrupted, @state_key, 0)
    end
  end

  test "rejects oversized metadata and invalid dynamic values" do
    too_many_attributes =
      for index <- 1..17, into: %{}, do: {"key-#{index}", "value"}

    assert :error =
             QueryRowCodec.encode(
               @state_key,
               Map.put(record(), :attributes, too_many_attributes),
               locator(),
               0
             )

    assert :error =
             QueryRowCodec.encode(
               @state_key,
               put_in(record(), [:state_meta, "queued", "bad"], %{nested: true}),
               locator(),
               0
             )

    assert :error =
             QueryRowCodec.encode(
               @state_key,
               put_in(record(), [:attributes, "too_big"], :binary.copy("x", 257)),
               locator(),
               0
             )

    for invalid_attributes <- [
          %{"empty" => []},
          %{"duplicate" => ["same", "same"]},
          %{"typed" => ["valid", 1]},
          %{String.duplicate("x", 65) => "value"},
          %{" padded" => "value"}
        ] do
      assert :error =
               QueryRowCodec.encode(
                 @state_key,
                 Map.put(record(), :attributes, invalid_attributes),
                 locator(),
                 0
               )
    end

    for invalid_state_meta <- [
          %{"queued" => %{"list" => ["not-scalar"]}},
          %{" queued" => %{"worker" => "one"}},
          %{"queued" => %{" worker" => "one"}}
        ] do
      assert :error =
               QueryRowCodec.encode(
                 @state_key,
                 Map.put(record(), :state_meta, invalid_state_meta),
                 locator(),
                 0
               )
    end
  end

  test "validates the bounded record before a physical locator exists" do
    max_metadata_bytes = QueryRowCodec.max_metadata_bytes()
    expected_error = "ERR flow query metadata exceeds #{max_metadata_bytes} bytes"

    assert :ok = QueryRowCodec.validate_record(@state_key, record())

    oversized = Map.put(record(), :type, :binary.copy("t", max_metadata_bytes))

    assert {:error, ^expected_error} =
             QueryRowCodec.validate_record(@state_key, oversized)

    assert :error = QueryRowCodec.encode(@state_key, oversized, locator(), 0)
  end

  test "every admitted record fits with the largest valid locator" do
    locator = locator(checksum: :binary.copy(<<1>>, 32))

    assert :ok = QueryRowCodec.validate_record(@state_key, record())
    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), locator, 0)
    assert byte_size(encoded) <= QueryRowCodec.max_encoded_bytes()
  end

  test "requires one fixed SHA-256 checksum in every query-row locator" do
    for checksum <- [nil, <<>>, :binary.copy(<<1>>, 31), :binary.copy(<<1>>, 33)] do
      invalid_locator = %{locator() | checksum: checksum}

      assert :error =
               QueryRowCodec.encode(@state_key, record(), invalid_locator, 0)
    end
  end

  test "rejects truncation, trailing bytes, malformed lengths, and unknown versions" do
    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), locator(), 0)

    for bytes <- 0..(byte_size(encoded) - 1) do
      assert :error = QueryRowCodec.decode(binary_part(encoded, 0, bytes), @state_key)
    end

    assert :error = QueryRowCodec.decode(encoded <> <<0>>, @state_key)
    assert :error = QueryRowCodec.decode(:binary.copy(<<0xFF>>, byte_size(encoded)), @state_key)
  end

  test "detects a single-bit corruption at every byte position" do
    assert {:ok, encoded} = QueryRowCodec.encode(@state_key, record(), locator(), 0)

    for offset <- 0..(byte_size(encoded) - 1) do
      <<prefix::binary-size(offset), byte, suffix::binary>> = encoded
      corrupted = <<prefix::binary, Bitwise.bxor(byte, 1), suffix::binary>>
      assert :error = QueryRowCodec.decode(corrupted, @state_key)
    end
  end

  defp record do
    %{
      id: "run-1",
      type: "invoice",
      state: "queued",
      version: 7,
      priority: -2,
      partition_key: "tenant-a",
      created_at_ms: 100,
      updated_at_ms: 200,
      next_run_at_ms: 300,
      lease_deadline_ms: nil,
      attempts: 2,
      run_state: "ready",
      max_active_ms: 10_000,
      state_enter_seq: 42,
      history_max_events: 100_000,
      history_hot_max_events: 1_000,
      parent_flow_id: "parent",
      root_flow_id: "root",
      correlation_id: "correlation",
      attributes: %{"tier" => "gold", "labels" => ["urgent", "finance"]},
      indexed_attributes: ["tier"],
      state_meta: %{
        "created" => %{"source" => "api"},
        "queued" => %{"worker" => "worker-1", "score" => 1.5}
      },
      indexed_state_meta: "worker"
    }
  end

  defp locator(overrides \\ []) do
    defaults = [
      flow_id: "run-1",
      kind: :state,
      version: 7,
      raft_index: 99,
      file_id: 3,
      offset: 128,
      value_size: 512,
      frame_size: nil,
      checksum: :binary.copy(<<1>>, 32),
      expire_at_ms: nil,
      segment_generation: 4
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end
end
