defmodule Ferricstore.FlowCodecTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow
  alias Ferricstore.Flow.RecordProjection

  test "record codec round trips small and nil fields" do
    record = base_record()

    assert Flow.encode_record(record) == Flow.encode_record_elixir(record)

    decoded =
      record
      |> Flow.encode_record_elixir()
      |> Flow.decode_record()

    assert decoded == Flow.decode_record_elixir(Flow.encode_record_elixir(record))
    assert decoded == record
  end

  test "record codec round trips fields that require multibyte length headers" do
    long_type = String.duplicate("a", 200)
    record = %{base_record() | type: long_type, correlation_id: String.duplicate("c", 180)}

    assert Flow.encode_record(record) == Flow.encode_record_elixir(record)

    decoded =
      record
      |> Flow.encode_record_elixir()
      |> Flow.decode_record()

    assert decoded == Flow.decode_record_elixir(Flow.encode_record_elixir(record))
    assert decoded == record
  end

  test "record codec omits redundant immutable defaults from every state version" do
    record =
      base_record()
      |> Map.merge(%{
        id: "flow-compact-state",
        type: "iot-fanout-worker",
        state: "queued",
        version: 1,
        attempts: 0,
        fencing_token: 0,
        next_run_at_ms: nil,
        priority: 0,
        ttl_ms: nil,
        root_flow_id: "flow-compact-state",
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        run_state: nil,
        payload_ref: "shared-payload-ref",
        result_ref: nil,
        error_ref: nil,
        child_groups: %{}
      })

    encoded = Flow.encode_record(record)

    assert Flow.decode_record(encoded) == record
    assert byte_size(encoded) <= 150
    assert length(:binary.matches(encoded, "flow-compact-state")) == 1
    refute String.contains?(encoded, "J{}")
  end

  test "record meta codec matches full record projection" do
    record =
      base_record()
      |> Map.merge(%{
        id: "flow-meta",
        type: "meta-worker",
        state: "waiting",
        version: 42,
        attempts: 3,
        fencing_token: 11,
        created_at_ms: 1_000,
        updated_at_ms: 2_000,
        next_run_at_ms: 3_000,
        priority: 7,
        partition_key: "tenant-a",
        payload_ref: "flow/value/flow-meta/payload/1",
        result_ref: "flow/value/flow-meta/result/2",
        error_ref: "flow/value/flow-meta/error/3",
        lease_owner: "worker-7",
        lease_token: "lease-7",
        lease_deadline_ms: 4_000,
        run_state: "running",
        value_refs: %{
          "reservation" => %{
            ref: "flow/value/flow-meta/reservation/1",
            version: 1,
            digest: "sha256:abc"
          }
        },
        child_groups: %{
          "children" => %{
            "children" => %{"child-1" => "running"},
            "child_partitions" => %{"child-1" => "tenant-a"}
          }
        }
      })

    encoded = Flow.encode_record(record)

    assert Flow.decode_record_meta(encoded) ==
             encoded
             |> Flow.decode_record()
             |> RecordProjection.meta()
  end

  test "terminal after noop batch NIF detects records without terminal side effects" do
    parented = %{base_record() | parent_flow_id: "parent-1"}

    parent = %{
      base_record()
      | child_groups: %{
          "default" => %{
            "children" => %{"child-1" => "running"},
            "child_partitions" => %{}
          }
        }
    }

    assert Ferricstore.Bitcask.NIF.flow_records_terminal_after_noop([
             Flow.encode_record(base_record()),
             Flow.encode_record(parented),
             Flow.encode_record(parent),
             "not-a-flow-record"
           ]) == [true, false, false, false]
  end

  test "history codec keeps terminal metadata and refs" do
    record = %{base_record() | state: "completed", result_ref: "flow/value/a/result/2"}
    encoded = Flow.encode_history_fields(record, "123-1", 123, %{reason: "ok"})
    elixir_encoded = Flow.encode_history_fields_elixir(record, "123-1", 123, %{reason: "ok"})

    assert encoded == elixir_encoded

    assert Flow.decode_history_fields(encoded, record) ==
             Flow.decode_history_fields_elixir(elixir_encoded, record)

    fields =
      encoded
      |> Flow.decode_history_fields(record)
      |> Enum.chunk_every(2)
      |> Map.new(fn [key, value] -> {key, value} end)

    assert fields["event"] == "123-1"
    assert fields["version"] == "1"
    assert fields["state"] == "completed"
    assert fields["result_ref"] == "flow/value/a/result/2"
    assert fields["reason"] == "ok"
  end

  test "history codec stores per-event data without repeated immutable flow metadata" do
    record =
      base_record()
      |> Map.merge(%{
        id: "flow-compact-history",
        type: "iot-fanout-worker",
        state: "running",
        version: 12,
        attempts: 2,
        fencing_token: 9,
        created_at_ms: 1_000,
        updated_at_ms: 2_000,
        next_run_at_ms: nil,
        priority: 0,
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        root_flow_id: "flow-compact-history",
        correlation_id: "device-batch-2026-05-23",
        payload_ref: "shared-payload-ref",
        result_ref: nil,
        error_ref: nil
      })

    encoded = Flow.encode_history_fields(record, "transition", 2_000, %{})

    assert byte_size(encoded) <= 95
    refute String.contains?(encoded, "flow-compact-history")
    refute String.contains?(encoded, "iot-fanout-worker")
    refute String.contains?(encoded, "device-batch-2026-05-23")

    fields =
      encoded
      |> Flow.decode_history_fields(record)
      |> Enum.chunk_every(2)
      |> Map.new(fn [key, value] -> {key, value} end)

    assert fields["id"] == "flow-compact-history"
    assert fields["type"] == "iot-fanout-worker"
    assert fields["correlation_id"] == "device-batch-2026-05-23"
    assert fields["payload_ref"] == "shared-payload-ref"
    assert fields["state"] == "running"
  end

  test "history codec round trips fields that require multibyte length headers" do
    record = %{
      base_record()
      | id: String.duplicate("f", 160),
        type: String.duplicate("t", 190),
        correlation_id: String.duplicate("c", 180)
    }

    meta = %{reason: String.duplicate("r", 170)}
    encoded = Flow.encode_history_fields(record, "123456789-1", 123_456_789, meta)
    elixir_encoded = Flow.encode_history_fields_elixir(record, "123456789-1", 123_456_789, meta)

    assert encoded == elixir_encoded

    assert Flow.decode_history_fields(encoded, record) ==
             Flow.decode_history_fields_elixir(elixir_encoded, record)
  end

  test "value codec stores normal binaries raw and escapes magic-prefixed binaries" do
    assert Flow.encode_value("payload-bytes") == "payload-bytes"
    assert Flow.decode_value("payload-bytes") == "payload-bytes"

    old_magic_prefixed = "FSV1" <> :erlang.term_to_binary(%{old: true})
    assert Flow.encode_value(old_magic_prefixed) == old_magic_prefixed
    assert Flow.decode_value(old_magic_prefixed) == old_magic_prefixed

    magic_prefixed = "FSV2" <> <<2>> <> "not-a-term"
    encoded_magic = Flow.encode_value(magic_prefixed)

    assert encoded_magic != magic_prefixed
    assert Flow.decode_value(encoded_magic) == magic_prefixed

    map_payload = %{kind: "typed"}
    assert Flow.decode_value(Flow.encode_value(map_payload)) == map_payload
  end

  defp base_record do
    %{
      id: "flow-1",
      type: "email",
      state: "running",
      version: 1,
      attempts: 1,
      fencing_token: 7,
      created_at_ms: 100,
      updated_at_ms: 110,
      next_run_at_ms: nil,
      priority: 0,
      ttl_ms: 60_000,
      history_hot_max_events: 100,
      history_max_events: 1_000,
      retention_ttl_ms: 86_400_000,
      max_active_ms: 300_000,
      terminal_retention_until_ms: nil,
      partition_key: nil,
      payload_ref: "flow/value/a/payload/1",
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: "flow-1",
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: "worker-1",
      lease_token: "lease-1",
      lease_deadline_ms: 1_000,
      run_state: "running",
      child_groups: %{}
    }
  end
end
