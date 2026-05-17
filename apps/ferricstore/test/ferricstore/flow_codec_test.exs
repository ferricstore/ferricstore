defmodule Ferricstore.FlowCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow

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

  test "history codec keeps terminal metadata and refs" do
    record = %{base_record() | state: "completed", result_ref: "flow/value/a/result/2"}
    encoded = Flow.encode_history_fields(record, "123-1", 123, %{reason: "ok"})
    elixir_encoded = Flow.encode_history_fields_elixir(record, "123-1", 123, %{reason: "ok"})

    assert encoded == elixir_encoded
    assert Flow.decode_history_fields(encoded) == Flow.decode_history_fields_elixir(elixir_encoded)

    fields =
      encoded
      |> Flow.decode_history_fields()
      |> Enum.chunk_every(2)
      |> Map.new(fn [key, value] -> {key, value} end)

    assert fields["event"] == "123-1"
    assert fields["version"] == "1"
    assert fields["state"] == "completed"
    assert fields["result_ref"] == "flow/value/a/result/2"
    assert fields["reason"] == "ok"
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
    assert Flow.decode_history_fields(encoded) == Flow.decode_history_fields_elixir(elixir_encoded)
  end

  test "value codec stores normal binaries raw and escapes magic-prefixed binaries" do
    assert Flow.encode_value("payload-bytes") == "payload-bytes"
    assert Flow.decode_value("payload-bytes") == "payload-bytes"

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
