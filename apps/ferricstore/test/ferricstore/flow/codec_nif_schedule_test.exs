defmodule Ferricstore.Flow.CodecNifScheduleTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.NifSchedule

  @flow_index_src Path.expand("../../../native/ferricstore_bitcask/src/flow_index.rs", __DIR__)

  # Every Flow command encodes or decodes several records; a dirty-scheduler
  # round trip costs more than the work on a record of typical size.
  @codec_nifs ~w(
    flow_record_encode
    flow_record_decode
    flow_records_decode
    flow_record_decode_meta
    flow_records_terminal_after_noop
    flow_history_encode
    flow_record_plan_claims
    flow_record_plan_claims_with_history
  )

  @tag :guard
  test "Flow codec NIFs run on normal schedulers with a dirty CPU twin for large inputs" do
    source = File.read!(@flow_index_src)

    for function <- @codec_nifs do
      assert source =~ ~r/#\[rustler::nif\(schedule = "Normal"\)\]\s+pub fn #{function}\b/,
             "expected #{function} to use the Normal scheduler"

      assert source =~ ~r/#\[rustler::nif\(schedule = "DirtyCpu"\)\]\s+pub fn #{function}_dirty\b/,
             "expected #{function}_dirty to use the DirtyCpu scheduler"
    end
  end

  test "inputs past the measured bounds go to the dirty twin" do
    assert NifSchedule.normal_record?(1_048_576)
    refute NifSchedule.normal_record?(1_048_577)

    assert NifSchedule.normal_record_batch?(256)
    refute NifSchedule.normal_record_batch?(257)

    assert NifSchedule.normal_claim_plan?(512, false)
    refute NifSchedule.normal_claim_plan?(513, false)
    assert NifSchedule.normal_claim_plan?(256, true)
    refute NifSchedule.normal_claim_plan?(257, true)

    assert NifSchedule.normal_terminal_check?(2_048)
    refute NifSchedule.normal_terminal_check?(2_049)
  end

  test "a record past the normal bound encodes and decodes on the dirty twin" do
    record = record(String.duplicate("f", 1_100_000))
    encoded = Codec.encode_record(record)
    refute NifSchedule.normal_record?(byte_size(encoded))

    decoded = Codec.decode_record(encoded)
    assert decoded.id == record.id
    assert decoded.state == "queued"
    assert Codec.decode_record_meta(encoded).id == record.id
    assert [%{id: id}] = Codec.decode_records([encoded])
    assert id == record.id
  end

  test "a batch past the normal bound decodes on the dirty twin like a small one" do
    values = for i <- 1..300, do: Codec.encode_record(record("flow-#{i}"))

    refute NifSchedule.normal_record_batch?(length(values))

    assert Enum.map(Codec.decode_records(values), & &1.id) ==
             Enum.map(1..300, &"flow-#{&1}")
  end

  defp record(id) do
    %{
      id: id,
      type: "email",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 10,
      updated_at_ms: 11,
      next_run_at_ms: 20,
      priority: 0,
      partition_key: "tenant-a",
      payload_ref: "payload-1",
      run_state: "queued"
    }
  end
end
