defmodule Ferricstore.Flow.CodecHistorySnapshotTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Codec

  @now 1_791_000_000_000

  defp record(overrides) do
    Map.merge(
      %{
        id: "order-1",
        type: "invoice",
        state: "queued",
        version: 1,
        priority: 0,
        attempts: 0,
        fencing_token: 0,
        created_at_ms: @now,
        updated_at_ms: @now,
        next_run_at_ms: @now,
        lease_deadline_ms: 0,
        lease_owner: nil,
        payload_ref: nil,
        parent_flow_id: nil,
        root_flow_id: "order-1",
        correlation_id: nil,
        result_ref: nil,
        error_ref: nil,
        partition_key: "tenant-a"
      },
      overrides
    )
  end

  defp cases do
    [
      {record(%{}), "created", @now, %{}},
      {record(%{payload_ref: "f:{f:AAAAAAAAAAAAAAAAAAAAAA}:v:p:order-1:1", priority: 7}),
       "created", @now, %{}},
      {record(%{
         state: "running",
         version: 3,
         attempts: 2,
         fencing_token: 2,
         lease_owner: "worker-1",
         lease_deadline_ms: @now + 30_000,
         updated_at_ms: @now + 5,
         next_run_at_ms: @now + 30_000,
         correlation_id: "corr-9",
         parent_flow_id: "parent-1",
         root_flow_id: "root-1"
       }), "claimed", @now + 5, %{worker: "worker-1", lease_ms: 30_000}},
      {record(%{
         state: "completed",
         version: 4,
         result_ref: "f:{f:AAAAAAAAAAAAAAAAAAAAAA}:v:r:order-1:4",
         error_ref: "f:{f:AAAAAAAAAAAAAAAAAAAAAA}:v:e:order-1:3",
         value_refs: %{
           "doc" => %{ref: "f:{f:AAAAAAAAAAAAAAAAAAAAAA}:svn:order-1:doc:2", version: 2}
         },
         attributes: %{"region" => "eu", "tier" => "gold"},
         rewound_to_event_id: "1791000000000-2"
       }), "completed", @now + 9, %{"signal" => "go", "note" => ""}}
    ]
  end

  test "a decoded history snapshot re-encodes to the same bytes" do
    for {record, event, now_ms, meta} <- cases() do
      encoded = Codec.encode_history_fields(record, event, now_ms, meta)
      assert {:ok, snapshot} = Codec.decode_history_snapshot(encoded)
      assert Codec.encode_history_snapshot(snapshot) == encoded
    end
  end

  test "the snapshot carries the stored value refs" do
    {record, event, now_ms, meta} = List.last(cases())
    encoded = Codec.encode_history_fields(record, event, now_ms, meta)
    assert {:ok, snapshot} = Codec.decode_history_snapshot(encoded)

    assert elem(snapshot, 18) == record.result_ref
    assert elem(snapshot, 19) == record.error_ref
    assert {"value_refs", _json} = List.keyfind(elem(snapshot, 21), "value_refs", 0)
  end

  test "malformed history values are rejected" do
    assert :error = Codec.decode_history_snapshot("not history")
    encoded = Codec.encode_history_fields(record(%{}), "created", @now, %{})
    assert :error = Codec.decode_history_snapshot(binary_part(encoded, 0, byte_size(encoded) - 1))
  end
end
