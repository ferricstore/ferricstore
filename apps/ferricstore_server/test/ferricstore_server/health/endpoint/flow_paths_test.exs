defmodule FerricstoreServer.Health.Endpoint.FlowPathsTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias FerricstoreServer.Health.Endpoint.FlowPaths

  test "decode_form_body parses query-encoded form fields" do
    assert FlowPaths.decode_form_body("id=flow-1&partition_key=tenant-a") == %{
             "id" => "flow-1",
             "partition_key" => "tenant-a"
           }
  end

  test "decode_form_body tolerates lone percent encoding" do
    assert FlowPaths.decode_form_body("%") == %{"%" => ""}
  end

  test "decode_flow_detail_request decodes id and dashboard options" do
    assert FlowPaths.decode_flow_detail_request("flow%2F1?partition_key=tenant-a&payload=true") ==
             {"flow/1", [partition_key: "tenant-a", history_count: 50]}
  end

  test "decode_flow_rewind_action accepts only rewind suffix" do
    assert FlowPaths.decode_flow_rewind_action("flow%2F1/rewind") == {:ok, "flow/1"}
    assert FlowPaths.decode_flow_rewind_action("flow%2F1/cancel") == :not_found
  end

  test "flow_detail_location encodes id and optional partition key" do
    assert FlowPaths.flow_detail_location("flow/1", "") == "/dashboard/flow/flow%2F1"

    assert FlowPaths.flow_detail_location("flow/1", "tenant-a") ==
             "/dashboard/flow/flow%2F1?partition_key=tenant-a"
  end

  test "flow_detail_location filters blank params and includes partition key" do
    assert FlowPaths.flow_detail_location("flow/1", "tenant-a", %{
             "status" => "rewound",
             "empty" => "",
             "nil" => nil
           }) ==
             "/dashboard/flow/flow%2F1?partition_key=tenant-a&status=rewound"
  end
end
