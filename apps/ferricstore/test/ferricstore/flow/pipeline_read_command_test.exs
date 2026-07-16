defmodule Ferricstore.Flow.PipelineReadCommandTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineReadCommand

  test "command parses get with default payload return" do
    assert {:get, "id", nil, %{enabled?: false, max_bytes: _}} =
             PipelineReadCommand.command(:ctx, {:get, "id", []})
  end

  test "command rejects unsupported operation" do
    assert PipelineReadCommand.command(:ctx, {:unknown, []}) ==
             {:error, "ERR unsupported flow pipeline read command"}
  end

  test "command rejects payload hydration above the configured ceiling" do
    assert {:error, "ERR flow payload_max_bytes exceeds maximum 65536"} =
             PipelineReadCommand.command(
               :ctx,
               {:get, "id", [payload_max_bytes: 65_537]}
             )
  end

  test "decode_get fails closed for corrupt stored records" do
    assert PipelineReadCommand.decode_get("not-a-flow-record") ==
             {:error, "ERR flow record is corrupt"}

    assert PipelineReadCommand.decode_get(:invalid_backend_reply) ==
             {:error, "ERR flow record is corrupt"}

    assert PipelineReadCommand.decode_get(nil) == {:ok, nil}
    assert PipelineReadCommand.decode_get({:error, "ERR backend"}) == {:error, "ERR backend"}
  end

  test "history command uses the direct history preparation contract" do
    assert {:history, "flow-1", nil, _history_key, query, true, true,
            %{enabled?: false, max_bytes: _}} =
             PipelineReadCommand.command(:ctx, {:history, "flow-1", []})

    assert query.count == 10_000
  end

  test "history decode contexts distinguish missing state from corrupt state" do
    assert PipelineReadCommand.decode_context_value(nil, "flow-1") == %{id: "flow-1"}

    assert PipelineReadCommand.decode_context_value("not-a-flow-record", "flow-1") ==
             {:error, "ERR flow record is corrupt"}

    assert PipelineReadCommand.decode_context_value(:invalid_backend_reply, "flow-1") ==
             {:error, "ERR flow record is corrupt"}
  end
end
