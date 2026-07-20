defmodule Ferricstore.Flow.PipelineReadCommandTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.PipelineReadCommand

  setup do
    {:ok, snapshot} =
      MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    {:ok, ctx: %{flow_metadata_snapshot: snapshot}}
  end

  test "command parses get with default payload return and sealed scope", %{ctx: ctx} do
    assert {:get, "id", partition_key, %{enabled?: false, max_bytes: _}, %{}} =
             PipelineReadCommand.command(ctx, {:get, "id", []})

    assert String.starts_with?(partition_key, "__flow_auto__:")
  end

  test "command preserves metadata-only return shape", %{ctx: ctx} do
    assert {:get, "id", _partition_key, %{enabled?: false, max_bytes: _, record_return: :meta},
            %{}} =
             PipelineReadCommand.command(ctx, {:get, "id", [return: :meta]})
  end

  test "scope-sensitive commands fail closed for a malformed context" do
    assert PipelineReadCommand.command(:ctx, {:get, "id", []}) ==
             {:error, "ERR Flow metadata extension is unavailable"}
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

  test "history command uses the direct history preparation contract", %{ctx: ctx} do
    assert {:history, "flow-1", partition_key, _history_key, query, true, true,
            %{enabled?: false, max_bytes: _}, %{}} =
             PipelineReadCommand.command(ctx, {:history, "flow-1", []})

    assert String.starts_with?(partition_key, "__flow_auto__:")
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
