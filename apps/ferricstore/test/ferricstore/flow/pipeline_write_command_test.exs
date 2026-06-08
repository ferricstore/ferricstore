defmodule Ferricstore.Flow.PipelineWriteCommandTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineWriteCommand

  test "command parses create through callbacks" do
    callbacks = %{create_attrs: fn id, _opts -> {:ok, %{id: id, partition_key: nil}} end}

    assert {:ok, :state, {_key, {:flow_create, _state_key, %{id: "id"}}}} =
             PipelineWriteCommand.command({:create, "id", []}, callbacks)
  end

  test "command rejects unsupported op" do
    assert PipelineWriteCommand.command({:unknown, []}, %{}) ==
             {:error, "ERR unsupported flow pipeline command"}
  end
end
