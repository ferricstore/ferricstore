defmodule Ferricstore.Flow.PipelineReadCommandTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.PipelineReadCommand

  test "command parses get with default payload return" do
    assert {:get, "id", nil, %{enabled?: false, max_bytes: _}} =
             PipelineReadCommand.command(:ctx, {:get, "id", []})
  end

  test "command rejects unsupported operation" do
    assert PipelineReadCommand.command(:ctx, {:unknown, []}) ==
             {:error, "ERR unsupported flow pipeline read command"}
  end
end
