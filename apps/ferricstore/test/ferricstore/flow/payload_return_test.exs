defmodule Ferricstore.Flow.PayloadReturnTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.PayloadReturn

  test "uses the configured payload return size as a hard ceiling" do
    previous = Application.get_env(:ferricstore, :flow_payload_return_max_bytes)
    Application.put_env(:ferricstore, :flow_payload_return_max_bytes, 8)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:ferricstore, :flow_payload_return_max_bytes)
      else
        Application.put_env(:ferricstore, :flow_payload_return_max_bytes, previous)
      end
    end)

    assert {:ok, %{max_bytes: 8}} = PayloadReturn.options([], true)
    assert {:ok, %{max_bytes: 4}} = PayloadReturn.history_options(payload_max_bytes: 4)

    assert {:error, "ERR flow payload_max_bytes exceeds maximum 8"} =
             PayloadReturn.options([payload_max_bytes: 9], true)
  end
end
