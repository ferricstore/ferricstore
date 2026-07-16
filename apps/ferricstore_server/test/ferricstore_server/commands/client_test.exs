defmodule FerricstoreServer.Commands.ClientTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Commands.Client

  test "unsupported client controls never report success" do
    state = %{
      client_id: 1,
      client_name: nil,
      created_at: System.monotonic_time(:millisecond),
      peer: nil
    }

    for {subcommand, args} <- [
          {"PAUSE", ["1000"]},
          {"UNPAUSE", []},
          {"NO-EVICT", ["ON"]},
          {"NO-TOUCH", ["ON"]}
        ] do
      assert {{:error, reason}, ^state} = Client.handle(subcommand, args, state, %{})
      assert reason =~ "not supported by the native protocol"
    end
  end
end
