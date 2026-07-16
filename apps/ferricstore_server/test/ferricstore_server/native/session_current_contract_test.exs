defmodule FerricstoreServer.Native.SessionCurrentContractTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.Session

  @tag :shared_command_spec
  test "native sessions expose only the prepared-command parser" do
    assert Code.ensure_loaded?(Session)
    assert function_exported?(Session, :prepare_command, 1)
    refute function_exported?(Session, :parse_command, 1)
    refute function_exported?(Session, :authorize_command, 5)
  end

  test "prepared-command parsing rejects structured argument values without raising" do
    for unsupported <- [%{}, [], ["nested"]] do
      assert {:error, "ERR native field args contains an unsupported value"} =
               Session.prepare_command(%{"command" => "MULTI", "args" => [unsupported]})
    end
  end
end
