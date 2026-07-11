defmodule FerricstoreServer.Native.SessionCurrentContractTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.Session

  test "native sessions expose only the prepared-command parser" do
    assert Code.ensure_loaded?(Session)
    assert function_exported?(Session, :prepare_command, 1)
    refute function_exported?(Session, :parse_command, 1)
  end
end
