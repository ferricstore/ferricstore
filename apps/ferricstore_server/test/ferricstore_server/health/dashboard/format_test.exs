defmodule FerricstoreServer.Health.Dashboard.FormatTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Format

  test "escape renders binary-safe identifiers without emitting invalid UTF-8" do
    escaped = Format.escape(<<255, ?<, ?&>>)

    assert String.valid?(escaped)
    assert escaped == "&lt;&lt;255, 60, 38&gt;&gt;"
  end
end
