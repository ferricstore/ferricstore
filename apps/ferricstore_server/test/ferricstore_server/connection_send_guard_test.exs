defmodule FerricstoreServer.ConnectionSendGuardTest do
  use ExUnit.Case, async: true

  @lib_root Path.expand("../../lib/ferricstore_server", __DIR__)
  @allowed Path.expand("../../lib/ferricstore_server/connection/send.ex", __DIR__)

  test "production code sends socket data through the telemetry wrapper" do
    offenders =
      @lib_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.reject(&(&1 == @allowed))
      |> Enum.flat_map(fn path ->
        path
        |> File.read!()
        |> String.split("\n")
        |> Enum.with_index(1)
        |> Enum.flat_map(fn {line, line_no} ->
          if String.contains?(line, "transport.send(") do
            ["#{Path.relative_to_cwd(path)}:#{line_no}: #{String.trim(line)}"]
          else
            []
          end
        end)
      end)

    assert offenders == [],
           "raw transport.send/2 bypasses send-failure telemetry:\n" <>
             Enum.join(offenders, "\n")
  end
end
