Code.require_file("advanced_types_tcp_test/sections/geoadd_over_tcp.exs", __DIR__)
Code.require_file("advanced_types_tcp_test/sections/json_type_over_tcp.exs", __DIR__)

defmodule FerricstoreServer.Integration.AdvancedTypesTcpTest do
  @moduledoc """
  End-to-end TCP integration tests for GEO, HYPERLOGLOG, STREAM, and JSON commands.

  Uses the same test infrastructure as CommandsTcpTest: real TCP socket,
  RESP3-encoded commands, full stack verification.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  use FerricstoreServer.Integration.AdvancedTypesTcpTest.Sections.GeoaddOverTcp

  use FerricstoreServer.Integration.AdvancedTypesTcpTest.Sections.JsonTypeOverTcp

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp send_pipeline(sock, commands) do
    data = commands |> Enum.map(&Encoder.encode/1) |> IO.iodata_to_binary()
    :ok = :gen_tcp.send(sock, data)
  end

  defp send_inline(sock, parts) do
    :ok = :gen_tcp.send(sock, Enum.join(parts, " ") <> "\r\n")
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp recv_n(sock, n) do
    do_recv_n(sock, n, "", [])
  end

  defp do_recv_n(_sock, 0, _buf, acc), do: acc

  defp do_recv_n(sock, remaining, buf, acc) when remaining > 0 do
    {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [_ | _] = vals, rest} ->
        taken = Enum.take(vals, remaining)
        new_acc = acc ++ taken
        new_remaining = remaining - length(taken)
        do_recv_n(sock, new_remaining, rest, new_acc)

      {:ok, [], _} ->
        do_recv_n(sock, remaining, buf2, acc)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(name), do: "#{name}_#{:rand.uniform(999_999)}"

  defp enable_sandbox_mode do
    previous_sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")
    :ets.insert(:ferricstore_config, {"sandbox_mode", "enabled"})

    on_exit(fn ->
      case previous_sandbox_mode do
        nil -> :ets.delete(:ferricstore_config, "sandbox_mode")
        value -> :ets.insert(:ferricstore_config, {"sandbox_mode", value})
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Setup — single listener for all tests
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup %{port: port} do
    ShardHelpers.flush_all_keys()

    sock = connect_and_hello(port)
    send_cmd(sock, ["FLUSHDB"])
    recv_response(sock)
    :gen_tcp.close(sock)
    :ok
  end

  # ===========================================================================
  # GEO commands
  # ===========================================================================

  # ===========================================================================
  # HYPERLOGLOG commands
  # ===========================================================================

  # ===========================================================================
  # STREAM commands
  # ===========================================================================

  # ===========================================================================
  # JSON commands
  # ===========================================================================
end
