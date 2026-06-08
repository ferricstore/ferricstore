defmodule FerricstoreServer.Spec.InputValidationTest do
  @moduledoc """
  Tests that prove input validation hardening works.

  Each test sends a malicious or edge-case input via TCP and verifies
  the server rejects it with an appropriate error — not a crash, OOM,
  or silent corruption.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    Ferricstore.MemoryGuard.reset_pressure_flags()
    ShardHelpers.wait_shards_alive(30_000)
    %{port: Listener.port()}
  end

  setup %{port: port} do
    sock = connect_and_hello(port)
    on_exit(fn -> :gen_tcp.close(sock) end)
    %{sock: sock, port: port}
  end

  defp connect_and_hello(port, retries \\ 5) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])

    case recv_response(sock) do
      {:error, _} when retries > 0 ->
        :gen_tcp.close(sock)
        Process.sleep(200)
        connect_and_hello(port, retries - 1)

      _ ->
        sock
    end
  end

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp send_raw(sock, data), do: :ok = :gen_tcp.send(sock, data)

  defp recv_response(sock), do: recv_response(sock, "")

  defp recv_response(sock, buf) do
    case :gen_tcp.recv(sock, 0, 10_000) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], ""} -> val
          {:ok, [val], _rest} -> val
          {:ok, [], _} -> recv_response(sock, buf2)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Normalize OK responses (RESP3 returns {:simple, "OK"})
  defp assert_ok(resp), do: assert(resp in ["OK", {:simple, "OK"}])

  # Extract error message from various error formats
  defp assert_error_contains(resp, substring) do
    msg =
      case resp do
        {:error, m} when is_binary(m) -> m
        m when is_binary(m) -> m
        other -> inspect(other)
      end

    assert String.contains?(msg, substring),
           "Expected error containing #{inspect(substring)}, got: #{inspect(resp)}"
  end

  defp ukey(base), do: "iv_#{base}_#{:rand.uniform(9_999_999)}"

  # ===========================================================================
  # INCR/DECRBY bounds validation
  # ===========================================================================

  describe "INCR/INCRBY bounds" do
    test "INCRBY with value exceeding i64 max returns error", %{sock: sock} do
      k = ukey("incr_overflow")
      send_cmd(sock, ["SET", k, "0"])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["INCRBY", k, "9223372036854775808"])
      assert_error_contains(recv_response(sock), "not an integer or out of range")
    end

    test "DECRBY with huge value returns error", %{sock: sock} do
      k = ukey("decr_overflow")
      send_cmd(sock, ["SET", k, "0"])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["DECRBY", k, "99999999999999999999999"])
      assert_error_contains(recv_response(sock), "not an integer or out of range")
    end

    test "INCRBY with valid i64 max works", %{sock: sock} do
      k = ukey("incr_valid_max")
      send_cmd(sock, ["SET", k, "0"])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["INCRBY", k, "9223372036854775807"])
      assert recv_response(sock) == 9_223_372_036_854_775_807
    end
  end

  # ===========================================================================
  # SETRANGE offset cap
  # ===========================================================================

  describe "SETRANGE offset" do
    test "SETRANGE with offset > 512MB returns error", %{sock: sock} do
      k = ukey("setrange_huge")
      send_cmd(sock, ["SET", k, "hello"])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["SETRANGE", k, "536870912", "x"])
      assert_error_contains(recv_response(sock), "maximum allowed size")
    end

    test "SETRANGE with valid offset works", %{sock: sock} do
      k = ukey("setrange_ok")
      send_cmd(sock, ["SET", k, "hello"])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["SETRANGE", k, "5", " world"])
      assert recv_response(sock) == 11
    end

    test "SETRANGE with negative offset returns error", %{sock: sock} do
      k = ukey("setrange_neg")
      send_cmd(sock, ["SETRANGE", k, "-1", "x"])
      assert_error_contains(recv_response(sock), "out of range")
    end
  end

  # ===========================================================================
  # Empty string value (not tombstone)
  # ===========================================================================

  describe "empty string value" do
    test "SET empty string, GET returns empty string not nil", %{sock: sock} do
      k = ukey("empty_val")
      send_cmd(sock, ["SET", k, ""])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["GET", k])
      resp = recv_response(sock)
      assert resp == "", "expected empty string, got: #{inspect(resp)}"
    end

    test "SET empty string, EXISTS returns 1", %{sock: sock} do
      k = ukey("empty_exists")
      send_cmd(sock, ["SET", k, ""])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["EXISTS", k])
      assert recv_response(sock) == 1
    end
  end

  # ===========================================================================
  # RESP protocol attacks
  # ===========================================================================

  describe "RESP protocol hardening" do
    test "huge array count is rejected (not OOM)", %{sock: sock} do
      send_raw(sock, "*999999999\r\n")

      case :gen_tcp.recv(sock, 0, 5_000) do
        {:ok, data} ->
          assert String.contains?(data, "array too large") or
                   String.contains?(data, "ERR")

        {:error, :closed} ->
          :ok
      end
    end

    test "server stays alive after huge array rejected", %{port: port} do
      {:ok, bad_sock} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      send_raw(bad_sock, "*999999999\r\n")
      :gen_tcp.recv(bad_sock, 0, 2_000)
      :gen_tcp.close(bad_sock)

      good_sock = connect_and_hello(port)
      k = ukey("after_attack")
      send_cmd(good_sock, ["SET", k, "alive"])
      assert_ok(recv_response(good_sock))
      send_cmd(good_sock, ["GET", k])
      assert recv_response(good_sock) == "alive"
      :gen_tcp.close(good_sock)
    end
  end

  # ===========================================================================
  # Null bytes in keys and values
  # ===========================================================================

  describe "binary safety" do
    test "key with null bytes works", %{sock: sock} do
      k = "iv_null\x00key_#{:rand.uniform(999_999)}"
      send_cmd(sock, ["SET", k, "null_val"])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "null_val"

      send_cmd(sock, ["DEL", k])
      assert recv_response(sock) == 1
    end

    test "value with null bytes roundtrips", %{sock: sock} do
      k = ukey("null_val")
      val = "before\x00after\x00end"
      send_cmd(sock, ["SET", k, val])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == val
    end
  end

  # ===========================================================================
  # Unknown and malformed commands
  # ===========================================================================

  describe "unknown commands" do
    test "unknown command returns error, doesn't crash", %{sock: sock} do
      send_cmd(sock, ["FAKECMD", "arg1"])
      assert_error_contains(recv_response(sock), "unknown command")
    end

    test "EVAL is not implemented", %{sock: sock} do
      send_cmd(sock, ["EVAL", "return 1", "0"])
      assert_error_contains(recv_response(sock), "unknown command")
    end

    test "server works after unknown command", %{sock: sock} do
      send_cmd(sock, ["TOTALLYNOTACOMMAND"])
      _err = recv_response(sock)

      k = ukey("after_unknown")
      send_cmd(sock, ["SET", k, "still_works"])
      assert_ok(recv_response(sock))
    end
  end

  # ===========================================================================
  # MULTI edge cases
  # ===========================================================================

  describe "MULTI edge cases" do
    test "MULTI inside MULTI returns error", %{sock: sock} do
      send_cmd(sock, ["MULTI"])
      assert_ok(recv_response(sock))

      send_cmd(sock, ["MULTI"])
      assert_error_contains(recv_response(sock), "nested")

      send_cmd(sock, ["DISCARD"])
      assert_ok(recv_response(sock))
    end
  end
end
