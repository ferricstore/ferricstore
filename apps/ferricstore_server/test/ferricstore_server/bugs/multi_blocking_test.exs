defmodule FerricstoreServer.Bugs.MultiBlockingTest do
  @moduledoc """
  Bug #5: SUBSCRIBE/BLPOP Bypass MULTI Transaction Guard

  SUBSCRIBE, PSUBSCRIBE, BLPOP, BRPOP, BLMOVE, BLMPOP have dispatch
  clauses that match BEFORE the MULTI queuing guard. They execute
  immediately instead of being queued/rejected during a transaction.
  Redis rejects blocking commands inside MULTI.

  File: connection.ex:446-459
  """

  use ExUnit.Case, async: false
  @moduletag timeout: 30_000

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, timeout \\ 5_000) do
    recv_response_loop(sock, "", timeout)
  end

  defp recv_response_loop(sock, buf, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} ->
        buf2 = buf <> data

        case Parser.parse(buf2) do
          {:ok, [val], ""} -> val
          {:ok, [val], _rest} -> val
          {:ok, [], _} -> recv_response_loop(sock, buf2, timeout)
        end

      {:error, :timeout} ->
        {:tcp_timeout}
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp ukey(name), do: "{multi_blk}:#{name}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "BLPOP inside MULTI" do
    test "BLPOP should be rejected at queue time inside MULTI", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("blpop_multi")

      # Start a transaction
      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} = recv_response(sock)

      # BLPOP inside MULTI: Redis rejects blocking commands at queue time
      # with "ERR command not allowed inside a transaction".
      # BUG: BLPOP is silently queued (returns QUEUED), then fails during
      # EXEC because the blocking dispatch doesn't work inside a transaction.
      send_cmd(sock, ["BLPOP", k, "1"])
      response = recv_response(sock, 3_000)

      # Redis: returns error at queue time — BLPOP is never queued
      # BUG: Returns {:simple, "QUEUED"} — silently accepts a command
      # that will fail at EXEC time
      assert match?({:error, _}, response),
             "BLPOP inside MULTI should be rejected with error at queue time, " <>
               "but got: #{inspect(response)} (was silently queued)"

      # Clean up
      send_cmd(sock, ["DISCARD"])
      recv_response(sock, 3_000)
      :gen_tcp.close(sock)
    end
  end

  describe "FLOW.CLAIM_DUE inside MULTI" do
    test "FLOW.CLAIM_DUE BLOCK 0 should be rejected at queue time inside MULTI", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, [
        "FLOW.CLAIM_DUE",
        "multi-block-flow",
        "STATE",
        "queued",
        "WORKER",
        "worker-1",
        "LEASE_MS",
        "1000",
        "LIMIT",
        "1",
        "BLOCK",
        "0"
      ])

      response = recv_response(sock, 3_000)

      assert match?({:error, _}, response),
             "FLOW.CLAIM_DUE BLOCK 0 inside MULTI should be rejected with error, " <>
               "but got: #{inspect(response)}"

      send_cmd(sock, ["DISCARD"])
      recv_response(sock, 3_000)
      :gen_tcp.close(sock)
    end
  end

  describe "SUBSCRIBE inside MULTI" do
    test "SUBSCRIBE should be rejected inside MULTI", %{port: port} do
      sock = connect_and_hello(port)

      # Start a transaction
      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} = recv_response(sock)

      # SUBSCRIBE inside MULTI should return an error
      # BUG: SUBSCRIBE dispatch clause matches before MULTI queuing guard
      send_cmd(sock, ["SUBSCRIBE", "test_channel"])
      response = recv_response(sock, 3_000)

      # Redis rejects SUBSCRIBE inside MULTI:
      # "ERR Command not allowed inside a transaction"
      assert match?({:error, _}, response),
             "SUBSCRIBE inside MULTI should be rejected with error, " <>
               "but got: #{inspect(response)} (executed immediately)"

      # Try to discard — may fail if SUBSCRIBE switched to pubsub mode
      send_cmd(sock, ["UNSUBSCRIBE", "test_channel"])
      recv_response(sock, 3_000)
      :gen_tcp.close(sock)
    end
  end

  describe "BRPOP inside MULTI" do
    test "BRPOP should be rejected at queue time inside MULTI", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("brpop_multi")

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, ["BRPOP", k, "1"])
      response = recv_response(sock, 3_000)

      # Redis: returns error at queue time — BRPOP is never queued
      # BUG: Returns {:simple, "QUEUED"} — silently accepts a command
      # that will fail at EXEC time
      assert match?({:error, _}, response),
             "BRPOP inside MULTI should be rejected with error at queue time, " <>
               "but got: #{inspect(response)} (was silently queued)"

      send_cmd(sock, ["DISCARD"])
      recv_response(sock, 3_000)
      :gen_tcp.close(sock)
    end
  end

  describe "stream blocking reads inside MULTI" do
    test "XREAD BLOCK should be rejected at queue time inside MULTI", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("xread_multi")

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, ["XREAD", "BLOCK", "1000", "STREAMS", k, "$"])
      response = recv_response(sock, 3_000)

      assert match?({:error, _}, response),
             "XREAD BLOCK inside MULTI should be rejected with error at queue time, " <>
               "but got: #{inspect(response)}"

      send_cmd(sock, ["DISCARD"])
      recv_response(sock, 3_000)
      :gen_tcp.close(sock)
    end

    test "XREADGROUP BLOCK should be rejected at queue time inside MULTI", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("xreadgroup_multi")

      send_cmd(sock, ["XGROUP", "CREATE", k, "g", "$", "MKSTREAM"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, ["MULTI"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, [
        "XREADGROUP",
        "GROUP",
        "g",
        "c1",
        "BLOCK",
        "1000",
        "STREAMS",
        k,
        ">"
      ])

      response = recv_response(sock, 3_000)

      assert match?({:error, _}, response),
             "XREADGROUP BLOCK inside MULTI should be rejected with error at queue time, " <>
               "but got: #{inspect(response)}"

      send_cmd(sock, ["DISCARD"])
      recv_response(sock, 3_000)
      :gen_tcp.close(sock)
    end
  end
end
