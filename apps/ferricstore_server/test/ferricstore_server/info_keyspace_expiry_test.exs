defmodule FerricstoreServer.InfoKeyspaceExpiryTest do
  @moduledoc """
  Integration tests for INFO keyspace expires count and avg_ttl.

  Verifies that the keyspace section reports accurate counts of keys with
  TTL set, and a reasonable avg_ttl value, rather than hardcoded zeros.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp parse_keyspace_line(info_str) do
    # Parse "db0:keys=N,expires=M,avg_ttl=T" from INFO keyspace output
    case Regex.run(~r/keys=(\d+),expires=(\d+),avg_ttl=(\d+)/, info_str) do
      [_, keys, expires, avg_ttl] ->
        %{
          keys: String.to_integer(keys),
          expires: String.to_integer(expires),
          avg_ttl: String.to_integer(avg_ttl)
        }

      nil ->
        nil
    end
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "INFO keyspace expires count" do
    test "no keys → expires=0", %{port: port} do
      sock = connect_and_hello(port)
      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.keys == 0
      assert parsed.expires == 0
    end

    test "keys without TTL → expires=0", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "no_ttl_1", "value1"])
      recv_response(sock)
      send_cmd(sock, ["SET", "no_ttl_2", "value2"])
      recv_response(sock)

      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.keys >= 2
      assert parsed.expires == 0
    end

    test "keys with TTL → expires reflects count", %{port: port} do
      sock = connect_and_hello(port)

      # SET with EX (seconds)
      send_cmd(sock, ["SET", "ttl_key_1", "value1", "EX", "300"])
      recv_response(sock)
      send_cmd(sock, ["SET", "ttl_key_2", "value2", "PX", "300000"])
      recv_response(sock)
      # Key without TTL
      send_cmd(sock, ["SET", "no_ttl", "value3"])
      recv_response(sock)

      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.keys >= 3
      assert parsed.expires == 2
    end

    test "DEL a key with TTL → expires decrements", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "del_ttl_1", "val", "EX", "300"])
      recv_response(sock)
      send_cmd(sock, ["SET", "del_ttl_2", "val", "EX", "300"])
      recv_response(sock)

      # Delete one
      send_cmd(sock, ["DEL", "del_ttl_1"])
      recv_response(sock)

      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.expires == 1
    end

    test "PERSIST removes TTL → expires decrements", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "persist_key", "val", "EX", "300"])
      recv_response(sock)

      # Verify expires=1
      send_cmd(sock, ["INFO", "keyspace"])
      result1 = recv_response(sock)
      assert parse_keyspace_line(result1).expires == 1

      # PERSIST removes TTL
      send_cmd(sock, ["PERSIST", "persist_key"])
      recv_response(sock)

      send_cmd(sock, ["INFO", "keyspace"])
      result2 = recv_response(sock)
      :gen_tcp.close(sock)

      assert parse_keyspace_line(result2).expires == 0
    end

    test "SET overwrite with TTL → no double count", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "overwrite_key", "val1", "EX", "300"])
      recv_response(sock)
      # Overwrite same key with new TTL
      send_cmd(sock, ["SET", "overwrite_key", "val2", "EX", "600"])
      recv_response(sock)

      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.expires == 1
    end

    test "SET overwrite TTL key without TTL → expires decrements", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "was_ttl", "val1", "EX", "300"])
      recv_response(sock)

      # Overwrite without TTL
      send_cmd(sock, ["SET", "was_ttl", "val2"])
      recv_response(sock)

      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.expires == 0
    end
  end

  describe "INFO keyspace avg_ttl" do
    test "keys with TTL → avg_ttl > 0", %{port: port} do
      sock = connect_and_hello(port)

      # Set keys with 5 minute TTL
      send_cmd(sock, ["SET", "avg_ttl_1", "val", "EX", "300"])
      recv_response(sock)
      send_cmd(sock, ["SET", "avg_ttl_2", "val", "EX", "300"])
      recv_response(sock)

      ShardHelpers.eventually(fn ->
        send_cmd(sock, ["INFO", "keyspace"])
        result = recv_response(sock)
        parsed = parse_keyspace_line(result)
        parsed != nil and parsed.avg_ttl > 0
      end, "avg_ttl should be > 0 after setting keys with TTL", 10, 100)

      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.avg_ttl > 0
      # avg_ttl is in milliseconds, should be close to 300_000
      assert parsed.avg_ttl > 200_000
      assert parsed.avg_ttl <= 300_000
    end

    test "no TTL keys → avg_ttl=0", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "plain_key", "val"])
      recv_response(sock)

      send_cmd(sock, ["INFO", "keyspace"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      parsed = parse_keyspace_line(result)
      assert parsed.avg_ttl == 0
    end
  end
end
