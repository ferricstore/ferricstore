Code.require_file("edge_cases_test/sections/part_01.exs", __DIR__)
Code.require_file("edge_cases_test/sections/part_02.exs", __DIR__)
defmodule FerricstoreServer.Integration.EdgeCasesTest do
  @moduledoc """
  Edge case and stress tests covering value size limits, key size limits,
  boundary conditions, TTL precision, binary safety, and protocol robustness.

  Organised by failure domain so regressions are easy to locate.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  @moduletag timeout: 60_000

  # max key length enforced by the on-disk u16 key_size field AND the Elixir guard
  @max_key_bytes 65_535
  # max value length enforced by the Rust NIF guard (512 MiB)
  @max_value_bytes 512 * 1024 * 1024

  setup_all do
    # Give any previously-killed shards time to restart before this module runs.
    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    Enum.each(0..(shard_count - 1), fn i ->
      name = Router.shard_name(FerricStore.Instance.get(:default), i)

      Enum.find_value(1..50, fn _ ->
        pid = Process.whereis(name)
        if is_pid(pid) and Process.alive?(pid), do: true, else: Process.sleep(100)
      end)
    end)

    :ok
  end

  use FerricstoreServer.Integration.EdgeCasesTest.Sections.Part01

  use FerricstoreServer.Integration.EdgeCasesTest.Sections.Part02

  defp ukey(base), do: "ec_#{base}_#{:rand.uniform(9_999_999)}"

  defp connect do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", Listener.port(), [
        :binary,
        active: false,
        packet: :raw,
        recbuf: 4 * 1024 * 1024,
        sndbuf: 4 * 1024 * 1024
      ])

    sock
  end

  # Send a RESP array command over `sock` and return the parsed response.
  # Uses a generous timeout for large-value round-trips.
  defp cmd(sock, args, timeout \\ 30_000) do
    :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
    recv_one(sock, timeout)
  end

  defp recv_one(sock, timeout) do
    recv_loop(sock, "", timeout)
  end

  defp recv_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _} ->
        val

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_loop(sock, buf <> data, timeout)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  # Receive exactly `count` RESP responses from `sock`.
  # Accumulates TCP chunks until `count` complete responses have been parsed.
  # Far faster than calling recv_one/2 in a loop for large pipelines.
  defp recv_n(sock, count, timeout \\ 30_000) do
    recv_n_loop(sock, count, "", timeout, [])
  end

  defp recv_n_loop(_sock, 0, _buf, _timeout, acc), do: Enum.reverse(acc)

  defp recv_n_loop(sock, remaining, buf, timeout, acc) do
    case Parser.parse(buf) do
      {:ok, vals, rest} when vals != [] ->
        take = min(length(vals), remaining)
        new_acc = Enum.reverse(Enum.take(vals, take)) ++ acc
        new_remaining = remaining - take

        if new_remaining == 0 do
          Enum.reverse(new_acc)
        else
          recv_n_loop(sock, new_remaining, rest, timeout, new_acc)
        end

      {:ok, [], _} ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> recv_n_loop(sock, remaining, buf <> data, timeout, acc)
          {:error, reason} -> {:tcp_error, reason}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # 1. Value size boundaries
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # 4. TTL edge cases
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # 5. Duplicate keys and update semantics
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # 6. Large value TCP round-trips
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # 7. Protocol stress
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # 8. Concurrent write stress
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # 10. Protocol-level size guards (Elixir dispatcher layer)
  # ---------------------------------------------------------------------------


  # ===========================================================================
  # 12. Protocol edge cases (TCP layer)
  # ===========================================================================

  # Helper: open a raw TCP connection with HELLO 3 handshake.
  defp connect_and_hello do
    sock = connect()
    assert {:simple, "OK"} = cmd(sock, ["HELLO", "3"]) |> normalize_hello()
    sock
  end

  # HELLO 3 returns a map (the greeting), not {:simple, "OK"}.
  # Normalize it so callers just need a truthy check.
  defp normalize_hello(resp) when is_map(resp), do: {:simple, "OK"}
  defp normalize_hello(resp), do: resp

  # Send raw bytes on a socket without RESP encoding.
  defp send_raw(sock, data) do
    :gen_tcp.send(sock, data)
  end

  # Receive raw bytes with a timeout; returns {:ok, data} | {:error, reason}.
  defp recv_raw(sock, timeout) do
    :gen_tcp.recv(sock, 0, timeout)
  end


  # ===========================================================================
  # 13. Connection edge cases (TCP layer)
  # ===========================================================================


  # ===========================================================================
  # 14. Data type boundaries over TCP
  # ===========================================================================


  # ===========================================================================
  # 15. Concurrent access over TCP
  # ===========================================================================


  # Helper for receiving a pubsub push message on a subscribed socket.
  # In pubsub mode, the server uses active:once and sends data asynchronously.
  defp recv_pubsub_message(sock, timeout) do
    # The socket may be in active:once mode (messages arrive as {:tcp, sock, data}).
    # But we can also try passive recv since the connection handler sends via
    # transport.send which writes to the socket directly.
    recv_pubsub_loop(sock, "", timeout)
  end

  defp recv_pubsub_loop(sock, buf, timeout) do
    case Parser.parse(buf) do
      {:ok, [val | _], _} ->
        val

      {:ok, [], _} ->
        # Try both passive recv and active message
        receive do
          {:tcp, ^sock, data} ->
            recv_pubsub_loop(sock, buf <> data, timeout)
        after
          0 ->
            case :gen_tcp.recv(sock, 0, timeout) do
              {:ok, data} -> recv_pubsub_loop(sock, buf <> data, timeout)
              {:error, reason} -> {:tcp_error, reason}
            end
        end
    end
  end
end
