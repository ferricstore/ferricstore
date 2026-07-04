defmodule FerricStore.SDK.Native.ConnectionTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.Codec
  alias FerricStore.SDK.Native.Connection

  test "TLS connections verify peer and hostname by default" do
    opts =
      Connection.tls_options(%{
        host: "db.internal",
        native_port: 6389,
        tls: true,
        cacertfile: "/tmp/ca.pem"
      })

    assert Keyword.fetch!(opts, :verify) == :verify_peer
    assert Keyword.fetch!(opts, :server_name_indication) == ~c"db.internal"
    assert Keyword.fetch!(opts, :cacertfile) == "/tmp/ca.pem"
    assert Keyword.has_key?(opts, :customize_hostname_check)
  end

  test "TLS verification can pin a server name while dialing a resolved address" do
    opts =
      Connection.tls_options(%{
        host: "93.184.216.34",
        server_name: "db.internal",
        native_port: 6389,
        tls: true,
        cacertfile: "/tmp/ca.pem"
      })

    assert Keyword.fetch!(opts, :verify) == :verify_peer
    assert Keyword.fetch!(opts, :server_name_indication) == ~c"db.internal"
    assert Keyword.fetch!(opts, :cacertfile) == "/tmp/ca.pem"
  end

  test "TLS verification can be explicitly disabled for local development" do
    opts =
      Connection.tls_options(%{
        host: "127.0.0.1",
        native_port: 6389,
        tls: true,
        verify: false
      })

    assert Keyword.fetch!(opts, :verify) == :verify_none
    refute Keyword.has_key?(opts, :customize_hostname_check)
  end

  test "unmatched response frames are bounded while waiting for a matching response" do
    unmatched_frames =
      for request_id <- 10..89 do
        response_frame(0x0003, request_id, String.duplicate("x", 1024))
      end

    matching_frame = response_frame(0x0003, 1, "PONG")

    {:ok, port, server} =
      start_fake_server(IO.iodata_to_binary([unmatched_frames, matching_frame]))

    {:ok, conn} =
      Connection.start(%{
        host: "127.0.0.1",
        native_port: port,
        tls: false,
        connect_timeout: 1_000
      })

    assert {:ok, "PONG"} = Connection.request(conn, 0x0003, %{"message" => "PONG"}, 0, 1_000)
    assert byte_size(:sys.get_state(conn).buffer) <= 64 * 1024

    Task.await(server)
  end

  test "chunked responses are reassembled before payload decode" do
    value = String.duplicate("chunked-value-", 32)
    frames = chunked_response_frames(0x0003, 1, value, 37)

    assert length(frames) > 1

    {:ok, port, server} = start_fake_server(IO.iodata_to_binary(frames))

    {:ok, conn} =
      Connection.start(%{
        host: "127.0.0.1",
        native_port: port,
        tls: false,
        connect_timeout: 1_000
      })

    assert {:ok, ^value} = Connection.request(conn, 0x0003, %{"message" => "PONG"}, 0, 1_000)

    Task.await(server)
  end

  test "incomplete chunked response waits for the final chunk instead of decoding a prefix" do
    [first_frame | _rest] = chunked_response_frames(0x0003, 1, "complete-value", 8)

    {:ok, port, server} = start_fake_server(first_frame, hold_open_ms: 150)

    {:ok, conn} =
      Connection.start(%{
        host: "127.0.0.1",
        native_port: port,
        tls: false,
        connect_timeout: 1_000
      })

    assert {:error, :timeout} =
             Connection.request(conn, 0x0003, %{"message" => "PONG"}, 0, 50)

    Task.await(server)
  end

  defp start_fake_server(response_bytes, opts \\ []) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 1_000)
        :ok = :gen_tcp.send(socket, response_bytes)
        Process.sleep(Keyword.get(opts, :hold_open_ms, 0))
        :ok = :gen_tcp.close(socket)
        :ok = :gen_tcp.close(listener)
      end)

    {:ok, port, server}
  end

  defp response_frame(opcode, request_id, payload) do
    body_payload = Codec.encode_value(payload)
    body = <<0::unsigned-16, body_payload::binary>>

    response_frame(opcode, request_id, body, 0)
  end

  defp response_frame(opcode, request_id, body, flags) do
    <<"FSNP", 0x81, 0, 0::unsigned-32, opcode::unsigned-16, request_id::unsigned-64,
      byte_size(body)::unsigned-32, body::binary>>
    |> put_flags(flags)
  end

  defp chunked_response_frames(opcode, request_id, payload, chunk_size) do
    body_payload = Codec.encode_value(payload)
    body = <<0::unsigned-16, body_payload::binary>>

    body
    |> chunks(chunk_size, [])
    |> Enum.with_index()
    |> Enum.map(fn {chunk, index} ->
      flags = if index == div(byte_size(body) + chunk_size - 1, chunk_size) - 1, do: 0, else: 0x20
      response_frame(opcode, request_id, chunk, flags)
    end)
  end

  defp chunks("", _chunk_size, acc), do: Enum.reverse(acc)

  defp chunks(body, chunk_size, acc) when byte_size(body) <= chunk_size,
    do: Enum.reverse([body | acc])

  defp chunks(body, chunk_size, acc) do
    <<chunk::binary-size(chunk_size), rest::binary>> = body
    chunks(rest, chunk_size, [chunk | acc])
  end

  defp put_flags(<<"FSNP", version, _old_flags, rest::binary>>, flags),
    do: <<"FSNP", version, flags, rest::binary>>
end
