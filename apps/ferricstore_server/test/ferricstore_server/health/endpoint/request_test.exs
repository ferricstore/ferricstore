defmodule FerricstoreServer.Health.Endpoint.RequestTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Endpoint.Request

  defmodule TricklingTransport do
    @delay_ms 20

    def prepare(chunks) do
      Process.put({__MODULE__, :chunks}, chunks)
      Process.put({__MODULE__, :recv_calls}, [])
    end

    def recv(_socket, length, timeout_ms) do
      Process.put(
        {__MODULE__, :recv_calls},
        [{length, timeout_ms} | Process.get({__MODULE__, :recv_calls}, [])]
      )

      Process.sleep(@delay_ms)

      case Process.get({__MODULE__, :chunks}, []) do
        [chunk | rest] ->
          Process.put({__MODULE__, :chunks}, rest)
          {:ok, chunk}

        [] ->
          {:error, :closed}
      end
    end

    def setopts(_socket, options) do
      Process.put({__MODULE__, :setopts}, options)
      :ok
    end

    def recv_calls do
      {__MODULE__, :recv_calls}
      |> Process.get([])
      |> Enum.reverse()
    end

    def setopts, do: Process.get({__MODULE__, :setopts}, [])
  end

  test "trickled headers and body consume one absolute request deadline" do
    TricklingTransport.prepare([
      "POST /dashboard/action HTTP/1.1\r\n",
      "Content-Length: 4\r\n",
      "\r\n",
      "body"
    ])

    assert {:ok, "POST", "/dashboard/action", _headers, "body"} =
             Request.read_request(:socket, TricklingTransport)

    timeouts = Enum.map(TricklingTransport.recv_calls(), &elem(&1, 1))

    assert length(timeouts) == 4
    assert TricklingTransport.setopts() == [buffer: 8_192]

    assert timeouts
           |> Enum.chunk_every(2, 1, :discard)
           |> Enum.all?(fn [earlier, later] -> later < earlier end)
  end

  test "parse_request_line parses method path headers and body" do
    request = "POST /dashboard/action HTTP/1.1\r\nContent-Length: 4\r\nX-Test: yes\r\n\r\nbody"

    assert Request.parse_request_line(request) ==
             {:ok, "POST", "/dashboard/action", %{"content-length" => "4", "x-test" => "yes"},
              "body"}
  end

  test "parse_request_line rejects malformed request lines" do
    assert Request.parse_request_line("bad\r\n\r\n") == :error
    assert Request.parse_request_line("GET /missing-terminator HTTP/1.1") == :error
  end

  test "parse_request_line rejects invalid UTF-8 before routing" do
    invalid_path = <<"GET /dashboard/", 0xFF, " HTTP/1.1\r\nHost: localhost\r\n\r\n">>
    invalid_header = <<"GET /dashboard HTTP/1.1\r\nX-Test: ", 0xFF, "\r\n\r\n">>

    assert Request.parse_request_line(invalid_path) == :error
    assert Request.parse_request_line(invalid_header) == :error
  end

  test "parse_request_line rejects ambiguous HTTP body framing" do
    assert Request.parse_request_line(
             "POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n"
           ) == :error

    assert Request.parse_request_line(
             "POST / HTTP/1.1\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n"
           ) == :error

    assert Request.parse_request_line("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n") ==
             :error
  end

  test "request byte limit includes both headers and the declared body" do
    TricklingTransport.prepare([
      "POST /dashboard/action HTTP/1.1\r\nContent-Length: 8192\r\n\r\n"
    ])

    assert :error = Request.read_request(:socket, TricklingTransport)
    assert length(TricklingTransport.recv_calls()) == 1
  end

  test "request_content_length defaults, parses, and rejects invalid lengths" do
    assert Request.request_content_length(%{}) == {:ok, 0}
    assert Request.request_content_length(%{"content-length" => "12"}) == {:ok, 12}
    assert Request.request_content_length(%{"content-length" => "-1"}) == :error
    assert Request.request_content_length(%{"content-length" => "abc"}) == :error
  end

  test "parse_headers normalizes valid fields and rejects malformed or duplicate fields" do
    assert Request.parse_headers(["Content-Type: text/plain ", "X-Id: 42"]) ==
             {:ok, %{"content-type" => "text/plain", "x-id" => "42"}}

    assert Request.parse_headers(["bad"]) == :error
    assert Request.parse_headers(["X-Id: 1", "x-id: 2"]) == :error
    assert Request.parse_headers(["Transfer-Encoding: chunked"]) == :error
  end
end
