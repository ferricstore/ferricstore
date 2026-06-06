defmodule FerricstoreServer.Health.Endpoint.RequestTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Endpoint.Request

  test "parse_request_line parses method path headers and body" do
    request = "POST /dashboard/action HTTP/1.1\r\nContent-Length: 4\r\nX-Test: yes\r\n\r\nbody"

    assert Request.parse_request_line(request) ==
             {:ok, "POST", "/dashboard/action",
              %{"content-length" => "4", "x-test" => "yes"}, "body"}
  end

  test "parse_request_line rejects malformed request lines" do
    assert Request.parse_request_line("bad\r\n\r\n") == :error
    assert Request.parse_request_line("GET /missing-terminator HTTP/1.1") == :error
  end

  test "request_content_length defaults, parses, and rejects invalid lengths" do
    assert Request.request_content_length(%{}) == {:ok, 0}
    assert Request.request_content_length(%{"content-length" => "12"}) == {:ok, 12}
    assert Request.request_content_length(%{"content-length" => "-1"}) == :error
    assert Request.request_content_length(%{"content-length" => "abc"}) == :error
  end

  test "parse_headers trims names and values and ignores malformed lines" do
    assert Request.parse_headers([" Content-Type : text/plain ", "bad", "X-Id: 42"]) == %{
             "content-type" => "text/plain",
             "x-id" => "42"
           }
  end
end
