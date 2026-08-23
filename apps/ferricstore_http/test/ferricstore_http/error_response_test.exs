defmodule FerricstoreHttp.ErrorResponseTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.ErrorResponse

  test "maps every public request failure to a stable, non-cacheable response" do
    cases = [
      {:unauthenticated, 401, "unauthenticated", %{}},
      {:reauthentication_required, 401, "unauthenticated", %{}},
      {{:invalid_credentials, :disabled}, 401, "unauthenticated", %{}},
      {{:rate_limited, 1_001}, 429, "rate_limited", %{}},
      {:authentication_unavailable, 503, "authentication_unavailable", %{}},
      {:body_too_large, 413, "body_too_large", %{}},
      {:malformed_json, 400, "malformed_json", %{}},
      {:malformed_msgpack, 400, "malformed_msgpack", %{}},
      {:malformed_envelope, 400, "malformed_envelope", %{}},
      {:malformed_binary_envelope, 400, "malformed_binary_envelope", %{}},
      {:request_timeout, 408, "request_timeout", %{}},
      {{:too_many_commands, 7}, 413, "too_many_commands", %{"max" => 7}},
      {{:malformed_command, 2}, 400, "malformed_command", %{"index" => 2}},
      {{:unsupported_command, 1, "MULTI"}, 400, "unsupported_command",
       %{"index" => 1, "command" => "MULTI"}},
      {{:busy, :execution_budget}, 503, "server_overloaded", %{}},
      {:resource_budget_unavailable, 503, "server_overloaded", %{}},
      {{:invalid_batch, :empty}, 400, "invalid_batch", %{}},
      {{:invalid_deadline, :expired}, 400, "invalid_deadline", %{}}
    ]

    Enum.each(cases, fn {reason, expected_status, expected_code, expected_details} ->
      assert {^expected_status, %{"error" => error}, headers} = ErrorResponse.response(reason)
      assert error == Map.put(expected_details, "code", expected_code)
      assert headers["cache-control"] == "no-store"
    end)
  end

  test "adds authentication and retry metadata only where appropriate" do
    assert {401, _body, headers} = ErrorResponse.response(:unauthenticated)
    assert headers["www-authenticate"] == ~s(Basic realm="FerricStore")

    assert {429, _body, headers} = ErrorResponse.response({:rate_limited, 1_001})
    assert headers["retry-after"] == "2"

    for reason <- [{:busy, :queue}, :resource_budget_unavailable] do
      assert {503, _body, headers} = ErrorResponse.response(reason)
      assert headers["retry-after"] == "1"
    end
  end

  test "unexpected terms do not leak internal details" do
    assert {500, %{"error" => %{"code" => "internal_error"}}, headers} =
             ErrorResponse.response({:database_error, "private credential material"})

    assert headers == %{"cache-control" => "no-store"}
  end
end
