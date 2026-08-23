defmodule FerricstoreHttp.ErrorResponse do
  @moduledoc false

  @type response :: {pos_integer(), map(), map()}

  @spec response(term()) :: response()
  def response(:unauthenticated), do: unauthenticated()
  def response(:reauthentication_required), do: unauthenticated()
  def response({:invalid_credentials, _reason}), do: unauthenticated()

  def response({:rate_limited, retry_after_ms}) do
    headers = %{"retry-after" => Integer.to_string(div(retry_after_ms + 999, 1_000))}
    error(429, "rate_limited", headers)
  end

  def response(:authentication_unavailable), do: error(503, "authentication_unavailable")
  def response(:body_too_large), do: error(413, "body_too_large")
  def response(:malformed_json), do: error(400, "malformed_json")
  def response(:malformed_msgpack), do: error(400, "malformed_msgpack")
  def response(:malformed_envelope), do: error(400, "malformed_envelope")
  def response(:malformed_binary_envelope), do: error(400, "malformed_binary_envelope")
  def response(:request_timeout), do: error(408, "request_timeout")
  def response(:invalid_invocation_name), do: error(400, "invalid_invocation_name")
  def response(:definition_not_found), do: error(404, "definition_not_found")
  def response(:invocation_not_found), do: error(404, "invocation_not_found")
  def response(:invocation_disabled), do: error(403, "invocation_disabled")
  def response(:forbidden), do: error(403, "forbidden")
  def response(:idempotency_key_required), do: error(409, "idempotency_key_required")
  def response(:idempotency_conflict), do: error(409, "idempotency_conflict")
  def response(:subject_required), do: error(400, "subject_required")
  def response(:payload_too_large), do: error(413, "payload_too_large")
  def response(:invocations_unavailable), do: error(501, "invocations_unavailable")
  def response(:value_not_found), do: error(404, "value_not_found")
  def response(:value_name_forbidden), do: error(403, "value_name_forbidden")
  def response(:value_too_large), do: error(413, "value_too_large")
  def response(:value_name_required), do: error(400, "value_name_required")
  def response(:value_names_required), do: error(400, "value_names_required")
  def response(:value_required), do: error(400, "value_required")
  def response(:invalid_value_encoding), do: error(400, "invalid_value_encoding")

  def response({:too_many_commands, limit}),
    do: error(413, "too_many_commands", %{}, %{"max" => limit})

  def response({:malformed_command, index}),
    do: error(400, "malformed_command", %{}, %{"index" => index})

  def response({:unsupported_command, index, command}) do
    error(400, "unsupported_command", %{}, %{"index" => index, "command" => command})
  end

  def response({:busy, _reason}), do: error(503, "server_overloaded", %{"retry-after" => "1"})

  def response(:resource_budget_unavailable),
    do: error(503, "server_overloaded", %{"retry-after" => "1"})

  def response({:invalid_batch, _reason}), do: error(400, "invalid_batch")
  def response({:invalid_deadline, _reason}), do: error(400, "invalid_deadline")
  def response(_unexpected), do: error(500, "internal_error")

  defp unauthenticated do
    error(401, "unauthenticated", %{"www-authenticate" => ~s(Basic realm="FerricStore")})
  end

  defp error(status, code, headers \\ %{}, details \\ %{}) do
    body = %{"error" => Map.merge(%{"code" => code}, details)}
    {status, body, Map.put(headers, "cache-control", "no-store")}
  end
end
