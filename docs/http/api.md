# HTTP API

The HTTP application is part of the normal FerricStore release and is disabled by default. See
[HTTP and HTTPS API](../../guides/http-api.md) for listener, TLS, ACL, Docker, and client setup.

## Command request

`POST /v1/commands` requires an `Authorization` header and one supported envelope.
The authenticated session is revalidated for every batch, and FerricStore applies command and key
ACL checks to every command before that command executes.

### Binary-safe JSON

Content type: `application/json`

```json
{
  "encoding": "ferricstore-json-v1",
  "commands": [
    [
      {"$ferricstore_bytes": "U0VU"},
      {"$ferricstore_bytes": "a2V5"},
      {"$ferricstore_bytes": "dmFsdWU="}
    ]
  ]
}
```

Every binary is represented by exactly one `$ferricstore_bytes` base64 marker. Maps are represented
by `$ferricstore_map` pairs so binary and non-string keys round trip without loss.

The response repeats `encoding` and encodes result values with the same marker format.

### MessagePack

Content type: `application/vnd.ferricstore.commands+msgpack`

The top-level map is:

```text
{
  "encoding" => "ferricstore-msgpack-v1",
  "commands" => [["SET", <binary key>, <binary value>]]
}
```

MessagePack binary values use the MessagePack binary type and do not incur base64 expansion.

### Legacy JSON

For compatibility, `{"commands":[["PING"]]}` is accepted. It is suitable only for JSON/UTF-8-safe
commands and values. New SDK integrations should use binary-safe JSON or MessagePack.

## Success response

HTTP `200` means the batch was accepted and executed in order. Individual command failures remain
in their original positions:

```json
{
  "results": [
    {"status": "ok", "value": "PONG"},
    {"status": "error", "error": {"code": "noperm", "message": "NOPERM ..."}}
  ]
}
```

Command errors always include bounded UTF-8 `code` and `message` fields. FerricStore may also return
the bounded public diagnostic fields `detail`, `hint`, `position`, `context`, `retryable`,
`safe_to_retry`, and `retry_after_ms`. The HTTP layer drops malformed or private backend fields;
SDKs may use the retry fields but must not infer retry safety from the HTTP status alone.

A top-level HTTP error means FerricStore submitted no command from the batch. The gateway validates
the full batch before execution.

## Error responses

All protocol errors use a stable JSON or MessagePack shape:

```json
{"error":{"code":"unauthenticated"}}
```

| Status | Code | Meaning |
| --- | --- | --- |
| 400 | `malformed_json`, `malformed_msgpack`, `malformed_envelope` | Invalid wire envelope |
| 400 | `malformed_command`, `unsupported_command`, `invalid_batch` | Invalid stateless batch |
| 401 | `unauthenticated` | Missing, invalid, disabled, or stale credential |
| 408 | `request_timeout` | Request body deadline expired |
| 413 | `body_too_large`, `too_many_commands` | Configured request limit exceeded |
| 414 | `request_line_too_large` | Configured request-line limit exceeded |
| 429 | `rate_limited` | Shared FerricStore auth rate limiter rejected the attempt |
| 431 | `request_headers_too_large` | Configured header name or value limit exceeded |
| 503 | `authentication_unavailable`, `server_overloaded` | Readiness or resource budget unavailable |
| 500 | `internal_error` | Unexpected server error without internal detail leakage |

Authentication and error responses set `Cache-Control: no-store`. Rate-limit and overload responses
provide `Retry-After` when appropriate.

## Stateful and blocking command behavior

The stateless gateway rejects connection/session commands, transactions, and Pub/Sub before
submitting any command in the batch. Use FerricStore's native TCP protocol for those workflows.

Blocking list and stream commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `XREAD`, and `XREADGROUP`)
are supported inside one ordered HTTP command batch. A request that contains one of these commands
is executed independently instead of being coalesced with another HTTP request. The configured HTTP
request deadline is still authoritative, including when the Redis command uses a zero timeout.

## Invocation API

These routes exist only when `invocations_enabled` is true. They use JSON and require the same
authentication as `/v1/commands`.

The default Basic provider uses the authenticated ACL username as the invocation subject. Ordinary
OSS deployments do not need an additional namespace header or trusted-proxy identity configuration.
Definitions without a partition template use automatic partitioning.

`POST /v1/invocations/:name` accepts an invocation attributes object and returns HTTP `202` with
the created invocation id and initial state. An idempotency key may be supplied either as the
`Idempotency-Key` header or the `idempotency_key` JSON field.

`GET /v1/invocations/:id` returns the underlying Flow record. `GET
/v1/invocations/:id/result` returns HTTP `202` while the record is pending and HTTP `200` after it
reaches a terminal state.

Invocation-scoped values use these routes:

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/invocations/:id/values/:name` | Read one value as JSON or base64 |
| `GET /v1/invocations/:id/values/:name/content` | Read one value as raw bytes |
| `POST /v1/invocations/:id/values/batch` | Read `{"names":[...]}` in one response |
| `POST /v1/invocations/:id/values` | Store a named `json`, `bytes_base64`, or string `value` |

The batch endpoint accepts at most `FERRICSTORE_HTTP_MAX_BATCH_COMMANDS` names, removes duplicate
Flow references, and fetches all remaining references with one bounded `FLOW.VALUE.MGET` operation.
Definitions may use any non-empty `flow_type`; `invocation:<name>` is only the default.

The HTTP layer implements these routes through the shared `INVOCATION.DEFINITION.*`,
`INVOCATION.CREATE`, `INVOCATION.GET`, and `INVOCATION.PARTITION.LIST` commands. It does not bypass
FerricStore ACLs: each command and its logical invocation key are authorized before execution. The
runner likewise uses the shared Flow commands under its own ACL identity.

Invocation failures preserve the normal error envelope. Stable codes include
`invalid_invocation_name`, `definition_not_found`, `invocation_not_found`, `invocation_disabled`,
`forbidden`, `idempotency_key_required`, `idempotency_conflict`, `subject_required`,
`payload_too_large`, `value_not_found`, `value_name_forbidden`,
`value_too_large`, and `invocations_unavailable`.
