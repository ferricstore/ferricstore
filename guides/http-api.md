# HTTP and HTTPS API

FerricStore ships an in-process HTTP application in the normal OSS release. It uses the same
authentication gateway, command planner, ACL checks, deadlines, resource limits, and storage path as
the native protocol. The HTTP application is disabled by default and does not open a port until it is
explicitly enabled.

Use the HTTP interface for SDK environments where persistent native connections are inconvenient,
for ordered stateless command batches, and for the optional invocation API. Use the native protocol
for transactions, Pub/Sub subscriptions, and other connection-scoped behavior. Blocking list and
stream commands are supported in an ordered HTTP command batch, but the HTTP request deadline still
bounds how long they may wait.

## Create an ACL identity first

HTTP Basic authentication uses FerricStore ACL usernames and passwords. Before exposing the HTTP
listener, create a least-privilege identity through a trusted native administration connection or the
local dashboard setup flow. For example, the logical ACL command for an application restricted to
one key prefix is:

```text
ACL SETUSER web-api on resetpass >replace-with-a-long-random-password \
  resetkeys +GET +SET +DEL ~web-api:*
```

Every command and key in an HTTP batch is checked against this identity. Authentication-session
caching does not bypass those checks, and an ACL or password change invalidates the cached session
before the next batch executes.

For the normal OSS setup, that ACL username is also the invocation subject. No additional namespace
identifier, identity header, or proxy configuration is required. Keep trusted context headers
disabled unless a verified ingress is deliberately supplying identity.

## Enable HTTPS in a native release

Provide a PEM certificate chain and its matching unencrypted PEM private key. The certificate must
contain a subject alternative name for every DNS name or IP address used by clients. FerricStore
accepts TLS 1.2 and TLS 1.3.

```bash
export FERRICSTORE_HTTP_ENABLED=true
export FERRICSTORE_HTTP_BIND=0.0.0.0
export FERRICSTORE_HTTP_PORT=8080
export FERRICSTORE_HTTP_TLS_ENABLED=true
export FERRICSTORE_HTTP_TLS_CERT_FILE=/run/secrets/ferricstore/http-cert.pem
export FERRICSTORE_HTTP_TLS_KEY_FILE=/run/secrets/ferricstore/http-key.pem

_build/prod/rel/ferricstore/bin/ferricstore start
```

The listener refuses to start when TLS is enabled without both files. Store the private key with the
smallest practical read permission and make it readable only by the FerricStore process. The
certificate file should include the leaf certificate followed by any required intermediate
certificates.

For a short-lived local certificate:

```bash
mkdir -p certs
openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 30 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout certs/http-key.pem \
  -out certs/http-cert.pem
chmod 600 certs/http-key.pem
```

This self-signed certificate is for local testing. Clients must explicitly trust
`certs/http-cert.pem`; do not disable certificate or hostname verification.

## Enable HTTPS in Docker

The image contains the HTTP application and exposes port `8080`, but the listener remains disabled
until configured. Mount certificates read-only:

```bash
docker run --rm \
  -p 6388:6388 \
  -p 8080:8080 \
  -e FERRICSTORE_HTTP_ENABLED=true \
  -e FERRICSTORE_HTTP_TLS_ENABLED=true \
  -e FERRICSTORE_HTTP_TLS_CERT_FILE=/run/secrets/ferricstore/http-cert.pem \
  -e FERRICSTORE_HTTP_TLS_KEY_FILE=/run/secrets/ferricstore/http-key.pem \
  -v "$PWD/certs:/run/secrets/ferricstore:ro" \
  -v ferricstore_data:/data \
  quay.io/ferricstore/ferricstore:0.11.11
```

The container runs as UID `10001`; ensure that user can read the mounted certificate and key.

## Send a command batch

The simplest JSON form is suitable for UTF-8-safe command arguments:

```bash
curl --fail-with-body \
  --cacert certs/http-cert.pem \
  --user 'web-api:replace-with-a-long-random-password' \
  --header 'content-type: application/json' \
  --data '{"commands":[["SET","web-api:example","value"],["GET","web-api:example"]]}' \
  https://localhost:8080/v1/commands
```

Use the binary-safe JSON or MessagePack envelope for arbitrary keys and values. See the complete
[HTTP wire contract](../docs/http/api.md).

## Listener configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `FERRICSTORE_HTTP_ENABLED` | `false` | Start the HTTP application listener. |
| `FERRICSTORE_HTTP_BIND` | `127.0.0.1` | Numeric IPv4 or IPv6 address on which to listen. |
| `FERRICSTORE_HTTP_PORT` | `8080` | Listener port. |
| `FERRICSTORE_HTTP2_ENABLED` | `false` | Advertise HTTP/2 in addition to HTTP/1.1. |
| `FERRICSTORE_HTTP_TLS_ENABLED` | `false` | Start the listener with TLS. |
| `FERRICSTORE_HTTP_TLS_CERT_FILE` | none | PEM certificate-chain path. |
| `FERRICSTORE_HTTP_TLS_KEY_FILE` | none | Matching PEM private-key path. |
| `FERRICSTORE_HTTP_MAX_CONNECTIONS` | `1024` | Maximum accepted HTTP connections. |
| `FERRICSTORE_HTTP_MAX_IN_FLIGHT_REQUESTS` | `1024` | Maximum admitted concurrent streams. |
| `FERRICSTORE_HTTP_REQUEST_TIMEOUT_MS` | `30000` | Absolute command-request time budget. |
| `FERRICSTORE_HTTP_MAX_BODY_BYTES` | `1048576` | Maximum request body size. |
| `FERRICSTORE_HTTP_MAX_BATCH_COMMANDS` | `1000` | Maximum commands in one request. |
| `FERRICSTORE_HTTP_METRICS_ENABLED` | `true` | Start the metrics collector and expose `GET /metrics`. |

The complete set also includes bounded header, keep-alive, authentication-cache, command-batching,
invocation-runner, and outbound-target controls. Invalid values fail application startup rather than
silently falling back.

The native-release bind default is loopback. The container image sets the bind value to `0.0.0.0`
but still keeps the listener disabled, so publishing port `8080` has no effect until
`FERRICSTORE_HTTP_ENABLED=true` is supplied.

## Health and readiness

The API listener exposes:

- `GET /health` for listener liveness;
- `GET /ready` for listener and command-gateway readiness;
- `GET /metrics` for HTTP metrics when enabled.

The existing isolated node health ports remain available independently. Point an HTTP API load
balancer at the API listener's `/ready` endpoint.

## TLS termination at an ingress

Direct TLS is preferred when the listener is reachable outside the host. A trusted ingress may
terminate TLS only when the hop to FerricStore stays on a protected private or same-host network.
The ingress must remove caller-supplied identity-context headers before adding verified values.
Keep `FERRICSTORE_HTTP_TRUST_CONTEXT_HEADERS=false` unless that condition is enforced.

## Invocation routes and outbound targets

Invocation routes and the background runner are separate opt-ins. Definitions and invocation
records use the shared FerricStore command engine, so the same command and key ACL checks apply to
HTTP callers, the definition loader, and every runner operation.

Create a caller identity for one invocation name:

```text
ACL SETUSER mail-api on resetpass >replace-with-a-long-random-password \
  resetkeys -@all +INVOCATION.CREATE +INVOCATION.GET +FLOW.GET \
  ~invocation:send-email ~invocation:send-email:*
```

Reading a result stored as a Flow value also requires `+FLOW.VALUE.MGET` and a key pattern matching
generated `f:*` value references. The named value routes require `+FLOW.VALUE.MGET` for reads and
`+FLOW.VALUE.PUT` for writes. Keep those permissions on a dedicated caller identity; the HTTP service
additionally verifies the invocation record and the definition's allowed value names before accessing
a reference.

Create a separate runner identity. The runner never reuses caller credentials:

```text
ACL SETUSER http-invocation-runner on resetpass >replace-with-another-random-password \
  resetkeys -@all \
  +INVOCATION.DEFINITION.LIST +INVOCATION.PARTITION.LIST \
  +FLOW.CLAIM_DUE +FLOW.GET +FLOW.COMPLETE +FLOW.RETRY +FLOW.FAIL \
  ~invocation:*
```

If startup loads definitions from a file, also grant the runner
`+INVOCATION.DEFINITION.PUT ~invocation:definition:*`. Narrow `~invocation:*` to the invocation names
and partition prefixes served by that runner when practical.

One definition file can contain a JSON array or a top-level `definitions` array:

```json
{
  "definitions": [
    {
      "name": "send-email",
      "enabled": true,
      "flow_type": "email-delivery",
      "initial_state": "queued",
      "target": {
        "kind": "http_endpoint",
        "url": "https://worker.example.com/invocations",
        "timeout_ms": 10000
      },
      "partition": {"key": "invocation:{name}:{subject}"},
      "limits": {
        "idempotency_required": true,
        "max_payload_bytes": 262144,
        "max_result_bytes": 1048576
      },
      "refs": {
        "allowed_read_names": ["receipt"],
        "allowed_write_names": ["attachment"]
      }
    }
  ]
}
```

The `partition` field is optional. When omitted, FerricStore chooses a partition from the invocation
id, which is the simplest single-installation setup. Use `{subject}` only when separate ACL users
should receive distinct partitions; Basic authentication supplies it from the ACL username.

Enable the routes, definition loader, and runner with direct TLS and restricted outbound HTTPS:

```bash
export FERRICSTORE_HTTP_INVOCATIONS_ENABLED=true
export FERRICSTORE_HTTP_INVOCATION_DEFINITIONS_FILE=/etc/ferricstore/invocations.json
export FERRICSTORE_HTTP_RUNNER_ENABLED=true
export FERRICSTORE_HTTP_RUNNER_USERNAME=http-invocation-runner
export FERRICSTORE_HTTP_RUNNER_PASSWORD=replace-with-another-random-password
export FERRICSTORE_HTTP_TARGET_REQUIRE_HTTPS=true
export FERRICSTORE_HTTP_TARGET_ALLOWED_HOSTS=worker.example.com
export FERRICSTORE_HTTP_TARGET_DENY_PRIVATE_NETWORKS=true
export FERRICSTORE_HTTP_TARGET_MAX_RESPONSE_BYTES=1048576
```

Keep the runner password in the process secret store rather than a checked-in environment file.
`FERRICSTORE_HTTP_RUNNER_USERNAME_ENV` and `FERRICSTORE_HTTP_RUNNER_PASSWORD_ENV` can point the
runner at differently named secret variables. Target requests are bounded by both the configured
target timeout and a runner job deadline. See [HTTP architecture](../docs/http/architecture.md) for
the trust and execution boundaries.
