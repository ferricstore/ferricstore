# Architecture

## Request path

```text
SDK HTTP connection
  -> Cowboy listener and bounded admission
  -> content-type and binary-envelope codec
  -> configured Auth.Provider
  -> Backend behavior
  -> FerricStore AuthenticationGateway / CommandGateway
  -> canonical FerricStore command execution
```

There is no loopback TCP connection. HTTP keep-alive is handled by Cowboy; FerricStore execution is
an in-process function boundary.

The shared command gateway recognizes exact homogeneous batches containing at least two plain
`GET key` or `SET key value` commands. It validates the absolute deadline, key shape, complete ACL
command/key scope, internal-key policy, and active resource-limit implementation before using the
core storage batch primitives. Bounded GET reads preserve every logical key access and the original
result order; missing keys, compound WRONGTYPE errors, storage failures, write errors, and duplicate
SET ordering retain their ordinary command semantics. Deadline-bearing batches and cross-shard SET
batches use the canonical ordered path. Any scoped denial, malformed shape, custom resource-limit
implementation, option-bearing SET, mixed command, or structured command also falls back before
storage is touched.

Eligibility metadata is collected while the gateway performs its original command-planning pass,
avoiding another full request scan before direct execution. `PreparedBatch` remains an untrusted
opaque value at the public combined-execution boundary: the gateway validates every planned command
and derives fresh eligibility metadata there, so forged acceleration metadata cannot substitute or
reorder commands.

When invocation routes are enabled, synchronous requests follow the same authenticated backend
boundary. Definitions and invocation records remain canonical FerricStore commands; Flow operations
use native descriptors through that boundary.

```text
Invocation/value route -> invocation service -> Backend behavior -> FerricStore
Definition seeder      -> system session      -> Backend behavior -> FerricStore
Runner                 -> claim Flow work     -> HTTP target      -> complete/retry/fail
```

The optional runner has a separate system session and never reuses a caller credential. Outbound
targets are checked against an explicit host policy before connection, private networks are denied by
default, authentication secrets are loaded from environment variables, and response sizes are
bounded. Each target exchange and complete/retry/fail operation also fits within a finite runner job
deadline; a custom target adapter cannot hold a claimed-job task indefinitely.

## Ownership

- `Handlers`, `HTTP`, `Listener`, and `Router` own HTTP details.
- `Auth.Provider` owns how request identity becomes an opaque backend session.
- `Backend` owns the boundary to a command engine.
- `Backends.Ferricstore` is the only production module allowed to call FerricStore internals.
- `Invocations` owns definitions, invocation lifecycle, and scoped values without owning storage.
- `Targets` owns outbound invocation transport and egress validation.
- FerricStore owns command parsing, ACL enforcement, deadlines, resource budgets, and cluster routing.

The auth provider receives a small normalized map, not a Cowboy request. This lets custom
identity support evolve without coupling it to a web server library.

## Concurrency and backpressure

Two independent limits protect the node:

- Ranch bounds accepted network connections.
- The admission stream handler bounds in-flight HTTP streams before a request process is created.

An overloaded request receives `503 server_overloaded` with `Retry-After: 1`. Admission is released
from stream termination, including disconnect and failure paths. FerricStore's own execution budget
is a second, deeper guard.

Successful authentication may be cached as an opaque session behind a process-secret HMAC key.
Entries are peer-scoped, TTL-bound, and size-bound; concurrent misses for the same credential and
peer are collapsed. Credential bytes are not retained. FerricStore still revalidates the session's
credential epoch before every execution, so a rotated or disabled credential invalidates the cache
entry on its next command.

Optional micro-batching is partitioned by backend, credential cache key, and exact opaque session.
It combines only independently prepared stateless requests, preserves each request deadline and
reply, and acquires one global FerricStore execution lease for the combined group.

## Deadlines

Each command request creates one absolute deadline. Body reading uses its remaining monotonic time;
the same deadline is converted once to system time and passed into FerricStore. Retries or internal
steps therefore cannot reset the caller's budget.

## Protocol choices

HTTP/1.1 is the default because it is universal in functions-as-a-service, ingress, and load
balancers. Persistent connections avoid repeated TCP/TLS handshakes. HTTP/2 is optional for
deployments that have measured a benefit and can support it end to end.

JSON remains available for inspectability. `ferricstore-json-v1` tags every byte string and supports
non-string map keys. MessagePack avoids base64 expansion and is preferred for high-throughput or
binary-heavy workloads.

## Enforced rules

ArchTest fails when:

- HTTP/auth modules bypass the backend adapter to call FerricStore;
- the core engine or protocol server depends on the HTTP application;
- application services depend on Cowboy outside the transport boundary;
- auth providers depend on Cowboy;
- handlers bypass the backend behavior;
- production request paths add debug IO or sleeps.

The root Credo configuration scans every umbrella application's production and test source. The
HTTP quality alias runs formatting, compilation with warnings treated as errors, strict Credo, the
architecture suite, and all in-repository HTTP integration tests.

## Deliberate non-goals

- TCP proxying or per-user TCP pools;
- command reimplementation in the HTTP application;
- a distributed coordination layer between HTTP server instances;
- connection-scoped transactions or subscriptions;
- redirect policy in this server.

Blocking list and stream commands are supported within one ordered request. Requests that contain
them bypass cross-request command coalescing and remain bounded by the HTTP request deadline.
