# FerricStore Elixir SDK

Topology-aware Elixir client for the FerricStore native TCP protocol.

The SDK bootstraps from one or more seed nodes, performs `HELLO`/`AUTH`, fetches
`SHARDS`, builds a slot table with the server-compatible CRC32/hash-tag
algorithm, and opens connections lazily per advertised native endpoint. Keyed
commands are routed to the endpoint for the relevant shard leader. Learned
endpoints must be on seed hosts by default; multi-host clusters should pass
`trusted_hosts: [...]`, `endpoint_policy: {:allow_hosts, [...]}`, or
`endpoint_policy: :any` with an `endpoint_validator`.

```elixir
{:ok, client} =
  FerricStore.SDK.start_link(
    seeds: [{"127.0.0.1", 6388}],
    username: "default",
    password: System.fetch_env!("FERRICSTORE_PASSWORD")
  )

:ok = FerricStore.SDK.set(client, "{tenant:1}:k", "value")
{:ok, "value"} = FerricStore.SDK.get(client, "{tenant:1}:k")
```

Multi-key reads are split by route group when keys span shards. Multi-key
writes require keys on the same shard unless `atomicity: :per_shard` is passed:

```elixir
:ok = FerricStore.SDK.mset(client, %{
  "{a}:1" => "one",
  "{b}:2" => "two"
}, atomicity: :per_shard)

{:ok, ["one", "two"]} = FerricStore.SDK.mget(client, ["{a}:1", "{b}:2"])
```

The top-level module exposes KV commands directly. Flow and admin commands are
available through typed native payloads:

```elixir
{:ok, "OK"} =
  FerricStore.SDK.Flow.create(client, %{
    id: "flow-1",
    type: "email",
    state: "queued",
    payload: "hello"
  })

{:ok, flow} = FerricStore.SDK.Flow.get(client, %{id: "flow-1", full: true})
{:ok, slot} = FerricStore.SDK.Admin.cluster_keyslot(client, %{key: "{a}:1", args: ["{a}:1"]})
```

Advanced callers can use raw native opcodes without hard-coded integers:

```elixir
FerricStore.SDK.request_by_key(client, :get, "k", %{key: "k"})
FerricStore.SDK.command_exec(client, "PING", [])
```

On stale endpoints, closed sockets, or reroute responses, the client refreshes
`SHARDS` and retries once before returning an error.
