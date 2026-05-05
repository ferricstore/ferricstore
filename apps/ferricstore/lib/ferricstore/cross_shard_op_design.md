# CrossShardOp — Mini-Percolator Design Spec

## Overview

Cross-shard multi-key commands use per-key locking through Raft consensus.
Only involved keys are locked — rest of the shard operates normally.

## Modes

- **Quorum mode**: Mini-Percolator (lock → intent → execute → unlock)
- **Async mode**: Return CROSSSLOT error with message suggesting quorum mode or hash tags
- **Same-shard**: No overhead — execute directly without locking

## Protocol (Quorum Cross-Shard)

1. Generate `owner_ref = make_ref()`
2. Lock phase — batch per shard, ordered by shard index (prevents deadlock):
   - `ra.process_command(shard_N, {:lock_keys, [key1, key2], owner_ref, 5000})`
   - All keys on a shard locked atomically (all-or-nothing per batch)
   - If any shard rejects → unlock acquired, retry with backoff (max 3 retries)
3. Write intent to coordinator shard (lowest shard index):
   - `{:cross_shard_intent, owner_ref, %{command, keys, value_hashes, status: :executing}}`
4. Execute the actual operations through Raft
5. Delete intent
6. Unlock all keys (batch per shard)

## Lock Storage

Locks are Raft commands applied through StateMachine. Lock map in state:
```elixir
%{locks: %{"key" => {owner_ref, expire_at_ms}}}
```

## New StateMachine Commands

- `{:lock_keys, [keys], owner_ref, ttl_ms}` — lock all keys atomically, reject if any already locked (unless expired)
- `{:unlock_keys, [keys], owner_ref}` — unlock keys owned by this ref
- `{:cross_shard_intent, owner_ref, intent_map}` — write intent record
- `{:delete_intent, owner_ref}` — remove intent record

## Write Rejection During Lock

When `:put` or `:delete` arrives for a locked key, StateMachine rejects with
`{:error, :key_locked}` UNLESS the command carries the matching owner_ref.

Execute phase commands include owner_ref:
- `{:locked_put, key, value, expire_at_ms, owner_ref}`
- `{:locked_delete, key, owner_ref}`

## Lock TTL

Default 5s. If coordinator crashes, locks expire automatically. StateMachine
checks expiry inline on every lock check — no background resolver needed for
lock cleanup.

## Intent Record

```elixir
%{
  command: :smove | :rename | :copy | ...,
  keys: %{source: "src", dest: "dst"},
  value_hashes: %{"src" => :erlang.phash2(value)},
  status: :executing,
  created_at: System.os_time(:millisecond)
}
```

## Intent Resolver

On startup (leader only), scan for intents with status `:executing`.
For each intent, read current state of involved keys. Compare value hashes:
- Hash matches → safe to complete the operation (re-execute remaining steps)
- Hash doesn't match → stale intent, someone wrote new data. Clean up intent, don't act.
- Key doesn't exist → already completed or expired. Clean up intent.

## Deadlock Prevention

Lock shards in ascending shard index order. Two concurrent operations always
try lower shard first — no circular wait.

## CROSSSLOT Error

```
CROSSSLOT Keys in request don't hash to the same slot. Use hash tags {tag}
to colocate keys.
```

## Affected Commands

| Command | Keys to lock | Notes |
|---------|-------------|-------|
| SMOVE | source, destination | Lock both |
| RENAME | old, new | Lock both |
| RENAMENX | old, new | Lock both |
| COPY | source, destination | Lock destination only |
| SDIFFSTORE | destination, keys... | Lock destination only |
| SINTERSTORE | destination, keys... | Lock destination only |
| SUNIONSTORE | destination, keys... | Lock destination only |
| ZINTERSTORE | destination, keys... | Lock destination only |
| ZUNIONSTORE | destination, keys... | Lock destination only |
| LMOVE | source, destination | Lock both |

## CrossShardOp Module API

```elixir
# Called by command modules:
CrossShardOp.execute(keys_with_roles, execute_fn, opts)

# keys_with_roles: [{"src", :read_write}, {"dst", :write}]
# execute_fn: fn -> ...command logic... end
# opts: [intent: %{command: :smove, ...}]

# Returns:
# - result of execute_fn (same-shard or quorum cross-shard)
# - {:error, "CROSSSLOT ..."} (async cross-shard)
```

## Same-Shard Fast Path

If all keys hash to the same shard, execute_fn is called directly.
No locking, no intent, no overhead. This is the common case.

## Batch Locking

One Raft command per shard, not per key:
```elixir
{:lock_keys, ["key1", "key2", "key3"], owner_ref, ttl_ms}
```
SUNIONSTORE with 100 keys across 4 shards = 4 lock commands, not 100.

## Performance

- Same-shard: zero overhead
- Cross-shard: ~4 Raft round-trips (lock + intent + execute + unlock)
- Lock/unlock parallelized across shards
