defmodule Ferricstore.Store.Router do
  @moduledoc """
  Routes keys to shard GenServers using the shared `Ferricstore.Store.SlotMap`
  hashing implementation.

  This is a pure module with no process state. It provides two categories of
  functions:

  1. **Routing helpers** -- `shard_for/2` and `shard_name/2` map a key to its
     owning shard index and registered process name respectively. Supports
     Redis hash tags: keys containing `{tag}` are hashed on the tag content,
     allowing related keys to co-locate on the same shard.

  2. **Convenience accessors** -- `get/2`, `put/4`, `delete/2`, `exists?/2`,
     `keys/1`, and `dbsize/1` dispatch to the correct shard GenServer
     transparently.

  All public functions take a `ctx` (`FerricStore.Instance.t()`) as the first
  argument, replacing all persistent_term lookups with instance-local state.

  ## Performance boundary

  Router helpers sit on GET/SET/Flow hot paths. Refactors here need native
  protocol SET/GET and DBOS Flow benchmark comparison. Avoid new allocations,
  dynamic dispatch, or extra process calls in keyed request paths.
  """

  alias Ferricstore.CommandTime
  alias Ferricstore.HLC
  alias Ferricstore.HyperLogLog, as: HLL
  alias Ferricstore.ErrorReasons
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.Locator
  alias Ferricstore.Raft.ReplyAwaiter
  alias Ferricstore.Stats

  alias Ferricstore.Store.{
    BlobRef,
    BlobStore,
    BlobValue,
    CompoundCommand,
    CompoundKey,
    LFU,
    ListOps,
    SlotMap,
    TypeRegistry
  }

  @cold_batch_read_timeout_ms 10_000
  @cold_location_retry_attempts 8
  @cold_location_retry_sleep_ms 1
  @default_async_key_latch_timeout_ms 30_000
  @flow_claim_cursor_table :ferricstore_flow_claim_due_any_cursor
  @flow_claim_due_any_window_multiplier 8
  @flow_claim_due_precheck_slack_ms 5
  @flow_shard_marker :__flow_shard_index__

  use Ferricstore.Store.Router.Part01
  use Ferricstore.Store.Router.Part02
  use Ferricstore.Store.Router.Part03
  use Ferricstore.Store.Router.Part04
  use Ferricstore.Store.Router.Part05
  use Ferricstore.Store.Router.Part06
  use Ferricstore.Store.Router.Part07
  use Ferricstore.Store.Router.Part08
  use Ferricstore.Store.Router.Part09
  use Ferricstore.Store.Router.Part10
  use Ferricstore.Store.Router.Part11
end
