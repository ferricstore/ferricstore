defmodule FerricstoreServer.Connection.Pipeline do
  @moduledoc """
  Pipeline dispatcher with batch fast paths for GET, SET, mixed GET+SET, and
  Flow workloads.

  ## Performance boundary

  This is a server hot path. It owns Redis-style pipeline coalescing and decides
  whether commands can use batch fast paths or must cross stateful barriers.
  Avoid behaviours/protocols, new per-command allocations, and extra tasks in
  this module. Any refactor needs before/after DBOS Flow and memtier pipeline
  benchmarks.
  """

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.Flow, as: FlowCommand
  alias Ferricstore.Stats
  alias Ferricstore.Store.PipelinePlanner
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Sendfile, as: ConnSendfile
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.TcpOpts
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  require Logger

  @stateful_cmds MapSet.new(~w(
    HELLO CLIENT QUIT AUTH ACL RESET CONFIG SANDBOX
    MULTI EXEC DISCARD WATCH UNWATCH
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
    BLPOP BRPOP BLMOVE BLMPOP
    FETCH_OR_COMPUTE
  ))

  @prefetch_read_only_keyed_cmds MapSet.new(~w(
    EXISTS STRLEN TTL PTTL TYPE
    HGET HMGET HGETALL HEXISTS HLEN HKEYS HVALS
    LRANGE LINDEX LLEN
    SCARD SISMEMBER SMEMBERS SMISMEMBER
    ZCARD ZSCORE ZMSCORE ZRANGE ZREVRANGE
    JSON.GET JSON.MGET
    BF.EXISTS BF.MEXISTS CF.EXISTS CMS.QUERY TOPK.QUERY TDIGEST.INFO
  ))

  @prefetch_read_only_keyless_cmds MapSet.new(~w(
    PING ECHO DBSIZE INFO COMMAND LOLWUT LASTSAVE
    CLUSTER.HEALTH CLUSTER.STATS CLUSTER.SLOTS CLUSTER.STATUS CLUSTER.ROLE
    FERRICSTORE.METRICS MEMORY
  ))

  # Maximum commands in a single pipeline batch (100K).
  @max_pipeline_size 100_000
  @max_key_size 65_535
  @max_value_size 512 * 1024 * 1024
  @max_setrange_offset 536_870_911
  @max_bit_offset 4_294_967_295

  @doc """
  Returns the maximum pipeline batch size.
  """
  @spec max_pipeline_size() :: pos_integer()
  def max_pipeline_size, do: @max_pipeline_size

  # ---------------------------------------------------------------------------
  # Pipeline dispatch entry point
  # ---------------------------------------------------------------------------

  @doc """
  Dispatches a pipeline of commands with tiered fast paths.

  Fast paths (all skip per-command overhead, batch response into one TCP write):
  1. All GETs → direct ETS batch lookup
  2. All SETs → batch Raft/ETS insert
  3. Mixed GET+SET → split, batch each, reassemble
  4. Other pure commands → batch Dispatcher.dispatch_ast with per-command ACL
  5. Stateful (MULTI/AUTH/etc) → sequential through handle_command_fn
  """
  @spec pipeline_dispatch(
          commands :: [term()],
          state :: struct(),
          handle_command_fn :: (term(), struct() -> {atom(), iodata(), struct()}),
          send_response_fn :: (term(), term(), iodata() -> :ok | {:error, term()})
        ) :: {:quit, struct()} | {:continue, struct()}
  def pipeline_dispatch([single_cmd], state, handle_command_fn, send_response_fn) do
    case handle_command_fn.(single_cmd, state) do
      {:quit, response, quit_state} ->
        _ = send_response_result(quit_state, send_response_fn, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_or_quit(new_state, send_response_fn, response)
    end
  end

  def pipeline_dispatch(commands, state, handle_command_fn, send_response_fn) do
    case try_batch_get_fast_path(commands, state, send_response_fn) do
      {:ok, result} ->
        result

      :fallback ->
        case try_batch_set_fast_path(commands, state, send_response_fn) do
          {:ok, result} ->
            result

          :fallback ->
            case try_batch_flow_write_fast_path(commands, state, send_response_fn) do
              {:ok, result} ->
                result

              :fallback ->
                try_mixed_fast_path(commands, state, handle_command_fn, send_response_fn)
            end
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Batch GET fast path
  # ---------------------------------------------------------------------------

  use FerricstoreServer.Connection.Pipeline.FastPaths
  use FerricstoreServer.Connection.Pipeline.Flow
  use FerricstoreServer.Connection.Pipeline.PureBatch
  use FerricstoreServer.Connection.Pipeline.Streaming
  use FerricstoreServer.Connection.Pipeline.Fallback
end
