defmodule Ferricstore.Health do
  @moduledoc """
  Tracks node readiness for Kubernetes health probes (spec 2C.1 Phase 3).

  The readiness flag starts as `false` during application startup and is set
  to `true` by `Ferricstore.Application` after the full supervision tree has
  started successfully. This prevents Kubernetes from routing traffic to a
  node that hasn't finished initializing its shards.

  Uses `:persistent_term` for zero-cost reads from any process. The write
  happens exactly once during normal operation (startup), so the global GC
  cost of `:persistent_term.put/2` is negligible.

  ## Public API

    * `ready?/0`     - returns `true` when the node is ready to serve traffic
    * `set_ready/1`  - sets the readiness flag (called by Application on startup)
    * `check/0`      - returns a detailed health map with shard status

  ## Usage by Kubernetes

  In standalone mode, configure readiness probes against the isolated
  `FerricstoreServer.Health.ProbeEndpoint` listener:

      readinessProbe:
        httpGet:
          path: /health/ready
          port: 4001
        initialDelaySeconds: 2
        periodSeconds: 5
  """

  alias Ferricstore.Stats
  alias Ferricstore.Store.Router

  @ready_key {__MODULE__, :ready}

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "Shard health info."
  @type shard_info :: %{index: non_neg_integer(), status: String.t(), keys: non_neg_integer()}

  @typedoc "Full health check result."
  @type health_result :: %{
          status: :ok | :starting,
          shard_count: non_neg_integer(),
          shards: [shard_info()],
          uptime_seconds: non_neg_integer()
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Returns `true` when the node has completed startup and is ready to serve
  traffic. Returns `false` during startup or if the readiness flag has not
  been set.

  This is a zero-cost read from `:persistent_term`.

  ## Examples

      iex> Ferricstore.Health.ready?()
      true

  """
  @spec ready?() :: boolean()
  def ready? do
    :persistent_term.get(@ready_key, false)
  end

  @doc """
  Sets the node readiness flag.

  Called by `Ferricstore.Application.start/2` after the supervision tree has
  started successfully. Can also be used in tests to simulate startup/shutdown
  transitions.

  ## Parameters

    * `value` - `true` to mark the node as ready, `false` to mark it as starting

  ## Examples

      iex> Ferricstore.Health.set_ready(true)
      :ok

  """
  @spec set_ready(boolean()) :: :ok
  def set_ready(value) when is_boolean(value) do
    :persistent_term.put(@ready_key, value)
    :ok
  end

  @doc """
  Returns a detailed health check map including per-shard status.

  The returned map contains:

    * `:status`         - `:ok` when ready, `:starting` otherwise
    * `:shard_count`    - configured number of shards
    * `:shards`         - list of per-shard info maps with `:index`, `:status`,
                          and `:keys`
    * `:uptime_seconds` - seconds since server start

  ## Examples

      iex> Ferricstore.Health.check()
      %{
        status: :ok,
        shard_count: 4,
        shards: [
          %{index: 0, status: "ok", keys: 42},
          ...
        ],
        uptime_seconds: 120
      }

  """
  @spec check() :: health_result()
  def check do
    ctx = default_instance()

    shard_count =
      if ctx, do: ctx.shard_count, else: Application.get_env(:ferricstore, :shard_count, 4)

    shards = collect_shard_info(shard_count, ctx)

    # Readiness requires: flag set + all shards alive + all Raft leaders elected
    ready = ready?()
    all_shards_ok = Enum.all?(shards, fn s -> s.status == "ok" end)
    raft_ready = ready and all_shards_ok and ctx != nil and check_raft_leaders(shard_count)

    status =
      cond do
        not ready -> :starting
        not all_shards_ok -> :starting
        not raft_ready -> :starting
        true -> :ok
      end

    %{
      status: status,
      shard_count: shard_count,
      shards: shards,
      uptime_seconds: Stats.uptime_seconds()
    }
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp default_instance do
    FerricStore.Instance.get(:default)
  rescue
    ArgumentError -> nil
  end

  # Checks that every shard's Raft server has an elected leader.
  # Without a leader, writes will fail. Returns true if all leaders
  # are elected, false if any shard has no leader.
  @spec check_raft_leaders(non_neg_integer()) :: boolean()
  defp check_raft_leaders(shard_count), do: check_raft_leaders(shard_count, &raft_leader?/1)

  defp check_raft_leaders(shard_count, checker) when shard_count > 0 do
    0..(shard_count - 1)
    |> Task.async_stream(checker,
      ordered: false,
      max_concurrency: min(shard_count, max(System.schedulers_online(), 1)),
      timeout: 1_100,
      on_timeout: :kill_task
    )
    |> Enum.all?(&match?({:ok, true}, &1))
  end

  defp check_raft_leaders(_shard_count, _checker), do: false

  defp raft_leader?(shard_index) do
    case Ferricstore.Raft.Cluster.members(shard_index, 1_000) do
      {:ok, _members, leader} when leader not in [nil, :undefined] -> true
      _ -> false
    end
  catch
    :exit, _ -> false
  end

  if Mix.env() == :test do
    @doc false
    def __check_raft_leaders_for_test__(shard_count, checker)
        when is_integer(shard_count) and is_function(checker, 1) do
      check_raft_leaders(shard_count, checker)
    end
  end

  @spec collect_shard_info(non_neg_integer(), FerricStore.Instance.t() | nil) :: [shard_info()]
  defp collect_shard_info(0, _ctx), do: []

  defp collect_shard_info(shard_count, ctx) do
    Enum.map(0..(shard_count - 1), fn index ->
      name = if ctx, do: Router.shard_name(ctx, index), else: nil
      keys = keydir_size(ctx, index)

      status =
        try do
          case Process.whereis(name) do
            pid when is_pid(pid) -> if Process.alive?(pid), do: "ok", else: "down"
            nil -> "down"
          end
        rescue
          ArgumentError -> "down"
        end

      %{index: index, status: status, keys: keys}
    end)
  end

  defp keydir_size(%{keydir_refs: refs}, index)
       when is_tuple(refs) and is_integer(index) and index >= 0 and index < tuple_size(refs) do
    safe_ets_size(elem(refs, index))
  end

  defp keydir_size(_ctx, index), do: safe_ets_size(:"keydir_#{index}")

  defp safe_ets_size(table) do
    case :ets.info(table, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  rescue
    ArgumentError -> 0
  end
end
