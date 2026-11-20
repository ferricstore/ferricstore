defmodule Ferricstore.Cluster.BootstrapFeatureLevel do
  @moduledoc """
  Starts a brand-new standalone node at the feature level its release supports.

  Feature levels keep a mixed-version cluster applying every write the same
  way, so a cluster upgraded in place stays at its level until an operator
  raises it. A standalone node that bootstrapped every shard in this start
  (`Ferricstore.Raft.WARaftBackend.fresh_standalone_bootstrap?/0`) has no
  other member and no data an older release wrote, so it raises right away
  and runs the current behavior instead of the level-0 compatibility paths:
  for Flows, index-kept hibernation and claims that read hibernated records
  (level 8), and claims of hibernated Flows without a partition key (level 7).

  `:bootstrap_feature_level` picks the level: `:supported` (the default), a
  level number, or `0` to stay at level 0 so the data directory can still be
  opened by an older release. A node that restarts on existing data, joins a
  cluster or forms a multi-node cluster is left alone; raise those with
  `FerricStore.raise_cluster_feature_level/1`.
  """

  require Logger

  alias Ferricstore.Cluster.FeatureLevel

  @attempts 120
  @retry_ms 500

  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {Task, :start_link, [__MODULE__, :run, [opts]]},
      restart: :temporary
    }
  end

  @doc """
  Raises a fresh node to the configured level, retrying while shards or the
  raise are not ready. Returns `:skipped` when there is nothing to do.
  """
  @spec run(keyword()) :: {:ok, map()} | {:error, term()} | :skipped
  def run(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)

    fresh? =
      Keyword.get_lazy(
        opts,
        :fresh?,
        &Ferricstore.Raft.WARaftBackend.fresh_standalone_bootstrap?/0
      )

    level =
      opts
      |> Keyword.get_lazy(:level, fn ->
        Application.get_env(:ferricstore, :bootstrap_feature_level, :supported)
      end)
      |> target_level()

    if fresh? and level > 0 do
      raise_fun = Keyword.get(opts, :raise_fun, &FeatureLevel.raise/2)
      attempts = Keyword.get(opts, :attempts, @attempts)
      retry_ms = Keyword.get(opts, :retry_ms, @retry_ms)
      raise_with_retry(raise_fun, ctx, level, attempts, retry_ms)
    else
      :skipped
    end
  end

  defp target_level(:supported), do: FeatureLevel.supported()

  defp target_level(level) when is_integer(level) and level >= 0,
    do: min(level, FeatureLevel.supported())

  defp target_level(_invalid), do: 0

  defp raise_with_retry(raise_fun, ctx, level, attempts, retry_ms) do
    case raise_fun.(ctx, level) do
      {:ok, %{active: active}} = ok when active >= level ->
        Logger.info("new standalone node started at feature level #{active}")
        ok

      other when attempts > 1 ->
        _ = other
        Process.sleep(retry_ms)
        raise_with_retry(raise_fun, ctx, level, attempts - 1, retry_ms)

      {:error, _reason} = error ->
        Logger.warning(
          "new standalone node stays at its feature level; raise it with " <>
            "FerricStore.raise_cluster_feature_level(#{level}): #{inspect(error)}"
        )

        error

      other ->
        {:error, {:unexpected_feature_level_raise, other}}
    end
  end
end
