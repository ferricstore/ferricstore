defmodule Ferricstore.Cluster.FeatureLevel do
  @moduledoc """
  Cluster feature levels gate replicated behavior that older releases would
  apply differently.

  A replica that does not understand a command leaves its state unchanged while
  newer replicas apply it, so new replicated behavior may only be used once
  every member runs a release that supports it. Each release declares the
  highest level it supports (`supported/0`). A level becomes active when it is
  raised: every member must report support, then a marker key is written to
  every shard's log.

  Markers are plain puts, which every release applies the same way, and one key
  is kept per level, so levels only ever go up and a gated command checks the
  marker of its level on the shard that applies it. Markers are server-internal
  keys: clients cannot read or write them, they are not counted by DBSIZE/KEYS,
  and FLUSHDB keeps them.
  """

  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.Router

  @supported 1
  @prefix ServerCatalog.root_prefix() <> "feature-level:"
  @marker "1"
  @probe_timeout_ms 5_000
  @members_timeout_ms 5_000

  @type probe :: (node() -> {:ok, non_neg_integer()} | {:error, term()})

  @doc "Highest feature level this release can apply."
  @spec supported() :: pos_integer()
  def supported, do: @supported

  @doc false
  @spec key(pos_integer()) :: binary()
  def key(level) when is_integer(level) and level > 0, do: @prefix <> Integer.to_string(level)

  @doc false
  @spec key?(term()) :: boolean()
  def key?(<<@prefix, _level::binary>>), do: true
  def key?(_key), do: false

  @doc false
  @spec marker() :: binary()
  def marker, do: @marker

  @doc """
  Feature levels per shard: `%{supported: n, active: lowest shard level, shards: [level]}`.
  """
  @spec status(FerricStore.Instance.t()) :: {:ok, map()} | {:error, term()}
  def status(ctx) do
    with {:ok, shards} <- shard_levels(ctx) do
      {:ok, %{supported: @supported, active: Enum.min(shards, fn -> 0 end), shards: shards}}
    end
  end

  @doc """
  Raises the cluster to `level` once every member reports support for it.
  Never lowers a level; raising to the current level again is a no-op.
  """
  @spec raise(FerricStore.Instance.t(), term(), keyword()) :: {:ok, map()} | {:error, term()}
  def raise(ctx, level, opts \\ [])

  def raise(ctx, level, opts) when is_integer(level) and level > 0 do
    with :ok <- validate_supported(level),
         {:ok, nodes} <- members(ctx, opts),
         :ok <- require_members_support(nodes, level, Keyword.get(opts, :member_supported)),
         :ok <- write_markers(ctx, level) do
      status(ctx)
    end
  end

  def raise(_ctx, _level, _opts), do: {:error, :invalid_feature_level}

  @doc """
  Refuses a joining node that cannot apply the highest level any shard has.
  """
  @spec validate_join_target(node(), FerricStore.Instance.t(), keyword()) ::
          :ok | {:error, term()}
  def validate_join_target(target_node, ctx, opts \\ []) do
    with {:ok, shards} <- shard_levels(ctx) do
      active = Enum.max(shards, fn -> 0 end)

      case member_supported(target_node, Keyword.get(opts, :member_supported)) do
        {:ok, supported} when supported >= active ->
          :ok

        {:ok, supported} ->
          {:error, {:target_below_feature_level, target_node, supported, active}}

        {:error, reason} when active == 0 ->
          # Nothing is gated yet, so an unreachable or older target is still safe.
          _ = reason
          :ok

        {:error, reason} ->
          {:error, {:feature_level_probe_failed, [{target_node, reason}]}}
      end
    end
  end

  @doc false
  # A member's supported level. A node without this module runs a release that
  # predates feature levels, which is level 0.
  @spec member_supported(node(), probe() | nil) :: {:ok, non_neg_integer()} | {:error, term()}
  def member_supported(member, probe \\ nil)

  def member_supported(member, nil) when member == node(), do: {:ok, @supported}
  def member_supported(member, nil), do: normalize_probe(remote_supported(member))

  def member_supported(member, probe) when is_function(probe, 1),
    do: normalize_probe(probe.(member))

  defp normalize_probe({:ok, level}) when is_integer(level) and level >= 0, do: {:ok, level}
  defp normalize_probe({:error, {:exception, :undef, _stack}}), do: {:ok, 0}
  defp normalize_probe({:error, _reason} = error), do: error
  defp normalize_probe(other), do: {:error, {:invalid_feature_level_probe, other}}

  defp remote_supported(member) do
    {:ok, :erpc.call(member, __MODULE__, :supported, [], @probe_timeout_ms)}
  catch
    :error, {:exception, _reason, _stack} = exception -> {:error, exception}
    :error, {:erpc, reason} -> {:error, reason}
    kind, reason -> {:error, {kind, reason}}
  end

  defp validate_supported(level) when level <= @supported, do: :ok

  defp validate_supported(level),
    do: {:error, {:unsupported_feature_level, level, @supported}}

  defp members(ctx, opts) do
    case Keyword.get(opts, :members) do
      fun when is_function(fun, 0) -> fun.()
      nil -> cluster_members(ctx)
    end
  end

  defp cluster_members(ctx) do
    if Router.durable_context?(ctx) do
      0..(ctx.shard_count - 1)
      |> Enum.reduce_while({:ok, MapSet.new([node()])}, fn shard_index, {:ok, nodes} ->
        case RaftCluster.members(shard_index, @members_timeout_ms) do
          {:ok, members, _leader} ->
            {:cont, {:ok, Enum.reduce(members, nodes, &MapSet.put(&2, member_node(&1)))}}

          {:error, reason} ->
            {:halt, {:error, {:feature_level_members_unavailable, shard_index, reason}}}
        end
      end)
      |> case do
        {:ok, nodes} -> {:ok, nodes |> MapSet.delete(nil) |> MapSet.to_list()}
        {:error, _reason} = error -> error
      end
    else
      {:ok, [node()]}
    end
  end

  defp member_node({_name, member}) when is_atom(member), do: member
  defp member_node(%{id: {_name, member}}) when is_atom(member), do: member
  defp member_node(_member), do: nil

  defp require_members_support(nodes, level, probe) do
    {below, failed} =
      Enum.reduce(nodes, {[], []}, fn member, {below, failed} ->
        case member_supported(member, probe) do
          {:ok, supported} when supported >= level -> {below, failed}
          {:ok, supported} -> {[{member, supported} | below], failed}
          {:error, reason} -> {below, [{member, reason} | failed]}
        end
      end)

    cond do
      failed != [] -> {:error, {:feature_level_probe_failed, Enum.sort(failed)}}
      below != [] -> {:error, {:members_below_feature_level, level, Enum.sort(below)}}
      true -> :ok
    end
  end

  # Markers for every level up to `level` on every shard. Rewriting an existing
  # marker is harmless, so a raise interrupted part way can simply be retried.
  defp write_markers(ctx, level) do
    for shard_index <- 0..(ctx.shard_count - 1), marker_level <- 1..level, reduce: :ok do
      :ok -> Router.cluster_feature_level_put(ctx, shard_index, key(marker_level), @marker)
      {:error, _reason} = error -> error
    end
  end

  defp shard_levels(ctx) do
    0..(ctx.shard_count - 1)
    |> Enum.reduce_while({:ok, []}, fn shard_index, {:ok, levels} ->
      case shard_level(ctx, shard_index) do
        {:ok, level} -> {:cont, {:ok, [level | levels]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, levels} -> {:ok, Enum.reverse(levels)}
      {:error, _reason} = error -> error
    end
  end

  # Highest consecutive level whose marker this shard has applied.
  defp shard_level(ctx, shard_index) do
    Enum.reduce_while(1..@supported, {:ok, 0}, fn level, {:ok, reached} ->
      case Router.read_shard_value(ctx, shard_index, key(level)) do
        {:ok, @marker} -> {:cont, {:ok, level}}
        {:ok, nil} -> {:halt, {:ok, reached}}
        {:ok, _other} -> {:halt, {:error, {:invalid_feature_level_marker, shard_index, level}}}
        :unavailable -> {:halt, {:error, {:feature_level_shard_unavailable, shard_index}}}
        {:error, reason} -> {:halt, {:error, {:feature_level_read_failed, shard_index, reason}}}
      end
    end)
  end
end
