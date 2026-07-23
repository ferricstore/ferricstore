defmodule Ferricstore.Store.Shard.NamespaceUsageFlowAccounting do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.{Keys, StorageScope}
  alias Ferricstore.Store.Shard.NamespaceUsageScopes

  @type flow_scope :: binary() | :unknown | :unscoped | nil

  @spec scope_for_put(:ets.tid() | atom(), binary(), term()) :: flow_scope()
  def scope_for_put(usage, storage_key, value) do
    if Keys.state_key?(storage_key) do
      case :ets.lookup(usage, {:flow, storage_key}) do
        [{{:flow, ^storage_key}, flow_scope}] -> flow_scope
        [] -> decode_scope(value)
      end
    end
  end

  @spec existing_or_unknown_scope(:ets.tid() | atom(), binary()) :: flow_scope()
  def existing_or_unknown_scope(usage, storage_key) do
    if Keys.state_key?(storage_key) do
      case :ets.lookup(usage, {:flow, storage_key}) do
        [{{:flow, ^storage_key}, flow_scope}] -> flow_scope
        [] -> :unknown
      end
    end
  end

  @spec decoded_scope(binary(), term()) :: flow_scope()
  def decoded_scope(storage_key, value) do
    if Keys.state_key?(storage_key), do: decode_scope(value)
  end

  @spec add(:ets.tid() | atom(), binary(), flow_scope()) :: :ok
  def add(_usage, _storage_key, nil), do: :ok

  def add(usage, storage_key, flow_scope) do
    :ets.insert(usage, {{:flow, storage_key}, flow_scope})
    update_matching_scopes(usage, flow_scope, 1)
  end

  @spec remove(:ets.tid() | atom(), binary()) :: :ok
  def remove(usage, storage_key) do
    case :ets.take(usage, {:flow, storage_key}) do
      [{{:flow, ^storage_key}, flow_scope}] -> update_matching_scopes(usage, flow_scope, -1)
      [] -> :ok
    end
  end

  @spec count(:ets.tid() | atom(), binary()) :: non_neg_integer()
  def count(usage, scope) do
    case :ets.lookup(usage, {:flow_scope, scope}) do
      [{{:flow_scope, ^scope}, count}] when is_integer(count) and count >= 0 -> count
      _missing_or_invalid -> 0
    end
  end

  @spec count_for_scope(:ets.tid() | atom(), binary()) :: non_neg_integer()
  def count_for_scope(usage, scope) do
    :ets.foldl(
      fn
        {{:flow, _storage_key}, flow_scope}, count ->
          if in_scope?(flow_scope, scope), do: count + 1, else: count

        _other, count ->
          count
      end,
      0,
      usage
    )
  end

  defp decode_scope(value) when is_binary(value) do
    record = Flow.decode_record(value)
    partition_key = Map.get(record, :partition_key)

    case StorageScope.physical_scope_prefix(partition_key) do
      {:ok, scope} ->
        scope

      :unscoped ->
        case StorageScope.logical_partition_key(record) do
          {:ok, scope} when is_binary(scope) -> scope
          {:ok, nil} -> :unscoped
          {:error, _reason} -> :unknown
        end
    end
  rescue
    _invalid_or_corrupt -> :unknown
  end

  defp decode_scope(_value), do: :unknown

  defp update_matching_scopes(usage, :unknown, delta) do
    usage
    |> NamespaceUsageScopes.tracked()
    |> Enum.each(&update_scope_count(usage, &1, delta))

    :ok
  end

  defp update_matching_scopes(usage, flow_scope, delta) when is_binary(flow_scope) do
    wildcard =
      if :ets.lookup(usage, {:tracked, "*"}) == [{{:tracked, "*"}, true}],
        do: ["*"],
        else: []

    usage
    |> NamespaceUsageScopes.tracked_for_key(flow_scope)
    |> Kernel.++(wildcard)
    |> Enum.uniq()
    |> Enum.each(&update_scope_count(usage, &1, delta))

    :ok
  end

  defp update_matching_scopes(_usage, :unscoped, _delta), do: :ok

  defp update_scope_count(usage, scope, delta) do
    next = count(usage, scope) + delta

    if next < 0 do
      raise "namespace usage Flow aggregate underflow"
    end

    :ets.insert(usage, {{:flow_scope, scope}, next})
    :ok
  end

  defp in_scope?(:unknown, _scope), do: true
  defp in_scope?(flow_scope, "*") when is_binary(flow_scope), do: true

  defp in_scope?(flow_scope, scope) when is_binary(flow_scope) and is_binary(scope),
    do: NamespaceUsageScopes.in_scope?(flow_scope, scope)

  defp in_scope?(_flow_scope, _scope), do: false
end
