defmodule Ferricstore.Store.Shard.NamespaceUsageScopes do
  @moduledoc false

  @active_key :"$ferricstore_namespace_usage_active"

  @spec initialize(:ets.tid() | atom(), [binary()]) :: :ok
  def initialize(_usage, []), do: :ok

  def initialize(usage, scopes) do
    rows = [{@active_key, true} | Enum.flat_map(Enum.uniq(scopes), &metadata_rows/1)]
    :ets.insert(usage, rows)
    :ok
  end

  @spec tracked(:ets.tid() | atom()) :: [binary()]
  def tracked(usage) do
    :ets.select(usage, [{{{:tracked, :"$1"}, true}, [{:is_binary, :"$1"}], [:"$1"]}])
  end

  @spec tracked_for_key(:ets.tid() | atom(), binary()) :: [binary()]
  def tracked_for_key(usage, logical_key) do
    wildcard =
      if logical_key != "*" and tracked?(usage, "*"), do: ["*"], else: []

    wildcard ++ Enum.filter(candidates(logical_key), &tracked?(usage, &1))
  end

  @spec metadata_rows(binary()) :: [tuple()]
  def metadata_rows(scope),
    do: [{{:tracked, scope}, true}, {{:scope, scope}, 0, 0}, {{:flow_scope, scope}, 0}]

  @spec in_scope?(binary(), binary()) :: boolean()
  def in_scope?(_key, "*"), do: true

  def in_scope?(key, scope) do
    key == scope or
      (byte_size(key) > byte_size(scope) and :binary.part(key, 0, byte_size(scope)) == scope and
         :binary.at(key, byte_size(scope)) == ?:)
  end

  defp candidates(key) do
    prefixes =
      key
      |> :binary.matches(":")
      |> Enum.map(fn {offset, 1} -> binary_part(key, 0, offset) end)

    [key | prefixes]
  end

  defp tracked?(usage, scope),
    do: :ets.lookup(usage, {:tracked, scope}) == [{{:tracked, scope}, true}]
end
