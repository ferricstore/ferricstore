defmodule Ferricstore.Flow.Governance.LimitCache do
  @moduledoc false

  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Governance.Telemetry

  @table :ferricstore_flow_governance_limit_cache
  @default_multiplier 4
  @default_max_chunk 10_000

  @spec spend(FerricStore.Instance.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def spend(ctx, scope, opts) when is_binary(scope) and is_list(opts) do
    if enabled?() do
      cached_spend(ctx, scope, opts)
    else
      LimitStore.spend(ctx, scope, opts)
    end
  end

  def spend(ctx, scope, opts), do: LimitStore.spend(ctx, scope, opts)

  @spec release(FerricStore.Instance.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def release(ctx, scope, opts) when is_binary(scope) and is_list(opts) do
    if enabled?() do
      cached_release(ctx, scope, opts)
    else
      LimitStore.release(ctx, scope, opts)
    end
  end

  def release(ctx, scope, opts), do: LimitStore.release(ctx, scope, opts)

  @spec clear() :: :ok
  def clear do
    case :ets.whereis(@table) do
      :undefined -> :ok
      table -> :ets.delete_all_objects(table)
    end
  end

  defp cached_spend(ctx, scope, opts) do
    with {:ok, shard_id} <- fetch_non_negative_integer(opts, :shard_id),
         {:ok, amount} <- fetch_positive_integer(opts, :amount),
         {:ok, now_ms} <- fetch_non_negative_integer(opts, :now_ms) do
      key = {instance_name(ctx), scope, shard_id}

      case take_cached(key, amount, now_ms) do
        :ok ->
          Telemetry.emit(:limit_cache_hit, :ok, %{
            scope: scope,
            shard_id: shard_id,
            amount: amount
          })

          {:ok, %{cache: :hit, scope: scope, shard_id: shard_id, amount: amount}}

        :miss ->
          Telemetry.emit(:limit_cache_miss, :ok, %{
            scope: scope,
            shard_id: shard_id,
            amount: amount
          })

          spend_and_fill_cache(ctx, scope, opts, key, amount)
      end
    else
      _invalid -> LimitStore.spend(ctx, scope, opts)
    end
  end

  defp spend_and_fill_cache(ctx, scope, opts, key, amount) do
    chunk = cache_chunk(amount)

    case LimitStore.spend(ctx, scope, Keyword.put(opts, :amount, chunk)) do
      {:ok, result} ->
        add_cached(key, chunk - amount, lease_expires_at_ms(result))
        {:ok, result}

      {:error, _reason} when chunk != amount ->
        LimitStore.spend(ctx, scope, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp cached_release(ctx, scope, opts) do
    with {:ok, shard_id} <- fetch_non_negative_integer(opts, :shard_id),
         {:ok, amount} <- fetch_positive_integer(opts, :amount) do
      key = {instance_name(ctx), scope, shard_id}
      now_ms = release_now_ms(opts)

      case add_released_credit(key, amount, now_ms) do
        :ok ->
          Telemetry.emit(:limit_cache_recycle, :ok, %{
            scope: scope,
            shard_id: shard_id,
            amount: amount
          })

          {:ok, %{cache: :recycled, scope: scope, shard_id: shard_id, amount: amount}}

        :miss ->
          Telemetry.emit(:limit_cache_release_miss, :ok, %{
            scope: scope,
            shard_id: shard_id,
            amount: amount
          })

          LimitStore.release(ctx, scope, opts)
      end
    else
      _invalid -> LimitStore.release(ctx, scope, opts)
    end
  end

  defp take_cached(key, amount, now_ms) do
    table = table()

    case :ets.lookup(table, key) do
      [{^key, _available, expires_at_ms}]
      when is_integer(expires_at_ms) and expires_at_ms <= now_ms ->
        :ets.delete(table, key)
        :miss

      _other ->
        new_available = :ets.update_counter(table, key, {2, -amount}, {key, 0, 0})

        if new_available >= 0 do
          :ok
        else
          :ets.update_counter(table, key, {2, amount}, {key, 0, 0})
          :miss
        end
    end
  end

  defp add_cached(_key, surplus, _expires_at_ms) when surplus <= 0, do: :ok

  defp add_cached(key, surplus, expires_at_ms) do
    table = table()
    :ets.update_counter(table, key, {2, surplus}, {key, 0, expires_at_ms})
    :ets.update_element(table, key, {3, expires_at_ms})
    :ok
  end

  defp add_released_credit(key, amount, now_ms) do
    table = table()

    case :ets.lookup(table, key) do
      [{^key, _available, expires_at_ms}]
      when is_integer(expires_at_ms) and (expires_at_ms == 0 or expires_at_ms > now_ms) ->
        :ets.update_counter(table, key, {2, amount})
        :ok

      [{^key, _available, _expires_at_ms}] ->
        :ets.delete(table, key)
        :miss

      [] ->
        :miss
    end
  end

  defp table do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [
            :named_table,
            :public,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> @table
        end

      table ->
        table
    end
  end

  defp cache_chunk(amount) do
    multiplier =
      :ferricstore
      |> Application.get_env(:flow_governance_limit_cache_multiplier, @default_multiplier)
      |> normalize_positive_integer(@default_multiplier)

    max_chunk =
      :ferricstore
      |> Application.get_env(:flow_governance_limit_cache_max_chunk, @default_max_chunk)
      |> normalize_positive_integer(@default_max_chunk)

    amount
    |> Kernel.*(multiplier)
    |> min(max_chunk)
    |> max(amount)
  end

  defp enabled? do
    Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled, true) != false
  end

  defp lease_expires_at_ms(%{lease: %{expires_at_ms: expires_at_ms}})
       when is_integer(expires_at_ms),
       do: expires_at_ms

  defp lease_expires_at_ms(_result), do: 0

  defp release_now_ms(opts) do
    case Keyword.get(opts, :now_ms) do
      value when is_integer(value) and value >= 0 -> value
      _other -> Ferricstore.CommandTime.now_ms()
    end
  end

  defp instance_name(%{name: name}), do: name
  defp instance_name(_ctx), do: :default

  defp fetch_positive_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, value}
      _other -> :error
    end
  end

  defp fetch_non_negative_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> :error
    end
  end

  defp normalize_positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp normalize_positive_integer(_value, default), do: default
end
