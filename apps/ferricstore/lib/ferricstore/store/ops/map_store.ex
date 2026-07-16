defmodule Ferricstore.Store.Ops.MapStore do
  @moduledoc false

  alias Ferricstore.Store.ReadResult

  def set(store, key, value, opts) do
    get? = Map.get(opts, :get, false)
    current = current_meta(store, key, get?, opts.keepttl)

    case current do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      current ->
        set_with_current(store, key, value, opts, get?, current)
    end
  end

  defp set_with_current(store, key, value, opts, get?, current) do
    current_loaded? = get? or opts.keepttl

    {old_value, effective_expire} =
      case current do
        nil ->
          {nil, opts.expire_at_ms}

        {old_val, old_exp} ->
          {old_val, if(opts.keepttl, do: old_exp, else: opts.expire_at_ms)}
      end

    case skip_write?(store, key, opts, current, current_loaded?) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      true ->
        if get?, do: old_value, else: nil

      false ->
        case put(store, key, value, effective_expire) do
          :ok -> if get?, do: old_value, else: :ok
          error -> error
        end
    end
  end

  defp current_meta(store, key, true, _keepttl), do: get_meta(store, key)

  defp current_meta(store, key, false, true) do
    case expire_at_ms(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      nil -> nil
      exp -> {nil, exp}
    end
  end

  defp current_meta(_store, _key, false, false), do: nil

  defp skip_write?(_store, _key, %{nx: false, xx: false}, _current, _current_loaded?),
    do: false

  defp skip_write?(store, key, opts, current, current_loaded?) do
    exists_result = if current_loaded?, do: current != nil, else: exists?(store, key)

    case exists_result do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      true -> opts.nx
      false -> opts.xx
      invalid -> ReadResult.failure({:invalid_exists_result, invalid})
    end
  end

  defp exists?(store, key) do
    case store do
      %{exists?: exists_fun} when is_function(exists_fun, 1) ->
        exists_fun.(key)

      _ ->
        case get_meta(store, key) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          nil -> false
          _current -> true
        end
    end
  end

  defp get_meta(store, key), do: store.get_meta.(key)

  defp expire_at_ms(store, key) do
    case store do
      %{expire_at_ms: expire_at_ms} when is_function(expire_at_ms, 1) ->
        expire_at_ms.(key)

      _ ->
        case get_meta(store, key) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          nil -> nil
          {_value, expire_at_ms} -> expire_at_ms
        end
    end
  end

  defp put(store, key, value, expire_at_ms), do: store.put.(key, value, expire_at_ms)
end
