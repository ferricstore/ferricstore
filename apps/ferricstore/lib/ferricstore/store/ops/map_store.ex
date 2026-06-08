defmodule Ferricstore.Store.Ops.MapStore do
  @moduledoc false

  def set(store, key, value, opts) do
    get? = Map.get(opts, :get, false)
    current = current_meta(store, key, get?, opts.keepttl)

    {old_value, effective_expire} =
      case current do
        nil ->
          {nil, opts.expire_at_ms}

        {old_val, old_exp} ->
          {old_val, if(opts.keepttl, do: old_exp, else: opts.expire_at_ms)}
      end

    skip? =
      cond do
        opts.nx and exists?(store, key) -> true
        opts.xx and not exists?(store, key) -> true
        true -> false
      end

    if skip? do
      if get?, do: old_value, else: nil
    else
      put(store, key, value, effective_expire)
      if get?, do: old_value, else: :ok
    end
  end

  defp current_meta(store, key, true, _keepttl), do: get_meta(store, key)

  defp current_meta(store, key, false, true) do
    case expire_at_ms(store, key) do
      nil -> nil
      exp -> {nil, exp}
    end
  end

  defp current_meta(_store, _key, false, false), do: nil

  defp exists?(store, key) do
    case store do
      %{exists?: exists_fun} when is_function(exists_fun, 1) ->
        exists_fun.(key)

      _ ->
        get_meta(store, key) != nil
    end
  end

  defp get_meta(store, key), do: store.get_meta.(key)

  defp expire_at_ms(store, key) do
    case store do
      %{expire_at_ms: expire_at_ms} when is_function(expire_at_ms, 1) ->
        expire_at_ms.(key)

      _ ->
        case get_meta(store, key) do
          nil -> nil
          {_value, expire_at_ms} -> expire_at_ms
        end
    end
  end

  defp put(store, key, value, expire_at_ms), do: store.put.(key, value, expire_at_ms)
end
