defmodule Ferricstore.Store.Ops.MapStore do
  @moduledoc false

  alias Ferricstore.Store.Ops

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
        opts.nx and Ops.exists?(store, key) -> true
        opts.xx and not Ops.exists?(store, key) -> true
        true -> false
      end

    if skip? do
      if get?, do: old_value, else: nil
    else
      Ops.put(store, key, value, effective_expire)
      if get?, do: old_value, else: :ok
    end
  end

  defp current_meta(store, key, true, _keepttl), do: Ops.get_meta(store, key)

  defp current_meta(store, key, false, true) do
    case Ops.expire_at_ms(store, key) do
      nil -> nil
      exp -> {nil, exp}
    end
  end

  defp current_meta(_store, _key, false, false), do: nil
end
