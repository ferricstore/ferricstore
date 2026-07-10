defmodule FerricStore.API.PublicAccess do
  @moduledoc false

  alias Ferricstore.Flow.InternalKey

  defmacro defguardeddelegate(call, opts) do
    {name, _meta, args} = call
    target = Keyword.fetch!(opts, :to)
    keys = Keyword.fetch!(opts, :keys)
    call_args = Enum.map(args || [], &strip_default/1)
    delegated_call = {{:., [], [target, name]}, [], call_args}

    quote do
      def unquote(call) do
        FerricStore.API.PublicAccess.call(unquote(keys), fn -> unquote(delegated_call) end)
      end
    end
  end

  defmacro defguardedinstance(call, opts) do
    {name, _meta, args} = call
    target = Keyword.fetch!(opts, :to)
    keys = Keyword.fetch!(opts, :keys)
    call_args = Enum.map(args || [], &strip_default/1)
    instance_call = {:__instance__, [], []}
    delegated_call = {{:., [], [target, name]}, [], [instance_call | call_args]}

    quote do
      def unquote(call) do
        FerricStore.API.PublicAccess.call(unquote(keys), fn -> unquote(delegated_call) end)
      end
    end
  end

  @spec call([term()], (-> term())) :: term()
  def call(keys, fun) when is_list(keys) and is_function(fun, 0) do
    case InternalKey.authorize_public(keys) do
      :ok -> fun.()
      {:error, _reason} = error -> error
    end
  end

  @spec keys(term()) :: [term()]
  def keys(value) when is_binary(value), do: [value]
  def keys(value) when is_list(value), do: value
  def keys(value) when is_map(value), do: Map.keys(value)
  def keys(_value), do: []

  @spec destination_keys(term(), term()) :: [term()]
  def destination_keys(destination, sources), do: keys(destination) ++ keys(sources)

  @spec pair_keys(term()) :: [term()]
  def pair_keys(pairs) when is_map(pairs), do: Map.keys(pairs)

  def pair_keys(pairs) when is_list(pairs) do
    Enum.flat_map(pairs, fn
      {key, _value} -> [key]
      [key, _value] -> [key]
      %{"key" => key} -> [key]
      _invalid -> []
    end)
  end

  def pair_keys(_pairs), do: []

  defp strip_default({:\\, _meta, [arg, _default]}), do: arg
  defp strip_default(arg), do: arg
end
