defmodule Ferricstore.FaultInjection do
  @moduledoc false

  @hook_key {__MODULE__, :hook}

  @spec maybe_pause(atom(), map()) :: :ok | {:error, term()}
  def maybe_pause(point, metadata \\ %{}) when is_atom(point) and is_map(metadata) do
    case :persistent_term.get(@hook_key, nil) do
      fun when is_function(fun, 2) -> normalize(fun.(point, metadata))
      _other -> :ok
    end
  catch
    kind, reason -> {:error, {:fault_injection_hook_failed, kind, reason}}
  end

  @doc false
  @spec put_hook((atom(), map() -> term())) :: :ok
  def put_hook(fun) when is_function(fun, 2) do
    :persistent_term.put(@hook_key, fun)
    :ok
  end

  @doc false
  @spec clear_hook() :: :ok
  def clear_hook do
    :persistent_term.erase(@hook_key)
    :ok
  catch
    :error, :badarg -> :ok
  end

  defp normalize(:ok), do: :ok
  defp normalize({:error, _reason} = error), do: error
  defp normalize(_other), do: :ok
end
