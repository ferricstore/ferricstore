defmodule FerricstoreServer.Native.Connection.InboundBudget do
  @moduledoc false

  alias FerricstoreServer.Native.ResourceBudget

  @spec resize(GenServer.server(), reference() | nil, non_neg_integer()) ::
          {:ok, reference() | nil} | {:error, term()}
  def resize(_budget, nil, 0), do: {:ok, nil}

  def resize(budget, nil, bytes) when is_integer(bytes) and bytes > 0 do
    ResourceBudget.acquire(budget, :inbound_bytes, self(), bytes)
  end

  def resize(budget, token, 0) when is_reference(token) do
    :ok = ResourceBudget.release(budget, token)
    {:ok, nil}
  end

  def resize(budget, token, bytes)
      when is_reference(token) and is_integer(bytes) and bytes > 0 do
    case ResourceBudget.resize(budget, token, bytes) do
      :ok -> {:ok, token}
      {:error, _reason} = error -> error
    end
  end

  @spec release(GenServer.server(), reference() | nil) :: :ok
  def release(_budget, nil), do: :ok
  def release(budget, token) when is_reference(token), do: ResourceBudget.release(budget, token)
end
