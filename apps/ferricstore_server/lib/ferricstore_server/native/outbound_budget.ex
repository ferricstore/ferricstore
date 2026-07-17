defmodule FerricstoreServer.Native.OutboundBudget do
  @moduledoc false

  alias FerricstoreServer.Native.ResourceBudget

  @enforce_keys [:resource_budget, :resource_token, :counter, :max_bytes, :bytes]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          resource_budget: GenServer.server(),
          resource_token: reference(),
          counter: :atomics.atomics_ref(),
          max_bytes: pos_integer(),
          bytes: pos_integer()
        }

  @spec new_counter() :: :atomics.atomics_ref()
  def new_counter, do: :atomics.new(1, signed: false)

  @spec usage(:atomics.atomics_ref()) :: non_neg_integer()
  def usage(counter), do: :atomics.get(counter, 1)

  @spec reserve_iodata(map(), pid(), iodata()) ::
          {:ok, t() | nil} | {:error, :connection_limit | :global_limit | :unavailable}
  def reserve_iodata(state, owner, iodata) when is_map(state) and is_pid(owner) do
    reserve_bytes(state, owner, IO.iodata_length(iodata))
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  @spec reserve_bytes(map(), pid(), non_neg_integer()) ::
          {:ok, t() | nil} | {:error, :connection_limit | :global_limit | :unavailable}
  def reserve_bytes(_state, _owner, 0), do: {:ok, nil}

  def reserve_bytes(
        %{
          outbound_counter: counter,
          max_outbound_bytes: limit,
          resource_budget: resource_budget
        },
        owner,
        bytes
      )
      when is_pid(owner) and is_integer(limit) and limit > 0 and is_integer(bytes) and bytes > 0 do
    case reserve_local(counter, limit, bytes) do
      :ok ->
        case ResourceBudget.acquire(resource_budget, :outbound_bytes, owner, bytes) do
          {:ok, token} ->
            {:ok,
             %__MODULE__{
               resource_budget: resource_budget,
               resource_token: token,
               counter: counter,
               max_bytes: limit,
               bytes: bytes
             }}

          {:error, {:limit, :outbound_bytes}} ->
            release_local(counter, bytes)
            {:error, :global_limit}

          {:error, _reason} ->
            release_local(counter, bytes)
            {:error, :unavailable}
        end

      {:error, :limit} ->
        {:error, :connection_limit}
    end
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  def reserve_bytes(_state, _owner, _bytes), do: {:error, :unavailable}

  @spec ensure_iodata(t(), iodata()) ::
          {:ok, t()} | {:error, :connection_limit | :global_limit | :unavailable}
  def ensure_iodata(%__MODULE__{} = lease, iodata) do
    ensure_bytes(lease, IO.iodata_length(iodata))
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  @spec ensure_bytes(t(), non_neg_integer()) ::
          {:ok, t()} | {:error, :connection_limit | :global_limit | :unavailable}
  def ensure_bytes(%__MODULE__{bytes: reserved} = lease, bytes) when bytes <= reserved,
    do: {:ok, lease}

  def ensure_bytes(%__MODULE__{} = lease, bytes) when is_integer(bytes) and bytes > lease.bytes do
    delta = bytes - lease.bytes

    case reserve_local(lease.counter, lease.max_bytes, delta) do
      :ok ->
        case ResourceBudget.resize(lease.resource_budget, lease.resource_token, bytes) do
          :ok ->
            {:ok, %{lease | bytes: bytes}}

          {:error, {:limit, :outbound_bytes}} ->
            release_local(lease.counter, delta)
            {:error, :global_limit}

          {:error, _reason} ->
            release_local(lease.counter, delta)
            {:error, :unavailable}
        end

      {:error, :limit} ->
        {:error, :connection_limit}
    end
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  def ensure_bytes(_lease, _bytes), do: {:error, :unavailable}

  @spec release(t() | nil) :: :ok
  def release(nil), do: :ok

  def release(%__MODULE__{} = lease) do
    release_local(lease.counter, lease.bytes)
    ResourceBudget.release(lease.resource_budget, lease.resource_token)
  end

  defp reserve_local(counter, limit, bytes) do
    used = :atomics.get(counter, 1)

    if bytes <= limit - used do
      case :atomics.compare_exchange(counter, 1, used, used + bytes) do
        :ok -> :ok
        _changed -> reserve_local(counter, limit, bytes)
      end
    else
      {:error, :limit}
    end
  end

  defp release_local(counter, bytes) do
    :atomics.sub(counter, 1, bytes)
    :ok
  end
end
