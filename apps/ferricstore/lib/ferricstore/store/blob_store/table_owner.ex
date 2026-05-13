defmodule Ferricstore.Store.BlobStore.TableOwner do
  @moduledoc false

  use GenServer

  @tables [
    {:ferricstore_blob_store_recovery,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, true}
     ]},
    {:ferricstore_blob_store_dirs,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, :auto}
     ]},
    {:ferricstore_blob_store_segments,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, true}
     ]},
    {:ferricstore_blob_store_locks,
     [
       :named_table,
       :public,
       :set,
       {:read_concurrency, true},
       {:write_concurrency, :auto}
     ]}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec ensure_tables() :: :ok
  def ensure_tables do
    case Process.whereis(__MODULE__) do
      nil ->
        start_unlinked()

      _pid ->
        GenServer.call(__MODULE__, :ensure_tables)
    end
  end

  @impl true
  def init(_opts) do
    :ok = ensure_all_tables()
    {:ok, %{}}
  end

  @impl true
  def handle_call(:ensure_tables, _from, state) do
    :ok = ensure_all_tables()
    {:reply, :ok, state}
  end

  defp start_unlinked do
    case GenServer.start(__MODULE__, [], name: __MODULE__) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> GenServer.call(__MODULE__, :ensure_tables)
    end
  end

  defp ensure_all_tables do
    Enum.each(@tables, &ensure_table/1)
    :ok
  end

  defp ensure_table({name, opts}) do
    case :ets.whereis(name) do
      :undefined ->
        try do
          :ets.new(name, opts)
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end
  end
end
