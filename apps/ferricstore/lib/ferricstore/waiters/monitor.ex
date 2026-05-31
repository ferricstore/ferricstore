defmodule Ferricstore.Waiters.Monitor do
  @moduledoc false

  use GenServer

  @table :ferricstore_waiter_pid_monitors

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts) do
    case GenServer.start_link(__MODULE__, [], name: __MODULE__) do
      {:error, {:already_started, _pid}} -> :ignore
      result -> result
    end
  end

  @spec ensure_table() :: :ok
  def ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [
            :set,
            :public,
            :named_table,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])

          :ok
        catch
          :error, :badarg -> :ok
        end

      _tid ->
        :ok
    end
  end

  @spec track(pid()) :: :ok
  def track(pid) when is_pid(pid) do
    ensure_table()

    case ensure_started() do
      :ok ->
        if :ets.member(@table, pid) do
          :ok
        else
          GenServer.call(__MODULE__, {:track, pid}, 5_000)
        end

      :error ->
        :ok
    end
  catch
    :exit, _reason -> :ok
    :error, _reason -> :ok
  end

  @impl true
  def init(_opts) do
    ensure_table()
    {:ok, %{}}
  end

  @impl true
  def handle_call({:track, pid}, _from, state) when is_pid(pid) do
    case :ets.lookup(@table, pid) do
      [] ->
        ref = Process.monitor(pid)
        :ets.insert(@table, {pid, ref})

      _existing ->
        :ok
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case :ets.lookup(@table, pid) do
      [{^pid, ^ref}] ->
        :ets.delete(@table, pid)
        cleanup_waiters(pid)

      _other ->
        :ok
    end

    {:noreply, state}
  end

  defp ensure_started do
    case Process.whereis(__MODULE__) do
      nil ->
        case GenServer.start(__MODULE__, [], name: __MODULE__) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, _reason} -> :error
        end

      _pid ->
        :ok
    end
  end

  defp cleanup_waiters(pid) do
    Ferricstore.Waiters.cleanup(pid)
    Ferricstore.Commands.Stream.cleanup_stream_waiters(pid)
    Ferricstore.Flow.ClaimWaiters.cleanup(pid)
  end
end
