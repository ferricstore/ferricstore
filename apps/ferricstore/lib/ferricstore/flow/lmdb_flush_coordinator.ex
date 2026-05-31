defmodule Ferricstore.Flow.LMDBFlushCoordinator do
  @moduledoc false

  use GenServer

  @default_max_concurrent 1

  def default_max_concurrent do
    @default_max_concurrent
  end

  def start_link(opts) do
    instance_name = Keyword.get(opts, :instance_name, :default)
    GenServer.start_link(__MODULE__, opts, name: name(instance_name))
  end

  def name(:default), do: __MODULE__
  def name(instance_name), do: :"Ferricstore.Flow.LMDBFlushCoordinator.#{instance_name}"

  def with_permit(instance_name, fun) when is_function(fun, 0) do
    case coordinator_pid(instance_name) do
      nil ->
        fun.()

      pid ->
        case acquire(pid) do
          {:ok, token} ->
            try do
              fun.()
            after
              GenServer.cast(pid, {:release, token})
            end

          _other ->
            fun.()
        end
    end
  end

  @impl true
  def init(opts) do
    max_concurrent =
      opts
      |> Keyword.get(:max_concurrent, configured_max_concurrent())
      |> normalize_positive_integer(default_max_concurrent())

    {:ok, %{max: max_concurrent, available: max_concurrent, queue: :queue.new(), holders: %{}}}
  end

  @impl true
  def handle_call(:acquire, {pid, _tag} = from, state) do
    if state.available > 0 do
      {token, state} = grant(pid, %{state | available: state.available - 1})
      {:reply, {:ok, token}, state}
    else
      {:noreply, %{state | queue: :queue.in(from, state.queue)}}
    end
  end

  @impl true
  def handle_cast({:release, token}, state) do
    {:noreply, release_token(state, token)}
  end

  @impl true
  def handle_info({:DOWN, token, :process, _pid, _reason}, state) do
    {:noreply, release_token(state, token)}
  end

  defp coordinator_pid(instance_name) do
    cond do
      is_pid(pid = Process.whereis(name(instance_name))) ->
        pid

      is_pid(pid = Process.whereis(name(:default))) ->
        pid

      true ->
        nil
    end
  end

  defp configured_max_concurrent do
    Application.get_env(
      :ferricstore,
      :flow_lmdb_max_concurrent_flushes,
      default_max_concurrent()
    )
  end

  defp acquire(pid) do
    GenServer.call(pid, :acquire, :infinity)
  catch
    :exit, _reason -> :unavailable
  end

  defp grant(pid, state) do
    token = Process.monitor(pid)
    {token, %{state | holders: Map.put(state.holders, token, pid)}}
  end

  defp release_token(state, token) do
    case Map.pop(state.holders, token) do
      {nil, holders} ->
        %{state | holders: holders}

      {_pid, holders} ->
        Process.demonitor(token, [:flush])
        grant_next(%{state | holders: holders})
    end
  end

  defp grant_next(state) do
    case :queue.out(state.queue) do
      {{:value, {pid, _tag} = from}, queue} ->
        {token, state} = grant(pid, %{state | queue: queue})
        GenServer.reply(from, {:ok, token})
        state

      {:empty, queue} ->
        %{state | queue: queue, available: min(state.available + 1, state.max)}
    end
  end

  defp normalize_positive_integer(value, _default) when is_integer(value) and value > 0,
    do: value

  defp normalize_positive_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> parsed
      _ -> default
    end
  end

  defp normalize_positive_integer(_value, default), do: default
end
