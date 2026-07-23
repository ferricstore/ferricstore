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
    with_acquired_permit(instance_name, nil, fun)
  end

  def with_shard_permit(instance_name, shard_index, fun)
      when is_integer(shard_index) and shard_index >= 0 and is_function(fun, 0) do
    with_acquired_permit(instance_name, {:shard, instance_name, shard_index}, fun)
  end

  defp with_acquired_permit(instance_name, scope, fun) do
    case coordinator_pid(instance_name) do
      nil ->
        fun.()

      pid ->
        case acquire(pid, scope) do
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

    {:ok,
     %{
       max: max_concurrent,
       available: max_concurrent,
       queue: :queue.new(),
       holders: %{},
       active_scopes: MapSet.new()
     }}
  end

  @impl true
  def handle_call({:acquire, scope}, from, state) do
    state = %{state | queue: :queue.in({from, scope}, state.queue)}
    {:noreply, grant_next(state)}
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

  defp acquire(pid, scope) do
    GenServer.call(pid, {:acquire, scope}, :infinity)
  catch
    :exit, _reason -> :unavailable
  end

  defp grant(pid, scope, state) do
    token = Process.monitor(pid)

    {token,
     %{
       state
       | available: state.available - 1,
         holders: Map.put(state.holders, token, {pid, scope}),
         active_scopes: put_active_scope(state.active_scopes, scope)
     }}
  end

  defp release_token(state, token) do
    case Map.pop(state.holders, token) do
      {nil, holders} ->
        %{state | holders: holders}

      {{_pid, scope}, holders} ->
        Process.demonitor(token, [:flush])

        state
        |> Map.put(:holders, holders)
        |> Map.update!(:available, &min(&1 + 1, state.max))
        |> Map.update!(:active_scopes, &delete_active_scope(&1, scope))
        |> grant_next()
    end
  end

  defp put_active_scope(scopes, nil), do: scopes
  defp put_active_scope(scopes, scope), do: MapSet.put(scopes, scope)

  defp delete_active_scope(scopes, nil), do: scopes
  defp delete_active_scope(scopes, scope), do: MapSet.delete(scopes, scope)

  defp grant_next(state) do
    case pop_grantable(state) do
      {:ok, {{pid, _tag} = from, scope}, queue} ->
        {token, state} = grant(pid, scope, %{state | queue: queue})
        GenServer.reply(from, {:ok, token})
        grant_next(state)

      :none ->
        state
    end
  end

  defp pop_grantable(%{available: available}) when available <= 0, do: :none

  defp pop_grantable(state) do
    pop_grantable(
      state.queue,
      :queue.new(),
      state.active_scopes,
      :queue.len(state.queue)
    )
  end

  defp pop_grantable(_queue, _blocked, _active_scopes, 0), do: :none

  defp pop_grantable(queue, blocked, active_scopes, remaining) do
    case :queue.out(queue) do
      {{:value, {_from, scope} = entry}, rest} ->
        if scope != nil and MapSet.member?(active_scopes, scope) do
          pop_grantable(rest, :queue.in(entry, blocked), active_scopes, remaining - 1)
        else
          {:ok, entry, :queue.join(blocked, rest)}
        end

      {:empty, _queue} ->
        :none
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
