defmodule Ferricstore.Store.KeydirTableOwner do
  @moduledoc false

  use GenServer

  alias Ferricstore.Store.ETSTableHeir
  alias Ferricstore.Store.Shard.{CompoundMemberIndex, CompoundRevisionIndex, LogicalKeyIndex}

  @heir_retry_ms 10

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    GenServer.start_link(__MODULE__, ctx, name: name(ctx))
  end

  @spec ensure_tables(map()) :: :ok
  def ensure_tables(%{name: _instance_name} = ctx) do
    owner = name(ctx)

    case Process.whereis(owner) do
      nil ->
        ensure_all_tables(ctx, nil)

      _pid ->
        GenServer.call(owner, :ensure_tables)
    end
  rescue
    _ -> ensure_all_tables(ctx, nil)
  catch
    :exit, _ -> ensure_all_tables(ctx, nil)
  end

  @impl true
  def init(ctx) do
    heir_name = table_heir_name(ctx)
    heir = Process.whereis(heir_name) || stable_heir()

    if Process.whereis(heir_name) == heir do
      :ok = ETSTableHeir.claim_tables(heir_name, table_names(ctx))
    end

    :ok = ensure_all_tables(ctx, heir)
    {:ok, monitor_heir(%{ctx: ctx, heir_name: heir_name, heir: heir, heir_monitor: nil}, heir)}
  end

  @impl true
  def handle_call(:ensure_tables, _from, %{ctx: ctx, heir: heir} = state) do
    {:reply, ensure_all_tables(ctx, heir), state}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", _table, _from, _gift}, state), do: {:noreply, state}

  def handle_info(
        {:DOWN, monitor, :process, heir, _reason},
        %{heir_monitor: monitor, heir: heir} = state
      ) do
    schedule_heir_rearm()
    {:noreply, %{state | heir: nil, heir_monitor: nil}}
  end

  def handle_info(:rearm_table_heir, %{ctx: ctx, heir_name: heir_name} = state) do
    case Process.whereis(heir_name) do
      heir when is_pid(heir) ->
        if Process.alive?(heir) do
          case rearm_tables(ctx, heir) do
            :ok -> {:noreply, monitor_heir(state, heir)}
            :retry -> schedule_rearm_and_continue(state)
          end
        else
          schedule_rearm_and_continue(state)
        end

      _missing ->
        schedule_rearm_and_continue(state)
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @spec table_heir_name(map()) :: atom()
  @doc false
  def table_heir_name(%{name: name}) when is_atom(name), do: :"#{name}.KeydirTableHeir"
  def table_heir_name(_ctx), do: :ferricstore_keydir_table_heir

  defp name(%{name: name}) when is_atom(name), do: :"#{name}.KeydirTableOwner"
  defp name(_ctx), do: :ferricstore_keydir_table_owner

  defp ensure_all_tables(%{name: instance_name, keydir_refs: refs}, heir)
       when is_tuple(refs) do
    refs
    |> Tuple.to_list()
    |> Enum.each(&ensure_table(&1, :set, heir))

    refs
    |> tuple_size()
    |> then(fn shard_count ->
      if shard_count > 0 do
        Enum.each(0..(shard_count - 1), fn shard_index ->
          instance_name
          |> CompoundMemberIndex.table_name(shard_index)
          |> ensure_table(:ordered_set, heir)

          instance_name
          |> CompoundRevisionIndex.table_name(shard_index)
          |> ensure_revision_table(heir)

          {logical_keys, logical_slots} =
            LogicalKeyIndex.table_names(instance_name, shard_index)

          ensure_table(logical_keys, :ordered_set, heir)
          ensure_table(logical_slots, :set, heir)
          LogicalKeyIndex.ensure_tables!(logical_keys, logical_slots)
        end)
      end
    end)

    :ok
  end

  defp ensure_all_tables(_ctx, _heir), do: :ok

  defp ensure_table(name, type, heir)
       when is_atom(name) and type in [:set, :ordered_set] do
    case :ets.whereis(name) do
      :undefined ->
        try do
          :ets.new(name, table_options(type, heir))
        rescue
          ArgumentError -> :ok
        end

      tid ->
        set_heir_if_owner(tid, heir)
        :ok
    end
  end

  defp ensure_table(_name, _type, _heir), do: :ok

  defp ensure_revision_table(name, heir) when is_atom(name) do
    ensure_table(name, :set, heir)
    _ = CompoundRevisionIndex.ensure_table!(name)
    :ok
  end

  defp table_options(type, heir) do
    options = [
      type,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, :auto},
      {:decentralized_counters, true}
    ]

    if is_pid(heir), do: [{:heir, heir, __MODULE__} | options], else: options
  end

  defp set_heir_if_owner(table, heir) when is_pid(heir) do
    if :ets.info(table, :owner) == self() do
      :ets.setopts(table, {:heir, heir, __MODULE__})
    end
  end

  defp set_heir_if_owner(_table, _heir), do: :ok

  defp table_names(%{name: instance_name, keydir_refs: refs}) when is_tuple(refs) do
    shard_tables =
      refs
      |> tuple_size()
      |> then(fn
        0 ->
          []

        shard_count ->
          Enum.flat_map(0..(shard_count - 1), fn shard_index ->
            {logical_keys, logical_slots} =
              LogicalKeyIndex.table_names(instance_name, shard_index)

            [
              CompoundMemberIndex.table_name(instance_name, shard_index),
              CompoundRevisionIndex.table_name(instance_name, shard_index),
              logical_keys,
              logical_slots
            ]
          end)
      end)

    Tuple.to_list(refs) ++ shard_tables
  end

  defp table_names(_ctx), do: []

  defp rearm_tables(ctx, heir) do
    ensure_all_tables(ctx, heir)
  rescue
    ArgumentError -> :retry
  end

  defp monitor_heir(%{heir_monitor: monitor} = state, heir) when is_pid(heir) do
    if is_reference(monitor), do: Process.demonitor(monitor, [:flush])
    %{state | heir: heir, heir_monitor: Process.monitor(heir)}
  end

  defp monitor_heir(state, _heir), do: state

  defp schedule_rearm_and_continue(state) do
    schedule_heir_rearm()
    {:noreply, state}
  end

  defp schedule_heir_rearm do
    Process.send_after(self(), :rearm_table_heir, @heir_retry_ms)
  end

  defp stable_heir do
    Process.get(:"$ancestors", [])
    |> Enum.find_value(fn
      pid when is_pid(pid) -> pid
      name when is_atom(name) -> Process.whereis(name)
      _other -> nil
    end)
  end
end
