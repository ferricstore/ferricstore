defmodule Ferricstore.Store.KeydirTableOwner do
  @moduledoc false

  use GenServer

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
        ensure_all_tables(ctx)

      _pid ->
        GenServer.call(owner, :ensure_tables)
    end
  rescue
    _ -> ensure_all_tables(ctx)
  catch
    :exit, _ -> ensure_all_tables(ctx)
  end

  @impl true
  def init(ctx) do
    :ok = ensure_all_tables(ctx)
    {:ok, ctx}
  end

  @impl true
  def handle_call(:ensure_tables, _from, ctx) do
    {:reply, ensure_all_tables(ctx), ctx}
  end

  defp name(%{name: name}) when is_atom(name), do: :"#{name}.KeydirTableOwner"
  defp name(_ctx), do: :ferricstore_keydir_table_owner

  defp ensure_all_tables(%{keydir_refs: refs}) when is_tuple(refs) do
    refs
    |> Tuple.to_list()
    |> Enum.each(&ensure_table/1)

    :ok
  end

  defp ensure_all_tables(_ctx), do: :ok

  defp ensure_table(name) when is_atom(name) do
    case :ets.whereis(name) do
      :undefined ->
        try do
          :ets.new(name, table_options())
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end
  end

  defp ensure_table(_name), do: :ok

  defp table_options do
    [
      :set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, :auto},
      {:decentralized_counters, true}
    ]
  end
end
