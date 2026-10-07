defmodule Ferricstore.Test.WARaftPeerContextOwner do
  @moduledoc false
  use GenServer

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance_name)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    ctx =
      FerricStore.Instance.build(
        Keyword.fetch!(opts, :instance_name),
        Keyword.fetch!(opts, :instance_opts)
      )

    {:ok, ctx}
  end

  @impl true
  def handle_call(:context, _from, ctx), do: {:reply, ctx, ctx}

  @impl true
  def handle_info({:EXIT, port, :normal}, ctx) when is_port(port), do: {:noreply, ctx}

  @impl true
  def terminate(_reason, ctx), do: FerricStore.Instance.cleanup(ctx.name)
end
