defmodule FerricstoreHttp.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    case FerricstoreHttp.Config.load() do
      {:ok, %{enabled: true} = config} ->
        Supervisor.start_link([{FerricstoreHttp.Server, config}],
          strategy: :one_for_one,
          name: FerricstoreHttp.ApplicationSupervisor
        )

      {:ok, %{enabled: false}} ->
        Supervisor.start_link([],
          strategy: :one_for_one,
          name: FerricstoreHttp.ApplicationSupervisor
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Application
  def prep_stop(state) do
    :ok = FerricstoreHttp.Listener.suspend()
    state
  end
end
