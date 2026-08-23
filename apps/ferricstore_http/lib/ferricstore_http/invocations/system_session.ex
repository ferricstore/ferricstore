defmodule FerricstoreHttp.Invocations.SystemSession do
  @moduledoc "Owns the authenticated backend session used by internal invocation workers."

  use GenServer

  alias FerricstoreHttp.{Auth, Config}

  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config),
    do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  @spec context() :: {:ok, Auth.Context.t()} | {:error, term()}
  def context, do: GenServer.call(__MODULE__, :context)

  @spec refresh() :: {:ok, Auth.Context.t()} | {:error, term()}
  def refresh, do: GenServer.call(__MODULE__, :refresh)

  @impl GenServer
  def init(%Config{} = config), do: {:ok, %{config: config, context: nil}}

  @impl GenServer
  def handle_call(:context, _from, %{context: %Auth.Context{} = context} = state),
    do: {:reply, {:ok, context}, state}

  def handle_call(action, _from, state) when action in [:context, :refresh] do
    case authenticate(state.config) do
      {:ok, context} -> {:reply, {:ok, context}, %{state | context: context}}
      {:error, _reason} = error -> {:reply, error, %{state | context: nil}}
    end
  end

  defp authenticate(%Config{} = config) do
    with {:ok, username} <- credential(config.runner_username_env),
         {:ok, password} <- credential(config.runner_password_env),
         {:ok, session} <-
           config.backend.authenticate(username, password, peer: {:internal, config.runner_id}) do
      {:ok, %Auth.Context{session: session, subject: username, scopes: ["invocations:runner"]}}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :runner_authentication_failed}
    end
  end

  defp credential(env_name) do
    case System.get_env(env_name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, :runner_credentials_missing}
    end
  end
end
