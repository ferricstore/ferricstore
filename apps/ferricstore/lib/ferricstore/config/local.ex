defmodule Ferricstore.Config.Local do
  @moduledoc """
  ETS-backed storage for node-local configuration parameters.

  Node-local settings are NOT replicated via Raft and are NOT persisted
  by `CONFIG REWRITE`. They are intentionally ephemeral -- on restart,
  all local settings are lost.

  ## Supported parameters

    * `"log_level"` -- maps to `Logger.configure(level: level)`.
      Valid values: `"debug"`, `"info"`, `"notice"`, `"warning"`, `"error"`.

  ## Usage

      Ferricstore.Config.Local.set("log_level", "debug")
      #=> :ok

      Ferricstore.Config.Local.get("log_level")
      #=> {:ok, "debug"}

      Ferricstore.Config.Local.get_all()
      #=> %{"log_level" => "debug"}
  """

  require Logger
  use GenServer

  @table :ferricstore_config_local

  @valid_log_levels MapSet.new(["debug", "info", "notice", "warning", "error"])

  @known_local_params MapSet.new(["log_level"])

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts \\ []) do
    case GenServer.start_link(__MODULE__, [], name: __MODULE__) do
      {:error, {:already_started, pid}} ->
        Process.link(pid)
        {:ok, pid}

      result ->
        result
    end
  end

  # -------------------------------------------------------------------
  # Types
  # -------------------------------------------------------------------

  @typedoc "A local configuration parameter name."
  @type param_name :: binary()

  @typedoc "A local configuration parameter value."
  @type param_value :: binary()

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Ensures the local config ETS table exists.

  Called during application startup. Safe to call multiple times.
  """
  @spec ensure_table() :: :ok
  def ensure_table do
    case Process.whereis(__MODULE__) do
      nil ->
        case GenServer.start(__MODULE__, [], name: __MODULE__) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
        end

      _pid ->
        :ok
    end
  end

  @doc """
  Sets a node-local configuration parameter.

  Validates the parameter name and value. Applies the corresponding
  side-effect (e.g. configuring the Logger level).

  ## Parameters

    - `key` -- parameter name (e.g. `"log_level"`)
    - `value` -- new value as a string

  ## Returns

    - `:ok` on success
    - `{:error, reason}` when the parameter is unknown or the value is invalid

  ## Examples

      iex> Ferricstore.Config.Local.set("log_level", "debug")
      :ok

      iex> Ferricstore.Config.Local.set("bogus", "value")
      {:error, "ERR Unsupported local CONFIG parameter: bogus"}
  """
  @spec set(param_name(), param_value()) :: :ok | {:error, binary()}
  def set(key, value) when is_binary(key) and is_binary(value) do
    if MapSet.member?(@known_local_params, key) do
      case validate_local(key, value) do
        :ok ->
          ensure_table()
          GenServer.call(__MODULE__, {:set, key, value})

        {:error, _} = err ->
          err
      end
    else
      {:error, "ERR Unsupported local CONFIG parameter: #{key}"}
    end
  end

  @doc """
  Returns the current value of a node-local configuration parameter.

  If the parameter has not been explicitly set via `CONFIG SET LOCAL`,
  returns the effective current value from the runtime (e.g. the current
  Logger level).

  ## Parameters

    - `key` -- parameter name

  ## Returns

    - `{:ok, value}` when the parameter is known
    - `{:error, reason}` when the parameter is unknown

  ## Examples

      iex> Ferricstore.Config.Local.get("log_level")
      {:ok, "warning"}
  """
  @spec get(param_name()) :: {:ok, param_value()} | {:error, binary()}
  def get(key) when is_binary(key) do
    if MapSet.member?(@known_local_params, key) do
      ensure_table()

      case :ets.lookup(@table, key) do
        [{^key, value}] -> {:ok, value}
        [] -> {:ok, read_current_value(key)}
      end
    else
      {:error, "ERR Unsupported local CONFIG parameter: #{key}"}
    end
  end

  @doc """
  Returns all explicitly set local configuration parameters as a map.

  Parameters that have not been explicitly set are not included.

  ## Returns

  A map of `%{param_name => param_value}`.
  """
  @spec get_all() :: %{param_name() => param_value()}
  def get_all do
    ensure_table()

    @table
    |> :ets.tab2list()
    |> Map.new(fn {k, v} -> {k, v} end)
  rescue
    ArgumentError -> %{}
  end

  @doc """
  Clears all local configuration settings.

  Does NOT revert side-effects (e.g. Logger level stays at whatever it
  was last set to). The next `get/1` call will read the live value.
  """
  @spec reset_all() :: :ok
  def reset_all do
    ensure_table()
    GenServer.call(__MODULE__, :reset_all)
  end

  @impl true
  def init([]) do
    table =
      :ets.new(@table, [
        :set,
        :protected,
        :named_table,
        {:read_concurrency, true}
      ])

    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:set, key, value}, _from, state) do
    apply_local_side_effect(key, value)
    true = :ets.insert(@table, {key, value})
    {:reply, :ok, state}
  end

  def handle_call(:reset_all, _from, state) do
    true = :ets.delete_all_objects(@table)
    {:reply, :ok, state}
  end

  # -------------------------------------------------------------------
  # Private -- validation
  # -------------------------------------------------------------------

  defp validate_local("log_level", value) do
    if MapSet.member?(@valid_log_levels, value) do
      :ok
    else
      {:error,
       "ERR Invalid argument '#{value}' for CONFIG SET LOCAL 'log_level'. " <>
         "Valid values: debug, info, notice, warning, error"}
    end
  end

  defp validate_local(_key, _value), do: :ok

  # -------------------------------------------------------------------
  # Private -- side effects
  # -------------------------------------------------------------------

  defp apply_local_side_effect("log_level", value) do
    level = String.to_existing_atom(value)
    Logger.configure(level: level)
  end

  defp apply_local_side_effect(_key, _value), do: :ok

  # -------------------------------------------------------------------
  # Private -- read current effective value
  # -------------------------------------------------------------------

  defp read_current_value("log_level") do
    Logger.level() |> Atom.to_string()
  end

  defp read_current_value(_key), do: ""
end
