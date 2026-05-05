defmodule Mix.Tasks.Ferricstore.Config do
  @moduledoc """
  Manages FerricStore namespace configuration from the command line.

  Provides `get` and `set` subcommands for reading and writing per-namespace
  configuration (commit window timing).

  ## Usage

      mix ferricstore.config get <prefix>
      mix ferricstore.config set <prefix> <field> <value>

  ## Subcommands

  ### get

  Retrieves the configuration for a namespace prefix. If no explicit override
  has been set, returns the default configuration.

      mix ferricstore.config get rate
      # prefix: rate
      # window_ms: 1

  ### set

  Sets a configuration field for a namespace prefix.

  Valid fields:

    * `window_ms` -- commit window in milliseconds (positive integer)

  Examples:

      mix ferricstore.config set rate window_ms 10

  """

  use Mix.Task

  @shortdoc "Manage FerricStore namespace configuration (get/set)"

  @doc """
  Runs the config task with the given subcommand and arguments.

  ## Parameters

    * `args` -- command-line arguments: `["get", prefix]` or
      `["set", prefix, field, value]`

  """
  @spec run(list()) :: :ok
  @impl Mix.Task
  def run(["get", prefix]) do
    ensure_started()

    case Ferricstore.NamespaceConfig.get(prefix) do
      {:ok, entry} ->
        Mix.shell().info("prefix: #{entry.prefix}")
        Mix.shell().info("window_ms: #{entry.window_ms}")
        Mix.shell().info("changed_at: #{entry.changed_at}")
        Mix.shell().info("changed_by: #{entry.changed_by}")
    end

    :ok
  end

  def run(["set", prefix, field, value]) do
    ensure_started()

    case Ferricstore.NamespaceConfig.set(prefix, field, value) do
      :ok ->
        Mix.shell().info("OK -- #{field} set to #{value} for namespace \"#{prefix}\"")

      {:error, reason} ->
        Mix.shell().info("ERROR: #{reason}")
    end

    :ok
  end

  def run(_args) do
    Mix.shell().info("""
    Usage:
      mix ferricstore.config get <prefix>
      mix ferricstore.config set <prefix> <field> <value>

    Fields: window_ms
    Values: window_ms takes a positive integer
    """)

    :ok
  end

  defp ensure_started do
    Mix.Task.run("app.start")
  end
end
