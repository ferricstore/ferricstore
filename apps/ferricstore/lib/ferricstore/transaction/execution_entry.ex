defmodule Ferricstore.Transaction.ExecutionEntry do
  @moduledoc false

  alias Ferricstore.Commands.{PreparedCommand, TransactionPolicy}

  @enforce_keys [:command, :ast, :routing_scope]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          command: binary(),
          ast: term(),
          routing_scope: :none | :keys
        }

  @spec from_prepared(PreparedCommand.t()) :: {:ok, t()} | {:error, binary()}
  def from_prepared(%PreparedCommand{
        command: command,
        ast: ast,
        routing_scope: routing_scope,
        transaction_mode: :local
      })
      when routing_scope in [:none, :keys] do
    if TransactionPolicy.safe?(command, ast, routing_scope) do
      {:ok, %__MODULE__{command: command, ast: ast, routing_scope: routing_scope}}
    else
      TransactionPolicy.error(command)
    end
  end

  def from_prepared(%PreparedCommand{command: command}), do: TransactionPolicy.error(command)

  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{command: command, ast: ast, routing_scope: routing_scope}) do
    TransactionPolicy.safe?(command, ast, routing_scope)
  end

  def valid?(_entry), do: false
end
