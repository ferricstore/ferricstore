defmodule Ferricstore.Transaction.ExecutionEntry do
  @moduledoc false

  alias Ferricstore.Commands.{
    KeyDiscovery,
    NativeAstParser,
    PreparedCommand,
    TransactionPolicy
  }

  alias Ferricstore.Transaction.Ast, as: TxAst

  @enforce_keys [:command, :ast, :routing_scope, :command_keys, :read_keys, :write_keys]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          command: binary(),
          ast: term(),
          routing_scope: :none | :keys,
          command_keys: [binary()],
          read_keys: [binary()],
          write_keys: [binary()]
        }

  @spec from_prepared(PreparedCommand.t()) :: {:ok, t()} | {:error, binary()}
  def from_prepared(%PreparedCommand{
        command: command,
        ast: ast,
        routing_scope: routing_scope,
        command_keys: command_keys,
        read_keys: read_keys,
        write_keys: write_keys,
        transaction_mode: :local
      })
      when routing_scope in [:none, :keys] and is_list(command_keys) and is_list(read_keys) and
             is_list(write_keys) do
    if NativeAstParser.command_matches_ast?(command, ast) and
         TransactionPolicy.safe?(command, ast, routing_scope) do
      {:ok,
       %__MODULE__{
         command: command,
         ast: ast,
         routing_scope: routing_scope,
         command_keys: command_keys,
         read_keys: read_keys,
         write_keys: write_keys
       }}
    else
      TransactionPolicy.error(command)
    end
  end

  def from_prepared(%PreparedCommand{command: command}), do: TransactionPolicy.error(command)

  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{
        command: command,
        ast: ast,
        routing_scope: routing_scope,
        command_keys: command_keys,
        read_keys: read_keys,
        write_keys: write_keys
      }) do
    with true <- valid_keys?(command_keys),
         true <- valid_keys?(read_keys),
         true <- valid_keys?(write_keys) do
      description = KeyDiscovery.describe(command, ast, command_keys)

      NativeAstParser.command_matches_ast?(command, ast) and
        TxAst.derive_command_keys(ast) == command_keys and
        TransactionPolicy.safe?(command, ast, routing_scope) and
        description.routing_scope == routing_scope and
        description.read_keys == read_keys and
        description.write_keys == write_keys and
        description.transaction_mode == :local
    else
      _invalid -> false
    end
  end

  def valid?(_entry), do: false

  @spec mutates?(t()) :: boolean()
  def mutates?(%__MODULE__{write_keys: [_key | _rest]}), do: true
  def mutates?(%__MODULE__{write_keys: []}), do: false

  defp valid_keys?(keys) when is_list(keys), do: Enum.all?(keys, &is_binary/1)
  defp valid_keys?(_keys), do: false
end
