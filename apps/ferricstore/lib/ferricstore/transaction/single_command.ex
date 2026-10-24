defmodule Ferricstore.Transaction.SingleCommand do
  @moduledoc false

  # Runs one command as a transaction entry, which the key's shard applies in
  # its state machine as it applies MULTI: the command's reads and writes run
  # there with nothing in between. A command that reads a value and writes it
  # back from the client process loses a write made in between; run this way
  # it cannot.
  #
  # Returns :fallback where it cannot run that way: an instance other than the
  # default one (the transaction coordinator serves the default instance), keys
  # on more than one shard (one entry belongs to one shard), or a form that
  # releases before the current feature level would apply differently inside
  # MULTI.

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.{Coordinator, ReplicaCompat}

  @spec run(term(), binary(), [binary()]) :: term() | :fallback
  def run(%FerricStore.Instance{name: :default} = ctx, command, args) do
    with {:ok, prepared} <- PreparedCommand.prepare(command, args),
         true <- one_shard?(ctx, prepared.command_keys),
         nil <- ReplicaCompat.queue_error(command, prepared.ast, prepared.command_keys, ctx) do
      case Coordinator.execute([prepared], %{}, nil) do
        [result] -> result
        {:error, _reason} = error -> error
        other -> {:error, {:unexpected_transaction_result, other}}
      end
    else
      _not_here -> :fallback
    end
  end

  def run(_store, _command, _args), do: :fallback

  defp one_shard?(ctx, [key | keys]),
    do: Enum.all?(keys, &(Router.shard_for(ctx, &1) == Router.shard_for(ctx, key)))

  defp one_shard?(_ctx, []), do: false
end
