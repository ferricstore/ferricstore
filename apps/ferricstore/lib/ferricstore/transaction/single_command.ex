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
  # MULTI. A command whose handler writes differently below a feature level
  # inside MULTI (`Ops.level_5_writes?/1`) passes `level:`, and only runs this
  # way once that level is active, where the transaction writes as the
  # instance does.

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.ErrorReasons
  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.{Coordinator, ReplicaCompat}

  @spec run(term(), binary(), [binary()], keyword()) :: term() | :fallback
  def run(store, command, args, opts \\ [])

  def run(%FerricStore.Instance{name: :default} = ctx, command, args, opts) do
    with {:ok, prepared} <- PreparedCommand.prepare(command, args),
         # A command transactions refuse runs on its own path.
         :local <- prepared.transaction_mode,
         true <- one_shard?(ctx, prepared.command_keys),
         true <- level_ready?(ctx, prepared.command_keys, Keyword.get(opts, :level)),
         nil <- ReplicaCompat.queue_error(command, prepared.ast, prepared.command_keys, ctx) do
      case Coordinator.execute([prepared], %{}, nil) do
        [result] ->
          result

        # The entry was refused before it changed anything, as when the
        # command's work is more than one transaction entry may do; the
        # command's own path may still take it.
        {:error, "ERR transaction raft unavailable: " <> _reason} ->
          :fallback

        # The entry may have applied, and running the command again could
        # apply it twice (BITOP XOR into one of its sources undoes itself):
        # answer as a timed-out write does.
        {:error, "ERR transaction outcome unknown: " <> _reason} ->
          ErrorReasons.write_timeout_unknown()

        {:error, _reason} = error ->
          error

        other ->
          {:error, {:unexpected_transaction_result, other}}
      end
    else
      _not_here -> :fallback
    end
  end

  def run(_store, _command, _args, _opts), do: :fallback

  defp level_ready?(_ctx, _keys, nil), do: true
  defp level_ready?(ctx, keys, level), do: ReplicaCompat.level_active?(ctx, keys, level)

  defp one_shard?(ctx, [key | keys]),
    do: Enum.all?(keys, &(Router.shard_for(ctx, &1) == Router.shard_for(ctx, key)))

  defp one_shard?(_ctx, []), do: false
end
