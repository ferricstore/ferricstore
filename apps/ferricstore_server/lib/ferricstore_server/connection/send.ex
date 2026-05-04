defmodule FerricstoreServer.Connection.Send do
  @moduledoc """
  Shared socket send wrapper for connection response paths.

  Transport send failures usually mean the client is gone, but swallowing them
  completely hides broken sockets, stalled subscribers, and invalidation drops.
  Keep the success path thin and emit telemetry only on failure.
  """

  @event [:ferricstore_server, :connection, :send_failed]

  @spec send(term(), module(), iodata(), atom(), map()) :: :ok | {:error, term()}
  def send(socket, transport, iodata, phase, metadata \\ %{}) do
    case transport.send(socket, iodata) do
      :ok ->
        :ok

      {:error, reason} = error ->
        :telemetry.execute(
          @event,
          %{count: 1},
          Map.merge(metadata, %{phase: phase, reason: reason})
        )

        error
    end
  end
end
