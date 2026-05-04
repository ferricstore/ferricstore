defmodule Ferricstore.Commands.PubSub do
  @moduledoc """
  Handles Redis pub/sub commands that go through the normal dispatcher:
  PUBLISH, and PUBSUB (with subcommands CHANNELS, NUMSUB, NUMPAT).

  The SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, and PUNSUBSCRIBE commands are
  handled directly in `Ferricstore.Server.Connection` because they require
  per-connection state management (pub/sub mode tracking, subscription counts).
  """

  alias Ferricstore.PubSub

  @doc """
  Handles a PUBLISH or PUBSUB command.

  ## PUBLISH channel message

  Publishes `message` to the given `channel`. Returns the number of subscribers
  that received the message (integer).

  ## PUBSUB CHANNELS [pattern]

  Lists channels with active subscribers. When `pattern` is given, only
  channels matching the glob pattern are returned.

  ## PUBSUB NUMSUB [channel ...]

  Returns a flat list of `[channel, count, channel, count, ...]` with
  subscriber counts for each specified channel.

  ## PUBSUB NUMPAT

  Returns the total number of active pattern subscriptions (from PSUBSCRIBE).

  ## Parameters

    - `cmd`  - Uppercased command name (`"PUBLISH"` or `"PUBSUB"`)
    - `args` - List of string arguments

  ## Returns

  Plain Elixir terms suitable for RESP encoding.
  """
  @spec handle(binary(), [binary()]) :: term()
  def handle(cmd, args)

  # PUBLISH channel message
  def handle("PUBLISH", [channel, message]), do: publish_message(channel, message)

  def handle("PUBLISH", _args) do
    {:error, "ERR wrong number of arguments for 'publish' command"}
  end

  # PUBSUB subcommand dispatch
  def handle("PUBSUB", [subcommand | args]) do
    case String.upcase(subcommand) do
      "CHANNELS" -> handle_channels(args)
      "NUMSUB" -> handle_numsub(args)
      "NUMPAT" -> handle_numpat(args)
      other -> {:error, "ERR unknown subcommand '#{String.downcase(other)}'. Try PUBSUB HELP."}
    end
  end

  def handle("PUBSUB", []) do
    {:error, "ERR wrong number of arguments for 'pubsub' command"}
  end

  @spec handle_ast(term()) :: term()
  def handle_ast({:publish, [channel, message]}), do: publish_message(channel, message)

  def handle_ast({:publish, _args}),
    do: {:error, "ERR wrong number of arguments for 'publish' command"}

  def handle_ast({:pubsub, ["CHANNELS" | args]}), do: handle_channels(args)
  def handle_ast({:pubsub, ["NUMSUB" | args]}), do: handle_numsub(args)
  def handle_ast({:pubsub, ["NUMPAT" | args]}), do: handle_numpat(args)

  def handle_ast({:pubsub, []}),
    do: {:error, "ERR wrong number of arguments for 'pubsub' command"}

  def handle_ast({:pubsub, [other | _rest]}) do
    {:error, "ERR unknown subcommand '#{String.downcase(other)}'. Try PUBSUB HELP."}
  end

  # ---------------------------------------------------------------------------
  # PUBSUB subcommand handlers
  # ---------------------------------------------------------------------------

  defp publish_message(channel, message), do: PubSub.publish(channel, message)

  defp handle_channels([]), do: PubSub.channels()
  defp handle_channels([pattern]), do: PubSub.channels(pattern)

  defp handle_channels(_args) do
    {:error, "ERR wrong number of arguments for 'pubsub|channels' command"}
  end

  defp handle_numsub(channels), do: PubSub.numsub(channels)

  defp handle_numpat([]), do: PubSub.numpat()

  defp handle_numpat(_args) do
    {:error, "ERR wrong number of arguments for 'pubsub|numpat' command"}
  end
end
