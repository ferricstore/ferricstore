defmodule Ferricstore.Commands.PubSub do
  @moduledoc """
  Handles Redis pub/sub commands that go through the normal dispatcher:
  PUBLISH, SPUBLISH, and PUBSUB (with subcommands CHANNELS, NUMSUB, NUMPAT,
  SHARDCHANNELS, SHARDNUMSUB).

  The SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE and
  SUNSUBSCRIBE commands are handled by the native protocol session
  (`FerricstoreServer.Native.Session`) because they need per-connection
  state.
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

  ## SPUBLISH shardchannel message

  Publishes `message` to the subscribers of a shard channel (SSUBSCRIBE).
  Shard channels are their own namespace, as in Redis: PUBLISH and pattern
  subscriptions never meet them.

  ## PUBSUB SHARDCHANNELS [pattern] / PUBSUB SHARDNUMSUB [shardchannel ...]

  CHANNELS and NUMSUB for shard channels.

  ## Parameters

    - `cmd`  - Uppercased command name (`"PUBLISH"` or `"PUBSUB"`)
    - `args` - List of string arguments

  ## Returns

  Plain Elixir terms suitable for wire encoding.
  """
  @spec handle(binary(), [binary()]) :: term()
  def handle(cmd, args)

  # PUBLISH channel message
  def handle("PUBLISH", [channel, message]), do: publish_message(channel, message)

  def handle("PUBLISH", _args) do
    {:error, "ERR wrong number of arguments for 'publish' command"}
  end

  def handle("SPUBLISH", [channel, message]), do: PubSub.spublish(channel, message)

  def handle("SPUBLISH", _args) do
    {:error, "ERR wrong number of arguments for 'spublish' command"}
  end

  # PUBSUB subcommand dispatch
  def handle("PUBSUB", args) when is_list(args), do: handle_pubsub(args)

  @spec handle_ast(term()) :: term()
  def handle_ast({:publish, [channel, message]}), do: publish_message(channel, message)

  def handle_ast({:publish, _args}),
    do: {:error, "ERR wrong number of arguments for 'publish' command"}

  def handle_ast({:spublish, [channel, message]}), do: PubSub.spublish(channel, message)

  def handle_ast({:spublish, _args}),
    do: {:error, "ERR wrong number of arguments for 'spublish' command"}

  def handle_ast({:pubsub, args}) when is_list(args), do: handle_pubsub(args)

  # ---------------------------------------------------------------------------
  # PUBSUB subcommand handlers
  # ---------------------------------------------------------------------------

  defp handle_pubsub([subcommand | args]) do
    case String.upcase(subcommand) do
      "CHANNELS" -> handle_channels(args)
      "NUMSUB" -> handle_numsub(args)
      "NUMPAT" -> handle_numpat(args)
      "SHARDCHANNELS" -> handle_shard_channels(args)
      "SHARDNUMSUB" -> PubSub.shard_numsub(args)
      other -> {:error, "ERR unknown subcommand '#{String.downcase(other)}'. Try PUBSUB HELP."}
    end
  end

  defp handle_pubsub([]) do
    {:error, "ERR wrong number of arguments for 'pubsub' command"}
  end

  defp publish_message(channel, message), do: PubSub.publish(channel, message)

  defp handle_channels([]), do: PubSub.channels()
  defp handle_channels([pattern]), do: PubSub.channels(pattern)

  defp handle_channels(_args) do
    {:error, "ERR wrong number of arguments for 'pubsub|channels' command"}
  end

  defp handle_numsub(channels), do: PubSub.numsub(channels)

  defp handle_shard_channels([]), do: PubSub.shard_channels()
  defp handle_shard_channels([pattern]), do: PubSub.shard_channels(pattern)

  defp handle_shard_channels(_args) do
    {:error, "ERR wrong number of arguments for 'pubsub|shardchannels' command"}
  end

  defp handle_numpat([]), do: PubSub.numpat()

  defp handle_numpat(_args) do
    {:error, "ERR wrong number of arguments for 'pubsub|numpat' command"}
  end
end
