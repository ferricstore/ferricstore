defmodule Ferricstore.Cluster.Relay do
  @moduledoc """
  Carries node-local notifications to the other members of the cluster.

  Some state lives in each node's own registry: Pub/Sub subscriptions
  (`Ferricstore.PubSub`) and blocked Flow claims (`Ferricstore.Flow.ClaimWaiters`).
  A PUBLISH through one node reached only that node's subscribers, and a Flow
  written through one node woke only that node's blocked claims, so a claim
  blocked on another node slept until its block ended (a native subscription,
  which has no end, could wait for good).

  The node that publishes, or that wrote a Flow, now also sends the publish or
  the wake-up to this process on every other member, which delivers it to its
  own registry and forwards nothing. PUBLISH answers the number of subscribers
  on the node that ran it, as Redis Cluster does. Delivery stays at-most-once:
  nothing is resent to a member that was not connected. A subscriber gets one
  publishing process's messages in the order they were published. A wake-up
  only makes a blocked claim try again, so a spare one costs a claim attempt.

  The members are the voters of the default instance's shards, as this node
  caches them (`Ferricstore.Raft.WARaftBackend.cached_members/1`), read again
  every second.
  """

  use GenServer

  require Logger

  @peers_key {__MODULE__, :peers}
  @refresh_ms 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  # Sends Pub/Sub `publishes` to every other member.
  @spec forward_publishes([{binary(), binary()}]) :: :ok
  def forward_publishes([]), do: :ok
  def forward_publishes(publishes) when is_list(publishes), do: forward({:publish, publishes})

  @doc false
  # Sends shard channel publishes (SPUBLISH) to every other member. A member
  # on an older release drops them, and has no shard subscribers either.
  @spec forward_shard_publishes([{binary(), binary()}]) :: :ok
  def forward_shard_publishes([]), do: :ok

  def forward_shard_publishes(publishes) when is_list(publishes),
    do: forward({:spublish, publishes})

  @doc false
  # Sends Flow claim wake-up `hints` (see `Ferricstore.Flow`) to every other member.
  @spec forward_claim_hints(list()) :: :ok
  def forward_claim_hints([]), do: :ok
  def forward_claim_hints(hints) when is_list(hints), do: forward({:claim_hints, hints})

  defp forward(message) do
    case :persistent_term.get(@peers_key, []) do
      [] ->
        :ok

      peers ->
        message = {:relay, message}
        Enum.each(peers, &:erlang.send({__MODULE__, &1}, message, [:noconnect]))
    end
  end

  @doc false
  @spec peers() :: [node()]
  def peers, do: :persistent_term.get(@peers_key, [])

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    {:ok, %{shard_count: ctx.shard_count}, {:continue, :refresh}}
  end

  @impl true
  def handle_continue(:refresh, state) do
    refresh_peers(state.shard_count)
    Process.send_after(self(), :refresh, @refresh_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info({:relay, message}, state) do
    deliver(message)
    {:noreply, state}
  rescue
    error ->
      Logger.warning("relay could not deliver: " <> Exception.message(error))
      {:noreply, state}
  end

  def handle_info(:refresh, state), do: handle_continue(:refresh, state)

  def handle_info(_message, state), do: {:noreply, state}

  # A member on another release may send what this one cannot deliver; that
  # must not stop the relay.
  defp deliver({:publish, [{channel, message}]}) when is_binary(channel) and is_binary(message),
    do: Ferricstore.PubSub.publish_local(channel, message)

  defp deliver({:publish, publishes}) when is_list(publishes) do
    if Enum.all?(
         publishes,
         &match?({channel, message} when is_binary(channel) and is_binary(message), &1)
       ),
       do: Ferricstore.PubSub.publish_many_local(publishes, nil)
  end

  defp deliver({:spublish, publishes}) when is_list(publishes) do
    for {channel, message} when is_binary(channel) and is_binary(message) <- publishes,
        do: Ferricstore.PubSub.spublish_local(channel, message)

    :ok
  end

  defp deliver({:claim_hints, hints}) when is_list(hints),
    do: Ferricstore.Flow.wake_relayed_claim_waiters(hints)

  defp deliver(_unknown), do: :ok

  @impl true
  def terminate(_reason, _state) do
    :persistent_term.erase(@peers_key)
    :ok
  end

  defp refresh_peers(shard_count) do
    peers =
      0..(shard_count - 1)//1
      |> Enum.flat_map(fn shard_index ->
        case Ferricstore.Raft.WARaftBackend.cached_members(shard_index) do
          {:ok, members, _leader} -> Enum.map(members, fn {_server, member} -> member end)
          {:error, _reason} -> []
        end
      end)
      |> Enum.uniq()
      |> List.delete(node())
      |> Enum.sort()

    if :persistent_term.get(@peers_key, []) != peers,
      do: :persistent_term.put(@peers_key, peers)
  rescue
    _not_started -> :ok
  end
end
