defmodule Ferricstore.PubSub do
  @moduledoc """
  ETS-based Pub/Sub registry for FerricStore.

  Provides a fire-and-forget, at-most-once messaging layer implemented entirely
  on the BEAM — no Raft consensus, no Bitcask persistence. Subscribers register
  their connection pid and receive messages as plain BEAM messages.

  ## Architecture

  Two ETS tables back the registry:

    * `:ferricstore_pubsub` — `{channel, pid}` entries for exact channel
      subscriptions. Uses a `:bag` so multiple pids can subscribe to the same
      channel while duplicate subscriptions from the same pid remain collapsed.

    * `:ferricstore_pubsub_patterns` — `{pattern, pid, compiled_regex}` entries
      for glob-pattern subscriptions (PSUBSCRIBE). Also a `:bag`.

  Both tables are owned by a `GenServer` (`Ferricstore.PubSub`) so they survive
  the lifetime of the application and are cleaned up on shutdown.

  Subscriber pids are monitored once by the owner process. If a connection dies
  before running its normal cleanup path, the monitor removes its channel and
  pattern entries so publish counts and PUBSUB introspection do not retain stale
  subscribers.

  ## Message protocol

  When a message is published to a channel, each matching subscriber pid receives
  one of:

    * `{:pubsub_message, channel, message}` — for exact channel subscriptions
    * `{:pubsub_pmessage, pattern, channel, message}` — for pattern subscriptions

  The connection process is responsible for encoding these into RESP3 push frames.
  """

  use GenServer

  @channels_table :ferricstore_pubsub
  @patterns_table :ferricstore_pubsub_patterns
  @monitors_table :ferricstore_pubsub_monitors

  @type channel :: binary()
  @type pattern :: binary()

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the PubSub registry GenServer.

  Creates the ETS tables `:ferricstore_pubsub` and
  `:ferricstore_pubsub_patterns`. Should be added to the application
  supervision tree before the Ranch listener.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Subscribes `pid` to the given `channel`.

  The subscription is idempotent — calling it twice with the same pid and
  channel keeps a single registry entry.

  ## Parameters

    - `channel` - The channel name (binary).
    - `pid`     - The subscriber process id.

  ## Returns

  `:ok`
  """
  @spec subscribe(channel(), pid()) :: :ok
  def subscribe(channel, pid) when is_binary(channel) and is_pid(pid) do
    :ets.insert(@channels_table, {channel, pid})
    ensure_monitor(pid)
    :ok
  end

  @doc """
  Unsubscribes `pid` from the given `channel`.

  Removes the `{channel, pid}` entry from the ETS table. If the pid was not
  subscribed, this is a no-op.

  ## Parameters

    - `channel` - The channel name (binary).
    - `pid`     - The subscriber process id.

  ## Returns

  `:ok`
  """
  @spec unsubscribe(channel(), pid()) :: :ok
  def unsubscribe(channel, pid) when is_binary(channel) and is_pid(pid) do
    :ets.match_delete(@channels_table, {channel, pid})
    maybe_demonitor(pid)
    :ok
  end

  @doc """
  Subscribes `pid` to all channels matching `pattern` (glob syntax).

  The glob pattern is compiled to a regex and stored alongside the entry
  for efficient matching at publish time.

  ## Parameters

    - `pattern` - A glob pattern (e.g. `"news.*"`, `"user:?"`).
    - `pid`     - The subscriber process id.

  ## Returns

  `:ok`
  """
  @spec psubscribe(pattern(), pid()) :: :ok
  def psubscribe(pattern, pid) when is_binary(pattern) and is_pid(pid) do
    if :ets.match(@patterns_table, {pattern, pid, :_}) == [] do
      regex = glob_to_regex(pattern)
      :ets.insert(@patterns_table, {pattern, pid, regex})
      ensure_monitor(pid)
    end

    :ok
  end

  @doc """
  Unsubscribes `pid` from the given glob `pattern`.

  Removes all entries matching `{pattern, pid, _}` from the patterns table.

  ## Parameters

    - `pattern` - The glob pattern (binary).
    - `pid`     - The subscriber process id.

  ## Returns

  `:ok`
  """
  @spec punsubscribe(pattern(), pid()) :: :ok
  def punsubscribe(pattern, pid) when is_binary(pattern) and is_pid(pid) do
    # match_delete with a wildcard for the compiled regex
    :ets.match_delete(@patterns_table, {pattern, pid, :_})
    maybe_demonitor(pid)
    :ok
  end

  @doc """
  Publishes `message` to all subscribers of `channel`.

  Looks up exact channel subscribers and pattern subscribers whose compiled
  regex matches the channel name. Sends a BEAM message to each matching pid.

  ## Parameters

    - `channel` - The channel to publish to (binary).
    - `message` - The message payload (binary).

  ## Returns

  The number of subscribers that received the message (integer).
  """
  @spec publish(channel(), binary()) :: non_neg_integer()
  def publish(channel, message) when is_binary(channel) and is_binary(message) do
    # Exact channel subscribers
    channel_count =
      @channels_table
      |> :ets.lookup(channel)
      |> Enum.reduce(0, fn {_ch, pid}, count ->
        send(pid, {:pubsub_message, channel, message})
        count + 1
      end)

    # Pattern subscribers
    pattern_count =
      @patterns_table
      |> :ets.tab2list()
      |> Enum.reduce(0, fn {pattern, pid, regex}, count ->
        if Regex.match?(regex, channel) do
          send(pid, {:pubsub_pmessage, pattern, channel, message})
          count + 1
        else
          count
        end
      end)

    channel_count + pattern_count
  end

  @doc """
  Lists active channels (channels with at least one subscriber).

  When `pattern` is `nil`, returns all channels. When a glob pattern is given,
  returns only channels whose name matches.

  ## Parameters

    - `pattern` - Optional glob pattern to filter channels (default: `nil`).

  ## Returns

  A list of channel name binaries.
  """
  @spec channels(pattern() | nil) :: [channel()]
  def channels(pattern \\ nil) do
    all_channels =
      @channels_table
      |> :ets.tab2list()
      |> Enum.map(fn {ch, _pid} -> ch end)
      |> Enum.uniq()

    case pattern do
      nil ->
        all_channels

      pat when is_binary(pat) ->
        regex = glob_to_regex(pat)
        Enum.filter(all_channels, &Regex.match?(regex, &1))
    end
  end

  @doc """
  Returns subscriber counts for the given channels.

  Returns a flat list of `[channel, count, channel, count, ...]` suitable
  for RESP encoding.

  ## Parameters

    - `channel_list` - List of channel names.

  ## Returns

  A flat list alternating channel names and their subscriber counts.
  """
  @spec numsub([channel()]) :: [channel() | non_neg_integer()]
  def numsub(channel_list) when is_list(channel_list) do
    Enum.flat_map(channel_list, fn channel ->
      count = length(:ets.lookup(@channels_table, channel))
      [channel, count]
    end)
  end

  @doc """
  Returns the total number of active pattern subscriptions.

  ## Returns

  A non-negative integer.
  """
  @spec numpat() :: non_neg_integer()
  def numpat do
    :ets.info(@patterns_table, :size)
  end

  @doc """
  Removes all subscriptions (channels and patterns) for the given `pid`.

  Called during connection cleanup when a client disconnects to prevent
  stale entries in the ETS tables.

  ## Parameters

    - `pid` - The process id to clean up.

  ## Returns

  `:ok`
  """
  @spec cleanup(pid()) :: :ok
  def cleanup(pid) when is_pid(pid) do
    cleanup_pid(pid)
    maybe_demonitor(pid)
    :ok
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    :ets.new(@channels_table, [:named_table, :bag, :public, read_concurrency: true])
    :ets.new(@patterns_table, [:named_table, :bag, :public, read_concurrency: true])
    :ets.new(@monitors_table, [:named_table, :set, :protected, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:ensure_monitor, pid}, _from, state) do
    case :ets.lookup(@monitors_table, pid) do
      [] ->
        ref = Process.monitor(pid)
        :ets.insert(@monitors_table, {pid, ref})
        {:reply, :ok, state}

      [_] ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:maybe_demonitor, pid}, _from, state) do
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case :ets.lookup(@monitors_table, pid) do
      [{^pid, ^ref}] ->
        :ets.delete(@monitors_table, pid)
        cleanup_pid(pid)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private — glob-to-regex conversion
  # ---------------------------------------------------------------------------

  defp ensure_monitor(pid) do
    GenServer.call(__MODULE__, {:ensure_monitor, pid})
  end

  defp maybe_demonitor(pid) do
    GenServer.call(__MODULE__, {:maybe_demonitor, pid})
  end

  defp demonitor_if_unused(pid) do
    if subscribed?(pid) do
      :ok
    else
      case :ets.lookup(@monitors_table, pid) do
        [{^pid, ref}] ->
          Process.demonitor(ref, [:flush])
          :ets.delete(@monitors_table, pid)

        [] ->
          :ok
      end
    end
  end

  defp subscribed?(pid) do
    :ets.match(@channels_table, {:_, pid}) != [] or
      :ets.match(@patterns_table, {:_, pid, :_}) != []
  end

  defp cleanup_pid(pid) do
    :ets.match_delete(@channels_table, {:_, pid})
    :ets.match_delete(@patterns_table, {:_, pid, :_})
  end

  @doc false
  @spec glob_to_regex(binary()) :: Regex.t()
  def glob_to_regex(pattern) do
    regex_str =
      pattern
      |> String.graphemes()
      |> Enum.map_join(&escape_glob_char/1)

    Regex.compile!("^#{regex_str}$")
  end

  defp escape_glob_char("*"), do: ".*"
  defp escape_glob_char("?"), do: "."
  defp escape_glob_char(char), do: Regex.escape(char)
end
