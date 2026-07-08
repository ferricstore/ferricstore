defmodule Ferricstore.PubSub do
  @moduledoc """
  ETS-based Pub/Sub registry for FerricStore.

  Provides a fire-and-forget, at-most-once messaging layer implemented entirely
  on the BEAM — no Raft consensus, no Bitcask persistence. Subscribers register
  their connection pid and receive messages as plain BEAM messages.

  ## Architecture

  Three ETS tables back the registry:

    * `:ferricstore_pubsub` — `{channel, pid}` entries for exact channel
      subscriptions. Uses a `:bag` so multiple pids can subscribe to the same
      channel while duplicate subscriptions from the same pid remain collapsed.

    * `:ferricstore_pubsub_channel_cache` — `{channel, [pid]}` entries derived
      from `:ferricstore_pubsub`. `PUBLISH` reads this table so the hot path
      avoids copying and reducing `{channel, pid}` tuples on every exact publish.

    * `:ferricstore_pubsub_patterns` — `{pattern, pid, matcher}` entries for
      glob-pattern subscriptions (PSUBSCRIBE). Also a `:bag`.

  The tables are owned by a `GenServer` (`Ferricstore.PubSub`) so they survive
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

  The protocol connection process is responsible for encoding these into event frames.
  """

  use GenServer

  alias Ferricstore.PubSub.ActivityLog

  @channels_table :ferricstore_pubsub
  @channel_cache_table :ferricstore_pubsub_channel_cache
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
    exact_subscription_changed([channel], pid)
    ActivityLog.record_subscription("SUBSCRIBE", :channel, [channel])
    :ok
  end

  @doc """
  Subscribes `pid` to all given channels with one monitor operation.

  Duplicate `{channel, pid}` entries are still collapsed by the ETS `:bag`
  table, matching `subscribe/2` semantics.
  """
  @spec subscribe_many([channel()], pid()) :: :ok
  def subscribe_many([], pid) when is_pid(pid), do: :ok

  def subscribe_many(channels, pid) when is_list(channels) and is_pid(pid) do
    entries = subscription_entries(channels, pid, [])
    :ets.insert(@channels_table, entries)
    exact_subscription_changed(unique_channels(channels), pid)
    ActivityLog.record_subscription("SUBSCRIBE", :channel, unique_channels(channels))
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
    exact_unsubscription_changed([channel], pid)
    ActivityLog.record_subscription("UNSUBSCRIBE", :channel, [channel])
    :ok
  end

  @doc """
  Unsubscribes `pid` from all given channels and checks the monitor once.
  """
  @spec unsubscribe_many([channel()], pid()) :: :ok
  def unsubscribe_many([], pid) when is_pid(pid), do: :ok

  def unsubscribe_many(channels, pid) when is_list(channels) and is_pid(pid) do
    Enum.each(channels, fn channel ->
      :ets.match_delete(@channels_table, {channel, pid})
    end)

    exact_unsubscription_changed(unique_channels(channels), pid)
    ActivityLog.record_subscription("UNSUBSCRIBE", :channel, unique_channels(channels))
    :ok
  end

  @doc """
  Subscribes `pid` to all channels matching `pattern` (glob syntax).

  The raw glob pattern is stored and evaluated with `Ferricstore.GlobMatcher`
  at publish time, so PubSub uses the same Redis pattern semantics as SCAN.

  ## Parameters

    - `pattern` - A glob pattern (e.g. `"news.*"`, `"user:?"`).
    - `pid`     - The subscriber process id.

  ## Returns

  `:ok`
  """
  @spec psubscribe(pattern(), pid()) :: :ok
  def psubscribe(pattern, pid) when is_binary(pattern) and is_pid(pid) do
    # The table is an ETS :bag, so inserting the same {pattern, pid, matcher}
    # object twice remains idempotent without scanning the pattern bucket first.
    :ets.insert(@patterns_table, {pattern, pid, pattern_matcher(pattern)})
    ensure_monitor(pid)
    ActivityLog.record_subscription("PSUBSCRIBE", :pattern, [pattern])
    :ok
  end

  @doc """
  Subscribes `pid` to all given glob patterns with one monitor operation.
  """
  @spec psubscribe_many([pattern()], pid()) :: :ok
  def psubscribe_many([], pid) when is_pid(pid), do: :ok

  def psubscribe_many(patterns, pid) when is_list(patterns) and is_pid(pid) do
    entries = pattern_subscription_entries(patterns, pid, [])
    :ets.insert(@patterns_table, entries)
    ensure_monitor(pid)
    ActivityLog.record_subscription("PSUBSCRIBE", :pattern, unique_channels(patterns))
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
    # match_delete with a wildcard for the matcher marker
    :ets.match_delete(@patterns_table, {pattern, pid, :_})
    maybe_demonitor(pid)
    ActivityLog.record_subscription("PUNSUBSCRIBE", :pattern, [pattern])
    :ok
  end

  @doc """
  Unsubscribes `pid` from all given glob patterns and checks the monitor once.
  """
  @spec punsubscribe_many([pattern()], pid()) :: :ok
  def punsubscribe_many([], pid) when is_pid(pid), do: :ok

  def punsubscribe_many(patterns, pid) when is_list(patterns) and is_pid(pid) do
    Enum.each(patterns, fn pattern ->
      :ets.match_delete(@patterns_table, {pattern, pid, :_})
    end)

    maybe_demonitor(pid)
    ActivityLog.record_subscription("PUNSUBSCRIBE", :pattern, unique_channels(patterns))
    :ok
  end

  @doc """
  Publishes `message` to all subscribers of `channel`.

  Looks up exact channel subscribers and pattern subscribers whose glob pattern
  matches the channel name. Sends a BEAM message to each matching pid.

  ## Parameters

    - `channel` - The channel to publish to (binary).
    - `message` - The message payload (binary).

  ## Returns

  The number of subscribers that received the message (integer).
  """
  @spec publish(channel(), binary()) :: non_neg_integer()
  def publish(channel, message) when is_binary(channel) and is_binary(message) do
    channel_count =
      case :ets.lookup(@channel_cache_table, channel) do
        [{^channel, pids}] -> publish_exact_pids(pids, channel, message, 0)
        [] -> 0
      end

    # Pattern subscribers
    pattern_count =
      if :ets.info(@patterns_table, :size) == 0 do
        0
      else
        :ets.foldl(
          fn {pattern, pid, matcher}, count ->
            if pattern_matches?(channel, matcher) do
              send(pid, {:pubsub_pmessage, pattern, channel, message})
              count + 1
            else
              count
            end
          end,
          0,
          @patterns_table
        )
      end

    total = channel_count + pattern_count
    ActivityLog.record_publish(channel, byte_size(message), total)
    total
  end

  @doc """
  Returns bounded Pub/Sub subscription metadata for observability dashboards.
  """
  @spec subscription_snapshot(non_neg_integer()) :: map()
  def subscription_snapshot(limit \\ 100) do
    limit = max(limit, 0)
    channels = channel_snapshot(limit)
    patterns = pattern_snapshot(limit)
    exact_subscriptions = safe_ets_size(@channels_table)
    pattern_subscriptions = safe_ets_size(@patterns_table)

    %{
      channels: channels,
      patterns: patterns,
      exact_subscriptions: exact_subscriptions,
      pattern_subscriptions: pattern_subscriptions,
      active_subscribers: active_subscriber_count()
    }
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
    collect_channels(:ets.first(@channels_table), pattern, [])
  end

  defp collect_channels(:"$end_of_table", _pattern, acc), do: Enum.reverse(acc)

  defp collect_channels(channel, nil, acc) do
    collect_channels(:ets.next(@channels_table, channel), nil, [channel | acc])
  end

  defp collect_channels(channel, pattern, acc) when is_binary(pattern) do
    next = :ets.next(@channels_table, channel)

    if Ferricstore.GlobMatcher.match?(channel, pattern) do
      collect_channels(next, pattern, [channel | acc])
    else
      collect_channels(next, pattern, acc)
    end
  end

  @doc """
  Returns subscriber counts for the given channels.

  Returns a flat list of `[channel, count, channel, count, ...]` suitable
  for wire encoding.

  ## Parameters

    - `channel_list` - List of channel names.

  ## Returns

  A flat list alternating channel names and their subscriber counts.
  """
  @spec numsub([channel()]) :: [channel() | non_neg_integer()]
  def numsub(channel_list) when is_list(channel_list) do
    numsub_reply(channel_list, [])
  end

  defp numsub_reply([], acc), do: Enum.reverse(acc)

  defp numsub_reply([channel | rest], acc) do
    count = length(:ets.lookup(@channels_table, channel))
    numsub_reply(rest, [count, channel | acc])
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
    GenServer.call(__MODULE__, {:cleanup, pid})
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    :ets.new(@channels_table, [:named_table, :bag, :public, read_concurrency: true])

    :ets.new(@channel_cache_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@patterns_table, [:named_table, :bag, :public, read_concurrency: true])
    :ets.new(@monitors_table, [:named_table, :set, :protected, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:ensure_monitor, pid}, _from, state) do
    ensure_monitor_local(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:maybe_demonitor, pid}, _from, state) do
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:exact_subscription_changed, channels, pid}, _from, state) do
    ensure_monitor_local(pid)
    rebuild_exact_channels(channels)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:exact_unsubscription_changed, channels, pid}, _from, state) do
    rebuild_exact_channels(channels)
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:cleanup, pid}, _from, state) do
    channels = exact_channels_for_pid(pid)
    cleanup_pid(pid)
    rebuild_exact_channels(channels)
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case :ets.lookup(@monitors_table, pid) do
      [{^pid, ^ref}] ->
        channels = exact_channels_for_pid(pid)
        :ets.delete(@monitors_table, pid)
        cleanup_pid(pid)
        rebuild_exact_channels(channels)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  defp ensure_monitor(pid) do
    GenServer.call(__MODULE__, {:ensure_monitor, pid})
  end

  defp exact_subscription_changed(channels, pid) do
    GenServer.call(__MODULE__, {:exact_subscription_changed, channels, pid})
  end

  defp exact_unsubscription_changed(channels, pid) do
    GenServer.call(__MODULE__, {:exact_unsubscription_changed, channels, pid})
  end

  defp subscription_entries([], _pid, acc), do: Enum.reverse(acc)

  defp subscription_entries([channel | rest], pid, acc) when is_binary(channel) do
    subscription_entries(rest, pid, [{channel, pid} | acc])
  end

  defp pattern_subscription_entries([], _pid, acc), do: Enum.reverse(acc)

  defp pattern_subscription_entries([pattern | rest], pid, acc) when is_binary(pattern) do
    pattern_subscription_entries(rest, pid, [{pattern, pid, pattern_matcher(pattern)} | acc])
  end

  defp channel_snapshot(limit) do
    if :ets.whereis(@channel_cache_table) == :undefined do
      []
    else
      @channel_cache_table
      |> :ets.tab2list()
      |> Enum.map(fn {channel, pids} ->
        %{channel: channel, subscribers: length(pids)}
      end)
      |> Enum.sort_by(& &1.subscribers, :desc)
      |> Enum.take(limit)
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp pattern_snapshot(limit) do
    if :ets.whereis(@patterns_table) == :undefined do
      []
    else
      @patterns_table
      |> :ets.tab2list()
      |> Enum.group_by(fn {pattern, _pid, _matcher} -> pattern end)
      |> Enum.map(fn {pattern, entries} ->
        %{pattern: pattern, subscribers: length(entries)}
      end)
      |> Enum.sort_by(& &1.subscribers, :desc)
      |> Enum.take(limit)
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp active_subscriber_count do
    exact_pids =
      if :ets.whereis(@channels_table) == :undefined do
        []
      else
        @channels_table
        |> :ets.tab2list()
        |> Enum.map(fn {_channel, pid} -> pid end)
      end

    pattern_pids =
      if :ets.whereis(@patterns_table) == :undefined do
        []
      else
        @patterns_table
        |> :ets.tab2list()
        |> Enum.map(fn {_pattern, pid, _matcher} -> pid end)
      end

    (exact_pids ++ pattern_pids)
    |> Enum.uniq()
    |> length()
  rescue
    _ -> 0
  catch
    :exit, _ -> 0
  end

  defp safe_ets_size(table) do
    case :ets.info(table, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  rescue
    ArgumentError -> 0
  end

  defp maybe_demonitor(pid) do
    GenServer.call(__MODULE__, {:maybe_demonitor, pid})
  end

  defp ensure_monitor_local(pid) do
    case :ets.lookup(@monitors_table, pid) do
      [] ->
        ref = Process.monitor(pid)
        :ets.insert(@monitors_table, {pid, ref})

      [_] ->
        :ok
    end
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

  defp rebuild_exact_channels([]), do: :ok

  defp rebuild_exact_channels([channel | rest]) do
    rebuild_exact_channel(channel)
    rebuild_exact_channels(rest)
  end

  defp rebuild_exact_channel(channel) do
    case exact_pids_for_channel(:ets.lookup(@channels_table, channel), []) do
      [] -> :ets.delete(@channel_cache_table, channel)
      pids -> :ets.insert(@channel_cache_table, {channel, pids})
    end
  end

  defp exact_pids_for_channel([{_channel, pid} | rest], acc) do
    exact_pids_for_channel(rest, [pid | acc])
  end

  defp exact_pids_for_channel([], acc), do: acc

  defp exact_channels_for_pid(pid) do
    @channels_table
    |> :ets.match({:"$1", pid})
    |> exact_channels_from_match([])
  end

  defp exact_channels_from_match([[channel] | rest], acc),
    do: exact_channels_from_match(rest, [channel | acc])

  defp exact_channels_from_match([], acc), do: unique_channels(acc)

  defp unique_channels(channels), do: unique_channels(channels, MapSet.new(), [])

  defp unique_channels([channel | rest], seen, acc) do
    if MapSet.member?(seen, channel) do
      unique_channels(rest, seen, acc)
    else
      unique_channels(rest, MapSet.put(seen, channel), [channel | acc])
    end
  end

  defp unique_channels([], _seen, acc), do: acc

  defp publish_exact_pids([pid | rest], channel, message, count) do
    send(pid, {:pubsub_message, channel, message})
    publish_exact_pids(rest, channel, message, count + 1)
  end

  defp publish_exact_pids([], _channel, _message, count), do: count

  defp pattern_matcher(pattern) do
    if byte_size(pattern) > 1024 do
      :never
    else
      simple_pattern_matcher(pattern, 0, byte_size(pattern), 0, -1)
    end
  end

  defp simple_pattern_matcher(pattern, pos, size, star_count, star_pos) when pos < size do
    case :binary.at(pattern, pos) do
      ?* ->
        simple_pattern_matcher(pattern, pos + 1, size, star_count + 1, pos)

      special when special in [??, ?[, ?\\] ->
        {:glob, pattern}

      _literal ->
        simple_pattern_matcher(pattern, pos + 1, size, star_count, star_pos)
    end
  end

  defp simple_pattern_matcher(pattern, _pos, _size, 0, _star_pos), do: {:exact, pattern}
  defp simple_pattern_matcher(_pattern, _pos, 1, 1, 0), do: :all

  defp simple_pattern_matcher(pattern, _pos, size, 1, star_pos) when star_pos == size - 1 do
    {:prefix, binary_part(pattern, 0, size - 1)}
  end

  defp simple_pattern_matcher(pattern, _pos, size, 1, 0) do
    {:suffix, binary_part(pattern, 1, size - 1)}
  end

  defp simple_pattern_matcher(pattern, _pos, _size, _star_count, _star_pos), do: {:glob, pattern}

  defp pattern_matches?(_channel, :all), do: true
  defp pattern_matches?(_channel, :never), do: false
  defp pattern_matches?(channel, {:exact, exact}), do: channel == exact

  defp pattern_matches?(channel, {:prefix, prefix}) do
    prefix_size = byte_size(prefix)
    byte_size(channel) >= prefix_size and binary_part(channel, 0, prefix_size) == prefix
  end

  defp pattern_matches?(channel, {:suffix, suffix}) do
    channel_size = byte_size(channel)
    suffix_size = byte_size(suffix)

    channel_size >= suffix_size and
      binary_part(channel, channel_size - suffix_size, suffix_size) == suffix
  end

  defp pattern_matches?(channel, {:glob, pattern}),
    do: Ferricstore.GlobMatcher.match?(channel, pattern)
end
