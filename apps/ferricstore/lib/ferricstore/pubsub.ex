defmodule Ferricstore.PubSub do
  @moduledoc """
  ETS-based Pub/Sub registry for FerricStore.

  Provides a fire-and-forget, at-most-once messaging layer implemented entirely
  on the BEAM — no Raft consensus, no Bitcask persistence. Subscribers register
  their connection pid and receive messages as plain BEAM messages.

  ## Architecture

  Thirteen ETS tables back the registry:

    * `:ferricstore_pubsub` — `{channel, pid}` entries for exact channel
      subscriptions. Uses a `:bag` so multiple pids can subscribe to the same
      channel while duplicate subscriptions from the same pid remain collapsed.

    * `:ferricstore_pubsub_channel_cache` —
      `{channel, [delivery], subscriber_count}` entries
      derived from `:ferricstore_pubsub`, where each delivery is an unguarded
      pid or a guarded `{pid, guard}` pair. `PUBLISH` reads this table so the
      hot path avoids copying subscription tuples and looking up each
      subscriber's stable delivery guard on every exact publish.

    * `:ferricstore_pubsub_pid_channels` — `{pid, channel}` reverse-index
      entries used to clean up one subscriber without scanning the complete
      channel registry.

    * `:ferricstore_pubsub_patterns` — `{pattern, pid, matcher}` entries for
      glob-pattern subscriptions (PSUBSCRIBE). Also a `:bag`.

    * `:ferricstore_pubsub_pattern_cache` —
      `{pattern, matcher, [delivery], subscriber_count}` rows used by pattern
      publishes and bounded observability snapshots. Matching runs once per
      unique pattern, rather than once per pattern subscription.

    * `:ferricstore_pubsub_pid_patterns` — `{pid, pattern}` reverse-index
      entries used for bounded per-subscriber pattern cleanup.

    * Two membership sets keyed by `{pid, channel_or_pattern}` provide constant
      time idempotence checks even when one client owns many subscriptions.

    * Three matcher indexes map simple prefixes, simple suffixes, anchored
      complex globs, and unanchored complex-glob fallbacks to cached pattern
      rows. Large pattern registries use these indexes; small registries keep
      the cheaper linear scan.

    * `:ferricstore_pubsub_monitors` — one monitor row per active subscriber.

    * `:ferricstore_pubsub_delivery_guards` — optional per-subscriber admission
      callbacks used by network servers before mailbox delivery.

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
    * `{:pubsub_messages, channel, messages, lease}` — an internal guarded exact
      batch consumed by protocol connection processes

  The protocol connection process is responsible for encoding these into event frames.
  """

  use GenServer

  alias Ferricstore.PubSub.ActivityLog

  @channels_table :ferricstore_pubsub
  @channel_cache_table :ferricstore_pubsub_channel_cache
  @pid_channels_table :ferricstore_pubsub_pid_channels
  @channel_memberships_table :ferricstore_pubsub_channel_memberships
  @patterns_table :ferricstore_pubsub_patterns
  @pattern_cache_table :ferricstore_pubsub_pattern_cache
  @pid_patterns_table :ferricstore_pubsub_pid_patterns
  @pattern_memberships_table :ferricstore_pubsub_pattern_memberships
  @pattern_prefix_index_table :ferricstore_pubsub_pattern_prefix_index
  @pattern_suffix_index_table :ferricstore_pubsub_pattern_suffix_index
  @pattern_glob_index_table :ferricstore_pubsub_pattern_glob_index
  @monitors_table :ferricstore_pubsub_monitors
  @delivery_guards_table :ferricstore_pubsub_delivery_guards
  @delivery_overhead_bytes 128
  @max_pattern_bytes 1024
  @pattern_linear_scan_limit 16
  @incremental_cache_fanout_threshold 32

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
    channels = unique_channels(channels)
    subscribe_unique_many(channels, pid)
  end

  @doc false
  @spec subscribe_unique_many([channel()], pid()) :: :ok
  def subscribe_unique_many([], pid) when is_pid(pid), do: :ok

  def subscribe_unique_many(channels, pid) when is_list(channels) and is_pid(pid) do
    exact_subscription_changed(channels, pid)
    ActivityLog.record_subscription("SUBSCRIBE", :channel, channels)
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
    channels = unique_channels(channels)
    unsubscribe_unique_many(channels, pid)
  end

  @doc false
  @spec unsubscribe_unique_many([channel()], pid()) :: :ok
  def unsubscribe_unique_many([], pid) when is_pid(pid), do: :ok

  def unsubscribe_unique_many(channels, pid) when is_list(channels) and is_pid(pid) do
    exact_unsubscription_changed(channels, pid)
    ActivityLog.record_subscription("UNSUBSCRIBE", :channel, channels)
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
    pattern_subscription_changed([pattern], pid)
    ActivityLog.record_subscription("PSUBSCRIBE", :pattern, [pattern])
    :ok
  end

  @doc """
  Subscribes `pid` to all given glob patterns with one monitor operation.
  """
  @spec psubscribe_many([pattern()], pid()) :: :ok
  def psubscribe_many([], pid) when is_pid(pid), do: :ok

  def psubscribe_many(patterns, pid) when is_list(patterns) and is_pid(pid) do
    patterns = unique_channels(patterns)
    psubscribe_unique_many(patterns, pid)
  end

  @doc false
  @spec psubscribe_unique_many([pattern()], pid()) :: :ok
  def psubscribe_unique_many([], pid) when is_pid(pid), do: :ok

  def psubscribe_unique_many(patterns, pid) when is_list(patterns) and is_pid(pid) do
    pattern_subscription_changed(patterns, pid)
    ActivityLog.record_subscription("PSUBSCRIBE", :pattern, patterns)
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
    pattern_unsubscription_changed([pattern], pid)
    ActivityLog.record_subscription("PUNSUBSCRIBE", :pattern, [pattern])
    :ok
  end

  @doc """
  Unsubscribes `pid` from all given glob patterns and checks the monitor once.
  """
  @spec punsubscribe_many([pattern()], pid()) :: :ok
  def punsubscribe_many([], pid) when is_pid(pid), do: :ok

  def punsubscribe_many(patterns, pid) when is_list(patterns) and is_pid(pid) do
    patterns = unique_channels(patterns)
    punsubscribe_unique_many(patterns, pid)
  end

  @doc false
  @spec punsubscribe_unique_many([pattern()], pid()) :: :ok
  def punsubscribe_unique_many([], pid) when is_pid(pid), do: :ok

  def punsubscribe_unique_many(patterns, pid) when is_list(patterns) and is_pid(pid) do
    pattern_unsubscription_changed(patterns, pid)
    ActivityLog.record_subscription("PUNSUBSCRIBE", :pattern, patterns)
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
        [{^channel, deliveries, _subscriber_count}] ->
          bytes = byte_size(channel) + byte_size(message) + @delivery_overhead_bytes
          publish_exact_deliveries(deliveries, channel, message, bytes, 0)

        [] ->
          0
      end

    pattern_count =
      case :ets.info(@pattern_cache_table, :size) do
        0 ->
          0

        pattern_count when pattern_count <= @pattern_linear_scan_limit ->
          publish_pattern_scan(channel, message)

        _pattern_count ->
          publish_pattern_indexes(channel, message)
      end

    total = channel_count + pattern_count
    ActivityLog.record_publish(channel, byte_size(message), total)
    total
  end

  @doc """
  Publishes an ordered batch and returns one subscriber count per item.

  Homogeneous exact-channel batches reserve guarded outbound capacity and
  enqueue once per subscriber. Mixed-channel batches retain the ordinary
  per-publish path.
  """
  @spec publish_many([{channel(), binary()}]) :: [non_neg_integer()]
  def publish_many([]), do: []

  def publish_many([{channel, message} | rest] = publishes)
      when is_binary(channel) and is_binary(message) and is_list(rest) do
    case same_channel_messages(rest, channel, [message]) do
      {:ok, messages} ->
        publish_same_channel_many(channel, messages)

      :mixed ->
        Enum.map(publishes, fn {item_channel, item_message} ->
          publish(item_channel, item_message)
        end)
    end
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
    matcher = if is_binary(pattern), do: pattern_matcher(pattern), else: nil
    collect_channels(:ets.first(@channels_table), matcher, [])
  end

  defp collect_channels(:"$end_of_table", _pattern, acc), do: Enum.reverse(acc)

  defp collect_channels(channel, nil, acc) do
    collect_channels(:ets.next(@channels_table, channel), nil, [channel | acc])
  end

  defp collect_channels(channel, matcher, acc) do
    next = :ets.next(@channels_table, channel)

    if pattern_matches?(channel, matcher) do
      collect_channels(next, matcher, [channel | acc])
    else
      collect_channels(next, matcher, acc)
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
    count = :ets.lookup_element(@channel_cache_table, channel, 3, 0)
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

  @doc false
  @spec set_delivery_guard(pid(), (non_neg_integer() -> {:ok, term()} | {:error, term()})) :: :ok
  def set_delivery_guard(pid, guard)
      when is_pid(pid) and pid == self() and is_function(guard, 1) do
    GenServer.call(__MODULE__, {:set_delivery_guard, pid, guard})
  end

  @doc false
  @spec clear_delivery_guard(pid()) :: :ok
  def clear_delivery_guard(pid) when is_pid(pid) and pid == self() do
    GenServer.call(__MODULE__, {:clear_delivery_guard, pid})
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    :ets.new(@channels_table, [:named_table, :bag, :protected, read_concurrency: true])

    :ets.new(@channel_cache_table, [
      :named_table,
      :set,
      :protected,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@pid_channels_table, [
      :named_table,
      :duplicate_bag,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@channel_memberships_table, [
      :named_table,
      :set,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@patterns_table, [:named_table, :bag, :protected, read_concurrency: true])

    :ets.new(@pattern_cache_table, [
      :named_table,
      :set,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@pid_patterns_table, [
      :named_table,
      :duplicate_bag,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@pattern_memberships_table, [
      :named_table,
      :set,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@pattern_prefix_index_table, [
      :named_table,
      :bag,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@pattern_suffix_index_table, [
      :named_table,
      :bag,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@pattern_glob_index_table, [
      :named_table,
      :set,
      :protected,
      read_concurrency: true
    ])

    :ets.new(@monitors_table, [:named_table, :set, :protected, read_concurrency: true])

    :ets.new(@delivery_guards_table, [
      :named_table,
      :set,
      :protected,
      read_concurrency: true
    ])

    {:ok, %{}}
  end

  @impl true
  def handle_call({:exact_subscription_changed, channels, pid}, _from, state) do
    new_channels = missing_subscription_values(channels, pid, @channel_memberships_table)

    if new_channels != [] do
      :ets.insert(@channels_table, subscription_entries(new_channels, pid, []))
      :ets.insert(@pid_channels_table, reverse_subscription_entries(new_channels, pid, []))
      :ets.insert(@channel_memberships_table, membership_entries(new_channels, pid, []))
      refresh_exact_cache_after_add(new_channels, delivery_for_pid(pid))
    end

    ensure_monitor_local(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:exact_unsubscription_changed, channels, pid}, _from, state) do
    removed_channels = existing_subscription_values(channels, pid, @channel_memberships_table)
    delete_exact_subscriptions(removed_channels, pid)
    refresh_exact_cache_after_remove(removed_channels, pid)
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:pattern_subscription_changed, patterns, pid}, _from, state) do
    new_patterns = missing_subscription_values(patterns, pid, @pattern_memberships_table)

    if new_patterns != [] do
      entries = pattern_subscription_entries(new_patterns, pid, [])
      :ets.insert(@patterns_table, entries)
      :ets.insert(@pid_patterns_table, reverse_subscription_entries(new_patterns, pid, []))
      :ets.insert(@pattern_memberships_table, membership_entries(new_patterns, pid, []))
      refresh_pattern_cache_after_add(entries, delivery_for_pid(pid))
    end

    ensure_monitor_local(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:pattern_unsubscription_changed, patterns, pid}, _from, state) do
    removed_patterns = existing_subscription_values(patterns, pid, @pattern_memberships_table)
    delete_pattern_subscriptions(removed_patterns, pid)
    refresh_pattern_cache_after_remove(removed_patterns, pid)
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:set_delivery_guard, pid, guard}, _from, state) do
    :ets.insert(@delivery_guards_table, {pid, guard})
    rebuild_exact_channels(exact_channels_for_pid(pid))
    rebuild_patterns(patterns_for_pid(pid))
    ensure_monitor_local(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:clear_delivery_guard, pid}, _from, state) do
    :ets.delete(@delivery_guards_table, pid)
    rebuild_exact_channels(exact_channels_for_pid(pid))
    rebuild_patterns(patterns_for_pid(pid))
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:cleanup, pid}, _from, state) do
    channels = exact_channels_for_pid(pid)
    patterns = patterns_for_pid(pid)
    cleanup_pid(pid, channels, patterns)
    rebuild_exact_channels(channels)
    rebuild_patterns(patterns)
    demonitor_if_unused(pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case :ets.lookup(@monitors_table, pid) do
      [{^pid, ^ref}] ->
        channels = exact_channels_for_pid(pid)
        patterns = patterns_for_pid(pid)
        :ets.delete(@monitors_table, pid)
        cleanup_pid(pid, channels, patterns)
        rebuild_exact_channels(channels)
        rebuild_patterns(patterns)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  defp exact_subscription_changed(channels, pid) do
    GenServer.call(__MODULE__, {:exact_subscription_changed, channels, pid})
  end

  defp exact_unsubscription_changed(channels, pid) do
    GenServer.call(__MODULE__, {:exact_unsubscription_changed, channels, pid})
  end

  defp pattern_subscription_changed(patterns, pid) do
    GenServer.call(__MODULE__, {:pattern_subscription_changed, patterns, pid})
  end

  defp pattern_unsubscription_changed(patterns, pid) do
    GenServer.call(__MODULE__, {:pattern_unsubscription_changed, patterns, pid})
  end

  defp subscription_entries([], _pid, acc), do: Enum.reverse(acc)

  defp subscription_entries([channel | rest], pid, acc) when is_binary(channel) do
    subscription_entries(rest, pid, [{channel, pid} | acc])
  end

  defp pattern_subscription_entries([], _pid, acc), do: Enum.reverse(acc)

  defp pattern_subscription_entries([pattern | rest], pid, acc) when is_binary(pattern) do
    pattern_subscription_entries(rest, pid, [{pattern, pid, pattern_matcher(pattern)} | acc])
  end

  defp reverse_subscription_entries([], _pid, acc), do: acc

  defp reverse_subscription_entries([value | rest], pid, acc) do
    reverse_subscription_entries(rest, pid, [{pid, value} | acc])
  end

  defp membership_entries([], _pid, acc), do: acc

  defp membership_entries([value | rest], pid, acc) do
    membership_entries(rest, pid, [{{pid, value}} | acc])
  end

  defp missing_subscription_values(values, pid, membership_table) do
    Enum.reject(values, &:ets.member(membership_table, {pid, &1}))
  end

  defp existing_subscription_values(values, pid, membership_table) do
    Enum.filter(values, &:ets.member(membership_table, {pid, &1}))
  end

  defp delete_exact_subscriptions([channel | rest], pid) do
    :ets.delete_object(@channels_table, {channel, pid})
    :ets.delete_object(@pid_channels_table, {pid, channel})
    :ets.delete(@channel_memberships_table, {pid, channel})
    delete_exact_subscriptions(rest, pid)
  end

  defp delete_exact_subscriptions([], _pid), do: :ok

  defp delete_pattern_subscriptions([pattern | rest], pid) do
    :ets.match_delete(@patterns_table, {pattern, pid, :_})
    :ets.delete_object(@pid_patterns_table, {pid, pattern})
    :ets.delete(@pattern_memberships_table, {pid, pattern})
    delete_pattern_subscriptions(rest, pid)
  end

  defp delete_pattern_subscriptions([], _pid), do: :ok

  defp channel_snapshot(limit) do
    bounded_snapshot(@channel_cache_table, limit, :channel, &channel_snapshot_entry/1)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp channel_snapshot_entry({channel, _deliveries, subscriber_count}) do
    {channel, subscriber_count}
  end

  defp pattern_snapshot(limit) do
    bounded_snapshot(@pattern_cache_table, limit, :pattern, &pattern_snapshot_entry/1)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp pattern_snapshot_entry({pattern, _matcher, _deliveries, subscriber_count}) do
    {pattern, subscriber_count}
  end

  defp active_subscriber_count, do: safe_ets_size(@monitors_table)

  defp bounded_snapshot(_table, 0, _name_key, _mapper), do: []

  defp bounded_snapshot(table, limit, name_key, mapper) do
    if :ets.whereis(table) == :undefined do
      []
    else
      :ets.foldl(
        fn row, entries ->
          {name, subscribers} = mapper.(row)
          insert_snapshot_entry(name, subscribers, entries, limit)
        end,
        :gb_sets.empty(),
        table
      )
      |> :gb_sets.to_list()
      |> Enum.map(fn {_rank, name, subscribers} ->
        %{name_key => name, subscribers: subscribers}
      end)
    end
  end

  defp insert_snapshot_entry(name, subscribers, entries, limit) do
    entries = :gb_sets.add_element({-subscribers, name, subscribers}, entries)

    if :gb_sets.size(entries) > limit do
      :gb_sets.delete(:gb_sets.largest(entries), entries)
    else
      entries
    end
  end

  defp safe_ets_size(table) do
    case :ets.info(table, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  rescue
    ArgumentError -> 0
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
      :ets.delete(@delivery_guards_table, pid)

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
    :ets.member(@pid_channels_table, pid) or :ets.member(@pid_patterns_table, pid)
  end

  defp cleanup_pid(pid, channels, patterns) do
    delete_exact_subscriptions(channels, pid)
    delete_pattern_subscriptions(patterns, pid)
    :ets.delete(@pid_channels_table, pid)
    :ets.delete(@pid_patterns_table, pid)
    :ets.delete(@delivery_guards_table, pid)
  end

  defp refresh_exact_cache_after_add([], _delivery), do: :ok

  defp refresh_exact_cache_after_add([channel | rest], delivery) do
    case :ets.lookup(@channel_cache_table, channel) do
      [{^channel, deliveries, subscriber_count}]
      when subscriber_count >= @incremental_cache_fanout_threshold ->
        :ets.insert(
          @channel_cache_table,
          {channel, [delivery | deliveries], subscriber_count + 1}
        )

      _small_or_new_channel ->
        rebuild_exact_channel(channel)
    end

    refresh_exact_cache_after_add(rest, delivery)
  end

  defp refresh_exact_cache_after_remove([], _pid), do: :ok

  defp refresh_exact_cache_after_remove([channel | rest], pid) do
    case :ets.lookup(@channel_cache_table, channel) do
      [{^channel, deliveries, subscriber_count}]
      when subscriber_count > @incremental_cache_fanout_threshold ->
        remaining = remove_delivery_for_pid(deliveries, pid, [])
        :ets.insert(@channel_cache_table, {channel, remaining, subscriber_count - 1})

      _small_or_missing_channel ->
        rebuild_exact_channel(channel)
    end

    refresh_exact_cache_after_remove(rest, pid)
  end

  defp refresh_pattern_cache_after_add([], _delivery), do: :ok

  defp refresh_pattern_cache_after_add([{pattern, _pid, _matcher} | rest], delivery) do
    case :ets.lookup(@pattern_cache_table, pattern) do
      [{^pattern, existing_matcher, deliveries, subscriber_count}]
      when subscriber_count >= @incremental_cache_fanout_threshold ->
        :ets.insert(
          @pattern_cache_table,
          {pattern, existing_matcher, [delivery | deliveries], subscriber_count + 1}
        )

      _small_or_new_pattern ->
        rebuild_pattern(pattern)
    end

    refresh_pattern_cache_after_add(rest, delivery)
  end

  defp refresh_pattern_cache_after_remove([], _pid), do: :ok

  defp refresh_pattern_cache_after_remove([pattern | rest], pid) do
    case :ets.lookup(@pattern_cache_table, pattern) do
      [{^pattern, matcher, deliveries, subscriber_count}]
      when subscriber_count > @incremental_cache_fanout_threshold ->
        remaining = remove_delivery_for_pid(deliveries, pid, [])

        :ets.insert(
          @pattern_cache_table,
          {pattern, matcher, remaining, subscriber_count - 1}
        )

      _small_or_missing_pattern ->
        rebuild_pattern(pattern)
    end

    refresh_pattern_cache_after_remove(rest, pid)
  end

  defp remove_delivery_for_pid([pid | rest], pid, acc) when is_pid(pid),
    do: Enum.reverse(acc, rest)

  defp remove_delivery_for_pid([{pid, _guard} | rest], pid, acc),
    do: Enum.reverse(acc, rest)

  defp remove_delivery_for_pid([delivery | rest], pid, acc),
    do: remove_delivery_for_pid(rest, pid, [delivery | acc])

  defp remove_delivery_for_pid([], _pid, acc), do: Enum.reverse(acc)

  defp rebuild_exact_channels([]), do: :ok

  defp rebuild_exact_channels([channel | rest]) do
    rebuild_exact_channel(channel)
    rebuild_exact_channels(rest)
  end

  defp rebuild_exact_channel(channel) do
    case exact_deliveries_for_channel(:ets.lookup(@channels_table, channel), []) do
      [] ->
        :ets.delete(@channel_cache_table, channel)

      deliveries ->
        subscriber_count = length(deliveries)
        :ets.insert(@channel_cache_table, {channel, deliveries, subscriber_count})
    end
  end

  defp rebuild_patterns([]), do: :ok

  defp rebuild_patterns([pattern | rest]) do
    rebuild_pattern(pattern)
    rebuild_patterns(rest)
  end

  defp rebuild_pattern(pattern) do
    case :ets.lookup(@patterns_table, pattern) do
      [] ->
        delete_pattern_index(pattern)
        :ets.delete(@pattern_cache_table, pattern)

      [{^pattern, _pid, matcher} | _rest] = entries ->
        deliveries = pattern_deliveries(entries, [])
        subscriber_count = length(deliveries)
        :ets.insert(@pattern_cache_table, {pattern, matcher, deliveries, subscriber_count})
        put_pattern_index(pattern, matcher)
    end
  end

  defp put_pattern_index(pattern, {:prefix, prefix}) do
    :ets.insert(@pattern_prefix_index_table, {prefix, pattern, :direct})
  end

  defp put_pattern_index(pattern, {:suffix, suffix}) do
    :ets.insert(@pattern_suffix_index_table, {suffix, pattern, :direct})
  end

  defp put_pattern_index(pattern, {:glob, pattern}) do
    case complex_glob_anchor(pattern) do
      {:prefix, prefix} ->
        :ets.insert(@pattern_prefix_index_table, {prefix, pattern, :glob})

      {:suffix, suffix} ->
        :ets.insert(@pattern_suffix_index_table, {suffix, pattern, :glob})

      :fallback ->
        :ets.insert(@pattern_glob_index_table, {pattern})
    end
  end

  defp put_pattern_index(_pattern, _direct_or_never_matcher), do: :ok

  defp delete_pattern_index(pattern) do
    case :ets.lookup(@pattern_cache_table, pattern) do
      [{^pattern, {:prefix, prefix}, _deliveries, _subscriber_count}] ->
        :ets.delete_object(@pattern_prefix_index_table, {prefix, pattern, :direct})

      [{^pattern, {:suffix, suffix}, _deliveries, _subscriber_count}] ->
        :ets.delete_object(@pattern_suffix_index_table, {suffix, pattern, :direct})

      [{^pattern, {:glob, ^pattern}, _deliveries, _subscriber_count}] ->
        delete_complex_glob_index(pattern)

      _direct_never_or_missing ->
        :ok
    end
  end

  defp exact_deliveries_for_channel([{_channel, pid} | rest], acc) do
    exact_deliveries_for_channel(rest, [delivery_for_pid(pid) | acc])
  end

  defp exact_deliveries_for_channel([], acc), do: acc

  defp pattern_deliveries([{_pattern, pid, _matcher} | rest], acc) do
    pattern_deliveries(rest, [delivery_for_pid(pid) | acc])
  end

  defp pattern_deliveries([], acc), do: acc

  defp delivery_for_pid(pid) do
    case :ets.lookup(@delivery_guards_table, pid) do
      [] -> pid
      [{^pid, guard}] -> {pid, guard}
    end
  end

  defp exact_channels_for_pid(pid) do
    @pid_channels_table
    |> :ets.lookup(pid)
    |> reverse_subscription_values([])
  end

  defp patterns_for_pid(pid) do
    @pid_patterns_table
    |> :ets.lookup(pid)
    |> reverse_subscription_values([])
  end

  defp reverse_subscription_values([{_pid, value} | rest], acc),
    do: reverse_subscription_values(rest, [value | acc])

  defp reverse_subscription_values([], acc), do: acc

  defp unique_channels(channels), do: unique_channels(channels, MapSet.new(), [])

  defp unique_channels([channel | rest], seen, acc) do
    if MapSet.member?(seen, channel) do
      unique_channels(rest, seen, acc)
    else
      unique_channels(rest, MapSet.put(seen, channel), [channel | acc])
    end
  end

  defp unique_channels([], _seen, acc), do: acc

  defp publish_pattern_scan(channel, message) do
    channel_size = byte_size(channel)
    message_size = byte_size(message)

    :ets.foldl(
      fn {pattern, matcher, deliveries, _subscriber_count}, count ->
        if pattern_matches?(channel, matcher) do
          bytes = pattern_delivery_bytes(pattern, channel_size, message_size)

          publish_pattern_deliveries(
            deliveries,
            pattern,
            channel,
            message,
            bytes,
            count
          )
        else
          count
        end
      end,
      0,
      @pattern_cache_table
    )
  end

  defp publish_pattern_indexes(channel, message) do
    channel_size = byte_size(channel)
    message_size = byte_size(message)

    count =
      publish_cached_pattern(
        channel,
        {:exact, channel},
        channel,
        message,
        channel_size,
        message_size,
        0
      )

    count =
      publish_cached_pattern(
        "*",
        :all,
        channel,
        message,
        channel_size,
        message_size,
        count
      )

    index_limit = min(channel_size, @max_pattern_bytes - 1)

    count =
      publish_prefix_index_matches(
        channel,
        1,
        index_limit,
        message,
        channel_size,
        message_size,
        count
      )

    count =
      publish_suffix_index_matches(
        channel,
        1,
        index_limit,
        message,
        channel_size,
        message_size,
        count
      )

    publish_glob_index_matches(channel, message, channel_size, message_size, count)
  end

  defp publish_cached_pattern(
         pattern,
         expected_matcher,
         channel,
         message,
         channel_size,
         message_size,
         count
       ) do
    case :ets.lookup(@pattern_cache_table, pattern) do
      [{^pattern, ^expected_matcher, deliveries, _subscriber_count}] ->
        bytes = pattern_delivery_bytes(pattern, channel_size, message_size)
        publish_pattern_deliveries(deliveries, pattern, channel, message, bytes, count)

      _missing_or_different_matcher ->
        count
    end
  end

  defp publish_indexed_pattern(
         pattern,
         channel,
         message,
         channel_size,
         message_size,
         count
       ) do
    case :ets.lookup(@pattern_cache_table, pattern) do
      [{^pattern, _matcher, deliveries, _subscriber_count}] ->
        bytes = pattern_delivery_bytes(pattern, channel_size, message_size)
        publish_pattern_deliveries(deliveries, pattern, channel, message, bytes, count)

      [] ->
        count
    end
  end

  defp publish_prefix_index_matches(
         _channel,
         position,
         limit,
         _message,
         _channel_size,
         _message_size,
         count
       )
       when position > limit,
       do: count

  defp publish_prefix_index_matches(
         channel,
         position,
         limit,
         message,
         channel_size,
         message_size,
         count
       ) do
    prefix = binary_part(channel, 0, position)

    count =
      @pattern_prefix_index_table
      |> :ets.lookup(prefix)
      |> publish_pattern_index_candidates(
        channel,
        message,
        channel_size,
        message_size,
        count
      )

    publish_prefix_index_matches(
      channel,
      position + 1,
      limit,
      message,
      channel_size,
      message_size,
      count
    )
  end

  defp publish_suffix_index_matches(
         _channel,
         length,
         limit,
         _message,
         _channel_size,
         _message_size,
         count
       )
       when length > limit,
       do: count

  defp publish_suffix_index_matches(
         channel,
         length,
         limit,
         message,
         channel_size,
         message_size,
         count
       ) do
    suffix = binary_part(channel, channel_size - length, length)

    count =
      @pattern_suffix_index_table
      |> :ets.lookup(suffix)
      |> publish_pattern_index_candidates(
        channel,
        message,
        channel_size,
        message_size,
        count
      )

    publish_suffix_index_matches(
      channel,
      length + 1,
      limit,
      message,
      channel_size,
      message_size,
      count
    )
  end

  defp publish_glob_index_matches(channel, message, channel_size, message_size, count) do
    :ets.foldl(
      fn {pattern}, acc ->
        if Ferricstore.GlobMatcher.match?(channel, pattern) do
          publish_indexed_pattern(
            pattern,
            channel,
            message,
            channel_size,
            message_size,
            acc
          )
        else
          acc
        end
      end,
      count,
      @pattern_glob_index_table
    )
  end

  defp publish_pattern_index_candidates(
         [{_anchor, pattern, :direct} | rest],
         channel,
         message,
         channel_size,
         message_size,
         count
       ) do
    next_count =
      publish_indexed_pattern(pattern, channel, message, channel_size, message_size, count)

    publish_pattern_index_candidates(
      rest,
      channel,
      message,
      channel_size,
      message_size,
      next_count
    )
  end

  defp publish_pattern_index_candidates(
         [{_anchor, pattern, :glob} | rest],
         channel,
         message,
         channel_size,
         message_size,
         count
       ) do
    next_count =
      if Ferricstore.GlobMatcher.match?(channel, pattern) do
        publish_indexed_pattern(pattern, channel, message, channel_size, message_size, count)
      else
        count
      end

    publish_pattern_index_candidates(
      rest,
      channel,
      message,
      channel_size,
      message_size,
      next_count
    )
  end

  defp publish_pattern_index_candidates(
         [],
         _channel,
         _message,
         _channel_size,
         _message_size,
         count
       ),
       do: count

  defp pattern_delivery_bytes(pattern, channel_size, message_size) do
    byte_size(pattern) + channel_size + message_size + @delivery_overhead_bytes
  end

  defp publish_exact_deliveries([delivery | rest], channel, message, bytes, count) do
    next_count =
      case deliver_exact(delivery, channel, message, bytes) do
        :sent -> count + 1
        :rejected -> count
      end

    publish_exact_deliveries(rest, channel, message, bytes, next_count)
  end

  defp publish_exact_deliveries([], _channel, _message, _bytes, count), do: count

  defp same_channel_messages([], _channel, acc), do: {:ok, Enum.reverse(acc)}

  defp same_channel_messages([{channel, message} | rest], channel, acc)
       when is_binary(message),
       do: same_channel_messages(rest, channel, [message | acc])

  defp same_channel_messages(_mixed_or_invalid, _channel, _acc), do: :mixed

  defp publish_same_channel_many(channel, messages) do
    case :ets.info(@pattern_cache_table, :size) do
      0 ->
        counts = publish_exact_same_channel_many(channel, messages)
        record_publish_batch(channel, messages, counts)
        counts

      _pattern_count ->
        # Preserve the established exact/pattern interleaving when any pattern
        # subscription could also observe this batch.
        Enum.map(messages, &publish(channel, &1))
    end
  end

  defp publish_exact_same_channel_many(channel, messages) do
    case :ets.lookup(@channel_cache_table, channel) do
      [{^channel, deliveries, _subscriber_count}] ->
        batch_bytes = exact_batch_delivery_bytes(channel, messages)
        publish_exact_batch_deliveries(deliveries, channel, messages, batch_bytes)

      [] ->
        List.duplicate(0, length(messages))
    end
  end

  defp exact_batch_delivery_bytes(channel, messages) do
    channel_bytes = byte_size(channel) + @delivery_overhead_bytes
    Enum.reduce(messages, 0, fn message, bytes -> bytes + channel_bytes + byte_size(message) end)
  end

  defp publish_exact_batch_deliveries(deliveries, channel, messages, batch_bytes) do
    {complete_deliveries, partial_counts} =
      publish_exact_batch_deliveries(
        deliveries,
        channel,
        messages,
        batch_bytes,
        0,
        List.duplicate(0, length(messages))
      )

    Enum.map(partial_counts, &(&1 + complete_deliveries))
  end

  defp publish_exact_batch_deliveries(
         [pid | rest],
         channel,
         messages,
         batch_bytes,
         complete_deliveries,
         partial_counts
       )
       when is_pid(pid) do
    Enum.each(messages, &send(pid, {:pubsub_message, channel, &1}))

    publish_exact_batch_deliveries(
      rest,
      channel,
      messages,
      batch_bytes,
      complete_deliveries + 1,
      partial_counts
    )
  end

  defp publish_exact_batch_deliveries(
         [{pid, guard} = delivery | rest],
         channel,
         messages,
         batch_bytes,
         complete_deliveries,
         partial_counts
       ) do
    case reserve_guarded_delivery(guard, batch_bytes) do
      {:ok, nil} ->
        send(pid, {:pubsub_messages, channel, messages})

        publish_exact_batch_deliveries(
          rest,
          channel,
          messages,
          batch_bytes,
          complete_deliveries + 1,
          partial_counts
        )

      {:ok, lease} ->
        send(pid, {:pubsub_messages, channel, messages, lease})

        publish_exact_batch_deliveries(
          rest,
          channel,
          messages,
          batch_bytes,
          complete_deliveries + 1,
          partial_counts
        )

      :rejected ->
        delivery_counts =
          Enum.map(messages, fn message ->
            case deliver_exact(delivery, channel, message, exact_delivery_bytes(channel, message)) do
              :sent -> 1
              :rejected -> 0
            end
          end)

        publish_exact_batch_deliveries(
          rest,
          channel,
          messages,
          batch_bytes,
          complete_deliveries,
          add_publish_counts(partial_counts, delivery_counts, [])
        )
    end
  end

  defp publish_exact_batch_deliveries(
         [],
         _channel,
         _messages,
         _batch_bytes,
         complete_deliveries,
         partial_counts
       ),
       do: {complete_deliveries, partial_counts}

  defp exact_delivery_bytes(channel, message),
    do: byte_size(channel) + byte_size(message) + @delivery_overhead_bytes

  defp add_publish_counts([left | left_rest], [right | right_rest], acc),
    do: add_publish_counts(left_rest, right_rest, [left + right | acc])

  defp add_publish_counts([], [], acc), do: Enum.reverse(acc)

  defp record_publish_batch(channel, [message | messages], [count | counts]) do
    ActivityLog.record_publish(channel, byte_size(message), count)
    record_publish_batch(channel, messages, counts)
  end

  defp record_publish_batch(_channel, [], []), do: :ok

  defp deliver_exact(pid, channel, message, _bytes) when is_pid(pid) do
    send(pid, {:pubsub_message, channel, message})
    :sent
  end

  defp deliver_exact({pid, guard}, channel, message, bytes) do
    case reserve_guarded_delivery(guard, bytes) do
      {:ok, nil} ->
        send(pid, {:pubsub_message, channel, message})
        :sent

      {:ok, lease} ->
        send(pid, {:pubsub_message, channel, message, lease})
        :sent

      :rejected ->
        reject_slow_subscriber(pid)
    end
  end

  defp publish_pattern_deliveries(
         [delivery | rest],
         pattern,
         channel,
         message,
         bytes,
         count
       ) do
    next_count =
      case deliver_pattern(delivery, pattern, channel, message, bytes) do
        :sent -> count + 1
        :rejected -> count
      end

    publish_pattern_deliveries(rest, pattern, channel, message, bytes, next_count)
  end

  defp publish_pattern_deliveries(
         [],
         _pattern,
         _channel,
         _message,
         _bytes,
         count
       ),
       do: count

  defp deliver_pattern(pid, pattern, channel, message, _bytes) when is_pid(pid) do
    send(pid, {:pubsub_pmessage, pattern, channel, message})
    :sent
  end

  defp deliver_pattern({pid, guard}, pattern, channel, message, bytes) do
    case reserve_guarded_delivery(guard, bytes) do
      {:ok, nil} ->
        send(pid, {:pubsub_pmessage, pattern, channel, message})
        :sent

      {:ok, lease} ->
        send(pid, {:pubsub_pmessage, pattern, channel, message, lease})
        :sent

      :rejected ->
        reject_slow_subscriber(pid)
    end
  end

  defp reserve_guarded_delivery(guard, bytes) do
    case guard.(bytes) do
      {:ok, lease} -> {:ok, lease}
      {:error, _reason} -> :rejected
      _invalid -> :rejected
    end
  rescue
    _error -> :rejected
  catch
    _kind, _reason -> :rejected
  end

  defp reject_slow_subscriber(pid) do
    Process.exit(pid, {:shutdown, :pubsub_outbound_overflow})
    :rejected
  end

  defp pattern_matcher(pattern) do
    if byte_size(pattern) > @max_pattern_bytes do
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

  defp delete_complex_glob_index(pattern) do
    case complex_glob_anchor(pattern) do
      {:prefix, prefix} ->
        :ets.delete_object(@pattern_prefix_index_table, {prefix, pattern, :glob})

      {:suffix, suffix} ->
        :ets.delete_object(@pattern_suffix_index_table, {suffix, pattern, :glob})

      :fallback ->
        :ets.delete(@pattern_glob_index_table, pattern)
    end
  end

  defp complex_glob_anchor(pattern) do
    prefix = glob_literal_prefix(pattern, 0, byte_size(pattern), [])
    suffix = glob_literal_suffix(pattern, 0, byte_size(pattern), [])

    cond do
      byte_size(prefix) >= byte_size(suffix) and prefix != "" -> {:prefix, prefix}
      suffix != "" -> {:suffix, suffix}
      true -> :fallback
    end
  end

  defp glob_literal_prefix(pattern, position, size, acc) when position < size do
    case :binary.at(pattern, position) do
      ?\\ when position + 1 < size ->
        glob_literal_prefix(
          pattern,
          position + 2,
          size,
          [:binary.at(pattern, position + 1) | acc]
        )

      ?\\ ->
        glob_literal_prefix(pattern, position + 1, size, [?\\ | acc])

      special when special in [?*, ??, ?[] ->
        acc |> Enum.reverse() |> :erlang.list_to_binary()

      literal ->
        glob_literal_prefix(pattern, position + 1, size, [literal | acc])
    end
  end

  defp glob_literal_prefix(_pattern, _position, _size, acc),
    do: acc |> Enum.reverse() |> :erlang.list_to_binary()

  defp glob_literal_suffix(pattern, position, size, acc) when position < size do
    case :binary.at(pattern, position) do
      ?\\ when position + 1 < size ->
        glob_literal_suffix(
          pattern,
          position + 2,
          size,
          [:binary.at(pattern, position + 1) | acc]
        )

      ?\\ ->
        glob_literal_suffix(pattern, position + 1, size, [?\\ | acc])

      special when special in [?*, ??] ->
        glob_literal_suffix(pattern, position + 1, size, [])

      ?[ ->
        case glob_character_class_end(pattern, position + 1, size) do
          {:ok, next_position} ->
            glob_literal_suffix(pattern, next_position, size, [])

          :error ->
            ""
        end

      literal ->
        glob_literal_suffix(pattern, position + 1, size, [literal | acc])
    end
  end

  defp glob_literal_suffix(_pattern, _position, _size, acc),
    do: acc |> Enum.reverse() |> :erlang.list_to_binary()

  defp glob_character_class_end(pattern, position, size) when position < size do
    case :binary.at(pattern, position) do
      ?\\ when position + 1 < size ->
        glob_character_class_end(pattern, position + 2, size)

      ?] ->
        {:ok, position + 1}

      _class_byte ->
        glob_character_class_end(pattern, position + 1, size)
    end
  end

  defp glob_character_class_end(_pattern, _position, _size), do: :error

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
