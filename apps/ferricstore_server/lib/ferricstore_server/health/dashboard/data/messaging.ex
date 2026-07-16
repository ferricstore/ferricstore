defmodule FerricstoreServer.Health.Dashboard.Data.Messaging do
  @moduledoc false

  alias Ferricstore.Commands.Stream.Groups, as: StreamGroups
  alias Ferricstore.Commands.Stream.Waiters, as: StreamWaiters
  alias Ferricstore.PubSub
  alias Ferricstore.PubSub.ActivityLog, as: PubSubActivityLog
  alias Ferricstore.Stream.ActivityLog
  alias FerricstoreServer.Health.Dashboard.Access

  import FerricstoreServer.Health.Dashboard.QueryParams, only: [dashboard_param: 2]

  @stream_activity_limit 128

  def collect_streams_page(opts \\ []) do
    acl_username = Access.keyspace_acl_username(opts)

    entries =
      @stream_activity_limit
      |> ActivityLog.get()
      |> Access.filter_stream_activity_for_acl(acl_username)

    groups =
      100
      |> StreamGroups.snapshot()
      |> Access.filter_stream_activity_for_acl(acl_username)

    waiters =
      100
      |> StreamWaiters.snapshot()
      |> Access.filter_stream_activity_for_acl(acl_username)

    %{
      summary: stream_activity_summary(entries),
      entries: entries,
      top_streams: top_streams(entries),
      consumer_groups: groups,
      waiters: waiters,
      filters: %{acl_username: dashboard_param(opts, "acl_username")}
    }
  end

  def collect_pubsub_page(opts \\ []) do
    acl_username = Access.keyspace_acl_username(opts)
    snapshot = PubSub.subscription_snapshot(100)

    channels = Access.filter_pubsub_channels_for_acl(snapshot.channels, acl_username)
    patterns = Access.filter_pubsub_channels_for_acl(snapshot.patterns, acl_username)

    activity =
      128
      |> PubSubActivityLog.get()
      |> Access.filter_pubsub_channels_for_acl(acl_username)

    %{
      summary:
        pubsub_summary(
          %{
            snapshot
            | channels: channels,
              patterns: patterns
          },
          acl_username
        ),
      channels: channels,
      patterns: patterns,
      activity: activity,
      filters: %{acl_username: dashboard_param(opts, "acl_username")}
    }
  end

  defp stream_activity_summary(entries) do
    xadd_count = Enum.count(entries, &(&1.command == "XADD"))
    consumer_count = Enum.count(entries, &(&1.role == :consumer))
    mutation_count = length(entries)
    unique_streams = entries |> Enum.map(& &1.key) |> Enum.uniq() |> length()

    latest_at_us =
      entries
      |> Enum.map(& &1.timestamp_us)
      |> Enum.max(fn -> nil end)

    %{
      mutations: mutation_count,
      appends: xadd_count,
      consumer_events: consumer_count,
      unique_streams: unique_streams,
      latest_at_us: latest_at_us
    }
  end

  defp pubsub_summary(snapshot, nil) do
    %{
      channels: length(Map.get(snapshot, :channels, [])),
      patterns: length(Map.get(snapshot, :patterns, [])),
      exact_subscriptions: Map.get(snapshot, :exact_subscriptions, 0),
      pattern_subscriptions: Map.get(snapshot, :pattern_subscriptions, 0),
      active_subscribers: Map.get(snapshot, :active_subscribers, 0)
    }
  end

  defp pubsub_summary(snapshot, _acl_username) do
    channels = Map.get(snapshot, :channels, [])
    patterns = Map.get(snapshot, :patterns, [])

    %{
      channels: length(channels),
      patterns: length(patterns),
      exact_subscriptions: visible_subscription_count(channels),
      pattern_subscriptions: visible_subscription_count(patterns),
      active_subscribers: nil
    }
  end

  defp visible_subscription_count(rows) do
    Enum.reduce(rows, 0, fn row, total -> total + Map.get(row, :subscribers, 0) end)
  end

  defp top_streams(entries) do
    entries
    |> Enum.group_by(& &1.key)
    |> Enum.map(fn {key, grouped} ->
      %{
        key: key,
        mutations: length(grouped),
        appends: Enum.count(grouped, &(&1.command == "XADD")),
        last_entry_id: latest_entry_id(grouped),
        last_at_us: grouped |> Enum.map(& &1.timestamp_us) |> Enum.max(fn -> nil end)
      }
    end)
    |> Enum.sort_by(fn row -> {row.mutations, row.last_at_us || 0} end, :desc)
    |> Enum.take(20)
  end

  defp latest_entry_id(entries) do
    entries
    |> Enum.filter(&(&1.command == "XADD" and is_binary(&1.entry_id)))
    |> Enum.sort_by(& &1.timestamp_us, :desc)
    |> case do
      [%{entry_id: id} | _] -> id
      _ -> nil
    end
  end
end
