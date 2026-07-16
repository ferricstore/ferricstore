defmodule FerricstoreServer.Health.Dashboard.LivePayload do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Access, as: DashboardAccess
  alias FerricstoreServer.Health.Dashboard.Data.{KV, Messaging, Operational}
  alias FerricstoreServer.Health.Dashboard.Flow.{Browse, Detail, Projection, Query}
  alias FerricstoreServer.Health.Endpoint.FlowPaths
  alias FerricstoreServer.Health.QueryDecoder

  import FerricstoreServer.Health.Dashboard.Flow.Calls

  import FerricstoreServer.Health.Dashboard.Format,
    only: [dashboard_internal_error: 2, dashboard_internal_error: 3]

  import FerricstoreServer.Health.Dashboard.Layout
  import FerricstoreServer.Health.Dashboard.Render.Admin
  import FerricstoreServer.Health.Dashboard.Render.FlowCharts
  import FerricstoreServer.Health.Dashboard.Render.FlowDetail
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory
  import FerricstoreServer.Health.Dashboard.Render.FlowOverview
  import FerricstoreServer.Health.Dashboard.Render.FlowTables
  import FerricstoreServer.Health.Dashboard.Render.KVPages, except: [kv_command_groups: 0]
  import FerricstoreServer.Health.Dashboard.Render.MessagingPages
  import FerricstoreServer.Health.Dashboard.Render.Overview
  import FerricstoreServer.Health.Dashboard.Render.Prefixes

  @spec overview_payload(map()) :: map()
  def overview_payload(data) do
    %{
      generated_at_ms: Map.get(data, :generated_at_ms, System.system_time(:millisecond)),
      components: %{
        "top_bar" => render_top_bar(data),
        "sidebar" => render_sidebar(data, "overview"),
        "content" => render_overview_content(data),
        "footer" => render_footer(data)
      }
    }
  end

  @spec flow_payload(map()) :: map()
  def flow_payload(data) do
    %{
      generated_at_ms: Map.get(data, :generated_at_ms, System.system_time(:millisecond)),
      components: render_flow_live_components(data)
    }
  end

  @spec live_payload(binary()) :: {:ok, map()} | :not_found
  @spec live_payload(binary(), keyword() | map()) :: {:ok, map()} | :not_found
  def live_payload("keyspace", opts) do
    data = KV.collect_keyspace_page(opts)

    live_component_payload(%{
      "keyspace_inspector" => render_keyspace_inspector(data.inspected),
      "keyspace_table" => render_keyspace_table(data)
    })
  end

  def live_payload("keyspace?" <> query, opts) do
    data =
      query
      |> QueryDecoder.decode()
      |> Map.merge(DashboardAccess.keyspace_live_payload_opts(opts))
      |> KV.collect_keyspace_page()

    live_component_payload(%{
      "keyspace_inspector" => render_keyspace_inspector(data.inspected),
      "keyspace_table" => render_keyspace_table(data)
    })
  end

  def live_payload("flow/states", opts), do: live_flow_states_payload("", opts)
  def live_payload("flow/states?" <> query, opts), do: live_flow_states_payload(query, opts)

  def live_payload("flow/workers", opts) do
    data = Browse.collect_workers_page(DashboardAccess.flow_acl_opts(opts))
    live_flow_workers_payload(data)
  end

  def live_payload("flow/due", opts) do
    data = Browse.collect_due_page(DashboardAccess.flow_acl_opts(opts))
    live_flow_due_payload(data)
  end

  def live_payload("flow/signals", opts), do: live_flow_signals_payload("", opts)
  def live_payload("flow/signals?" <> query, opts), do: live_flow_signals_payload(query, opts)
  def live_payload("flow/value?" <> query, opts), do: live_flow_value_payload(query, opts)
  def live_payload("flow/value", opts), do: live_flow_value_payload("", opts)

  def live_payload("flow/" <> encoded_id, opts),
    do: live_flow_detail_payload(encoded_id, opts)

  def live_payload("streams", opts) do
    data = Messaging.collect_streams_page(opts)

    live_component_payload(%{
      "streams_summary" => render_stream_activity_summary(data),
      "streams_top" => render_stream_top_streams(data),
      "streams_consumers" => render_stream_consumers(data),
      "streams_waiters" => render_stream_waiters(data),
      "streams_log" => render_stream_activity_log(data)
    })
  end

  def live_payload("pubsub", opts) do
    data = Messaging.collect_pubsub_page(opts)

    live_component_payload(%{
      "pubsub_summary" => render_pubsub_summary(data),
      "pubsub_channels" => render_pubsub_channels(data),
      "pubsub_patterns" => render_pubsub_patterns(data),
      "pubsub_activity" => render_pubsub_activity(data)
    })
  end

  def live_payload(path, _opts), do: live_payload(path)

  def live_payload("slowlog") do
    data = Operational.collect_slowlog_page()

    live_component_payload(%{
      "slowlog_summary" => render_slowlog_summary(data.slowlog),
      "slowlog_table" => render_slowlog_table(data.slowlog)
    })
  end

  def live_payload("merge") do
    data = Operational.collect_merge_page()

    live_component_payload(%{
      "merge_summary" => render_merge_summary(data.merge),
      "merge_table" => render_merge_table(data.merge)
    })
  end

  def live_payload("raft") do
    data = Operational.collect_raft_page()

    live_component_payload(%{
      "cluster_info" => render_cluster_info(data.cluster),
      "consensus_summary" => render_consensus_summary(data.raft_shards),
      "raft_table" => render_raft_table(data.raft_shards)
    })
  end

  def live_payload("clients") do
    data = Operational.collect_clients_page()

    live_component_payload(%{
      "clients_summary" => render_clients_summary(data.connections, data.clients),
      "clients_table" => render_clients_table(data.clients)
    })
  end

  def live_payload("storage") do
    data = Operational.collect_storage_page()

    live_component_payload(%{
      "storage_summary" => render_storage_summary(data),
      "storage_table" => render_storage_table(data.shards)
    })
  end

  def live_payload("keyspace") do
    data = KV.collect_keyspace_page()

    live_component_payload(%{
      "keyspace_inspector" => render_keyspace_inspector(data.inspected),
      "keyspace_table" => render_keyspace_table(data)
    })
  end

  def live_payload("keyspace?" <> query) do
    data = KV.collect_keyspace_page(QueryDecoder.decode(query))

    live_component_payload(%{
      "keyspace_inspector" => render_keyspace_inspector(data.inspected),
      "keyspace_table" => render_keyspace_table(data)
    })
  end

  def live_payload("commands") do
    data = KV.collect_commands_page()

    live_component_payload(%{
      "commands_summary" => render_commands_summary(data),
      "commands_slowlog" => render_command_slowlog_table(data)
    })
  end

  def live_payload("reads") do
    data = KV.collect_reads_page()

    live_component_payload(%{
      "reads_summary" => render_reads_summary(data),
      "reads_prefixes" => render_read_prefix_table(data)
    })
  end

  def live_payload("streams") do
    data = Messaging.collect_streams_page()

    live_component_payload(%{
      "streams_summary" => render_stream_activity_summary(data),
      "streams_top" => render_stream_top_streams(data),
      "streams_consumers" => render_stream_consumers(data),
      "streams_waiters" => render_stream_waiters(data),
      "streams_log" => render_stream_activity_log(data)
    })
  end

  def live_payload("pubsub") do
    data = Messaging.collect_pubsub_page()

    live_component_payload(%{
      "pubsub_summary" => render_pubsub_summary(data),
      "pubsub_channels" => render_pubsub_channels(data),
      "pubsub_patterns" => render_pubsub_patterns(data),
      "pubsub_activity" => render_pubsub_activity(data)
    })
  end

  def live_payload("prefixes") do
    data = KV.collect_prefixes_page()

    live_component_payload(%{
      "prefixes_summary" => render_prefixes_summary(data),
      "prefixes_table" => render_prefixes_table(data)
    })
  end

  def live_payload("flow/states"), do: live_flow_states_payload("")
  def live_payload("flow/states?" <> query), do: live_flow_states_payload(query)

  def live_payload("flow/workers") do
    Browse.collect_workers_page()
    |> live_flow_workers_payload()
  end

  def live_payload("flow/due") do
    Browse.collect_due_page()
    |> live_flow_due_payload()
  end

  def live_payload("flow/signals"), do: live_flow_signals_payload("")
  def live_payload("flow/signals?" <> query), do: live_flow_signals_payload(query)
  def live_payload("flow/projections"), do: :not_found
  def live_payload("flow/value?" <> query), do: live_flow_value_payload(query)
  def live_payload("flow/value"), do: live_flow_value_payload("")

  def live_payload("flow/" <> encoded_id), do: live_flow_detail_payload(encoded_id, [])
  def live_payload(_path), do: :not_found

  defp live_flow_detail_payload(encoded_id, access_opts) do
    {id, opts} = decode_flow_detail_request(encoded_id)

    opts =
      opts
      |> Keyword.put(:values, false)
      |> Keyword.merge(DashboardAccess.flow_acl_opts(access_opts))

    data = Detail.collect_page(id, opts)

    live_component_payload(%{
      "flow_detail" => render_flow_detail(data),
      "flow_debug" => render_flow_debug(data),
      "flow_history" =>
        render_flow_history_timeline(
          data.history,
          data.history_status,
          Map.get(data, :history_page)
        ),
      "flow_timeline_chart" => render_flow_timeline_chart(data.history)
    })
  end

  defp live_flow_workers_payload(data) do
    live_component_payload(%{
      "flow_workers_chart" => render_flow_workers_chart(data.workers),
      "flow_workers" => render_flow_workers(data.workers),
      "flow_fifo_lanes" =>
        render_flow_fifo_lanes(
          Map.get(data, :fifo_lanes, []),
          data.total_sampled,
          data.sample_limit
        ),
      "flow_running_records" =>
        render_flow_running_records(data.running_records, data.total_sampled, data.sample_limit)
    })
  end

  defp live_flow_due_payload(data) do
    live_component_payload(%{
      "flow_due_chart" => render_flow_due_chart(data.due_now, data.scheduled),
      "flow_fifo_lanes" =>
        render_flow_fifo_lanes(
          Map.get(data, :fifo_lanes, []),
          data.total_sampled,
          data.sample_limit
        ),
      "flow_due_now" =>
        render_flow_due_records("Due Now", data.due_now, data.total_sampled, data.sample_limit),
      "flow_scheduled" =>
        render_flow_due_records(
          "Scheduled Future",
          data.scheduled,
          data.total_sampled,
          data.sample_limit
        )
    })
  end

  defp live_flow_value_payload(query, access_opts \\ []) do
    params = QueryDecoder.decode(query)
    ref = params |> Map.get("ref", "") |> String.trim()
    flow_id = params |> Map.get("flow", "") |> String.trim()
    partition_key = params |> Map.get("partition_key", "") |> String.trim()

    cond do
      ref == "" ->
        {:ok, live_flow_value_error("", "missing value ref")}

      flow_id == "" ->
        {:ok, live_flow_value_error(ref, "missing flow id")}

      true ->
        opts =
          if partition_key == "",
            do: [values: false],
            else: [values: false, partition_key: partition_key]

        opts = Keyword.merge(opts, DashboardAccess.flow_acl_opts(access_opts))
        data = Detail.collect_page(flow_id, opts)
        visible_refs = flow_detail_value_refs(data.record, data.history) |> MapSet.new(& &1.ref)

        if MapSet.member?(visible_refs, ref) do
          live_flow_value_payload_from_ref(ref)
        else
          {:ok, live_flow_value_error(ref, "value ref is not visible on this Flow detail page")}
        end
    end
  rescue
    error ->
      {:ok, live_flow_value_error("", dashboard_internal_error("value lookup failed", error))}
  catch
    :exit, error ->
      {:ok,
       live_flow_value_error("", dashboard_internal_error("value lookup exited", :exit, error))}
  end

  defp live_flow_value_payload_from_ref(ref) do
    timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_value_mget([ref]) end,
           timeout_ms,
           :value
         ) do
      {:ok, {:ok, [value]}} ->
        {:ok,
         %{
           generated_at_ms: System.system_time(:millisecond),
           status: "ok",
           ref: ref,
           value: flow_value_preview(value)
         }}

      {:ok, {:ok, _values}} ->
        {:ok, live_flow_value_error(ref, "unexpected value result count")}

      {:ok, {:error, reason}} ->
        {:ok, live_flow_value_error(ref, dashboard_internal_error("value lookup failed", reason))}

      {:ok, _other} ->
        {:ok, live_flow_value_error(ref, "unexpected value lookup result")}

      {:error, :timeout} ->
        {:ok, live_flow_value_error(ref, "value lookup timed out")}

      {:error, reason} ->
        {:ok, live_flow_value_error(ref, dashboard_internal_error("value lookup failed", reason))}
    end
  end

  defp live_flow_value_error(ref, message) do
    %{
      generated_at_ms: System.system_time(:millisecond),
      status: "error",
      ref: ref,
      error: message,
      value: message
    }
  end

  defp live_flow_signals_payload(query, opts \\ []) do
    data =
      query
      |> Query.signals_opts_from_query()
      |> Keyword.merge(DashboardAccess.flow_acl_opts(opts))
      |> Query.collect_signals_page()

    live_component_payload(%{
      "flow_signals_table" =>
        render_flow_signals_table(
          data.signals,
          data.total_sampled,
          data.filtered_sampled,
          data.sample_limit,
          data.filters
        )
    })
  end

  defp live_flow_states_payload(query, opts \\ []) do
    data =
      query
      |> Browse.states_opts_from_query()
      |> Keyword.merge(DashboardAccess.flow_acl_opts(opts))
      |> Browse.collect_states_page()

    live_component_payload(%{
      "flow_states_chart" => render_flow_states_chart(data.states),
      "flow_states_table" =>
        render_flow_states_table(
          data.states,
          data.total_sampled,
          data.filtered_sampled,
          data.sample_limit,
          data.filters
        ),
      "flow_fifo_lanes" =>
        render_flow_fifo_lanes(
          Map.get(data, :fifo_lanes, []),
          data.total_sampled,
          data.sample_limit
        ),
      "flow_recent_records" => render_flow_recent_records(data.records, data.limit)
    })
  end

  defp decode_flow_detail_request(encoded_id_with_query) do
    FlowPaths.decode_flow_detail_request(encoded_id_with_query)
  end

  defp live_component_payload(components) do
    {:ok, %{generated_at_ms: System.system_time(:millisecond), components: components}}
  end

  defp render_overview_content(data) do
    """
    #{render_cache_performance(data.hotcold)}
    #{render_lifecycle(data.lifecycle)}
    #{render_shards(data.shards)}
    #{render_memory_alert(data.memory)}
    #{render_connections(data.connections)}
    """
  end

  defp render_flow_live_components(data) do
    %{
      "flow_overview" =>
        render_flow_overview(data.summary, data.filtered_sampled, data.sample_limit),
      "flow_issue_cards" => render_flow_issue_cards(data.summary),
      "flow_projection_health" =>
        render_flow_projection_health(Map.get(data, :projection, Projection.default_health())),
      "flow_state_breakdown" => render_flow_state_breakdown(data.types),
      "flow_workers" => render_flow_workers(data.workers),
      "flow_recent_records" => render_flow_recent_records(data.records)
    }
  end
end
