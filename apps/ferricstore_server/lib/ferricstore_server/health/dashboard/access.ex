defmodule FerricstoreServer.Health.Dashboard.Access do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.QueryParams, only: [dashboard_param: 2]

  def keyspace_acl_username(opts) do
    case dashboard_param(opts, "acl_username") do
      username when is_binary(username) ->
        username
        |> String.trim()
        |> case do
          "" -> nil
          trimmed -> trimmed
        end

      _ ->
        nil
    end
  end

  def keyspace_live_payload_opts(opts) do
    case keyspace_acl_username(opts) do
      nil -> %{}
      username -> %{"acl_username" => username}
    end
  end

  def flow_acl_opts(opts) do
    case keyspace_acl_username(opts) do
      nil -> []
      username -> [acl_username: username]
    end
  end

  def filter_flow_records_for_acl(records, nil), do: records

  def filter_flow_records_for_acl(records, username) when is_list(records) do
    Enum.filter(records, &flow_record_acl_allowed?(&1, username))
  end

  def flow_query_filter_result_for_acl(result, nil), do: result

  def flow_query_filter_result_for_acl(result, username) when is_map(result) do
    rows = result |> Map.get(:rows, []) |> filter_flow_query_rows_for_acl(username)

    result
    |> Map.put(:rows, rows)
    |> Map.put(:message, "#{length(rows)} visible row(s)")
  end

  def flow_lineage_filter_result_for_acl(result, nil), do: result

  def flow_lineage_filter_result_for_acl(result, username) when is_map(result) do
    Map.update(result, :records, [], &filter_flow_records_for_acl(&1, username))
  end

  def filter_keyspace_rows_for_acl(rows, nil), do: rows

  def filter_keyspace_rows_for_acl(rows, username) do
    Enum.filter(rows, fn row ->
      case FerricstoreServer.Acl.check_key_access(username, row.key, :read) do
        :ok -> true
        {:error, _reason} -> false
      end
    end)
  end

  def filter_stream_activity_for_acl(entries, nil), do: entries

  def filter_stream_activity_for_acl(entries, username) when is_list(entries) do
    Enum.filter(entries, fn
      %{key: key} when is_binary(key) ->
        case FerricstoreServer.Acl.check_key_access(username, key, :read) do
          :ok -> true
          {:error, _reason} -> false
        end

      _entry ->
        false
    end)
  end

  def filter_pubsub_channels_for_acl(rows, nil), do: rows

  def filter_pubsub_channels_for_acl(rows, username) when is_list(rows) do
    Enum.filter(rows, fn
      %{channel: channel} when is_binary(channel) -> pubsub_channel_allowed?(username, channel)
      %{pattern: pattern} when is_binary(pattern) -> pubsub_channel_allowed?(username, pattern)
      %{target: target} when is_binary(target) -> pubsub_channel_allowed?(username, target)
      _row -> false
    end)
  end

  defp pubsub_channel_allowed?(username, channel) do
    case FerricstoreServer.Acl.get_user(username) do
      %{channels: :all} ->
        true

      %{channels: patterns} when is_list(patterns) ->
        FerricstoreServer.Acl.channel_matches_any?(channel, patterns)

      _other ->
        false
    end
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp filter_flow_query_rows_for_acl(rows, username) when is_list(rows) do
    Enum.filter(rows, fn
      row when is_map(row) ->
        case flow_record_acl_keys(row) do
          [] -> true
          _keys -> flow_record_acl_allowed?(row, username)
        end

      _row ->
        true
    end)
  end

  defp flow_record_acl_allowed?(record, username) when is_map(record) do
    keys = flow_record_acl_keys(record)

    keys != [] and
      Enum.any?(keys, fn key ->
        case FerricstoreServer.Acl.check_key_access(username, key, :read) do
          :ok -> true
          {:error, _reason} -> false
        end
      end)
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp flow_record_acl_allowed?(_record, _username), do: false

  defp flow_record_acl_keys(record) when is_map(record) do
    [flow_record_partition_key(record), flow_record_id(record), flow_record_type(record)]
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end
end
