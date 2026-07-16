defmodule FerricstoreServer.Health.Dashboard.Data.KV do
  @moduledoc false

  alias Ferricstore.Stats
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Store.{BlobRef, CompoundKey}
  alias FerricstoreServer.Health.Dashboard.Access
  alias FerricstoreServer.Health.Dashboard.Data.Operational

  import FerricstoreServer.Health.Dashboard.Format, only: [format_bytes: 1, format_duration_ms: 1]

  import FerricstoreServer.Health.Dashboard.QueryParams,
    only: [dashboard_param: 2, truthy_dashboard_param?: 1]

  import FerricstoreServer.Health.Dashboard.Render.KVPages, only: [kv_command_groups: 0]

  @keyspace_dashboard_default_limit 50
  @keyspace_dashboard_max_limit 500
  @keyspace_dashboard_max_scan 10_000
  @keyspace_dashboard_select_batch 256

  def collect_prefixes_page do
    hotness = Stats.hotness_top(20)
    hotness_map = Map.new(hotness, fn {prefix, hot, cold, _pct} -> {prefix, {hot, cold}} end)
    {prefix_counts, total_sampled} = sample_prefix_counts()
    total_keys = Enum.reduce(prefix_counts, 0, fn {_prefix, count}, acc -> acc + count end)

    prefixes =
      prefix_counts
      |> Enum.map(fn {prefix, count} ->
        pct = if total_keys > 0, do: Float.round(count / total_keys * 100, 1), else: 0.0
        {hot, cold} = Map.get(hotness_map, prefix, {0, 0})
        %{prefix: prefix, keys: count, pct: pct, hot_reads: hot, cold_reads: cold}
      end)
      |> Enum.sort_by(fn p -> p.keys end, :desc)
      |> Enum.take(50)

    %{prefixes: prefixes, total_sampled: total_sampled}
  end

  def collect_keyspace_page(opts \\ []) do
    filters = keyspace_filters(opts)
    acl_username = Access.keyspace_acl_username(opts)
    {rows, sampled} = collect_keyspace_rows(filters)
    rows = Access.filter_keyspace_rows_for_acl(rows, acl_username)
    visible_sampled = if is_binary(acl_username), do: length(rows), else: sampled

    %{
      filters: filters,
      rows: rows,
      inspected: inspect_keyspace_key(filters.key, rows),
      total_sampled: visible_sampled
    }
  end

  def collect_commands_page do
    slowlog = Operational.collect_slowlog()
    uptime = max(Stats.uptime_seconds(), 1)

    %{
      summary: %{
        total_commands: Stats.total_commands(),
        ops_per_sec: Float.round(Stats.total_commands() / uptime, 1),
        slowlog_entries: length(slowlog),
        slowest_us: Enum.reduce(slowlog, 0, fn entry, acc -> max(acc, entry.duration_us) end)
      },
      slow_by_command: group_slowlog_by_command(slowlog),
      command_groups: kv_command_groups()
    }
  end

  def collect_reads_page do
    prefixes =
      Stats.hotness_top(50)
      |> Enum.map(fn {prefix, hot, cold, cold_pct} ->
        %{prefix: prefix, hot_reads: hot, cold_reads: cold, cold_pct: cold_pct}
      end)

    %{hotcold: Operational.collect_hotcold(), prefixes: prefixes}
  end

  def keyspace_filters(opts) do
    key = dashboard_param(opts, "key") |> String.trim()
    prefix = dashboard_param(opts, "prefix") |> String.trim()

    %{
      key: key,
      prefix: prefix,
      include_internal: truthy_dashboard_param?(dashboard_param(opts, "include_internal")),
      limit:
        opts
        |> dashboard_param("limit")
        |> parse_bounded_int(@keyspace_dashboard_default_limit, 1, @keyspace_dashboard_max_limit)
    }
  end

  defp sample_prefix_counts do
    {counts_map, total, _scanned} =
      Enum.reduce_while(
        0..(shard_count() - 1),
        {%{}, 0, 0},
        fn index, {counts, total, scanned} ->
          budget = @keyspace_dashboard_max_scan - scanned

          if budget <= 0 do
            {:halt, {counts, total, scanned}}
          else
            reducer = fn {key, _val, _exp, _lfu, _fid, _off, _vsize}, {acc, count} ->
              if InternalKey.reserved?(key) do
                {:cont, {acc, count}}
              else
                prefix = Stats.extract_prefix(key)
                {:cont, {Map.update(acc, prefix, 1, &(&1 + 1)), count + 1}}
              end
            end

            {{counts, total}, shard_scanned} =
              bounded_keydir_reduce(:"keydir_#{index}", budget, {counts, total}, reducer)

            {:cont, {counts, total, scanned + shard_scanned}}
          end
        end
      )

    {Enum.to_list(counts_map), total}
  end

  defp collect_keyspace_rows(%{key: key} = filters) when key != "" do
    if InternalKey.reserved?(key) do
      {[], 0}
    else
      rows =
        0..(shard_count() - 1)
        |> Enum.flat_map(fn index ->
          keydir = :"keydir_#{index}"

          [key, CompoundKey.type_key(key), CompoundKey.list_meta_key(key)]
          |> Enum.flat_map(&lookup_keyspace_row(keydir, index, &1))
        end)
        |> Enum.uniq_by(& &1.physical_key)
        |> Enum.reject(&(not filters.include_internal and &1.internal? and &1.key != key))
        |> Enum.take(filters.limit)

      {rows, length(rows)}
    end
  end

  defp collect_keyspace_rows(filters) do
    {rows, scanned} =
      Enum.reduce_while(0..(shard_count() - 1), {[], 0}, fn index, {rows, scanned} ->
        budget = @keyspace_dashboard_max_scan - scanned

        if length(rows) >= filters.limit or budget <= 0 do
          {:halt, {rows, scanned}}
        else
          reducer = fn entry, acc ->
            row = keyspace_entry_row(index, entry)

            if keyspace_row_matches?(row, filters) do
              rows = [row | acc]
              if length(rows) >= filters.limit, do: {:halt, rows}, else: {:cont, rows}
            else
              {:cont, acc}
            end
          end

          {rows, shard_scanned} =
            bounded_keydir_reduce(:"keydir_#{index}", budget, rows, reducer)

          {:cont, {rows, scanned + shard_scanned}}
        end
      end)

    {rows |> Enum.reverse() |> Enum.take(filters.limit), scanned}
  end

  defp lookup_keyspace_row(keydir, index, key) do
    try do
      keydir |> :ets.lookup(key) |> Enum.map(&keyspace_entry_row(index, &1))
    rescue
      ArgumentError -> []
    catch
      :exit, _ -> []
    end
  end

  defp bounded_keydir_reduce(_keydir, budget, acc, _reducer) when budget <= 0,
    do: {acc, 0}

  defp bounded_keydir_reduce(keydir, budget, acc, reducer) do
    match_spec = [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"}, [], [:"$_"]}]

    try do
      page_size = min(@keyspace_dashboard_select_batch, budget)

      case :ets.select(keydir, match_spec, page_size) do
        :"$end_of_table" ->
          {acc, 0}

        {entries, continuation} ->
          continue_bounded_reduce(entries, continuation, budget, acc, 0, reducer)
      end
    rescue
      ArgumentError -> {acc, 0}
    catch
      :exit, _ -> {acc, 0}
    end
  end

  defp continue_bounded_reduce(_entries, _continuation, 0, acc, scanned, _reducer),
    do: {acc, scanned}

  defp continue_bounded_reduce([entry | rest], continuation, budget, acc, scanned, reducer) do
    case reducer.(entry, acc) do
      {:cont, acc} ->
        continue_bounded_reduce(rest, continuation, budget - 1, acc, scanned + 1, reducer)

      {:halt, acc} ->
        {acc, scanned + 1}
    end
  end

  defp continue_bounded_reduce([], continuation, budget, acc, scanned, reducer) do
    case continuation do
      :"$end_of_table" ->
        {acc, scanned}

      continuation ->
        case :ets.select(continuation) do
          :"$end_of_table" ->
            {acc, scanned}

          {entries, next_continuation} ->
            continue_bounded_reduce(
              entries,
              next_continuation,
              budget,
              acc,
              scanned,
              reducer
            )
        end
    end
  end

  defp keyspace_entry_row(
         index,
         {physical_key, value, expire_at_ms, lfu, file_id, offset, value_size}
       ) do
    %{
      key: keyspace_logical_key(physical_key),
      physical_key: physical_key,
      shard: index,
      type: keyspace_entry_type(physical_key, value),
      ttl: keyspace_ttl_label(expire_at_ms),
      size: keyspace_size_label(value, value_size),
      location: keyspace_location_label(value, file_id),
      lfu: lfu,
      offset: offset,
      internal?: CompoundKey.internal_key?(physical_key)
    }
  end

  defp keyspace_logical_key(key), do: CompoundKey.extract_redis_key(key)

  defp keyspace_row_matches?(row, filters) do
    internal_ok? =
      not InternalKey.reserved?(row.physical_key) and
        (filters.include_internal or not row.internal?)

    prefix_ok? = filters.prefix == "" or String.starts_with?(row.key, filters.prefix)
    internal_ok? and prefix_ok?
  end

  defp keyspace_entry_type(<<"T:", _rest::binary>>, value) when is_binary(value), do: value
  defp keyspace_entry_type(<<"H:", _rest::binary>>, _value), do: "hash field"
  defp keyspace_entry_type(<<"L:", _rest::binary>>, _value), do: "list element"
  defp keyspace_entry_type(<<"LM:", _rest::binary>>, _value), do: "list metadata"
  defp keyspace_entry_type(<<"S:", _rest::binary>>, _value), do: "set member"
  defp keyspace_entry_type(<<"Z:", _rest::binary>>, _value), do: "zset member"
  defp keyspace_entry_type(<<"X:", _rest::binary>>, _value), do: "stream record"
  defp keyspace_entry_type(<<"V:", _rest::binary>>, _value), do: "flow value"
  defp keyspace_entry_type(<<"VM:", _rest::binary>>, _value), do: "flow value metadata"
  defp keyspace_entry_type(<<"PM:", _rest::binary>>, _value), do: "promotion marker"
  defp keyspace_entry_type(_key, _value), do: "string"

  defp keyspace_ttl_label(0), do: "none"

  defp keyspace_ttl_label(expire_at_ms) when is_integer(expire_at_ms) do
    remaining = expire_at_ms - System.system_time(:millisecond)
    if remaining > 0, do: format_duration_ms(remaining), else: "expired"
  end

  defp keyspace_ttl_label(_), do: "-"

  defp keyspace_size_label(value, _value_size) when is_binary(value) do
    case BlobRef.decode(value) do
      {:ok, %BlobRef{size: logical_size}} -> "#{format_bytes(logical_size)} blob"
      :error -> format_bytes(byte_size(value))
    end
  end

  defp keyspace_size_label(_value, value_size) when is_integer(value_size) and value_size >= 0,
    do: format_bytes(value_size)

  defp keyspace_size_label(_value, _value_size), do: "-"

  defp keyspace_location_label(value, _file_id) when is_binary(value) do
    if BlobRef.ref?(value), do: "blob ref", else: "hot"
  end

  defp keyspace_location_label(_value, :pending), do: "pending"
  defp keyspace_location_label(_value, {:flow_history, _file_id}), do: "flow history"
  defp keyspace_location_label(_value, {:waraft_segment, _index}), do: "segment cold"
  defp keyspace_location_label(_value, {:waraft_projection, _index}), do: "projection cold"
  defp keyspace_location_label(_value, {:waraft_apply_projection, _index}), do: "projection cold"

  defp keyspace_location_label(_value, file_id) when is_integer(file_id) and file_id >= 0,
    do: "bitcask cold"

  defp keyspace_location_label(_value, _file_id), do: "unknown"

  defp inspect_keyspace_key("", _rows), do: nil

  defp inspect_keyspace_key(key, rows) do
    case Enum.find(rows, &(&1.key == key or &1.physical_key == key)) do
      nil ->
        %{key: key, found?: false, type: "none", ttl: "-", size: "-", location: "-", shard: "-"}

      row ->
        %{
          key: key,
          found?: true,
          type: row.type,
          ttl: row.ttl,
          size: row.size,
          location: row.location,
          shard: row.shard
        }
    end
  end

  defp parse_bounded_int(value, default, min_value, max_value) do
    parsed =
      cond do
        is_integer(value) ->
          value

        is_binary(value) ->
          case Integer.parse(value) do
            {int, ""} -> int
            _ -> default
          end

        true ->
          default
      end

    parsed |> max(min_value) |> min(max_value)
  end

  defp group_slowlog_by_command(entries) do
    entries
    |> Enum.group_by(fn entry ->
      case entry.command do
        [cmd | _] -> cmd |> to_string() |> String.upcase()
        _ -> "(unknown)"
      end
    end)
    |> Enum.map(fn {command, grouped} ->
      total = Enum.reduce(grouped, 0, fn entry, acc -> acc + entry.duration_us end)
      count = length(grouped)

      %{
        command: command,
        count: count,
        worst_us: Enum.reduce(grouped, 0, fn entry, acc -> max(acc, entry.duration_us) end),
        avg_us: if(count > 0, do: div(total, count), else: 0)
      }
    end)
    |> Enum.sort_by(& &1.worst_us, :desc)
  end

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)
end
