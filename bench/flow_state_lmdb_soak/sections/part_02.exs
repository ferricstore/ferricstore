defmodule FlowStateLMDBSoak.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      defp waraft_hot_flush_kind_stats(table, kind) do
        key = {:waraft_hot_flush_kind, kind}

        {count, items, groups} =
          case :ets.lookup(table, key) do
            [{^key, count, items, groups, _queue_us, _flush_us, _total_us}] ->
              {count, items, groups}

            _ ->
              {0, 0, 0}
          end

        %{
          count: count,
          avg_items: if(count > 0, do: div(items, count), else: 0),
          max_items: counter_max(table, {:max_batch_items, key}),
          avg_groups: if(count > 0, do: div(groups, count), else: 0),
          max_groups: counter_max(table, {:max_batch_groups, key}),
          max_queue_us: counter_max(table, {:max_queue_us, key}),
          max_flush_us: counter_max(table, {:max_flush_us, key}),
          max_total_us: counter_max(table, {:max_total_us, key})
        }
      end

      defp waraft_internal_metric_stats(table, metric) do
        key = {:waraft_internal_metric, metric}

        {count, duration_us} =
          case :ets.lookup(table, key) do
            [{^key, count, duration_us, _items, _bytes}] -> {count, duration_us}
            _ -> {0, 0}
          end

        %{
          avg_us: if(count > 0, do: div(duration_us, count), else: 0),
          max_us: max_us_for_key(table, key)
        }
      end

      defp storage_apply_phase_stats(table, phase) do
        key = {:storage_apply_phase, phase}

        {count, duration_us} =
          case :ets.lookup(table, key) do
            [{^key, count, duration_us, _items, _bytes}] -> {count, duration_us}
            _ -> {0, 0}
          end

        %{
          avg_us: if(count > 0, do: div(duration_us, count), else: 0),
          max_us: max_us_for_key(table, key)
        }
      end

      defp record_timing(table, phase, value) when is_integer(value) and value >= 0 do
        key = {:waraft_flush_timing, phase}

        :ets.update_counter(
          table,
          key,
          [{2, 1}, {3, value}],
          {key, 0, 0}
        )

        update_max(table, {:max_us, key}, value)
      end

      defp record_timing(_table, _phase, _value), do: :ok

      defp latency_bucket(duration_us) do
        Enum.find(@latency_us_buckets, :infinity, fn
          :infinity -> true
          bucket_us -> duration_us <= bucket_us
        end)
      end

      defp flow_latency_snapshot(table) do
        Map.new(@flow_latency_commands, fn command ->
          {command, flow_latency_stats(table, command)}
        end)
      end

      defp flow_latency_sample_rate(table) do
        case :ets.lookup(table, {:config, :flow_latency_sample_rate}) do
          [{{:config, :flow_latency_sample_rate}, rate}] when is_integer(rate) and rate > 0 ->
            rate

          _ ->
            1
        end
      end

      defp flow_latency_stats(table, command) do
        {count, total_us, items} =
          case :ets.lookup(table, {:flow_latency, command}) do
            [{{:flow_latency, ^command}, count, total_us, items}] -> {count, total_us, items}
            _ -> {0, 0, 0}
          end

        max_us =
          case :ets.lookup(table, {:flow_latency_max_us, command}) do
            [{{:flow_latency_max_us, ^command}, value}] when is_integer(value) -> value
            _ -> 0
          end

        %{
          calls: count,
          items: items,
          avg_us: if(count > 0, do: div(total_us, count), else: 0),
          avg_item_us: if(items > 0, do: div(total_us, items), else: 0),
          p50_us: flow_latency_percentile(table, command, count, max_us, 0.50),
          p95_us: flow_latency_percentile(table, command, count, max_us, 0.95),
          p99_us: flow_latency_percentile(table, command, count, max_us, 0.99),
          max_us: max_us
        }
      end

      defp flow_latency_percentile(_table, _command, count, _max_us, _percentile) when count <= 0,
        do: 0

      defp flow_latency_percentile(table, command, count, max_us, percentile) do
        target = max(ceil(count * percentile), 1)

        @latency_us_buckets
        |> Enum.reduce_while(0, fn bucket, acc ->
          bucket_count =
            case :ets.lookup(table, {:flow_latency_bucket, command, bucket}) do
              [{{:flow_latency_bucket, ^command, ^bucket}, value}] -> value
              _ -> 0
            end

          next = acc + bucket_count

          if next >= target do
            {:halt, latency_bucket_value(bucket, max_us)}
          else
            {:cont, next}
          end
        end)
        |> case do
          value when is_integer(value) -> value
          _ -> max_us
        end
      end

      defp latency_bucket_value(:infinity, max_us), do: max_us
      defp latency_bucket_value(bucket_us, _max_us), do: bucket_us

      defp print_flow_latency_line(latency, prefix, sample_rate) do
        parts =
          @flow_latency_commands
          |> Enum.map(fn command ->
            latency
            |> Map.get(command, empty_latency_stats())
            |> format_flow_latency(command)
          end)
          |> Enum.reject(&(&1 == ""))

        if parts != [] do
          sample_tag = if sample_rate > 1, do: " sample_rate=#{sample_rate}", else: ""
          IO.puts(prefix <> sample_tag <> " " <> Enum.join(parts, " "))
        end
      end

      defp empty_latency_stats do
        %{
          calls: 0,
          items: 0,
          avg_us: 0,
          avg_item_us: 0,
          p50_us: 0,
          p95_us: 0,
          p99_us: 0,
          max_us: 0
        }
      end

      defp format_flow_latency(%{calls: calls}, _command) when calls <= 0, do: ""

      defp format_flow_latency(stats, command) do
        "#{command}=" <>
          "calls:#{stats.calls}," <>
          "items:#{stats.items}," <>
          "avg:#{ms(stats.avg_us)}," <>
          "avg_item:#{ms(stats.avg_item_us)}," <>
          "p50<=#{ms(stats.p50_us)}," <>
          "p95<=#{ms(stats.p95_us)}," <>
          "p99<=#{ms(stats.p99_us)}," <>
          "max:#{ms(stats.max_us)}"
      end

      defp ms(us) when is_integer(us), do: Float.round(us / 1000, 3)

      defp event_count(table, key) do
        case :ets.lookup(table, key) do
          [{^key, count, _duration, _items, _bytes}] -> count
          _ -> 0
        end
      end

      defp event_items(table, key) do
        case :ets.lookup(table, key) do
          [{^key, _count, _duration, items, _bytes}] -> items
          _ -> 0
        end
      end

      defp event_count_duration_prefix(table, prefix) do
        table
        |> :ets.tab2list()
        |> Enum.reduce({0, 0}, fn
          {{^prefix, _status}, count, duration, _items, _bytes}, {count_acc, duration_acc} ->
            {count_acc + count, duration_acc + duration}

          _row, acc ->
            acc
        end)
      end

      defp timing_count_duration(table, phase) do
        key = {:waraft_flush_timing, phase}

        case :ets.lookup(table, key) do
          [{^key, count, duration}] -> {count, duration}
          _ -> {0, 0}
        end
      end

      defp event_count_bytes_prefix(table, prefix) do
        table
        |> :ets.tab2list()
        |> Enum.reduce({0, 0}, fn
          {{^prefix, _status}, count, _duration, _items, bytes}, {count_acc, bytes_acc} ->
            {count_acc + count, bytes_acc + bytes}

          _row, acc ->
            acc
        end)
      end

      defp event_count_matching(table, prefix, predicate) do
        table
        |> :ets.tab2list()
        |> Enum.reduce(0, fn
          {{^prefix, status}, count, _duration, _items, _bytes}, acc ->
            if predicate.(status), do: acc + count, else: acc

          _row, acc ->
            acc
        end)
      end

      defp event_count_prefix(table, prefix) do
        table
        |> :ets.tab2list()
        |> Enum.reduce(0, fn
          {{^prefix, _status}, count, _duration, _items, _bytes}, acc -> acc + count
          _row, acc -> acc
        end)
      end

      defp max_us(table, prefix) do
        table
        |> :ets.tab2list()
        |> Enum.reduce(0, fn
          {{:max_us, {^prefix, _status}}, value}, acc when is_integer(value) -> max(acc, value)
          _row, acc -> acc
        end)
      end

      defp max_us_for_key(table, timing_key) do
        key = {:max_us, timing_key}

        case :ets.lookup(table, key) do
          [{^key, value}] when is_integer(value) -> value
          _ -> 0
        end
      end

      defp counter_max(table, key) do
        case :ets.lookup(table, key) do
          [{^key, value}] when is_integer(value) -> value
          _ -> 0
        end
      end

      defp counter_value(table, key) do
        case :ets.lookup(table, key) do
          [{^key, value}] when is_integer(value) -> value
          _ -> 0
        end
      end

      defp timing_max_us(table, phase) do
        key = {:max_us, {:waraft_flush_timing, phase}}

        case :ets.lookup(table, key) do
          [{^key, value}] when is_integer(value) -> value
          _ -> 0
        end
      end

      defp timing_key_max_us(table, timing_key) do
        key = {:max_us, timing_key}

        case :ets.lookup(table, key) do
          [{^key, value}] when is_integer(value) -> value
          _ -> 0
        end
      end

      defp lmdb_status do
        case safe_instance() do
          nil ->
            %{pending_ops: 0, max_oldest_lag_ms: 0.0, max_replay_safe_lag: 0, flush_failures: 0}

          ctx ->
            shards = max(Map.get(ctx, :shard_count, 0), 0)

            Enum.reduce(
              0..max(shards - 1, 0),
              %{
                pending_ops: 0,
                max_oldest_lag_ms: 0.0,
                max_replay_safe_lag: 0,
                flush_failures: 0
              },
              fn shard, acc ->
                pending_ops = atomic(ctx, :flow_lmdb_writer_pending_ops, shard)
                age_us = atomic(ctx, :flow_lmdb_writer_oldest_pending_age_us, shard)
                requested = atomic(ctx, :flow_lmdb_replay_safe_requested_index, shard)
                durable = atomic(ctx, :flow_lmdb_replay_safe_index, shard)
                failures = atomic(ctx, :flow_lmdb_writer_flush_failures, shard)

                %{
                  pending_ops: acc.pending_ops + pending_ops,
                  max_oldest_lag_ms: max(acc.max_oldest_lag_ms, age_us / 1000),
                  max_replay_safe_lag: max(acc.max_replay_safe_lag, max(requested - durable, 0)),
                  flush_failures: acc.flush_failures + failures
                }
              end
            )
        end
      end

      defp flow_admission_status do
        status = Ferricstore.Flow.Admission.status()

        %{
          paused: status.reject_new_creates?,
          reason: status.reason,
          retry_after_ms: status.retry_after_ms
        }
      rescue
        _ -> %{paused: false, reason: :unknown, retry_after_ms: 0}
      end

      defp production_health_status do
        case safe_instance() do
          nil ->
            empty_production_health_status()

          ctx ->
            shards = max(Map.get(ctx, :shard_count, 0), 0)

            health =
              Enum.reduce(0..max(shards - 1, 0), empty_production_health_status(), fn shard,
                                                                                      acc ->
                history_pending = atomic(ctx, :flow_history_projector_pending_entries, shard)
                history_age_us = atomic(ctx, :flow_history_projector_oldest_pending_age_us, shard)
                history_requested = atomic(ctx, :flow_history_requested_index, shard)
                history_projected = atomic(ctx, :flow_history_projected_index, shard)
                history_lag = max(history_requested - history_projected, 0)

                history_flush_failures =
                  atomic(ctx, :flow_history_projector_flush_failures, shard)

                history_queue_full = atomic(ctx, :flow_history_projector_queue_full, shard)
                last_applied = atomic(ctx, :last_applied_index, shard)
                last_released = atomic(ctx, :last_released_cursor_index, shard)
                release_cursor_gap = max(last_applied - last_released, 0)

                %{
                  acc
                  | history_pending_entries: acc.history_pending_entries + history_pending,
                    history_oldest_lag_ms: max(acc.history_oldest_lag_ms, history_age_us / 1000),
                    history_projection_lag: max(acc.history_projection_lag, history_lag),
                    history_flush_failures: acc.history_flush_failures + history_flush_failures,
                    history_queue_full: acc.history_queue_full + history_queue_full,
                    release_cursor_gap: max(acc.release_cursor_gap, release_cursor_gap)
                }
              end)

            blob_stats = hardened_blob_stats(ctx)

            %{
              health
              | blob_hardened_count: blob_stats.count,
                blob_hardened_oldest_ms: blob_stats.oldest_age_ms
            }
        end
      end

      defp empty_production_health_status do
        %{
          history_pending_entries: 0,
          history_oldest_lag_ms: 0.0,
          history_projection_lag: 0,
          history_flush_failures: 0,
          history_queue_full: 0,
          blob_hardened_count: 0,
          blob_hardened_oldest_ms: 0,
          release_cursor_gap: 0
        }
      end

      defp hardened_blob_stats(%{data_dir: data_dir}) when is_binary(data_dir) do
        Ferricstore.Store.BlobStore.hardened_protection_stats(data_dir)
      rescue
        _ -> %{count: 0, oldest_age_ms: 0}
      end

      defp hardened_blob_stats(_ctx), do: %{count: 0, oldest_age_ms: 0}

      defp keydir_status do
        case safe_instance() do
          %{keydir_refs: refs} = ctx when is_tuple(refs) ->
            initial = %{
              entries: 0,
              binary_mb: atomic_total_mb(ctx, :keydir_binary_bytes),
              state: 0,
              history: 0,
              value: 0,
              flow_other: 0,
              other: 0
            }

            if bool_env("KEYDIR_BREAKDOWN", true) do
              refs
              |> Tuple.to_list()
              |> Enum.reduce(initial, &count_keydir_table/2)
            else
              entries =
                refs
                |> Tuple.to_list()
                |> Enum.reduce(0, fn table, acc -> acc + ets_info(table, :size) end)

              %{initial | entries: entries}
            end

          _ ->
            empty_keydir_status()
        end
      rescue
        _ -> empty_keydir_status()
      end

      defp empty_keydir_status do
        %{
          entries: 0,
          binary_mb: 0.0,
          state: 0,
          history: 0,
          value: 0,
          flow_other: 0,
          other: 0
        }
      end

      defp count_keydir_table(table, acc) do
        :ets.foldl(
          fn
            {key, _value, _expire_at_ms, _lfu, _fid, _offset, _value_size}, table_acc
            when is_binary(key) ->
              increment_keydir_kind(table_acc, keydir_key_kind(key))

            _row, table_acc ->
              increment_keydir_kind(table_acc, :other)
          end,
          acc,
          table
        )
      rescue
        _ -> acc
      end

      defp increment_keydir_kind(acc, kind) do
        acc
        |> Map.update!(:entries, &(&1 + 1))
        |> Map.update!(kind, &(&1 + 1))
      end

      defp keydir_key_kind("X:f:" <> _rest), do: :history

      defp keydir_key_kind("f:" <> rest) do
        cond do
          :binary.match(rest, "}:s:") != :nomatch -> :state
          :binary.match(rest, "}:v:") != :nomatch -> :value
          true -> :flow_other
        end
      end

      defp keydir_key_kind(_key), do: :other

      defp flow_index_status do
        case safe_instance() do
          %{name: name, shard_count: count}
          when is_atom(name) and is_integer(count) and count > 0 ->
            Enum.reduce(0..(count - 1), %{index_entries: 0, lookup_entries: 0}, fn shard, acc ->
              {index, lookup} = Ferricstore.Flow.OrderedIndex.table_names(name, shard)

              %{
                index_entries: acc.index_entries + ets_info(index, :size),
                lookup_entries: acc.lookup_entries + ets_info(lookup, :size)
              }
            end)

          _ ->
            %{index_entries: 0, lookup_entries: 0}
        end
      rescue
        _ -> %{index_entries: 0, lookup_entries: 0}
      end

      defp waraft_log_status do
        shard_count =
          case safe_instance() do
            %{shard_count: count} when is_integer(count) and count > 0 -> count
            _ -> int_env("SHARDS", 16)
          end

        Enum.reduce(1..max(shard_count, 1), %{entries: 0, ets_mb: 0.0}, fn partition, acc ->
          table = :"raft_log_ferricstore_waraft_backend_#{partition}"
          size = ets_info(table, :size)
          memory_words = ets_info(table, :memory)

          %{
            entries: acc.entries + size,
            ets_mb: acc.ets_mb + bytes_to_mb(memory_words * :erlang.system_info(:wordsize))
          }
        end)
      rescue
        _ -> %{entries: 0, ets_mb: 0.0}
      end

      defp ets_info(table, item) do
        case :ets.info(table, item) do
          value when is_integer(value) -> value
          _ -> 0
        end
      rescue
        _ -> 0
      end

      defp atomic(ctx, field, shard) do
        case Map.get(ctx, field) do
          ref when is_reference(ref) ->
            if shard < :atomics.info(ref).size, do: :atomics.get(ref, shard + 1), else: 0

          _ ->
            0
        end
      rescue
        _ -> 0
      end

      defp atomic_total_mb(ctx, field), do: bytes_to_mb(atomic_total(ctx, field))

      defp atomic_total(ctx, field) do
        case Map.get(ctx, field) do
          ref when is_reference(ref) ->
            size = :atomics.info(ref).size

            Enum.reduce(1..size, 0, fn idx, acc ->
              acc + :atomics.get(ref, idx)
            end)

          _ ->
            0
        end
      rescue
        _ -> 0
      end

      defp safe_instance do
        FerricStore.Instance.get(:default)
      rescue
        _ -> nil
      end

      defp memory_status do
        memory = :erlang.memory()
        os = os_process_status()

        %{
          total_mb: bytes_to_mb(Keyword.get(memory, :total, 0)),
          binary_mb: bytes_to_mb(Keyword.get(memory, :binary, 0)),
          ets_mb: bytes_to_mb(Keyword.get(memory, :ets, 0)),
          rss_mb: os.rss_mb,
          cpu_pct: os.cpu_pct,
          process_count: :erlang.system_info(:process_count),
          run_queue: :erlang.statistics(:run_queue)
        }
      end

      defp rss_guard_mb(%{rss_mb: rss_mb}) when rss_mb > 0, do: rss_mb
      defp rss_guard_mb(%{total_mb: total_mb}), do: total_mb

      defp maybe_print_top_binary_holders(sample_count) do
        if diagnostic_due?("TOP_BINARY_HOLDERS", "TOP_BINARY_HOLDERS_EVERY_N", sample_count) do
          limit = int_env("TOP_BINARY_HOLDERS_LIMIT", 8)

          Process.list()
          |> Enum.map(&process_binary_holder/1)
          |> Enum.filter(fn %{bytes: bytes} -> bytes > 0 end)
          |> Enum.sort_by(& &1.bytes, :desc)
          |> Enum.take(limit)
          |> Enum.each(fn holder ->
            IO.puts(
              "top_binary_holder pid=#{inspect(holder.pid)} name=#{inspect(holder.name)} " <>
                "initial_call=#{inspect(holder.initial_call)} binary_mb=#{Float.round(bytes_to_mb(holder.bytes), 1)} " <>
                "binary_count=#{holder.count}"
            )
          end)
        end
      end

      defp maybe_print_top_ets_binary_tables(sample_count) do
        if diagnostic_due?("TOP_ETS_BINARY_TABLES", "TOP_ETS_BINARY_TABLES_EVERY_N", sample_count) do
          limit = int_env("TOP_ETS_BINARY_TABLES_LIMIT", 8)
          max_rows = int_env("TOP_ETS_BINARY_TABLES_MAX_ROWS", 1_000)

          :ets.all()
          |> Enum.map(&ets_binary_table_sample(&1, max_rows))
          |> Enum.filter(fn %{bytes: bytes} -> bytes > 0 end)
          |> Enum.sort_by(& &1.bytes, :desc)
          |> Enum.take(limit)
          |> Enum.each(fn table ->
            IO.puts(
              "top_ets_binary_table table=#{inspect(table.table)} sampled_binary_mb=#{Float.round(bytes_to_mb(table.bytes), 1)} " <>
                "sampled_rows=#{table.rows} table_size=#{inspect(table.size)}"
            )
          end)
        end
      end

      defp maybe_print_top_ets_memory_tables(sample_count) do
        if diagnostic_due?("TOP_ETS_MEMORY_TABLES", "TOP_ETS_MEMORY_TABLES_EVERY_N", sample_count) do
          limit = int_env("TOP_ETS_MEMORY_TABLES_LIMIT", 12)
          wordsize = :erlang.system_info(:wordsize)

          :ets.all()
          |> Enum.map(fn table ->
            %{
              table: table,
              memory_mb: ets_table_memory_mb(table, wordsize),
              size: ets_table_info(table, :size),
              owner: ets_table_info(table, :owner),
              name: ets_table_info(table, :name),
              type: ets_table_info(table, :type)
            }
          end)
          |> Enum.filter(fn %{memory_mb: memory_mb} -> memory_mb > 0 end)
          |> Enum.sort_by(& &1.memory_mb, :desc)
          |> Enum.take(limit)
          |> Enum.each(fn table ->
            IO.puts(
              "top_ets_memory_table table=#{inspect(table.table)} name=#{inspect(table.name)} " <>
                "type=#{inspect(table.type)} owner=#{inspect(table.owner)} " <>
                "memory_mb=#{Float.round(table.memory_mb, 1)} size=#{inspect(table.size)}"
            )
          end)
        end
      end

      defp ets_table_memory_mb(table, wordsize) do
        case ets_table_info(table, :memory) do
          memory when is_integer(memory) -> bytes_to_mb(memory * wordsize)
          _ -> 0.0
        end
      end

      defp ets_table_info(table, key) do
        :ets.info(table, key)
      rescue
        _ -> nil
      end

      defp maybe_print_process_profile(previous, sample_count) do
        current = process_profile_snapshot()

        if diagnostic_due?("PROCESS_PROFILE", "PROCESS_PROFILE_EVERY_N", sample_count) do
          limit = int_env("PROCESS_PROFILE_TOP", 12)

          rows =
            current
            |> Enum.map(fn {pid, info} ->
              previous_info = Map.get(previous, pid, %{})
              reductions = Map.get(info, :reductions, 0) - Map.get(previous_info, :reductions, 0)

              Map.merge(info, %{pid: pid, reduction_delta: reductions})
            end)
            |> Enum.filter(&(&1.reduction_delta > 0))
            |> Enum.sort_by(& &1.reduction_delta, :desc)
            |> Enum.take(limit)

          IO.puts("process_profile_top_reductions count=#{length(rows)}")

          Enum.each(rows, fn row ->
            IO.puts(
              "process_profile pid=#{inspect(row.pid)} name=#{inspect(row.registered_name)} " <>
                "reductions=#{row.reduction_delta} mq=#{row.message_queue_len} " <>
                "memory_mb=#{Float.round(bytes_to_mb(row.memory), 2)} " <>
                "initial=#{inspect(row.initial_call)} current=#{inspect(row.current_function)}"
            )

            maybe_print_process_stack(row)
          end)
        end

        current
      end

      defp process_profile_snapshot do
        if env("PROCESS_PROFILE", "false") in ["1", "true", "TRUE"] do
          Process.list()
          |> Enum.reduce(%{}, fn pid, acc ->
            case Process.info(pid, [
                   :registered_name,
                   :initial_call,
                   :current_function,
                   :current_stacktrace,
                   :reductions,
                   :message_queue_len,
                   :memory
                 ]) do
              nil ->
                acc

              info ->
                Map.put(acc, pid, Map.new(info))
            end
          end)
        else
          %{}
        end
      end

      defp maybe_print_process_stack(row) do
        if env("PROCESS_PROFILE_STACK", "false") in ["1", "true", "TRUE"] do
          depth = int_env("PROCESS_PROFILE_STACK_DEPTH", 6)

          stack =
            row
            |> Map.get(:current_stacktrace, [])
            |> Enum.take(depth)
            |> Enum.map(&format_stack_frame/1)
            |> Enum.join(" <= ")

          IO.puts("process_profile_stack pid=#{inspect(row.pid)} stack=#{stack}")
        end
      end

      defp format_stack_frame({mod, fun, arity, location}) when is_integer(arity) do
        "#{inspect(mod)}.#{fun}/#{arity}#{format_stack_location(location)}"
      end

      defp format_stack_frame({mod, fun, args, location}) when is_list(args) do
        "#{inspect(mod)}.#{fun}/#{length(args)}#{format_stack_location(location)}"
      end

      defp format_stack_frame(frame), do: inspect(frame)

      defp format_stack_location(location) when is_list(location) do
        case Keyword.get(location, :file) do
          nil ->
            ""

          file ->
            line = Keyword.get(location, :line)

            ":#{Path.basename(to_string(file))}#{if line, do: ":" <> Integer.to_string(line), else: ""}"
        end
      end

      defp format_stack_location(_location), do: ""

      defp diagnostic_due?(enabled_env, every_env, sample_count) do
        case env(enabled_env, "0") do
          value when value in ["1", "true", "TRUE"] ->
            every = max(int_env(every_env, 4), 1)
            rem(sample_count, every) == 0

          _ ->
            false
        end
      end

      defp process_binary_holder(pid) do
        binaries =
          case Process.info(pid, :binary) do
            {:binary, binaries} when is_list(binaries) -> binaries
            _ -> []
          end

        {bytes, count} =
          Enum.reduce(binaries, {0, 0}, fn
            {_binary, size, _refs}, {sum, total} when is_integer(size) ->
              {sum + size, total + 1}

            _other, acc ->
              acc
          end)

        %{
          pid: pid,
          name: process_info_value(pid, :registered_name),
          initial_call: process_info_value(pid, :initial_call),
          bytes: bytes,
          count: count
        }
      end

      defp process_info_value(pid, key) do
        case Process.info(pid, key) do
          {^key, value} -> value
          _ -> nil
        end
      rescue
        _ -> nil
      end

      defp ets_binary_table_sample(table, max_rows) do
        size = ets_info(table, :size)

        {bytes, rows} =
          if is_integer(size) and size > 0 do
            sample_ets_table_binaries(table, max_rows)
          else
            {0, 0}
          end

        %{table: table, bytes: bytes, rows: rows, size: size}
      end
    end
  end
end
