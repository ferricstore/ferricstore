defmodule Ferricstore.Flow.ClaimWaiterScheduler do
  @moduledoc false

  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Store.Router

  @claim_due_cold_schedule_horizon_ms Application.compile_env(
                                        :ferricstore,
                                        :flow_claim_due_cold_schedule_horizon_ms,
                                        24 * 60 * 60 * 1_000
                                      )
  @claim_due_cold_schedule_scan_limit Application.compile_env(
                                        :ferricstore,
                                        :flow_claim_due_cold_schedule_scan_limit,
                                        1_000
                                      )
  @claim_due_cold_schedule_min_horizon_ms Application.compile_env(
                                            :ferricstore,
                                            :flow_claim_due_cold_schedule_min_horizon_ms,
                                            60_000
                                          )
  @claim_due_cold_schedule_bucket_ms 60_000

  @doc false
  def schedule_next_due(ctx, type, state_filter, priority, partition_filter, wait_horizon_ms)
      when is_binary(type) do
    cold_horizon_ms = cold_horizon_ms(wait_horizon_ms)

    case {schedule_states(state_filter), schedule_partitions(partition_filter)} do
      {{:ok, states}, {:ok, partitions}} ->
        priorities = schedule_priorities(priority)

        for state <- states,
            partition_key <- partitions,
            priority <- priorities do
          schedule_next_due_key(ctx, type, state, priority, partition_key)
        end

        schedule_next_cold_due(
          ctx,
          type,
          state_filter,
          priority,
          partition_filter,
          cold_horizon_ms
        )

        :ok

      _broad_filter ->
        schedule_next_due_matching(
          ctx,
          type,
          state_filter,
          priority,
          partition_filter
        )

        schedule_next_cold_due(
          ctx,
          type,
          state_filter,
          priority,
          partition_filter,
          cold_horizon_ms
        )
    end
  end

  def schedule_next_due(_ctx, _type, _state_filter, _priority, _partition_filter, _horizon),
    do: :ok

  defp schedule_states(state) do
    case state do
      state when is_binary(state) -> {:ok, [state]}
      states when is_list(states) -> schedule_binary_list(states)
      _unsupported -> :unsupported
    end
  end

  defp schedule_partitions(partition_filter) do
    case partition_filter do
      partition_key when is_binary(partition_key) ->
        {:ok, [partition_key]}

      partition_keys when is_list(partition_keys) ->
        schedule_binary_list(partition_keys)

      _unsupported ->
        :unsupported
    end
  end

  defp schedule_binary_list(values) when is_list(values) do
    if values != [] and Enum.all?(values, &is_binary/1), do: {:ok, values}, else: :unsupported
  end

  defp schedule_priorities(nil), do: [2, 1, 0]
  defp schedule_priorities(priority) when is_integer(priority), do: [priority]
  defp schedule_priorities(_priority), do: []

  defp schedule_next_due_matching(
         ctx,
         type,
         state_filter,
         priority_filter,
         partition_filter
       ) do
    with {:ok, prefixes, needles, suffixes} <-
           hot_due_matchers(type, state_filter, priority_filter, partition_filter),
         {:ok, score} when is_float(score) <-
           Router.flow_earliest_due_score(ctx, prefixes, needles, suffixes) do
      due_at = round(score)

      for state <- notify_states(state_filter),
          partition_key <- notify_partitions(partition_filter) do
        ClaimWaiters.schedule_ready(type, state, priority_filter, partition_key, due_at, 1)
      end

      :ok
    else
      _other -> :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp hot_due_matchers(type, state_filter, priority_filter, partition_filter) do
    with {:ok, prefixes} <- hot_due_prefixes(partition_filter),
         {:ok, needles} <- hot_due_needles(type, state_filter),
         {:ok, suffixes} <- hot_due_suffixes(priority_filter) do
      {:ok, prefixes, needles, suffixes}
    end
  end

  defp hot_due_prefixes(partition_filter) when partition_filter in [nil, :any], do: {:ok, []}
  defp hot_due_prefixes(:auto), do: {:ok, ["f:{fa:"]}

  defp hot_due_prefixes(partition_keys) when is_list(partition_keys) do
    case schedule_binary_list(partition_keys) do
      {:ok, values} -> {:ok, values |> Enum.map(&hot_due_partition_prefix/1) |> Enum.uniq()}
      :unsupported -> :unsupported
    end
  end

  defp hot_due_prefixes(partition_key) when is_binary(partition_key),
    do: {:ok, [hot_due_partition_prefix(partition_key)]}

  defp hot_due_prefixes(_unsupported), do: :unsupported

  defp hot_due_partition_prefix(partition_key),
    do: "f:" <> Ferricstore.Flow.Keys.tag(partition_key) <> ":"

  defp hot_due_needles(type, state_filter) do
    encoded_type = Ferricstore.Flow.Keys.index_component(type)

    case state_filter do
      state when state in [nil, :any] ->
        {:ok, ["}:d:" <> encoded_type <> ":", "}:da:" <> encoded_type <> ":p"]}

      state when is_binary(state) ->
        {:ok,
         ["}:d:" <> encoded_type <> ":" <> Ferricstore.Flow.Keys.index_component(state) <> ":p"]}

      states when is_list(states) ->
        case schedule_binary_list(states) do
          {:ok, values} ->
            {:ok,
             values
             |> Enum.map(fn state ->
               "}:d:" <>
                 encoded_type <> ":" <> Ferricstore.Flow.Keys.index_component(state) <> ":p"
             end)
             |> Enum.uniq()}

          :unsupported ->
            :unsupported
        end

      _unsupported ->
        :unsupported
    end
  end

  defp hot_due_suffixes(nil), do: {:ok, []}

  defp hot_due_suffixes(priority) when is_integer(priority),
    do: {:ok, [":p" <> Integer.to_string(priority)]}

  defp hot_due_suffixes(_unsupported), do: :unsupported

  defp schedule_next_cold_due(
         ctx,
         type,
         state_filter,
         priority_filter,
         partition_filter,
         cold_horizon_ms
       ) do
    with true <- Ferricstore.Flow.Hibernation.enabled?(),
         true <- cold_horizon_ms >= @claim_due_cold_schedule_min_horizon_ms,
         due_at when is_integer(due_at) <-
           earliest_cold_score(
             ctx,
             type,
             state_filter,
             priority_filter,
             partition_filter,
             cold_horizon_ms
           ) do
      for state <- notify_states(state_filter),
          partition_key <- notify_partitions(partition_filter) do
        ClaimWaiters.schedule_ready(type, state, priority_filter, partition_key, due_at, 1)
      end

      :ok
    else
      _other -> :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp earliest_cold_score(
         ctx,
         type,
         state_filter,
         priority_filter,
         partition_filter,
         cold_horizon_ms
       )
       when is_binary(type) do
    now_ms = Ferricstore.CommandTime.now_ms()

    ctx
    |> cold_lmdb_paths()
    |> Enum.reduce(nil, fn path, earliest ->
      cold_bucket_prefixes(
        type,
        state_filter,
        priority_filter,
        partition_filter,
        now_ms,
        cold_horizon_ms
      )
      |> Enum.reduce_while(earliest, fn {bucket_ms, prefixes}, current ->
        if is_integer(current) and bucket_ms >= current do
          {:halt, current}
        else
          bucket_due =
            earliest_cold_bucket_due(
              path,
              prefixes,
              bucket_ms,
              type,
              state_filter,
              priority_filter,
              partition_filter
            )

          if is_integer(bucket_due) do
            next = if is_integer(current), do: min(current, bucket_due), else: bucket_due
            {:halt, next}
          else
            {:cont, current}
          end
        end
      end)
    end)
  end

  defp earliest_cold_score(_ctx, _type, _state, _priority, _partition, _horizon),
    do: nil

  defp cold_lmdb_paths(%{data_dir: data_dir, shard_count: shard_count})
       when is_binary(data_dir) and is_integer(shard_count) and shard_count > 0 do
    for shard_index <- 0..(shard_count - 1) do
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()
    end
  end

  defp cold_lmdb_paths(_ctx), do: []

  defp cold_bucket_prefixes(
         type,
         state_filter,
         priority_filter,
         partition_filter,
         now_ms,
         cold_horizon_ms
       ) do
    buckets = cold_schedule_buckets(now_ms, cold_horizon_ms)

    case {schedule_states(state_filter), schedule_partitions(partition_filter)} do
      {{:ok, states}, {:ok, partitions}} ->
        priorities = schedule_priorities(priority_filter)

        if priorities == [] do
          cold_type_prefixes(buckets, type)
        else
          Stream.map(buckets, fn bucket_ms ->
            prefixes =
              for state <- states,
                  partition_key <- partitions,
                  priority <- priorities do
                {
                  Ferricstore.Flow.LMDB.cold_due_claim_prefix(
                    bucket_ms: bucket_ms,
                    type: type,
                    state: state,
                    partition_key: partition_key,
                    priority: priority
                  ),
                  false
                }
              end

            {bucket_ms, prefixes}
          end)
        end

      _broad_filter ->
        cold_type_prefixes(buckets, type)
    end
  end

  defp cold_type_prefixes(buckets, type) do
    Stream.map(buckets, fn bucket_ms ->
      {bucket_ms, [{Ferricstore.Flow.LMDB.cold_due_type_bucket_prefix(bucket_ms, type), true}]}
    end)
  end

  defp earliest_cold_bucket_due(
         path,
         prefixes,
         bucket_ms,
         type,
         state_filter,
         priority_filter,
         partition_filter
       ) do
    Enum.reduce(prefixes, nil, fn {prefix, _broad?}, current ->
      case Ferricstore.Flow.LMDB.prefix_entries(
             path,
             prefix,
             @claim_due_cold_schedule_scan_limit + 1
           ) do
        {:ok, entries} ->
          entries
          |> Enum.take(@claim_due_cold_schedule_scan_limit)
          |> Enum.reduce(current, fn {_due_key, park_key}, acc ->
            case cold_due_at(
                   path,
                   park_key,
                   type,
                   state_filter,
                   priority_filter,
                   partition_filter
                 ) do
              due_at when is_integer(due_at) ->
                if is_integer(acc), do: min(acc, due_at), else: due_at

              _missing_or_stale ->
                acc
            end
          end)
          |> conservative_truncated_due(
            bucket_ms,
            length(entries) > @claim_due_cold_schedule_scan_limit
          )

        _unavailable ->
          current
      end
    end)
  end

  defp conservative_truncated_due(current, _bucket_ms, false), do: current

  defp conservative_truncated_due(current, bucket_ms, true) do
    if is_integer(current), do: min(current, bucket_ms), else: bucket_ms
  end

  defp cold_horizon_ms(value) when is_integer(value) and value >= 0 do
    min(value, @claim_due_cold_schedule_horizon_ms)
  end

  defp cold_horizon_ms(_value), do: @claim_due_cold_schedule_horizon_ms

  defp cold_schedule_buckets(now_ms, horizon_ms) do
    first = Ferricstore.Flow.LMDB.cold_due_bucket_ms(now_ms, @claim_due_cold_schedule_bucket_ms)
    horizon = now_ms + cold_horizon_ms(horizon_ms)
    last = Ferricstore.Flow.LMDB.cold_due_bucket_ms(horizon, @claim_due_cold_schedule_bucket_ms)

    first
    |> Stream.iterate(&(&1 + @claim_due_cold_schedule_bucket_ms))
    |> Stream.take_while(&(&1 <= last))
  end

  defp cold_due_at(
         path,
         park_key,
         type,
         state_filter,
         priority_filter,
         partition_filter
       )
       when is_binary(park_key) do
    with {:ok, park_blob} <- Ferricstore.Flow.LMDB.get(path, park_key),
         {:ok, park} <- Ferricstore.Flow.LMDB.decode_cold_park(park_blob),
         true <-
           cold_park_matches?(
             park,
             type,
             state_filter,
             priority_filter,
             partition_filter
           ),
         due_at when is_integer(due_at) <- Map.get(park, :due_at_ms) do
      due_at
    else
      _other -> nil
    end
  end

  defp cold_due_at(_path, _park_key, _type, _state, _priority, _partition),
    do: nil

  defp cold_park_matches?(
         park,
         type,
         state_filter,
         priority_filter,
         partition_filter
       )
       when is_map(park) do
    Map.get(park, :type) == type and
      cold_state_match?(Map.get(park, :state), state_filter) and
      cold_priority_match?(Map.get(park, :priority, 0), priority_filter) and
      cold_partition_match?(Map.get(park, :partition_key), partition_filter)
  end

  defp cold_park_matches?(_park, _type, _state, _priority, _partition),
    do: false

  defp cold_state_match?(state, state_filter)
       when is_binary(state) and state_filter in [nil, :any],
       do: true

  defp cold_state_match?(state, states) when is_binary(state) and is_list(states),
    do: state in states

  defp cold_state_match?(state, state), do: true
  defp cold_state_match?(_state, _filter), do: false

  defp cold_priority_match?(_priority, nil), do: true
  defp cold_priority_match?(priority, priority), do: true
  defp cold_priority_match?(_priority, _filter), do: false

  defp cold_partition_match?(partition, partition_filter)
       when is_binary(partition) and partition_filter in [nil, :any],
       do: true

  defp cold_partition_match?(partition, :auto) when is_binary(partition),
    do: Ferricstore.Flow.Keys.auto_partition_key?(partition)

  defp cold_partition_match?(partition, partitions)
       when is_binary(partition) and is_list(partitions),
       do: partition in partitions

  defp cold_partition_match?(partition, partition), do: true
  defp cold_partition_match?(_partition, _filter), do: false

  @doc false
  def __cold_partition_match_for_test__(partition, filter),
    do: cold_partition_match?(partition, filter)

  defp notify_states(states) when is_list(states) do
    states
    |> Enum.filter(&is_binary/1)
    |> case do
      [] -> [:any]
      values -> Enum.uniq(values)
    end
  end

  defp notify_states(state) when is_binary(state), do: [state]
  defp notify_states(_state), do: [:any]

  defp notify_partitions(partitions) when is_list(partitions) do
    partitions
    |> Enum.filter(&is_binary/1)
    |> case do
      [] -> [:any]
      values -> Enum.uniq(values)
    end
  end

  defp notify_partitions(partition) when is_binary(partition), do: [partition]
  defp notify_partitions(partition), do: [partition]

  defp schedule_next_due_key(ctx, type, state, priority, partition_key) do
    key = Ferricstore.Flow.Keys.due_key(type, state, priority, partition_key)

    case Router.flow_index_rank_range(ctx, key, 0, 0, false) do
      {:ok, [{_id, score}]} ->
        due_at_ms = round(score)
        ClaimWaiters.schedule_ready(type, state, priority, partition_key, due_at_ms, 1)

      _other ->
        :ok
    end
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end
end
