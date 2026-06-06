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
    with {:ok, due_keys} <- Router.flow_due_count_keys(ctx),
         matched_keys =
           Enum.filter(
             due_keys,
             &due_key_matches?(
               &1,
               type,
               state_filter,
               priority_filter,
               partition_filter
             )
           ),
         true <- matched_keys != [],
         {:ok, results} <-
           Router.flow_index_rank_range_many(
             ctx,
             Enum.map(matched_keys, &{&1, 0, 0, false})
           ),
         due_at when is_integer(due_at) <- earliest_score(results) do
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
      path
      |> cold_bucket_prefixes(
        type,
        state_filter,
        priority_filter,
        partition_filter,
        now_ms,
        cold_horizon_ms
      )
      |> Enum.reduce_while(earliest, fn prefix, current ->
        case Ferricstore.Flow.LMDB.prefix_entries(
               path,
               prefix,
               @claim_due_cold_schedule_scan_limit
             ) do
          {:ok, entries} ->
            next =
              entries
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

                  _ ->
                    acc
                end
              end)

            if is_integer(next) and next <= now_ms do
              {:halt, next}
            else
              {:cont, next}
            end

          _other ->
            {:cont, current}
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
         path,
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
          for bucket_ms <- buckets,
              state <- states,
              partition_key <- partitions,
              priority <- priorities do
            Ferricstore.Flow.LMDB.cold_due_claim_prefix(
              bucket_ms: bucket_ms,
              type: type,
              state: state,
              partition_key: partition_key,
              priority: priority
            )
          end
        end

      _broad_filter ->
        cold_type_prefixes(buckets, type)
    end
    |> Enum.filter(&cold_prefix_present?(path, &1))
  end

  defp cold_type_prefixes(buckets, type) do
    Enum.map(buckets, &Ferricstore.Flow.LMDB.cold_due_type_bucket_prefix(&1, type))
  end

  defp cold_prefix_present?(path, prefix) do
    case Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1) do
      {:ok, [_ | _]} -> true
      _ -> false
    end
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
    |> Enum.to_list()
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
    do: String.starts_with?(partition, "__flow_auto__:")

  defp cold_partition_match?(partition, partitions)
       when is_binary(partition) and is_list(partitions),
       do: partition in partitions

  defp cold_partition_match?(partition, partition), do: true
  defp cold_partition_match?(_partition, _filter), do: false

  defp earliest_score(results) when is_list(results) do
    results
    |> Enum.reduce(nil, fn
      [{_id, score} | _], nil when is_number(score) ->
        round(score)

      [{_id, score} | _], current when is_number(score) ->
        min(current, round(score))

      _other, current ->
        current
    end)
  end

  defp earliest_score(_results), do: nil

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

  defp due_key_matches?(key, type, state_filter, priority_filter, partition_filter)
       when is_binary(key) and is_binary(type) do
    due_key?(key) and
      partition_match?(key, partition_filter) and
      state_match?(key, type, state_filter) and
      priority_match?(key, priority_filter)
  end

  defp due_key_matches?(_key, _type, _state_filter, _priority, _partition),
    do: false

  defp due_key?(key) do
    String.starts_with?(key, "f:{") and
      (:binary.match(key, "}:d:") != :nomatch or :binary.match(key, "}:da:") != :nomatch)
  end

  defp partition_match?(_key, nil), do: true
  defp partition_match?(_key, :any), do: true

  defp partition_match?(key, :auto),
    do: String.starts_with?(key, "f:{fa:") and due_key?(key)

  defp partition_match?(key, partitions) when is_list(partitions),
    do: Enum.any?(partitions, &partition_match?(key, &1))

  defp partition_match?(key, partition_key) do
    tag = Ferricstore.Flow.Keys.tag(partition_key)

    String.starts_with?(key, "f:" <> tag <> ":d:") or
      String.starts_with?(key, "f:" <> tag <> ":da:")
  end

  defp state_match?(key, type, state_filter) when state_filter in [nil, :any] do
    String.contains?(key, "}:d:" <> type <> ":") or
      String.contains?(key, "}:da:" <> type <> ":p")
  end

  defp state_match?(key, type, states) when is_list(states),
    do: Enum.any?(states, &state_match?(key, type, &1))

  defp state_match?(key, type, state) when is_binary(state),
    do: String.contains?(key, "}:d:" <> type <> ":" <> state <> ":p")

  defp state_match?(_key, _type, _state), do: false

  defp priority_match?(_key, nil), do: true

  defp priority_match?(key, priority) when is_integer(priority),
    do: String.ends_with?(key, ":p" <> Integer.to_string(priority))

  defp priority_match?(_key, _priority), do: false

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
