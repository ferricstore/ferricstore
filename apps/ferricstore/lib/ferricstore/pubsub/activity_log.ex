defmodule Ferricstore.PubSub.ActivityLog do
  @moduledoc """
  Metadata-only ring buffer for Pub/Sub activity.

  Message payloads are intentionally not stored. Publish entries keep only
  channel, message byte size, and receiver count. Set
  `:pubsub_activity_log_sample_every` in the `:ferricstore` application to a
  positive integer to retain one out of every N publish events; subscription
  changes are always retained.
  """

  use GenServer

  @table :ferricstore_pubsub_activity_log
  @counter_key :ferricstore_pubsub_activity_log_counter
  @max_len_key :ferricstore_pubsub_activity_log_max_len
  @publish_config_key :ferricstore_pubsub_activity_log_publish_config
  @publish_sample_counter_key :ferricstore_pubsub_activity_log_sample_counter
  @default_max_len 512
  @default_read_count 128
  @max_read_count 500
  @max_metadata_bytes 256

  @type entry :: %{
          id: non_neg_integer(),
          timestamp_us: integer(),
          command: binary(),
          target_type: :channel | :pattern,
          target: binary(),
          targets: non_neg_integer(),
          subscribers: non_neg_integer() | nil,
          message_bytes: non_neg_integer() | nil
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec record_publish(binary(), non_neg_integer(), non_neg_integer()) :: :ok
  def record_publish(channel, message_bytes, subscribers) do
    entry = {:publish, channel, max(message_bytes, 0), max(subscribers, 0)}

    case :persistent_term.get(@publish_config_key, nil) do
      {max_len, 1, _counter} ->
        record(entry, max_len)

      {max_len, sample_every, counter}
      when max_len > 0 and sample_every > 1 and is_reference(counter) ->
        if sample_publish?(counter, sample_every), do: record(entry, max_len), else: :ok

      {0, _sample_every, _counter} ->
        :ok

      _not_initialized ->
        record(entry)
    end
  end

  @doc false
  @spec record_publish_batch(binary(), [binary()], [non_neg_integer()]) :: :ok
  def record_publish_batch(_channel, [], []), do: :ok

  def record_publish_batch(channel, messages, subscribers)
      when is_binary(channel) and is_list(messages) and is_list(subscribers) do
    with {:ok, publish_count} <- paired_length(messages, subscribers) do
      case :persistent_term.get(@publish_config_key, nil) do
        {max_len, 1, _counter} ->
          record_publish_entries(channel, select_publish_entries(messages, subscribers), max_len)

        {max_len, sample_every, counter}
        when max_len > 0 and sample_every > 1 and is_reference(counter) ->
          first_sample_index = :atomics.add_get(counter, 1, publish_count) - publish_count

          record_publish_entries(
            channel,
            select_publish_entries(messages, subscribers, first_sample_index, sample_every),
            max_len
          )

        {0, _sample_every, _counter} ->
          :ok

        _not_initialized ->
          record_publish_batch_individually(channel, messages, subscribers)
      end
    else
      :error -> :ok
    end
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @spec record_subscription(binary(), :channel | :pattern, [binary()]) :: :ok
  def record_subscription(_command, _target_type, []), do: :ok

  def record_subscription(command, target_type, targets) when is_list(targets) do
    record({:subscription, command, target_type, targets, length(targets)})
  end

  @doc false
  @spec configure_publish_sampling(pos_integer()) :: :ok
  def configure_publish_sampling(sample_every)
      when is_integer(sample_every) and sample_every >= 1 do
    counter = publish_sample_counter()
    :atomics.put(counter, 1, 0)
    :persistent_term.put(@publish_config_key, {max_len(), sample_every, counter})
    :ok
  end

  @spec get(non_neg_integer() | nil) :: [entry()]
  def get(count \\ @default_read_count) do
    count = bounded_count(count)

    if table_ready?() do
      newest_entries(:ets.last(@table), count, [])
    else
      []
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  @spec reset() :: :ok
  def reset do
    if table_ready?() do
      :ets.delete_all_objects(@table)
    end

    reset_counter()
    reset_publish_sample_counter()
    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init(_opts) do
    table =
      :ets.new(@table, [
        :ordered_set,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    :persistent_term.put(@counter_key, :atomics.new(1, signed: false))
    sample_counter = :atomics.new(1, signed: false)
    :persistent_term.put(@publish_sample_counter_key, sample_counter)

    max_len =
      Application.get_env(:ferricstore, :pubsub_activity_log_max_len, @default_max_len)

    sample_every =
      case Application.get_env(:ferricstore, :pubsub_activity_log_sample_every, 1) do
        value when is_integer(value) and value >= 1 -> value
        _invalid -> 1
      end

    :persistent_term.put(@max_len_key, max_len)

    :persistent_term.put(
      @publish_config_key,
      {normalized_max_len(max_len), sample_every, sample_counter}
    )

    {:ok, %{table: table}}
  end

  defp record(entry) do
    record(entry, max_len())
  end

  defp record(entry, max_len) do
    if max_len > 0 and table_ready?() do
      id = next_id()

      :ets.insert(
        @table,
        {id, normalize_and_timestamp_entry(entry, System.os_time(:microsecond))}
      )

      maybe_evict_overflow(id, max_len)
    end

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp record_publish_entries(_channel, [], _max_len), do: :ok

  defp record_publish_entries(channel, entries, max_len) do
    if max_len > 0 and table_ready?() do
      entry_count = length(entries)
      retained = keep_newest(entries, max_len)
      retained_count = length(retained)
      first_id = reserve_ids(entry_count) + entry_count - retained_count
      timestamp_us = System.os_time(:microsecond)
      target = normalize_binary(channel)

      rows =
        retained
        |> Enum.with_index(first_id)
        |> Enum.map(fn {{message_bytes, subscribers}, id} ->
          {id, {:publish, timestamp_us, target, message_bytes, subscribers}}
        end)

      :ets.insert(@table, rows)
      evict_overflow(max_len)
    end

    :ok
  end

  defp select_publish_entries(messages, subscribers),
    do: select_publish_entries(messages, subscribers, 0, 1)

  defp select_publish_entries(messages, subscribers, first_sample_index, sample_every) do
    select_publish_entries(
      messages,
      subscribers,
      first_sample_index,
      sample_every,
      []
    )
  end

  defp select_publish_entries(
         [message | messages],
         [subscriber_count | subscribers],
         sample_index,
         sample_every,
         entries
       ) do
    entries =
      if rem(sample_index, sample_every) == 0 do
        [{byte_size(message), max(subscriber_count, 0)} | entries]
      else
        entries
      end

    select_publish_entries(
      messages,
      subscribers,
      sample_index + 1,
      sample_every,
      entries
    )
  end

  defp select_publish_entries([], [], _sample_index, _sample_every, entries),
    do: Enum.reverse(entries)

  defp paired_length(messages, subscribers), do: paired_length(messages, subscribers, 0)

  defp paired_length([_message | messages], [_count | subscribers], count),
    do: paired_length(messages, subscribers, count + 1)

  defp paired_length([], [], count), do: {:ok, count}
  defp paired_length(_messages, _subscribers, _count), do: :error

  defp keep_newest(entries, max_len) do
    discard = length(entries) - max_len
    if discard > 0, do: Enum.drop(entries, discard), else: entries
  end

  defp reserve_ids(count) do
    case :persistent_term.get(@counter_key, nil) do
      counter when is_reference(counter) -> :atomics.add_get(counter, 1, count) - count
      _ -> System.unique_integer([:positive, :monotonic])
    end
  end

  defp record_publish_batch_individually(channel, [message | messages], [count | counts]) do
    record_publish(channel, byte_size(message), count)
    record_publish_batch_individually(channel, messages, counts)
  end

  defp record_publish_batch_individually(_channel, [], []), do: :ok

  defp max_len do
    case :persistent_term.get(@max_len_key, @default_max_len) do
      n when is_integer(n) and n >= 0 -> n
      _ -> @default_max_len
    end
  end

  defp normalized_max_len(max_len) when is_integer(max_len) and max_len >= 0, do: max_len
  defp normalized_max_len(_invalid), do: @default_max_len

  defp sample_publish?(counter, sample_every) do
    rem(:atomics.add_get(counter, 1, 1) - 1, sample_every) == 0
  end

  defp publish_sample_counter do
    case :persistent_term.get(@publish_sample_counter_key, nil) do
      counter when is_reference(counter) -> counter
      _missing -> :atomics.new(1, signed: false)
    end
  end

  defp next_id do
    case :persistent_term.get(@counter_key, nil) do
      counter when is_reference(counter) -> :atomics.add_get(counter, 1, 1) - 1
      _ -> System.unique_integer([:positive, :monotonic])
    end
  end

  defp reset_counter do
    case :persistent_term.get(@counter_key, nil) do
      counter when is_reference(counter) -> :atomics.put(counter, 1, 0)
      _ -> :ok
    end
  end

  defp reset_publish_sample_counter do
    case :persistent_term.get(@publish_sample_counter_key, nil) do
      counter when is_reference(counter) -> :atomics.put(counter, 1, 0)
      _ -> :ok
    end
  end

  defp maybe_evict_overflow(id, max_len) do
    interval = min(max(max_len, 1), 16)

    if rem(id + 1, interval) == 0 do
      evict_overflow(max_len)
    end
  end

  defp evict_overflow(max_len) do
    case :ets.info(@table, :size) do
      size when is_integer(size) and size > max_len -> delete_oldest(size - max_len)
      _ -> :ok
    end
  end

  defp delete_oldest(remaining) when remaining <= 0, do: :ok

  defp delete_oldest(remaining) do
    case :ets.first(@table) do
      :"$end_of_table" ->
        :ok

      id ->
        :ets.delete(@table, id)
        delete_oldest(remaining - 1)
    end
  end

  defp newest_entries(:"$end_of_table", _remaining, acc), do: Enum.reverse(acc)
  defp newest_entries(_id, 0, acc), do: Enum.reverse(acc)

  defp newest_entries(id, remaining, acc) do
    previous_id = :ets.prev(@table, id)

    case :ets.lookup(@table, id) do
      [{^id, entry}] ->
        newest_entries(previous_id, remaining - 1, [materialize_entry(entry, id) | acc])

      [] ->
        newest_entries(previous_id, remaining, acc)
    end
  end

  defp bounded_count(nil), do: @default_read_count
  defp bounded_count(count) when is_integer(count), do: count |> max(0) |> min(@max_read_count)
  defp bounded_count(_count), do: @default_read_count

  defp table_ready?, do: :ets.whereis(@table) != :undefined

  defp normalize_and_timestamp_entry(
         {:publish, channel, message_bytes, subscribers},
         timestamp_us
       ) do
    {:publish, timestamp_us, normalize_binary(channel), message_bytes, subscribers}
  end

  defp normalize_and_timestamp_entry(
         {:subscription, command, target_type, targets, target_count},
         timestamp_us
       ) do
    {:subscription, timestamp_us, normalize_binary(command), target_type, sample_target(targets),
     target_count}
  end

  defp materialize_entry(
         {:publish, timestamp_us, target, message_bytes, subscribers},
         id
       ) do
    %{
      id: id,
      timestamp_us: timestamp_us,
      command: "PUBLISH",
      target_type: :channel,
      target: target,
      targets: 1,
      subscribers: subscribers,
      message_bytes: message_bytes
    }
  end

  defp materialize_entry(
         {:subscription, timestamp_us, command, target_type, target, target_count},
         id
       ) do
    %{
      id: id,
      timestamp_us: timestamp_us,
      command: command,
      target_type: target_type,
      target: target,
      targets: target_count,
      subscribers: nil,
      message_bytes: nil
    }
  end

  defp materialize_entry(entry, id) when is_map(entry), do: Map.put(entry, :id, id)

  defp sample_target([target | rest]) do
    suffix = if rest == [], do: "", else: " +#{length(rest)}"
    normalize_binary(target) <> suffix
  end

  defp normalize_binary(value) when not is_binary(value),
    do: value |> to_string() |> normalize_binary()

  defp normalize_binary(value) when byte_size(value) <= @max_metadata_bytes do
    :binary.copy(value)
  end

  defp normalize_binary(value) do
    omitted = byte_size(value) - @max_metadata_bytes
    prefix = value |> binary_part(0, @max_metadata_bytes) |> :binary.copy()
    prefix <> "...[#{omitted} more bytes]"
  end
end
