defmodule Ferricstore.PubSub.ActivityLog do
  @moduledoc """
  Metadata-only ring buffer for Pub/Sub activity.

  Message payloads are intentionally not stored. Publish entries keep only
  channel, message byte size, and receiver count.
  """

  use GenServer

  @table :ferricstore_pubsub_activity_log
  @counter_key :ferricstore_pubsub_activity_log_counter
  @max_len_key :ferricstore_pubsub_activity_log_max_len
  @default_max_len 512
  @default_read_count 128
  @max_read_count 500

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
    record(%{
      command: "PUBLISH",
      target_type: :channel,
      target: normalize_binary(channel),
      targets: 1,
      subscribers: max(subscribers, 0),
      message_bytes: max(message_bytes, 0)
    })
  end

  @spec record_subscription(binary(), :channel | :pattern, [binary()]) :: :ok
  def record_subscription(_command, _target_type, []), do: :ok

  def record_subscription(command, target_type, targets) when is_list(targets) do
    record(%{
      command: normalize_binary(command),
      target_type: target_type,
      target: sample_target(targets),
      targets: length(targets),
      subscribers: nil,
      message_bytes: nil
    })
  end

  @spec get(non_neg_integer() | nil) :: [entry()]
  def get(count \\ @default_read_count) do
    count = bounded_count(count)

    if table_ready?() do
      @table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {id, _entry} -> id end, :desc)
      |> Enum.take(count)
      |> Enum.map(fn {id, entry} -> Map.put(entry, :id, id) end)
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

    :persistent_term.put(
      @max_len_key,
      Application.get_env(:ferricstore, :pubsub_activity_log_max_len, @default_max_len)
    )

    {:ok, %{table: table}}
  end

  defp record(entry) do
    max_len = max_len()

    if max_len > 0 and table_ready?() do
      id = next_id()

      :ets.insert(
        @table,
        {id, Map.put(entry, :timestamp_us, System.os_time(:microsecond))}
      )

      maybe_evict_overflow(id, max_len)
    end

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp max_len do
    case :persistent_term.get(@max_len_key, @default_max_len) do
      n when is_integer(n) and n >= 0 -> n
      _ -> @default_max_len
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

  defp bounded_count(nil), do: @default_read_count
  defp bounded_count(count) when is_integer(count), do: count |> max(0) |> min(@max_read_count)
  defp bounded_count(_count), do: @default_read_count

  defp table_ready?, do: :ets.whereis(@table) != :undefined

  defp sample_target([target | rest]) do
    suffix = if rest == [], do: "", else: " +#{length(rest)}"
    normalize_binary(target) <> suffix
  end

  defp normalize_binary(value) when is_binary(value), do: value
  defp normalize_binary(value), do: to_string(value)
end
