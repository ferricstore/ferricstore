defmodule Ferricstore.Commands.Stream.GroupInspect do
  @moduledoc false

  # XPENDING, XINFO GROUPS and XINFO CONSUMERS: a consumer group's pending
  # entries and consumers, read from the group's state as XREADGROUP keeps it.
  # A pending entry delivered once has a delivery count of 1.

  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Stream.{Groups, ID}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @spec pending(binary(), binary(), [binary()], term()) :: term()
  def pending(key, group, args, store) do
    with {:ok, form} <- parse_pending(args),
         :ok <- TypeRegistry.command_check_type(key, :stream, store),
         {:ok, pending} <- pending_of(store, key, group) do
      case form do
        :summary -> summary(pending)
        {:range, options} -> range(pending, options, CommandTime.now_ms())
      end
    end
  end

  @spec groups(binary(), term()) :: term()
  def groups(key, store) do
    with :ok <- TypeRegistry.command_check_type(key, :stream, store),
         {:ok, names} <- group_names(store, key) do
      if names == [] and not stream_exists?(store, key) do
        {:error, "ERR no such key"}
      else
        Enum.reduce_while(names, {:ok, []}, fn group, {:ok, acc} ->
          case Groups.lookup(store, key, group) do
            {:ok, last_delivered, consumers, pending} ->
              info = %{
                "name" => group,
                "consumers" => map_size(consumers),
                "pending" => map_size(pending),
                "last-delivered-id" => last_delivered,
                "entries-read" => nil,
                "lag" => nil
              }

              {:cont, {:ok, [info | acc]}}

            # Deleted between the scan and the read.
            :missing ->
              {:cont, {:ok, acc}}

            {:error, _reason} = error ->
              {:halt, error}
          end
        end)
        |> case do
          {:ok, infos} -> Enum.reverse(infos)
          {:error, _reason} = error -> error
        end
      end
    end
  end

  @spec consumers(binary(), binary(), term()) :: term()
  def consumers(key, group, store) do
    with :ok <- TypeRegistry.command_check_type(key, :stream, store) do
      case Groups.lookup(store, key, group) do
        {:ok, _last_delivered, consumers, pending} ->
          now_ms = CommandTime.now_ms()
          owned = pending |> Map.values() |> Enum.frequencies_by(&elem(&1, 0))

          consumers
          |> Enum.sort_by(&elem(&1, 0))
          |> Enum.map(fn {name, seen_at_ms} ->
            idle = max(now_ms - seen_at_ms, 0)

            %{
              "name" => name,
              "pending" => Map.get(owned, name, 0),
              "idle" => idle,
              "inactive" => idle
            }
          end)

        :missing ->
          {:error, "NOGROUP No such consumer group '#{group}' for key name '#{key}'"}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp pending_of(store, key, group) do
    case Groups.lookup(store, key, group) do
      {:ok, _last_delivered, _consumers, pending} -> {:ok, pending}
      :missing -> {:error, "NOGROUP No such key '#{key}' or consumer group '#{group}'"}
      {:error, _reason} = error -> error
    end
  end

  defp summary(pending) when map_size(pending) == 0, do: [0, nil, nil, nil]

  defp summary(pending) do
    ids = pending |> Map.keys() |> Enum.sort_by(&ID.parse_id!/1)

    per_consumer =
      pending
      |> Map.values()
      |> Enum.frequencies_by(&elem(&1, 0))
      |> Enum.sort()
      |> Enum.map(fn {consumer, count} -> [consumer, Integer.to_string(count)] end)

    [map_size(pending), List.first(ids), List.last(ids), per_consumer]
  end

  defp range(pending, options, now_ms) do
    pending
    |> Enum.map(fn {id, entry} -> {ID.parse_id!(id), id, entry} end)
    |> Enum.filter(fn {parsed, _id, entry} ->
      ID.in_range?(parsed, options.start, options.stop) and
        consumer_matches?(entry, options.consumer) and
        idle(entry, now_ms) >= options.idle
    end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.take(options.count)
    |> Enum.map(fn {_parsed, id, entry} ->
      [id, elem(entry, 0), idle(entry, now_ms), delivery_count(entry)]
    end)
  end

  defp consumer_matches?(_entry, nil), do: true
  defp consumer_matches?(entry, consumer), do: elem(entry, 0) == consumer

  defp idle(entry, now_ms), do: max(now_ms - elem(entry, 1), 0)

  defp delivery_count({_consumer, _delivered_at_ms, count}), do: count
  defp delivery_count({_consumer, _delivered_at_ms}), do: 1

  defp parse_pending([]), do: {:ok, :summary}

  defp parse_pending([option, min_idle | rest]) when byte_size(option) == 4 do
    if String.upcase(option) == "IDLE" do
      with {:ok, idle} <- integer(min_idle), do: parse_range(rest, max(idle, 0))
    else
      parse_range([option, min_idle | rest], 0)
    end
  end

  defp parse_pending(args), do: parse_range(args, 0)

  defp parse_range([start, stop, count | consumer], idle) when length(consumer) <= 1 do
    with {:ok, count} <- integer(count),
         {:ok, start} <- ID.parse_range_id(start, :min),
         {:ok, stop} <- ID.parse_range_id(stop, :max) do
      {:ok,
       {:range,
        %{
          start: start,
          stop: stop,
          count: max(count, 0),
          consumer: List.first(consumer),
          idle: idle
        }}}
    end
  end

  defp parse_range(_args, _idle), do: {:error, "ERR syntax error"}

  defp integer(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp group_names(store, key) do
    prefix = CompoundKey.stream_group_prefix(key)

    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      pairs when is_list(pairs) -> {:ok, pairs |> Enum.map(&elem(&1, 0)) |> Enum.sort()}
    end
  end

  defp stream_exists?(store, key), do: TypeRegistry.get_type(key, store) == "stream"
end
