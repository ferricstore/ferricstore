defmodule Ferricstore.Raft.WARaftSegmentReader.DiskReader do
  @moduledoc false

  use GenServer

  alias Ferricstore.Raft.WARaftSegmentReader.DiskReaderSupervisor

  @registry Ferricstore.Raft.WARaftSegmentReader.DiskReaderRegistry
  @default_max_handles 8
  @maximum_handles 64
  @default_lanes 4
  @maximum_lanes 16
  @call_timeout 30_000
  @maximum_batch_records 4_096
  @maximum_batch_bytes 1 * 1_024 * 1_024 * 1_024

  @type location :: {non_neg_integer(), non_neg_integer(), pos_integer()}

  @spec registry() :: atom()
  def registry, do: @registry

  @spec start_link({binary(), non_neg_integer(), pos_integer()}) :: GenServer.on_start()
  def start_link({root, lane, max_handles} = init)
      when is_binary(root) and root != "" and is_integer(lane) and lane >= 0 and
             lane < @maximum_lanes and is_integer(max_handles) and max_handles > 0 do
    GenServer.start_link(__MODULE__, init, name: via(root, lane))
  end

  @spec read(binary(), pos_integer(), location()) ::
          {:ok, term()} | :not_found | {:error, term()}
  def read(root, index, {ordinal, offset, encoded_size} = location)
      when is_binary(root) and root != "" and is_integer(index) and index > 0 and
             is_integer(ordinal) and ordinal >= 0 and is_integer(offset) and offset >= 0 and
             is_integer(encoded_size) and encoded_size >= 8 do
    case ensure_selected_reader(root) do
      {:ok, pid} -> GenServer.call(pid, {:read, index, location}, @call_timeout)
      {:error, reason} -> {:error, {:segment_reader_unavailable, reason}}
    end
  catch
    :exit, reason -> {:error, {:segment_reader_unavailable, reason}}
  end

  def read(_root, _index, _location), do: {:error, :invalid_segment_reader_location}

  @spec read_many(binary(), [{pos_integer(), location()}]) ::
          {:ok, [term()]} | {:error, term()}
  def read_many(root, requests), do: read_many(root, requests, @call_timeout)

  @spec read_many(binary(), [{pos_integer(), location()}], pos_integer()) ::
          {:ok, [term()]} | {:error, term()}
  def read_many(root, requests, timeout_ms)
      when is_binary(root) and root != "" and is_list(requests) and is_integer(timeout_ms) and
             timeout_ms > 0 do
    read_many_at(root, Path.join(root, "apply_projection_log"), requests, timeout_ms)
  end

  def read_many(_root, _requests, _timeout_ms), do: {:error, :invalid_segment_reader_batch}

  @doc false
  @spec read_many_at(binary(), binary(), [{pos_integer(), location()}], pos_integer()) ::
          {:ok, [term()]} | {:error, term()}
  def read_many_at(root, log_root, requests, timeout_ms)
      when is_binary(root) and root != "" and is_binary(log_root) and log_root != "" and
             is_list(requests) and is_integer(timeout_ms) and timeout_ms > 0 do
    with :ok <- validate_batch(requests) do
      case requests do
        [] ->
          {:ok, []}

        [_ | _] ->
          case ensure_selected_reader(root) do
            {:ok, pid} ->
              GenServer.call(
                pid,
                {:read_many, log_root, requests},
                min(timeout_ms, @call_timeout)
              )

            {:error, reason} ->
              {:error, {:segment_reader_unavailable, reason}}
          end
      end
    end
  catch
    :exit, {:timeout, _call} = reason -> {:error, {:segment_reader_timeout, reason}}
    :exit, reason -> {:error, {:segment_reader_unavailable, reason}}
  end

  def read_many_at(_root, _log_root, _requests, _timeout_ms),
    do: {:error, :invalid_segment_reader_batch}

  @spec invalidate(binary()) :: :ok
  def invalidate(root) when is_binary(root) and root != "" do
    root
    |> reader_pids()
    |> Enum.each(&safe_call(&1, :invalidate))

    :ok
  end

  def invalidate(_root), do: :ok

  @doc false
  @spec status(binary()) :: map()
  def status(root) when is_binary(root) and root != "" do
    statuses =
      root
      |> reader_pids()
      |> Enum.map(&safe_status/1)

    {configured_lanes, max_handles} = configured_pool()

    Enum.reduce(statuses, empty_status(), fn status, total ->
      %{
        total
        | handles: total.handles + status.handles,
          opens: total.opens + status.opens,
          evictions: total.evictions + status.evictions,
          lanes: total.lanes + 1
      }
    end)
    |> Map.put(:configured_lanes, configured_lanes)
    |> Map.put(:max_handles, max_handles)
  end

  @doc false
  @spec stop(binary()) :: :ok
  def stop(root), do: DiskReaderSupervisor.stop_reader(root)

  @doc false
  @spec whereis(binary()) :: pid() | nil
  def whereis(root) when is_binary(root), do: whereis(root, 0)

  @doc false
  @spec whereis(binary(), non_neg_integer()) :: pid() | nil
  def whereis(root, lane) when is_binary(root) and is_integer(lane) and lane >= 0 do
    case Registry.lookup(@registry, {root, lane}) do
      [{pid, _value}] when is_pid(pid) -> pid
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  def whereis(_root, _lane), do: nil

  @doc false
  @spec reader_pids(binary()) :: [pid()]
  def reader_pids(root) when is_binary(root) do
    0..(@maximum_lanes - 1)
    |> Enum.map(&whereis(root, &1))
    |> Enum.filter(&is_pid/1)
  end

  def reader_pids(_root), do: []

  @impl true
  def init({root, lane, max_handles}) do
    projection_root = Path.join(root, "apply_projection_log")

    {:ok,
     %{
       root: root,
       lane: lane,
       projection_root: projection_root,
       readers: %{},
       clock: 0,
       opens: 0,
       evictions: 0,
       max_handles: max_handles
     }}
  end

  @impl true
  def handle_call({:read, index, {ordinal, offset, encoded_size}}, _from, state) do
    case acquire_reader(state, index, ordinal) do
      {:ok, reader, state} ->
        result =
          :ferricstore_waraft_spike_segment_log.read_disk_reader(
            reader,
            index,
            offset,
            encoded_size
          )

        {:reply, result, state}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}

      {:not_found, state} ->
        {:reply, :not_found, state}
    end
  end

  def handle_call({:read_many, log_root, requests}, _from, state) do
    indexed = Enum.with_index(requests)

    groups =
      Enum.group_by(indexed, fn {{_index, {ordinal, _offset, _encoded_size}}, _position} ->
        ordinal
      end)

    case read_groups(log_root, groups, state, %{}) do
      {:ok, entries, state} ->
        ordered = Enum.map(0..(length(requests) - 1), &Map.fetch!(entries, &1))
        {:reply, {:ok, ordered}, state}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:invalidate, _from, state) do
    {:reply, :ok, close_all(state)}
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       handles: map_size(state.readers),
       opens: state.opens,
       evictions: state.evictions,
       max_handles: state.max_handles,
       lane: state.lane
     }, state}
  end

  @impl true
  def terminate(_reason, state) do
    _ = close_all(state)
    :ok
  end

  defp acquire_reader(state, index, ordinal),
    do: acquire_reader(state, state.projection_root, index, ordinal)

  defp acquire_reader(state, log_root, index, ordinal) do
    clock = state.clock + 1
    cache_key = {log_root, ordinal}

    case Map.fetch(state.readers, cache_key) do
      {:ok, %{reader: reader} = cached} ->
        readers = Map.put(state.readers, cache_key, %{cached | used_at: clock})
        {:ok, reader, %{state | readers: readers, clock: clock}}

      :error ->
        case :ferricstore_waraft_spike_segment_log.open_disk_reader(
               to_charlist(log_root),
               index,
               ordinal
             ) do
          {:ok, reader} ->
            state = state |> Map.put(:clock, clock) |> make_room()
            readers = Map.put(state.readers, cache_key, %{reader: reader, used_at: clock})
            {:ok, reader, %{state | readers: readers, opens: state.opens + 1}}

          :not_found ->
            {:not_found, %{state | clock: clock}}

          {:error, reason} ->
            {:error, reason, %{state | clock: clock}}

          invalid ->
            {:error, {:invalid_segment_reader_open_result, invalid}, %{state | clock: clock}}
        end
    end
  end

  defp read_groups(log_root, groups, state, entries) do
    Enum.reduce_while(groups, {:ok, entries, state}, fn
      {ordinal, [{{first_index, _location}, _position} | _] = group}, {:ok, entries, state} ->
        case acquire_reader(state, log_root, first_index, ordinal) do
          {:ok, reader, state} ->
            reads =
              Enum.map(group, fn {{index, {_ordinal, offset, encoded_size}}, _position} ->
                {index, offset, encoded_size}
              end)

            case :ferricstore_waraft_spike_segment_log.read_disk_reader_many(reader, reads) do
              {:ok, group_entries} when length(group_entries) == length(group) ->
                entries =
                  Enum.zip(group, group_entries)
                  |> Enum.reduce(entries, fn {{_request, position}, entry}, acc ->
                    Map.put(acc, position, entry)
                  end)

                {:cont, {:ok, entries, state}}

              {:ok, _entries} ->
                {:halt, {:error, :segment_reader_batch_count_mismatch, state}}

              :not_found ->
                {:halt, {:error, :segment_reader_batch_not_found, state}}

              {:error, reason} ->
                {:halt, {:error, reason, state}}

              invalid ->
                {:halt, {:error, {:invalid_segment_reader_batch_result, invalid}, state}}
            end

          {:error, reason, state} ->
            {:halt, {:error, reason, state}}

          {:not_found, state} ->
            {:halt, {:error, :segment_reader_batch_not_found, state}}
        end
    end)
  end

  defp validate_batch(requests) do
    Enum.reduce_while(requests, {0, 0}, fn
      {index, {ordinal, offset, encoded_size}}, {count, bytes}
      when is_integer(index) and index > 0 and is_integer(ordinal) and ordinal >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(encoded_size) and
             encoded_size >= 8 ->
        next_count = count + 1
        next_bytes = bytes + encoded_size

        if next_count <= @maximum_batch_records and next_bytes <= @maximum_batch_bytes,
          do: {:cont, {next_count, next_bytes}},
          else: {:halt, :invalid}

      _invalid, _acc ->
        {:halt, :invalid}
    end)
    |> case do
      :invalid -> {:error, :invalid_segment_reader_batch}
      {_count, _bytes} -> :ok
    end
  end

  defp make_room(%{readers: readers, max_handles: maximum} = state)
       when map_size(readers) < maximum,
       do: state

  defp make_room(state) do
    {cache_key, %{reader: reader}} =
      Enum.min_by(state.readers, fn {_ordinal, cached} -> cached.used_at end)

    _ = :ferricstore_waraft_spike_segment_log.close_disk_reader(reader)

    %{
      state
      | readers: Map.delete(state.readers, cache_key),
        evictions: state.evictions + 1
    }
  end

  defp close_all(state) do
    Enum.each(state.readers, fn {_ordinal, %{reader: reader}} ->
      _ = :ferricstore_waraft_spike_segment_log.close_disk_reader(reader)
    end)

    %{state | readers: %{}}
  end

  defp ensure_selected_reader(root) do
    {lanes, max_handles} = configured_pool()
    lane = :erlang.phash2(self(), lanes)
    lane_handles = lane_handle_quota(max_handles, lanes, lane)
    DiskReaderSupervisor.ensure_reader(root, lane, lane_handles)
  end

  defp configured_pool do
    max_handles =
      case Application.get_env(
             :ferricstore,
             :waraft_segment_reader_cache_max_handles,
             @default_max_handles
           ) do
        value when is_integer(value) and value > 0 and value <= @maximum_handles -> value
        _invalid -> @default_max_handles
      end

    configured_lanes =
      case Application.get_env(:ferricstore, :waraft_segment_reader_lanes, @default_lanes) do
        value when is_integer(value) and value > 0 and value <= @maximum_lanes -> value
        _invalid -> @default_lanes
      end

    {min(configured_lanes, max_handles), max_handles}
  end

  defp lane_handle_quota(total, lanes, lane) do
    div(total, lanes) + if lane < rem(total, lanes), do: 1, else: 0
  end

  defp safe_call(pid, message) do
    GenServer.call(pid, message, @call_timeout)
  catch
    :exit, _reason -> :ok
  end

  defp safe_status(pid) do
    case safe_call(pid, :status) do
      %{handles: handles, opens: opens, evictions: evictions}
      when is_integer(handles) and is_integer(opens) and is_integer(evictions) ->
        %{handles: handles, opens: opens, evictions: evictions}

      _invalid ->
        %{handles: 0, opens: 0, evictions: 0}
    end
  end

  defp via(root, lane), do: {:via, Registry, {@registry, {root, lane}}}

  defp empty_status do
    {configured_lanes, max_handles} = configured_pool()

    %{
      handles: 0,
      opens: 0,
      evictions: 0,
      lanes: 0,
      configured_lanes: configured_lanes,
      max_handles: max_handles
    }
  end
end
