defmodule Ferricstore.Raft.ReplaySafeIndexWriter do
  @moduledoc false

  use GenServer

  alias Ferricstore.Raft.ReplaySafeIndex

  require Logger

  @default_flush_delay_ms 0
  @default_retry_delay_ms 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    instance_ctx = Keyword.get(opts, :instance_ctx)
    name = Keyword.get(opts, :name, process_name(shard_index, instance_ctx))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec process_name(non_neg_integer(), map() | nil) :: atom()
  def process_name(shard_index, nil), do: :"ferricstore_replay_safe_index_writer_#{shard_index}"

  def process_name(shard_index, %{name: instance_name}),
    do: :"ferricstore_replay_safe_index_writer_#{instance_name}_#{shard_index}"

  def process_name(shard_index, _instance_ctx), do: process_name(shard_index, nil)

  @spec durable?(map() | nil, non_neg_integer(), binary(), non_neg_integer()) :: boolean()
  def durable?(instance_ctx, shard_index, shard_data_path, index) do
    durable_index(instance_ctx, shard_index, shard_data_path) >= index
  end

  @spec durable_index(map() | nil, non_neg_integer(), binary()) :: non_neg_integer()
  def durable_index(%{replay_safe_index: replay_safe_index}, shard_index, _shard_data_path)
      when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      :atomics.get(replay_safe_index, shard_index + 1)
    else
      0
    end
  rescue
    _ -> 0
  end

  def durable_index(_instance_ctx, _shard_index, shard_data_path) do
    ReplaySafeIndex.read(shard_data_path)
  end

  @spec request(map() | nil, non_neg_integer(), binary(), non_neg_integer()) ::
          :requested | :durable | {:error, term()}
  def request(instance_ctx, shard_index, shard_data_path, index) do
    publish_requested(instance_ctx, shard_index, index)

    cond do
      durable?(instance_ctx, shard_index, shard_data_path, index) ->
        :durable

      is_pid(writer_pid = Process.whereis(process_name(shard_index, instance_ctx))) ->
        GenServer.cast(writer_pid, {:persist, index})
        :requested

      true ->
        sync_persist(instance_ctx, shard_index, shard_data_path, index)
    end
  catch
    :exit, reason ->
      {:error, reason}
  end

  @impl true
  def init(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    shard_data_path = Keyword.fetch!(opts, :shard_data_path)
    instance_ctx = Keyword.get(opts, :instance_ctx)
    flush_delay_ms = Keyword.get(opts, :flush_delay_ms, @default_flush_delay_ms)
    retry_delay_ms = Keyword.get(opts, :retry_delay_ms, @default_retry_delay_ms)
    durable_index = ReplaySafeIndex.read(shard_data_path)
    publish_initial_durable(instance_ctx, shard_index, durable_index)

    {:ok,
     %{
       shard_index: shard_index,
       shard_data_path: shard_data_path,
       instance_ctx: instance_ctx,
       durable_index: durable_index,
       requested_index: durable_index,
       flush_delay_ms: flush_delay_ms,
       retry_delay_ms: retry_delay_ms,
       flush_ref: nil
     }}
  end

  @impl true
  def handle_cast({:persist, index}, state) when is_integer(index) and index >= 0 do
    requested_index = max(state.requested_index, index)
    publish_requested(state.instance_ctx, state.shard_index, requested_index)
    {:noreply, schedule_flush(%{state | requested_index: requested_index})}
  end

  @impl true
  def handle_info(:flush, state) do
    state = %{state | flush_ref: nil}

    if state.requested_index > state.durable_index do
      {:noreply, persist_requested(state)}
    else
      {:noreply, state}
    end
  end

  defp sync_persist(instance_ctx, shard_index, shard_data_path, index) do
    publish_requested(instance_ctx, shard_index, index)

    case ReplaySafeIndex.persist(shard_data_path, index) do
      :ok ->
        publish_durable(instance_ctx, shard_index, index)
        :durable

      {:error, _reason} = error ->
        error
    end
  end

  defp schedule_flush(%{flush_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_flush(state) do
    ref = Process.send_after(self(), :flush, state.flush_delay_ms)
    %{state | flush_ref: ref}
  end

  defp persist_requested(state) do
    index = state.requested_index
    started_at = System.monotonic_time()

    case ReplaySafeIndex.persist(state.shard_data_path, index) do
      :ok ->
        publish_durable(state.instance_ctx, state.shard_index, index)
        emit_persist(:ok, state, index, started_at)
        poke_release_cursor(state, index)
        %{state | durable_index: index}

      {:error, reason} ->
        record_persist_failure(state.instance_ctx, state.shard_index)
        emit_persist({:error, reason}, state, index, started_at)
        schedule_retry(state)
    end
  end

  defp schedule_retry(%{flush_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_retry(state) do
    ref = Process.send_after(self(), :flush, max(state.retry_delay_ms, 1))
    %{state | flush_ref: ref}
  end

  defp poke_release_cursor(_state, _index), do: :ok

  defp publish_durable(%{replay_safe_index: replay_safe_index}, shard_index, index)
       when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      put_atomic_max(replay_safe_index, shard_index, index)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp publish_durable(_instance_ctx, _shard_index, _index), do: :ok

  defp publish_initial_durable(%{replay_safe_index: replay_safe_index}, shard_index, index)
       when is_reference(replay_safe_index) do
    if shard_index < :atomics.info(replay_safe_index).size do
      :atomics.put(replay_safe_index, shard_index + 1, index)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp publish_initial_durable(_instance_ctx, _shard_index, _index), do: :ok

  defp publish_requested(%{replay_safe_requested_index: requested_index}, shard_index, index)
       when is_reference(requested_index) do
    put_atomic_max(requested_index, shard_index, index)
  rescue
    _ -> :ok
  end

  defp publish_requested(_instance_ctx, _shard_index, _index), do: :ok

  defp record_persist_failure(%{replay_safe_persist_failures: failures}, shard_index)
       when is_reference(failures) do
    if shard_index < :atomics.info(failures).size do
      :atomics.add(failures, shard_index + 1, 1)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp record_persist_failure(_instance_ctx, _shard_index), do: :ok

  defp put_atomic_max(ref, shard_index, value) do
    if shard_index < :atomics.info(ref).size do
      position = shard_index + 1
      compare_exchange_atomic_max(ref, position, value, :atomics.get(ref, position))
    end

    :ok
  end

  defp compare_exchange_atomic_max(_ref, _position, value, current) when value <= current,
    do: :ok

  defp compare_exchange_atomic_max(ref, position, value, current) do
    case :atomics.compare_exchange(ref, position, current, value) do
      :ok -> :ok
      actual when is_integer(actual) -> compare_exchange_atomic_max(ref, position, value, actual)
    end
  end

  defp emit_persist(status, state, index, started_at) do
    requested_index = max(state.requested_index, index)
    durable_index = if status == :ok, do: index, else: state.durable_index

    :telemetry.execute(
      [:ferricstore, :raft, :replay_safe_index, :persist],
      %{
        duration_us: duration_us(started_at),
        index: index,
        requested_index: requested_index,
        durable_index: durable_index,
        lag: max(requested_index - durable_index, 0)
      },
      %{
        status: persist_status(status),
        shard_index: state.shard_index,
        reason: persist_reason(status)
      }
    )

    if match?({:error, _}, status) do
      Logger.warning(
        "failed to persist replay-safe index #{index} for shard #{state.shard_index}: #{inspect(status)}"
      )
    end
  end

  defp persist_status(:ok), do: :ok
  defp persist_status({:error, _}), do: :error

  defp persist_reason(:ok), do: :none
  defp persist_reason({:error, reason}), do: reason

  defp duration_us(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
  end
end
