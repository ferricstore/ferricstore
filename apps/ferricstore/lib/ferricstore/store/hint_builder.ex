defmodule Ferricstore.Store.HintBuilder do
  @moduledoc false

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{HintMetadata, SegmentLock}

  require Logger

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)
    ctx = Keyword.get(opts, :instance_ctx)
    name = Keyword.get(opts, :name, process_name(index, ctx))
    GenServer.start_link(__MODULE__, %{index: index, instance_ctx: ctx}, name: name)
  end

  @spec process_name(non_neg_integer(), map() | nil) :: atom()
  def process_name(index, nil), do: :"ferricstore_hint_builder_#{index}"
  def process_name(index, %{name: :default}), do: :"ferricstore_hint_builder_#{index}"

  def process_name(index, %{name: instance_name}),
    do: :"#{instance_name}.HintBuilder.#{index}"

  @spec enqueue(map() | nil, non_neg_integer(), non_neg_integer(), binary(), binary()) :: :ok
  def enqueue(ctx, index, file_id, log_path, shard_path) do
    GenServer.cast(
      process_name(index, ctx),
      {:build, file_id, log_path, hint_path(shard_path, file_id), shard_path}
    )
  end

  @spec build_now(binary(), binary(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def build_now(log_path, hint_path, file_id, shard_path) do
    SegmentLock.with_lock(log_path, fn ->
      do_build_now(log_path, hint_path, file_id, shard_path)
    end)
  end

  defp do_build_now(log_path, hint_path, file_id, shard_path) do
    with {:ok, before_snapshot} <- HintMetadata.source_snapshot(log_path),
         :ok <- HintMetadata.prepare_publish(hint_path, shard_path),
         {:ok, _entry_count, _end_offset} <-
           NIF.v2_build_hint_file_from_log(log_path, hint_path, file_id),
         {:ok, after_snapshot} <- HintMetadata.source_snapshot(log_path),
         :ok <- ensure_unchanged(before_snapshot, after_snapshot),
         :ok <-
           HintMetadata.publish(log_path, hint_path, file_id, before_snapshot, shard_path) do
      :ok
    else
      {:error, :source_changed} = error ->
        _ = Ferricstore.FS.rm(hint_path)
        _ = HintMetadata.remove(hint_path)
        _ = NIF.v2_fsync_dir(shard_path)
        error

      {:error, _reason} = error ->
        error

      other ->
        {:error, {:unexpected_hint_build_result, other}}
    end
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_cast({:build, file_id, log_path, hint_path, shard_path}, state) do
    started_at = System.monotonic_time()
    result = build_now(log_path, hint_path, file_id, shard_path)

    :telemetry.execute(
      [:ferricstore, :bitcask, :hint_build],
      %{duration: System.monotonic_time() - started_at},
      %{file_id: file_id, shard_index: state.index, status: hint_status(result)}
    )

    if match?({:error, _}, result) do
      Logger.warning(
        "HintBuilder shard=#{state.index} failed for #{log_path}: #{inspect(result)}"
      )
    end

    {:noreply, state}
  end

  defp hint_path(shard_path, file_id) do
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.hint")
  end

  defp ensure_unchanged(snapshot, snapshot), do: :ok
  defp ensure_unchanged(_before, _after), do: {:error, :source_changed}

  defp hint_status(:ok), do: :ok
  defp hint_status({:error, _reason}), do: :error
  defp hint_status(_other), do: :error
end
