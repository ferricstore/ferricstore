defmodule Ferricstore.Flow.LMDBWriter.Config do
  @moduledoc false

  @default_lagged_flush_interval_ms 500
  @default_lagged_flush_jitter_ms 250
  @default_lagged_flush_quiet_ms 250
  @default_lagged_flush_max_lag_ms 30_000
  @default_lagged_max_ops 25_000
  @default_flush_chunk_ops 5_000
  @default_lagged_flush_chunk_pause_ms 1
  @max_timer_ms 4_294_967_295
  @max_batch_ops 1_000_000
  @max_flush_chunk_ops 100_000
  @max_flush_chunk_pause_ms 60_000

  def instance_name_from_opts(opts) do
    case {Keyword.get(opts, :instance_name), Keyword.get(opts, :instance_ctx)} do
      {name, _ctx} when is_atom(name) and not is_nil(name) -> name
      {_name, %{name: name}} when is_atom(name) and not is_nil(name) -> name
      _ -> :default
    end
  end

  def instance_name_from_ctx(%{name: name}) when is_atom(name) and not is_nil(name), do: name
  def instance_name_from_ctx(_ctx), do: :default

  def default_flush_interval_ms, do: @default_lagged_flush_interval_ms
  def default_max_ops, do: @default_lagged_max_ops
  def default_flush_on_max_ops(_mode), do: true
  def default_flush_jitter_ms, do: @default_lagged_flush_jitter_ms
  def default_flush_quiet_ms, do: @default_lagged_flush_quiet_ms
  def default_flush_max_lag_ms, do: @default_lagged_flush_max_lag_ms
  def default_flush_chunk_ops, do: @default_flush_chunk_ops
  def default_flush_chunk_pause_ms, do: @default_lagged_flush_chunk_pause_ms

  def initial_state(opts, instance_name, shard_index, data_dir, enqueue_seq) do
    mode = Ferricstore.Flow.LMDB.mode()
    shard_data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

    %{
      instance_name: instance_name,
      mode: mode,
      shard_index: shard_index,
      data_dir: data_dir,
      shard_data_path: shard_data_path,
      path: Ferricstore.Flow.LMDB.path(shard_data_path),
      instance_ctx: Keyword.get(opts, :instance_ctx),
      pending: [],
      pending_after_flush: [],
      count: 0,
      first_pending_at: nil,
      last_enqueue_at: nil,
      timer_ref: nil,
      durable_index: 0,
      requested_index: 0,
      terminal_count_inits: MapSet.new(),
      terminal_atomic_write?: false,
      write_group_sizes: [],
      lmdb_ready: false,
      suspended?: false,
      projection_dirty?: false,
      enqueue_seq: enqueue_seq,
      processed_enqueue_seq: 0,
      processed_enqueue_gaps: MapSet.new(),
      flush_waiters: [],
      flush_interval_ms:
        positive_config(
          :flow_lmdb_flush_interval_ms,
          default_flush_interval_ms(),
          @max_timer_ms
        ),
      flush_jitter_ms:
        non_negative_config(
          :flow_lmdb_flush_jitter_ms,
          default_flush_jitter_ms(),
          @max_timer_ms
        ),
      flush_quiet_ms:
        non_negative_config(
          :flow_lmdb_flush_quiet_ms,
          default_flush_quiet_ms(),
          @max_timer_ms
        ),
      flush_max_lag_ms:
        non_negative_config(
          :flow_lmdb_flush_max_lag_ms,
          default_flush_max_lag_ms(),
          @max_timer_ms
        ),
      max_ops: positive_config(:flow_lmdb_max_batch_ops, default_max_ops(), @max_batch_ops),
      flush_on_max_ops?:
        boolean_config(
          :ferricstore,
          :flow_lmdb_flush_on_max_ops,
          default_flush_on_max_ops(mode)
        ),
      flush_chunk_ops:
        positive_config(
          :flow_lmdb_flush_chunk_ops,
          default_flush_chunk_ops(),
          @max_flush_chunk_ops
        ),
      flush_chunk_pause_ms:
        non_negative_config(
          :flow_lmdb_flush_chunk_pause_ms,
          default_flush_chunk_pause_ms(),
          @max_flush_chunk_pause_ms
        )
    }
  end

  defp positive_config(key, default, max_value) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> min(value, max_value)
      _invalid -> default
    end
  end

  defp non_negative_config(key, default, max_value) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value >= 0 -> min(value, max_value)
      _invalid -> default
    end
  end

  defp boolean_config(app, key, default) do
    case Application.get_env(app, key, default) do
      value when is_boolean(value) -> value
      _invalid -> default
    end
  end
end
