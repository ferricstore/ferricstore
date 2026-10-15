defmodule Ferricstore.Test.FlowReadDiagnostics do
  @moduledoc """
  Describes every layer a Flow state read passes through, for assertion
  messages when a record that should exist reads as missing.

  ScheduleTest has twice seen a just-completed schedule read as nil on CI, and
  neither occurrence reproduces locally. This report says which layer lost it:
  the hot keydir row, the LMDB query row and reverse rows, the writer and mirror
  health, or the clock that decides terminal retention.
  """

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.Store.Router

  @schedule_prefix "__ferricstore_schedule__:"
  @schedule_partition_buckets 256

  @spec schedule_report(binary()) :: map()
  def schedule_report(schedule_id) when is_binary(schedule_id) do
    partition_key =
      @schedule_prefix <>
        Integer.to_string(:erlang.phash2(schedule_id, @schedule_partition_buckets))

    state_report(@schedule_prefix <> schedule_id, partition_key)
  end

  @spec state_report(binary(), binary() | nil) :: map()
  def state_report(flow_id, partition_key) when is_binary(flow_id) do
    ctx = FerricStore.Instance.get(:default)
    state_key = Ferricstore.Flow.Keys.state_key(flow_id, partition_key)
    shard_index = Router.shard_for(ctx, state_key)

    path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(shard_index) |> LMDB.path()

    %{
      state_key: state_key,
      shard_index: shard_index,
      router_flow_get:
        safe(fn -> summarize_value(Router.flow_get_with_status(ctx, flow_id, partition_key)) end),
      keydir_row: safe(fn -> keydir_row(ctx, shard_index, state_key) end),
      lmdb_query_row: safe(fn -> lmdb_query_row(path, state_key) end),
      lmdb_terminal_reverse:
        safe(fn -> LMDB.get(path, LMDB.terminal_by_state_key_key(state_key)) end),
      lmdb_active_reverse:
        safe(fn -> LMDB.get(path, LMDB.active_by_state_key_key(state_key)) end),
      mirror_degraded: safe(fn -> shard_atomic(ctx, :flow_lmdb_mirror_degraded, shard_index) end),
      writer_flush_failures:
        safe(fn -> shard_atomic(ctx, :flow_lmdb_writer_flush_failures, shard_index) end),
      writer: safe(fn -> writer_state(ctx, shard_index) end),
      command_now_ms: Ferricstore.CommandTime.now_ms(),
      wall_now_ms: System.os_time(:millisecond),
      hlc_drift_ms: safe(fn -> Ferricstore.HLC.drift_ms() end)
    }
  end

  defp keydir_row(ctx, shard_index, state_key) do
    case :ets.lookup(elem(ctx.keydir_refs, shard_index), state_key) do
      [{_key, value, expire_at_ms, _lfu, file_id, offset, value_size}] ->
        %{
          value: summarize_value(value),
          expire_at_ms: expire_at_ms,
          file_id: file_id,
          offset: offset,
          value_size: value_size
        }

      [] ->
        :missing
    end
  end

  defp lmdb_query_row(path, state_key) do
    case LMDB.get(path, state_key) do
      {:ok, encoded} ->
        case QueryRowCodec.decode(encoded, state_key) do
          {:ok, %{record: record} = row} ->
            %{
              record: summarize_record(record),
              expire_at_ms: Map.get(row, :expire_at_ms),
              locator: Map.get(row, :locator)
            }

          other ->
            {:undecodable, other}
        end

      other ->
        other
    end
  end

  defp writer_state(ctx, shard_index) do
    name = Ferricstore.Flow.LMDBWriter.Registry.name(ctx.name, shard_index)

    case Process.whereis(name) do
      nil ->
        :not_running

      pid ->
        state = :sys.get_state(pid, 1_000)

        Map.take(state, [
          :count,
          :requested_index,
          :durable_index,
          :projection_dirty?,
          :suspended?,
          :lmdb_ready
        ])
    end
  end

  defp shard_atomic(ctx, field, shard_index) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) -> :atomics.get(ref, shard_index + 1)
      other -> {:unavailable, other}
    end
  end

  defp summarize_value(value) when is_binary(value) do
    summarize_record(Ferricstore.Flow.decode_record(value))
  rescue
    _error -> {:undecodable_binary, byte_size(value)}
  end

  defp summarize_value(value), do: value

  defp summarize_record(record) when is_map(record) do
    Map.take(record, [
      :id,
      :state,
      :version,
      :incarnation,
      :state_enter_seq,
      :updated_at_ms,
      :retention_ttl_ms,
      :terminal_retention_until_ms,
      :partition_key
    ])
  end

  defp summarize_record(other), do: other

  defp safe(fun) do
    fun.()
  rescue
    error -> {:raised, Exception.message(error)}
  catch
    kind, reason -> {kind, reason}
  end
end
