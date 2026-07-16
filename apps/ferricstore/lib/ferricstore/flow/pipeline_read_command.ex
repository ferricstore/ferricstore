defmodule Ferricstore.Flow.PipelineReadCommand do
  @moduledoc false

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.{HistoryAPI, InfoAPI, PayloadReturn, ReadAPI}
  alias Ferricstore.Flow.RecordProjection
  alias Ferricstore.Store.Router

  @corrupt_record_error {:error, "ERR flow record is corrupt"}

  def command(_ctx, {:get, id, opts}) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         :ok <- validate_opts(opts),
         {:ok, payload_return} <- PayloadReturn.options(opts, false),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)) do
      {:get, id, partition_key, payload_return}
    end
  end

  def command(ctx, {:flow_get, id, opts}) do
    command(ctx, {:get, id, opts})
  end

  def command(_ctx, {:history, id, opts}) when is_binary(id) and is_list(opts) do
    with {:ok, {partition_key, history_key, query, include_cold?, consistent?, value_return}} <-
           history_query_attrs(id, opts) do
      {:history, id, partition_key, history_key, query, include_cold?, consistent?, value_return}
    end
  end

  def command(ctx, {:flow_history, id, opts}) do
    command(ctx, {:history, id, opts})
  end

  def command(ctx, {:list, type, opts}) when is_binary(type) and is_list(opts) do
    read_opts = Keyword.delete(opts, :return)

    read_result({:list, type, opts}, fn ->
      ctx
      |> ReadAPI.list(type, read_opts)
      |> RecordProjection.maybe_meta_result(opts)
    end)
  end

  def command(ctx, {:flow_list, type, opts}) do
    command(ctx, {:list, type, opts})
  end

  def command(ctx, {:stats, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result({:stats, type, opts}, fn -> ReadAPI.stats(ctx, type, opts) end)

  def command(ctx, {:flow_stats, type, opts}) do
    command(ctx, {:stats, type, opts})
  end

  def command(ctx, {:attributes, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result({:attributes, type, opts}, fn -> ReadAPI.attributes(ctx, type, opts) end)

  def command(ctx, {:flow_attributes, type, opts}) do
    command(ctx, {:attributes, type, opts})
  end

  def command(ctx, {:attribute_values, type, attr_name, opts})
      when is_binary(type) and is_binary(attr_name) and is_list(opts),
      do:
        read_result({:attribute_values, type, attr_name, opts}, fn ->
          ReadAPI.attribute_values(ctx, type, attr_name, opts)
        end)

  def command(ctx, {:flow_attribute_values, type, attr_name, opts}) do
    command(ctx, {:attribute_values, type, attr_name, opts})
  end

  def command(ctx, {:terminals, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result({:terminals, type, opts}, fn -> ReadAPI.terminals(ctx, type, opts) end)

  def command(ctx, {:flow_terminals, type, opts}) do
    command(ctx, {:terminals, type, opts})
  end

  def command(ctx, {:failures, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result({:failures, type, opts}, fn -> ReadAPI.failures(ctx, type, opts) end)

  def command(ctx, {:flow_failures, type, opts}) do
    command(ctx, {:failures, type, opts})
  end

  def command(ctx, {:by_parent, parent_flow_id, opts})
      when is_binary(parent_flow_id) and is_list(opts),
      do:
        read_result({:by_parent, parent_flow_id, opts}, fn ->
          ReadAPI.by_parent(ctx, parent_flow_id, opts)
        end)

  def command(ctx, {:flow_by_parent, parent_flow_id, opts}) do
    command(ctx, {:by_parent, parent_flow_id, opts})
  end

  def command(ctx, {:by_root, root_flow_id, opts})
      when is_binary(root_flow_id) and is_list(opts),
      do:
        read_result({:by_root, root_flow_id, opts}, fn ->
          ReadAPI.by_root(ctx, root_flow_id, opts)
        end)

  def command(ctx, {:flow_by_root, root_flow_id, opts}) do
    command(ctx, {:by_root, root_flow_id, opts})
  end

  def command(ctx, {:by_correlation, correlation_id, opts})
      when is_binary(correlation_id) and is_list(opts),
      do:
        read_result({:by_correlation, correlation_id, opts}, fn ->
          ReadAPI.by_correlation(ctx, correlation_id, opts)
        end)

  def command(ctx, {:flow_by_correlation, correlation_id, opts}) do
    command(ctx, {:by_correlation, correlation_id, opts})
  end

  def command(ctx, {:info, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result({:info, type, opts}, fn -> InfoAPI.info(ctx, type, opts) end)

  def command(ctx, {:flow_info, type, opts}) do
    command(ctx, {:info, type, opts})
  end

  def command(ctx, {:stuck, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result({:stuck, type, opts}, fn -> ReadAPI.stuck(ctx, type, opts) end)

  def command(ctx, {:flow_stuck, type, opts}) do
    command(ctx, {:stuck, type, opts})
  end

  def command(_ctx, _op), do: {:error, "ERR unsupported flow pipeline read command"}

  def decode_get(nil), do: {:ok, nil}

  def decode_get(value) when is_binary(value) do
    {:ok, value |> Codec.decode_record() |> RecordProjection.public()}
  rescue
    _ -> @corrupt_record_error
  end

  def decode_get({:error, _reason} = error), do: error
  def decode_get(_other), do: @corrupt_record_error

  def history_results(history_ops, ctx) do
    Ferricstore.Flow.PipelineHistoryRead.results(history_ops, ctx, %{
      read: &Ferricstore.Flow.HistoryRead.read/8,
      fetch_count: &Ferricstore.Flow.HistoryRead.query_fetch_count/1,
      hot_range: &Ferricstore.Flow.HistoryRead.hot_range/5,
      hot_range_for_max: &Ferricstore.Flow.HistoryRead.hot_range_for_max/4,
      rank_range_many: &Router.flow_index_rank_range_many/2,
      prepare_consistent: &prepare_consistent_history/2,
      fallback: &Ferricstore.Flow.HistoryRead.hot_fallback_scan/4,
      from_event_ids: &Ferricstore.Flow.HistoryRead.from_event_ids/6,
      from_event_ids_with_context: &Ferricstore.Flow.HistoryRead.from_event_ids_with_context/7,
      decode_contexts: &decode_contexts/2,
      apply_query: &Ferricstore.Flow.HistoryRead.apply_query/2
    })
  end

  defp decode_contexts(_ctx, []), do: %{}

  defp decode_contexts(ctx, entries) do
    entries
    |> Enum.uniq()
    |> Enum.group_by(fn {_id, partition_key} -> partition_key end)
    |> Enum.flat_map(fn {partition_key, group} ->
      ids = Enum.map(group, fn {id, _partition_key} -> id end)
      values = Router.flow_batch_get(ctx, ids, partition_key)

      group
      |> Enum.zip(values)
      |> Enum.map(fn {{id, partition_key}, value} ->
        {{id, partition_key}, decode_context_value(value, id)}
      end)
    end)
    |> Map.new()
  end

  @doc false
  def decode_context_value(value, _id) when is_binary(value) do
    Codec.decode_record(value)
  rescue
    _ -> @corrupt_record_error
  end

  def decode_context_value(nil, id), do: %{id: id}
  def decode_context_value({:error, _reason} = error, _id), do: error
  def decode_context_value(_invalid, _id), do: @corrupt_record_error

  defp prepare_consistent_history(_ctx, []), do: :ok

  defp prepare_consistent_history(ctx, history_ops) do
    history_shards =
      history_ops
      |> Enum.map(fn {_idx, _id, _partition_key, history_key, _query, _include_cold?,
                      _consistent?, _value_return} ->
        Router.shard_for(ctx, history_key)
      end)
      |> Enum.uniq()

    lmdb_shards =
      history_ops
      |> Enum.flat_map(fn
        {_idx, _id, _partition_key, history_key, _query, true, _consistent?, _value_return} ->
          [Router.shard_for(ctx, history_key)]

        _hot_only ->
          []
      end)
      |> Enum.uniq()

    with :ok <- flush_history_shards(ctx, history_shards),
         :ok <- flush_lmdb_shards(ctx, lmdb_shards) do
      :ok
    end
  end

  defp flush_history_shards(ctx, shards) do
    Enum.reduce_while(shards, :ok, fn shard_index, :ok ->
      case Ferricstore.Flow.HistoryProjector.flush(ctx, shard_index, 120_000) do
        :ok ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, "ERR flow history projection unavailable: #{inspect(reason)}"}}
      end
    end)
  end

  defp flush_lmdb_shards(ctx, shards) do
    Enum.reduce_while(shards, :ok, fn shard_index, :ok ->
      case Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_index, 120_000) do
        :ok ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, "ERR flow LMDB projection unavailable: #{inspect(reason)}"}}
      end
    end)
  end

  defp read_result(key, read_fun), do: {:other, key, read_fun}

  defp history_query_attrs(id, opts) do
    HistoryAPI.prepare(id, opts)
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string or :global"}
    end
  end
end
