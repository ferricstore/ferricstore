defmodule Ferricstore.Store.Shard.ETS.PrefixScan do
  @moduledoc false

  alias Ferricstore.HLC
  alias Ferricstore.Store.{BlobValue, ColdRead}
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS.Accounting

  @cold_batch_read_timeout_ms 10_000

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  defguardp valid_waraft_segment_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                   is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  @spec prefix_scan_entries(map() | :ets.tid(), binary(), binary() | nil) :: [
          {binary(), binary()}
        ]
  @doc false
  def prefix_scan_entries(%{keydir: keydir} = state, prefix, shard_data_path),
    do: maybe_compound_index_scan_entries(state, keydir, prefix, shard_data_path)

  def prefix_scan_entries(%{ets: keydir} = state, prefix, shard_data_path),
    do:
      state
      |> Map.put(:keydir, keydir)
      |> maybe_compound_index_scan_entries(keydir, prefix, shard_data_path)

  def prefix_scan_entries(keydir, prefix, shard_data_path),
    do: do_prefix_scan_entries(nil, keydir, prefix, shard_data_path)

  defp maybe_compound_index_scan_entries(state, keydir, prefix, shard_data_path) do
    case CompoundMemberIndex.scan_entries(compound_member_index_ref(state), state, prefix) do
      {:ok, entries} -> entries
      :unavailable -> do_prefix_scan_entries(state, keydir, prefix, shard_data_path)
    end
  end

  defp compound_member_index_ref(state),
    do: Map.get(state, :compound_member_index) || Map.get(state, :compound_member_index_name)

  def do_prefix_scan_entries(state, keydir, prefix, shard_data_path) do
    do_select_prefix_scan_entries(state, keydir, prefix, shard_data_path)
  end

  defp do_select_prefix_scan_entries(state, keydir, prefix, shard_data_path) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)
    # Select all 7-tuple fields so we can cold-read nil values
    ms = [
      {{:"$1", :"$2", :"$3", :_, :"$4", :"$5", :"$6"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6"}}]}
    ]

    {tokens, {cold_entries, _cold_count}} =
      :ets.select(keydir, ms)
      |> Enum.reduce({[], {[], 0}}, fn {key, value, exp, fid, off, vsize},
                                       {tokens, {cold_entries, cold_count}} ->
        cond do
          exp != 0 and exp <= now ->
            maybe_delete_expired_prefix_entry(state, keydir, key)
            {tokens, {cold_entries, cold_count}}

          value == nil and
              not (valid_cold_location(fid, off, vsize) or
                     flow_history_cold_location?(fid, off, vsize) or
                       valid_waraft_segment_location(fid, off, vsize)) ->
            maybe_delete_expired_prefix_entry(state, keydir, key)
            {tokens, {cold_entries, cold_count}}

          value == nil and valid_waraft_segment_location(fid, off, vsize) and state != nil ->
            case read_waraft_segment_value(state, fid, key) do
              {:ok, segment_value} ->
                {[{:value, {prefix_field(key), segment_value}} | tokens],
                 {cold_entries, cold_count}}

              _ ->
                {tokens, {cold_entries, cold_count}}
            end

          value == nil and shard_data_path != nil ->
            field = prefix_field(key)
            file_path = file_path(shard_data_path, fid)
            entry = {field, key, file_path, off}
            {[{:cold, cold_count} | tokens], {[entry | cold_entries], cold_count + 1}}

          value != nil ->
            {[{:value, {prefix_field(key), value}} | tokens], {cold_entries, cold_count}}

          true ->
            {tokens, {cold_entries, cold_count}}
        end
      end)

    prefix_scan_tokens_to_results(tokens, cold_entries, state)
  end

  defp prefix_scan_tokens_to_results(tokens, cold_entries, state) do
    cold_values =
      cold_entries
      |> Enum.reverse()
      |> prefix_read_cold_batch_async(state)
      |> List.to_tuple()

    Enum.flat_map(tokens, fn
      {:value, result} ->
        [result]

      {:cold, index} ->
        case elem(cold_values, index) do
          nil -> []
          result -> [result]
        end
    end)
  end

  def prefix_field(key) do
    case :binary.split(key, <<0>>) do
      [_pre, sub] -> sub
      _ -> key
    end
  end

  @spec prefix_scan_fields(map() | :ets.tid(), binary()) :: [binary()]
  @doc false
  def prefix_scan_fields(%{keydir: keydir} = state, prefix),
    do: do_prefix_scan_fields(state, keydir, prefix)

  def prefix_scan_fields(keydir, prefix),
    do: do_prefix_scan_fields(nil, keydir, prefix)

  def do_prefix_scan_fields(state, keydir, prefix) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)

    prefix_guard =
      {:andalso, {:is_binary, :"$1"},
       {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
        {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}

    live_exp_guard = {:orelse, {:==, :"$3", 0}, {:>, :"$3", now}}

    valid_bitcask_cold_guard =
      {:andalso, {:is_integer, :"$4"},
       {:andalso, {:>=, :"$4", 0},
        {:andalso, {:is_integer, :"$5"},
         {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}

    valid_waraft_segment_guard =
      {:andalso, {:is_tuple, :"$4"},
       {:andalso, {:==, {:tuple_size, :"$4"}, 2},
        {:andalso,
         {:orelse, {:==, {:element, 1, :"$4"}, :waraft_segment},
          {:orelse, {:==, {:element, 1, :"$4"}, :waraft_projection},
           {:==, {:element, 1, :"$4"}, :waraft_apply_projection}}},
         {:andalso, {:is_integer, {:element, 2, :"$4"}},
          {:andalso, {:>, {:element, 2, :"$4"}, 0},
           {:andalso, {:is_integer, :"$5"},
            {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}}}}

    valid_cold_guard = {:orelse, valid_bitcask_cold_guard, valid_waraft_segment_guard}

    visible_value_guard =
      {:orelse, {:"/=", :"$2", nil}, {:orelse, valid_cold_guard, {:==, :"$4", :pending}}}

    ms = [
      {{:"$1", :"$2", :"$3", :_, :"$4", :"$5", :"$6"},
       [{:andalso, prefix_guard, {:andalso, live_exp_guard, visible_value_guard}}], [:"$1"]}
    ]

    keys = :ets.select(keydir, ms)

    if state != nil do
      delete_expired_prefix_entries(state, keydir, prefix, prefix_len, now)
      delete_invalid_cold_prefix_entries(state, keydir, prefix, prefix_len, now)
    end

    Enum.map(keys, &prefix_field/1)
  end

  def flow_history_cold_location?({:flow_history, file_id}, offset, value_size)
      when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
             is_integer(value_size) and value_size >= 0,
      do: true

  def flow_history_cold_location?(_file_id, _offset, _value_size), do: false

  def prefix_read_cold_batch_async([], _state), do: []

  def prefix_read_cold_batch_async(entries, state) do
    locations = Enum.map(entries, fn {_field, key, file_path, off} -> {file_path, off, key} end)

    values =
      case ColdRead.pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
        {:ok, values} when is_list(values) and length(values) == length(entries) ->
          values

        {:ok, _bad_values} ->
          List.duplicate({:error, :batch_result_length_mismatch}, length(entries))

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    emit_prefix_cold_read_errors(entries, values)

    Enum.zip(entries, prefix_materialize_blob_values(state, values))
    |> Enum.map(fn
      {{field, _key, _file_path, _off}, {:ok, materialized}} ->
        {field, materialized}

      {_entry, _value} ->
        nil
    end)
  end

  def prefix_materialize_blob_values(%{data_dir: data_dir, index: shard_index} = state, values) do
    {binary_values, indexed_results} =
      values
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {value, index}, {binary_values, indexed_results} when is_binary(value) ->
          {[{index, value} | binary_values], indexed_results}

        {_value, index}, {binary_values, indexed_results} ->
          {binary_values, Map.put(indexed_results, index, :skip)}
      end)

    indexed_results =
      if binary_values == [] do
        indexed_results
      else
        ordered_values = Enum.reverse(binary_values)
        threshold = blob_side_channel_threshold(state)
        values = Enum.map(ordered_values, fn {_index, value} -> value end)

        ordered_values
        |> Enum.zip(BlobValue.maybe_materialize_many(data_dir, shard_index, threshold, values))
        |> Enum.reduce(indexed_results, fn {{index, _value}, result}, acc ->
          Map.put(acc, index, result)
        end)
      end

    values
    |> Enum.with_index()
    |> Enum.map(fn {_value, index} -> Map.fetch!(indexed_results, index) end)
  end

  def prefix_materialize_blob_values(_state, values),
    do: Enum.map(values, fn _value -> :skip end)

  def emit_prefix_cold_read_errors(entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {{_field, _key, file_path, _off}, {:error, raw_reason}}, acc ->
        Map.update(acc, {file_path, raw_reason}, 1, &(&1 + 1))

      {_entry, _value}, acc ->
        acc
    end)
    |> Enum.each(fn {{path, raw_reason}, count} ->
      ColdRead.emit_pread_error(path, raw_reason, count)
    end)
  end

  def read_cold_async(state, path, offset, key) do
    with {:ok, value} <-
           Ferricstore.Store.ColdRead.pread_keyed(path, offset, key, @cold_batch_read_timeout_ms),
         {:ok, materialized} <- materialize_blob_value(state, value) do
      {:ok, materialized}
    end
  end

  def read_waraft_segment_value(%{instance_ctx: ctx, index: shard_index} = state, file_id, key) do
    with {:ok, value} <-
           Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
             ctx,
             shard_index,
             file_id,
             key
           ),
         {:ok, materialized} <- materialize_blob_value(state, value) do
      {:ok, materialized}
    end
  end

  def read_waraft_segment_value(_state, _file_id, _key), do: {:error, :missing_instance_ctx}

  def materialize_blob_value(%{data_dir: data_dir, index: shard_index} = state, value) do
    BlobValue.maybe_materialize(data_dir, shard_index, blob_side_channel_threshold(state), value)
  end

  def materialize_blob_value(_state, value), do: {:ok, value}

  def blob_side_channel_threshold(%{instance_ctx: ctx}), do: BlobValue.threshold(ctx)
  def blob_side_channel_threshold(_state), do: 0

  @spec prefix_count_entries(map() | :ets.tid(), binary()) :: non_neg_integer()
  @doc false
  def prefix_count_entries(%{keydir: keydir} = state, prefix),
    do: do_prefix_count_entries(state, keydir, prefix)

  def prefix_count_entries(keydir, prefix),
    do: do_prefix_count_entries(nil, keydir, prefix)

  def do_prefix_count_entries(state, keydir, prefix) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)

    prefix_guard =
      {:andalso, {:is_binary, :"$1"},
       {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
        {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}

    live_exp_guard = {:orelse, {:==, :"$3", 0}, {:>, :"$3", now}}

    valid_bitcask_cold_guard =
      {:andalso, {:is_integer, :"$4"},
       {:andalso, {:>=, :"$4", 0},
        {:andalso, {:is_integer, :"$5"},
         {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}

    valid_waraft_segment_guard =
      {:andalso, {:is_tuple, :"$4"},
       {:andalso, {:==, {:tuple_size, :"$4"}, 2},
        {:andalso,
         {:orelse, {:==, {:element, 1, :"$4"}, :waraft_segment},
          {:orelse, {:==, {:element, 1, :"$4"}, :waraft_projection},
           {:==, {:element, 1, :"$4"}, :waraft_apply_projection}}},
         {:andalso, {:is_integer, {:element, 2, :"$4"}},
          {:andalso, {:>, {:element, 2, :"$4"}, 0},
           {:andalso, {:is_integer, :"$5"},
            {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}}}}

    valid_cold_guard = {:orelse, valid_bitcask_cold_guard, valid_waraft_segment_guard}

    live_hot_ms = [
      {{:"$1", :"$2", :"$3", :_, :_, :_, :_},
       [{:andalso, prefix_guard, {:andalso, live_exp_guard, {:"/=", :"$2", nil}}}], [true]}
    ]

    live_cold_ms = [
      {{:"$1", nil, :"$3", :_, :"$4", :"$5", :"$6"},
       [{:andalso, prefix_guard, {:andalso, live_exp_guard, valid_cold_guard}}], [true]}
    ]

    live_pending_ms = [
      {{:"$1", nil, :"$3", :_, :pending, :_, :_}, [{:andalso, prefix_guard, live_exp_guard}],
       [true]}
    ]

    count =
      :ets.select_count(keydir, live_hot_ms) + :ets.select_count(keydir, live_cold_ms) +
        :ets.select_count(keydir, live_pending_ms)

    if state != nil do
      delete_expired_prefix_entries(state, keydir, prefix, prefix_len, now)
      delete_invalid_cold_prefix_entries(state, keydir, prefix, prefix_len, now)
    end

    count
  end

  def delete_expired_prefix_entries(state, keydir, prefix, prefix_len, now) do
    expired_ms = [
      {{:"$1", :_, :"$3", :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:andalso, {:"/=", :"$3", 0}, {:"=<", :"$3", now}}}}}
       ], [:"$1"]}
    ]

    keydir
    |> :ets.select(expired_ms)
    |> Enum.each(&delete_prefix_entry(state, keydir, &1))
  end

  def delete_invalid_cold_prefix_entries(state, keydir, prefix, prefix_len, now) do
    cold_ms = [
      {{:"$1", nil, :"$2", :_, :"$3", :"$4", :"$5"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:orelse, {:==, :"$2", 0}, {:>, :"$2", now}}}}}
       ], [{{:"$1", :"$3", :"$4", :"$5"}}]}
    ]

    keydir
    |> :ets.select(cold_ms)
    |> Enum.each(fn {key, fid, off, vsize} ->
      unless fid == :pending or valid_cold_location(fid, off, vsize) or
               valid_waraft_segment_location(fid, off, vsize) do
        delete_prefix_entry(state, keydir, key)
      end
    end)
  end

  def maybe_delete_expired_prefix_entry(nil, _keydir, _key), do: :ok

  def maybe_delete_expired_prefix_entry(state, keydir, key),
    do: delete_prefix_entry(state, keydir, key)

  def delete_prefix_entry(state, keydir, key) do
    Accounting.track_binary_delete(state, key)
    :ets.delete(keydir, key)
  end

  defp file_path(shard_path, {:flow_history, file_id}) do
    Ferricstore.Flow.HistoryProjector.history_file_path(shard_path, file_id)
  end

  defp file_path(shard_path, file_id) do
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  @doc false
  def prefix_collect_keys(keydir, prefix) do
    prefix_len = byte_size(prefix)

    ms = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    :ets.select(keydir, ms)
  end
end
