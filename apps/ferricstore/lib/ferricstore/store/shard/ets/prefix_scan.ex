defmodule Ferricstore.Store.Shard.ETS.PrefixScan do
  @moduledoc false

  alias Ferricstore.ExpiryContext
  alias Ferricstore.Store.{BlobValue, ColdRead, ReadResult}
  alias Ferricstore.Store.Shard.CompoundMemberIndex

  @cold_batch_read_timeout_ms 10_000
  @bounded_select_chunk_size 128

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

  @spec prefix_scan_entries(map() | :ets.tid(), binary(), binary() | nil) ::
          [{binary(), binary()}] | ReadResult.failure()
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

  @doc false
  @spec prefix_scan_entries_slice(
          map(),
          binary(),
          binary() | nil,
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: [{binary(), binary()}] | ReadResult.failure()
  def prefix_scan_entries_slice(
        %{keydir: keydir} = state,
        prefix,
        shard_data_path,
        start,
        count,
        total
      )
      when is_binary(prefix) and is_integer(start) and start >= 0 and is_integer(count) and
             count >= 0 and is_integer(total) and total >= 0 do
    index = compound_member_index_ref(state)

    rows =
      if CompoundMemberIndex.supports_prefix?(prefix) and CompoundMemberIndex.ready?(index) do
        CompoundMemberIndex.row_slice(index, state, prefix, start, count, total)
      else
        :unavailable
      end

    case rows do
      {:ok, selected_rows} ->
        selected_rows
        |> then(&do_prefix_scan_rows(state, keydir, prefix, shard_data_path, &1))
        |> ReadResult.map_success(&Enum.sort_by(&1, fn {field, _value} -> field end))

      {:error, reason} ->
        ReadResult.failure({:compound_catalog_slice_failed, reason})

      :unavailable ->
        state
        |> do_prefix_scan_entries(keydir, prefix, shard_data_path)
        |> ReadResult.map_success(fn entries ->
          entries
          |> Enum.sort_by(fn {field, _value} -> field end)
          |> Enum.slice(start, count)
        end)
    end
  rescue
    ArgumentError -> ReadResult.failure(:compound_slice_unavailable)
  end

  def prefix_scan_entries_slice(
        %{ets: keydir} = state,
        prefix,
        shard_data_path,
        start,
        count,
        total
      ) do
    state
    |> Map.put(:keydir, keydir)
    |> prefix_scan_entries_slice(prefix, shard_data_path, start, count, total)
  end

  @doc false
  @spec prefix_scan_entries_bounded(map(), binary(), binary() | nil, map()) ::
          [{binary(), binary()}]
          | {:error, :collection_response_limit | :response_byte_limit}
          | ReadResult.failure()
  def prefix_scan_entries_bounded(%{keydir: keydir} = state, prefix, shard_data_path, limits)
      when is_binary(prefix) and is_map(limits) do
    case bounded_prefix_rows(state, keydir, prefix, limits) do
      {:ok, rows} ->
        bounded_prefix_rows_result(state, keydir, prefix, shard_data_path, rows, limits)

      {:error, reason} = error
      when reason in [:collection_response_limit, :response_byte_limit] ->
        error

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      {:catalog_error, reason} ->
        ReadResult.failure({:compound_catalog_scan_failed, reason})
    end
  rescue
    ArgumentError -> ReadResult.failure(:compound_bounded_scan_unavailable)
  end

  def prefix_scan_entries_bounded(%{ets: keydir} = state, prefix, shard_data_path, limits),
    do:
      state
      |> Map.put(:keydir, keydir)
      |> prefix_scan_entries_bounded(prefix, shard_data_path, limits)

  defp maybe_compound_index_scan_entries(state, keydir, prefix, shard_data_path) do
    index = compound_member_index_ref(state)

    if CompoundMemberIndex.supports_prefix?(prefix) and CompoundMemberIndex.ready?(index) do
      case CompoundMemberIndex.scan_rows(index, state, prefix) do
        {:ok, rows} -> do_prefix_scan_rows(state, keydir, prefix, shard_data_path, rows)
        {:error, reason} -> ReadResult.failure({:compound_catalog_scan_failed, reason})
        :unavailable -> do_prefix_scan_entries(state, keydir, prefix, shard_data_path)
      end
    else
      do_prefix_scan_entries(state, keydir, prefix, shard_data_path)
    end
  end

  defp compound_member_index_ref(state),
    do: Map.get(state, :compound_member_index) || Map.get(state, :compound_member_index_name)

  def do_prefix_scan_entries(state, keydir, prefix, shard_data_path) do
    do_select_prefix_scan_entries(state, keydir, prefix, shard_data_path)
  end

  defp do_select_prefix_scan_entries(state, keydir, prefix, shard_data_path) do
    rows = select_prefix_rows(state, keydir, prefix)
    do_prefix_scan_rows(state, keydir, prefix, shard_data_path, rows)
  end

  defp do_prefix_scan_rows(state, keydir, _prefix, shard_data_path, rows) do
    expiry_context = ExpiryContext.capture()

    {tokens, {cold_entries, _cold_count}} =
      Enum.reduce(rows, {[], {[], 0}}, fn
        {key, value, exp, _lfu, fid, off, vsize} = observed,
        {tokens, {cold_entries, cold_count}} ->
          case prefix_expiry_status(expiry_context, exp) do
            :expired ->
              maybe_delete_expired_prefix_entry(state, keydir, observed)
              {tokens, {cold_entries, cold_count}}

            {:error, reason} ->
              failure = ReadResult.failure(reason)
              {[{:failure, failure} | tokens], {cold_entries, cold_count}}

            :live ->
              cond do
                value == nil and
                    not (valid_cold_location(fid, off, vsize) or
                           flow_history_cold_location?(fid, off, vsize) or
                             valid_waraft_segment_location(fid, off, vsize)) ->
                  failure = ReadResult.failure({:invalid_prefix_scan_location, key})
                  {[{:failure, failure} | tokens], {cold_entries, cold_count}}

                value == nil and valid_waraft_segment_location(fid, off, vsize) and
                    state != nil ->
                  case read_waraft_segment_value(state, fid, key) do
                    {:ok, segment_value} ->
                      {[{:value, {prefix_field(key), segment_value}} | tokens],
                       {cold_entries, cold_count}}

                    {:error, reason} ->
                      failure = ReadResult.failure({:waraft_prefix_value_unavailable, reason})
                      {[{:failure, failure} | tokens], {cold_entries, cold_count}}
                  end

                value == nil and shard_data_path != nil ->
                  field = prefix_field(key)
                  file_path = file_path(shard_data_path, fid)
                  entry = {field, key, file_path, off}
                  {[{:cold, cold_count} | tokens], {[entry | cold_entries], cold_count + 1}}

                value != nil ->
                  {[{:value, {prefix_field(key), value}} | tokens], {cold_entries, cold_count}}

                true ->
                  failure = ReadResult.failure(:prefix_scan_data_path_unavailable)
                  {[{:failure, failure} | tokens], {cold_entries, cold_count}}
              end
          end
      end)

    prefix_scan_tokens_to_results(tokens, cold_entries, state)
  end

  defp bounded_prefix_rows(state, keydir, prefix, limits) do
    reducer = bounded_prefix_row_reducer(state, keydir, prefix, limits)
    index = compound_member_index_ref(state)

    result =
      if CompoundMemberIndex.supports_prefix?(prefix) and CompoundMemberIndex.ready?(index) do
        CompoundMemberIndex.reduce_rows_while(index, state, prefix, {[], 0, 0}, reducer)
      else
        :unavailable
      end

    result =
      case result do
        :unavailable -> reduce_selected_prefix_rows(keydir, prefix, {[], 0, 0}, reducer)
        indexed_result -> indexed_result
      end

    normalize_bounded_prefix_rows(result)
  end

  defp bounded_prefix_row_reducer(state, keydir, prefix, limits) do
    expiry_context = ExpiryContext.capture()
    prefix_len = byte_size(prefix)
    max_entries = Map.get(limits, :max_entries, :unlimited)
    max_bytes = Map.get(limits, :max_bytes, :unlimited)
    entry_overhead = Map.get(limits, :entry_overhead, 0)
    include_values = Map.get(limits, :include_values, true)

    fn
      {key, value, exp, _lfu, fid, off, vsize} = observed, {rows, count, bytes} ->
        case prefix_expiry_status(expiry_context, exp) do
          :expired ->
            maybe_delete_expired_prefix_entry(state, keydir, observed)
            {:cont, {rows, count, bytes}}

          {:error, reason} ->
            {:halt, ReadResult.failure(reason)}

          :live ->
            if value == nil and not valid_prefix_location?(fid, off, vsize) do
              {:halt, ReadResult.failure({:invalid_prefix_scan_location, key})}
            else
              value_bytes = if include_values, do: prefix_value_size(value, vsize), else: 0

              if is_integer(value_bytes) do
                next_count = count + 1
                next_bytes = bytes + byte_size(key) - prefix_len + entry_overhead + value_bytes

                cond do
                  limit_exceeded?(next_count, max_entries) ->
                    {:halt, {:error, :collection_response_limit}}

                  limit_exceeded?(next_bytes, max_bytes) ->
                    {:halt, {:error, :response_byte_limit}}

                  true ->
                    {:cont, {[observed | rows], next_count, next_bytes}}
                end
              else
                {:halt, ReadResult.failure({:invalid_prefix_value_size, key})}
              end
            end
        end

      invalid, _acc ->
        {:halt, ReadResult.failure({:invalid_prefix_scan_row, invalid})}
    end
  end

  defp normalize_bounded_prefix_rows({:ok, {rows, _count, _bytes}}),
    do: {:ok, Enum.reverse(rows)}

  defp normalize_bounded_prefix_rows({:halt, result}), do: result
  defp normalize_bounded_prefix_rows({:error, reason}), do: {:catalog_error, reason}

  defp reduce_selected_prefix_rows(keydir, prefix, acc, reducer) do
    case :ets.select(keydir, prefix_rows_match_spec(prefix), @bounded_select_chunk_size) do
      :"$end_of_table" -> {:ok, acc}
      {rows, continuation} -> continue_selected_prefix_rows(rows, continuation, acc, reducer)
    end
  end

  defp continue_selected_prefix_rows(rows, continuation, acc, reducer) do
    case reduce_selected_rows(rows, acc, reducer) do
      {:ok, next_acc} ->
        case :ets.select(continuation) do
          :"$end_of_table" ->
            {:ok, next_acc}

          {next_rows, next_continuation} ->
            continue_selected_prefix_rows(next_rows, next_continuation, next_acc, reducer)
        end

      {:halt, _result} = halted ->
        halted
    end
  end

  defp reduce_selected_rows([], acc, _reducer), do: {:ok, acc}

  defp reduce_selected_rows([row | rows], acc, reducer) do
    case reducer.(row, acc) do
      {:cont, next_acc} -> reduce_selected_rows(rows, next_acc, reducer)
      {:halt, result} -> {:halt, result}
    end
  end

  defp bounded_prefix_rows_result(state, keydir, prefix, shard_data_path, rows, limits) do
    if Map.get(limits, :fields_only, false) do
      prefix_fields_from_rows(rows, prefix)
    else
      do_prefix_scan_rows(state, keydir, prefix, shard_data_path, rows)
    end
  end

  defp prefix_fields_from_rows(rows, prefix) do
    prefix_len = byte_size(prefix)

    Enum.reduce(rows, [], fn {key, _value, _exp, _lfu, _fid, _off, _vsize}, acc ->
      [binary_part(key, prefix_len, byte_size(key) - prefix_len) | acc]
    end)
  end

  defp prefix_expiry_status(expiry_context, expire_at_ms)
       when is_integer(expire_at_ms) and expire_at_ms >= 0 do
    case ExpiryContext.classify(expiry_context, expire_at_ms) do
      :live -> :live
      :expired -> :expired
      {:unsafe, reason} -> {:error, reason}
    end
  end

  defp prefix_expiry_status(_expiry_context, expire_at_ms),
    do: {:error, {:invalid_expire_at_ms, expire_at_ms}}

  defp select_prefix_rows(_state, keydir, prefix) do
    :ets.select(keydir, prefix_rows_match_spec(prefix))
  end

  defp prefix_rows_match_spec(prefix) do
    prefix_len = byte_size(prefix)

    # Select the exact row so cleanup cannot remove a concurrently replaced generation.
    [
      {{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$_"]}
    ]
  end

  defp valid_prefix_location?(fid, off, vsize)
       when valid_cold_location(fid, off, vsize) or valid_waraft_segment_location(fid, off, vsize),
       do: true

  defp valid_prefix_location?(fid, off, vsize),
    do: flow_history_cold_location?(fid, off, vsize)

  defp prefix_value_size(value, _stored_size) when is_binary(value), do: byte_size(value)

  defp prefix_value_size(nil, stored_size) when is_integer(stored_size) and stored_size >= 0,
    do: stored_size

  defp prefix_value_size(value, _stored_size) when is_integer(value),
    do: value |> Integer.to_string() |> byte_size()

  defp prefix_value_size(value, _stored_size) when is_float(value),
    do: value |> Float.to_string() |> byte_size()

  defp prefix_value_size(_value, _stored_size), do: :invalid

  defp limit_exceeded?(_value, :unlimited), do: false
  defp limit_exceeded?(value, limit) when is_integer(limit) and limit >= 0, do: value > limit
  defp limit_exceeded?(_value, _invalid_limit), do: true

  defp prefix_scan_tokens_to_results(tokens, cold_entries, state) do
    cold_values =
      cold_entries
      |> Enum.reverse()
      |> prefix_read_cold_batch_async(state)
      |> List.to_tuple()

    results =
      Enum.map(tokens, fn
        {:value, result} -> result
        {:failure, failure} -> failure
        {:cold, index} -> elem(cold_values, index)
      end)

    case ReadResult.first_failure(results) do
      nil -> results
      failure -> failure
    end
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
    expiry_cutoff_ms =
      ExpiryContext.capture()
      |> ExpiryContext.safe_expiry_cutoff_ms()

    prefix_len = byte_size(prefix)

    prefix_guard =
      {:andalso, {:is_binary, :"$1"},
       {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
        {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}

    live_exp_guard =
      {:andalso, {:is_integer, :"$3"}, {:orelse, {:==, :"$3", 0}, {:>, :"$3", expiry_cutoff_ms}}}

    ms = [
      {{:"$1", :_, :"$3", :_, :_, :_, :_}, [{:andalso, prefix_guard, live_exp_guard}], [:"$1"]}
    ]

    fields = collect_prefix_fields(keydir, ms)

    if state != nil do
      delete_expired_prefix_entries(state, keydir, prefix, prefix_len, expiry_cutoff_ms)
    end

    fields
  end

  defp collect_prefix_fields(keydir, ms) do
    case :ets.select(keydir, ms, @bounded_select_chunk_size) do
      :"$end_of_table" -> []
      {keys, continuation} -> continue_collect_prefix_fields(keys, continuation, [])
    end
  end

  defp continue_collect_prefix_fields(keys, continuation, acc) do
    acc = Enum.reduce(keys, acc, fn key, fields -> [prefix_field(key) | fields] end)

    case :ets.select(continuation) do
      :"$end_of_table" ->
        Enum.reverse(acc)

      {next_keys, next_continuation} ->
        continue_collect_prefix_fields(next_keys, next_continuation, acc)
    end
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

      {_entry, {:error, {:storage_read_failed, _reason}} = failure} ->
        failure
    end)
  end

  def prefix_materialize_blob_values(%{data_dir: data_dir, index: shard_index} = state, values) do
    {binary_values, indexed_results} =
      values
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {value, index}, {binary_values, indexed_results} when is_binary(value) ->
          {[{index, value} | binary_values], indexed_results}

        {{:error, reason}, index}, {binary_values, indexed_results} ->
          failure = ReadResult.failure({:cold_prefix_value_unavailable, reason})
          {binary_values, Map.put(indexed_results, index, failure)}

        {value, index}, {binary_values, indexed_results} ->
          failure = ReadResult.failure({:invalid_cold_prefix_value, value})
          {binary_values, Map.put(indexed_results, index, failure)}
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
          Map.put(acc, index, normalize_materialized_prefix_value(result))
        end)
      end

    values
    |> Enum.with_index()
    |> Enum.map(fn {_value, index} -> Map.fetch!(indexed_results, index) end)
  end

  def prefix_materialize_blob_values(_state, values) do
    Enum.map(values, fn
      value when is_binary(value) -> {:ok, value}
      {:error, reason} -> ReadResult.failure({:cold_prefix_value_unavailable, reason})
      value -> ReadResult.failure({:invalid_cold_prefix_value, value})
    end)
  end

  defp normalize_materialized_prefix_value({:ok, _value} = result), do: result

  defp normalize_materialized_prefix_value({:error, reason}),
    do: ReadResult.failure({:blob_prefix_value_unavailable, reason})

  defp normalize_materialized_prefix_value(result),
    do: ReadResult.failure({:invalid_blob_prefix_result, result})

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

  @spec prefix_count_entries(map() | :ets.tid(), binary()) ::
          non_neg_integer() | ReadResult.failure()
  @doc false
  def prefix_count_entries(%{keydir: keydir} = state, prefix),
    do: do_prefix_count_entries(state, keydir, prefix)

  def prefix_count_entries(keydir, prefix),
    do: do_prefix_count_entries(nil, keydir, prefix)

  def do_prefix_count_entries(state, keydir, prefix) do
    case indexed_prefix_count(state, prefix) do
      {:ok, count} -> count
      {:error, reason} -> ReadResult.failure({:compound_count_failed, reason})
      :unavailable -> select_prefix_count_entries(state, keydir, prefix)
    end
  end

  defp indexed_prefix_count(nil, _prefix), do: :unavailable

  defp indexed_prefix_count(state, prefix) do
    index = compound_member_index_ref(state)

    if CompoundMemberIndex.supports_prefix?(prefix) and CompoundMemberIndex.ready?(index) do
      case CompoundMemberIndex.count_live_indexed(
             index,
             state,
             prefix,
             compound_count_cleanup_budget(state)
           ) do
        {:ok, count, _inspected} -> {:ok, count}
        {:error, _reason} = error -> error
        :unavailable -> :unavailable
      end
    else
      :unavailable
    end
  end

  defp compound_count_cleanup_budget(%{apply_context: apply_context})
       when is_map(apply_context) do
    case Map.get(apply_context, :compound_member_apply_budget) do
      budget when is_integer(budget) and budget > 0 -> budget
      _missing_or_invalid -> 4_096
    end
  end

  defp compound_count_cleanup_budget(_state), do: 4_096

  defp select_prefix_count_entries(state, keydir, prefix) do
    expiry_cutoff_ms =
      ExpiryContext.capture()
      |> ExpiryContext.safe_expiry_cutoff_ms()

    prefix_len = byte_size(prefix)

    prefix_guard =
      {:andalso, {:is_binary, :"$1"},
       {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
        {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}

    live_exp_guard =
      {:andalso, {:is_integer, :"$3"}, {:orelse, {:==, :"$3", 0}, {:>, :"$3", expiry_cutoff_ms}}}

    live_ms = [
      {{:"$1", :_, :"$3", :_, :_, :_, :_}, [{:andalso, prefix_guard, live_exp_guard}], [true]}
    ]

    count = :ets.select_count(keydir, live_ms)

    if state != nil do
      delete_expired_prefix_entries(state, keydir, prefix, prefix_len, expiry_cutoff_ms)
    end

    count
  end

  def delete_expired_prefix_entries(state, keydir, prefix, prefix_len, now) do
    expired_ms = [
      {{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:andalso, {:is_integer, :"$3"}, {:andalso, {:>, :"$3", 0}, {:"=<", :"$3", now}}}}}}
       ], [:"$_"]}
    ]

    case :ets.select(keydir, expired_ms, @bounded_select_chunk_size) do
      :"$end_of_table" ->
        :ok

      {rows, continuation} ->
        continue_delete_expired_prefix_entries(state, keydir, rows, continuation)
    end
  end

  defp continue_delete_expired_prefix_entries(state, keydir, rows, continuation) do
    Enum.each(rows, &delete_prefix_entry(state, keydir, &1))

    case :ets.select(continuation) do
      :"$end_of_table" ->
        :ok

      {next_rows, next_continuation} ->
        continue_delete_expired_prefix_entries(state, keydir, next_rows, next_continuation)
    end
  end

  def maybe_delete_expired_prefix_entry(nil, _keydir, _entry), do: :ok

  def maybe_delete_expired_prefix_entry(state, keydir, entry),
    do: delete_prefix_entry(state, keydir, entry)

  def delete_prefix_entry(%{keydir: keydir} = state, keydir, entry),
    do: Ferricstore.Store.Shard.ETS.delete_exact_entry(state, entry)

  def delete_prefix_entry(state, keydir, entry) do
    state
    |> Map.put(:keydir, keydir)
    |> Ferricstore.Store.Shard.ETS.delete_exact_entry(entry)
  end

  defp file_path(shard_path, {:flow_history, file_id}) do
    Ferricstore.Flow.HistoryProjector.history_file_path(shard_path, file_id)
  end

  defp file_path(shard_path, file_id) do
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  @doc false
  def prefix_collect_keys(keydir, prefix) do
    collect_prefix_keys(keydir, prefix_key_match_spec(prefix))
  end

  @doc false
  @spec prefix_each_key(:ets.tid() | atom(), binary(), (binary() -> term())) :: :ok
  def prefix_each_key(keydir, prefix, fun)
      when is_binary(prefix) and is_function(fun, 1) do
    :ets.safe_fixtable(keydir, true)

    try do
      each_prefix_key_page(
        :ets.select(keydir, prefix_key_match_spec(prefix), @bounded_select_chunk_size),
        fun
      )
    after
      unfix_prefix_table(keydir)
    end
  end

  defp each_prefix_key_page(:"$end_of_table", _fun), do: :ok

  defp each_prefix_key_page({keys, continuation}, fun) do
    Enum.each(keys, fun)
    each_prefix_key_page(:ets.select(continuation), fun)
  end

  defp unfix_prefix_table(keydir) do
    :ets.safe_fixtable(keydir, false)
  rescue
    ArgumentError -> :ok
  end

  defp collect_prefix_keys(keydir, match_spec) do
    case :ets.select(keydir, match_spec, @bounded_select_chunk_size) do
      :"$end_of_table" -> []
      {keys, continuation} -> continue_collect_prefix_keys(keys, continuation, [])
    end
  end

  defp continue_collect_prefix_keys(keys, continuation, acc) do
    acc = Enum.reduce(keys, acc, fn key, collected -> [key | collected] end)

    case :ets.select(continuation) do
      :"$end_of_table" ->
        Enum.reverse(acc)

      {next_keys, next_continuation} ->
        continue_collect_prefix_keys(next_keys, next_continuation, acc)
    end
  end

  @doc false
  def prefix_collect_keys(_keydir, _prefix, 0), do: []

  def prefix_collect_keys(keydir, prefix, limit)
      when is_integer(limit) and limit > 0 do
    case :ets.select(keydir, prefix_key_match_spec(prefix), limit) do
      :"$end_of_table" -> []
      {keys, _continuation} -> keys
    end
  end

  defp prefix_key_match_spec(prefix) do
    prefix_len = byte_size(prefix)

    [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]
  end
end
