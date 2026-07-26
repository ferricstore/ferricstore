defmodule Ferricstore.Flow.Query.QueryRowRelocator do
  @moduledoc false

  alias Ferricstore.Flow.{LMDB, Locator}
  alias Ferricstore.Flow.Query.{QueryRow, QueryRowCodec}
  alias Ferricstore.Raft.WARaftSegmentReader

  @maximum_rows 4_096
  @maximum_cas_retries 8

  @type entry :: %{state_key: binary(), encoded: binary(), locator: Locator.t()}

  @doc false
  @spec relocate_many(map(), non_neg_integer(), binary(), [entry()], keyword()) ::
          :ok | {:error, term()}
  def relocate_many(ctx, shard_index, lmdb_path, entries, opts \\ [])

  def relocate_many(ctx, shard_index, lmdb_path, entries, opts)
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(lmdb_path) and lmdb_path != "" and is_list(entries) and is_list(opts) do
    with :ok <- validate_entries(entries),
         {:ok, dependencies} <- dependencies(opts, ctx, shard_index),
         {:ok, updates} <- build_updates(entries, dependencies),
         :ok <- write_updates(lmdb_path, updates, dependencies) do
      :ok
    end
  rescue
    error -> {:error, {:query_row_relocation_failed, error}}
  catch
    kind, reason -> {:error, {:query_row_relocation_failed, {kind, reason}}}
  end

  def relocate_many(_ctx, _shard_index, _lmdb_path, _entries, _opts),
    do: {:error, :invalid_query_row_relocation_request}

  @doc false
  @spec repair_many(
          map(),
          non_neg_integer(),
          binary(),
          [{binary(), Locator.t()}],
          keyword()
        ) :: {:ok, non_neg_integer()} | {:error, term()}
  def repair_many(ctx, shard_index, lmdb_path, requests, opts \\ [])

  def repair_many(ctx, shard_index, lmdb_path, requests, opts)
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(lmdb_path) and lmdb_path != "" and is_list(requests) and is_list(opts) do
    get_many = Keyword.get(opts, :get_many, &LMDB.get_many/2)

    with :ok <- validate_repair_requests(requests),
         true <- is_function(get_many, 2),
         {:ok, entries} <- read_matching_entries(get_many, lmdb_path, requests),
         :ok <- relocate_many(ctx, shard_index, lmdb_path, entries, opts) do
      {:ok, length(entries)}
    else
      false -> {:error, :invalid_query_row_repair_dependencies}
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, {:query_row_repair_failed, error}}
  catch
    kind, reason -> {:error, {:query_row_repair_failed, {kind, reason}}}
  end

  def repair_many(_ctx, _shard_index, _lmdb_path, _requests, _opts),
    do: {:error, :invalid_query_row_repair_request}

  defp validate_repair_requests(requests) when length(requests) <= @maximum_rows do
    requests
    |> Enum.reduce_while(MapSet.new(), fn
      {state_key, %Locator{} = locator}, seen when is_binary(state_key) and state_key != "" ->
        if MapSet.member?(seen, state_key) or not Locator.hydration_ready?(locator) or
             not waraft_locator?(locator) do
          {:halt, :invalid}
        else
          {:cont, MapSet.put(seen, state_key)}
        end

      _invalid, _seen ->
        {:halt, :invalid}
    end)
    |> case do
      %MapSet{} -> :ok
      :invalid -> {:error, :invalid_query_row_repair_requests}
    end
  end

  defp validate_repair_requests(_requests),
    do: {:error, :query_row_repair_row_limit_exceeded}

  defp read_matching_entries(_get_many, _lmdb_path, []), do: {:ok, []}

  defp read_matching_entries(get_many, lmdb_path, requests) do
    keys = Enum.map(requests, &elem(&1, 0))

    case get_many.(lmdb_path, keys) do
      {:ok, values} when is_list(values) and length(values) == length(requests) ->
        collect_matching_entries(requests, values, [])

      {:ok, values} when is_list(values) ->
        {:error, {:query_row_repair_result_count_mismatch, length(requests), length(values)}}

      {:error, reason} ->
        {:error, {:query_row_repair_read_failed, reason}}

      invalid ->
        {:error, {:invalid_query_row_repair_read, invalid}}
    end
  end

  defp collect_matching_entries([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp collect_matching_entries(
         [{_state_key, _expected} | requests],
         [:not_found | values],
         acc
       ),
       do: collect_matching_entries(requests, values, acc)

  defp collect_matching_entries(
         [{state_key, expected} | requests],
         [{:ok, encoded} | values],
         acc
       )
       when is_binary(encoded) do
    case QueryRowCodec.decode(encoded, state_key) do
      {:ok, %QueryRow{locator: current}} ->
        next_acc =
          if Locator.same_physical_record?(current, expected) do
            [%{state_key: state_key, encoded: encoded, locator: current} | acc]
          else
            acc
          end

        collect_matching_entries(requests, values, next_acc)

      :error ->
        {:error, {:corrupt_query_row, state_key}}
    end
  end

  defp collect_matching_entries(_requests, _values, _acc),
    do: {:error, :invalid_query_row_repair_results}

  defp validate_entries(entries) when length(entries) <= @maximum_rows do
    entries
    |> Enum.reduce_while(MapSet.new(), fn
      %{state_key: state_key, encoded: encoded, locator: %Locator{} = locator}, seen
      when is_binary(state_key) and state_key != "" and is_binary(encoded) ->
        cond do
          MapSet.member?(seen, state_key) ->
            {:halt, :invalid}

          not Locator.hydration_ready?(locator) ->
            {:halt, :invalid}

          true ->
            case QueryRowCodec.decode(encoded, state_key) do
              {:ok, %QueryRow{locator: encoded_locator}}
              when encoded_locator == locator ->
                {:cont, MapSet.put(seen, state_key)}

              _invalid ->
                {:halt, :invalid}
            end
        end

      _invalid, _seen ->
        {:halt, :invalid}
    end)
    |> case do
      %MapSet{} -> :ok
      :invalid -> {:error, :invalid_query_row_relocation_entries}
    end
  end

  defp validate_entries(_entries), do: {:error, :query_row_relocation_row_limit_exceeded}

  defp build_updates(entries, dependencies) do
    entries
    |> Enum.group_by(& &1.locator.file_id)
    |> Enum.reduce_while({:ok, []}, fn {file_id, grouped_entries}, {:ok, acc} ->
      case locate(dependencies, file_id) do
        {:ok, location} ->
          case build_group_updates(grouped_entries, location, acc) do
            {:ok, next} -> {:cont, {:ok, next}}
            {:error, _reason} = error -> {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp build_group_updates([], _location, acc), do: {:ok, acc}

  defp build_group_updates(
         [%{state_key: state_key, encoded: encoded, locator: current} | entries],
         {ordinal, offset, frame_size},
         acc
       ) do
    with {:ok, relocated} <- relocate_locator(current, ordinal, offset, frame_size) do
      if Locator.same_physical_record?(current, relocated) do
        build_group_updates(entries, {ordinal, offset, frame_size}, acc)
      else
        case QueryRowCodec.relocate(encoded, state_key, current, relocated) do
          {:ok, replacement} ->
            update = %{state_key: state_key, expected: encoded, replacement: replacement}
            build_group_updates(entries, {ordinal, offset, frame_size}, [update | acc])

          {:error, reason} ->
            {:error, {:query_row_relocation_encode_failed, state_key, reason}}
        end
      end
    else
      {:error, reason} -> {:error, {:invalid_query_row_relocation, state_key, reason}}
    end
  end

  defp relocate_locator(locator, ordinal, offset, frame_size) do
    Locator.relocate(locator,
      segment_generation: ordinal,
      offset: offset,
      frame_size: frame_size
    )
  end

  defp locate(dependencies, file_id) do
    case dependencies.physical_location.(dependencies.ctx, dependencies.shard_index, file_id) do
      {:ok, {ordinal, offset, frame_size}}
      when is_integer(ordinal) and ordinal >= 0 and is_integer(offset) and offset >= 0 and
             is_integer(frame_size) and frame_size >= 8 ->
        {:ok, {ordinal, offset, frame_size}}

      {:error, reason} ->
        {:error, {:query_row_relocation_location_failed, file_id, reason}}

      invalid ->
        {:error, {:invalid_query_row_relocation_location, file_id, invalid}}
    end
  end

  defp write_updates(_lmdb_path, [], _dependencies), do: :ok

  defp write_updates(lmdb_path, updates, dependencies) do
    case dependencies.write_batch.(lmdb_path, relocation_ops(updates)) do
      :ok ->
        :ok

      {:error, {:compare_failed, state_key}} when is_binary(state_key) ->
        case Enum.split_with(updates, &(&1.state_key == state_key)) do
          {[failed], rest} ->
            with :ok <- write_updates(lmdb_path, rest, dependencies) do
              reconcile_raced_row(lmdb_path, failed, dependencies, 0)
            end

          _invalid ->
            {:error, {:unexpected_query_row_compare_failure, state_key}}
        end

      {:error, reason} ->
        {:error, {:query_row_relocation_write_failed, reason}}

      invalid ->
        {:error, {:invalid_query_row_relocation_write, invalid}}
    end
  end

  defp reconcile_raced_row(_lmdb_path, update, _dependencies, attempts)
       when attempts >= @maximum_cas_retries,
       do: {:error, {:query_row_relocation_race_limit, update.state_key}}

  defp reconcile_raced_row(lmdb_path, update, dependencies, attempts) do
    case dependencies.get.(lmdb_path, update.state_key) do
      :not_found ->
        :ok

      {:ok, current} when current == update.replacement ->
        :ok

      {:ok, current} when is_binary(current) ->
        with {:ok, %QueryRow{locator: locator}} <- decode_current(current, update.state_key),
             {:ok, next_update} <-
               rebuild_raced_update(update.state_key, current, locator, dependencies) do
          write_raced_update(lmdb_path, update.state_key, next_update, dependencies, attempts)
        end

      {:error, reason} ->
        {:error, {:query_row_relocation_read_failed, update.state_key, reason}}

      invalid ->
        {:error, {:invalid_query_row_relocation_read, update.state_key, invalid}}
    end
  end

  defp write_raced_update(_lmdb_path, _state_key, :current, _dependencies, _attempts), do: :ok

  defp write_raced_update(lmdb_path, state_key, next, dependencies, attempts) do
    case dependencies.write_batch.(lmdb_path, relocation_ops([next])) do
      :ok ->
        :ok

      {:error, {:compare_failed, ^state_key}} ->
        reconcile_raced_row(lmdb_path, next, dependencies, attempts + 1)

      {:error, reason} ->
        {:error, {:query_row_relocation_write_failed, reason}}

      invalid ->
        {:error, {:invalid_query_row_relocation_write, invalid}}
    end
  end

  defp decode_current(encoded, state_key) do
    case QueryRowCodec.decode(encoded, state_key) do
      {:ok, %QueryRow{} = row} -> {:ok, row}
      :error -> {:error, {:corrupt_query_row, state_key}}
    end
  end

  defp rebuild_raced_update(state_key, encoded, locator, dependencies) do
    if waraft_locator?(locator) do
      with {:ok, {ordinal, offset, frame_size}} <- locate(dependencies, locator.file_id),
           {:ok, relocated} <- relocate_locator(locator, ordinal, offset, frame_size) do
        if Locator.same_physical_record?(locator, relocated) do
          {:ok, :current}
        else
          case QueryRowCodec.relocate(encoded, state_key, locator, relocated) do
            {:ok, replacement} ->
              {:ok, %{state_key: state_key, expected: encoded, replacement: replacement}}

            {:error, reason} ->
              {:error, {:query_row_relocation_encode_failed, state_key, reason}}
          end
        end
      end
    else
      {:ok, :current}
    end
  end

  defp waraft_locator?(%Locator{file_id: {kind, index}})
       when kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0,
       do: true

  defp waraft_locator?(_locator), do: false

  defp relocation_ops(updates) do
    Enum.flat_map(updates, fn update ->
      [
        {:compare, update.state_key, update.expected},
        {:put, update.state_key, update.replacement}
      ]
    end)
  end

  defp dependencies(opts, ctx, shard_index) do
    dependencies = %{
      physical_location:
        Keyword.get(opts, :physical_location, &WARaftSegmentReader.physical_location/3),
      write_batch: Keyword.get(opts, :write_batch, &LMDB.write_batch/2),
      get: Keyword.get(opts, :get, &LMDB.get/2),
      ctx: ctx,
      shard_index: shard_index
    }

    if is_function(dependencies.physical_location, 3) and
         is_function(dependencies.write_batch, 2) and is_function(dependencies.get, 2) do
      {:ok, dependencies}
    else
      {:error, :invalid_query_row_relocation_dependencies}
    end
  end
end
