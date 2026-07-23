defmodule Ferricstore.Flow.Query.IndexValidation do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    BackfillSource,
    CompositeCounter,
    CompositeIndex,
    IndexDefinition
  }

  alias Ferricstore.Flow.Query.IndexValidation.DataPasses

  @max_definitions 16
  @max_items 16
  @max_bytes 16 * 1_024 * 1_024
  @max_read_keys 2_048
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @max_storage_key_bytes 511
  @validation_option_arities %{
    counter_get_fun: 2,
    definition_validation_observer_fun: 1,
    get_many_fun: 3,
    prefix_count_fun: 2,
    range_entries_fun: 6,
    staging_page_fun: 6
  }

  @type checkpoint :: %{
          phase: :source | :index | :counter | :cleanup,
          cursor: binary(),
          fenced: true,
          definition_position: non_neg_integer(),
          checked_records: non_neg_integer(),
          checked_entries: non_neg_integer(),
          mismatches: 0,
          counter_runs: [counter_run()]
        }

  @type counter_run :: %{
          prefix: binary(),
          count: pos_integer(),
          expiring_count: non_neg_integer(),
          physical_count: pos_integer(),
          expected_count: non_neg_integer()
        }

  @spec empty_checkpoint() :: checkpoint()
  def empty_checkpoint do
    %{
      phase: :source,
      cursor: "",
      fenced: true,
      definition_position: 0,
      checked_records: 0,
      checked_entries: 0,
      mismatches: 0,
      counter_runs: []
    }
  end

  @spec step(
          map(),
          non_neg_integer(),
          binary(),
          [IndexDefinition.t()],
          checkpoint(),
          pos_integer(),
          pos_integer(),
          keyword()
        ) ::
          {:ok, checkpoint()}
          | {:retry, :query_index_validation_concurrent_change}
          | {:restart, :query_index_validation_concurrent_change}
          | {:mismatch, map()}
          | {:error, term()}
  def step(
        ctx,
        shard_index,
        build_id,
        definitions,
        checkpoint,
        max_items,
        max_bytes,
        opts \\ []
      ) do
    with :ok <-
           validate_request(
             ctx,
             shard_index,
             build_id,
             definitions,
             checkpoint,
             max_items,
             max_bytes,
             opts
           ) do
      definitions = Enum.sort_by(definitions, &{&1.id, &1.version})
      do_step(ctx, shard_index, build_id, definitions, checkpoint, max_items, max_bytes, opts)
    end
  rescue
    _error -> {:error, :query_index_validation_failed}
  catch
    :exit, _reason -> {:error, :query_index_validation_dependency_unavailable}
    _kind, _reason -> {:error, :query_index_validation_failed}
  end

  defp do_step(
         ctx,
         shard_index,
         build_id,
         definitions,
         %{phase: :source} = checkpoint,
         max_items,
         max_bytes,
         opts
       ) do
    page_items = effective_page_items(ctx, max_items, max_bytes, length(definitions))
    staging_page = Keyword.get(opts, :staging_page_fun, &BackfillSource.staging_page/6)

    with {:ok, page} <-
           staging_page.(
             ctx,
             shard_index,
             build_id,
             checkpoint.cursor,
             page_items,
             max_bytes
           ),
         :ok <-
           validate_staging_page(
             page,
             page_items,
             max_bytes,
             build_id,
             checkpoint.cursor
           ),
         {:ok, checked_records} <-
           checked_add(checkpoint.checked_records, page.scanned_entries),
         result <-
           DataPasses.validate_source_rows(
             ctx,
             shard_index,
             definitions,
             page.state_keys,
             max_bytes,
             opts
           ) do
      case result do
        {:ok, expected_entries} ->
          with {:ok, checked_entries} <-
                 checked_add(checkpoint.checked_entries, expected_entries) do
            {:ok,
             %{
               checkpoint
               | phase: if(page.done?, do: :index, else: :source),
                 cursor: if(page.done?, do: "", else: page.cursor),
                 definition_position: 0,
                 checked_records: checked_records,
                 checked_entries: checked_entries
             }}
          end

        {:retry, _reason} = retry ->
          retry

        {:mismatch, mismatches, reason, expected_entries} ->
          case evidence(
                 checkpoint,
                 page.scanned_entries,
                 expected_entries,
                 mismatches,
                 reason
               ) do
            {:ok, evidence} -> {:mismatch, evidence}
            {:error, _reason} = error -> error
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp do_step(
         ctx,
         shard_index,
         _build_id,
         definitions,
         %{phase: :index, definition_position: position} = checkpoint,
         max_items,
         max_bytes,
         opts
       ) do
    if position >= length(definitions) do
      {:ok, %{checkpoint | phase: :cleanup, cursor: "", definition_position: length(definitions)}}
    else
      definition = Enum.at(definitions, position)
      prefix = IndexDefinition.storage_prefix(definition)
      range_entries = Keyword.get(opts, :range_entries_fun, &LMDB.range_entries_bounded/6)

      with {:ok, rows, exhausted, read_bytes} <-
             range_entries.(
               lmdb_path(ctx, shard_index),
               prefix,
               checkpoint.cursor,
               "",
               max_items,
               max_bytes
             ),
           :ok <-
             validate_index_page(
               rows,
               exhausted,
               read_bytes,
               prefix,
               checkpoint.cursor,
               max_items,
               max_bytes
             ),
           {:ok, checked_entries} <-
             checked_add(checkpoint.checked_entries, length(rows)),
           {:ok, counter_prefixes} <-
             DataPasses.counter_prefixes_for_page(definition, checkpoint.counter_runs, rows),
           result <-
             DataPasses.validate_index_rows(
               ctx,
               shard_index,
               definition,
               rows,
               checkpoint.counter_runs,
               counter_prefixes,
               exhausted,
               max_bytes,
               opts
             ) do
        case result do
          {:ok, counter_runs} ->
            next_index_checkpoint(
              checkpoint,
              definitions,
              rows,
              exhausted,
              checked_entries,
              counter_runs
            )

          {:retry, _reason} = retry ->
            retry

          {:restart, _reason} = restart ->
            restart

          {:mismatch, mismatches, reason} ->
            case evidence(checkpoint, 0, length(rows), mismatches, reason) do
              {:ok, evidence} -> {:mismatch, evidence}
              {:error, _reason} = error -> error
            end

          {:error, _reason} = error ->
            error
        end
      end
    end
  end

  defp do_step(
         ctx,
         shard_index,
         _build_id,
         definitions,
         %{phase: :counter, definition_position: position} = checkpoint,
         max_items,
         max_bytes,
         opts
       ) do
    if position >= length(definitions) do
      {:ok, %{checkpoint | phase: :cleanup, cursor: "", definition_position: length(definitions)}}
    else
      definition = Enum.at(definitions, position)
      prefix = CompositeCounter.definition_storage_prefix(definition)
      range_entries = Keyword.get(opts, :range_entries_fun, &LMDB.range_entries_bounded/6)

      with {:ok, rows, exhausted, read_bytes} <-
             range_entries.(
               lmdb_path(ctx, shard_index),
               prefix,
               checkpoint.cursor,
               "",
               max_items,
               max_bytes
             ),
           :ok <-
             validate_index_page(
               rows,
               exhausted,
               read_bytes,
               prefix,
               checkpoint.cursor,
               max_items,
               max_bytes
             ),
           {:ok, checked_entries} <-
             checked_add(checkpoint.checked_entries, length(rows)),
           result <-
             DataPasses.validate_counter_inventory_rows(
               ctx,
               shard_index,
               definition,
               rows,
               max_bytes,
               opts
             ) do
        case result do
          :ok ->
            next_counter_checkpoint(
              checkpoint,
              definitions,
              rows,
              exhausted,
              checked_entries
            )

          {:retry, _reason} = retry ->
            retry

          {:mismatch, mismatches, reason} ->
            case evidence(checkpoint, 0, length(rows), mismatches, reason) do
              {:ok, evidence} -> {:mismatch, evidence}
              {:error, _reason} = error -> error
            end

          {:error, _reason} = error ->
            error
        end
      end
    end
  end

  defp do_step(
         _ctx,
         _shard_index,
         _build_id,
         _definitions,
         %{phase: :cleanup} = checkpoint,
         _items,
         _bytes,
         _opts
       ),
       do: {:ok, checkpoint}

  defp do_step(_ctx, _shard_index, _build_id, _definitions, _checkpoint, _items, _bytes, _opts),
    do: {:error, :invalid_query_index_validation_checkpoint}

  defp next_index_checkpoint(
         checkpoint,
         definitions,
         rows,
         exhausted,
         checked_entries,
         counter_runs
       ) do
    cond do
      exhausted and checkpoint.definition_position + 1 >= length(definitions) ->
        {:ok,
         %{
           checkpoint
           | phase: :counter,
             cursor: "",
             definition_position: 0,
             checked_entries: checked_entries,
             counter_runs: []
         }}

      exhausted ->
        {:ok,
         %{
           checkpoint
           | cursor: "",
             definition_position: checkpoint.definition_position + 1,
             checked_entries: checked_entries,
             counter_runs: []
         }}

      true ->
        {:ok,
         %{
           checkpoint
           | cursor: rows |> List.last() |> elem(0),
             checked_entries: checked_entries,
             counter_runs: counter_runs
         }}
    end
  end

  defp next_counter_checkpoint(checkpoint, definitions, rows, exhausted, checked_entries) do
    cond do
      exhausted and checkpoint.definition_position + 1 >= length(definitions) ->
        {:ok,
         %{
           checkpoint
           | phase: :cleanup,
             cursor: "",
             definition_position: length(definitions),
             checked_entries: checked_entries,
             counter_runs: []
         }}

      exhausted ->
        {:ok,
         %{
           checkpoint
           | cursor: "",
             definition_position: checkpoint.definition_position + 1,
             checked_entries: checked_entries,
             counter_runs: []
         }}

      true ->
        {:ok,
         %{
           checkpoint
           | cursor: rows |> List.last() |> elem(0),
             checked_entries: checked_entries,
             counter_runs: []
         }}
    end
  end

  defp evidence(checkpoint, checked_records, checked_entries, mismatches, reason) do
    with {:ok, total_records} <- checked_add(checkpoint.checked_records, checked_records),
         {:ok, total_entries} <- checked_add(checkpoint.checked_entries, checked_entries),
         true <- nonnegative_u64?(mismatches) do
      {:ok,
       %{
         checked_records: total_records,
         checked_entries: total_entries,
         mismatches: mismatches,
         reason: reason
       }}
    else
      _overflow -> {:error, :query_index_validation_counter_overflow}
    end
  end

  defp validate_request(
         %{data_dir: data_dir, shard_count: shard_count},
         shard_index,
         build_id,
         definitions,
         checkpoint,
         max_items,
         max_bytes,
         opts
       )
       when is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and shard_count > 0 and
              is_integer(shard_index) and shard_index >= 0 and shard_index < shard_count and
              is_binary(build_id) and build_id != "" and byte_size(build_id) <= 128 and
              is_list(definitions) and definitions != [] and
              length(definitions) <= @max_definitions and is_integer(max_items) and max_items > 0 and
              max_items <= @max_items and is_integer(max_bytes) and max_bytes > 0 and
              max_bytes <= @max_bytes and is_list(opts) do
    if valid_options?(opts) and valid_definitions?(definitions, opts) and
         valid_checkpoint?(checkpoint, definitions),
       do: :ok,
       else: {:error, :invalid_query_index_validation_request}
  end

  defp validate_request(_ctx, _shard, _build, _definitions, _checkpoint, _items, _bytes, _opts),
    do: {:error, :invalid_query_index_validation_request}

  defp valid_checkpoint?(
         %{
           phase: phase,
           cursor: cursor,
           fenced: true,
           definition_position: position,
           checked_records: records,
           checked_entries: entries,
           mismatches: 0,
           counter_runs: counter_runs
         },
         definitions
       )
       when is_list(definitions) and definitions != [] and length(definitions) <= @max_definitions do
    definitions = Enum.sort_by(definitions, &{&1.id, &1.version})
    definition_count = length(definitions)

    phase in [:source, :index, :counter, :cleanup] and is_binary(cursor) and
      byte_size(cursor) <= @max_storage_key_bytes and
      is_integer(position) and position >= 0 and position <= @max_definitions and
      nonnegative_u64?(records) and nonnegative_u64?(entries) and
      valid_phase_position?(phase, position, cursor, definition_count) and
      valid_counter_runs?(phase, position, cursor, counter_runs, definitions)
  end

  defp valid_checkpoint?(_checkpoint, _definitions), do: false

  defp valid_phase_position?(:source, 0, _cursor, _definition_count), do: true

  defp valid_phase_position?(:index, position, _cursor, definition_count),
    do: position < definition_count

  defp valid_phase_position?(:counter, position, _cursor, definition_count),
    do: position < definition_count

  defp valid_phase_position?(:cleanup, position, "", definition_count),
    do: position == definition_count

  defp valid_phase_position?(_phase, _position, _cursor, _definition_count), do: false

  defp valid_counter_runs?(phase, _position, _cursor, [], _definitions)
       when phase in [:source, :counter, :cleanup],
       do: true

  defp valid_counter_runs?(:index, _position, "", [], _definitions), do: true

  defp valid_counter_runs?(:index, position, _cursor, [], definitions),
    do: Enum.at(definitions, position).count_prefixes == []

  defp valid_counter_runs?(:index, position, cursor, runs, definitions)
       when is_binary(cursor) and cursor != "" and is_list(runs) do
    definition = Enum.at(definitions, position)

    case CompositeCounter.prefixes_for_validated_key(definition, cursor) do
      {:ok, prefixes} when length(prefixes) == length(runs) ->
        Enum.zip(runs, prefixes)
        |> Enum.all?(fn
          {%{
             prefix: prefix,
             count: count,
             expiring_count: expiring_count,
             physical_count: physical_count,
             expected_count: expected
           } = run, expected_prefix} ->
            map_size(run) == 5 and prefix == expected_prefix and nonnegative_u64?(count) and
              count > 0 and nonnegative_u64?(expiring_count) and expiring_count <= count and
              nonnegative_u64?(physical_count) and physical_count >= count and
              nonnegative_u64?(expected) and count <= expected

          _invalid ->
            false
        end)

      _invalid ->
        false
    end
  end

  defp valid_counter_runs?(_phase, _position, _cursor, _runs, _definitions), do: false

  defp valid_definitions?(definitions, opts) do
    observer = Keyword.get(opts, :definition_validation_observer_fun, fn _definition -> :ok end)

    valid? =
      Enum.all?(definitions, fn definition ->
        validation = IndexDefinition.validate(definition)
        observer.(definition)
        validation == :ok
      end)

    valid? and unique_definition_identities?(definitions)
  end

  defp unique_definition_identities?(definitions) do
    identities = Enum.map(definitions, &{&1.id, &1.version})
    length(identities) == MapSet.size(MapSet.new(identities))
  end

  defp valid_options?(opts) do
    if Keyword.keyword?(opts) and length(opts) <= map_size(@validation_option_arities) do
      keys = Keyword.keys(opts)

      length(keys) == MapSet.size(MapSet.new(keys)) and
        Enum.all?(opts, fn {key, value} ->
          case Map.fetch(@validation_option_arities, key) do
            {:ok, arity} -> is_function(value, arity)
            :error -> false
          end
        end)
    else
      false
    end
  end

  defp validate_staging_page(
         %{
           state_keys: state_keys,
           cursor: cursor,
           done?: done?,
           scanned_entries: scanned_entries,
           staging_bytes: staging_bytes
         },
         max_items,
         max_bytes,
         build_id,
         previous_cursor
       )
       when is_list(state_keys) and is_binary(cursor) and is_boolean(done?) and
              is_integer(scanned_entries) and scanned_entries >= 0 and is_integer(staging_bytes) and
              staging_bytes >= 0 and is_binary(build_id) and is_binary(previous_cursor) do
    prefix = BackfillSource.staging_prefix(build_id)
    valid_state_keys? = Enum.all?(state_keys, &Keys.state_key?/1)

    actual_bytes =
      if valid_state_keys? do
        Enum.reduce(state_keys, 0, fn state_key, total ->
          total + byte_size(prefix) + 32 + byte_size(state_key)
        end)
      else
        0
      end

    cond do
      length(state_keys) != scanned_entries ->
        {:error, :invalid_query_index_validation_page}

      scanned_entries > max_items ->
        {:error, :invalid_query_index_validation_page}

      not valid_state_keys? ->
        {:error, :invalid_query_index_validation_page}

      staging_bytes > max_bytes or actual_bytes > max_bytes ->
        {:error, :query_index_validation_read_budget_exceeded}

      staging_bytes != actual_bytes ->
        {:error, :invalid_query_index_validation_page}

      not valid_staging_progress?(state_keys, cursor, done?, prefix, previous_cursor) ->
        if not done? and state_keys == [],
          do: {:error, :query_index_validation_made_no_progress},
          else: {:error, :invalid_query_index_validation_page}

      true ->
        :ok
    end
  end

  defp validate_staging_page(_page, _max_items, _max_bytes, _build_id, _previous_cursor),
    do: {:error, :invalid_query_index_validation_page}

  defp validate_index_page(
         rows,
         exhausted,
         read_bytes,
         prefix,
         previous_cursor,
         max_items,
         max_bytes
       )
       when is_list(rows) and is_boolean(exhausted) and is_integer(read_bytes) and read_bytes >= 0 and
              is_binary(prefix) and is_binary(previous_cursor) do
    valid_rows? = valid_page_rows?(rows, prefix, previous_cursor)
    actual_bytes = if valid_rows?, do: rows_bytes(rows), else: 0

    cond do
      length(rows) > max_items ->
        {:error, :invalid_query_index_validation_page}

      not exhausted and rows == [] ->
        {:error, :query_index_validation_made_no_progress}

      not valid_rows? ->
        {:error, :invalid_query_index_validation_page}

      read_bytes > max_bytes or actual_bytes > max_bytes ->
        {:error, :query_index_validation_read_budget_exceeded}

      read_bytes != actual_bytes ->
        {:error, :invalid_query_index_validation_page}

      true ->
        :ok
    end
  end

  defp validate_index_page(
         _rows,
         _exhausted,
         _read_bytes,
         _prefix,
         _previous_cursor,
         _max_items,
         _max_bytes
       ),
       do: {:error, :invalid_query_index_validation_page}

  defp valid_staging_progress?([], cursor, true, _prefix, previous_cursor),
    do: cursor == previous_cursor

  defp valid_staging_progress?([], _cursor, false, _prefix, _previous_cursor), do: false

  defp valid_staging_progress?(state_keys, cursor, _done?, prefix, previous_cursor) do
    keys = Enum.map(state_keys, &(prefix <> :crypto.hash(:sha256, &1)))
    valid_page_keys?(keys, previous_cursor) and cursor == List.last(keys)
  end

  defp valid_page_rows?([], _prefix, _previous_cursor), do: true

  defp valid_page_rows?([{key, value} | rows], prefix, previous_cursor)
       when is_binary(key) and is_binary(value) do
    byte_size(key) <= @max_storage_key_bytes and String.starts_with?(key, prefix) and
      key > previous_cursor and
      valid_page_rows?(rows, prefix, key)
  end

  defp valid_page_rows?(_rows, _prefix, _previous_cursor), do: false

  defp valid_page_keys?([], _previous_cursor), do: true

  defp valid_page_keys?([key | keys], previous_cursor),
    do: key > previous_cursor and valid_page_keys?(keys, key)

  defp rows_bytes(rows) do
    Enum.reduce(rows, 0, fn {key, value}, total ->
      total + byte_size(key) + byte_size(value)
    end)
  end

  defp effective_page_items(ctx, max_items, max_bytes, definition_count) do
    max_value_size =
      case Map.get(ctx, :max_value_size, 1_048_576) do
        value when is_integer(value) and value > 0 -> min(value, @max_bytes)
        _invalid -> 1_048_576
      end

    value_limited_items = max(div(max_bytes, max_value_size), 1)

    read_limited_items =
      @max_read_keys
      |> div(definition_count * CompositeIndex.max_entries_per_record())
      |> max(1)

    min(max_items, min(value_limited_items, read_limited_items))
  end

  defp checked_add(left, right)
       when is_integer(left) and left >= 0 and left <= @max_u64 and is_integer(right) and
              right >= 0 and right <= @max_u64 and left <= @max_u64 - right,
       do: {:ok, left + right}

  defp checked_add(_left, _right), do: {:error, :query_index_validation_counter_overflow}

  defp nonnegative_u64?(value),
    do: is_integer(value) and value >= 0 and value <= @max_u64

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
