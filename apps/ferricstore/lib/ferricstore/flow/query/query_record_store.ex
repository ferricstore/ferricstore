defmodule Ferricstore.Flow.Query.QueryRecordStore do
  @moduledoc false

  alias Ferricstore.Flow.{Locator, RecordHydrator}

  alias Ferricstore.Flow.Query.{
    MemoryBudget,
    QueryRow,
    QueryRowCodec,
    QueryRowReference,
    QueryRowRelocator,
    QueryRowStore
  }

  @default_max_value_size 1_048_576
  @default_timeout_ms 10_000
  @maximum_hydration_bytes 1 * 1_024 * 1_024 * 1_024

  @spec max_input_bytes(map()) :: pos_integer()
  def max_input_bytes(ctx) when is_map(ctx) do
    max_value_size =
      case Map.get(ctx, :max_value_size, @default_max_value_size) do
        value when is_integer(value) and value > 0 -> value
        _invalid -> @default_max_value_size
      end

    min(max_value_size + 2 * QueryRowCodec.max_encoded_bytes(), @maximum_hydration_bytes)
  end

  @type query_row_reader ::
          (binary(), [binary()], non_neg_integer(), pos_integer() ->
             {:ok, [QueryRow.t() | QueryRowReference.t() | nil], non_neg_integer(), boolean()}
             | {:error, term()})

  @type hydrator ::
          (map(), non_neg_integer(), [{binary(), Locator.t()}], keyword() ->
             {:ok, [map() | nil]} | {:error, term()})

  @spec read_many(
          map(),
          non_neg_integer(),
          binary(),
          [binary()],
          non_neg_integer(),
          pos_integer(),
          keyword()
        ) :: {:ok, [map() | nil], boolean()} | {:error, atom()}
  def read_many(ctx, shard_index, path, state_keys, now_ms, max_input_bytes, opts \\ [])

  def read_many(ctx, shard_index, path, state_keys, now_ms, max_input_bytes, opts)
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and is_binary(path) and
             path != "" and is_list(state_keys) and is_integer(now_ms) and now_ms >= 0 and
             is_integer(max_input_bytes) and max_input_bytes > 0 and is_list(opts) do
    expired_fallback? = Keyword.get(opts, :expired_query_row_fallback, false)

    query_row_read =
      Keyword.get(
        opts,
        :query_row_read,
        if(expired_fallback?,
          do: &QueryRowStore.read_many/4,
          else: &QueryRowStore.read_references_many/4
        )
      )

    hydrate = Keyword.get(opts, :hydrate, &RecordHydrator.read_many/4)
    repair_locators = Keyword.get(opts, :repair_locators, &QueryRowRelocator.repair_many/4)

    with true <-
           is_boolean(expired_fallback?) and is_function(query_row_read, 4) and
             is_function(hydrate, 4) and
             is_function(repair_locators, 4),
         {:ok, include_expired?} <- include_expired_mode(opts),
         true <- not expired_fallback? or include_expired?,
         row_visibility_ms = if(include_expired?, do: 0, else: now_ms),
         normalized_opts =
           opts
           |> Keyword.put(:include_expired, include_expired?)
           |> Keyword.put(:expired_query_row_fallback, expired_fallback?)
           |> Keyword.put(:hydration_now_ms, now_ms)
           |> Keyword.put(:repair_locators, repair_locators),
         {:ok, hydration_opts} <- prepare_hydration_deadline(normalized_opts),
         {:ok, rows, complete?} <-
           read_query_rows(query_row_read, path, state_keys, row_visibility_ms, max_input_bytes),
         consumed_keys = Enum.take(state_keys, length(rows)),
         {:ok, records} <-
           hydrate_rows(
             ctx,
             shard_index,
             path,
             consumed_keys,
             rows,
             row_visibility_ms,
             max_input_bytes,
             query_row_read,
             hydrate,
             hydration_opts,
             0
           ) do
      {:ok, records, complete?}
    else
      false -> {:error, :query_storage_inconsistent}
      {:error, _reason} = error -> error
      _invalid -> {:error, :query_storage_inconsistent}
    end
  rescue
    _error -> {:error, :query_storage_unavailable}
  catch
    _kind, _reason -> {:error, :query_storage_unavailable}
  end

  def read_many(_ctx, _shard_index, _path, _state_keys, _now_ms, _max_input_bytes, _opts),
    do: {:error, :query_storage_inconsistent}

  defp include_expired_mode(opts) do
    case Keyword.get(opts, :include_expired, false) do
      value when is_boolean(value) -> {:ok, value}
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp prepare_hydration_deadline(opts) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    clock_ms = Keyword.get(opts, :clock_ms, fn -> System.monotonic_time(:millisecond) end)

    with true <- is_integer(timeout_ms) and timeout_ms > 0 and is_function(clock_ms, 0),
         {:ok, started_ms} <- call_clock(clock_ms) do
      {:ok,
       opts
       |> Keyword.put(:timeout_ms, timeout_ms)
       |> Keyword.put(:hydration_deadline_ms, started_ms + timeout_ms)
       |> Keyword.put(:hydration_clock_ms, clock_ms)}
    else
      _invalid -> {:error, :query_storage_inconsistent}
    end
  end

  defp call_clock(clock_ms) do
    case clock_ms.() do
      now_ms when is_integer(now_ms) -> {:ok, now_ms}
      _invalid -> {:error, :query_storage_inconsistent}
    end
  rescue
    _error -> {:error, :query_storage_inconsistent}
  catch
    _kind, _reason -> {:error, :query_storage_inconsistent}
  end

  defp remaining_hydration_timeout(opts) do
    deadline_ms = Keyword.fetch!(opts, :hydration_deadline_ms)
    clock_ms = Keyword.fetch!(opts, :hydration_clock_ms)

    with {:ok, now_ms} <- call_clock(clock_ms) do
      remaining_ms = deadline_ms - now_ms

      if remaining_ms > 0,
        do: {:ok, remaining_ms},
        else: {:error, :query_deadline_exceeded}
    end
  end

  @spec read_encoded_many(
          map(),
          non_neg_integer(),
          binary(),
          [binary()],
          non_neg_integer(),
          pos_integer(),
          keyword()
        ) :: {:ok, [binary() | nil], boolean()} | {:error, atom()}
  def read_encoded_many(
        ctx,
        shard_index,
        path,
        state_keys,
        now_ms,
        max_input_bytes,
        opts \\ []
      ) do
    opts = Keyword.put_new(opts, :hydrate, &RecordHydrator.read_encoded_many/4)
    read_many(ctx, shard_index, path, state_keys, now_ms, max_input_bytes, opts)
  end

  defp read_query_rows(reader, path, state_keys, now_ms, max_input_bytes) do
    case reader.(path, state_keys, now_ms, max_input_bytes) do
      {:ok, rows, value_bytes, complete?}
      when is_list(rows) and is_integer(value_bytes) and value_bytes >= 0 and
             value_bytes <= max_input_bytes and is_boolean(complete?) ->
        count = length(rows)
        requested = length(state_keys)

        cond do
          complete? and count == requested ->
            {:ok, rows, true}

          not complete? and count > 0 and count < requested ->
            {:ok, rows, false}

          not complete? and count == 0 and requested > 0 ->
            {:error, :query_hydration_batch_too_large}

          true ->
            {:error, :query_storage_inconsistent}
        end

      {:error, :batch_value_budget_exceeded} ->
        {:error, :query_hydration_batch_too_large}

      {:error, reason}
      when reason in [
             :invalid_query_row,
             :invalid_query_row_read,
             :query_row_result_count_mismatch
           ] ->
        {:error, :query_storage_inconsistent}

      {:error, _reason} ->
        {:error, :query_storage_unavailable}

      _invalid ->
        {:error, :query_storage_inconsistent}
    end
  end

  defp hydrate_rows(
         ctx,
         shard_index,
         path,
         state_keys,
         rows,
         now_ms,
         max_input_bytes,
         query_row_read,
         hydrate,
         opts,
         attempt
       ) do
    with {:ok, requests, positions} <- hydration_requests(state_keys, rows, now_ms),
         {:ok, hydration_max_bytes} <- hydration_budget(rows, requests, max_input_bytes),
         {:ok, hydrated} <-
           call_hydrator(ctx, shard_index, requests, hydration_max_bytes, hydrate, opts),
         {:ok, records} <- restore_records(rows, positions, hydrated) do
      case {attempt, Enum.any?(hydrated, &is_nil/1)} do
        {0, true} ->
          retry_opts =
            Keyword.put(
              opts,
              :locator_repair_keys,
              missing_hydration_keys(state_keys, positions, hydrated)
            )

          retry_repairable_hydration(
            ctx,
            shard_index,
            path,
            state_keys,
            rows,
            now_ms,
            max_input_bytes,
            query_row_read,
            hydrate,
            retry_opts,
            :query_storage_inconsistent
          )

        {_attempt, true} ->
          if Keyword.fetch!(opts, :expired_query_row_fallback) do
            restore_expired_query_row_records(
              rows,
              positions,
              hydrated,
              Keyword.fetch!(opts, :hydration_now_ms)
            )
          else
            {:error, :query_storage_inconsistent}
          end

        {_attempt, false} ->
          {:ok, records}
      end
    else
      {:error, :hydrated_record_identity_mismatch} when attempt == 0 ->
        retry_repairable_hydration(
          ctx,
          shard_index,
          path,
          state_keys,
          rows,
          now_ms,
          max_input_bytes,
          query_row_read,
          hydrate,
          Keyword.put(opts, :locator_repair_keys, :all),
          :query_storage_inconsistent
        )

      {:error, :hydrated_record_identity_mismatch} ->
        {:error, :query_storage_inconsistent}

      {:error, :query_storage_unavailable} when attempt == 0 ->
        retry_repairable_hydration(
          ctx,
          shard_index,
          path,
          state_keys,
          rows,
          now_ms,
          max_input_bytes,
          query_row_read,
          hydrate,
          Keyword.put(opts, :locator_repair_keys, :all),
          :query_storage_unavailable
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp retry_repairable_hydration(
         ctx,
         shard_index,
         path,
         state_keys,
         previous_rows,
         now_ms,
         max_input_bytes,
         query_row_read,
         hydrate,
         opts,
         no_progress_reason
       ) do
    case read_query_rows(query_row_read, path, state_keys, now_ms, max_input_bytes) do
      {:ok, rows, true} ->
        if hydration_locations_changed?(state_keys, previous_rows, rows, opts) do
          hydrate_rows(
            ctx,
            shard_index,
            path,
            state_keys,
            rows,
            now_ms,
            max_input_bytes,
            query_row_read,
            hydrate,
            opts,
            1
          )
        else
          repair_unchanged_locations(
            ctx,
            shard_index,
            path,
            state_keys,
            rows,
            now_ms,
            max_input_bytes,
            query_row_read,
            hydrate,
            opts,
            no_progress_reason
          )
        end

      {:ok, _rows, false} ->
        {:error, :query_hydration_batch_too_large}

      {:error, _reason} = error ->
        error
    end
  end

  defp repair_unchanged_locations(
         ctx,
         shard_index,
         path,
         state_keys,
         rows,
         now_ms,
         max_input_bytes,
         query_row_read,
         hydrate,
         opts,
         no_progress_reason
       ) do
    repair = Keyword.fetch!(opts, :repair_locators)

    with {:ok, _remaining_ms} <- remaining_hydration_timeout(opts),
         requests when requests != [] <- repair_requests(state_keys, rows, opts),
         {:ok, repaired_count} when is_integer(repaired_count) and repaired_count > 0 <-
           repair.(ctx, shard_index, path, requests),
         :ok <- emit_locator_repair(ctx, shard_index, repaired_count),
         {:ok, _remaining_ms} <- remaining_hydration_timeout(opts),
         {:ok, repaired_rows, true} <-
           read_query_rows(query_row_read, path, state_keys, now_ms, max_input_bytes),
         true <- hydration_locations_changed?(state_keys, rows, repaired_rows, opts) do
      hydrate_rows(
        ctx,
        shard_index,
        path,
        state_keys,
        repaired_rows,
        now_ms,
        max_input_bytes,
        query_row_read,
        hydrate,
        opts,
        1
      )
    else
      {:error, :query_deadline_exceeded} = error ->
        error

      {:ok, _rows, false} ->
        {:error, :query_hydration_batch_too_large}

      {:error, _reason} ->
        {:error, :query_storage_unavailable}

      _not_repaired ->
        continue_after_no_locator_repair(
          ctx,
          shard_index,
          path,
          state_keys,
          rows,
          now_ms,
          max_input_bytes,
          query_row_read,
          hydrate,
          opts,
          no_progress_reason
        )
    end
  end

  defp continue_after_no_locator_repair(
         ctx,
         shard_index,
         path,
         state_keys,
         rows,
         now_ms,
         max_input_bytes,
         query_row_read,
         hydrate,
         opts,
         no_progress_reason
       ) do
    if Keyword.fetch!(opts, :expired_query_row_fallback) do
      hydrate_rows(
        ctx,
        shard_index,
        path,
        state_keys,
        rows,
        now_ms,
        max_input_bytes,
        query_row_read,
        hydrate,
        opts,
        1
      )
    else
      {:error, no_progress_reason}
    end
  end

  defp repair_requests(state_keys, rows, opts) when length(state_keys) == length(rows) do
    state_keys
    |> Enum.zip(rows)
    |> Enum.reduce([], fn
      {state_key, row}, acc when is_binary(state_key) ->
        if not locator_repair_key?(state_key, opts) or
             expired_fallback_record_available?(row, opts) do
          acc
        else
          case row_locator(row) do
            {:ok, %Locator{file_id: {kind, index}} = locator}
            when kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
                   is_integer(index) and index > 0 ->
              [{state_key, locator} | acc]

            _not_repairable ->
              acc
          end
        end

      _invalid, acc ->
        acc
    end)
    |> Enum.reverse()
  end

  defp repair_requests(_state_keys, _rows, _opts), do: []

  defp emit_locator_repair(ctx, shard_index, repaired_count) do
    :telemetry.execute(
      [:ferricstore, :flow, :query, :locator_repair],
      %{count: repaired_count},
      %{instance: Map.get(ctx, :name), shard_index: shard_index}
    )
  end

  defp expired_fallback_record_available?(%QueryRow{expire_at_ms: expire_at_ms} = row, opts)
       when is_integer(expire_at_ms) and expire_at_ms > 0 do
    Keyword.fetch!(opts, :expired_query_row_fallback) and
      expire_at_ms <= Keyword.fetch!(opts, :hydration_now_ms) and
      match?({:ok, _record}, QueryRow.internal_record(row))
  end

  defp expired_fallback_record_available?(_row, _opts), do: false

  defp hydration_locations_changed?(state_keys, previous_rows, rows, opts)
       when is_list(state_keys) and is_list(previous_rows) and is_list(rows) and
              length(state_keys) == length(previous_rows) and
              length(previous_rows) == length(rows) do
    state_keys
    |> Enum.zip(Enum.zip(previous_rows, rows))
    |> Enum.any?(fn {state_key, {previous, current}} ->
      cond do
        not is_binary(state_key) ->
          true

        not locator_repair_key?(state_key, opts) ->
          false

        is_nil(previous) and is_nil(current) ->
          false

        true ->
          case {row_locator(previous), row_locator(current)} do
            {{:ok, previous}, {:ok, current}} -> previous != current
            _invalid -> true
          end
      end
    end)
  end

  defp hydration_locations_changed?(_state_keys, _previous_rows, _rows, _opts), do: true

  defp missing_hydration_keys(state_keys, positions, hydrated)
       when is_list(state_keys) and is_list(positions) and is_list(hydrated) and
              length(positions) == length(hydrated) do
    state_keys = List.to_tuple(state_keys)

    positions
    |> Enum.zip(hydrated)
    |> Enum.reduce(MapSet.new(), fn
      {position, nil}, keys
      when is_integer(position) and position >= 0 and position < tuple_size(state_keys) ->
        state_key = elem(state_keys, position)
        if is_binary(state_key), do: MapSet.put(keys, state_key), else: keys

      {_position, _record}, keys ->
        keys
    end)
  end

  defp missing_hydration_keys(_state_keys, _positions, _hydrated), do: MapSet.new()

  defp locator_repair_key?(state_key, opts) do
    case Keyword.fetch!(opts, :locator_repair_keys) do
      :all -> true
      %MapSet{} = keys -> MapSet.member?(keys, state_key)
    end
  end

  defp hydration_requests(state_keys, rows, now_ms) when length(state_keys) == length(rows) do
    state_keys
    |> Enum.zip(rows)
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, [], []}, fn
      {{_state_key, nil}, _index}, {:ok, requests, positions} ->
        {:cont, {:ok, requests, positions}}

      {{state_key, row}, index}, {:ok, requests, positions} when is_binary(state_key) ->
        case hydration_reference(row) do
          {:ok, row_key, flow_id, version, %Locator{} = locator, expire_at_ms}
          when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
            valid =
              row_key == state_key and Locator.hydration_ready?(locator) and
                flow_id == locator.flow_id and version == locator.version and
                (expire_at_ms == 0 or expire_at_ms > now_ms)

            if valid do
              {:cont, {:ok, [{state_key, locator} | requests], [index | positions]}}
            else
              {:halt, {:error, :query_storage_inconsistent}}
            end

          _invalid ->
            {:halt, {:error, :query_storage_inconsistent}}
        end

      _invalid, _acc ->
        {:halt, {:error, :query_storage_inconsistent}}
    end)
    |> case do
      {:ok, requests, positions} ->
        {:ok, Enum.reverse(requests), Enum.reverse(positions)}

      {:error, _reason} = error ->
        error
    end
  end

  defp hydration_requests(_state_keys, _rows, _now_ms),
    do: {:error, :query_storage_inconsistent}

  defp hydration_reference(%QueryRow{
         state_key: state_key,
         record: record,
         locator: %Locator{} = locator,
         expire_at_ms: expire_at_ms
       })
       when is_binary(state_key) and is_map(record) do
    {:ok, state_key, Map.get(record, :id), Map.get(record, :version), locator, expire_at_ms}
  end

  defp hydration_reference(%QueryRowReference{
         state_key: state_key,
         flow_id: flow_id,
         version: version,
         locator: %Locator{} = locator,
         expire_at_ms: expire_at_ms
       })
       when is_binary(state_key) and is_binary(flow_id) and is_integer(version) do
    {:ok, state_key, flow_id, version, locator, expire_at_ms}
  end

  defp hydration_reference(_row), do: :error

  defp row_locator(%QueryRow{locator: %Locator{} = locator}), do: {:ok, locator}
  defp row_locator(%QueryRowReference{locator: %Locator{} = locator}), do: {:ok, locator}
  defp row_locator(_row), do: :error

  defp hydration_budget(_rows, [], _max_input_bytes), do: {:ok, 1}

  defp hydration_budget(rows, _requests, max_input_bytes) do
    available_memory = MemoryBudget.decoded_record_reservation(max_input_bytes)
    remaining_memory = available_memory - MemoryBudget.term_bytes(rows)
    max_bytes = MemoryBudget.encoded_record_input_bytes(remaining_memory)

    if max_bytes > 0,
      do: {:ok, max_bytes},
      else: {:error, :query_hydration_batch_too_large}
  end

  defp call_hydrator(_ctx, _shard_index, [], _max_bytes, _hydrate, _opts), do: {:ok, []}

  defp call_hydrator(ctx, shard_index, requests, max_bytes, hydrate, opts) do
    with {:ok, timeout_ms} <- remaining_hydration_timeout(opts) do
      hydrate_opts = [
        max_bytes: max_bytes,
        timeout_ms: timeout_ms,
        now_ms: Keyword.fetch!(opts, :hydration_now_ms)
      ]

      hydrate_opts =
        if Keyword.get(opts, :include_expired, false),
          do: Keyword.put(hydrate_opts, :include_expired, true),
          else: hydrate_opts

      case hydrate.(ctx, shard_index, requests, hydrate_opts) do
        {:ok, records} when is_list(records) and length(records) == length(requests) ->
          {:ok, records}

        {:ok, _records} ->
          {:error, :query_storage_inconsistent}

        {:error, :hydrated_record_identity_mismatch} = error ->
          error

        {:error, :hydration_timeout} ->
          {:error, :query_deadline_exceeded}

        {:error, :query_deadline_exceeded} = error ->
          error

        {:error, reason}
        when reason in [
               :hydration_byte_budget_exceeded,
               :hydration_record_limit_exceeded,
               :invalid_hydration_byte_budget
             ] ->
          {:error, :query_hydration_batch_too_large}

        {:error, reason}
        when reason in [
               :invalid_hydration_request,
               :hydration_result_count_mismatch,
               :invalid_hydrated_record
             ] ->
          {:error, :query_storage_inconsistent}

        {:error, _reason} ->
          {:error, :query_storage_unavailable}

        _invalid ->
          {:error, :query_storage_inconsistent}
      end
    end
  end

  defp restore_records(rows, positions, hydrated) do
    true = length(positions) == length(hydrated)
    values = positions |> Enum.zip(hydrated) |> Map.new()

    {:ok,
     rows
     |> Enum.with_index()
     |> Enum.map(fn
       {nil, _index} -> nil
       {%QueryRow{}, index} -> Map.fetch!(values, index)
       {%QueryRowReference{}, index} -> Map.fetch!(values, index)
     end)}
  rescue
    _error -> {:error, :query_storage_inconsistent}
  end

  defp restore_expired_query_row_records(rows, positions, hydrated, now_ms) do
    hydrated_by_position = positions |> Enum.zip(hydrated) |> Map.new()

    rows
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn
      {nil, _index}, {:ok, acc} ->
        {:cont, {:ok, [nil | acc]}}

      {row, index}, {:ok, acc}
      when is_struct(row, QueryRow) or is_struct(row, QueryRowReference) ->
        case Map.fetch(hydrated_by_position, index) do
          {:ok, record} when is_map(record) ->
            {:cont, {:ok, [record | acc]}}

          {:ok, nil}
          when is_struct(row, QueryRow) and row.expire_at_ms > 0 and row.expire_at_ms <= now_ms ->
            case QueryRow.internal_record(row) do
              {:ok, record} -> {:cont, {:ok, [record | acc]}}
              :error -> {:halt, {:error, :query_storage_inconsistent}}
            end

          _missing_or_live ->
            {:halt, {:error, :query_storage_inconsistent}}
        end

      {_invalid, _index}, _acc ->
        {:halt, {:error, :query_storage_inconsistent}}
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, _reason} = error -> error
    end
  rescue
    _error -> {:error, :query_storage_inconsistent}
  end
end
