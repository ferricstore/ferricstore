defmodule Ferricstore.Flow.Query.IndexValidation.DataPasses do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeCounter,
    CompositeIndex,
    Field,
    IndexDefinition,
    Limits
  }

  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.TermCodec

  @max_read_keys 2_048
  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  def validate_source_rows(_ctx, _shard_index, _definitions, [], _max_bytes, _opts),
    do: {:ok, 0}

  def validate_source_rows(ctx, shard_index, definitions, state_keys, max_bytes, opts) do
    path = lmdb_path(ctx, shard_index)
    reverse_keys = Enum.map(state_keys, &CompositeIndex.reverse_key/1)
    snapshot_keys = state_keys ++ reverse_keys
    get_many = Keyword.get(opts, :get_many_fun, &LMDB.get_many_bounded/3)

    with {:ok, first} <- bounded_get_many(get_many, path, snapshot_keys, max_bytes),
         {:ok, states, reverses} <- split_snapshot(first, length(state_keys)),
         {:ok, prepared} <- prepare_source_rows(definitions, state_keys, states, reverses),
         expected_keys <- prepared |> Enum.flat_map(& &1.expected) |> Enum.map(& &1.key),
         :ok <- validate_read_key_count(expected_keys),
         {:ok, expected_values} <- bounded_get_many(get_many, path, expected_keys, max_bytes),
         {:ok, second} <- bounded_get_many(get_many, path, snapshot_keys, max_bytes) do
      if first == second do
        classify_source_rows(prepared, expected_keys, expected_values)
      else
        {:retry, :query_index_validation_concurrent_change}
      end
    end
  end

  defp prepare_source_rows(definitions, state_keys, states, reverses) do
    prefixes = Enum.map(definitions, &IndexDefinition.storage_prefix/1)

    Enum.zip([state_keys, states, reverses])
    |> Enum.reduce_while({:ok, []}, fn {state_key, state_result, reverse_result}, {:ok, acc} ->
      with {:ok, projected} <- decode_projected_state(state_result),
           {:ok, reverse_keys} <- decode_reverse_result(reverse_result, state_key),
           {:ok, expected} <- expected_entries(definitions, projected, state_key) do
        candidate_reverse =
          reverse_keys
          |> Enum.filter(fn key -> Enum.any?(prefixes, &String.starts_with?(key, &1)) end)
          |> Enum.sort()

        {:cont,
         {:ok,
          [
            %{
              expected: expected,
              reverse_keys: candidate_reverse,
              expected_keys: expected |> Enum.map(& &1.key) |> Enum.sort()
            }
            | acc
          ]}}
      else
        {:error, reason} ->
          {:cont,
           {:ok, [%{expected: [], reverse_keys: [], expected_keys: [], issue: reason} | acc]}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp classify_source_rows(prepared, expected_keys, expected_values) do
    values = Map.new(Enum.zip(expected_keys, expected_values))

    {issues, expected_count} =
      Enum.reduce(prepared, {[], 0}, fn row, {issues, count} ->
        count = count + length(row.expected)

        issues =
          cond do
            Map.has_key?(row, :issue) ->
              [row.issue | issues]

            row.reverse_keys != row.expected_keys ->
              [:reverse_index_mismatch | issues]

            true ->
              Enum.reduce(row.expected, issues, fn entry, entry_issues ->
                case Map.get(values, entry.key) do
                  {:ok, value} when value == entry.value -> entry_issues
                  :not_found -> [:missing_index_entry | entry_issues]
                  _other -> [:entry_value_mismatch | entry_issues]
                end
              end)
          end

        {issues, count}
      end)

    case Enum.reverse(issues) do
      [] -> {:ok, expected_count}
      [reason | _] = mismatches -> {:mismatch, length(mismatches), reason, expected_count}
    end
  end

  def validate_index_rows(
        ctx,
        shard_index,
        definition,
        rows,
        counter_runs,
        counter_prefixes,
        exhausted,
        max_bytes,
        opts
      ) do
    path = lmdb_path(ctx, shard_index)

    decoded =
      Enum.map(rows, fn {key, value} -> {key, value, CompositeIndex.decode_entry_value(value)} end)

    state_keys =
      decoded
      |> Enum.flat_map(fn
        {_key, _value, {:ok, %{state_key: state_key}}} -> [state_key]
        _invalid -> []
      end)
      |> Enum.uniq()

    row_keys = Enum.map(rows, &elem(&1, 0))
    counter_keys = Enum.map(counter_prefixes, &CompositeCounter.key(definition, &1))
    snapshot_keys = state_keys ++ row_keys ++ counter_keys
    get_many = Keyword.get(opts, :get_many_fun, &LMDB.get_many_bounded/3)

    with {:ok, first} <- bounded_get_many(get_many, path, snapshot_keys, max_bytes),
         {:ok, second} <- bounded_get_many(get_many, path, snapshot_keys, max_bytes) do
      {state_results, remaining} = Enum.split(first, length(state_keys))
      {row_results, counter_results} = Enum.split(remaining, length(row_keys))
      expected_row_results = Enum.map(rows, fn {_key, value} -> {:ok, value} end)

      if first == second and row_results == expected_row_results do
        states = Map.new(Enum.zip(state_keys, state_results))

        with {:ok, counter_rows} <- classify_index_rows(definition, decoded, states),
             {:ok, counter_page} <-
               prepare_counter_page(counter_runs, counter_rows, exhausted),
             {:ok, counter_runs} <-
               classify_counter_runs(
                 path,
                 definition,
                 counter_page,
                 counter_prefixes,
                 counter_results,
                 opts
               ) do
          {:ok, counter_runs}
        end
      else
        {:retry, :query_index_validation_concurrent_change}
      end
    end
  end

  defp classify_index_rows(definition, rows, states) do
    {issues, counter_rows} =
      Enum.reduce(rows, {[], []}, fn
        {_key, _value, :error}, {issues, counter_rows} ->
          {[:invalid_index_entry | issues], counter_rows}

        {key, value, {:ok, decoded}}, {issues, counter_rows} ->
          case classify_index_row(definition, key, value, decoded, states) do
            {:ok, counter_row} -> {issues, [counter_row | counter_rows]}
            {:error, reason} -> {[reason | issues], counter_rows}
          end
      end)

    case Enum.reverse(issues) do
      [] -> {:ok, Enum.reverse(counter_rows)}
      [reason | _] = mismatches -> {:mismatch, length(mismatches), reason}
    end
  end

  defp classify_index_row(definition, key, value, decoded, states) do
    case Map.get(states, decoded.state_key, :not_found) do
      :not_found ->
        {:error, :orphan_index_entry}

      state_result ->
        with {:ok, projected} when not is_nil(projected) <- decode_projected_state(state_result),
             {:ok, expected} <- expected_entries([definition], projected, decoded.state_key),
             %{value: ^value} <- Enum.find(expected, &(&1.key == key)),
             {:ok, prefixes} <- CompositeCounter.prefixes_for_validated_key(definition, key) do
          counted_prefixes = canonical_counter_prefixes(key, prefixes, expected)

          {:ok,
           %{
             prefixes: prefixes,
             counted_prefixes: MapSet.new(counted_prefixes),
             expiring?: decoded.expire_at_ms > 0
           }}
        else
          {:ok, nil} -> {:error, :orphan_index_entry}
          nil -> {:error, :entry_key_mismatch}
          %{value: _different} -> {:error, :entry_value_mismatch}
          {:error, _reason} -> {:error, :invalid_projected_state}
        end
    end
  end

  defp canonical_counter_prefixes(key, prefixes, expected) do
    Enum.filter(prefixes, fn prefix ->
      expected
      |> Enum.reduce(nil, fn %{key: expected_key}, smallest ->
        if String.starts_with?(expected_key, prefix) and
             (is_nil(smallest) or expected_key < smallest),
           do: expected_key,
           else: smallest
      end)
      |> Kernel.==(key)
    end)
  end

  def counter_prefixes_for_page(definition, counter_runs, rows) do
    initial = Enum.map(counter_runs, & &1.prefix)

    Enum.reduce_while(rows, {:ok, initial}, fn {key, _value}, {:ok, prefixes} ->
      case CompositeCounter.prefixes_for_validated_key(definition, key) do
        {:ok, row_prefixes} -> {:cont, {:ok, :lists.reverse(row_prefixes, prefixes)}}
        {:error, _reason} -> {:halt, {:error, :invalid_query_index_validation_counter_key}}
      end
    end)
    |> case do
      {:ok, prefixes} -> {:ok, prefixes |> Enum.reverse() |> Enum.uniq()}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_counter_page(counter_runs, rows, exhausted) do
    Enum.reduce_while(rows, {:ok, counter_runs, []}, fn row, {:ok, active, completed} ->
      with {:ok, next_active, next_completed} <-
             advance_counter_runs(
               active,
               row.prefixes,
               row.counted_prefixes,
               row.expiring?,
               completed
             ) do
        {:cont, {:ok, next_active, next_completed}}
      else
        {:error, _reason} -> {:halt, {:error, :invalid_query_index_validation_counter_key}}
      end
    end)
    |> case do
      {:ok, active, completed} ->
        {active, completed} =
          if exhausted,
            do: {[], :lists.reverse(active, completed)},
            else: {active, completed}

        {:ok,
         %{
           active_runs: active,
           completed_runs: completed
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp advance_counter_runs([], prefixes, counted_prefixes, expiring?, completed) do
    if Enum.all?(prefixes, &MapSet.member?(counted_prefixes, &1)) do
      {:ok, Enum.map(prefixes, &new_counter_run(&1, expiring?)), completed}
    else
      {:error, :invalid_query_index_validation_counter_runs}
    end
  end

  defp advance_counter_runs(active, prefixes, counted_prefixes, expiring?, completed)
       when length(active) == length(prefixes) do
    Enum.zip(active, prefixes)
    |> Enum.reduce_while({:ok, [], completed}, fn {run, prefix},
                                                  {:ok, next_active, next_completed} ->
      if run.prefix == prefix do
        increment = if MapSet.member?(counted_prefixes, prefix), do: 1, else: 0

        with {:ok, count} <- checked_add(run.count, increment),
             {:ok, expiring_count} <-
               checked_add(run.expiring_count, if(expiring? and increment == 1, do: 1, else: 0)),
             {:ok, physical_count} <- checked_add(run.physical_count, 1) do
          {:cont,
           {:ok,
            [
              %{
                run
                | count: count,
                  expiring_count: expiring_count,
                  physical_count: physical_count
              }
              | next_active
            ], next_completed}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      else
        if not MapSet.member?(counted_prefixes, prefix) do
          {:halt, {:error, :invalid_query_index_validation_counter_runs}}
        else
          {:cont,
           {:ok, [new_counter_run(prefix, expiring?) | next_active], [run | next_completed]}}
        end
      end
    end)
    |> case do
      {:ok, reversed, next_completed} ->
        {:ok, Enum.reverse(reversed), next_completed}

      {:error, _reason} = error ->
        error
    end
  end

  defp advance_counter_runs(_active, _prefixes, _counted_prefixes, _expiring?, _completed),
    do: {:error, :invalid_query_index_validation_counter_runs}

  defp new_counter_run(prefix, expiring?) do
    %{
      prefix: prefix,
      count: 1,
      expiring_count: if(expiring?, do: 1, else: 0),
      physical_count: 1,
      expected_count: nil
    }
  end

  defp classify_counter_runs(path, definition, counter_page, prefixes, results, opts) do
    with {:ok, states} <- decode_counter_results(prefixes, results, %{}),
         {:ok, active} <- hydrate_active_counter_runs(counter_page.active_runs, states),
         :ok <-
           validate_completed_counter_runs(
             path,
             definition,
             counter_page.completed_runs,
             states,
             opts
           ) do
      {:ok, active}
    end
  end

  defp decode_counter_results([], [], counts), do: {:ok, counts}

  defp decode_counter_results([prefix | prefixes], [:not_found | results], counts),
    do:
      decode_counter_results(
        prefixes,
        results,
        Map.put(counts, prefix, %{count: 0, expiring_count: 0, physical_count: 0})
      )

  defp decode_counter_results([prefix | prefixes], [{:ok, blob} | results], counts) do
    case CompositeCounter.decode_state(blob, prefix) do
      {:ok, state} -> decode_counter_results(prefixes, results, Map.put(counts, prefix, state))
      :error -> {:mismatch, 1, :invalid_composite_counter}
    end
  end

  defp decode_counter_results(_prefixes, _results, _counts),
    do: {:error, :invalid_query_index_validation_read}

  defp hydrate_active_counter_runs(runs, states) do
    Enum.reduce_while(runs, {:ok, []}, fn run, {:ok, hydrated} ->
      current = Map.fetch!(states, run.prefix)
      expected = max(run.expected_count || 0, max(current.count, run.count))
      {:cont, {:ok, [%{run | expected_count: expected} | hydrated]}}
    end)
    |> then(fn {:ok, reversed} -> {:ok, Enum.reverse(reversed)} end)
  end

  defp validate_completed_counter_runs(path, definition, runs, states, opts) do
    Enum.reduce_while(runs, :ok, fn run, :ok ->
      current = Map.fetch!(states, run.prefix)

      cond do
        run.physical_count == current.physical_count and run.count != current.count ->
          {:halt, {:mismatch, 1, :composite_counter_mismatch}}

        run.physical_count == current.physical_count and
            not expiring_counter_safe?(run, current) ->
          {:halt, {:mismatch, 1, :composite_counter_expiry_mismatch}}

        run.physical_count == current.physical_count ->
          {:cont, :ok}

        true ->
          case confirm_counter_prefix(path, definition, run, current, opts) do
            :ok ->
              if expiring_counter_safe?(run, current),
                do: {:cont, :ok},
                else: {:halt, {:mismatch, 1, :composite_counter_expiry_mismatch}}

            {:restart, _reason} = restart ->
              {:halt, restart}

            {:retry, _reason} = retry ->
              {:halt, retry}

            {:mismatch, _count, _reason} = mismatch ->
              {:halt, mismatch}

            {:error, _reason} = error ->
              {:halt, error}
          end
      end
    end)
  end

  # The executor only needs a conservative non-zero signal to avoid the fast
  # count path. Exact projection bookkeeping is retained for recovery, while
  # validation accepts concurrent growth that cannot make a query unsafe.
  defp expiring_counter_safe?(run, current) do
    run.expiring_count == 0 or current.expiring_count > 0
  end

  # A row inserted behind a saved cursor can make the page aggregate stale. The
  # persisted physical count lets this discrepancy path distinguish that race
  # from corruption with one native LMDB prefix count; normal validation remains
  # a single bounded pass.
  defp confirm_counter_prefix(path, definition, run, observed, opts) do
    prefix_count = Keyword.get(opts, :prefix_count_fun, &LMDB.prefix_count/2)
    counter_get = Keyword.get(opts, :counter_get_fun, &LMDB.get/2)
    counter_key = CompositeCounter.key(definition, run.prefix)

    with {:ok, index_count} <- prefix_count.(path, run.prefix),
         true <- nonnegative_u64?(index_count),
         {:ok, confirmed} <- read_counter_state(counter_get.(path, counter_key), run.prefix) do
      cond do
        confirmed != observed ->
          {:retry, :query_index_validation_concurrent_change}

        index_count == run.physical_count ->
          {:mismatch, 1, :composite_counter_physical_mismatch}

        index_count == confirmed.physical_count and
            confirmed.count == confirmed.physical_count ->
          :ok

        index_count == confirmed.physical_count ->
          {:restart, :query_index_validation_concurrent_change}

        true ->
          {:mismatch, 1, :composite_counter_physical_mismatch}
      end
    else
      false -> {:error, :invalid_query_index_validation_read}
      {:mismatch, _count, _reason} = mismatch -> mismatch
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_query_index_validation_read}
    end
  end

  defp read_counter_state(:not_found, _prefix),
    do: {:ok, %{count: 0, expiring_count: 0, physical_count: 0}}

  defp read_counter_state({:ok, blob}, prefix) do
    case CompositeCounter.decode_state(blob, prefix) do
      {:ok, state} -> {:ok, state}
      :error -> {:mismatch, 1, :invalid_composite_counter}
    end
  end

  defp read_counter_state({:error, _reason} = error, _prefix), do: error
  defp read_counter_state(_invalid, _prefix), do: {:error, :invalid_query_index_validation_read}

  def validate_counter_inventory_rows(_ctx, _shard_index, _definition, [], _max_bytes, _opts),
    do: :ok

  def validate_counter_inventory_rows(ctx, shard_index, definition, rows, max_bytes, opts) do
    path = lmdb_path(ctx, shard_index)
    range_entries = Keyword.get(opts, :range_entries_fun, &LMDB.range_entries_bounded/6)

    Enum.reduce_while(rows, {:ok, 0}, fn {key, blob}, {:ok, read_bytes} ->
      result =
        with {:ok, %{prefix: prefix, count: count}} when count > 0 <-
               CompositeCounter.decode_validated_storage_entry(definition, key, blob),
             remaining when remaining > 0 <- max_bytes - read_bytes,
             {:ok, entries, exhausted, bytes} <-
               range_entries.(path, prefix, "", "", 1, remaining),
             :ok <- validate_counter_witness(entries, exhausted, bytes, prefix, remaining),
             next_bytes <- read_bytes + bytes,
             true <- next_bytes <= max_bytes do
          {:ok, next_bytes}
        else
          :error ->
            {:mismatch, 1, :invalid_composite_counter}

          {:ok, %{count: 0}} ->
            {:mismatch, 1, :orphan_composite_counter}

          remaining when is_integer(remaining) and remaining <= 0 ->
            {:error, :query_index_validation_read_budget_exceeded}

          {:mismatch, _count, _reason} = mismatch ->
            mismatch

          {:error, _reason} = error ->
            error

          false ->
            {:error, :query_index_validation_read_budget_exceeded}

          _invalid ->
            {:error, :invalid_query_index_validation_read}
        end

      case result do
        {:ok, next_bytes} ->
          {:cont, {:ok, next_bytes}}

        {:mismatch, _count, _reason} = mismatch ->
          case confirm_counter_inventory_mismatch(path, key, blob, mismatch, opts) do
            {:retry, _reason} = retry -> {:halt, retry}
            confirmed -> {:halt, confirmed}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, _read_bytes} -> :ok
      {:retry, _reason} = retry -> retry
      {:mismatch, _count, _reason} = mismatch -> mismatch
      {:error, _reason} = error -> error
    end
  end

  # Inventory pages and their witness reads use separate short LMDB snapshots.
  # Pay for an exact counter re-read only before reporting corruption so a
  # normal concurrent projection update is retried rather than misclassified.
  defp confirm_counter_inventory_mismatch(path, key, blob, mismatch, opts) do
    counter_get = Keyword.get(opts, :counter_get_fun, &LMDB.get/2)

    case counter_get.(path, key) do
      {:ok, ^blob} -> mismatch
      :not_found -> {:retry, :query_index_validation_concurrent_change}
      {:ok, _changed} -> {:retry, :query_index_validation_concurrent_change}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_query_index_validation_read}
    end
  end

  defp validate_counter_witness([{key, value}], _exhausted, read_bytes, prefix, max_bytes)
       when is_binary(key) and is_binary(value) and is_integer(read_bytes) and read_bytes >= 0 do
    actual_bytes = byte_size(key) + byte_size(value)

    if String.starts_with?(key, prefix) and read_bytes == actual_bytes and read_bytes <= max_bytes,
      do: :ok,
      else: {:error, :invalid_query_index_validation_read}
  end

  defp validate_counter_witness([], true, 0, _prefix, _max_bytes),
    do: {:mismatch, 1, :orphan_composite_counter}

  defp validate_counter_witness(_entries, _exhausted, _bytes, _prefix, _max_bytes),
    do: {:error, :invalid_query_index_validation_read}

  defp decode_projected_state(:not_found), do: {:ok, nil}

  defp decode_projected_state({:ok, blob}) when is_binary(blob) do
    with {:ok, {expire_at_ms, encoded}} <- TermCodec.decode(blob),
         true <- is_integer(expire_at_ms) and expire_at_ms >= 0 and is_binary(encoded),
         record when is_map(record) <- Ferricstore.Flow.decode_record(encoded) do
      {:ok,
       %{
         record: record,
         expire_at_ms: ProjectionOps.flow_state_projection_expire_at(record, expire_at_ms)
       }}
    else
      _invalid -> {:error, :invalid_projected_state}
    end
  rescue
    _error -> {:error, :invalid_projected_state}
  end

  defp decode_projected_state(_invalid), do: {:error, :invalid_projected_state}

  defp decode_reverse_result(:not_found, _state_key), do: {:ok, []}

  defp decode_reverse_result({:ok, blob}, state_key) when is_binary(blob) do
    case CompositeIndex.decode_reverse_value(blob, state_key) do
      {:ok, keys} -> {:ok, keys}
      :error -> {:error, :invalid_composite_reverse}
    end
  end

  defp decode_reverse_result(_invalid, _state_key), do: {:error, :invalid_composite_reverse}

  defp expected_entries(_definitions, nil, _state_key), do: {:ok, []}

  defp expected_entries(definitions, projected, state_key) do
    with :ok <- validate_projected_identity(projected.record, state_key) do
      Enum.reduce_while(definitions, {:ok, []}, fn definition, {:ok, acc} ->
        case CompositeIndex.entries_validated(
               definition,
               projected.record,
               state_key,
               projected.expire_at_ms
             ) do
          {:ok, entries} -> {:cont, {:ok, :lists.reverse(entries, acc)}}
          {:error, :unscoped_record} -> {:cont, {:ok, acc}}
          {:error, _reason} -> {:halt, {:error, :invalid_projected_state}}
        end
      end)
      |> case do
        {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
        {:error, _reason} = error -> error
      end
    end
  end

  defp validate_projected_identity(record, state_key) do
    with {:ok, id} <- Field.fetch(record, :run_id),
         true <- Limits.valid_run_id?(id),
         {:ok, partition_key} <- projected_partition_key(record),
         true <- is_nil(partition_key) or Limits.valid_partition_key?(partition_key),
         true <- Keys.state_key(id, partition_key) == state_key do
      :ok
    else
      _invalid -> {:error, :state_key_identity_mismatch}
    end
  end

  defp projected_partition_key(record) do
    case Field.fetch(record, :partition_key) do
      :missing -> {:ok, nil}
      {:ok, nil} -> {:ok, nil}
      {:ok, partition_key} -> {:ok, partition_key}
    end
  end

  defp bounded_get_many(_get_many, _path, [], _max_bytes), do: {:ok, []}

  defp bounded_get_many(get_many, path, keys, max_bytes) do
    with :ok <- validate_read_key_count(keys) do
      case get_many.(path, keys, max_bytes) do
        {:ok, values, bytes}
        when is_list(values) and length(values) == length(keys) and is_integer(bytes) and
               bytes >= 0 and bytes <= max_bytes ->
          if Enum.all?(values, &valid_read_result?/1) and read_bytes(values) == bytes,
            do: {:ok, values},
            else: {:error, :invalid_query_index_validation_read}

        {:error, reason}
        when reason in [:batch_value_budget_exceeded, :batch_key_budget_exceeded] ->
          {:error, :query_index_validation_read_budget_exceeded}

        {:error, _reason} = error ->
          error

        _invalid ->
          {:error, :invalid_query_index_validation_read}
      end
    end
  end

  defp split_snapshot(values, state_count) do
    if length(values) == state_count * 2 do
      {states, reverses} = Enum.split(values, state_count)
      {:ok, states, reverses}
    else
      {:error, :invalid_query_index_validation_read}
    end
  end

  defp validate_read_key_count(keys) when length(keys) <= @max_read_keys do
    if Enum.all?(keys, &(is_binary(&1) and &1 != "")),
      do: :ok,
      else: {:error, :invalid_query_index_validation_read}
  end

  defp validate_read_key_count(_keys),
    do: {:error, :query_index_validation_read_budget_exceeded}

  defp valid_read_result?(:not_found), do: true
  defp valid_read_result?({:ok, value}), do: is_binary(value)
  defp valid_read_result?(_invalid), do: false

  defp read_bytes(values) do
    Enum.reduce(values, 0, fn
      {:ok, value}, total -> total + byte_size(value)
      :not_found, total -> total
    end)
  end

  defp checked_add(left, right)
       when is_integer(left) and left >= 0 and left <= @max_u64 and is_integer(right) and
              right >= 0 and right <= @max_u64 and left <= @max_u64 - right,
       do: {:ok, left + right}

  defp checked_add(_left, _right),
    do: {:error, :query_index_validation_counter_overflow}

  defp nonnegative_u64?(value),
    do: is_integer(value) and value >= 0 and value <= @max_u64

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
