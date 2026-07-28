defmodule Ferricstore.Flow.Schedule.Catalog do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.{Keys, LMDB, LMDBMirror, LMDBWriter, PolicyMigration}
  alias Ferricstore.Flow.Schedule.Metadata
  alias Ferricstore.Store.Router

  @schedule_type "__ferricstore_schedule"
  @schedule_id_prefix "__ferricstore_schedule__:"
  @max_page_bytes 4 * 1_024 * 1_024

  @type summary :: %{
          id: binary(),
          flow_id: binary(),
          state: binary(),
          kind: :one_shot | :delay | :interval | :cron,
          next_run_at_ms: non_neg_integer() | nil,
          target_type: binary(),
          timezone: binary() | nil,
          partition_key: binary(),
          version: pos_integer()
        }

  @spec reduce_summaries(FerricStore.Instance.t(), pos_integer(), term(), function()) ::
          {:ok, term()} | {:error, binary()}
  def reduce_summaries(ctx, page_size, initial, reducer)
      when is_integer(page_size) and page_size > 0 and page_size <= 512 and
             is_function(reducer, 2) do
    with :ok <- validate_context(ctx),
         :ok <- flush_catalog(ctx),
         :ok <- require_healthy_catalog(ctx) do
      ctx.data_dir
      |> LMDBMirror.shard_paths(ctx.shard_count)
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, initial}, fn {path, shard_index}, {:ok, acc} ->
        case reduce_shard(ctx, path, shard_index, page_size, "", acc, reducer) do
          {:ok, next_acc} -> {:cont, {:ok, next_acc}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  def reduce_summaries(_ctx, _page_size, _initial, _reducer),
    do: {:error, "ERR flow schedule catalog is unavailable"}

  defp reduce_shard(ctx, path, shard_index, page_size, cursor, acc, reducer) do
    prefix = Keys.policy_catalog_projection_prefix(@schedule_type)

    case LMDB.range_entries_bounded(
           path,
           prefix,
           cursor,
           "",
           page_size,
           @max_page_bytes
         ) do
      {:ok, rows, exhausted?, _bytes} ->
        with {:ok, summaries} <- decode_page(ctx, shard_index, rows) do
          next_acc = reducer.(summaries, acc)

          cond do
            exhausted? ->
              {:ok, next_acc}

            rows == [] ->
              corrupt_catalog()

            true ->
              {next_cursor, _value} = List.last(rows)
              reduce_shard(ctx, path, shard_index, page_size, next_cursor, next_acc, reducer)
          end
        end

      {:error, _reason} ->
        unavailable_catalog()
    end
  end

  defp decode_page(_ctx, _shard_index, []), do: {:ok, []}

  defp decode_page(ctx, shard_index, rows) do
    with {:ok, candidates} <- decode_projection_rows(rows),
         {:ok, state_keys} <- read_catalog_state_keys(ctx, shard_index, candidates),
         {:ok, state_values} <- read_state_values(ctx, state_keys) do
      decode_state_records(state_keys, state_values)
    end
  end

  defp decode_projection_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      {key, <<1>>}, {:ok, acc} ->
        case Keys.decode_policy_catalog_projection_key(@schedule_type, key) do
          {:ok, candidate} -> {:cont, {:ok, [candidate | acc]}}
          :error -> {:halt, corrupt_catalog()}
        end

      _invalid, _acc ->
        {:halt, corrupt_catalog()}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp read_catalog_state_keys(ctx, shard_index, candidates) do
    catalog_keys = Enum.map(candidates, & &1.catalog_key)

    with {:ok, values} <- read_shard_values(ctx, shard_index, catalog_keys) do
      candidates
      |> Enum.zip(values)
      |> Enum.reduce_while({:ok, []}, fn
        {_candidate, nil}, {:ok, acc} ->
          {:cont, {:ok, acc}}

        {candidate, encoded}, {:ok, acc} when is_binary(encoded) ->
          case PolicyMigration.decode_catalog(encoded) do
            {:ok, catalog} ->
              if valid_catalog_owner?(candidate, catalog) do
                {:cont, {:ok, [catalog.state_key | acc]}}
              else
                {:halt, corrupt_catalog()}
              end

            :error ->
              {:halt, corrupt_catalog()}
          end

        _invalid, _acc ->
          {:halt, corrupt_catalog()}
      end)
      |> case do
        {:ok, reversed} -> {:ok, reversed |> Enum.reverse() |> Enum.uniq()}
        {:error, _reason} = error -> error
      end
    end
  end

  defp valid_catalog_owner?(candidate, catalog) do
    catalog.migration_generation >= candidate.migration_generation and
      Keys.type_catalog_member_key(@schedule_type, catalog.state_key) == candidate.catalog_key
  end

  defp decode_state_records(state_keys, values) when length(state_keys) == length(values) do
    state_keys
    |> Enum.zip(values)
    |> Enum.reduce_while({:ok, []}, fn
      {_state_key, nil}, {:ok, acc} ->
        {:cont, {:ok, acc}}

      {state_key, encoded}, {:ok, acc} when is_binary(encoded) ->
        case decode_summary(state_key, encoded) do
          {:ok, summary} -> {:cont, {:ok, [summary | acc]}}
          :error -> {:halt, corrupt_catalog()}
        end

      _invalid, _acc ->
        {:halt, corrupt_catalog()}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_state_records(_state_keys, _values), do: corrupt_catalog()

  defp read_state_values(_ctx, []), do: {:ok, []}

  defp read_state_values(ctx, state_keys) do
    case Router.flow_batch_get_state_keys_with_status(ctx, state_keys) do
      values when is_list(values) and length(values) == length(state_keys) ->
        cond do
          Enum.any?(values, &(&1 == :unavailable)) -> unavailable_catalog()
          Enum.all?(values, &(is_binary(&1) or is_nil(&1))) -> {:ok, values}
          true -> corrupt_catalog()
        end

      _invalid ->
        corrupt_catalog()
    end
  end

  defp decode_summary(state_key, encoded) do
    with %{id: @schedule_id_prefix <> id, type: @schedule_type} = record <-
           Flow.decode_record(encoded),
         true <- Keys.state_key(record.id, Map.get(record, :partition_key)) == state_key,
         {:ok, metadata} <- Metadata.fetch_record(record),
         state when is_binary(state) and state != "" <- Map.get(record, :state),
         version when is_integer(version) and version > 0 <- Map.get(record, :version),
         partition_key when is_binary(partition_key) and partition_key != "" <-
           Map.get(record, :partition_key) do
      {:ok,
       %{
         id: id,
         flow_id: record.id,
         state: state,
         kind: metadata.kind,
         next_run_at_ms: visible_next_run_at_ms(record),
         target_type: metadata.target_type,
         timezone: Map.get(metadata, :timezone),
         partition_key: partition_key,
         version: version
       }}
    else
      _invalid -> :error
    end
  rescue
    _error -> :error
  end

  defp visible_next_run_at_ms(%{state: state})
       when state in ["completed", "failed", "cancelled"],
       do: nil

  defp visible_next_run_at_ms(record) do
    case Map.get(record, :next_run_at_ms) do
      value when is_integer(value) and value >= 0 -> value
      _none -> nil
    end
  end

  defp read_shard_values(_ctx, _shard_index, []), do: {:ok, []}

  defp read_shard_values(ctx, shard_index, keys) do
    case Router.read_shard_values_chunked(ctx, shard_index, keys) do
      {:ok, values} when is_list(values) and length(values) == length(keys) -> {:ok, values}
      :unavailable -> unavailable_catalog()
      {:error, _reason} -> unavailable_catalog()
      _invalid -> corrupt_catalog()
    end
  end

  defp validate_context(%{
         name: name,
         data_dir: data_dir,
         shard_count: shard_count,
         keydir_refs: keydir_refs
       })
       when is_atom(name) and is_binary(data_dir) and is_integer(shard_count) and shard_count > 0 and
              is_tuple(keydir_refs) and tuple_size(keydir_refs) >= shard_count,
       do: :ok

  defp validate_context(_ctx), do: unavailable_catalog()

  defp flush_catalog(ctx) do
    case LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000) do
      :ok -> :ok
      {:error, _reason} -> unavailable_catalog()
      _invalid -> unavailable_catalog()
    end
  end

  defp require_healthy_catalog(ctx) do
    case LMDBMirror.require_healthy(
           ctx,
           Keys.policy_catalog_projection_prefix(@schedule_type),
           nil
         ) do
      :ok -> :ok
      {:error, _reason} -> unavailable_catalog()
    end
  end

  defp unavailable_catalog, do: {:error, "ERR flow schedule catalog is unavailable"}
  defp corrupt_catalog, do: {:error, "ERR flow schedule catalog is corrupt"}
end
