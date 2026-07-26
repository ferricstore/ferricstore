defmodule Ferricstore.Flow.Query.QueryRowCompaction do
  @moduledoc false

  alias Ferricstore.Flow.{LMDB, Locator, RecordHydrator}
  alias Ferricstore.Flow.Query.{QueryRow, QueryRowCodec, QueryRowRelocator, SourceCatalog}

  @page_items 256
  @catalog_page_bytes 16 * 1_024 * 1_024
  @row_page_bytes @page_items * QueryRowCodec.max_encoded_bytes()
  @hydration_bytes 1 * 1_024 * 1_024 * 1_024
  @hydration_timeout_ms 30_000

  @type retention_ref :: {{pos_integer(), binary()}, {binary(), binary(), non_neg_integer()}}

  @doc false
  @spec collect_retention_entries(
          map(),
          non_neg_integer(),
          binary(),
          pos_integer(),
          non_neg_integer()
        ) :: {:ok, [retention_ref()]} | {:error, term()}
  def collect_retention_entries(ctx, shard_index, lmdb_path, trim_index, expiry_cutoff_ms)
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(lmdb_path) and is_integer(trim_index) and trim_index > 0 and
             is_integer(expiry_cutoff_ms) and expiry_cutoff_ms >= 0 do
    case stream_retention_entries(
           ctx,
           shard_index,
           lmdb_path,
           trim_index,
           expiry_cutoff_ms,
           [],
           fn entries, acc -> {:ok, :lists.reverse(entries, acc)} end
         ) do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, {:query_row_retention_failed, error}}
  catch
    kind, reason -> {:error, {:query_row_retention_failed, {kind, reason}}}
  end

  def collect_retention_entries(_ctx, _shard_index, _lmdb_path, _trim_index, _expiry_cutoff_ms),
    do: {:error, :invalid_query_row_retention_request}

  @doc false
  @spec stream_retention_entries(
          map(),
          non_neg_integer(),
          binary(),
          pos_integer(),
          non_neg_integer(),
          term(),
          ([retention_ref()], term() -> {:ok, term()} | {:error, term()})
        ) :: {:ok, term()} | {:error, term()}
  def stream_retention_entries(
        ctx,
        shard_index,
        lmdb_path,
        trim_index,
        expiry_cutoff_ms,
        acc,
        emit
      )
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(lmdb_path) and is_integer(trim_index) and trim_index > 0 and
             is_integer(expiry_cutoff_ms) and expiry_cutoff_ms >= 0 and
             is_function(emit, 2) do
    stream_retention_pages(
      ctx,
      shard_index,
      lmdb_path,
      trim_index,
      expiry_cutoff_ms,
      "",
      acc,
      emit
    )
  rescue
    error -> {:error, {:query_row_retention_failed, error}}
  catch
    kind, reason -> {:error, {:query_row_retention_failed, {kind, reason}}}
  end

  def stream_retention_entries(
        _ctx,
        _shard_index,
        _lmdb_path,
        _trim_index,
        _expiry_cutoff_ms,
        _acc,
        _emit
      ),
      do: {:error, :invalid_query_row_retention_request}

  @doc false
  @spec relocate_after_rewrite(
          map(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          keyword()
        ) :: :ok | {:error, term()}
  def relocate_after_rewrite(ctx, shard_index, lmdb_path, expiry_cutoff_ms, opts \\ [])

  def relocate_after_rewrite(ctx, shard_index, lmdb_path, expiry_cutoff_ms, opts)
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(lmdb_path) and is_integer(expiry_cutoff_ms) and expiry_cutoff_ms >= 0 and
             is_list(opts) do
    relocate_pages(ctx, shard_index, lmdb_path, expiry_cutoff_ms, "", opts)
  rescue
    error -> {:error, {:query_row_relocation_failed, error}}
  catch
    kind, reason -> {:error, {:query_row_relocation_failed, {kind, reason}}}
  end

  def relocate_after_rewrite(_ctx, _shard_index, _lmdb_path, _expiry_cutoff_ms, _opts),
    do: {:error, :invalid_query_row_relocation_request}

  defp stream_retention_pages(
         ctx,
         shard_index,
         lmdb_path,
         trim_index,
         expiry_cutoff_ms,
         cursor,
         acc,
         emit
       ) do
    with {:ok, page} <- catalog_page(lmdb_path, cursor),
         {:ok, entries} <-
           retention_page_entries(
             ctx,
             shard_index,
             lmdb_path,
             page.state_keys,
             trim_index,
             expiry_cutoff_ms,
             0
           ),
         {:ok, next_acc} <- emit.(entries, acc) do
      if page.done? do
        {:ok, next_acc}
      else
        stream_retention_pages(
          ctx,
          shard_index,
          lmdb_path,
          trim_index,
          expiry_cutoff_ms,
          page.cursor,
          next_acc,
          emit
        )
      end
    end
  end

  defp retention_page_entries(
         ctx,
         shard_index,
         lmdb_path,
         state_keys,
         trim_index,
         expiry_cutoff_ms,
         attempt
       ) do
    with {:ok, rows} <- read_rows(lmdb_path, state_keys, expiry_cutoff_ms) do
      eligible = Enum.filter(rows, &retained_before?(&1.row.locator, trim_index))
      requests = Enum.map(eligible, &{&1.state_key, &1.row.locator})

      case read_stored_adaptive(ctx, shard_index, requests) do
        {:ok, stored_values} ->
          retention_entries(eligible, stored_values, [])

        {:error, _reason} when attempt == 0 ->
          with :ok <- repair_retention_locators(ctx, shard_index, lmdb_path, eligible) do
            retention_page_entries(
              ctx,
              shard_index,
              lmdb_path,
              state_keys,
              trim_index,
              expiry_cutoff_ms,
              1
            )
          else
            {:error, reason} ->
              {:error, {:query_row_retention_locator_repair_failed, reason}}
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp repair_retention_locators(_ctx, _shard_index, _lmdb_path, []), do: :ok

  defp repair_retention_locators(ctx, shard_index, lmdb_path, eligible) do
    entries =
      Enum.map(eligible, fn entry ->
        %{state_key: entry.state_key, encoded: entry.encoded, locator: entry.row.locator}
      end)

    QueryRowRelocator.relocate_many(ctx, shard_index, lmdb_path, entries)
  end

  defp relocate_pages(ctx, shard_index, lmdb_path, expiry_cutoff_ms, cursor, opts) do
    with {:ok, page} <- catalog_page(lmdb_path, cursor),
         {:ok, rows} <- read_rows(lmdb_path, page.state_keys, expiry_cutoff_ms),
         eligible = Enum.filter(rows, &apply_projection_locator?(&1.row.locator)),
         relocation_entries =
           Enum.map(eligible, fn entry ->
             %{state_key: entry.state_key, encoded: entry.encoded, locator: entry.row.locator}
           end),
         :ok <-
           QueryRowRelocator.relocate_many(
             ctx,
             shard_index,
             lmdb_path,
             relocation_entries,
             opts
           ) do
      if page.done? do
        :ok
      else
        relocate_pages(
          ctx,
          shard_index,
          lmdb_path,
          expiry_cutoff_ms,
          page.cursor,
          opts
        )
      end
    end
  end

  defp catalog_page(lmdb_path, cursor) do
    case SourceCatalog.page(lmdb_path, cursor, @page_items, @catalog_page_bytes) do
      {:ok, %{done?: false, state_keys: []}} -> {:error, :query_source_catalog_made_no_progress}
      {:ok, page} -> {:ok, page}
      {:error, reason} -> {:error, {:query_source_catalog_read_failed, reason}}
    end
  end

  defp read_rows(_lmdb_path, [], _expiry_cutoff_ms), do: {:ok, []}

  defp read_rows(lmdb_path, state_keys, expiry_cutoff_ms) do
    case LMDB.get_many_bounded(lmdb_path, state_keys, @row_page_bytes) do
      {:ok, values, _value_bytes} when length(values) == length(state_keys) ->
        decode_rows(state_keys, values, expiry_cutoff_ms, [])

      {:ok, values, _value_bytes} ->
        {:error, {:query_row_result_count_mismatch, length(state_keys), length(values)}}

      {:error, reason} ->
        {:error, {:query_row_batch_read_failed, reason}}

      invalid ->
        {:error, {:invalid_query_row_batch_read, invalid}}
    end
  end

  defp decode_rows([], [], _expiry_cutoff_ms, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_rows([_state_key | state_keys], [:not_found | values], expiry_cutoff_ms, acc),
    do: decode_rows(state_keys, values, expiry_cutoff_ms, acc)

  defp decode_rows(
         [state_key | state_keys],
         [{:ok, encoded} | values],
         expiry_cutoff_ms,
         acc
       )
       when is_binary(encoded) do
    case QueryRowCodec.decode(encoded, state_key, expiry_cutoff_ms) do
      {:ok, %QueryRow{} = row} ->
        decode_rows(state_keys, values, expiry_cutoff_ms, [
          %{state_key: state_key, encoded: encoded, row: row} | acc
        ])

      :expired ->
        decode_rows(state_keys, values, expiry_cutoff_ms, acc)

      :error ->
        {:error, {:corrupt_query_row, state_key}}
    end
  end

  defp decode_rows(_state_keys, _values, _expiry_cutoff_ms, _acc),
    do: {:error, :invalid_query_row_batch}

  defp retained_before?(
         %Locator{file_id: {:waraft_apply_projection, index}},
         trim_index
       )
       when is_integer(index) and index > 0,
       do: index < trim_index

  defp retained_before?(_locator, _trim_index), do: false

  defp apply_projection_locator?(%Locator{
         file_id: {:waraft_apply_projection, index}
       })
       when is_integer(index) and index > 0,
       do: true

  defp apply_projection_locator?(_locator), do: false

  defp read_stored_adaptive(_ctx, _shard_index, []), do: {:ok, []}

  defp read_stored_adaptive(ctx, shard_index, requests) do
    case RecordHydrator.read_stored_many(ctx, shard_index, requests,
           max_bytes: @hydration_bytes,
           timeout_ms: @hydration_timeout_ms,
           include_expired: true
         ) do
      {:error, :hydration_byte_budget_exceeded} when length(requests) > 1 ->
        {left, right} = Enum.split(requests, div(length(requests), 2))

        with {:ok, left_values} <- read_stored_adaptive(ctx, shard_index, left),
             {:ok, right_values} <- read_stored_adaptive(ctx, shard_index, right) do
          {:ok, left_values ++ right_values}
        end

      {:ok, values} when length(values) == length(requests) ->
        {:ok, values}

      {:ok, values} ->
        {:error, {:query_row_hydration_count_mismatch, length(requests), length(values)}}

      {:error, reason} ->
        {:error, {:query_row_retention_hydration_failed, reason}}

      invalid ->
        {:error, {:invalid_query_row_retention_hydration, invalid}}
    end
  end

  defp retention_entries([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp retention_entries(
         [
           %{state_key: state_key, row: %QueryRow{locator: locator}}
           | rows
         ],
         [stored | stored_values],
         acc
       )
       when is_binary(stored) do
    {:waraft_apply_projection, index} = locator.file_id
    ref = {index, state_key}

    case source_expire_at_ms(locator) do
      {:ok, expire_at_ms} ->
        entry = {state_key, stored, expire_at_ms}
        retention_entries(rows, stored_values, [{ref, entry} | acc])

      :error ->
        {:error, {:invalid_query_row_source_expiry, state_key}}
    end
  end

  defp retention_entries(
         [%{state_key: state_key, row: %QueryRow{locator: locator}} | _rows],
         [nil | _stored_values],
         _acc
       ),
       do: {:error, {:query_row_retention_value_missing, state_key, locator.file_id}}

  defp retention_entries(_rows, _stored_values, _acc),
    do: {:error, :invalid_query_row_retention_values}

  defp source_expire_at_ms(%Locator{expire_at_ms: nil}), do: {:ok, 0}

  defp source_expire_at_ms(%Locator{expire_at_ms: expire_at_ms})
       when is_integer(expire_at_ms) and expire_at_ms >= 0,
       do: {:ok, expire_at_ms}

  defp source_expire_at_ms(_locator), do: :error
end
