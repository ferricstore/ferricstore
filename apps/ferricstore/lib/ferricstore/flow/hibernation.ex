defmodule Ferricstore.Flow.Hibernation do
  @moduledoc false

  alias Ferricstore.Flow.{ClaimWaiters, Keys, LMDB, Locator}
  alias Ferricstore.Raft.ApplyContext

  @default_hot_window_ms Application.compile_env(
                           :ferricstore,
                           :flow_hibernation_hot_window_ms,
                           5 * 60 * 1_000
                         )
  @default_safety_margin_ms Application.compile_env(
                              :ferricstore,
                              :flow_hibernation_safety_margin_ms,
                              0
                            )
  @default_promote_window_ms Application.compile_env(
                               :ferricstore,
                               :flow_hibernation_promote_window_ms,
                               60 * 1_000
                             )
  @default_late_promote_window_ms Application.compile_env(
                                    :ferricstore,
                                    :flow_hibernation_late_promote_window_ms,
                                    5 * 60 * 1_000
                                  )
  @enabled true
  @enabled_key {__MODULE__, :enabled}
  @max_u64 18_446_744_073_709_551_615
  @min_i64 -9_223_372_036_854_775_808
  @max_i64 9_223_372_036_854_775_807
  @max_candidate_batch 1_000
  @max_promotion_batch 1_000
  @max_promotion_scan_pages 10_000
  @max_promotion_scan_entries 10_000
  @max_stale_due_cleanup_batches 2_048
  @stale_due_cleanup_chunk_size 256
  @max_key_bytes 65_535

  @type candidate :: %{
          required(:locator) => Locator.t(),
          required(:record) => map()
        }

  @type demote_result :: %{
          attempted: non_neg_integer(),
          cold_written: non_neg_integer(),
          hot_evicted: non_neg_integer(),
          hot_changed: non_neg_integer(),
          hot_failed: non_neg_integer()
        }

  @type promotion_row :: %{
          required(:locator) => Locator.t(),
          required(:park) => map(),
          optional(:due_key) => binary()
        }

  @type promotion_result :: %{
          attempted: non_neg_integer(),
          read: non_neg_integer(),
          installed: non_neg_integer(),
          stale: non_neg_integer(),
          failed: non_neg_integer()
        }

  def enabled? do
    case :persistent_term.get(@enabled_key, :unset) do
      :unset ->
        enabled? = Application.get_env(:ferricstore, :flow_hibernation_enabled, @enabled) == true
        :persistent_term.put(@enabled_key, enabled?)
        enabled?

      enabled? ->
        enabled? == true
    end
  end

  @spec enabled?(ApplyContext.t()) :: boolean()
  def enabled?(%ApplyContext{} = context), do: context.flow_hibernation_enabled

  def refresh_config! do
    :persistent_term.erase(@enabled_key)
    enabled?()
  end

  def hot_window_ms, do: @default_hot_window_ms
  def safety_margin_ms, do: @default_safety_margin_ms
  def promote_window_ms, do: @default_promote_window_ms
  def late_promote_window_ms, do: @default_late_promote_window_ms

  def hot_window_ms(%ApplyContext{} = context), do: context.flow_hibernation_hot_window_ms

  def safety_margin_ms(%ApplyContext{} = context),
    do: context.flow_hibernation_safety_margin_ms

  def promote_window_ms(%ApplyContext{} = context),
    do: context.flow_hibernation_promote_window_ms

  def late_promote_window_ms(%ApplyContext{} = context),
    do: context.flow_hibernation_late_promote_window_ms

  @spec maybe_schedule_claim_waiter(map()) :: :ok
  def maybe_schedule_claim_waiter(%{type: type, state: state, next_run_at_ms: due_at_ms} = record)
      when is_binary(type) and type != "" and byte_size(type) <= @max_key_bytes and
             is_binary(state) and state != "" and byte_size(state) <= @max_key_bytes and
             is_integer(due_at_ms) and due_at_ms >= 0 and due_at_ms <= @max_u64 do
    priority = Map.get(record, :priority, 0)
    partition_key = Map.get(record, :partition_key)

    if signed_i64?(priority) and optional_bounded_binary?(partition_key) and
         ClaimWaiters.any_waiters?() do
      if claim_waiters_waiting_for?(type, state, priority, partition_key) do
        ClaimWaiters.schedule_ready(type, state, priority, partition_key, due_at_ms, 1)
      end
    end

    :ok
  end

  def maybe_schedule_claim_waiter(_record), do: :ok

  defp claim_waiters_waiting_for?(type, state, priority, partition_key) do
    type
    |> ClaimWaiters.ready_keys(state, priority, partition_key)
    |> Enum.any?(&ClaimWaiters.has_live_waiter?/1)
  end

  @spec demotable?(map(), non_neg_integer(), keyword()) :: boolean()
  def demotable?(record, now_ms, opts \\ [])

  def demotable?(record, now_ms, opts)
      when is_map(record) and is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_u64 and
             is_list(opts) do
    with true <- valid_keyword_options?(opts, [:hot_window_ms, :safety_margin_ms]),
         hot_window_ms
         when is_integer(hot_window_ms) and hot_window_ms >= 0 and hot_window_ms <= @max_u64 <-
           Keyword.get(opts, :hot_window_ms, @default_hot_window_ms),
         safety_margin_ms
         when is_integer(safety_margin_ms) and safety_margin_ms >= 0 and
                safety_margin_ms <= @max_u64 <-
           Keyword.get(opts, :safety_margin_ms, @default_safety_margin_ms),
         due_at_ms when is_integer(due_at_ms) and due_at_ms >= 0 and due_at_ms <= @max_u64 <-
           Map.get(record, :next_run_at_ms) do
      waiting?(record) and unleased?(record) and not terminal?(record) and
        due_at_ms > now_ms + hot_window_ms + safety_margin_ms
    else
      _invalid -> false
    end
  end

  def demotable?(_record, _now_ms, _opts), do: false

  @spec demote_candidates([candidate()], keyword()) ::
          {:ok, demote_result()} | {:error, term(), demote_result()}
  def demote_candidates(candidates, opts)
      when is_list(candidates) and is_list(opts) do
    base = empty_demote_result()

    with true <- valid_keyword_options?(opts, [:write_cold_fun, :evict_hot_fun]),
         {:ok, write_cold_fun} <- required_callback(opts, :write_cold_fun, 1),
         {:ok, evict_hot_fun} <- required_callback(opts, :evict_hot_fun, 1),
         {:ok, bounded_candidates} <- bounded_batch(candidates, @max_candidate_batch),
         :ok <- validate_demotion_candidates(bounded_candidates) do
      do_demote_candidates(bounded_candidates, write_cold_fun, evict_hot_fun)
    else
      false -> {:error, :invalid_options, base}
      {:error, reason} -> {:error, reason, base}
    end
  end

  def demote_candidates(_candidates, _opts),
    do: {:error, :invalid_arguments, empty_demote_result()}

  defp do_demote_candidates([], _write_cold_fun, _evict_hot_fun),
    do: {:ok, empty_demote_result()}

  defp do_demote_candidates(candidates, write_cold_fun, evict_hot_fun) do
    attempted = length(candidates)
    base = %{empty_demote_result() | attempted: attempted}

    with {:ok, ops} <- demotion_batch_ops(candidates) do
      write_demoted_candidates(
        candidates,
        ops,
        base,
        write_cold_fun,
        evict_hot_fun
      )
    else
      {:error, reason} -> {:error, reason, base}
    end
  end

  defp write_demoted_candidates(candidates, ops, base, write_cold_fun, evict_hot_fun) do
    attempted = base.attempted

    case invoke_callback(write_cold_fun, [ops], :write_cold_fun) do
      {:ok, :ok} ->
        {result, first_error} =
          Enum.reduce(
            candidates,
            {%{base | cold_written: attempted}, nil},
            fn candidate, {acc, first_error} ->
              case invoke_callback(evict_hot_fun, [candidate.locator], :evict_hot_fun) do
                {:ok, :ok} ->
                  {%{acc | hot_evicted: acc.hot_evicted + 1}, first_error}

                {:ok, {:error, :changed}} ->
                  {%{acc | hot_changed: acc.hot_changed + 1}, first_error}

                {:ok, {:error, reason}} ->
                  {
                    %{acc | hot_failed: acc.hot_failed + 1},
                    first_error || {:evict_hot_failed, reason}
                  }

                {:ok, other} ->
                  {
                    %{acc | hot_failed: acc.hot_failed + 1},
                    first_error || {:invalid_evict_hot_result, other}
                  }

                {:error, reason} ->
                  {
                    %{acc | hot_failed: acc.hot_failed + 1},
                    first_error || reason
                  }
              end
            end
          )

        if first_error, do: {:error, first_error, result}, else: {:ok, result}

      {:ok, {:error, reason}} ->
        {:error, reason, base}

      {:ok, other} ->
        {:error, {:invalid_write_cold_result, other}, base}

      {:error, reason} ->
        {:error, reason, base}
    end
  end

  @spec demotion_ops(candidate()) :: list()
  def demotion_ops(candidate) do
    case demotion_ops_result(candidate) do
      {:ok, ops} -> ops
      {:error, _reason} -> raise ArgumentError, "invalid Flow hibernation demotion candidate"
    end
  end

  @spec demotion_ops_result(candidate()) :: {:ok, list()} | {:error, term()}
  def demotion_ops_result(candidate) do
    with true <- valid_demotion_candidate?(candidate),
         {:ok, ops} <- build_demotion_ops(candidate) do
      {:ok, ops}
    else
      false -> {:error, :invalid_candidate}
      {:error, _reason} = error -> error
    end
  rescue
    error in [ArgumentError, KeyError] ->
      {:error, {:invalid_candidate, Exception.message(error)}}
  end

  defp build_demotion_ops(
         %{locator: %Locator{kind: :state} = locator, record: record} = candidate
       ) do
    due_at_ms = Map.fetch!(record, :next_run_at_ms)
    version = Map.get(record, :version, locator.version)
    park_key = park_key(locator, record)
    state_value = Map.get(candidate, :state_value)

    park =
      LMDB.encode_cold_park(locator,
        due_at_ms: due_at_ms,
        type: Map.get(record, :type),
        state: Map.get(record, :state),
        partition_key: Map.get(record, :partition_key),
        state_key: Map.get(record, :state_key),
        priority: Map.get(record, :priority, 0),
        lease_until_ms: Map.get(record, :lease_deadline_ms),
        fencing_token: Map.get(record, :fencing_token),
        retention_at_ms: Map.get(record, :terminal_retention_until_ms),
        value_refs_digest: value_refs_digest(Map.get(record, :value_refs, %{})),
        state_value: if(is_binary(state_value), do: state_value)
      )

    due_key =
      LMDB.cold_due_key(
        type: Map.fetch!(record, :type),
        state: Map.fetch!(record, :state),
        partition_key: Map.get(record, :partition_key, ""),
        priority: Map.get(record, :priority, 0),
        due_at_ms: due_at_ms,
        flow_id: locator.flow_id,
        version: version
      )

    with {:ok, active_index_ops} <- demotion_active_index_ops(candidate) do
      ops = [
        {:put, park_key, park},
        {:put, due_key, park_key},
        {:put, LMDB.cold_by_segment_key(locator), park_key}
        | active_index_ops
      ]

      if valid_lmdb_op_keys?(ops),
        do: {:ok, ops},
        else: {:error, :generated_key_too_large}
    end
  end

  @spec rebuild_cold_ops([candidate()], non_neg_integer(), keyword()) :: list()
  def rebuild_cold_ops(candidates, now_ms, opts \\ []) do
    case rebuild_cold_ops_result(candidates, now_ms, opts) do
      {:ok, ops} -> ops
      {:error, _reason} -> raise ArgumentError, "invalid Flow hibernation rebuild candidates"
    end
  end

  @spec rebuild_cold_ops_result([candidate()], non_neg_integer(), keyword()) ::
          {:ok, list()} | {:error, term()}
  def rebuild_cold_ops_result(candidates, now_ms, opts \\ [])

  def rebuild_cold_ops_result(candidates, now_ms, opts)
      when is_list(candidates) and is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_u64 and
             is_list(opts) do
    with true <- valid_keyword_options?(opts, [:hot_window_ms, :safety_margin_ms]),
         {:ok, bounded_candidates} <- bounded_batch(candidates, @max_candidate_batch),
         :ok <- validate_demotion_candidates(bounded_candidates) do
      bounded_candidates
      |> Enum.filter(fn %{record: record} -> demotable?(record, now_ms, opts) end)
      |> demotion_batch_ops()
    else
      false -> {:error, :invalid_options}
      {:error, _reason} = error -> error
    end
  end

  def rebuild_cold_ops_result(_candidates, _now_ms, _opts), do: {:error, :invalid_arguments}

  @type promotion_scan_cursor :: %{
          bucket_ms: non_neg_integer(),
          after_key: binary() | nil
        }

  @type promotion_scan_result(acc) :: %{
          cursor: promotion_scan_cursor(),
          acc: acc,
          scanned_pages: non_neg_integer(),
          scanned_entries: non_neg_integer(),
          wrapped?: boolean(),
          halted?: boolean()
        }

  @spec reduce_promotion_buckets(
          non_neg_integer(),
          non_neg_integer(),
          promotion_scan_cursor() | nil,
          keyword(),
          acc,
          (binary(), binary() | nil, pos_integer() ->
             {:ok, [{binary(), binary()}]} | {:error, term()}),
          (term(), acc -> {:cont, acc} | {:halt, acc})
        ) ::
          {:ok, promotion_scan_result(acc)}
          | {:error, term(), promotion_scan_result(acc)}
        when acc: term()
  def reduce_promotion_buckets(
        start_ms,
        horizon_ms,
        cursor,
        opts,
        acc,
        scan_fun,
        reduce_fun
      )
      when is_integer(start_ms) and start_ms >= 0 and start_ms <= @max_u64 and
             is_integer(horizon_ms) and horizon_ms >= start_ms and horizon_ms <= @max_u64 and
             is_list(opts) and is_function(scan_fun, 3) and is_function(reduce_fun, 2) do
    with true <- valid_keyword_options?(opts, [:bucket_ms, :max_pages, :max_entries]),
         bucket_ms
         when is_integer(bucket_ms) and bucket_ms > 0 and bucket_ms <= @max_u64 <-
           Keyword.get(opts, :bucket_ms, 60_000),
         max_pages
         when is_integer(max_pages) and max_pages > 0 and
                max_pages <= @max_promotion_scan_pages <-
           Keyword.get(opts, :max_pages),
         max_entries
         when is_integer(max_entries) and max_entries > 0 and
                max_entries <= @max_promotion_scan_entries <-
           Keyword.get(opts, :max_entries) do
      first_bucket = LMDB.cold_due_bucket_ms(start_ms, bucket_ms)
      last_bucket = LMDB.cold_due_bucket_ms(horizon_ms, bucket_ms)
      cursor = normalize_promotion_scan_cursor(cursor, first_bucket, last_bucket, bucket_ms)
      result = promotion_scan_result(cursor, acc)

      do_reduce_promotion_buckets(
        first_bucket,
        last_bucket,
        bucket_ms,
        max_pages,
        max_entries,
        scan_fun,
        reduce_fun,
        result
      )
    else
      _invalid ->
        fallback_cursor = %{bucket_ms: 0, after_key: nil}

        {:error, :invalid_promotion_scan, promotion_scan_result(fallback_cursor, acc)}
    end
  end

  def reduce_promotion_buckets(
        _start_ms,
        _horizon_ms,
        _cursor,
        _opts,
        acc,
        _scan_fun,
        _reduce_fun
      ) do
    fallback_cursor = %{bucket_ms: 0, after_key: nil}
    {:error, :invalid_promotion_scan, promotion_scan_result(fallback_cursor, acc)}
  end

  defp do_reduce_promotion_buckets(
         first_bucket,
         last_bucket,
         bucket_ms,
         max_pages,
         max_entries,
         scan_fun,
         reduce_fun,
         result
       ) do
    if result.scanned_pages >= max_pages or result.scanned_entries >= max_entries do
      {:ok, result}
    else
      reduce_promotion_bucket_page(
        first_bucket,
        last_bucket,
        bucket_ms,
        max_pages,
        max_entries,
        scan_fun,
        reduce_fun,
        result
      )
    end
  end

  defp reduce_promotion_bucket_page(
         first_bucket,
         last_bucket,
         bucket_ms,
         max_pages,
         max_entries,
         scan_fun,
         reduce_fun,
         %{cursor: cursor} = result
       ) do
    prefix = LMDB.cold_due_bucket_prefix(cursor.bucket_ms)
    remaining_entries = max_entries - result.scanned_entries

    case invoke_callback(
           scan_fun,
           [prefix, cursor.after_key, remaining_entries],
           :promotion_scan_fun
         ) do
      {:ok, {:ok, entries}} ->
        with :ok <-
               validate_promotion_scan_page(
                 entries,
                 prefix,
                 cursor.after_key,
                 remaining_entries
               ),
             {:ok, reduced} <- reduce_promotion_scan_entries(entries, result.acc, reduce_fun) do
          result =
            result
            |> Map.put(:acc, reduced.acc)
            |> Map.put(:scanned_pages, result.scanned_pages + 1)
            |> Map.put(:scanned_entries, result.scanned_entries + reduced.processed)
            |> maybe_advance_promotion_entry_cursor(reduced.last_key)

          cond do
            reduced.halted? ->
              {:ok, %{result | halted?: true}}

            length(entries) < remaining_entries ->
              case advance_promotion_bucket(
                     result.cursor.bucket_ms,
                     first_bucket,
                     last_bucket,
                     bucket_ms
                   ) do
                {:next, next_bucket} ->
                  do_reduce_promotion_buckets(
                    first_bucket,
                    last_bucket,
                    bucket_ms,
                    max_pages,
                    max_entries,
                    scan_fun,
                    reduce_fun,
                    %{result | cursor: %{bucket_ms: next_bucket, after_key: nil}}
                  )

                {:wrapped, next_bucket} ->
                  {:ok,
                   %{
                     result
                     | cursor: %{bucket_ms: next_bucket, after_key: nil},
                       wrapped?: true
                   }}
              end

            true ->
              {:ok, result}
          end
        else
          {:error, reason} -> {:error, reason, result}
        end

      {:ok, {:error, reason}} ->
        {:error, {:promotion_scan_failed, reason}, result}

      {:ok, _invalid} ->
        {:error, :invalid_promotion_scan_page, result}

      {:error, reason} ->
        {:error, reason, result}
    end
  end

  defp validate_promotion_scan_page(entries, prefix, after_key, limit)
       when is_list(entries) and length(entries) <= limit do
    entries
    |> Enum.reduce_while({:ok, after_key}, fn
      {key, value}, {:ok, previous}
      when is_binary(key) and is_binary(value) and byte_size(key) <= @max_key_bytes ->
        if String.starts_with?(key, prefix <> ":") and
             (is_nil(previous) or key > previous) do
          {:cont, {:ok, key}}
        else
          {:halt, {:error, :invalid_promotion_scan_page}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_promotion_scan_page}}
    end)
    |> case do
      {:ok, _last_key} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_promotion_scan_page(_entries, _prefix, _after_key, _limit),
    do: {:error, :invalid_promotion_scan_page}

  defp reduce_promotion_scan_entries(entries, acc, reduce_fun) do
    entries
    |> Enum.reduce_while(
      {:ok, %{acc: acc, processed: 0, last_key: nil, halted?: false}},
      fn {key, _value} = entry, {:ok, reduced} ->
        case reduce_fun.(entry, reduced.acc) do
          {:cont, next_acc} ->
            {:cont,
             {:ok,
              %{
                reduced
                | acc: next_acc,
                  processed: reduced.processed + 1,
                  last_key: key
              }}}

          {:halt, next_acc} ->
            {:halt,
             {:ok,
              %{
                reduced
                | acc: next_acc,
                  processed: reduced.processed + 1,
                  last_key: key,
                  halted?: true
              }}}

          _invalid ->
            {:halt, {:error, :invalid_promotion_scan_reducer_result}}
        end
      end
    )
  rescue
    _error -> {:error, :promotion_scan_reducer_failed}
  catch
    _kind, _reason -> {:error, :promotion_scan_reducer_failed}
  end

  defp maybe_advance_promotion_entry_cursor(result, nil), do: result

  defp maybe_advance_promotion_entry_cursor(result, last_key),
    do: %{result | cursor: %{result.cursor | after_key: last_key}}

  defp advance_promotion_bucket(current, _first, last, bucket_ms) when current < last,
    do: {:next, current + bucket_ms}

  defp advance_promotion_bucket(_current, first, _last, _bucket_ms),
    do: {:wrapped, first}

  defp normalize_promotion_scan_cursor(nil, first, _last, _bucket_ms),
    do: %{bucket_ms: first, after_key: nil}

  defp normalize_promotion_scan_cursor(
         %{bucket_ms: bucket, after_key: after_key},
         first,
         last,
         bucket_ms
       )
       when is_integer(bucket) and bucket >= first and bucket <= last and
              rem(bucket, bucket_ms) == 0 and
              (is_nil(after_key) or is_binary(after_key)) do
    prefix = LMDB.cold_due_bucket_prefix(bucket)

    if is_nil(after_key) or
         (byte_size(after_key) <= @max_key_bytes and
            String.starts_with?(after_key, prefix <> ":")) do
      %{bucket_ms: bucket, after_key: after_key}
    else
      %{bucket_ms: first, after_key: nil}
    end
  end

  defp normalize_promotion_scan_cursor(_cursor, first, _last, _bucket_ms),
    do: %{bucket_ms: first, after_key: nil}

  defp promotion_scan_result(cursor, acc) do
    %{
      cursor: cursor,
      acc: acc,
      scanned_pages: 0,
      scanned_entries: 0,
      wrapped?: false,
      halted?: false
    }
  end

  @spec hot_index_keys(map(), keyword()) :: [binary()]
  def hot_index_keys(record, opts \\ [])

  def hot_index_keys(record, opts) when is_map(record) and is_list(opts) do
    if valid_keyword_options?(opts, [:due_any?]) and valid_hot_index_record?(record) and
         is_boolean(Keyword.get(opts, :due_any?, true)) do
      build_hot_index_keys(record, opts)
    else
      []
    end
  end

  def hot_index_keys(_record, _opts), do: []

  defp build_hot_index_keys(record, opts) do
    id = Map.get(record, :id)
    type = Map.get(record, :type)
    flow_state = Map.get(record, :state)
    partition_key = Map.get(record, :partition_key)
    priority = Map.get(record, :priority, 0)

    []
    |> maybe_index_key(
      type && flow_state && Keys.state_index_key(type, flow_state, partition_key)
    )
    |> maybe_due_index_key(record, type, flow_state, priority, partition_key)
    |> maybe_due_any_index_key(
      record,
      type,
      priority,
      partition_key,
      Keyword.get(opts, :due_any?, true)
    )
    |> maybe_running_index_keys(record, type, partition_key)
    |> maybe_metadata_index_key(:parent, Map.get(record, :parent_flow_id), partition_key, id)
    |> maybe_metadata_index_key(:root, Map.get(record, :root_flow_id), partition_key, id)
    |> maybe_metadata_index_key(:correlation, Map.get(record, :correlation_id), partition_key, id)
    |> Enum.uniq()
    |> then(fn keys ->
      if Enum.all?(keys, &(byte_size(&1) <= @max_key_bytes)), do: keys, else: []
    end)
  end

  @spec cleanup_ops(promotion_row()) :: list()
  def cleanup_ops(%{locator: %Locator{}} = row), do: promotion_cleanup_ops(row)

  @type stale_due_cleanup_batch ::
          [
            {:compare, binary(), binary()}
            | {:compare_missing, binary()}
            | {:delete, binary()}
          ]

  @spec stale_due_cleanup_batch(binary(), binary(), binary() | :missing) ::
          {:ok, stale_due_cleanup_batch()} | {:error, :invalid_stale_due_cleanup}
  def stale_due_cleanup_batch(due_key, park_key, park_snapshot)
      when is_binary(due_key) and due_key != "" and byte_size(due_key) <= @max_key_bytes and
             is_binary(park_key) and park_key != "" and byte_size(park_key) <= @max_key_bytes and
             due_key != park_key and is_binary(park_snapshot) do
    {:ok,
     [
       {:compare, due_key, park_key},
       {:compare, park_key, park_snapshot},
       {:delete, due_key}
     ]}
  end

  def stale_due_cleanup_batch(due_key, park_key, :missing)
      when is_binary(due_key) and due_key != "" and byte_size(due_key) <= @max_key_bytes and
             is_binary(park_key) and park_key != "" and byte_size(park_key) <= @max_key_bytes and
             due_key != park_key do
    {:ok,
     [
       {:compare, due_key, park_key},
       {:compare_missing, park_key},
       {:delete, due_key}
     ]}
  end

  def stale_due_cleanup_batch(_due_key, _park_key, _park_snapshot),
    do: {:error, :invalid_stale_due_cleanup}

  @spec cleanup_stale_due_batches(binary(), [stale_due_cleanup_batch()]) ::
          :ok | {:error, term()}
  def cleanup_stale_due_batches(path, batches)
      when is_binary(path) and is_list(batches) and
             length(batches) <= @max_stale_due_cleanup_batches do
    if Enum.all?(batches, &valid_stale_due_cleanup_batch?/1) do
      batches
      |> Enum.chunk_every(@stale_due_cleanup_chunk_size)
      |> Enum.reduce_while(:ok, fn chunk, :ok ->
        case cleanup_stale_due_batch_chunk(path, chunk) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    else
      {:error, :invalid_stale_due_cleanup_batches}
    end
  rescue
    _error -> {:error, :stale_due_cleanup_failed}
  catch
    _kind, _reason -> {:error, :stale_due_cleanup_failed}
  end

  def cleanup_stale_due_batches(_path, _batches),
    do: {:error, :invalid_stale_due_cleanup_batches}

  defp cleanup_stale_due_batch_chunk(_path, []), do: :ok

  defp cleanup_stale_due_batch_chunk(path, [batch]) do
    case LMDB.write_batch(path, batch) do
      :ok ->
        :ok

      {:error, {:compare_failed, key}} when is_binary(key) ->
        if stale_due_cleanup_compare_key?(batch, key),
          do: :ok,
          else: {:error, {:unexpected_stale_due_compare_failure, key}}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_stale_due_cleanup_result, invalid}}
    end
  end

  defp cleanup_stale_due_batch_chunk(path, batches) do
    case LMDB.write_batch(path, Enum.concat(batches)) do
      :ok ->
        :ok

      {:error, {:compare_failed, _key}} ->
        {left, right} = Enum.split(batches, div(length(batches), 2))

        with :ok <- cleanup_stale_due_batch_chunk(path, left),
             :ok <- cleanup_stale_due_batch_chunk(path, right) do
          :ok
        end

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_stale_due_cleanup_result, invalid}}
    end
  end

  defp valid_stale_due_cleanup_batch?([
         {:compare, due_key, park_key},
         {:compare, park_key, park_snapshot},
         {:delete, due_key}
       ]) do
    valid_stale_due_cleanup_values?(due_key, park_key) and is_binary(park_snapshot)
  end

  defp valid_stale_due_cleanup_batch?([
         {:compare, due_key, park_key},
         {:compare_missing, park_key},
         {:delete, due_key}
       ]) do
    valid_stale_due_cleanup_values?(due_key, park_key)
  end

  defp valid_stale_due_cleanup_batch?(_batch), do: false

  defp valid_stale_due_cleanup_values?(due_key, park_key) do
    is_binary(due_key) and due_key != "" and byte_size(due_key) <= @max_key_bytes and
      is_binary(park_key) and park_key != "" and byte_size(park_key) <= @max_key_bytes and
      due_key != park_key
  end

  defp stale_due_cleanup_compare_key?(batch, key) do
    Enum.any?(batch, fn
      {:compare, ^key, _expected} -> true
      {:compare_missing, ^key} -> true
      _op -> false
    end)
  end

  @spec promote_candidates([promotion_row()], keyword()) ::
          {:ok, promotion_result()} | {:error, term(), promotion_result()}
  def promote_candidates(rows, opts) when is_list(rows) and is_list(opts) do
    base = empty_promotion_result()

    with true <-
           valid_keyword_options?(opts, [
             :read_state_fun,
             :install_hot_fun,
             :cleanup_cold_fun,
             :validate_fun,
             :limit
           ]),
         {:ok, read_state_fun} <- required_callback(opts, :read_state_fun, 1),
         {:ok, install_hot_fun} <- required_callback(opts, :install_hot_fun, 2),
         {:ok, cleanup_cold_fun} <-
           optional_callback(opts, :cleanup_cold_fun, 1, fn _ops -> :ok end),
         {:ok, validate_fun} <-
           optional_callback(opts, :validate_fun, 2, &valid_promotion_record?/2),
         {:ok, bounded_rows} <- bounded_promotion_rows(rows, opts),
         :ok <- validate_promotion_rows(bounded_rows) do
      do_promote_candidates(
        bounded_rows,
        read_state_fun,
        install_hot_fun,
        cleanup_cold_fun,
        validate_fun
      )
    else
      false -> {:error, :invalid_options, base}
      {:error, reason} -> {:error, reason, base}
    end
  end

  def promote_candidates(_rows, _opts),
    do: {:error, :invalid_arguments, empty_promotion_result()}

  defp do_promote_candidates(
         rows,
         read_state_fun,
         install_hot_fun,
         cleanup_cold_fun,
         validate_fun
       ) do
    {reversed_cleanup_ops, result, first_error} =
      Enum.reduce(rows, {[], empty_promotion_result(), nil}, fn row, acc ->
        promote_candidate(
          row,
          acc,
          read_state_fun,
          install_hot_fun,
          validate_fun
        )
      end)

    cleanup_result =
      case reversed_cleanup_ops do
        [] ->
          :ok

        [_ | _] ->
          cleanup_cold_fun
          |> invoke_callback([Enum.reverse(reversed_cleanup_ops)], :cleanup_cold_fun)
          |> normalize_cleanup_result()
      end

    case cleanup_result do
      :ok when is_nil(first_error) -> {:ok, result}
      :ok -> {:error, first_error, result}
      {:error, reason} -> {:error, reason, result}
    end
  end

  defp promote_candidate(
         row,
         {cleanup_ops, result, first_error},
         read_state_fun,
         install_hot_fun,
         validate_fun
       ) do
    result = %{result | attempted: result.attempted + 1}

    case invoke_callback(read_state_fun, [row.locator], :read_state_fun) do
      {:ok, {:ok, record}} when is_map(record) ->
        result = %{result | read: result.read + 1}

        promote_read_record(
          row,
          record,
          {cleanup_ops, result, first_error},
          install_hot_fun,
          validate_fun
        )

      {:ok, {:error, reason}} ->
        promotion_failure(
          {cleanup_ops, result, first_error},
          {:read_state_failed, reason}
        )

      {:ok, other} ->
        promotion_failure(
          {cleanup_ops, result, first_error},
          {:invalid_read_state_result, other}
        )

      {:error, reason} ->
        promotion_failure({cleanup_ops, result, first_error}, reason)
    end
  end

  defp promote_read_record(
         row,
         record,
         acc,
         install_hot_fun,
         validate_fun
       ) do
    case invoke_callback(validate_fun, [record, row], :validate_fun) do
      {:ok, true} -> install_promoted_record(row, record, acc, install_hot_fun)
      {:ok, false} -> stale_promotion(row, acc)
      {:ok, other} -> promotion_failure(acc, {:invalid_validate_result, other})
      {:error, reason} -> promotion_failure(acc, reason)
    end
  end

  defp install_promoted_record(
         row,
         record,
         {cleanup_ops, result, first_error} = acc,
         install_hot_fun
       ) do
    case invoke_callback(install_hot_fun, [row.locator, record], :install_hot_fun) do
      {:ok, :ok} ->
        {
          prepend_promotion_cleanup(row, cleanup_ops),
          %{result | installed: result.installed + 1},
          first_error
        }

      {:ok, {:error, reason}} ->
        promotion_failure(acc, {:install_hot_failed, reason})

      {:ok, other} ->
        promotion_failure(acc, {:invalid_install_hot_result, other})

      {:error, reason} ->
        promotion_failure(acc, reason)
    end
  end

  defp stale_promotion(row, {cleanup_ops, result, first_error}) do
    {
      prepend_promotion_cleanup(row, cleanup_ops),
      %{result | stale: result.stale + 1},
      first_error
    }
  end

  defp promotion_failure({cleanup_ops, result, first_error}, reason) do
    {
      cleanup_ops,
      %{result | failed: result.failed + 1},
      first_error || reason
    }
  end

  defp prepend_promotion_cleanup(row, cleanup_ops),
    do: Enum.reverse(promotion_cleanup_ops(row), cleanup_ops)

  defp normalize_cleanup_result({:ok, :ok}), do: :ok
  defp normalize_cleanup_result({:ok, {:error, reason}}), do: {:error, reason}
  defp normalize_cleanup_result({:ok, other}), do: {:error, {:invalid_cleanup_cold_result, other}}
  defp normalize_cleanup_result({:error, reason}), do: {:error, reason}

  @spec fetch_or_promote(binary(), keyword()) ::
          {:ok, :hot, map()}
          | {:ok, :cold_promoted, map()}
          | :not_found
          | {:error, term()}
  def fetch_or_promote(flow_id, opts)
      when is_binary(flow_id) and is_list(opts) do
    with true <- flow_id != "" and byte_size(flow_id) <= @max_key_bytes,
         true <-
           valid_keyword_options?(opts, [
             :fetch_hot_fun,
             :fetch_cold_fun,
             :read_state_fun,
             :install_hot_fun,
             :cleanup_cold_fun,
             :validate_fun
           ]),
         {:ok, fetch_hot_fun} <- required_callback(opts, :fetch_hot_fun, 1),
         {:ok, fetch_cold_fun} <- required_callback(opts, :fetch_cold_fun, 1),
         {:ok, read_state_fun} <- required_callback(opts, :read_state_fun, 1),
         {:ok, install_hot_fun} <- required_callback(opts, :install_hot_fun, 2),
         {:ok, cleanup_cold_fun} <-
           optional_callback(opts, :cleanup_cold_fun, 1, fn _ops -> :ok end),
         {:ok, validate_fun} <-
           optional_callback(opts, :validate_fun, 2, &valid_promotion_record?/2) do
      do_fetch_or_promote(
        flow_id,
        fetch_hot_fun,
        fetch_cold_fun,
        read_state_fun,
        install_hot_fun,
        cleanup_cold_fun,
        validate_fun
      )
    else
      false ->
        if flow_id == "" or byte_size(flow_id) > @max_key_bytes,
          do: {:error, :invalid_flow_id},
          else: {:error, :invalid_options}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def fetch_or_promote(_flow_id, _opts), do: {:error, :invalid_arguments}

  defp do_fetch_or_promote(
         flow_id,
         fetch_hot_fun,
         fetch_cold_fun,
         read_state_fun,
         install_hot_fun,
         cleanup_cold_fun,
         validate_fun
       ) do
    case fetch_hot_record(fetch_hot_fun, flow_id) do
      {:ok, record} ->
        {:ok, :hot, record}

      :not_found ->
        with_promotion_lock(flow_id, fn ->
          case fetch_hot_record(fetch_hot_fun, flow_id) do
            {:ok, record} ->
              {:ok, :hot, record}

            :not_found ->
              promote_cold_point_lookup(
                flow_id,
                fetch_cold_fun,
                read_state_fun,
                install_hot_fun,
                cleanup_cold_fun,
                validate_fun
              )

            {:error, _reason} = error ->
              error
          end
        end)

      {:error, _reason} = error ->
        error
    end
  end

  defp fetch_hot_record(fetch_hot_fun, flow_id) do
    case invoke_callback(fetch_hot_fun, [flow_id], :fetch_hot_fun) do
      {:ok, {:ok, record}} when is_map(record) -> {:ok, record}
      {:ok, :not_found} -> :not_found
      {:ok, {:error, _reason} = error} -> error
      {:ok, other} -> {:error, {:invalid_fetch_hot_result, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp with_promotion_lock(flow_id, fun) do
    lock = {{__MODULE__, :promotion, flow_id}, self()}

    case :global.trans(lock, fun, [node()]) do
      :aborted -> {:error, :promotion_lock_busy}
      {:aborted, reason} -> {:error, {:promotion_lock_failed, reason}}
      result -> result
    end
  catch
    :exit, reason -> {:error, {:promotion_lock_failed, reason}}
  end

  defp valid_promotion_record?(record, %{locator: %Locator{} = locator, park: park}) do
    with true <- is_map(record) and is_map(park) and Locator.valid?(locator),
         {:ok, version} <- Map.fetch(record, :version),
         true <- u64?(version) and version == locator.version,
         true <- Map.get(record, :id) == locator.flow_id,
         true <- valid_required_binary?(Map.get(record, :type)),
         true <- valid_required_binary?(Map.get(record, :state)),
         due_at_ms when is_integer(due_at_ms) <- Map.get(record, :next_run_at_ms),
         true <- u64?(due_at_ms) and due_at_ms == Map.get(park, :due_at_ms),
         true <- waiting?(record) and unleased?(record) and not terminal?(record),
         true <- promotion_park_matches_record?(park, record),
         true <- Map.get(park, :locator, locator) == locator do
      true
    else
      _invalid -> false
    end
  end

  defp promotion_park_matches_record?(park, record) do
    Enum.all?([:type, :state, :partition_key, :state_key, :priority], fn key ->
      not Map.has_key?(park, key) or Map.get(park, key) == Map.get(record, key)
    end)
  end

  defp promote_cold_point_lookup(
         flow_id,
         fetch_cold_fun,
         read_state_fun,
         install_hot_fun,
         cleanup_cold_fun,
         validate_fun
       ) do
    case invoke_callback(fetch_cold_fun, [flow_id], :fetch_cold_fun) do
      {:ok, {:ok, row}} ->
        if valid_promotion_row?(row) do
          promote_cold_point_row(
            row,
            read_state_fun,
            install_hot_fun,
            cleanup_cold_fun,
            validate_fun
          )
        else
          {:error, :invalid_promotion_row}
        end

      {:ok, :not_found} ->
        :not_found

      {:ok, {:error, _reason} = error} ->
        error

      {:ok, other} ->
        {:error, {:invalid_fetch_cold_result, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp promote_cold_point_row(
         %{locator: %Locator{} = locator} = row,
         read_state_fun,
         install_hot_fun,
         cleanup_cold_fun,
         validate_fun
       ) do
    case invoke_callback(read_state_fun, [locator], :read_state_fun) do
      {:ok, {:ok, record}} when is_map(record) ->
        validate_cold_point_record(
          row,
          record,
          install_hot_fun,
          cleanup_cold_fun,
          validate_fun
        )

      {:ok, {:error, _reason} = error} ->
        error

      {:ok, other} ->
        {:error, {:invalid_read_state_result, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_cold_point_record(
         row,
         record,
         install_hot_fun,
         cleanup_cold_fun,
         validate_fun
       ) do
    case invoke_callback(validate_fun, [record, row], :validate_fun) do
      {:ok, true} ->
        install_cold_point_record(row, record, install_hot_fun, cleanup_cold_fun)

      {:ok, false} ->
        case cleanup_promotion_row(row, cleanup_cold_fun) do
          :ok -> {:error, :stale_cold_locator}
          {:error, reason} -> {:error, {:stale_cold_cleanup_failed, reason}}
        end

      {:ok, other} ->
        {:error, {:invalid_validate_result, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp install_cold_point_record(
         %{locator: %Locator{} = locator} = row,
         record,
         install_hot_fun,
         cleanup_cold_fun
       ) do
    case invoke_callback(install_hot_fun, [locator, record], :install_hot_fun) do
      {:ok, :ok} ->
        case cleanup_promotion_row(row, cleanup_cold_fun) do
          :ok -> {:ok, :cold_promoted, record}
          {:error, reason} -> {:error, reason}
        end

      {:ok, {:error, _reason} = error} ->
        error

      {:ok, other} ->
        {:error, {:invalid_install_hot_result, other}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp cleanup_promotion_row(row, cleanup_cold_fun) do
    cleanup_cold_fun
    |> invoke_callback([promotion_cleanup_ops(row)], :cleanup_cold_fun)
    |> normalize_cleanup_result()
  end

  defp promotion_cleanup_ops(%{locator: %Locator{} = locator} = row) do
    park_key = Map.get(row, :park_key, LMDB.cold_park_key(locator.flow_id))

    [
      {:delete, park_key},
      {:delete, LMDB.cold_by_segment_key(locator)}
    ] ++
      case Map.get(row, :due_key) do
        due_key when is_binary(due_key) -> [{:delete, due_key}]
        _ -> []
      end
  end

  @spec relocate_cold_row(promotion_row(), keyword() | map()) ::
          {:ok, promotion_row()} | {:error, :bad_locator | :invalid_cold_row}
  def relocate_cold_row(%{locator: %Locator{} = locator, park: park} = row, attrs) do
    with true <- valid_promotion_row?(row),
         true <- valid_relocation_attrs?(attrs),
         {:ok, relocated} <- Locator.relocate(locator, attrs) do
      {:ok, %{row | locator: relocated, park: Map.put(park, :locator, relocated)}}
    else
      false ->
        if valid_promotion_row?(row),
          do: {:error, :bad_locator},
          else: {:error, :invalid_cold_row}

      {:error, :bad_locator} = error ->
        error
    end
  end

  def relocate_cold_row(_row, _attrs), do: {:error, :invalid_cold_row}

  @spec cold_compaction_ops(promotion_row(), promotion_row()) :: {:ok, list()} | {:error, term()}
  def cold_compaction_ops(old_row, new_row) do
    with true <- valid_promotion_row?(old_row) and valid_promotion_row?(new_row),
         %{locator: %Locator{} = old_locator, park: old_park} <- old_row,
         %{locator: %Locator{} = new_locator, park: new_park} <- new_row,
         true <- Locator.same_logical_record?(old_locator, new_locator),
         {:ok, park_key} <- cold_compaction_park_key(old_row, new_row, new_locator) do
      old_reverse_key = LMDB.cold_by_segment_key(old_locator)

      {:ok,
       [
         {:compare, park_key, LMDB.encode_cold_park(old_locator, Map.delete(old_park, :locator))},
         {:compare, old_reverse_key, park_key},
         {:delete, old_reverse_key},
         {:put, LMDB.cold_by_segment_key(new_locator), park_key},
         {:put, park_key, LMDB.encode_cold_park(new_locator, Map.delete(new_park, :locator))}
       ]}
    else
      false ->
        if valid_promotion_row?(old_row) and valid_promotion_row?(new_row),
          do: {:error, :logical_generation_mismatch},
          else: {:error, :invalid_cold_row}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_cold_row}
    end
  rescue
    _error -> {:error, :invalid_cold_row}
  end

  defp valid_relocation_attrs?(attrs) when is_map(attrs), do: true
  defp valid_relocation_attrs?(attrs) when is_list(attrs), do: Keyword.keyword?(attrs)
  defp valid_relocation_attrs?(_attrs), do: false

  defp cold_compaction_park_key(old_row, new_row, locator) do
    fallback = LMDB.cold_park_key(locator.flow_id)
    old_key = Map.get(old_row, :park_key, fallback)
    new_key = Map.get(new_row, :park_key, old_key)

    if is_binary(old_key) and old_key != "" and new_key == old_key,
      do: {:ok, old_key},
      else: {:error, :park_key_mismatch}
  end

  defp park_key(_locator, %{state_key: state_key}) when is_binary(state_key),
    do: LMDB.cold_park_key_for_state_key(state_key)

  defp park_key(%Locator{} = locator, _record), do: LMDB.cold_park_key(locator.flow_id)

  defp maybe_index_key(keys, key) when is_binary(key), do: [key | keys]
  defp maybe_index_key(keys, _key), do: keys

  defp maybe_due_index_key(
         keys,
         %{next_run_at_ms: next_run_at_ms},
         type,
         flow_state,
         priority,
         partition_key
       )
       when is_integer(next_run_at_ms) and is_binary(type) and is_binary(flow_state) do
    [Keys.due_key(type, flow_state, priority, partition_key) | keys]
  end

  defp maybe_due_index_key(keys, _record, _type, _flow_state, _priority, _partition_key), do: keys

  defp maybe_due_any_index_key(
         keys,
         %{next_run_at_ms: next_run_at_ms},
         type,
         priority,
         partition_key,
         true
       )
       when is_integer(next_run_at_ms) and is_binary(type) do
    [Keys.due_any_key(type, priority, partition_key) | keys]
  end

  defp maybe_due_any_index_key(keys, _record, _type, _priority, _partition_key, _enabled?),
    do: keys

  defp maybe_running_index_keys(keys, %{state: "running"} = record, type, partition_key)
       when is_binary(type) do
    keys = [Keys.inflight_index_key(type, partition_key) | keys]

    case Map.get(record, :lease_owner) do
      worker when is_binary(worker) and worker != "" ->
        [Keys.worker_index_key(worker, partition_key) | keys]

      _unleased ->
        keys
    end
  end

  defp maybe_running_index_keys(keys, _record, _type, _partition_key), do: keys

  defp maybe_metadata_index_key(keys, :root, id, _partition_key, id), do: keys
  defp maybe_metadata_index_key(keys, _kind, nil, _partition_key, _id), do: keys
  defp maybe_metadata_index_key(keys, _kind, "", _partition_key, _id), do: keys

  defp maybe_metadata_index_key(keys, kind, value, partition_key, _id) when is_binary(value) do
    key =
      case kind do
        :parent -> Keys.parent_index_key(value, partition_key)
        :root -> Keys.root_index_key(value, partition_key)
        :correlation -> Keys.correlation_index_key(value, partition_key)
      end

    [key | keys]
  end

  defp waiting?(record) do
    Map.get(record, :run_state) in [nil, "waiting", :waiting] and
      Map.get(record, :state) not in [nil, ""]
  end

  defp unleased?(record) do
    Map.get(record, :lease_owner) in [nil, ""] and
      Map.get(record, :lease_token) in [nil, ""] and
      Map.get(record, :lease_deadline_ms, 0) in [nil, 0]
  end

  defp terminal?(record) do
    Map.get(record, :run_state) in [
      "completed",
      "failed",
      "cancelled",
      :completed,
      :failed,
      :cancelled
    ] or
      Map.get(record, :terminal_retention_until_ms) not in [nil, 0]
  end

  defp value_refs_digest(refs) when is_map(refs) do
    refs
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp value_refs_digest(_refs), do: nil

  defp empty_demote_result do
    %{attempted: 0, cold_written: 0, hot_evicted: 0, hot_changed: 0, hot_failed: 0}
  end

  defp empty_promotion_result do
    %{attempted: 0, read: 0, installed: 0, stale: 0, failed: 0}
  end

  defp required_callback(opts, key, arity) do
    case Keyword.fetch(opts, key) do
      {:ok, callback} when is_function(callback, arity) -> {:ok, callback}
      {:ok, _invalid} -> {:error, {:invalid_callback, key}}
      :error -> {:error, {:missing_callback, key}}
    end
  end

  defp optional_callback(opts, key, arity, default) do
    case Keyword.fetch(opts, key) do
      {:ok, callback} when is_function(callback, arity) -> {:ok, callback}
      {:ok, _invalid} -> {:error, {:invalid_callback, key}}
      :error -> {:ok, default}
    end
  end

  defp invoke_callback(callback, args, name) do
    {:ok, apply(callback, args)}
  rescue
    exception ->
      {:error, {:callback_failed, name, {exception.__struct__, Exception.message(exception)}}}
  catch
    kind, reason -> {:error, {:callback_failed, name, {kind, reason}}}
  end

  defp bounded_batch(items, max_items) when is_list(items) and is_integer(max_items) do
    case Enum.split(items, max_items) do
      {bounded, []} -> {:ok, bounded}
      {_bounded, _remainder} -> {:error, {:batch_too_large, max_items}}
    end
  end

  defp bounded_promotion_rows(rows, opts) do
    case Keyword.fetch(opts, :limit) do
      {:ok, limit} when is_integer(limit) and limit >= 0 and limit <= @max_promotion_batch ->
        {:ok, Enum.take(rows, limit)}

      {:ok, limit} when is_integer(limit) and limit > @max_promotion_batch ->
        {:error, {:limit_too_large, @max_promotion_batch}}

      {:ok, _invalid} ->
        {:error, :invalid_limit}

      :error ->
        bounded_batch(rows, @max_promotion_batch)
    end
  end

  defp validate_promotion_rows(rows) do
    if Enum.all?(rows, &valid_promotion_row?/1),
      do: :ok,
      else: {:error, :invalid_promotion_row}
  end

  defp valid_promotion_row?(%{locator: %Locator{kind: :state} = locator, park: park} = row)
       when is_map(park) do
    Locator.valid?(locator) and
      bounded_binary?(locator.flow_id) and
      u64?(Map.get(park, :due_at_ms)) and
      Map.get(park, :locator, locator) == locator and
      optional_bounded_binary?(Map.get(row, :park_key)) and
      optional_bounded_binary?(Map.get(row, :due_key))
  end

  defp valid_promotion_row?(_row), do: false

  defp demotion_batch_ops(candidates) do
    candidates
    |> Enum.reduce_while({:ok, []}, fn candidate, {:ok, acc} ->
      case demotion_ops_result(candidate) do
        {:ok, ops} -> {:cont, {:ok, Enum.reverse(ops, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp demotion_active_index_ops(
         %{
           record: %{state_key: state_key} = record
         } = candidate
       )
       when is_binary(state_key) and state_key != "" do
    with {:ok, previous_cleanup_ops} <-
           previous_active_index_cleanup_ops(
             state_key,
             record,
             Map.get(candidate, :active_index_reverse_value)
           ) do
      timeout_ops = LMDB.active_timeout_index_put_ops(state_key, record, 0)
      {:ok, Enum.uniq(previous_cleanup_ops ++ timeout_ops)}
    end
  end

  defp demotion_active_index_ops(_candidate), do: {:ok, []}

  defp previous_active_index_cleanup_ops(_state_key, _record, nil), do: {:ok, []}

  defp previous_active_index_cleanup_ops(state_key, record, reverse_value)
       when is_binary(reverse_value) do
    with {:ok, reverse_keys} <- LMDB.decode_active_index_reverse_value(reverse_value),
         true <- active_reverse_owned_by_record?(reverse_keys, state_key, record) do
      LMDB.active_index_delete_ops_from_reverse_result(state_key, reverse_value)
    else
      :error -> {:error, :invalid_active_index_reverse}
      false -> {:error, :active_index_reverse_owner_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp previous_active_index_cleanup_ops(_state_key, _record, _invalid),
    do: {:error, :invalid_active_index_reverse}

  defp active_reverse_owned_by_record?(reverse_keys, state_key, record)
       when is_list(reverse_keys) do
    {projection_ops, _reverse_value} =
      LMDB.active_index_put_ops_with_reverse(state_key, record, 0)

    expected_keys =
      projection_ops
      |> Enum.reduce(MapSet.new(), fn
        {:put, key, _value}, acc ->
          if String.starts_with?(key, LMDB.active_index_global_prefix()),
            do: MapSet.put(acc, key),
            else: acc

        _other, acc ->
          acc
      end)

    MapSet.new(reverse_keys) == expected_keys and MapSet.size(expected_keys) <= 5
  end

  defp validate_demotion_candidates(candidates) do
    if Enum.all?(candidates, &valid_demotion_candidate?/1),
      do: :ok,
      else: {:error, :invalid_candidate}
  end

  defp valid_demotion_candidate?(
         %{
           locator: %Locator{kind: :state} = locator,
           record: record
         } = candidate
       )
       when is_map(record) do
    Locator.valid?(locator) and
      bounded_binary?(locator.flow_id) and
      Map.get(record, :id) == locator.flow_id and
      Map.get(record, :version) == locator.version and
      bounded_binary?(Map.get(record, :type)) and
      bounded_binary?(Map.get(record, :state)) and
      u64?(Map.get(record, :next_run_at_ms)) and
      signed_i64?(Map.get(record, :priority, 0)) and
      optional_bounded_binary?(Map.get(record, :partition_key)) and
      is_map(Map.get(record, :value_refs, %{})) and
      optional_nonempty_blob?(Map.get(candidate, :active_index_reverse_value)) and
      optional_blob?(Map.get(candidate, :state_value)) and
      valid_active_projection_record?(record)
  end

  defp valid_demotion_candidate?(_candidate), do: false

  defp valid_active_projection_record?(%{state_key: state_key} = record)
       when is_binary(state_key) and state_key != "",
       do: bounded_binary?(state_key) and u64?(Map.get(record, :updated_at_ms))

  defp valid_active_projection_record?(record), do: is_nil(Map.get(record, :state_key))

  defp valid_hot_index_record?(record) do
    bounded_binary?(Map.get(record, :id)) and
      bounded_binary?(Map.get(record, :type)) and
      bounded_binary?(Map.get(record, :state)) and
      signed_i64?(Map.get(record, :priority, 0)) and
      optional_bounded_binary?(Map.get(record, :partition_key)) and
      optional_u64?(Map.get(record, :next_run_at_ms)) and
      optional_bounded_index_component?(Map.get(record, :lease_owner)) and
      optional_bounded_index_component?(Map.get(record, :parent_flow_id)) and
      optional_bounded_index_component?(Map.get(record, :root_flow_id)) and
      optional_bounded_index_component?(Map.get(record, :correlation_id))
  end

  defp valid_lmdb_op_keys?(ops) do
    Enum.all?(ops, fn
      {:put, key, _value} -> bounded_binary?(key)
      {:delete, key} -> bounded_binary?(key)
      {:compare, key, _value} -> bounded_binary?(key)
      _invalid -> false
    end)
  end

  defp u64?(value), do: is_integer(value) and value >= 0 and value <= @max_u64
  defp signed_i64?(value), do: is_integer(value) and value >= @min_i64 and value <= @max_i64
  defp valid_required_binary?(value), do: is_binary(value) and value != ""

  defp bounded_binary?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= @max_key_bytes

  defp optional_bounded_binary?(nil), do: true
  defp optional_bounded_binary?(value), do: bounded_binary?(value)
  defp optional_u64?(nil), do: true
  defp optional_u64?(value), do: u64?(value)
  defp optional_bounded_index_component?(nil), do: true

  defp optional_bounded_index_component?(value),
    do: is_binary(value) and byte_size(value) <= @max_key_bytes

  defp optional_nonempty_blob?(nil), do: true
  defp optional_nonempty_blob?(value), do: is_binary(value) and value != ""
  defp optional_blob?(nil), do: true
  defp optional_blob?(value), do: is_binary(value)

  defp valid_keyword_options?(opts, allowed) when is_list(opts) and is_list(allowed) do
    Keyword.keyword?(opts) and
      Enum.all?(opts, fn {key, _value} -> key in allowed end) and
      opts
      |> Keyword.keys()
      |> Enum.uniq()
      |> length()
      |> Kernel.==(length(opts))
  end
end
