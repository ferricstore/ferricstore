defmodule Ferricstore.Flow.Hibernation do
  @moduledoc false

  alias Ferricstore.Flow.{ClaimWaiters, Keys, LMDB, Locator}

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

  @type candidate :: %{
          required(:locator) => Locator.t(),
          required(:record) => map()
        }

  @type demote_result :: %{
          attempted: non_neg_integer(),
          cold_written: non_neg_integer(),
          hot_evicted: non_neg_integer(),
          hot_changed: non_neg_integer()
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

  def refresh_config! do
    :persistent_term.erase(@enabled_key)
    enabled?()
  end

  def hot_window_ms, do: @default_hot_window_ms
  def safety_margin_ms, do: @default_safety_margin_ms
  def promote_window_ms, do: @default_promote_window_ms
  def late_promote_window_ms, do: @default_late_promote_window_ms

  @spec maybe_schedule_claim_waiter(map()) :: :ok
  def maybe_schedule_claim_waiter(%{type: type, state: state, next_run_at_ms: due_at_ms} = record)
      when is_binary(type) and is_binary(state) and is_integer(due_at_ms) do
    if ClaimWaiters.any_waiters?() do
      priority = Map.get(record, :priority, 0)
      partition_key = Map.get(record, :partition_key)

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
  def demotable?(record, now_ms, opts \\ []) when is_map(record) and is_integer(now_ms) do
    hot_window_ms = Keyword.get(opts, :hot_window_ms, @default_hot_window_ms)
    safety_margin_ms = Keyword.get(opts, :safety_margin_ms, @default_safety_margin_ms)
    due_at_ms = Map.get(record, :next_run_at_ms)

    waiting?(record) and unleased?(record) and not terminal?(record) and is_integer(due_at_ms) and
      due_at_ms > now_ms + hot_window_ms + safety_margin_ms
  end

  @spec demote_candidates([candidate()], keyword()) ::
          {:ok, demote_result()} | {:error, term(), demote_result()}
  def demote_candidates(candidates, opts)
      when is_list(candidates) and is_list(opts) do
    write_cold_fun = Keyword.fetch!(opts, :write_cold_fun)
    evict_hot_fun = Keyword.fetch!(opts, :evict_hot_fun)

    ops = Enum.flat_map(candidates, &demotion_ops/1)
    attempted = length(candidates)
    base = %{attempted: attempted, cold_written: 0, hot_evicted: 0, hot_changed: 0}

    case write_cold_fun.(ops) do
      :ok ->
        result =
          Enum.reduce(candidates, %{base | cold_written: attempted}, fn candidate, acc ->
            case evict_hot_fun.(candidate.locator) do
              :ok -> %{acc | hot_evicted: acc.hot_evicted + 1}
              {:error, :changed} -> %{acc | hot_changed: acc.hot_changed + 1}
              _other -> acc
            end
          end)

        {:ok, result}

      {:error, reason} ->
        {:error, reason, base}
    end
  end

  @spec demotion_ops(candidate()) :: list()
  def demotion_ops(%{locator: %Locator{kind: :state} = locator, record: record} = candidate) do
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

    active_index_ops =
      case Map.get(record, :state_key) do
        state_key when is_binary(state_key) ->
          LMDB.active_timeout_index_put_ops(
            state_key,
            record,
            0,
            Map.get(candidate, :active_index_reverse_value)
          )

        _missing ->
          []
      end

    [
      {:put, park_key, park},
      {:put, due_key, park_key},
      {:put, LMDB.cold_by_segment_key(locator), park_key}
      | active_index_ops
    ]
  end

  @spec rebuild_cold_ops([candidate()], non_neg_integer(), keyword()) :: list()
  def rebuild_cold_ops(candidates, now_ms, opts \\ [])
      when is_list(candidates) and is_integer(now_ms) and is_list(opts) do
    candidates
    |> Enum.filter(fn
      %{locator: %Locator{kind: :state}, record: record} -> demotable?(record, now_ms, opts)
      _other -> false
    end)
    |> Enum.flat_map(&demotion_ops/1)
  end

  @spec promotion_bucket_prefixes(non_neg_integer(), non_neg_integer(), pos_integer()) :: [
          binary()
        ]
  def promotion_bucket_prefixes(now_ms, horizon_ms, bucket_ms \\ 60_000)
      when is_integer(now_ms) and now_ms >= 0 and is_integer(horizon_ms) and horizon_ms >= now_ms and
             is_integer(bucket_ms) and bucket_ms > 0 do
    first = LMDB.cold_due_bucket_ms(now_ms, bucket_ms)
    last = LMDB.cold_due_bucket_ms(horizon_ms, bucket_ms)

    first
    |> Stream.iterate(&(&1 + bucket_ms))
    |> Stream.take_while(&(&1 <= last))
    |> Enum.map(&LMDB.cold_due_bucket_prefix/1)
  end

  @spec hot_index_keys(map(), keyword()) :: [binary()]
  def hot_index_keys(record, opts \\ []) when is_map(record) do
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
  end

  @spec cleanup_ops(promotion_row()) :: list()
  def cleanup_ops(%{locator: %Locator{}} = row), do: promotion_cleanup_ops(row)

  @spec promote_candidates([promotion_row()], keyword()) ::
          {:ok, promotion_result()} | {:error, term(), promotion_result()}
  def promote_candidates(rows, opts) when is_list(rows) and is_list(opts) do
    read_state_fun = Keyword.fetch!(opts, :read_state_fun)
    install_hot_fun = Keyword.fetch!(opts, :install_hot_fun)
    cleanup_cold_fun = Keyword.get(opts, :cleanup_cold_fun, fn _ops -> :ok end)
    validate_fun = Keyword.get(opts, :validate_fun, &valid_promotion_record?/2)
    limit = Keyword.get(opts, :limit, length(rows))

    {cleanup_ops, result} =
      rows
      |> Enum.take(limit)
      |> Enum.reduce({[], %{attempted: 0, read: 0, installed: 0, stale: 0, failed: 0}}, fn row,
                                                                                           {ops,
                                                                                            acc} ->
        acc = %{acc | attempted: acc.attempted + 1}

        case read_state_fun.(row.locator) do
          {:ok, record} ->
            acc = %{acc | read: acc.read + 1}

            if validate_fun.(record, row) do
              case install_hot_fun.(row.locator, record) do
                :ok ->
                  {promotion_cleanup_ops(row) ++ ops, %{acc | installed: acc.installed + 1}}

                _other ->
                  {ops, %{acc | failed: acc.failed + 1}}
              end
            else
              {promotion_cleanup_ops(row) ++ ops, %{acc | stale: acc.stale + 1}}
            end

          _other ->
            {ops, %{acc | failed: acc.failed + 1}}
        end
      end)

    case cleanup_cold_fun.(Enum.reverse(cleanup_ops)) do
      :ok -> {:ok, result}
      {:error, reason} -> {:error, reason, result}
    end
  end

  @spec fetch_or_promote(binary(), keyword()) ::
          {:ok, :hot, map()}
          | {:ok, :cold_promoted, map()}
          | :not_found
          | {:error, term()}
  def fetch_or_promote(flow_id, opts) when is_binary(flow_id) and is_list(opts) do
    fetch_hot_fun = Keyword.fetch!(opts, :fetch_hot_fun)
    fetch_cold_fun = Keyword.fetch!(opts, :fetch_cold_fun)
    read_state_fun = Keyword.fetch!(opts, :read_state_fun)
    install_hot_fun = Keyword.fetch!(opts, :install_hot_fun)
    cleanup_cold_fun = Keyword.get(opts, :cleanup_cold_fun, fn _ops -> :ok end)
    validate_fun = Keyword.get(opts, :validate_fun, &valid_promotion_record?/2)

    case fetch_hot_fun.(flow_id) do
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
  end

  defp valid_promotion_record?(record, %{locator: %Locator{} = locator, park: park}) do
    Map.get(record, :id) == locator.flow_id and
      Map.get(record, :version, locator.version) == locator.version and
      Map.get(record, :next_run_at_ms) == Map.get(park, :due_at_ms) and
      not terminal?(record)
  end

  defp promote_cold_point_lookup(
         flow_id,
         fetch_cold_fun,
         read_state_fun,
         install_hot_fun,
         cleanup_cold_fun,
         validate_fun
       ) do
    case fetch_cold_fun.(flow_id) do
      {:ok, %{locator: %Locator{} = locator} = row} ->
        with {:ok, record} <- read_state_fun.(locator),
             true <- validate_fun.(record, row),
             :ok <- install_hot_fun.(locator, record),
             :ok <- cleanup_cold_fun.(promotion_cleanup_ops(row)) do
          {:ok, :cold_promoted, record}
        else
          false -> {:error, :stale_cold_locator}
          {:error, _reason} = error -> error
          other -> {:error, other}
        end

      :not_found ->
        :not_found

      {:error, _reason} = error ->
        error
    end
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
          {:ok, promotion_row()} | {:error, :bad_locator}
  def relocate_cold_row(%{locator: %Locator{} = locator, park: park} = row, attrs)
      when is_list(attrs) or is_map(attrs) do
    with {:ok, relocated} <- Locator.relocate(locator, attrs) do
      {:ok, %{row | locator: relocated, park: Map.put(park, :locator, relocated)}}
    end
  end

  @spec cold_compaction_ops(promotion_row(), promotion_row()) :: {:ok, list()} | {:error, term()}
  def cold_compaction_ops(
        %{locator: %Locator{} = old_locator},
        %{locator: %Locator{} = new_locator, park: new_park} = new_row
      ) do
    if Locator.same_logical_record?(old_locator, new_locator) do
      park_key = Map.get(new_row, :park_key, LMDB.cold_park_key(new_locator.flow_id))

      {:ok,
       [
         {:delete, LMDB.cold_by_segment_key(old_locator)},
         {:put, LMDB.cold_by_segment_key(new_locator), park_key},
         {:put, park_key, LMDB.encode_cold_park(new_locator, Map.delete(new_park, :locator))}
       ]}
    else
      {:error, :logical_generation_mismatch}
    end
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
    worker = Map.get(record, :lease_owner, "")

    [
      Keys.worker_index_key(worker, partition_key),
      Keys.inflight_index_key(type, partition_key) | keys
    ]
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
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp value_refs_digest(_refs), do: nil
end
