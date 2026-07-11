defmodule Ferricstore.Flow.Governance.LimitReconciler do
  @moduledoc false

  use GenServer

  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.LimitCatalogOutbox
  alias Ferricstore.Flow.Governance.LimitRecord
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Governance.ReleaseOutbox
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @default_interval_ms 1_000
  @default_reservation_limit 128

  def start_link(opts) when is_list(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    name = Keyword.get(opts, :name, process_name(ctx))
    GenServer.start_link(__MODULE__, ctx, name: name)
  end

  @impl true
  def init(ctx) do
    state = %{ctx: ctx, cursor: nil, catalog_shard: 0, timer: nil}
    {:ok, schedule(state)}
  end

  @impl true
  def handle_info(:reconcile, state) do
    state = %{state | timer: nil}

    {catalog_shard, _catalog_result} =
      reconcile_catalog_publications(
        state.ctx,
        state.catalog_shard,
        reconcile_reservation_limit()
      )

    cursor =
      case reconcile_page(state.ctx, state.cursor, reconcile_reservation_limit(), now_ms()) do
        {:ok, %{next_cursor: next_cursor}} -> next_cursor
        {:error, _reason} -> nil
      end

    {:noreply, schedule(%{state | cursor: cursor, catalog_shard: catalog_shard})}
  end

  @impl true
  def terminate(_reason, state) do
    _ = Ferricstore.Flow.Governance.LimitCache.flush(state.ctx)
    :ok
  end

  @doc false
  def run_once(ctx, opts \\ []) when is_list(opts) do
    now_ms = Keyword.get(opts, :now_ms, now_ms())

    reservation_limit =
      Keyword.get(opts, :reservation_limit, Keyword.get(opts, :scope_limit, 256))

    cursor = Keyword.get(opts, :cursor)
    _catalog_result = reconcile_catalog_publications(ctx, 0, reservation_limit)
    reconcile_page(ctx, cursor, reservation_limit, now_ms)
  end

  defp reconcile_catalog_publications(ctx, start_shard, limit)
       when is_integer(start_shard) and start_shard >= 0 and is_integer(limit) and limit > 0 and
              limit <= 256 do
    shard_count = Map.fetch!(ctx, :shard_count)
    shard_order = Enum.map(0..(shard_count - 1), &rem(start_shard + &1, shard_count))
    scan_catalog_publication_shards(ctx, shard_order, limit, shard_count)
  end

  defp reconcile_catalog_publications(ctx, start_shard, _invalid_limit) do
    shard_count = Map.fetch!(ctx, :shard_count)
    {rem(start_shard + 1, shard_count), %{published: 0, errors: 1}}
  end

  defp scan_catalog_publication_shards(_ctx, [], _limit, _shard_count),
    do: {0, %{published: 0, errors: 0}}

  defp scan_catalog_publication_shards(ctx, [shard_index | rest], limit, shard_count) do
    case LimitCatalogOutbox.read_page(ctx, shard_index, limit) do
      {:ok, %{entries: []}} ->
        scan_catalog_publication_shards(ctx, rest, limit, shard_count)

      {:ok, page} ->
        result = publish_catalog_page(ctx, page)
        {rem(shard_index + 1, shard_count), result}

      {:error, _reason} ->
        {rem(shard_index + 1, shard_count), %{published: 0, errors: 1}}
    end
  end

  defp publish_catalog_page(ctx, %{entries: entries, head: head, shard_index: shard_index}) do
    {published, last_sequence, errors} =
      Enum.reduce_while(entries, {0, nil, 0}, fn {sequence, owner_key},
                                                 {published, last, _errors} ->
        case publish_catalog_owner(ctx, shard_index, owner_key) do
          :ok -> {:cont, {published + 1, sequence, 0}}
          {:error, _reason} -> {:halt, {published, last, 1}}
        end
      end)

    case last_sequence do
      nil ->
        %{published: published, errors: errors}

      sequence ->
        case Router.flow_governance_limit_catalog_outbox_ack(
               ctx,
               shard_index,
               head,
               sequence
             ) do
          :ok -> %{published: published, errors: errors}
          {:ok, _result} -> %{published: published, errors: errors}
          {:error, _reason} -> %{published: published, errors: errors + 1}
        end
    end
  end

  defp publish_catalog_owner(ctx, shard_index, owner_key) do
    case Router.read_shard_value(ctx, shard_index, owner_key) do
      {:ok, value} when is_binary(value) ->
        with {:ok, owner} <- LimitRecord.decode_owner(value),
             true <- Keys.governance_limit_key(owner.scope) == owner_key do
          Catalog.register_key(ctx, Keys.governance_catalog_key(:limit), owner_key)
        else
          _invalid -> {:error, :limit_catalog_owner_corrupt}
        end

      {:ok, nil} ->
        :ok

      :unavailable ->
        {:error, :limit_catalog_owner_unavailable}

      _invalid ->
        {:error, :limit_catalog_owner_corrupt}
    end
  end

  defp reconcile_page(ctx, cursor, reservation_limit, now_ms) do
    result = run_page(ctx, cursor, reservation_limit, now_ms)
    Telemetry.emit(:limit_reconcile, result, reconcile_metadata(result))
  end

  defp run_page(ctx, cursor, reservation_limit, now_ms)
       when is_integer(reservation_limit) and reservation_limit > 0 and
              reservation_limit <= 256 and
              is_integer(now_ms) and now_ms >= 0 do
    with {:ok, cursor_state} <-
           ReleaseOutbox.decode_reconcile_cursor(cursor, Map.fetch!(ctx, :shard_count)),
         {:ok, page} <- next_outbox_page(ctx, cursor_state, reservation_limit) do
      reconcile_outbox_page(ctx, page, cursor_state, now_ms)
    end
  end

  defp run_page(_ctx, _cursor, _reservation_limit, _now_ms),
    do: {:error, "ERR invalid flow limit reconciliation options"}

  defp reconcile_metadata({:ok, counts}) when is_map(counts) do
    Map.take(counts, [:released, :retained, :errors, :read_batches])
  end

  defp reconcile_metadata(_error),
    do: %{released: 0, retained: 0, errors: 1, read_batches: 0}

  defp next_outbox_page(ctx, %{next_shard: start_shard, positions: positions}, reservation_limit) do
    shard_count = Map.fetch!(ctx, :shard_count)
    shard_order = Enum.map(0..(shard_count - 1), &rem(start_shard + &1, shard_count))
    scan_outbox_shards(ctx, shard_order, positions, reservation_limit, 0)
  end

  defp scan_outbox_shards(_ctx, [], _positions, _reservation_limit, scan_errors) do
    {:ok,
     %{
       entries: [],
       shard_index: nil,
       head: nil,
       tail: nil,
       more?: false,
       scan_errors: scan_errors
     }}
  end

  defp scan_outbox_shards(
         ctx,
         [shard_index | remaining],
         positions,
         reservation_limit,
         scan_errors
       ) do
    case ReleaseOutbox.read_page(
           ctx,
           shard_index,
           reservation_limit,
           Map.get(positions, shard_index)
         ) do
      {:ok, %{entries: [_ | _]} = page} ->
        {:ok, Map.put(page, :scan_errors, scan_errors)}

      {:ok, %{entries: []}} ->
        scan_outbox_shards(ctx, remaining, positions, reservation_limit, scan_errors)

      {:error, _reason} ->
        scan_outbox_shards(ctx, remaining, positions, reservation_limit, scan_errors + 1)
    end
  end

  defp reconcile_outbox_page(
         ctx,
         %{entries: [], scan_errors: scan_errors},
         cursor_state,
         _now_ms
       ) do
    next_cursor =
      if scan_errors == 0 do
        nil
      else
        advance_empty_cursor(ctx, cursor_state)
      end

    {:ok,
     %{
       released: 0,
       retained: 0,
       errors: scan_errors,
       read_batches: 0,
       next_cursor: next_cursor
     }}
  end

  defp reconcile_outbox_page(
         ctx,
         %{entries: entries, shard_index: shard_index, head: head, scan_errors: scan_errors} =
           page,
         cursor_state,
         now_ms
       ) do
    {flow_values, read_batches} = batch_flow_values(ctx, entries)
    decisions = release_decisions(entries, flow_values)
    release_results = release_groups(ctx, decisions, now_ms)
    statuses = decision_statuses(decisions, release_results)
    counts = count_statuses(statuses, read_batches, scan_errors)

    case persist_release_progress(ctx, shard_index, head, statuses) do
      {:ok, acknowledged_up_to} ->
        next_cursor = next_cursor(ctx, page, cursor_state, acknowledged_up_to)
        {:ok, Map.put(counts, :next_cursor, next_cursor)}

      {:error, _reason} ->
        retry_cursor = retry_cursor(ctx, shard_index, cursor_state)

        {:ok,
         counts
         |> Map.update!(:errors, &(&1 + 1))
         |> Map.put(:next_cursor, retry_cursor)}
    end
  end

  defp batch_flow_values(ctx, entries) do
    entries
    |> Enum.filter(fn {_sequence, intent} -> is_map(intent) end)
    |> Enum.group_by(fn {_sequence, intent} -> Map.get(intent, :partition_key) end)
    |> Enum.reduce({%{}, 0}, fn {partition_key, group}, {values, batches} ->
      ids = Enum.map(group, fn {_sequence, intent} -> Map.fetch!(intent, :flow_id) end)

      records =
        ctx
        |> Router.flow_batch_get_with_status(ids, partition_key)
        |> normalize_flow_batch_results(length(ids))

      values =
        group
        |> Enum.zip(records)
        |> Enum.reduce(values, fn {{sequence, _intent}, value}, acc ->
          Map.put(acc, sequence, value)
        end)

      {values, batches + 1}
    end)
  end

  defp release_decisions(entries, flow_values) do
    Enum.map(entries, fn
      {sequence, :completed} ->
        {sequence, nil, :completed}

      {sequence, intent} ->
        status = classify_release(intent, Map.get(flow_values, sequence))
        {sequence, intent, status}
    end)
  end

  @doc false
  def classify_release(%{reservation_id: reservation_id}, value) when is_binary(value) do
    case Ferricstore.Flow.decode_record(value) do
      %{state: "running", governance_limit: %{reservation_id: ^reservation_id}} -> :retain
      record when is_map(record) -> :release
      _corrupt -> :error
    end
  rescue
    _decode_error -> :error
  end

  def classify_release(_intent, nil), do: :release
  def classify_release(_intent, _unavailable_or_malformed), do: :error

  @doc false
  def normalize_flow_batch_results(results, expected_count)
      when is_list(results) and is_integer(expected_count) and expected_count >= 0 and
             length(results) == expected_count,
      do: results

  def normalize_flow_batch_results(_malformed, expected_count)
      when is_integer(expected_count) and expected_count >= 0,
      do: List.duplicate(:unavailable, expected_count)

  defp release_groups(ctx, decisions, now_ms) do
    decisions
    |> Enum.filter(fn {_sequence, _intent, status} -> status == :release end)
    |> Enum.group_by(fn {_sequence, intent, _status} ->
      {intent.scope, intent.shard_id}
    end)
    |> Map.new(fn {{scope, shard_id} = group_key, group} ->
      reservation_ids =
        group
        |> Enum.map(fn {_sequence, intent, _status} -> intent.reservation_id end)
        |> Enum.uniq()

      result =
        LimitStore.release(ctx, scope,
          shard_id: shard_id,
          amount: length(reservation_ids),
          reservation_ids: reservation_ids,
          now_ms: now_ms
        )

      {group_key, result}
    end)
  end

  defp decision_statuses(decisions, release_results) do
    Enum.map(decisions, fn
      {sequence, _intent, :retain} ->
        {sequence, :retained}

      {sequence, _intent, :error} ->
        {sequence, :error}

      {sequence, _intent, :completed} ->
        {sequence, :completed}

      {sequence, intent, :release} ->
        case Map.fetch!(release_results, {intent.scope, intent.shard_id}) do
          {:ok, _owner} -> {sequence, :released}
          {:error, _reason} -> {sequence, :error}
        end
    end)
  end

  defp count_statuses(statuses, read_batches, scan_errors) do
    Enum.reduce(
      statuses,
      %{released: 0, retained: 0, errors: scan_errors, read_batches: read_batches},
      fn
        {_sequence, :released}, acc -> Map.update!(acc, :released, &(&1 + 1))
        {_sequence, :retained}, acc -> Map.update!(acc, :retained, &(&1 + 1))
        {_sequence, :error}, acc -> Map.update!(acc, :errors, &(&1 + 1))
        {_sequence, :completed}, acc -> acc
      end
    )
  end

  defp persist_release_progress(ctx, shard_index, head, statuses) do
    acknowledged_up_to = contiguous_completed_up_to(head, statuses)

    sparse_releases =
      for {sequence, :released} <- statuses,
          is_nil(acknowledged_up_to) or sequence > acknowledged_up_to,
          do: sequence

    with :ok <- mark_sparse_releases(ctx, shard_index, sparse_releases) do
      acknowledge_contiguous(ctx, shard_index, head, acknowledged_up_to)
    end
  end

  defp contiguous_completed_up_to(head, statuses) do
    acknowledged_up_to =
      case statuses do
        [{^head, _status} | _remaining] ->
          Enum.reduce_while(statuses, nil, fn
            {sequence, status}, _last when status in [:released, :completed] ->
              {:cont, sequence}

            {_sequence, _retained_or_error}, last ->
              {:halt, last}
          end)

        _scan_started_after_head ->
          nil
      end

    acknowledged_up_to
  end

  defp mark_sparse_releases(_ctx, _shard_index, []), do: :ok

  defp mark_sparse_releases(ctx, shard_index, sequences) do
    case Router.flow_governance_release_outbox_mark_completed(ctx, shard_index, sequences) do
      :ok -> :ok
      {:ok, _result} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp acknowledge_contiguous(ctx, shard_index, head, acknowledged_up_to) do
    case acknowledged_up_to do
      nil ->
        {:ok, nil}

      sequence ->
        case Router.flow_governance_release_outbox_ack(ctx, shard_index, head, sequence) do
          :ok -> {:ok, sequence}
          {:ok, _result} -> {:ok, sequence}
          {:error, _reason} = error -> error
        end
    end
  end

  defp next_cursor(
         ctx,
         %{entries: entries, shard_index: shard_index, tail: tail},
         %{positions: positions},
         _acknowledged_up_to
       ) do
    last_sequence = entries |> List.last() |> elem(0)

    positions =
      if last_sequence < tail do
        Map.put(positions, shard_index, last_sequence + 1)
      else
        Map.delete(positions, shard_index)
      end

    encode_next_cursor(ctx, shard_index, positions)
  end

  defp retry_cursor(ctx, shard_index, %{positions: positions}) do
    encode_next_cursor(ctx, shard_index, Map.delete(positions, shard_index))
  end

  defp encode_next_cursor(ctx, shard_index, positions) do
    next_shard = rem(shard_index + 1, Map.fetch!(ctx, :shard_count))
    ReleaseOutbox.encode_reconcile_cursor(next_shard, positions)
  end

  defp advance_empty_cursor(ctx, %{next_shard: next_shard, positions: positions}) do
    advanced_shard = rem(next_shard + 1, Map.fetch!(ctx, :shard_count))
    ReleaseOutbox.encode_reconcile_cursor(advanced_shard, positions)
  end

  defp schedule(%{timer: nil} = state) do
    %{state | timer: Process.send_after(self(), :reconcile, reconcile_interval_ms())}
  end

  defp schedule(state), do: state

  defp reconcile_interval_ms do
    case Application.get_env(
           :ferricstore,
           :flow_governance_limit_reconcile_interval_ms,
           @default_interval_ms
         ) do
      interval when is_integer(interval) and interval > 0 -> interval
      _invalid -> @default_interval_ms
    end
  end

  defp reconcile_reservation_limit do
    case Application.get_env(
           :ferricstore,
           :flow_governance_limit_reconcile_reservation_limit,
           @default_reservation_limit
         ) do
      limit when is_integer(limit) and limit > 0 -> min(limit, 256)
      _invalid -> @default_reservation_limit
    end
  end

  defp now_ms, do: Ferricstore.CommandTime.now_ms()

  defp process_name(%{name: :default}), do: __MODULE__
  defp process_name(%{name: name}), do: {:global, {__MODULE__, name}}
end
