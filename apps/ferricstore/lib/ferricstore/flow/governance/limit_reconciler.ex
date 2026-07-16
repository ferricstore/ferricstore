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
  @max_exact_version 9_007_199_254_740_991

  def start_link(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, ctx} <- Keyword.fetch(opts, :instance_ctx) do
      name = Keyword.get(opts, :name, process_name(ctx))
      GenServer.start_link(__MODULE__, ctx, name: name)
    else
      _invalid -> {:error, "ERR invalid flow limit reconciler options"}
    end
  end

  def start_link(_opts), do: {:error, "ERR invalid flow limit reconciler options"}

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
  def run_once(ctx, opts \\ [])

  def run_once(ctx, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      now_ms = Keyword.get(opts, :now_ms, now_ms())
      reservation_limit = Keyword.get(opts, :reservation_limit, 256)
      cursor = Keyword.get(opts, :cursor)
      catalog_shard = Keyword.get(opts, :catalog_shard, 0)

      if valid_run_options?(ctx, cursor, reservation_limit, now_ms, catalog_shard) do
        {next_catalog_shard, catalog_result} =
          reconcile_catalog_publications(ctx, catalog_shard, reservation_limit)

        case reconcile_page(ctx, cursor, reservation_limit, now_ms) do
          {:ok, result} ->
            {:ok, attach_catalog_result(result, catalog_result, next_catalog_shard)}

          {:error, _reason} = error ->
            error
        end
      else
        {:error, "ERR invalid flow limit reconciliation options"}
      end
    else
      {:error, "ERR invalid flow limit reconciliation options"}
    end
  end

  def run_once(_ctx, _opts), do: {:error, "ERR invalid flow limit reconciliation options"}

  defp valid_run_options?(ctx, cursor, reservation_limit, now_ms, catalog_shard) do
    is_map(ctx) and is_integer(Map.get(ctx, :shard_count)) and Map.get(ctx, :shard_count) > 0 and
      is_integer(reservation_limit) and reservation_limit > 0 and reservation_limit <= 256 and
      is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_exact_version and
      is_integer(catalog_shard) and catalog_shard >= 0 and
      catalog_shard <= @max_exact_version and
      match?(
        {:ok, _cursor_state},
        ReleaseOutbox.decode_reconcile_cursor(cursor, Map.get(ctx, :shard_count))
      )
  end

  defp reconcile_catalog_publications(ctx, start_shard, limit)
       when is_integer(start_shard) and start_shard >= 0 and is_integer(limit) and limit > 0 and
              limit <= 256 do
    shard_count = Map.fetch!(ctx, :shard_count)
    shard_index = rem(start_shard, shard_count)

    result =
      case LimitCatalogOutbox.read_page(ctx, shard_index, limit) do
        {:ok, %{entries: []}} ->
          catalog_result(0, 0, 0, 0)

        {:ok, page} ->
          page_result = publish_catalog_page(ctx, page)
          Map.update!(page_result, :read_batches, &(&1 + 1))

        {:error, _reason} ->
          catalog_result(0, 1, 0, 0)
      end

    {rem(shard_index + 1, shard_count), Map.put(result, :shards_scanned, 1)}
  end

  defp reconcile_catalog_publications(ctx, start_shard, _invalid_limit) do
    shard_count = Map.fetch!(ctx, :shard_count)

    shard_index =
      if is_integer(start_shard) and start_shard >= 0, do: rem(start_shard, shard_count), else: 0

    {rem(shard_index + 1, shard_count), Map.put(catalog_result(0, 1, 0, 0), :shards_scanned, 1)}
  end

  defp publish_catalog_page(ctx, %{entries: entries, head: head, shard_index: shard_index}) do
    owner_keys = Enum.map(entries, &elem(&1, 1))

    case Router.read_shard_values(ctx, shard_index, owner_keys) do
      {:ok, values} when is_list(values) and length(values) == length(entries) ->
        {catalog_keys, published, last_sequence, errors} =
          catalog_prefix(entries, values)

        publish_catalog_prefix(
          ctx,
          shard_index,
          head,
          catalog_keys,
          published,
          last_sequence,
          errors
        )

      _unavailable_or_invalid ->
        catalog_result(0, 1, 1, 0)
    end
  end

  defp catalog_prefix(entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce_while({[], 0, nil, 0}, fn {{sequence, owner_key}, value},
                                             {keys, published, last, _errors} ->
      case catalog_owner(value, owner_key) do
        {:ok, nil} ->
          {:cont, {keys, published + 1, sequence, 0}}

        {:ok, catalog_key} ->
          {:cont, {[catalog_key | keys], published + 1, sequence, 0}}

        {:error, _reason} ->
          {:halt, {keys, published, last, 1}}
      end
    end)
    |> then(fn {keys, published, last_sequence, errors} ->
      {Enum.reverse(keys), published, last_sequence, errors}
    end)
  end

  defp catalog_owner(nil, _owner_key), do: {:ok, nil}

  defp catalog_owner(value, owner_key) when is_binary(value) do
    case LimitRecord.decode_owner(value) do
      {:ok, owner} ->
        if Keys.governance_limit_key(owner.scope) == owner_key do
          {:ok, owner_key}
        else
          {:error, :limit_catalog_owner_corrupt}
        end

      {:error, _reason} ->
        {:error, :limit_catalog_owner_corrupt}
    end
  end

  defp catalog_owner(_value, _owner_key), do: {:error, :limit_catalog_owner_corrupt}

  defp publish_catalog_prefix(
         _ctx,
         _shard_index,
         _head,
         _catalog_keys,
         0,
         nil,
         errors
       ),
       do: catalog_result(0, errors, 1, 0)

  defp publish_catalog_prefix(
         ctx,
         shard_index,
         head,
         catalog_keys,
         published,
         last_sequence,
         errors
       ) do
    write_batches = if catalog_keys == [], do: 0, else: 1

    with :ok <-
           Catalog.register_keys(
             ctx,
             Keys.governance_catalog_key(:limit),
             catalog_keys
           ),
         :ok <- ack_catalog_prefix(ctx, shard_index, head, last_sequence) do
      catalog_result(published, errors, 1, write_batches)
    else
      {:error, _reason} -> catalog_result(0, errors + 1, 1, write_batches)
    end
  end

  defp ack_catalog_prefix(ctx, shard_index, head, last_sequence) do
    case Router.flow_governance_limit_catalog_outbox_ack(
           ctx,
           shard_index,
           head,
           last_sequence
         ) do
      :ok -> :ok
      {:ok, _result} -> :ok
      {:error, _reason} = error -> error
      _invalid -> {:error, :limit_catalog_ack_failed}
    end
  end

  defp catalog_result(published, errors, read_batches, write_batches) do
    %{
      published: published,
      errors: errors,
      read_batches: read_batches,
      write_batches: write_batches,
      shards_scanned: 0
    }
  end

  defp attach_catalog_result(result, catalog_result, next_catalog_shard) do
    Map.merge(result, %{
      catalog_published: catalog_result.published,
      catalog_errors: catalog_result.errors,
      catalog_read_batches: catalog_result.read_batches,
      catalog_write_batches: catalog_result.write_batches,
      catalog_shards_scanned: catalog_result.shards_scanned,
      next_catalog_shard: next_catalog_shard
    })
  end

  defp reconcile_page(ctx, cursor, reservation_limit, now_ms) do
    result = run_page(ctx, cursor, reservation_limit, now_ms)
    Telemetry.emit(:limit_reconcile, result, reconcile_metadata(result))
  end

  defp run_page(ctx, cursor, reservation_limit, now_ms)
       when is_integer(reservation_limit) and reservation_limit > 0 and
              reservation_limit <= 256 and
              is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_exact_version do
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
