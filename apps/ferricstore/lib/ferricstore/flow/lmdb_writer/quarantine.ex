defmodule Ferricstore.Flow.LMDBWriter.Quarantine do
  @moduledoc false

  # A Flow projection that fails because of that Flow's own data (an
  # undecodable source record, a corrupt or conflicting LMDB row of its own) is
  # held here instead of failing every later batch on its shard.
  #
  # Holding a projection keeps the integrity rules of a failed flush:
  #   * the shard's mirror is marked degraded, so reads that need a complete
  #     mirror still refuse;
  #   * the replay-safe index does not advance while anything is held, so a
  #     restart replays the held projection;
  #   * the held Flow's after-flush actions (hot prune, hibernation evict,
  #     tombstone cleanup) wait with it, so its hot copy is never dropped
  #     while LMDB lacks it.
  #
  # Held projections are retried on their own with backoff and released once
  # they write, for example after the record is repaired or deleted.

  @default_retry_ms 1_000
  @max_retry_ms 60_000
  @max_held_actions 64

  # Failures attributable to one Flow's data. Environmental failures (I/O,
  # LMDB map full or busy, a source location not yet readable) are not listed:
  # they would fail every Flow alike and keep the whole batch pending instead.
  @isolatable_reasons [
    :invalid_source_flow_record,
    :source_flow_identity_mismatch,
    :invalid_flow_query_row,
    :invalid_query_row,
    :invalid_query_source_catalog_entry,
    :invalid_active_index_reverse,
    :invalid_terminal_index_value,
    :terminal_reverse_mismatch,
    :invalid_composite_projection,
    :composite_projection_too_large,
    :metadata_too_large
  ]

  @isolatable_reason_tags [
    :compare_failed,
    :invalid_active_index_reverse_read,
    :invalid_active_index_value,
    :active_index_reverse_state_mismatch,
    :invalid_active_timeout_index_value,
    :invalid_terminal_index_value,
    :invalid_terminal_index_read,
    :invalid_terminal_reverse_read,
    :invalid_terminal_state_read
  ]

  @type entry :: %{
          ops: [tuple()],
          after_flush: [term()],
          reason: term(),
          since_ms: integer(),
          attempts: non_neg_integer(),
          next_retry_ms: integer()
        }

  @spec entries(map()) :: %{binary() => entry()}
  def entries(state), do: Map.get(state, :quarantine, %{})

  @spec count(map()) :: non_neg_integer()
  def count(state), do: map_size(entries(state))

  @spec empty?(map()) :: boolean()
  def empty?(state), do: count(state) == 0

  @spec reply(map()) :: :ok | {:error, {:flow_projection_quarantined, pos_integer()}}
  def reply(state) do
    case count(state) do
      0 -> :ok
      held -> {:error, {:flow_projection_quarantined, held}}
    end
  end

  @spec isolatable?(term()) :: boolean()
  def isolatable?(reason) when is_atom(reason), do: reason in @isolatable_reasons

  def isolatable?(reason) when is_tuple(reason) and tuple_size(reason) > 0,
    do: elem(reason, 0) in @isolatable_reason_tags

  def isolatable?(_reason), do: false

  @doc "The Flow state key a source projection op projects, if it is one."
  @spec op_state_key(term()) :: {:ok, binary()} | :error
  def op_state_key({kind, key})
      when kind in [:project_flow_state_from_source, :project_flow_query_state_from_source] and
             is_binary(key),
      do: {:ok, key}

  def op_state_key({kind, key, version})
      when kind in [:project_flow_state_from_source, :project_flow_query_state_from_source] and
             is_binary(key) and is_integer(version),
      do: {:ok, key}

  def op_state_key(_op), do: :error

  @doc "The Flow state key an after-flush action works on, if it has one."
  @spec action_state_key(term()) :: {:ok, binary()} | :error
  def action_state_key({:defer_after_flush, _delay_ms, action}), do: action_state_key(action)

  def action_state_key(action)
      when is_tuple(action) and tuple_size(action) > 8 and
             elem(action, 0) in [:prune_terminal_flow, :prune_terminal_flow_from_source] do
    case elem(action, 8) do
      key when is_binary(key) -> {:ok, key}
      _invalid -> :error
    end
  end

  def action_state_key({:hibernate_flow_evict_hot, %{state_key: key}}) when is_binary(key),
    do: {:ok, key}

  def action_state_key({:delete_flow_tombstone, _ets, key}) when is_binary(key), do: {:ok, key}
  def action_state_key(_action), do: :error

  @doc """
  Moves pending ops and after-flush actions of held Flows into their entries.
  Returns the state and the ops and actions left to flush, in order.
  """
  @spec divert(map(), [term()], [term()]) :: {map(), [term()], [term()]}
  def divert(state, ops, after_flush) do
    held = entries(state)

    if map_size(held) == 0 do
      {state, ops, after_flush}
    else
      {held, kept_ops} = divert_ops(held, ops)
      {held, kept_actions} = divert_actions(held, after_flush)
      {Map.put(state, :quarantine, held), kept_ops, kept_actions}
    end
  end

  @doc "Moves after-flush actions of held Flows into their entries."
  @spec divert_after_flush(map(), [term()]) :: {map(), [term()]}
  def divert_after_flush(state, after_flush) do
    {held, kept} = divert_actions(entries(state), after_flush)
    {Map.put(state, :quarantine, held), kept}
  end

  defp divert_ops(held, ops) do
    {held, reversed} =
      Enum.reduce(ops, {held, []}, fn op, {held, kept} ->
        with {:ok, key} <- op_state_key(op),
             %{} = entry <- Map.get(held, key) do
          {Map.put(held, key, %{entry | ops: merge_op(entry.ops, op)}), kept}
        else
          _not_held -> {held, [op | kept]}
        end
      end)

    {held, Enum.reverse(reversed)}
  end

  defp divert_actions(held, actions) when map_size(held) == 0, do: {held, actions}

  defp divert_actions(held, actions) do
    {held, reversed} =
      Enum.reduce(actions, {held, []}, fn action, {held, kept} ->
        with {:ok, key} <- action_state_key(action),
             %{} = entry <- Map.get(held, key) do
          {Map.put(held, key, %{entry | after_flush: hold_action(entry.after_flush, action)}),
           kept}
        else
          _not_held -> {held, [action | kept]}
        end
      end)

    {held, Enum.reverse(reversed)}
  end

  # Source projections re-read the current record, so the latest op of each
  # kind (full or query-only) is enough to bring a held Flow up to date.
  defp merge_op(ops, op) do
    kind = elem(op, 0)
    Enum.reject(ops, &(elem(&1, 0) == kind)) ++ [op]
  end

  defp hold_action(actions, action), do: Enum.take(actions ++ [action], -@max_held_actions)

  @doc "Holds `key`'s projection, or records another failure of a held one."
  @spec hold(map(), binary(), term(), term(), integer()) :: {map(), :new | :existing}
  def hold(state, key, op, reason, now_ms) do
    held = entries(state)

    case Map.get(held, key) do
      nil ->
        entry = %{
          ops: [op],
          after_flush: [],
          reason: reason,
          since_ms: now_ms,
          attempts: 0,
          next_retry_ms: now_ms + retry_delay_ms(0)
        }

        {Map.put(state, :quarantine, Map.put(held, key, entry)), :new}

      entry ->
        entry = %{entry | ops: merge_op(entry.ops, op), reason: reason}
        {Map.put(state, :quarantine, Map.put(held, key, entry)), :existing}
    end
  end

  @spec due(map(), integer()) :: [{binary(), entry()}]
  def due(state, now_ms) do
    state
    |> entries()
    |> Enum.filter(fn {_key, entry} -> entry.next_retry_ms <= now_ms end)
    |> Enum.sort_by(fn {_key, entry} -> entry.next_retry_ms end)
  end

  @spec release(map(), binary()) :: map()
  def release(state, key), do: Map.put(state, :quarantine, Map.delete(entries(state), key))

  @spec back_off(map(), binary(), term(), integer()) :: map()
  def back_off(state, key, reason, now_ms) do
    held = entries(state)

    case Map.get(held, key) do
      nil ->
        state

      entry ->
        attempts = entry.attempts + 1

        entry = %{
          entry
          | reason: reason,
            attempts: attempts,
            next_retry_ms: now_ms + retry_delay_ms(attempts)
        }

        Map.put(state, :quarantine, Map.put(held, key, entry))
    end
  end

  @doc "Milliseconds until the next held projection is due, or nil when none is held."
  @spec next_retry_in_ms(map(), integer()) :: non_neg_integer() | nil
  def next_retry_in_ms(state, now_ms) do
    case Map.values(entries(state)) do
      [] -> nil
      held -> held |> Enum.map(& &1.next_retry_ms) |> Enum.min() |> Kernel.-(now_ms) |> max(0)
    end
  end

  defp retry_delay_ms(attempts) do
    base =
      case Application.get_env(:ferricstore, :flow_lmdb_quarantine_retry_ms, @default_retry_ms) do
        value when is_integer(value) and value > 0 -> value
        _invalid -> @default_retry_ms
      end

    min(base * Integer.pow(2, min(attempts, 16)), max(@max_retry_ms, base))
  end
end
