defmodule Ferricstore.Raft.ApplyContext do
  @moduledoc """
  Immutable runtime limits used by deterministic Raft apply code.

  The context is captured before a command reaches apply and stored with the
  replicated state-machine state. It contains only versioned, serializable
  values so snapshots and replay never depend on node-local application or
  process configuration.
  """

  @version 1
  @max_retention_ttl_ms 31_536_000_000
  @max_history_hot_max_events 10_000
  @max_history_max_events 1_000_000
  @max_retention_cleanup_key_budget 100_000
  @max_retention_cleanup_byte_budget 64 * 1_024 * 1_024
  @max_lmdb_cleanup_scan_limit 1_000_000
  @max_hibernation_window_ms @max_retention_ttl_ms

  @default_retention_ttl_ms 604_800_000
  @default_history_hot_max_events 0
  @default_history_max_events 100_000
  @default_max_history_hot_max_events 10_000
  @default_max_history_max_events 1_000_000
  @default_retention_cleanup_key_budget 1_024
  @default_retention_cleanup_byte_budget 8 * 1_024 * 1_024
  @default_lmdb_cleanup_scan_limit 100_000

  @flow_command_tags [
    :flow_cancel,
    :flow_cancel_many,
    :flow_claim_due,
    :flow_complete,
    :flow_complete_many,
    :flow_cross_policy_put,
    :flow_cross_retention_cleanup,
    :flow_cross_spawn_children,
    :flow_cross_terminal,
    :flow_cross_terminal_many,
    :flow_create,
    :flow_create_many,
    :flow_create_pipeline_batch,
    :flow_extend_lease,
    :flow_fail,
    :flow_fail_many,
    :flow_governance_limit_mutate,
    :flow_named_value_put,
    :flow_named_value_put_pipeline_batch,
    :flow_policy_catalog_backfill_step,
    :flow_policy_migration_step,
    :flow_policy_put,
    :flow_reschedule,
    :flow_retention_cleanup,
    :flow_retry,
    :flow_retry_many,
    :flow_rewind,
    :flow_run_steps_many,
    :flow_schedule_replace,
    :flow_signal,
    :flow_signal_many,
    :flow_spawn_children,
    :flow_start_and_claim,
    :flow_start_and_claim_pipeline_batch,
    :flow_step_continue,
    :flow_step_continue_many,
    :flow_terminal_pipeline_batch,
    :flow_transition,
    :flow_transition_many
  ]

  @context_wrapper_tags [
    :async,
    :batch,
    :cross_shard_tx,
    :ferricstore_apply_context,
    :ferricstore_latency_trace,
    :flow_shared_ref_write,
    :ttb
  ]
  @context_command_tags @flow_command_tags ++ @context_wrapper_tags

  @default_hibernation_hot_window_ms Application.compile_env(
                                       :ferricstore,
                                       :flow_hibernation_hot_window_ms,
                                       5 * 60 * 1_000
                                     )
  @default_hibernation_safety_margin_ms Application.compile_env(
                                          :ferricstore,
                                          :flow_hibernation_safety_margin_ms,
                                          0
                                        )
  @default_hibernation_promote_window_ms Application.compile_env(
                                           :ferricstore,
                                           :flow_hibernation_promote_window_ms,
                                           60 * 1_000
                                         )
  @default_hibernation_late_promote_window_ms Application.compile_env(
                                                :ferricstore,
                                                :flow_hibernation_late_promote_window_ms,
                                                5 * 60 * 1_000
                                              )

  @enforce_keys [:version]
  defstruct version: @version,
            flow_default_retention_ttl_ms: @default_retention_ttl_ms,
            flow_default_history_hot_max_events: @default_history_hot_max_events,
            flow_default_history_max_events: @default_history_max_events,
            flow_max_history_hot_max_events: @default_max_history_hot_max_events,
            flow_max_history_max_events: @default_max_history_max_events,
            flow_retention_cleanup_key_budget: @default_retention_cleanup_key_budget,
            flow_retention_cleanup_byte_budget: @default_retention_cleanup_byte_budget,
            flow_lmdb_history_cleanup_scan_limit: @default_lmdb_cleanup_scan_limit,
            flow_lmdb_value_cleanup_scan_limit: @default_lmdb_cleanup_scan_limit,
            flow_hibernation_enabled: true,
            flow_hibernation_hot_window_ms: @default_hibernation_hot_window_ms,
            flow_hibernation_safety_margin_ms: @default_hibernation_safety_margin_ms,
            flow_hibernation_promote_window_ms: @default_hibernation_promote_window_ms,
            flow_hibernation_late_promote_window_ms: @default_hibernation_late_promote_window_ms

  @type t :: %__MODULE__{
          version: pos_integer(),
          flow_default_retention_ttl_ms: pos_integer(),
          flow_default_history_hot_max_events: non_neg_integer(),
          flow_default_history_max_events: pos_integer(),
          flow_max_history_hot_max_events: pos_integer(),
          flow_max_history_max_events: pos_integer(),
          flow_retention_cleanup_key_budget: pos_integer(),
          flow_retention_cleanup_byte_budget: pos_integer(),
          flow_lmdb_history_cleanup_scan_limit: pos_integer(),
          flow_lmdb_value_cleanup_scan_limit: pos_integer(),
          flow_hibernation_enabled: boolean(),
          flow_hibernation_hot_window_ms: non_neg_integer(),
          flow_hibernation_safety_margin_ms: non_neg_integer(),
          flow_hibernation_promote_window_ms: non_neg_integer(),
          flow_hibernation_late_promote_window_ms: non_neg_integer()
        }

  @type encoded ::
          {:flow_apply_context_v1, pos_integer(), pos_integer(), non_neg_integer(), pos_integer(),
           pos_integer(), pos_integer(), pos_integer(), pos_integer(), pos_integer(), boolean(),
           non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @spec default() :: t()
  def default, do: new([])

  @spec new(keyword() | map()) :: t()
  def new(values) when is_map(values), do: values |> Map.to_list() |> new()

  def new(values) when is_list(values) do
    max_history_hot =
      values
      |> Keyword.get(:flow_max_history_hot_max_events, @default_max_history_hot_max_events)
      |> positive(@default_max_history_hot_max_events)
      |> min(@max_history_hot_max_events)

    max_history =
      values
      |> Keyword.get(:flow_max_history_max_events, @default_max_history_max_events)
      |> positive(@default_max_history_max_events)
      |> min(@max_history_max_events)

    default_hot =
      values
      |> Keyword.get(:flow_default_history_hot_max_events, @default_history_hot_max_events)
      |> non_negative(@default_history_hot_max_events)
      |> min(max_history_hot)

    default_history =
      values
      |> Keyword.get(:flow_default_history_max_events, @default_history_max_events)
      |> positive(@default_history_max_events)
      |> min(max_history)
      |> max(default_hot)

    %__MODULE__{
      version: @version,
      flow_default_retention_ttl_ms:
        bounded_positive(
          Keyword.get(
            values,
            :flow_default_retention_ttl_ms,
            @default_retention_ttl_ms
          ),
          @max_retention_ttl_ms,
          @default_retention_ttl_ms
        ),
      flow_default_history_hot_max_events: default_hot,
      flow_default_history_max_events: default_history,
      flow_max_history_hot_max_events: max_history_hot,
      flow_max_history_max_events: max_history,
      flow_retention_cleanup_key_budget:
        values
        |> Keyword.get(
          :flow_retention_cleanup_key_budget,
          @default_retention_cleanup_key_budget
        )
        |> positive_minimum(8, @default_retention_cleanup_key_budget)
        |> min(@max_retention_cleanup_key_budget),
      flow_retention_cleanup_byte_budget:
        values
        |> Keyword.get(
          :flow_retention_cleanup_byte_budget,
          @default_retention_cleanup_byte_budget
        )
        |> positive_minimum(4_096, @default_retention_cleanup_byte_budget)
        |> min(@max_retention_cleanup_byte_budget),
      flow_lmdb_history_cleanup_scan_limit:
        values
        |> Keyword.get(
          :flow_lmdb_history_cleanup_scan_limit,
          @default_lmdb_cleanup_scan_limit
        )
        |> positive(@default_lmdb_cleanup_scan_limit)
        |> min(@max_lmdb_cleanup_scan_limit),
      flow_lmdb_value_cleanup_scan_limit:
        values
        |> Keyword.get(
          :flow_lmdb_value_cleanup_scan_limit,
          @default_lmdb_cleanup_scan_limit
        )
        |> positive(@default_lmdb_cleanup_scan_limit)
        |> min(@max_lmdb_cleanup_scan_limit),
      flow_hibernation_enabled: Keyword.get(values, :flow_hibernation_enabled, true) == true,
      flow_hibernation_hot_window_ms:
        values
        |> Keyword.get(:flow_hibernation_hot_window_ms, @default_hibernation_hot_window_ms)
        |> non_negative(@default_hibernation_hot_window_ms)
        |> min(@max_hibernation_window_ms),
      flow_hibernation_safety_margin_ms:
        values
        |> Keyword.get(
          :flow_hibernation_safety_margin_ms,
          @default_hibernation_safety_margin_ms
        )
        |> non_negative(@default_hibernation_safety_margin_ms)
        |> min(@max_hibernation_window_ms),
      flow_hibernation_promote_window_ms:
        values
        |> Keyword.get(
          :flow_hibernation_promote_window_ms,
          @default_hibernation_promote_window_ms
        )
        |> non_negative(@default_hibernation_promote_window_ms)
        |> min(@max_hibernation_window_ms),
      flow_hibernation_late_promote_window_ms:
        values
        |> Keyword.get(
          :flow_hibernation_late_promote_window_ms,
          @default_hibernation_late_promote_window_ms
        )
        |> non_negative(@default_hibernation_late_promote_window_ms)
        |> min(@max_hibernation_window_ms)
    }
  end

  @spec from_runtime(keyword() | map()) :: t()
  def from_runtime(overrides \\ []) do
    overrides = if is_map(overrides), do: Map.to_list(overrides), else: overrides

    __struct__()
    |> Map.from_struct()
    |> Map.delete(:version)
    |> Map.keys()
    |> Enum.reduce(overrides, fn key, acc ->
      if Keyword.has_key?(acc, key) do
        acc
      else
        Keyword.put(
          acc,
          key,
          Application.get_env(:ferricstore, key, Map.fetch!(__struct__(), key))
        )
      end
    end)
    |> new()
  end

  @spec encode(t()) :: encoded()
  def encode(%__MODULE__{} = context) do
    {:flow_apply_context_v1, context.flow_default_retention_ttl_ms,
     context.flow_default_history_hot_max_events, context.flow_default_history_max_events,
     context.flow_max_history_hot_max_events, context.flow_max_history_max_events,
     context.flow_retention_cleanup_key_budget, context.flow_retention_cleanup_byte_budget,
     context.flow_lmdb_history_cleanup_scan_limit, context.flow_lmdb_value_cleanup_scan_limit,
     context.flow_hibernation_enabled, context.flow_hibernation_hot_window_ms,
     context.flow_hibernation_safety_margin_ms, context.flow_hibernation_promote_window_ms,
     context.flow_hibernation_late_promote_window_ms}
  end

  @spec decode(term()) :: {:ok, t()} | {:error, :invalid_apply_context}
  def decode(
        {:flow_apply_context_v1, retention_ttl_ms, history_hot, history_max, max_history_hot,
         max_history, cleanup_keys, cleanup_bytes, history_scan, value_scan, hibernation_enabled,
         hot_window_ms, safety_margin_ms, promote_window_ms, late_promote_window_ms} = encoded
      ) do
    context =
      new(
        flow_default_retention_ttl_ms: retention_ttl_ms,
        flow_default_history_hot_max_events: history_hot,
        flow_default_history_max_events: history_max,
        flow_max_history_hot_max_events: max_history_hot,
        flow_max_history_max_events: max_history,
        flow_retention_cleanup_key_budget: cleanup_keys,
        flow_retention_cleanup_byte_budget: cleanup_bytes,
        flow_lmdb_history_cleanup_scan_limit: history_scan,
        flow_lmdb_value_cleanup_scan_limit: value_scan,
        flow_hibernation_enabled: hibernation_enabled,
        flow_hibernation_hot_window_ms: hot_window_ms,
        flow_hibernation_safety_margin_ms: safety_margin_ms,
        flow_hibernation_promote_window_ms: promote_window_ms,
        flow_hibernation_late_promote_window_ms: late_promote_window_ms
      )

    if encode(context) == encoded do
      {:ok, context}
    else
      {:error, :invalid_apply_context}
    end
  end

  def decode(_invalid), do: {:error, :invalid_apply_context}

  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{} = context), do: decode(encode(context)) == {:ok, context}
  def valid?(_other), do: false

  @spec wrap_command(tuple(), t()) :: tuple()
  def wrap_command(command, %__MODULE__{} = context) when is_tuple(command) do
    if context_command_shape?(command) do
      command
      |> sanitize_reserved_wrappers(context)
      |> wrap_sanitized_command(context)
    else
      command
    end
  end

  defp context_command_shape?({inner, %{hlc_ts: {physical_ms, logical}}})
       when is_tuple(inner) and is_integer(physical_ms) and physical_ms >= 0 and
              is_integer(logical) and logical >= 0,
       do: true

  defp context_command_shape?(command)
       when tuple_size(command) > 0 and elem(command, 0) in @context_command_tags,
       do: true

  defp context_command_shape?(_command), do: false

  defp wrap_sanitized_command(
         {inner, %{hlc_ts: {physical_ms, logical}} = metadata} = command,
         context
       )
       when is_tuple(inner) and is_integer(physical_ms) and physical_ms >= 0 and
              is_integer(logical) and logical >= 0 do
    case wrap_sanitized_command(inner, context) do
      ^inner -> command
      wrapped -> {wrapped, metadata}
    end
  end

  defp wrap_sanitized_command(command, context) do
    if flow_command?(command) do
      {:ferricstore_apply_context, encode(context), command}
    else
      command
    end
  end

  defp sanitize_reserved_wrappers(
         {:ferricstore_apply_context, _untrusted, inner},
         context
       )
       when is_tuple(inner),
       do: sanitize_reserved_wrappers(inner, context)

  defp sanitize_reserved_wrappers(
         {:ferricstore_apply_context, _untrusted, invalid_inner},
         context
       ),
       do: {:ferricstore_apply_context, encode(context), invalid_inner}

  defp sanitize_reserved_wrappers({:ttb, binary}, context) when is_binary(binary) do
    case decode_preencoded_command(binary) do
      {:ok, decoded} when is_tuple(decoded) ->
        sanitize_reserved_wrappers(decoded, context)

      _invalid ->
        {:ferricstore_apply_context, encode(context), :invalid_preencoded_command}
    end
  end

  defp sanitize_reserved_wrappers(
         {:ferricstore_latency_trace, inner} = command,
         context
       )
       when is_tuple(inner) do
    case sanitize_reserved_wrappers(inner, context) do
      ^inner -> command
      sanitized -> {:ferricstore_latency_trace, sanitized}
    end
  end

  defp sanitize_reserved_wrappers({:async, origin, inner} = command, context)
       when is_tuple(inner) do
    case sanitize_reserved_wrappers(inner, context) do
      ^inner -> command
      sanitized -> {:async, origin, sanitized}
    end
  end

  defp sanitize_reserved_wrappers(
         {:flow_shared_ref_write, shard_index, inner} = command,
         context
       )
       when is_tuple(inner) do
    case sanitize_reserved_wrappers(inner, context) do
      ^inner -> command
      sanitized -> {:flow_shared_ref_write, shard_index, sanitized}
    end
  end

  defp sanitize_reserved_wrappers({:batch, commands} = command, context)
       when is_list(commands) do
    case sanitize_command_list(commands, context) do
      {^commands, false} -> command
      {sanitized, true} -> {:batch, sanitized}
    end
  end

  defp sanitize_reserved_wrappers(
         {inner, %{hlc_ts: {physical_ms, logical}} = metadata} = command,
         context
       )
       when is_tuple(inner) and is_integer(physical_ms) and physical_ms >= 0 and
              is_integer(logical) and logical >= 0 do
    case sanitize_reserved_wrappers(inner, context) do
      ^inner -> command
      sanitized -> {sanitized, metadata}
    end
  end

  defp sanitize_reserved_wrappers(command, _context), do: command

  defp sanitize_command_list([], _context), do: {[], false}

  defp sanitize_command_list([command | rest] = commands, context) do
    sanitized_command = sanitize_reserved_wrappers(command, context)
    {sanitized_rest, rest_changed?} = sanitize_command_list(rest, context)

    if sanitized_command == command and not rest_changed? do
      {commands, false}
    else
      {[sanitized_command | sanitized_rest], true}
    end
  end

  defp decode_preencoded_command(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  end

  defp flow_command?({:cross_shard_tx, shard_batches}), do: nested_flow_command?(shard_batches)

  defp flow_command?({:cross_shard_tx, shard_batches, _watched_keys}),
    do: nested_flow_command?(shard_batches)

  defp flow_command?({:ferricstore_latency_trace, inner}), do: flow_command?(inner)
  defp flow_command?({:async, _origin, inner}), do: flow_command?(inner)

  defp flow_command?({inner, %{hlc_ts: {physical_ms, logical}}})
       when is_tuple(inner) and is_integer(physical_ms) and physical_ms >= 0 and
              is_integer(logical) and logical >= 0,
       do: flow_command?(inner)

  defp flow_command?({:flow_shared_ref_write, _shard_index, inner}) when is_tuple(inner),
    do: flow_command?(inner)

  defp flow_command?({:batch, commands}) when is_list(commands),
    do: Enum.any?(commands, &flow_command?/1)

  defp flow_command?(command)
       when is_tuple(command) and tuple_size(command) > 0 and
              elem(command, 0) in @flow_command_tags,
       do: true

  defp flow_command?(_command), do: false

  defp nested_flow_command?(terms) when is_list(terms),
    do: Enum.any?(terms, &nested_flow_command?/1)

  defp nested_flow_command?(term) when is_tuple(term) do
    flow_command?(term) or tuple_has_flow_command?(term, 0, tuple_size(term))
  end

  defp nested_flow_command?(_term), do: false

  defp tuple_has_flow_command?(_term, index, size) when index >= size, do: false

  defp tuple_has_flow_command?(term, index, size) do
    nested_flow_command?(elem(term, index)) or tuple_has_flow_command?(term, index + 1, size)
  end

  defp positive(value, _default) when is_integer(value) and value > 0, do: value
  defp positive(_value, default), do: default

  defp non_negative(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative(_value, default), do: default

  defp bounded_positive(value, max, _default)
       when is_integer(value) and value > 0 and value <= max,
       do: value

  defp bounded_positive(_value, _max, default), do: default

  defp positive_minimum(value, minimum, _default) when is_integer(value) and value > 0,
    do: max(value, minimum)

  defp positive_minimum(_value, _minimum, default), do: default
end
