defmodule Ferricstore.Flow.PipelineClaimDueCommand do
  @moduledoc false

  alias Ferricstore.Flow.ClaimFilter
  alias Ferricstore.Store.Router

  import Ferricstore.Flow.Options,
    only: [
      maybe_put_attr: 3,
      maybe_put_keyword: 3,
      optional_boolean: 3,
      required_binary: 2
    ]

  @default_lease_ms 30_000
  @default_limit 1
  @default_max_claim_limit 1_000
  @max_exact_ms 9_007_199_254_740_991
  @max_priority 2

  def command({:claim_due, type, opts}, callbacks) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts, return: true),
         :ok <- validate_type(type),
         {:ok, state} <- optional_claim_states(opts),
         {:ok, worker} <- required_binary(opts, :worker),
         {:ok, lease_ms} <- optional_pos_integer(opts, :lease_ms, @default_lease_ms),
         {:ok, limit} <- optional_claim_limit(opts),
         {:ok, priority} <- optional_priority_or_nil(opts),
         {:ok, now} <- callbacks.optional_now_ms.(opts),
         :ok <- validate_deadline(now, lease_ms, :lease_ms),
         {:ok, return_mode} <- optional_claim_return(opts),
         {:ok, payload_return} <- callbacks.payload_return_opts.(opts, return_mode == :records),
         {:ok, named_values} <- callbacks.named_value_return_opts.(opts),
         {:ok, reclaim_expired?} <- optional_boolean(opts, :reclaim_expired, true),
         {:ok, reclaim_ratio} <- optional_reclaim_ratio(opts),
         {:ok, partition_key, partition_keys} <- optional_claim_partitions(opts),
         partition_filter = partition_keys || partition_key,
         :ok <- validate_claim_due_keys(type, state, priority, partition_filter) do
      normalized_opts =
        normalized_opts(
          state,
          worker,
          lease_ms,
          limit,
          priority,
          now,
          return_mode,
          payload_return,
          reclaim_expired?,
          reclaim_ratio,
          partition_key,
          partition_keys,
          named_values
        )

      attrs =
        %{
          type: type,
          state: state,
          worker: worker,
          lease_ms: lease_ms,
          limit: limit,
          priority: priority,
          partition_key: partition_key
        }
        |> maybe_put_attr(:partition_keys, partition_keys)
        |> maybe_put_attr(:now_ms, now)

      queue_key = {type, state, priority, now, partition_filter}

      key =
        {type, state, worker, lease_ms, priority, now, return_mode, payload_return, named_values,
         reclaim_expired?, reclaim_ratio, partition_filter}

      {:ok,
       %{
         type: type,
         attrs: attrs,
         opts: normalized_opts,
         limit: limit,
         key: key,
         queue_key: queue_key,
         return_mode: return_mode,
         payload_return: payload_return,
         named_values: named_values,
         reclaim_expired?: reclaim_expired?,
         reclaim_ratio: reclaim_ratio,
         groupable?: true
       }}
    end
  end

  def command(_op, _callbacks), do: {:error, "ERR unsupported flow pipeline command"}

  defp normalized_opts(
         state,
         worker,
         lease_ms,
         limit,
         priority,
         now,
         return_mode,
         payload_return,
         reclaim_expired?,
         reclaim_ratio,
         partition_key,
         partition_keys,
         named_values
       ) do
    [
      state: state,
      worker: worker,
      lease_ms: lease_ms,
      limit: limit,
      return: return_mode,
      payload: payload_return.enabled?,
      payload_max_bytes: payload_return.max_bytes,
      reclaim_expired: reclaim_expired?,
      reclaim_ratio: reclaim_ratio
    ]
    |> maybe_put_keyword(:priority, priority)
    |> maybe_put_keyword(:now_ms, now)
    |> maybe_put_keyword(:partition_key, partition_key)
    |> maybe_put_keyword(:partition_keys, partition_keys)
    |> maybe_put_keyword(:values, named_values)
  end

  defp validate_opts(opts, allowed) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, "ERR flow opts must be a keyword list"}

      Keyword.has_key?(opts, :return) and not Keyword.get(allowed, :return, false) ->
        {:error, "ERR flow return option is not supported"}

      Keyword.has_key?(opts, :block_ms) ->
        {:error, "ERR flow block_ms is not supported in pipelines"}

      true ->
        :ok
    end
  end

  defp validate_type(type) when is_binary(type) and type != "", do: :ok
  defp validate_type(_type), do: {:error, "ERR flow type must be a non-empty string"}

  defp optional_claim_limit(opts) do
    with {:ok, limit} <- optional_pos_integer(opts, :limit, @default_limit) do
      max = flow_max_claim_limit()

      if limit <= max do
        {:ok, limit}
      else
        {:error, "ERR flow limit exceeds maximum #{max}"}
      end
    end
  end

  defp optional_reclaim_ratio(opts) do
    case Keyword.get(opts, :reclaim_ratio, 25) do
      value when is_integer(value) and value >= 0 and value <= 100 -> {:ok, value}
      _ -> {:error, "ERR flow reclaim_ratio must be an integer between 0 and 100"}
    end
  end

  defp flow_max_claim_limit do
    case Application.get_env(:ferricstore, :flow_max_claim_limit, @default_max_claim_limit) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_claim_limit
    end
  end

  defp optional_priority_or_nil(opts) do
    case Keyword.get(opts, :priority, nil) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 and value <= @max_priority -> {:ok, value}
      _ -> {:error, "ERR flow priority must be between 0 and #{@max_priority}"}
    end
  end

  defp optional_pos_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 and value <= @max_exact_ms ->
        {:ok, value}

      value when is_integer(value) and value > @max_exact_ms ->
        {:error, "ERR flow #{key} exceeds maximum #{@max_exact_ms}"}

      _ -> {:error, "ERR flow #{key} must be a positive integer"}
    end
  end

  defp validate_deadline(nil, _duration_ms, _key), do: :ok

  defp validate_deadline(now_ms, duration_ms, _key)
       when now_ms <= @max_exact_ms - duration_ms,
       do: :ok

  defp validate_deadline(_now_ms, _duration_ms, key),
    do: {:error, "ERR flow #{key} deadline exceeds maximum #{@max_exact_ms}"}

  defp validate_claim_due_keys(type, state, nil, partition_key) do
    with :ok <- ClaimFilter.validate_footprint(state, partition_key) do
      validate_claim_due_key_lengths(type, state, nil, partition_key, Router.max_key_size())
    end
  end

  defp validate_claim_due_keys(type, state, priority, partition_key) do
    with :ok <- ClaimFilter.validate_footprint(state, partition_key) do
      validate_claim_due_key_lengths(type, state, priority, partition_key, Router.max_key_size())
    end
  end

  defp validate_claim_due_key_lengths(type, :any, priority, partition_filter, max_key_size) do
    validate_generated_key_size(
      due_any_key_size(
        type,
        priority_key_size(priority),
        max_claim_partition_tag_size(partition_filter)
      ),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, states, priority, partition_keys, max_key_size)
       when is_list(states) and is_list(partition_keys) do
    state_size = max_binary_size(states)
    tag_size = max_partition_tag_size(partition_keys)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, state_size, priority_size, tag_size),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, state, priority, partition_keys, max_key_size)
       when is_binary(state) and is_list(partition_keys) do
    tag_size = max_partition_tag_size(partition_keys)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, byte_size(state), priority_size, tag_size),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, states, priority, partition_key, max_key_size)
       when is_list(states) do
    state_size = max_binary_size(states)
    tag_size = partition_tag_size(partition_key)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, state_size, priority_size, tag_size),
      max_key_size
    )
  end

  defp validate_claim_due_key_lengths(type, state, priority, partition_key, max_key_size) do
    tag_size = partition_tag_size(partition_key)
    priority_size = priority_key_size(priority)

    validate_generated_key_size(
      due_key_size(type, byte_size(state), priority_size, tag_size),
      max_key_size
    )
  end

  defp due_key_size(type, state_size, priority_size, tag_size),
    do:
      2 + tag_size + 3 + encoded_index_component_size(byte_size(type)) + 1 +
        encoded_index_component_size(state_size) + 2 + priority_size

  defp due_any_key_size(type, priority_size, tag_size),
    do: 2 + tag_size + 4 + encoded_index_component_size(byte_size(type)) + 2 + priority_size

  defp encoded_index_component_size(size) when is_integer(size) and size >= 0,
    do: div(size * 4 + 2, 3)

  defp priority_key_size(nil), do: max_key_priority_len()
  defp priority_key_size(priority), do: integer_decimal_size(priority)

  defp max_key_priority_len, do: integer_decimal_size(@max_priority)

  defp integer_decimal_size(value) when value < 10, do: 1

  defp integer_decimal_size(value),
    do: value |> Integer.to_string() |> byte_size()

  defp max_binary_size([head | tail]) do
    Enum.reduce(tail, byte_size(head), fn value, max_size ->
      max(max_size, byte_size(value))
    end)
  end

  defp max_partition_tag_size([head | tail]) do
    Enum.reduce(tail, partition_tag_size(head), fn partition_key, max_size ->
      max(max_size, partition_tag_size(partition_key))
    end)
  end

  defp max_claim_partition_tag_size(partition_keys) when is_list(partition_keys),
    do: max_partition_tag_size(partition_keys)

  defp max_claim_partition_tag_size(partition_key), do: partition_tag_size(partition_key)

  defp partition_tag_size(nil), do: 3
  defp partition_tag_size(:any), do: 3
  defp partition_tag_size(:auto), do: 3

  defp partition_tag_size(partition_key),
    do: partition_key |> Ferricstore.Flow.Keys.tag() |> byte_size()

  defp validate_generated_key_size(size, max_key_size) when size <= max_key_size, do: :ok

  defp validate_generated_key_size(_size, max_key_size),
    do: {:error, "ERR key too large (max #{max_key_size} bytes)"}

  defp optional_claim_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil ->
        {:ok, :auto}

      :any ->
        {:ok, :any}

      :auto ->
        {:ok, :auto}

      :global ->
        {:ok, nil}

      value when is_binary(value) and value != "" ->
        case String.upcase(value) do
          "ANY" -> {:ok, :any}
          "AUTO" -> {:ok, :auto}
          "GLOBAL" -> {:ok, nil}
          _ -> {:ok, value}
        end

      _ ->
        optional_partition_key(opts)
    end
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string or :global"}
    end
  end

  defp optional_claim_partitions(opts) do
    case Keyword.fetch(opts, :partition_keys) do
      :error ->
        with {:ok, partition_key} <- optional_claim_partition_key(opts) do
          {:ok, partition_key, nil}
        end

      {:ok, partition_keys} ->
        cond do
          Keyword.has_key?(opts, :partition_key) ->
            {:error, "ERR flow partition_key and partition_keys are mutually exclusive"}

          not is_list(partition_keys) or partition_keys == [] ->
            {:error, "ERR flow partition_keys must be a non-empty list"}

          true ->
            normalize_claim_partition_keys(partition_keys)
        end
    end
  end

  defp normalize_claim_partition_keys(partition_keys) do
    if Enum.all?(partition_keys, &(is_binary(&1) and &1 != "")) do
      {:ok, nil, Enum.uniq(partition_keys)}
    else
      {:error, "ERR flow partition_keys must be non-empty strings"}
    end
  end

  defp optional_claim_return(opts) do
    case Keyword.get(opts, :return, :records) do
      value when value in [:records, :record, :full] ->
        {:ok, :records}

      value when value in [:jobs, :job] ->
        {:ok, :jobs}

      value when value in [:jobs_compact, :job_compact] ->
        {:ok, :jobs_compact}

      value when value in [:jobs_compact_attrs, :job_compact_attrs] ->
        {:ok, :jobs_compact_attrs}

      value
      when value in [
             :jobs_compact_state,
             :job_compact_state,
             :jobs_compact_with_state,
             :job_compact_with_state
           ] ->
        {:ok, :jobs_compact_state}

      value
      when value in [
             :jobs_compact_state_attrs,
             :job_compact_state_attrs,
             :jobs_compact_with_state_attrs,
             :job_compact_with_state_attrs
           ] ->
        {:ok, :jobs_compact_state_attrs}

      value when is_binary(value) ->
        case String.upcase(value) do
          "RECORDS" -> {:ok, :records}
          "RECORD" -> {:ok, :records}
          "FULL" -> {:ok, :records}
          "JOBS" -> {:ok, :jobs}
          "JOB" -> {:ok, :jobs}
          "JOBS_COMPACT" -> {:ok, :jobs_compact}
          "JOB_COMPACT" -> {:ok, :jobs_compact}
          "JOBS_COMPACT_ATTRS" -> {:ok, :jobs_compact_attrs}
          "JOB_COMPACT_ATTRS" -> {:ok, :jobs_compact_attrs}
          "JOBS_COMPACT_ATTRIBUTES" -> {:ok, :jobs_compact_attrs}
          "JOB_COMPACT_ATTRIBUTES" -> {:ok, :jobs_compact_attrs}
          "JOBS_COMPACT_STATE" -> {:ok, :jobs_compact_state}
          "JOB_COMPACT_STATE" -> {:ok, :jobs_compact_state}
          "JOBS_COMPACT_WITH_STATE" -> {:ok, :jobs_compact_state}
          "JOB_COMPACT_WITH_STATE" -> {:ok, :jobs_compact_state}
          "JOBS_COMPACT_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
          "JOB_COMPACT_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
          "JOBS_COMPACT_WITH_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
          "JOB_COMPACT_WITH_STATE_ATTRS" -> {:ok, :jobs_compact_state_attrs}
          "JOBS_COMPACT_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
          "JOB_COMPACT_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
          "JOBS_COMPACT_WITH_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
          "JOB_COMPACT_WITH_STATE_ATTRIBUTES" -> {:ok, :jobs_compact_state_attrs}
          _ -> {:error, "ERR flow claim return must be records, jobs, or compact jobs"}
        end

      _ ->
        {:error,
         "ERR flow claim return must be records, jobs, jobs_compact, jobs_compact_attrs, jobs_compact_state, or jobs_compact_state_attrs"}
    end
  end

  defp optional_claim_states(opts) do
    state_values = Keyword.get_values(opts, :state)
    states_value = Keyword.get(opts, :states, nil)

    cond do
      state_values != [] and not is_nil(states_value) ->
        {:error, "ERR flow state and states are mutually exclusive"}

      state_values != [] ->
        normalize_claim_state_values(state_values)

      not is_nil(states_value) ->
        normalize_claim_state_values(states_value)

      true ->
        {:ok, :any}
    end
  end

  defp normalize_claim_state_values(:any), do: {:ok, :any}

  defp normalize_claim_state_values(value) when is_binary(value) do
    cond do
      claim_state_any?(value) -> {:ok, :any}
      value != "" -> {:ok, value}
      true -> {:error, "ERR flow state must be a non-empty string"}
    end
  end

  defp normalize_claim_state_values([value]) do
    cond do
      claim_state_any?(value) -> {:ok, :any}
      is_binary(value) and value != "" -> {:ok, value}
      true -> {:error, "ERR flow state must be a non-empty string"}
    end
  end

  defp normalize_claim_state_values(values) when is_list(values) do
    cond do
      values == [] ->
        {:error, "ERR flow states must be a non-empty list"}

      true ->
        normalize_claim_state_list(values)
    end
  end

  defp normalize_claim_state_values(_value),
    do: {:error, "ERR flow state must be a non-empty string"}

  defp claim_state_any?(:any), do: true
  defp claim_state_any?(<<a, n, y>>), do: ascii_a?(a) and ascii_n?(n) and ascii_y?(y)
  defp claim_state_any?(_value), do: false

  defp ascii_a?(?A), do: true
  defp ascii_a?(?a), do: true
  defp ascii_a?(_), do: false

  defp ascii_n?(?N), do: true
  defp ascii_n?(?n), do: true
  defp ascii_n?(_), do: false

  defp ascii_y?(?Y), do: true
  defp ascii_y?(?y), do: true
  defp ascii_y?(_), do: false

  defp normalize_claim_state_list(values) do
    values
    |> Enum.reduce_while({:ok, false, []}, fn value, {:ok, any?, acc} ->
      cond do
        claim_state_any?(value) ->
          {:cont, {:ok, true, acc}}

        is_binary(value) and value != "" ->
          {:cont, {:ok, any?, [value | acc]}}

        true ->
          {:halt, {:error, "ERR flow state must be a non-empty string"}}
      end
    end)
    |> case do
      {:ok, true, []} ->
        {:ok, :any}

      {:ok, true, _states} ->
        {:error, "ERR flow STATE ANY cannot be mixed with explicit states"}

      {:ok, false, states} ->
        case dedupe_claim_states_keep_last(states) do
          [single] -> {:ok, single}
          deduped -> {:ok, deduped}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp dedupe_claim_states_keep_last(states) do
    {deduped, _seen} =
      Enum.reduce(states, {[], MapSet.new()}, fn state, {acc, seen} ->
        if MapSet.member?(seen, state) do
          {acc, seen}
        else
          {[state | acc], MapSet.put(seen, state)}
        end
      end)

    deduped
  end
end
