defmodule Ferricstore.Raft.ApplyLimits do
  @moduledoc false

  alias Ferricstore.Raft.ApplyContext

  @hard_max_value_size 512 * 1_024 * 1_024
  @default_max_value_size 1_048_576
  @max_exact_flow_ms 9_007_199_254_740_991
  @flow_timestamp_fields [
    :now_ms,
    :step_now_ms,
    :run_at_ms,
    :due_at_ms,
    :next_run_at_ms,
    :created_at_ms,
    :updated_at_ms,
    :lease_deadline_ms,
    :terminal_retention_until_ms,
    :expire_at_ms,
    :expires_at_ms
  ]
  @flow_duration_fields [:lease_ms, :ttl_ms, :retention_ttl_ms, :max_active_ms]

  @spec validate_value_size(map(), non_neg_integer()) :: :ok | {:error, binary()}
  def validate_value_size(%{apply_context: %ApplyContext{max_value_size: max}}, size)
      when is_integer(size) and size >= 0 do
    if size <= max do
      :ok
    else
      {:error, "ERR value too large (#{size} bytes, max #{max} bytes)"}
    end
  end

  def validate_value_size(_state, size) when is_integer(size) and size >= 0 do
    validate_value_size(%{apply_context: ApplyContext.default()}, size)
  end

  @spec validate_instance_value_size(map(), non_neg_integer()) :: :ok | {:error, binary()}
  def validate_instance_value_size(ctx, size)
      when is_map(ctx) and is_integer(size) and size >= 0 do
    max =
      case Map.get(ctx, :max_value_size, @default_max_value_size) do
        configured when is_integer(configured) and configured > 0 ->
          min(configured, @hard_max_value_size)

        _invalid ->
          @default_max_value_size
      end

    if size <= max do
      :ok
    else
      {:error, "ERR value too large (#{size} bytes, max #{max} bytes)"}
    end
  end

  @spec validate_value(map(), term()) :: :ok | {:error, binary()}
  def validate_value(state, value) when is_binary(value),
    do: validate_value_size(state, byte_size(value))

  def validate_value(_state, _value), do: :ok

  @spec validate_flow_batch(map(), map()) :: :ok | {:error, binary()}
  def validate_flow_batch(
        %{apply_context: %ApplyContext{flow_max_batch_items: max_items}},
        attrs
      )
      when is_map(attrs) do
    with {:ok, remaining} <- consume_flow_batch_items(Map.get(attrs, :records), max_items),
         {:ok, _remaining} <- consume_flow_batch_items(Map.get(attrs, :children), remaining) do
      :ok
    else
      :limit_exceeded ->
        {:error, "ERR flow batch item count exceeds maximum #{max_items}"}

      :invalid_list ->
        {:error, "ERR flow batch items must be proper lists"}
    end
  end

  def validate_flow_batch(_state, attrs) when is_map(attrs) do
    validate_flow_batch(%{apply_context: ApplyContext.default()}, attrs)
  end

  def validate_flow_batch(_state, _attrs),
    do: {:error, "ERR flow command attributes must be a map"}

  @spec validate_flow_time(map(), non_neg_integer()) :: :ok | {:error, binary()}
  def validate_flow_time(attrs, apply_now_ms)
      when is_map(attrs) and is_integer(apply_now_ms) do
    with :ok <- validate_flow_timestamp(:now_ms, apply_now_ms),
         :ok <- validate_flow_attrs(attrs, %{}, apply_now_ms),
         {:ok, shared} <- flow_structural_map(attrs, :shared),
         :ok <- validate_flow_structural_list(attrs, :records, shared, apply_now_ms),
         :ok <- validate_flow_structural_list(attrs, :children, attrs, apply_now_ms) do
      :ok
    end
  end

  def validate_flow_time(_attrs, _apply_now_ms),
    do: {:error, "ERR flow now_ms must be a non-negative integer"}

  defp validate_flow_attrs(attrs, inherited, apply_now_ms) do
    with :ok <- validate_flow_timestamp_fields(attrs),
         :ok <- validate_flow_duration_fields(attrs),
         {:ok, now_ms} <- flow_effective_timestamp(attrs, inherited, :now_ms, apply_now_ms),
         {:ok, step_now_ms} <-
           flow_effective_timestamp(attrs, inherited, :step_now_ms, now_ms),
         :ok <- validate_flow_step_deadline(attrs, step_now_ms),
         {:ok, deadline_base_ms} <- flow_deadline_base(attrs, step_now_ms, now_ms, apply_now_ms),
         :ok <- validate_flow_duration_deadlines(attrs, inherited, deadline_base_ms) do
      :ok
    end
  end

  defp validate_flow_timestamp_fields(attrs) do
    Enum.reduce_while(@flow_timestamp_fields, :ok, fn key, :ok ->
      case Map.fetch(attrs, key) do
        {:ok, nil} -> {:cont, :ok}
        {:ok, value} -> reduce_validation(validate_flow_timestamp(key, value))
        :error -> {:cont, :ok}
      end
    end)
  end

  defp validate_flow_duration_fields(attrs) do
    Enum.reduce_while(@flow_duration_fields, :ok, fn key, :ok ->
      case Map.fetch(attrs, key) do
        {:ok, nil} ->
          {:cont, :ok}

        {:ok, :infinity} when key == :max_active_ms ->
          {:cont, :ok}

        {:ok, value} ->
          reduce_validation(validate_flow_duration(key, value))

        :error ->
          {:cont, :ok}
      end
    end)
  end

  defp validate_flow_duration_deadlines(attrs, inherited, deadline_base_ms) do
    Enum.reduce_while(@flow_duration_fields, :ok, fn key, :ok ->
      case flow_effective_value(attrs, inherited, key) do
        {:ok, value} when is_integer(value) and value > 0 ->
          reduce_validation(validate_flow_deadline(key, deadline_base_ms, value))

        _missing_or_unbounded ->
          {:cont, :ok}
      end
    end)
  end

  defp validate_flow_step_deadline(attrs, step_now_ms) do
    case Map.fetch(attrs, :step_count) do
      {:ok, count} when is_integer(count) and count > 0 and count <= @max_exact_flow_ms ->
        validate_flow_deadline(:step_count, step_now_ms, count)

      {:ok, count} when is_integer(count) and count > @max_exact_flow_ms ->
        {:error, "ERR flow step_count exceeds maximum #{@max_exact_flow_ms}"}

      {:ok, _invalid} ->
        {:error, "ERR flow step_count must be a positive integer"}

      :error ->
        :ok
    end
  end

  defp flow_deadline_base(attrs, step_now_ms, now_ms, apply_now_ms) do
    case Map.fetch(attrs, :step_count) do
      {:ok, count} when is_integer(count) and count > 0 ->
        {:ok, max(max(now_ms, apply_now_ms), step_now_ms + count)}

      _missing ->
        {:ok, max(now_ms, apply_now_ms)}
    end
  end

  defp validate_flow_structural_list(attrs, key, inherited, apply_now_ms) do
    case Map.get(attrs, key) do
      values when is_list(values) ->
        Enum.reduce_while(values, :ok, fn
          value, :ok when is_map(value) ->
            case validate_flow_attrs(value, inherited, apply_now_ms) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end

          _value, :ok ->
            {:cont, :ok}
        end)

      _missing_or_invalid ->
        :ok
    end
  end

  defp flow_structural_map(attrs, key) do
    case Map.get(attrs, key) do
      value when is_map(value) ->
        case validate_flow_attrs(value, %{}, 0) do
          :ok -> {:ok, value}
          {:error, _reason} = error -> error
        end

      _missing ->
        {:ok, %{}}
    end
  end

  defp flow_effective_timestamp(attrs, inherited, key, default) do
    case flow_effective_value(attrs, inherited, key) do
      {:ok, nil} -> {:ok, default}
      {:ok, value} -> validate_and_return_flow_timestamp(key, value)
      :error -> {:ok, default}
    end
  end

  defp flow_effective_value(attrs, inherited, key) do
    case Map.fetch(attrs, key) do
      :error -> Map.fetch(inherited, key)
      found -> found
    end
  end

  defp validate_and_return_flow_timestamp(key, value) do
    case validate_flow_timestamp(key, value) do
      :ok -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  defp validate_flow_timestamp(_key, value)
       when is_integer(value) and value >= 0 and value <= @max_exact_flow_ms,
       do: :ok

  defp validate_flow_timestamp(key, value)
       when is_integer(value) and value > @max_exact_flow_ms,
       do: {:error, "ERR flow #{key} exceeds maximum #{@max_exact_flow_ms}"}

  defp validate_flow_timestamp(key, _value),
    do: {:error, "ERR flow #{key} must be a non-negative integer"}

  defp validate_flow_duration(_key, value)
       when is_integer(value) and value > 0 and value <= @max_exact_flow_ms,
       do: :ok

  defp validate_flow_duration(key, value)
       when is_integer(value) and value > @max_exact_flow_ms,
       do: {:error, "ERR flow #{key} exceeds maximum #{@max_exact_flow_ms}"}

  defp validate_flow_duration(key, _value),
    do: {:error, "ERR flow #{key} must be a positive integer"}

  defp validate_flow_deadline(_key, now_ms, duration_ms)
       when now_ms <= @max_exact_flow_ms - duration_ms,
       do: :ok

  defp validate_flow_deadline(key, _now_ms, _duration_ms),
    do: {:error, "ERR flow #{key} deadline exceeds maximum #{@max_exact_flow_ms}"}

  defp reduce_validation(:ok), do: {:cont, :ok}
  defp reduce_validation({:error, _reason} = error), do: {:halt, error}

  defp consume_flow_batch_items(values, remaining) when is_list(values),
    do: consume_flow_batch_list(values, remaining)

  defp consume_flow_batch_items(_missing_or_invalid, remaining), do: {:ok, remaining}

  defp consume_flow_batch_list([], remaining), do: {:ok, remaining}
  defp consume_flow_batch_list([_item | _rest], 0), do: :limit_exceeded

  defp consume_flow_batch_list([_item | rest], remaining),
    do: consume_flow_batch_list(rest, remaining - 1)

  defp consume_flow_batch_list(_improper_tail, _remaining), do: :invalid_list

  @spec append_size(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def append_size(current_size, suffix_size), do: current_size + suffix_size

  @spec setrange_size(non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          non_neg_integer()
  def setrange_size(current_size, offset, 0), do: max(current_size, offset)
  def setrange_size(current_size, offset, value_size), do: max(current_size, offset + value_size)

  @spec setbit_size(non_neg_integer(), non_neg_integer()) :: pos_integer()
  def setbit_size(current_size, bit_offset), do: max(current_size, div(bit_offset, 8) + 1)
end
