defmodule Ferricstore.Flow.Governance.CircuitStore do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.Circuit
  alias Ferricstore.Flow.Governance.Telemetry
  alias Ferricstore.Flow.Governance.View
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @max_error_classes 1_000
  @max_error_class_bytes 256
  @max_error_class_input_bytes 262_144
  @max_exact_integer 9_007_199_254_740_991

  def open(ctx, scope, opts \\ [])

  def open(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- circuit_key(scope),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, rule_opts} <- optional_rule_opts(opts) do
        AtomicRecord.mutate(
          ctx,
          key,
          &decode/1,
          &encode/1,
          fn -> {:ok, Circuit.new(scope, rule_opts)} end,
          fn circuit ->
            with :ok <- validate_mutation_time(circuit, now_ms) do
              opened =
                circuit
                |> Circuit.configure(rule_opts)
                |> Circuit.record_manual_open(now_ms)

              {:ok, opened, public(opened)}
            end
          end,
          catalog_kind: :circuit
        )
      else
        false -> {:error, "ERR flow circuit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:circuit_open, result, circuit_telemetry_metadata(scope, result))
  end

  def open(_ctx, _scope, _opts), do: {:error, "ERR flow circuit opts must be a keyword list"}

  def close(ctx, scope, opts \\ [])

  def close(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- circuit_key(scope),
           {:ok, now_ms} <- optional_now_ms(opts) do
        AtomicRecord.mutate(
          ctx,
          key,
          &decode/1,
          &encode/1,
          fn -> {:ok, Circuit.new(scope, failure_threshold: 1, open_ms: 30_000)} end,
          fn circuit ->
            with :ok <- validate_mutation_time(circuit, now_ms) do
              closed = Circuit.record_manual_close(circuit, now_ms)
              {:ok, closed, public(closed)}
            end
          end,
          catalog_kind: :circuit
        )
      else
        false -> {:error, "ERR flow circuit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:circuit_close, result, circuit_telemetry_metadata(scope, result))
  end

  def close(_ctx, _scope, _opts), do: {:error, "ERR flow circuit opts must be a keyword list"}

  def record_failure(ctx, scope, opts \\ [])

  def record_failure(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- circuit_key(scope),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, rule_opts} <- optional_rule_opts(opts) do
        AtomicRecord.mutate(
          ctx,
          key,
          &decode/1,
          &encode/1,
          fn ->
            {:ok, Circuit.new(scope, rule_opts)}
          end,
          fn circuit ->
            with :ok <- validate_mutation_time(circuit, now_ms) do
              updated =
                circuit
                |> configure(rule_opts)
                |> Circuit.record_failure(now_ms,
                  latency_ms: Keyword.get(rule_opts, :latency_ms),
                  error_class: Keyword.get(rule_opts, :error_class)
                )

              {:ok, updated, public(updated)}
            end
          end,
          catalog_kind: :circuit
        )
      else
        false -> {:error, "ERR flow circuit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:circuit_failure, result, circuit_telemetry_metadata(scope, result))
  end

  def record_failure(_ctx, _scope, _opts),
    do: {:error, "ERR flow circuit opts must be a keyword list"}

  def record_success(ctx, scope, opts \\ [])

  def record_success(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    result =
      with true <- Keyword.keyword?(opts),
           {:ok, key} <- circuit_key(scope),
           {:ok, now_ms} <- optional_now_ms(opts),
           {:ok, rule_opts} <- optional_rule_opts(opts) do
        AtomicRecord.mutate(
          ctx,
          key,
          &decode/1,
          &encode/1,
          fn ->
            if success_tracking_required?(rule_opts) do
              {:ok, Circuit.new(scope, rule_opts)}
            else
              {:return, {:ok, nil}}
            end
          end,
          fn circuit ->
            with :ok <- validate_mutation_time(circuit, now_ms) do
              configured = configure(circuit, rule_opts)

              configured =
                if configured == circuit,
                  do: configured,
                  else: %{configured | updated_at_ms: now_ms}

              updated =
                Circuit.record_success(configured, now_ms,
                  latency_ms: Keyword.get(rule_opts, :latency_ms)
                )

              if updated == circuit do
                {:return, {:ok, public(circuit)}}
              else
                {:ok, updated, public(updated)}
              end
            end
          end,
          catalog_kind: :circuit
        )
      else
        false -> {:error, "ERR flow circuit opts must be a keyword list"}
        {:error, _reason} = error -> error
      end

    Telemetry.emit(:circuit_success, result, circuit_telemetry_metadata(scope, result))
  end

  def record_success(_ctx, _scope, _opts),
    do: {:error, "ERR flow circuit opts must be a keyword list"}

  def get(ctx, scope, opts \\ [])

  def get(ctx, scope, opts) when is_binary(scope) and scope != "" and is_list(opts) do
    if Keyword.keyword?(opts) do
      case get_record(ctx, scope) do
        {:ok, %Circuit{} = circuit} -> {:ok, public(circuit)}
        other -> other
      end
    else
      {:error, "ERR flow circuit opts must be a keyword list"}
    end
  end

  def get(_ctx, _scope, _opts), do: {:error, "ERR flow circuit opts must be a keyword list"}

  def list(ctx, opts \\ [])

  def list(ctx, opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, limit} <- optional_list_limit(opts),
         {:ok, scopes} <- optional_scope_filters(opts),
         {:ok, status} <- optional_status(opts),
         {:ok, circuits} <-
           collect_list_circuits(ctx, scopes, status, limit) do
      {:ok, circuits}
    else
      false -> {:error, "ERR flow circuit opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow circuit opts must be a keyword list"}

  defp collect_list_circuits(ctx, nil, status, limit) do
    Catalog.collect(
      ctx,
      :circuit,
      limit,
      &load_list_circuit(ctx, &1, nil, status),
      &circuit_list_sort_key/1
    )
  end

  defp collect_list_circuits(ctx, scopes, status, limit) when is_list(scopes) do
    keys = Enum.map(scopes, &Keys.governance_circuit_key/1)

    Catalog.collect_keys(
      keys,
      limit,
      &load_list_circuit(ctx, &1, scopes, status),
      &circuit_list_sort_key/1
    )
  end

  defp circuit_list_sort_key(circuit) do
    {circuit_status_rank(Map.get(circuit, :status)), Map.get(circuit, :scope, "")}
  end

  defp load_list_circuit(ctx, key, scopes, status) do
    case Router.get(ctx, key) do
      nil ->
        :skip

      value when is_binary(value) ->
        with {:ok, circuit} <- decode(value) do
          circuit = public(circuit)

          if matches_scope?(circuit, scopes) and matches_status?(circuit, status),
            do: {:ok, circuit},
            else: :skip
        end

      {:error, _reason} = error ->
        error

      _other ->
        {:error, "ERR flow circuit record is corrupt"}
    end
  end

  def allow(ctx, scope, now_ms) when is_binary(scope) and scope != "" do
    case acquire(ctx, scope, now_ms) do
      {:ok, _permit} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @doc false
  def acquire(ctx, scope, now_ms) when is_binary(scope) and scope != "" do
    case get_record(ctx, scope) do
      {:ok, nil} -> {:ok, nil}
      {:ok, circuit} -> acquire_record(ctx, scope, circuit, now_ms)
      {:error, _reason} = error -> error
    end
  end

  defp acquire_record(ctx, scope, %Circuit{status: status} = circuit, now_ms)
       when status in [:open, :half_open] do
    if Circuit.probe_available?(circuit, now_ms) do
      claim_probe(ctx, scope, now_ms)
    else
      circuit |> Circuit.allow?(now_ms) |> acquire_result()
    end
  end

  defp acquire_record(_ctx, _scope, %Circuit{} = circuit, now_ms) do
    circuit |> Circuit.allow?(now_ms) |> acquire_result()
  end

  defp claim_probe(ctx, scope, now_ms) do
    {:ok, key} = circuit_key(scope)

    result =
      AtomicRecord.mutate(
        ctx,
        key,
        &decode/1,
        &encode/1,
        fn -> {:return, {:ok, nil}} end,
        fn circuit ->
          with :ok <- validate_mutation_time(circuit, now_ms) do
            cond do
              Circuit.probe_available?(circuit, now_ms) ->
                {:ok, Circuit.claim_probe(circuit, now_ms), :probe_claimed}

              true ->
                case Circuit.allow?(circuit, now_ms) do
                  :allow -> {:return, {:ok, nil}}
                  {:deny, denial} -> {:return, {:error, denial}}
                end
            end
          end
        end
      )

    case result do
      {:ok, :probe_claimed} -> {:ok, {:half_open_probe, scope}}
      other -> other
    end
  end

  @doc false
  def release_permit(ctx, {:half_open_probe, scope}, now_ms)
      when is_binary(scope) and scope != "" and is_integer(now_ms) and now_ms >= 0 and
             now_ms <= @max_exact_integer do
    with {:ok, key} <- circuit_key(scope) do
      result =
        AtomicRecord.mutate(
          ctx,
          key,
          &decode/1,
          &encode/1,
          fn -> {:return, :ok} end,
          fn circuit ->
            with :ok <- validate_mutation_time(circuit, now_ms) do
              updated = Circuit.release_probe(circuit, now_ms)

              if updated == circuit do
                {:return, :ok}
              else
                {:ok, updated, :ok}
              end
            end
          end
        )

      case result do
        {:ok, :ok} -> :ok
        other -> other
      end
    end
  end

  def release_permit(_ctx, nil, _now_ms), do: :ok

  def release_permit(_ctx, _permit, _now_ms),
    do: {:error, "ERR invalid flow circuit permit"}

  defp get_record(ctx, scope) do
    with {:ok, key} <- circuit_key(scope) do
      case Router.get(ctx, key) do
        nil -> {:ok, nil}
        value when is_binary(value) -> decode(value)
        _other -> {:error, "ERR flow circuit record is corrupt"}
      end
    end
  end

  defp acquire_result(:allow), do: {:ok, nil}
  defp acquire_result({:deny, denial}), do: {:error, denial}

  defp configure(%Circuit{} = circuit, opts), do: Circuit.configure(circuit, opts)

  defp validate_mutation_time(%Circuit{updated_at_ms: nil}, _now_ms), do: :ok

  defp validate_mutation_time(%Circuit{updated_at_ms: updated_at_ms}, now_ms)
       when is_integer(updated_at_ms) and now_ms >= updated_at_ms,
       do: :ok

  defp validate_mutation_time(%Circuit{}, _now_ms),
    do: {:error, "ERR flow circuit now_ms cannot precede updated_at_ms"}

  defp success_tracking_required?(opts) do
    Keyword.has_key?(opts, :failure_rate_pct) or slow_success?(opts)
  end

  defp slow_success?(opts) do
    case {Keyword.get(opts, :latency_threshold_ms), Keyword.get(opts, :latency_ms)} do
      {threshold, latency_ms} when is_integer(threshold) and is_integer(latency_ms) ->
        latency_ms >= threshold

      _other ->
        false
    end
  end

  defp circuit_telemetry_metadata(scope, result) do
    metadata = %{scope: scope}

    case result do
      {:ok, circuit} when is_map(circuit) ->
        metadata
        |> Map.put(:circuit_status, Map.get(circuit, :status))
        |> Map.put(:failure_count, Map.get(circuit, :failure_count, Map.get(circuit, :failures)))
        |> Map.put(:failure_threshold, Map.get(circuit, :failure_threshold))
        |> Map.put(:open_ms, Map.get(circuit, :open_ms))
        |> maybe_put_telemetry(:window_ms, Map.get(circuit, :window_ms))
        |> maybe_put_telemetry(:failure_rate_pct, Map.get(circuit, :failure_rate_pct))
        |> maybe_put_telemetry(:latency_threshold_ms, Map.get(circuit, :latency_threshold_ms))
        |> maybe_put_telemetry(:retry_after_ms, Map.get(circuit, :retry_after_ms))

      _other ->
        metadata
    end
  end

  defp maybe_put_telemetry(metadata, _key, nil), do: metadata
  defp maybe_put_telemetry(metadata, key, value), do: Map.put(metadata, key, value)

  defp encode(circuit), do: TermCodec.encode({:flow_governance_circuit_v1, circuit})

  defp decode(value) do
    case TermCodec.decode(value) do
      {:ok, {:flow_governance_circuit_v1, %Circuit{} = circuit}} ->
        if Circuit.valid?(circuit),
          do: {:ok, circuit},
          else: {:error, "ERR flow circuit record is corrupt"}

      _other ->
        {:error, "ERR flow circuit record is corrupt"}
    end
  end

  defp optional_now_ms(opts) do
    case Keyword.get(opts, :now_ms, CommandTime.now_ms()) do
      value when is_integer(value) and value >= 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow circuit now_ms must be a non-negative integer"}
    end
  end

  defp optional_rule_opts(opts) do
    with {:ok, open_ms} <- optional_positive_integer_or_nil(opts, :open_ms),
         {:ok, failure_threshold} <- optional_positive_integer_or_nil(opts, :failure_threshold),
         {:ok, window_ms} <- optional_positive_integer_or_nil(opts, :window_ms),
         {:ok, min_calls} <- optional_positive_integer_or_nil(opts, :min_calls),
         {:ok, failure_rate_pct} <- optional_percent_or_nil(opts, :failure_rate_pct),
         {:ok, latency_threshold_ms} <-
           optional_positive_integer_or_nil(opts, :latency_threshold_ms),
         {:ok, error_classes} <- optional_string_list(opts, :error_classes),
         {:ok, half_open_max_probes} <-
           optional_positive_integer_or_nil(opts, :half_open_max_probes),
         {:ok, half_open_success_threshold} <-
           optional_positive_integer_or_nil(opts, :half_open_success_threshold),
         {:ok, latency_ms} <- optional_non_negative_integer_or_nil(opts, :latency_ms),
         {:ok, error_class} <- optional_binary_or_nil(opts, :error_class),
         {:ok, :ok} <-
           validate_tracking_bounds(failure_threshold || 5, min_calls, failure_rate_pct) do
      {:ok,
       [
         open_ms: open_ms,
         failure_threshold: failure_threshold,
         window_ms: window_ms,
         min_calls: min_calls,
         failure_rate_pct: failure_rate_pct,
         latency_threshold_ms: latency_threshold_ms,
         error_classes: error_classes,
         half_open_max_probes: half_open_max_probes,
         half_open_success_threshold: half_open_success_threshold,
         latency_ms: latency_ms,
         error_class: error_class
       ]
       |> Enum.reject(fn {_key, value} -> is_nil(value) end)}
    end
  end

  defp optional_positive_integer_or_nil(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow circuit #{key} must be a positive integer"}
    end
  end

  defp optional_non_negative_integer_or_nil(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow circuit #{key} must be a non-negative integer"}
    end
  end

  defp optional_percent_or_nil(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 1 and value <= 100 -> {:ok, value}
      _other -> {:error, "ERR flow circuit #{key} must be an integer from 1 to 100"}
    end
  end

  defp optional_binary_or_nil(opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        {:ok, nil}

      value
      when is_binary(value) and value != "" and byte_size(value) <= @max_error_class_input_bytes ->
        {:ok, value}

      _other ->
        {:error,
         "ERR flow circuit #{key} must be a non-empty string of at most #{@max_error_class_input_bytes} bytes"}
    end
  end

  defp optional_string_list(opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        {:ok, nil}

      values when is_list(values) ->
        normalize_string_list(values, key, MapSet.new(), [], 0)

      _other ->
        {:error, "ERR flow circuit #{key} must be a list of non-empty strings"}
    end
  end

  defp normalize_string_list([], _key, _seen, values, _count),
    do: {:ok, Enum.reverse(values)}

  defp normalize_string_list([_value | _rest], key, _seen, _values, @max_error_classes),
    do: {:error, "ERR flow circuit #{key} must contain at most 1000 values"}

  defp normalize_string_list([value | rest], key, seen, values, count)
       when is_binary(value) and value != "" and byte_size(value) <= @max_error_class_bytes do
    if MapSet.member?(seen, value) do
      normalize_string_list(rest, key, seen, values, count + 1)
    else
      normalize_string_list(rest, key, MapSet.put(seen, value), [value | values], count + 1)
    end
  end

  defp normalize_string_list([value | _rest], key, _seen, _values, _count)
       when is_binary(value) and value != "",
       do:
         {:error,
          "ERR flow circuit #{key} must contain non-empty strings of at most #{@max_error_class_bytes} bytes"}

  defp normalize_string_list(_values, key, _seen, _normalized, _count),
    do: {:error, "ERR flow circuit #{key} must be a list of non-empty strings"}

  defp validate_tracking_bounds(failure_threshold, min_calls, failure_rate_pct) do
    cond do
      is_integer(min_calls) and min_calls > 64 ->
        {:error, "ERR flow circuit min_calls must be <= 64"}

      is_nil(failure_rate_pct) and failure_threshold > 64 ->
        {:error, "ERR flow circuit failure_threshold must be <= 64 without failure_rate_pct"}

      not is_nil(failure_rate_pct) and is_nil(min_calls) and failure_threshold > 64 ->
        {:error, "ERR flow circuit min_calls is required when failure_threshold exceeds 64"}

      true ->
        {:ok, :ok}
    end
  end

  defp optional_list_limit(opts) do
    case Keyword.get(opts, :limit, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 1_000)}
      _other -> {:error, "ERR flow circuit limit must be a positive integer"}
    end
  end

  defp optional_scope_filters(opts) do
    case {Keyword.get(opts, :scope), Keyword.get(opts, :partition_key)} do
      {scope, _partition_key} when is_binary(scope) and scope != "" ->
        with {:ok, _key} <- circuit_key(scope), do: {:ok, [scope]}

      {nil, partition_key} when is_binary(partition_key) and partition_key != "" ->
        scopes = [partition_key, "partition:" <> partition_key]

        with true <- Enum.all?(scopes, &match?({:ok, _key}, circuit_key(&1))) do
          {:ok, scopes}
        else
          false -> {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
        end

      {nil, nil} ->
        {:ok, nil}

      _other ->
        {:error, "ERR flow circuit scope must be a non-empty string"}
    end
  end

  defp optional_status(opts) do
    case Keyword.get(opts, :circuit_status) do
      nil -> {:ok, nil}
      value when value in [:closed, :open, :half_open] -> {:ok, value}
      "closed" -> {:ok, :closed}
      "open" -> {:ok, :open}
      "half_open" -> {:ok, :half_open}
      _other -> {:error, "ERR flow circuit status must be closed, open, or half_open"}
    end
  end

  defp matches_scope?(_circuit, nil), do: true

  defp matches_scope?(circuit, scopes) do
    Map.get(circuit, :scope) in scopes
  end

  defp matches_status?(_circuit, nil), do: true
  defp matches_status?(circuit, status), do: Map.get(circuit, :status) == status

  defp circuit_status_rank(:open), do: 0
  defp circuit_status_rank(:half_open), do: 1
  defp circuit_status_rank(:closed), do: 2
  defp circuit_status_rank(_status), do: 3

  defp public(%Circuit{} = circuit) do
    circuit
    |> View.public()
    |> Map.put(:failure_count, circuit.failures)
    |> Map.put(:event_count, length(circuit.events || []))
  end

  defp circuit_key(scope) do
    key = Keys.governance_circuit_key(scope)

    if byte_size(key) <= Router.max_key_size() do
      {:ok, key}
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end
end
