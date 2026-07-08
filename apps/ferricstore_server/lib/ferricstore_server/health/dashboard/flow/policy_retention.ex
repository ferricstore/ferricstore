defmodule FerricstoreServer.Health.Dashboard.Flow.PolicyRetention do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Access, as: DashboardAccess
  alias FerricstoreServer.Health.Dashboard.Data.Operational

  import FerricstoreServer.Health.Dashboard.Flow.Sample
  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowQueryPolicy, only: [flow_policy_field: 3]

  @flow_dashboard_sample_limit 400
  @flow_dashboard_policy_scan_limit 20_000
  @flow_dashboard_policy_key_select_batch 256
  @flow_dashboard_retention_default_limit 100
  @flow_dashboard_retention_max_limit 10_000
  @flow_dashboard_retention_candidate_preview_limit 100
  @flow_terminal_states ~w(completed failed cancelled)

  def collect_policies_page(opts \\ []) when is_list(opts) do
    records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    active_types = flow_available_types(records)
    policy_scan = collect_flow_policy_type_scan(@flow_dashboard_policy_scan_limit)
    configured_types = Map.get(policy_scan, :types, MapSet.new())
    edit_type = opts |> Keyword.get(:edit_type, "") |> flow_policy_clean_form_value()

    types =
      active_types
      |> Enum.concat(MapSet.to_list(configured_types))
      |> maybe_include_policy_edit_type(edit_type)
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.uniq()
      |> Enum.sort()

    %{
      policies: Enum.map(types, &flow_policy_row(&1, configured_types)),
      editor: flow_policy_editor_data(edit_type),
      flash: Keyword.get(opts, :flash),
      active_types: active_types,
      configured_types: MapSet.size(configured_types),
      total_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit,
      policy_scan: Map.delete(policy_scan, :types),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  def apply_policy_form(params) when is_map(params) do
    with {:ok, type} <- flow_policy_required_form_value(params, "type", "flow type"),
         {:ok, state} <- flow_policy_optional_form_value(params, "state"),
         {:ok, mode} <- flow_policy_form_mode(params),
         {:ok, retry} <- flow_policy_form_retry_opts(params),
         {:ok, retention} <- flow_policy_form_retention_opts(params),
         {:ok, indexes} <- flow_policy_form_index_opts(params),
         {:ok, existing_opts} <- flow_policy_existing_set_opts(type),
         opts = flow_policy_merge_form_opts(existing_opts, state, mode, retry, retention, indexes),
         {:ok, _policy} <- FerricStore.flow_policy_set(type, opts) do
      {:ok, type}
    else
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  def apply_policy_form(_params), do: {:error, "ERR policy form must be a map"}

  def policy_flash_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    case Map.get(params, "status") do
      "ok" ->
        type = params |> Map.get("type", "") |> flow_policy_clean_form_value()
        %{kind: :ok, message: "Policy saved", type: type}

      "error" ->
        message =
          params |> Map.get("message", "Policy update failed") |> flow_policy_clean_form_value()

        %{kind: :error, message: message}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  def collect_retention_page(opts \\ []) when is_list(opts) do
    now_ms = System.system_time(:millisecond)

    limit =
      flow_retention_limit!(Keyword.get(opts, :limit, @flow_dashboard_retention_default_limit))

    sampled_records = collect_flow_records_sample(@flow_dashboard_sample_limit)

    records =
      DashboardAccess.filter_flow_records_for_acl(
        sampled_records,
        DashboardAccess.keyspace_acl_username(opts)
      )

    candidates = flow_retention_candidates(records, now_ms)
    terminal_sampled = Enum.count(records, &flow_retention_terminal_record?/1)

    %{
      now_ms: now_ms,
      limit: limit,
      sample_limit: @flow_dashboard_sample_limit,
      total_sampled: length(sampled_records),
      filtered_sampled: length(records),
      terminal_sampled: terminal_sampled,
      active_sampled: max(length(records) - terminal_sampled, 0),
      eligible_sampled: length(candidates),
      candidates:
        candidates |> Enum.take(min(limit, @flow_dashboard_retention_candidate_preview_limit)),
      storage: Operational.collect_storage_summary(),
      projection: FerricstoreServer.Health.Dashboard.collect_flow_projection_health(),
      flash: Keyword.get(opts, :flash),
      generated_at_ms: now_ms
    }
  end

  def apply_retention_form(params) when is_map(params) do
    with {:ok, limit} <- flow_retention_form_limit(Map.get(params, "limit")) do
      case Map.get(params, "action", "dry_run") do
        "dry_run" ->
          {:ok, :dry_run, %{limit: limit}}

        "cleanup" ->
          with :ok <- flow_retention_cleanup_confirmed(params) do
            case flow_dashboard_retention_cleanup(limit: limit) do
              {:ok, result} when is_map(result) ->
                {:ok, :cleanup, flow_retention_cleanup_counts(result, limit)}

              {:error, reason} when is_binary(reason) ->
                {:error, reason}

              {:error, reason} ->
                {:error, inspect(reason)}

              other ->
                {:error, "ERR unexpected retention cleanup result: #{inspect(other, limit: 8)}"}
            end
          end

        _other ->
          {:error, "ERR retention action must be dry_run or cleanup"}
      end
    end
  end

  def apply_retention_form(_params), do: {:error, "ERR retention form must be a map"}

  def retention_flash_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    case Map.get(params, "status") do
      "dry_run" ->
        limit = flow_retention_limit!(Map.get(params, "limit"))
        %{kind: :dry_run, message: "Dry run ready", limit: limit}

      "ok" ->
        %{
          kind: :ok,
          message: "Cleanup completed",
          limit: flow_retention_limit!(Map.get(params, "limit")),
          counts: %{
            flows: flow_retention_query_integer(params, "flows"),
            history: flow_retention_query_integer(params, "history"),
            values: flow_retention_query_integer(params, "values")
          }
        }

      "error" ->
        message =
          params
          |> Map.get("message", "Retention cleanup failed")
          |> flow_policy_clean_form_value()

        %{kind: :error, message: message}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  def clean_form_value(value), do: flow_policy_clean_form_value(value)
  def query_integer(params, key), do: flow_retention_query_integer(params, key)

  defp flow_retention_cleanup_confirmed(params) do
    case Map.get(params, "confirm_cleanup") do
      value when value in ["true", "on", "yes", "1"] ->
        :ok

      _ ->
        {:error, "ERR cleanup requires confirm_cleanup=true after reviewing the sample preview"}
    end
  end

  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  @spec collect_flow_policy_type_scan(pos_integer()) :: map()
  defp collect_flow_policy_type_scan(limit) when is_integer(limit) and limit > 0 do
    sc = max(shard_count(), 1)

    0..(sc - 1)
    |> Enum.reduce_while(%{types: MapSet.new(), scanned_entries: 0, truncated: false}, fn
      _index, %{scanned_entries: scanned} = acc when scanned >= limit ->
        {:halt, %{acc | truncated: true}}

      index, acc ->
        remaining = max(limit - acc.scanned_entries, 0)
        scan = collect_flow_policy_types_from_keydir(index, remaining)

        next = %{
          types: MapSet.union(acc.types, scan.types),
          scanned_entries: acc.scanned_entries + scan.scanned_entries,
          truncated: acc.truncated or scan.truncated
        }

        if next.truncated, do: {:halt, next}, else: {:cont, next}
    end)
  end

  defp collect_flow_policy_type_scan(_limit),
    do: %{types: MapSet.new(), scanned_entries: 0, truncated: false}

  @spec collect_flow_policy_types_from_keydir(non_neg_integer(), non_neg_integer()) :: map()
  defp collect_flow_policy_types_from_keydir(_index, limit) when limit <= 0,
    do: %{types: MapSet.new(), scanned_entries: 0, truncated: true}

  defp collect_flow_policy_types_from_keydir(index, limit) do
    keydir = :"keydir_#{index}"
    batch = min(@flow_dashboard_policy_key_select_batch, limit)

    try do
      keydir
      |> :ets.select(flow_keydir_key_select_spec(), batch)
      |> collect_flow_policy_types_select(MapSet.new(), 0, limit)
    rescue
      ArgumentError -> %{types: MapSet.new(), scanned_entries: 0, truncated: false}
    catch
      :exit, _ -> %{types: MapSet.new(), scanned_entries: 0, truncated: false}
    end
  end

  defp collect_flow_policy_types_select(:"$end_of_table", types, scanned, _limit) do
    %{types: types, scanned_entries: scanned, truncated: false}
  end

  defp collect_flow_policy_types_select({keys, continuation}, types, scanned, limit) do
    scanned = scanned + length(keys)

    types =
      Enum.reduce(keys, types, fn key, acc ->
        case flow_policy_type_from_key(key) do
          nil -> acc
          type -> MapSet.put(acc, type)
        end
      end)

    if scanned >= limit do
      %{types: types, scanned_entries: scanned, truncated: true}
    else
      continuation
      |> :ets.select()
      |> collect_flow_policy_types_select(types, scanned, limit)
    end
  end

  defp collect_flow_policy_types_select(keys, types, scanned, _limit) when is_list(keys) do
    types =
      Enum.reduce(keys, types, fn key, acc ->
        case flow_policy_type_from_key(key) do
          nil -> acc
          type -> MapSet.put(acc, type)
        end
      end)

    %{types: types, scanned_entries: scanned + length(keys), truncated: false}
  end

  @spec flow_keydir_key_select_spec() :: list()
  defp flow_keydir_key_select_spec do
    [{{:"$1", :_, :_, :_, :_, :_, :_}, [], [:"$1"]}]
  end

  @spec flow_policy_type_from_key(term()) :: binary() | nil
  defp flow_policy_type_from_key(key) when is_binary(key) do
    prefix = Ferricstore.Flow.Keys.policy_key("")

    cond do
      not Ferricstore.Flow.Keys.policy_key?(key) ->
        nil

      key == prefix ->
        nil

      true ->
        String.replace_prefix(key, prefix, "")
    end
  end

  defp flow_policy_type_from_key(_key), do: nil

  @spec flow_policy_row(binary(), MapSet.t()) :: map()
  defp flow_policy_row(type, configured_types) do
    source = flow_policy_source(type, configured_types)

    case FerricStore.flow_policy_get(type) do
      {:ok, policy} when is_map(policy) ->
        %{
          type: type,
          source: source,
          retry: Map.get(policy, :retry, %{}),
          retention: Map.get(policy, :retention, %{}),
          indexed_attributes: Map.get(policy, :indexed_attributes, []),
          indexed_state_meta: Map.get(policy, :indexed_state_meta),
          states: flow_policy_state_rows(Map.get(policy, :states, %{})),
          error: nil
        }

      {:error, reason} ->
        %{
          type: type,
          source: source,
          retry: %{},
          retention: %{},
          states: [],
          error: reason
        }
    end
  rescue
    error ->
      flow_policy_error_row(type, configured_types, Exception.message(error))
  catch
    :exit, reason ->
      flow_policy_error_row(type, configured_types, inspect(reason))
  end

  @spec flow_policy_source(binary(), MapSet.t()) :: binary()
  defp flow_policy_source(type, configured_types),
    do: if(MapSet.member?(configured_types, type), do: "configured", else: "default")

  @spec flow_policy_error_row(binary(), MapSet.t(), binary()) :: map()
  defp flow_policy_error_row(type, configured_types, error) do
    %{
      type: type,
      source: flow_policy_source(type, configured_types),
      retry: %{},
      retention: %{},
      states: [],
      error: error
    }
  end

  @spec flow_policy_state_rows(map()) :: [map()]
  defp flow_policy_state_rows(states) when is_map(states) do
    states
    |> Enum.map(fn {state, policy} ->
      %{
        state: to_string(state),
        mode: flow_policy_field(policy, :mode, :parallel),
        retry: Map.get(policy, :retry, %{}),
        retention: Map.get(policy, :retention, %{})
      }
    end)
    |> Enum.sort_by(& &1.state)
  end

  defp flow_policy_state_rows(_states), do: []

  @spec maybe_include_policy_edit_type([binary()], binary()) :: [binary()]
  defp maybe_include_policy_edit_type(types, ""), do: types
  defp maybe_include_policy_edit_type(types, type), do: [type | types]

  @spec flow_policy_editor_data(binary() | nil) :: map()
  defp flow_policy_editor_data(type) do
    type = flow_policy_clean_form_value(type || "")

    policy =
      case type do
        "" ->
          flow_policy_default_response(type)

        _ ->
          case FerricStore.flow_policy_get(type) do
            {:ok, policy} when is_map(policy) -> policy
            _ -> flow_policy_default_response(type)
          end
      end

    retry = Map.get(policy, :retry, Ferricstore.Flow.RetryPolicy.default())
    backoff = flow_policy_field(retry, :backoff, Ferricstore.Flow.RetryPolicy.default().backoff)
    retention = Map.get(policy, :retention, Ferricstore.Flow.RetryPolicy.default_retention())

    indexed_attributes =
      policy
      |> Map.get(:indexed_attributes, [])
      |> flow_policy_indexed_attributes_string()

    %{
      type: type,
      state: "",
      mode: :parallel,
      indexed_attributes: indexed_attributes,
      indexed_state_meta: flow_policy_field(policy, :indexed_state_meta, "") || "",
      max_retries: flow_policy_field(retry, :max_retries, 3),
      backoff_kind: flow_policy_field(backoff, :kind, :exponential),
      base_ms: flow_policy_field(backoff, :base_ms, 1_000),
      max_ms: flow_policy_field(backoff, :max_ms, 30_000),
      jitter_pct: flow_policy_field(backoff, :jitter_pct, 20),
      exhausted_to: flow_policy_field(retry, :exhausted_to, "failed"),
      retention_ttl_ms: flow_policy_field(retention, :ttl_ms, 604_800_000),
      history_max_events: flow_policy_field(retention, :history_max_events, 100_000)
    }
  end

  @spec flow_policy_default_response(binary()) :: map()
  defp flow_policy_default_response(type) do
    %{
      type: type,
      retry: Ferricstore.Flow.RetryPolicy.default(),
      retention:
        Ferricstore.Flow.RetryPolicy.default_retention()
        |> Map.delete(:history_hot_max_events),
      states: %{}
    }
  end

  @spec flow_policy_existing_set_opts(binary()) :: {:ok, keyword()} | {:error, binary()}
  defp flow_policy_existing_set_opts(type) do
    case FerricStore.flow_policy_get(type) do
      {:ok, policy} when is_map(policy) ->
        {:ok, flow_policy_response_to_set_opts(policy)}

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  @spec flow_policy_response_to_set_opts(map()) :: keyword()
  defp flow_policy_response_to_set_opts(policy) do
    [
      retry: flow_policy_retry_to_set_opts(Map.get(policy, :retry, %{})),
      retention: flow_policy_retention_to_set_opts(Map.get(policy, :retention, %{})),
      states: flow_policy_states_to_set_opts(Map.get(policy, :states, %{}))
    ]
    |> flow_policy_maybe_put_opt(
      :indexed_attributes,
      flow_policy_field(policy, :indexed_attributes, [])
    )
    |> flow_policy_maybe_put_opt(
      :indexed_state_meta,
      flow_policy_field(policy, :indexed_state_meta, nil)
    )
    |> flow_policy_maybe_put_opt(:version, flow_policy_field(policy, :version, nil))
    |> flow_policy_maybe_put_opt(:governance, flow_policy_field(policy, :governance, nil))
  end

  @spec flow_policy_states_to_set_opts(map() | term()) :: list()
  defp flow_policy_states_to_set_opts(states) when is_map(states) do
    Enum.map(states, fn {state, policy} ->
      {to_string(state),
       [
         mode: flow_policy_field(policy, :mode, :parallel),
         retry: flow_policy_retry_to_set_opts(Map.get(policy, :retry, %{})),
         retention: flow_policy_retention_to_set_opts(Map.get(policy, :retention, %{}))
       ]
       |> flow_policy_maybe_put_opt(:governance, flow_policy_field(policy, :governance, nil))}
    end)
  end

  defp flow_policy_states_to_set_opts(_states), do: []

  @spec flow_policy_retry_to_set_opts(map()) :: keyword()
  defp flow_policy_retry_to_set_opts(retry) when is_map(retry) do
    backoff = flow_policy_field(retry, :backoff, %{})

    [
      max_retries: flow_policy_field(retry, :max_retries, 3),
      backoff: [
        kind: flow_policy_field(backoff, :kind, :exponential),
        base_ms: flow_policy_field(backoff, :base_ms, 1_000),
        max_ms: flow_policy_field(backoff, :max_ms, 30_000),
        jitter_pct: flow_policy_field(backoff, :jitter_pct, 20)
      ],
      exhausted_to: flow_policy_field(retry, :exhausted_to, "failed")
    ]
  end

  defp flow_policy_retry_to_set_opts(_retry),
    do: flow_policy_retry_to_set_opts(Ferricstore.Flow.RetryPolicy.default())

  @spec flow_policy_retention_to_set_opts(map()) :: keyword()
  defp flow_policy_retention_to_set_opts(retention) when is_map(retention) do
    [
      ttl_ms: flow_policy_field(retention, :ttl_ms, 604_800_000),
      history_max_events: flow_policy_field(retention, :history_max_events, 100_000)
    ]
  end

  defp flow_policy_retention_to_set_opts(_retention),
    do: flow_policy_retention_to_set_opts(Ferricstore.Flow.RetryPolicy.default_retention())

  @spec flow_policy_merge_form_opts(
          keyword(),
          binary() | nil,
          atom(),
          keyword(),
          keyword(),
          keyword()
        ) ::
          keyword()
  defp flow_policy_merge_form_opts(existing_opts, nil, _mode, retry, retention, indexes) do
    existing_opts
    |> Keyword.put(:retry, retry)
    |> Keyword.put(:retention, retention)
    |> flow_policy_apply_index_action(
      :indexed_attributes,
      Keyword.get(indexes, :indexed_attributes, :preserve)
    )
    |> flow_policy_apply_index_action(
      :indexed_state_meta,
      Keyword.get(indexes, :indexed_state_meta, :preserve)
    )
  end

  defp flow_policy_merge_form_opts(existing_opts, state, mode, retry, retention, _indexes)
       when is_binary(state) do
    states =
      existing_opts
      |> Keyword.get(:states, [])
      |> flow_policy_put_state_policy(state, mode: mode, retry: retry, retention: retention)

    Keyword.put(existing_opts, :states, states)
  end

  @spec flow_policy_put_state_policy(list(), binary(), keyword()) :: list()
  defp flow_policy_put_state_policy(states, state, policy) do
    states
    |> Enum.reject(fn {existing_state, _policy} -> existing_state == state end)
    |> Kernel.++([{state, policy}])
  end

  @spec flow_policy_form_retry_opts(map()) :: {:ok, keyword()} | {:error, binary()}
  defp flow_policy_form_retry_opts(params) do
    with {:ok, max_retries} <- flow_policy_form_integer(params, "max_retries", 0),
         {:ok, kind} <- flow_policy_form_backoff_kind(params),
         {:ok, base_ms} <- flow_policy_form_integer(params, "base_ms", 0),
         {:ok, max_ms} <- flow_policy_form_integer(params, "max_ms", 0),
         {:ok, jitter_pct} <- flow_policy_form_integer(params, "jitter_pct", 0),
         {:ok, exhausted_to} <-
           flow_policy_required_form_value(params, "exhausted_to", "exhausted_to") do
      {:ok,
       [
         max_retries: max_retries,
         backoff: [kind: kind, base_ms: base_ms, max_ms: max_ms, jitter_pct: jitter_pct],
         exhausted_to: exhausted_to
       ]}
    end
  end

  @spec flow_policy_form_retention_opts(map()) :: {:ok, keyword()} | {:error, binary()}
  defp flow_policy_form_retention_opts(params) do
    with {:ok, ttl_ms} <- flow_policy_form_integer(params, "retention_ttl_ms", 1),
         {:ok, history_max_events} <- flow_policy_form_integer(params, "history_max_events", 1) do
      {:ok,
       [
         ttl_ms: ttl_ms,
         history_max_events: history_max_events
       ]}
    end
  end

  @spec flow_policy_form_mode(map()) :: {:ok, :parallel | :fifo} | {:error, binary()}
  defp flow_policy_form_mode(params) do
    case params
         |> Map.get("mode", "parallel")
         |> flow_policy_clean_form_value()
         |> String.downcase() do
      "parallel" -> {:ok, :parallel}
      "fifo" -> {:ok, :fifo}
      _ -> {:error, "ERR flow state mode must be parallel or fifo"}
    end
  end

  @spec flow_policy_form_index_opts(map()) :: {:ok, keyword()} | {:error, binary()}
  defp flow_policy_form_index_opts(params) do
    with {:ok, indexed_attributes} <- flow_policy_form_indexed_attributes(params),
         {:ok, indexed_state_meta} <- flow_policy_form_indexed_state_meta(params) do
      opts =
        []
        |> flow_policy_maybe_put_index_action(:indexed_attributes, indexed_attributes)
        |> flow_policy_maybe_put_index_action(:indexed_state_meta, indexed_state_meta)

      {:ok, opts}
    end
  end

  @spec flow_policy_form_indexed_attributes(map()) ::
          {:ok, :preserve | {:set, [binary()]}} | {:error, binary()}
  defp flow_policy_form_indexed_attributes(params) do
    if Map.has_key?(params, "indexed_attributes") do
      attrs =
        params
        |> Map.get("indexed_attributes", "")
        |> flow_policy_clean_form_value()

      names =
        attrs
        |> String.split([",", " ", "\n", "\t"], trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
        |> Enum.uniq()

      {:ok, {:set, names}}
    else
      {:ok, :preserve}
    end
  end

  @spec flow_policy_form_indexed_state_meta(map()) :: {:ok, :preserve | {:set, binary() | nil}}
  defp flow_policy_form_indexed_state_meta(params) do
    if Map.has_key?(params, "indexed_state_meta") do
      with {:ok, indexed_state_meta} <-
             flow_policy_optional_form_value(params, "indexed_state_meta") do
        {:ok, {:set, indexed_state_meta}}
      end
    else
      {:ok, :preserve}
    end
  end

  @spec flow_policy_form_backoff_kind(map()) :: {:ok, atom()} | {:error, binary()}
  defp flow_policy_form_backoff_kind(params) do
    case params
         |> Map.get("backoff_kind", "")
         |> flow_policy_clean_form_value()
         |> String.downcase() do
      "none" -> {:ok, :none}
      "fixed" -> {:ok, :fixed}
      "linear" -> {:ok, :linear}
      "exponential" -> {:ok, :exponential}
      _ -> {:error, "ERR flow retry backoff kind must be none, fixed, linear, or exponential"}
    end
  end

  @spec flow_policy_form_integer(map(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  defp flow_policy_form_integer(params, field, min) do
    value = params |> Map.get(field, "") |> flow_policy_clean_form_value()

    case Integer.parse(value) do
      {integer, ""} when integer >= min ->
        {:ok, integer}

      _ ->
        {:error, "ERR #{String.replace(field, "_", " ")} must be an integer >= #{min}"}
    end
  end

  @spec flow_policy_required_form_value(map(), binary(), binary()) ::
          {:ok, binary()} | {:error, binary()}
  defp flow_policy_required_form_value(params, field, label) do
    case flow_policy_clean_form_value(Map.get(params, field, "")) do
      "" -> {:error, "ERR #{label} is required"}
      value -> {:ok, value}
    end
  end

  @spec flow_policy_optional_form_value(map(), binary()) :: {:ok, binary() | nil}
  defp flow_policy_optional_form_value(params, field) do
    case flow_policy_clean_form_value(Map.get(params, field, "")) do
      "" -> {:ok, nil}
      value -> {:ok, value}
    end
  end

  @spec flow_policy_clean_form_value(term()) :: binary()
  defp flow_policy_clean_form_value(value) when is_binary(value), do: String.trim(value)
  defp flow_policy_clean_form_value(value), do: value |> to_string() |> String.trim()

  defp flow_policy_maybe_put_opt(opts, _key, value) when value in [nil, []], do: opts
  defp flow_policy_maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp flow_policy_put_nullable_opt(opts, key, nil), do: Keyword.delete(opts, key)
  defp flow_policy_put_nullable_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp flow_policy_apply_index_action(opts, _key, :preserve), do: opts

  defp flow_policy_apply_index_action(opts, :indexed_state_meta, {:set, value}) do
    flow_policy_put_nullable_opt(opts, :indexed_state_meta, value)
  end

  defp flow_policy_apply_index_action(opts, key, {:set, value}), do: Keyword.put(opts, key, value)

  defp flow_policy_maybe_put_index_action(opts, _key, :preserve), do: opts
  defp flow_policy_maybe_put_index_action(opts, key, action), do: Keyword.put(opts, key, action)

  defp flow_policy_indexed_attributes_string(names) when is_list(names) do
    names
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.map_join(", ", &to_string/1)
  end

  defp flow_policy_indexed_attributes_string(_names), do: ""

  @spec flow_retention_form_limit(term()) :: {:ok, pos_integer()} | {:error, binary()}
  defp flow_retention_form_limit(nil), do: {:ok, @flow_dashboard_retention_default_limit}
  defp flow_retention_form_limit(""), do: {:ok, @flow_dashboard_retention_default_limit}

  defp flow_retention_form_limit(value) when is_integer(value) do
    if value >= 1 and value <= @flow_dashboard_retention_max_limit do
      {:ok, value}
    else
      {:error, "ERR cleanup limit must be between 1 and #{@flow_dashboard_retention_max_limit}"}
    end
  end

  defp flow_retention_form_limit(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} -> flow_retention_form_limit(integer)
      _ -> {:error, "ERR cleanup limit must be an integer"}
    end
  end

  defp flow_retention_form_limit(_value), do: {:error, "ERR cleanup limit must be an integer"}

  @spec flow_retention_limit!(term()) :: pos_integer()
  defp flow_retention_limit!(value) do
    case flow_retention_form_limit(value) do
      {:ok, limit} -> limit
      {:error, _reason} -> @flow_dashboard_retention_default_limit
    end
  end

  @spec flow_retention_candidates([map()], integer()) :: [map()]
  defp flow_retention_candidates(records, now_ms) do
    records
    |> Enum.filter(&flow_retention_candidate?(&1, now_ms))
    |> Enum.sort_by(&flow_retention_until_ms/1, :asc)
  end

  @spec flow_retention_candidate?(map(), integer()) :: boolean()
  defp flow_retention_candidate?(record, now_ms) do
    case flow_retention_until_ms(record) do
      until_ms when is_integer(until_ms) ->
        flow_retention_terminal_record?(record) and until_ms <= now_ms

      _ ->
        false
    end
  end

  @spec flow_retention_terminal_record?(map()) :: boolean()
  defp flow_retention_terminal_record?(record) do
    record
    |> flow_record_state()
    |> String.downcase()
    |> then(&(&1 in @flow_terminal_states))
  end

  @spec flow_dashboard_retention_cleanup(keyword()) :: {:ok, map()} | {:error, term()}
  defp flow_dashboard_retention_cleanup(opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_retention_cleanup_fun) do
      fun when is_function(fun, 1) -> fun.(opts)
      _ -> FerricStore.flow_retention_cleanup(opts)
    end
  end

  @spec flow_retention_cleanup_counts(map(), pos_integer()) :: map()
  defp flow_retention_cleanup_counts(result, limit) do
    %{
      limit: limit,
      flows: flow_retention_count(result, :flows),
      history: flow_retention_count(result, :history),
      values: flow_retention_count(result, :values)
    }
  end

  @spec flow_retention_count(map(), atom()) :: non_neg_integer()
  defp flow_retention_count(result, key) do
    case Map.get(result, key, Map.get(result, Atom.to_string(key), 0)) do
      count when is_integer(count) and count >= 0 -> count
      _ -> 0
    end
  end

  @spec flow_retention_query_integer(map(), binary()) :: non_neg_integer()
  defp flow_retention_query_integer(params, key) do
    case Integer.parse(Map.get(params, key, "0")) do
      {integer, ""} when integer >= 0 -> integer
      _ -> 0
    end
  end
end
