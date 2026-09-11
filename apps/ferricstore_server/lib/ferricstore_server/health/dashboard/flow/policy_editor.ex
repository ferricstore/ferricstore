defmodule FerricstoreServer.Health.Dashboard.Flow.PolicyEditor do
  @moduledoc false

  alias Ferricstore.Flow.RetryPolicy

  @fields ~w(type state mode indexed_attributes indexed_state_meta max_retries backoff_kind
             base_ms max_ms jitter_pct exhausted_to max_active_ms retention_ttl_ms
             history_max_events expected_generation base_ms_unit max_ms_unit max_active_ms_unit retention_ttl_ms_unit)a

  def load(type, state \\ "") do
    if type == "" do
      empty()
    else
      opts = if state == "", do: [], else: [state: state]

      case FerricStore.flow_policy_get(type, opts) do
        {:ok, policy} -> from_policy(policy, state)
        {:error, _reason} -> Map.merge(empty(), %{type: type, state: state, load_error: true})
      end
    end
  end

  def empty do
    from_policy(%{type: "", generation: 0}, "")
  end

  def error_page(params, reason) do
    # Return only the submitted draft; a writer need not have catalog read access.
    editor =
      Enum.reduce(@fields, empty(), fn field, draft ->
        case Map.fetch(params, Atom.to_string(field)) do
          {:ok, value} when is_binary(value) -> Map.put(draft, field, value)
          _ -> draft
        end
      end)

    %{
      editor: Map.put(editor, :dirty, true),
      policies: [],
      catalog_loaded: false,
      flash: %{kind: :error, message: error_message(reason)}
    }
  end

  defp from_policy(policy, state) do
    retry = Map.get(policy, :retry, RetryPolicy.default())
    backoff = retry.backoff
    retention = Map.get(policy, :retention, RetryPolicy.default_retention())

    %{
      type: Map.get(policy, :type, ""),
      state: state,
      mode: Map.get(policy, :mode, :parallel),
      expected_generation: Map.get(policy, :generation, 0),
      indexed_attributes: Enum.join(Map.get(policy, :indexed_attributes, []), ", "),
      indexed_state_meta: Map.get(policy, :indexed_state_meta) || "",
      max_retries: retry.max_retries,
      backoff_kind: backoff.kind,
      base_ms: backoff.base_ms,
      max_ms: backoff.max_ms,
      jitter_pct: backoff.jitter_pct,
      exhausted_to: retry.exhausted_to,
      max_active_ms: Map.get(policy, :max_active_ms) || "",
      retention_ttl_ms: retention.ttl_ms,
      history_max_events: retention.history_max_events
    }
  end

  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message(_reason), do: "Policy could not be saved. Review the draft and retry."
end
