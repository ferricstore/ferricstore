defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryProvenance do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  import FerricstoreServer.Health.Dashboard.Render.FlowQueryControls,
    only: [flow_query_kind_options: 0]

  @input_fields [
    :kind,
    :type,
    :partition_key,
    :state,
    :run_state,
    :id,
    :limit,
    :from_ms,
    :to_ms,
    :rev,
    :attribute_key,
    :attribute_value_type,
    :attribute_value,
    :state_meta_state,
    :state_meta_key,
    :state_meta_value_type,
    :state_meta_value
  ]

  def render(%{result: %{status: :ok}, generated_at_ms: captured_at} = data)
      when is_integer(captured_at) do
    form = Map.get(data, :workbench, %{})
    mode = if Map.get(form, :mode) == :advanced, do: "advanced", else: "guided"
    filters = Map.get(data, :filters, %{})
    timestamp = captured_at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

    """
    <div class="flow-query-provenance" data-flow-query-provenance data-flow-query-result-mode="#{mode}">
      <p>Result captured <time datetime="#{timestamp}">#{format_timestamp_ms_or_dash(captured_at)} UTC</time></p>
      <p>#{scope_summary(mode, filters)}</p>
      <details><summary>Executed inputs</summary><pre>#{executed_inputs(mode, filters, form)}</pre></details>
    </div>
    <p class="flow-query-draft-status" data-flow-query-draft-status hidden role="status" aria-live="polite"></p>
    #{draft_script()}
    """
  end

  def render(_data), do: ""

  defp scope_summary("advanced", _filters), do: "Raw FQL"

  defp scope_summary("guided", filters) do
    {_kind, operation} =
      List.keyfind(flow_query_kind_options(), filters[:kind], 0, {nil, "Guided query"})

    [
      {"Type", filters[:type]},
      {"Partition", filters[:partition_key]},
      {"Reference", filters[:id]}
    ]
    |> Enum.reject(fn {_label, value} -> value in [nil, ""] end)
    |> Enum.map(fn {label, value} -> label <> ": " <> escape(to_string(value)) end)
    |> then(&[operation | &1])
    |> Kernel.++(state_summary(filters))
    |> Enum.join(" &middot; ")
  end

  defp state_summary(%{kind: kind, state: state}) when kind in ~w(list search stats terminals) do
    cond do
      state not in [nil, ""] -> ["State: " <> escape(state)]
      kind == "terminals" -> []
      true -> ["All states"]
    end
  end

  defp state_summary(_filters), do: []

  defp executed_inputs("advanced", _filters, form) do
    escape(Map.get(form, :fql, "")) <> "\n\n" <> escape(Map.get(form, :params_json, "{}"))
  end

  defp executed_inputs("guided", filters, _form) do
    filters
    |> Map.take(@input_fields)
    |> Jason.encode!(pretty: true)
    |> escape()
  end

  defp draft_script do
    """
    <script>
    (() => {
      const workspace = document.currentScript.closest(".flow-query-workspace");
      const captured = workspace?.querySelector("[data-flow-query-provenance]");
      const status = workspace?.querySelector("[data-flow-query-draft-status]");
      if (!captured || !status) return;
      const mode = captured.dataset.flowQueryResultMode;
      const form = workspace.querySelector(mode === "advanced" ? "[data-flow-query-workbench-form]" : "[data-flow-query-form]");
      if (!form) return;
      const fingerprint = () => JSON.stringify(Array.from(new FormData(form).entries()));
      const executed = fingerprint();
      const update = () => {
        const selected = workspace.querySelector('[data-flow-query-mode-tab][aria-selected="true"]');
        const active = selected?.dataset.flowQueryModeTab;
        const dirty = active !== mode || fingerprint() !== executed;
        status.hidden = !dirty;
        status.textContent = !dirty ? "" : active === "guided"
          ? "Filters changed. Run to update results."
          : "Query changed. Run to update results.";
      };
      workspace.addEventListener("input", update);
      workspace.addEventListener("change", update);
      workspace.addEventListener("click", (event) => {
        if (event.target.closest("[data-flow-query-mode-tab]")) update();
      });
      workspace.addEventListener("keydown", (event) => {
        if (event.target.closest("[data-flow-query-mode-tab]")) update();
      });
      workspace.addEventListener("reset", () => window.setTimeout(update, 0));
      update();
    })();
    </script>
    """
  end
end
