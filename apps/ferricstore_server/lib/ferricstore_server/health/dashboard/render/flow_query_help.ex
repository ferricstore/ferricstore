defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryHelp do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  def examples do
    [
      %{
        label: "Recent runs",
        query:
          "FROM runs WHERE partition_key = @partition AND type = @type ORDER BY updated_at_ms DESC LIMIT 40 RETURN RECORDS(run_id, state, run_state, updated_at_ms)",
        params: %{"partition" => "orders", "type" => "invoice"}
      },
      %{
        label: "Exact run",
        query: "FROM runs WHERE partition_key = @partition AND run_id = @id RETURN RECORD",
        params: %{"partition" => "orders", "id" => "invoice-1042"}
      },
      %{
        label: "Indexed metadata",
        query:
          "FROM runs WHERE partition_key = @partition AND type = @type AND attribute['region'] = @region ORDER BY updated_at_ms DESC LIMIT 40 RETURN RECORDS(run_id, attribute['region'])",
        params: %{"partition" => "orders", "type" => "invoice", "region" => "eu"}
      },
      %{
        label: "Run history",
        query:
          "FROM events WHERE partition_key = @partition AND run_id = @id ORDER BY event_id ASC LIMIT 40 RETURN RECORDS(event_id, fields)",
        params: %{"partition" => "orders", "id" => "invoice-1042"}
      }
    ]
  end

  def render_reference do
    examples =
      Enum.map_join(examples(), "", fn example ->
        """
        <details class="flow-query-reference-example">
          <summary>#{escape(example.label)}</summary>
          <pre>#{escape(example.query)}</pre>
          <pre>#{escape(Jason.encode!(example.params, pretty: true))}</pre>
        </details>
        """
      end)

    """
    <details class="flow-query-reference" data-dashboard-disclosure-key="fql-reference">
      <summary>FQL1 reference</summary>
      <dl class="flow-query-reference-terms">
        <dt>Sources</dt><dd><code>FROM runs</code> or <code>FROM events</code></dd>
        <dt>Scope</dt><dd>Collection queries require one exact <code>partition_key</code>, a supported index, explicit order, and a bounded limit. A partition is a routing and ACL scope.</dd>
        <dt>Predicates</dt><dd><code>=</code>, <code>IN (...)</code>, <code>BETWEEN ... AND ...</code>, <code>IS NULL</code>, <code>IS MISSING</code>; joined with <code>AND</code>. Named values use <code>@parameter</code> and the JSON parameter map.</dd>
        <dt>Identity and state</dt><dd><code>run_id</code>, <code>event_id</code>, <code>type</code>, <code>state</code>, <code>run_state</code>, <code>parent_flow_id</code>, <code>root_flow_id</code>, <code>correlation_id</code></dd>
        <dt>Numeric and time fields</dt><dd><code>version</code>, <code>priority</code>, <code>attempts</code>, <code>max_active_ms</code>, <code>created_at_ms</code>, <code>updated_at_ms</code>, <code>next_run_at_ms</code>, <code>lease_deadline_ms</code>. Timestamps are Unix milliseconds.</dd>
        <dt>Metadata</dt><dd><code>attribute['key']</code>, <code>state_meta['state']['key']</code>. Search requires the corresponding configured index; names are case-sensitive.</dd>
        <dt>Return</dt><dd><code>RETURN RECORD</code> for a point lookup; <code>RETURN COUNT</code> for supported counts; <code>ORDER BY field ASC|DESC LIMIT n RETURN RECORDS(...)</code> for collections. Projections select up to 32 supported fields. Event metadata uses <code>fields</code> or <code>fields['key']</code>.</dd>
        <dt>Explain</dt><dd><code>EXPLAIN</code> plans but does not execute the read. <code>EXPLAIN ANALYZE</code> executes a bounded read and returns the plan and measured usage, not records.</dd>
        <dt>Boundaries</dt><dd>No full-scan fallback, SQL SELECT, OR, joins, or mutations. Read and explain permissions are checked independently.</dd>
      </dl>
      #{examples}
    </details>
    """
  end

  def render_empty_recovery(%{result: %{status: :ok, rows: []} = result} = data)
      when not is_map_key(result, :scalar) and not is_map_key(result, :explain) do
    mode = get_in(data, [:workbench, :mode]) || :guided
    filters = Map.get(data, :filters, %{})
    raw? = mode == :advanced
    panel = if raw?, do: "advanced", else: "guided"

    field =
      if raw?, do: "fql", else: if(Map.get(filters, :kind) == "history", do: "id", else: "type")

    scope =
      if raw? do
        "The executed FQL and parameters remain in the captured query above."
      else
        [type: "Type", partition_key: "Partition"]
        |> Enum.flat_map(fn {key, label} ->
          case Map.get(filters, key) do
            value when is_binary(value) and value != "" ->
              ["#{label}: <span class=\"mono\">#{escape(value)}</span>"]

            _ ->
              []
          end
        end)
        |> Enum.join(" &middot; ")
      end

    reset =
      if not raw? and Map.get(filters, :kind) in ["list", "search", "stats"] do
        ~s(<button type="button" class="flow-search-button secondary" data-flow-query-clear-optional>Remove optional filters</button>)
      else
        ""
      end

    """
    <section class="flow-query-empty" aria-label="Empty query recovery">
      <h3>No rows in this result page</h3>
      <p>#{scope}</p>
      <div class="flow-query-actions">
        <a class="flow-search-button" href="#flow-query-panel-#{panel}" data-flow-query-recover="#{panel}" data-flow-query-recover-field="#{field}">#{if raw?, do: "Edit FQL", else: "Review query scope"}</a>
        #{reset}
      </div>
      <p class="flow-section-note" data-flow-query-recovery-status aria-live="polite"></p>
    </section>
    """
  end

  def render_empty_recovery(_data), do: ""
end
