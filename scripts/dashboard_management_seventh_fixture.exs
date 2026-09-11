alias FerricstoreServer.Health.Dashboard

alias FerricstoreServer.Health.Dashboard.Flow.{
  PolicyEditor,
  RetentionReview,
  ScheduleEditor,
  Schedules
}

alias Ferricstore.Flow.Schedule

output = List.first(System.argv()) || "outputs/dashboard-seventh-fixes/management/fixtures"
File.mkdir_p!(output)
now = System.system_time(:millisecond)

{:ok, definition} =
  Schedule.preview("customer-reconciliation-europe",
    every_ms: 60_001,
    start_at_ms: now + 3600_000,
    end_at_ms: now + 86_400_000,
    overlap_policy: :queue_after_previous,
    overlap_retry_ms: 4321,
    max_fires: 1440,
    target: [
      type: "customer-reconciliation",
      partition_key: "tenant-europe",
      state: "scheduled",
      priority: 2,
      payload: %{"source" => "scheduler", "region" => "eu-west"}
    ],
    now_ms: now
  )

schedule =
  Map.merge(definition, %{
    version: 7,
    state: "paused",
    fire_count: 12,
    action_capabilities: %{
      create: true,
      edit: true,
      fire: true,
      pause: true,
      resume: true,
      delete: true
    }
  })

{:ok, draft} = ScheduleEditor.draft(schedule)
filters = %{id: schedule.id, state: :all, kind: nil, q: nil, limit: 1}

schedule_data = %{
  summary: %{"paused" => 1, total: 1},
  schedules: [schedule],
  filters: filters,
  draft: draft,
  hydrated_edit: true,
  action_capabilities: %{create: true},
  generated_at_ms: now
}

error =
  Schedules.create_error_page(
    Map.put(draft, "target_payload", "{invalid"),
    "target payload must be valid JSON"
  )
  |> Map.put(:filters, filters)

{:ok, planned} =
  Schedule.preview(schedule.id,
    every_ms: 120_000,
    start_at_ms: now + 3600_000,
    end_at_ms: now + 86_400_000,
    overlap_policy: :queue_after_previous,
    overlap_retry_ms: 4321,
    max_fires: 1440,
    target: Map.to_list(schedule.target),
    now_ms: now
  )

review_fields = %{
  "expected_state" => "paused",
  "expected_version" => "7",
  "reviewed_at_ms" => to_string(now),
  "review_fingerprint" => "fixture-not-for-mutation"
}

reviewed = %{
  draft: Map.put(draft, "every_ms", "120000"),
  filters: filters,
  catalog_loaded: false,
  review: %{current: schedule, planned: planned, fields: review_fields}
}

editor = %{
  PolicyEditor.empty()
  | type: "customer-reconciliation",
    max_retries: 8,
    base_ms: 5000,
    max_ms: 300_000,
    max_active_ms: 3_600_000,
    retention_ttl_ms: 604_800_000
}

states =
  Enum.map(1..11, fn i ->
    %{state: "state-#{i}", mode: if(rem(i, 2) == 0, do: :fifo, else: :parallel)}
  end) ++ [%{state: "last<&state", mode: :fifo}]

policy = %{
  type: editor.type,
  source: "configured",
  generation: 12,
  retry: %{max_retries: 8, backoff: %{kind: :exponential, base_ms: 5000, max_ms: 300_000}},
  retention: %{ttl_ms: 604_800_000, history_max_events: 1000},
  max_active_ms: 3_600_000,
  states: states
}

policy_data = %{editor: editor, policies: [policy], generated_at_ms: now}

policy_draft =
  PolicyEditor.error_page(
    %{
      "type" => editor.type,
      "state" => "state-2",
      "max_retries" => "9",
      "retention_ttl_ms" => "7",
      "retention_ttl_ms_unit" => "days"
    },
    "Policy changed since this draft was loaded"
  )

retention = %{
  limit: 1,
  limit_input: "1",
  now_ms: now,
  total_sampled: 400,
  sample_limit: 400,
  active_sampled: 312,
  active_timeout_eligible_sampled: 12,
  terminal_eligible_sampled: 3,
  active_timeout_candidates: [
    %{
      id: "expired-active-001",
      type: "customer-reconciliation",
      state: "running",
      partition_key: "tenant-europe",
      created_at_ms: now - 7200_000,
      max_active_ms: 3600_000
    }
  ],
  candidates: [
    %{
      id: "expired-terminal-001",
      type: "customer-reconciliation",
      state: "completed",
      partition_key: "tenant-europe",
      terminal_retention_until_ms: now - 3600_000
    }
  ],
  action_capabilities: %{cleanup: true}
}

global_review = RetentionReview.prepare(1, now) |> Map.put(:kind, :review)

pages = %{
  "schedules-edit" => Dashboard.render_flow_schedules_page(schedule_data),
  "schedules-review" => Dashboard.render_flow_schedules_page(reviewed),
  "schedules-error" => Dashboard.render_flow_schedules_page(error),
  "schedules-view-only" =>
    Dashboard.render_flow_schedules_page(%{
      schedules: [
        Map.put(schedule, :action_capabilities, %{
          create: false,
          edit: false,
          fire: false,
          pause: false,
          resume: false,
          delete: false
        })
      ],
      action_capabilities: %{create: false},
      filters: filters
    }),
  "policies" => Dashboard.render_flow_policies_page(policy_data),
  "policy-error" => Dashboard.render_flow_policies_page(policy_draft),
  "retention-sample" => Dashboard.render_flow_retention_page(retention),
  "retention-review" =>
    Dashboard.render_flow_retention_page(Map.put(retention, :flash, global_review)),
  "retention-error" =>
    Dashboard.render_flow_retention_page(
      Map.merge(retention, %{
        limit_input: "not-a-number",
        flash: %{kind: :error, message: "ERR cleanup limit must be an integer"}
      })
    )
}

for {name, html} <- pages, do: File.write!(Path.join(output, name <> ".html"), html)

assets = Map.new([:css, :js], fn kind ->
  asset_path = Dashboard.Assets.path(kind)
  {:ok, content_type, body} = Dashboard.Assets.fetch(asset_path)
  {asset_path, %{content_type: content_type, body: body}}
end)
File.write!(Path.join(output, "assets.json"), Jason.encode!(assets))

IO.puts(
  "Management fixtures: #{Path.expand(output)} (#{map_size(pages)} pages; no store mutations)"
)
