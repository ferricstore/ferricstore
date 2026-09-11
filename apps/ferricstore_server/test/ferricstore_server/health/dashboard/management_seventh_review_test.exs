defmodule FerricstoreServer.Health.Dashboard.ManagementSeventhReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.{PolicyEditor, PolicyRetention, Schedules}

  alias FerricstoreServer.Health.Dashboard.Render.{
    FlowFormScripts,
    FlowPolicy,
    FlowRetention,
    FlowSchedules
  }

  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "finding 2: global cleanup has a separate exact command review with unknown impact" do
    assert {:ok, :review, review} =
             PolicyRetention.apply_retention_form(%{"action" => "review_cleanup", "limit" => "7"})

    assert review.limit == 7

    html =
      FlowRetention.render_flow_retention_controls(%{
        limit: 7,
        flash: Map.put(review, :kind, :review)
      })

    assert html =~ "Global cleanup review"
    assert html =~ "FLOW.RETENTION_CLEANUP LIMIT 7"
    assert html =~ "All shards, all workflow types and partitions"
    assert html =~ "Impact unknown"
    assert html =~ ~s(name="reviewed_limit" value="7")
    assert html =~ ~r/name="confirm_cleanup"[^>]*required/

    assert {:error, reason} =
             PolicyRetention.apply_retention_form(%{
               "action" => "cleanup",
               "limit" => "7",
               "confirm_cleanup" => "true"
             })

    assert reason =~ "review"
  end

  test "finding 3: cleanup rejection retains exact submitted limit including invalid input" do
    for raw <- ["1", "not-a-number"] do
      {token, csrf_cookie} = Session.csrf_pair()
      cookie = csrf_cookie |> String.split(";", parts: 2) |> hd()

      response =
        http_post_form(
          Endpoint.port(),
          "/dashboard/flow/retention",
          %{"action" => "cleanup", "limit" => raw, "_csrf_token" => token},
          [{"Cookie", cookie}]
        )

      location = extract_header(response, "location")
      assert URI.decode_query(URI.parse(location).query)["limit"] == raw
      html = location |> then(&http_get(Endpoint.port(), &1)) |> extract_body()
      assert html =~ ~s(name="limit")
      assert html =~ ~s(value="#{raw}")
      refute html =~ ~r/name="confirm_cleanup"[^>]*checked/
    end
  end

  test "cleanup scope uses the backend's shared global record budget" do
    html = FlowRetention.render_flow_retention_controls(%{limit: 7})
    assert html =~ "Global record limit"
    assert html =~ "Active timeouts consume the shared limit before terminal deletions"
    refute html =~ "Per-shard limit"
    refute html =~ "supplied per-shard limit"
  end

  test "finding 4: omitted terminal candidates are not reported as absent" do
    html =
      FlowRetention.render_flow_retention_candidates(%{
        candidates: [],
        active_timeout_candidates: [],
        terminal_eligible_sampled: 3,
        active_timeout_eligible_sampled: 0,
        now_ms: 1000
      })

    assert html =~ "0 of 3 sampled candidates shown"
    assert html =~ "omitted"
    refute html =~ "No expired terminal Flow records found"
  end

  test "finding 5: exact schedule return context survives actions and failed deletion" do
    params = %{
      "id" => "outside/sample & 1",
      "return_id" => "outside/sample & 1",
      "action" => "pause",
      "q" => "sample",
      "limit" => "1"
    }

    for result <- [{:ok, "paused"}, {:error, "stale version"}] do
      query =
        params
        |> Schedules.redirect_location(result)
        |> URI.parse()
        |> Map.fetch!(:query)
        |> URI.decode_query()

      assert query["id"] == params["return_id"]
      assert query["limit"] == "1"
    end

    failed_delete =
      Schedules.redirect_location(Map.put(params, "action", "delete"), {:error, "stale"})

    assert URI.decode_query(URI.parse(failed_delete).query)["id"] == params["id"]
    deleted = Schedules.redirect_location(Map.put(params, "action", "delete"), {:ok, "deleted"})
    refute Map.has_key?(URI.decode_query(URI.parse(deleted).query), "id")

    html =
      FlowSchedules.render_flow_schedules_table(
        [%{id: params["id"], state: "active", version: 1, kind: :interval}],
        %{id: params["id"]}
      )

    assert html =~ ~s(name="return_id" value="outside/sample &amp; 1")
  end

  test "finding 9: returned policy error is dirty before the first keystroke" do
    data =
      PolicyEditor.error_page(
        %{"type" => "draft-policy", "state" => "queued", "max_retries" => "9"},
        "stale policy generation"
      )

    assert data.editor.dirty == true
    assert FlowPolicy.render_flow_policy_editor(data) =~ ~s(data-policy-draft="true")
    assert FlowFormScripts.policy_script() =~ "form.dataset.policyDraft === 'true'"
  end

  test "finding 19: hydrated schedule edits preserve definition fields the form does not expose" do
    id = "seventh-edit-#{System.unique_integer([:positive])}"
    now = System.system_time(:millisecond)

    assert {:ok, _} =
             FerricStore.flow_schedule_create(id,
               every_ms: 60_001,
               start_at_ms: now + 3600_000,
               end_at_ms: now + 7200_000,
               max_fires: 6,
               overlap_policy: :queue_after_previous,
               overlap_retry_ms: 4321,
               target: [
                 type: "seventh-edit",
                 partition_key: "tenant",
                 state: "scheduled",
                 priority: 2,
                 payload: %{"keep" => [1, true]},
                 correlation_id: "keep-correlation"
               ]
             )

    data = Schedules.collect_page(id: id, edit: true)
    assert data.draft["id"] == id
    assert data.draft["every_ms"] == "60001"
    assert data.draft["overwrite"] == "true"
    params = Map.put(data.draft, "max_fires", "8")
    assert {:ok, review} = Schedules.preview_form(params)
    assert review.review.planned.target.state == "scheduled"
    assert review.review.planned.target.priority == 2
    assert review.review.planned.target.correlation_id == "keep-correlation"
    assert review.review.planned.overlap_retry_ms == 4321

    submit =
      params
      |> Map.merge(review.review.fields)
      |> Map.merge(%{"action" => "create", "confirm_replace" => "true"})

    assert {:ok, _} = Schedules.apply_form(submit)
    assert {:ok, saved} = FerricStore.flow_schedule_get(id)
    assert saved.max_fires == 8
    assert saved.every_ms == 60_001
    assert saved.target.payload == %{"keep" => [1, true]}
    assert saved.target.state == "scheduled"
    assert saved.target.priority == 2
    assert saved.overlap_retry_ms == 4321
    assert {:error, stale_reason} = Schedules.preview_form(params)
    assert stale_reason =~ "changed"
  end

  test "finding 20: every state override has an escaped discoverable editor link" do
    states =
      Enum.map(1..9, &%{state: "state-#{&1}", mode: :fifo}) ++
        [%{state: "last<&state", mode: :parallel}]

    html = FlowPolicy.render_flow_policy_state_overrides(states, "type<&")
    assert html =~ "<details"
    assert html =~ "data-policy-override-search"

    for state <- states do
      assert html =~ URI.encode_www_form(state.state)
    end

    assert html =~ "last&lt;&amp;state"
    refute html =~ "last<&state"
  end

  test "finding 22: policy and schedule controls have stable semantic decision groups" do
    policy =
      FlowPolicy.render_flow_policy_editor(%{editor: %{PolicyEditor.empty() | type: "groups"}})

    for group <- ["Scope", "Retry", "Indexing", "Retention"] do
      assert policy =~ "<legend>#{group}</legend>"
    end

    schedule = FlowSchedules.render_flow_schedule_create_form()

    for group <- ["Identity", "Timing", "Target", "Recurrence"] do
      assert schedule =~ "<legend>#{group}</legend>"
    end
  end

  test "finding 23: duration units preserve exact milliseconds and reject fractions below one millisecond" do
    duration = FerricstoreServer.Health.Dashboard.Flow.DurationFields

    assert {:ok, %{"every_ms" => "60001"}} =
             duration.normalize(%{"every_ms" => "60.001", "every_ms_unit" => "seconds"}, [
               "every_ms"
             ])

    assert {:ok, %{"retention_ttl_ms" => "604800000"}} =
             duration.normalize(%{"retention_ttl_ms" => "7", "retention_ttl_ms_unit" => "days"}, [
               "retention_ttl_ms"
             ])

    assert {:error, {"every_ms", _}} =
             duration.normalize(%{"every_ms" => "0.0001", "every_ms_unit" => "seconds"}, [
               "every_ms"
             ])

    assert {:error, {"every_ms", _}} =
             duration.normalize(%{"every_ms" => "1e3", "every_ms_unit" => "seconds"}, ["every_ms"])

    html = FlowSchedules.render_flow_schedule_create_form()
    assert html =~ ~s(name="every_ms_unit")
    assert html =~ ">Minutes</option>"

    assert FlowPolicy.render_flow_policy_editor(%{
             editor: %{PolicyEditor.empty() | type: "units"}
           }) =~ ~s(name="retention_ttl_ms_unit")
  end

  test "finding 24: policy and schedule drafts expose an explicit discard action" do
    policy =
      FlowPolicy.render_flow_policy_editor(%{editor: %{PolicyEditor.empty() | type: "discard"}})

    schedule = FlowSchedules.render_flow_schedule_create_form()

    for html <- [policy, schedule] do
      assert html =~ ">Discard changes</a>"
      assert html =~ "data-discard-draft"
    end

    for script <- [FlowFormScripts.policy_script(), FlowFormScripts.schedule_script()] do
      assert script =~ "data-discard-draft"
      assert script =~ "Discard unsaved changes?"
    end
  end

  test "cleanup review is read-only and rejected changed or expired review never reaches mutation" do
    previous = Application.get_env(:ferricstore, :flow_dashboard_retention_cleanup_fun)
    owner = self()

    Application.put_env(:ferricstore, :flow_dashboard_retention_cleanup_fun, fn opts ->
      send(owner, {:cleanup_called, opts})
      {:ok, %{active_timeouts: 1, flows: 0, history: 0, values: 0}}
    end)

    on_exit(fn -> restore_env(:flow_dashboard_retention_cleanup_fun, previous) end)

    assert {:ok, :review, review} =
             PolicyRetention.apply_retention_form(%{"action" => "review_cleanup", "limit" => "1"})

    refute_received {:cleanup_called, _}
    fields = Map.new(review, fn {key, value} -> {to_string(key), to_string(value)} end)
    confirmed = Map.merge(fields, %{"action" => "cleanup", "confirm_cleanup" => "true"})

    for rejected <- [
          Map.put(confirmed, "limit", "2"),
          Map.put(confirmed, "reviewed_at_ms", "1"),
          Map.delete(confirmed, "confirm_cleanup")
        ] do
      assert {:error, _} = PolicyRetention.apply_retention_form(rejected)
      refute_received {:cleanup_called, _}
    end

    assert {:ok, :cleanup, %{limit: 1}} = PolicyRetention.apply_retention_form(confirmed)
    assert_received {:cleanup_called, [limit: 1]}
  end

  test "unit-based schedule review retains the draft unit and uses exact milliseconds" do
    params = %{
      "id" => "units-only-preview",
      "schedule_kind" => "interval",
      "every_ms" => "1.00001",
      "every_ms_unit" => "minutes",
      "target_type" => "preview"
    }

    assert {:error, _} = Schedules.preview_form(params)
    params = Map.put(params, "every_ms", "1.00005")
    assert {:ok, page} = Schedules.preview_form(params)
    assert page.review.planned.every_ms == 60_003
    assert page.draft["every_ms_unit"] == "minutes"
    assert page.draft["every_ms"] == "1.00005"
  end

  test "hydrated edits retain an explicit initial run configured with at_ms" do
    id = "seventh-edit-initial-#{System.unique_integer([:positive])}"
    initial = System.system_time(:millisecond) + 3600_000

    assert {:ok, _} =
             FerricStore.flow_schedule_create(id,
               every_ms: 60_000,
               at_ms: initial,
               target: [type: "initial-timing"]
             )

    draft = Schedules.collect_page(id: id, edit: true).draft
    assert {:ok, page} = Schedules.preview_form(Map.put(draft, "max_fires", "3"))
    assert page.review.planned.initial_run_at_ms == initial
  end

  test "changing overlap mode does not retain a now-incompatible hidden retry option" do
    id = "seventh-edit-overlap-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             FerricStore.flow_schedule_create(id,
               every_ms: 60_000,
               overlap_policy: :queue_after_previous,
               overlap_retry_ms: 4321,
               target: [type: "overlap-edit"]
             )

    draft = Schedules.collect_page(id: id, edit: true).draft
    assert {:ok, page} = Schedules.preview_form(Map.put(draft, "overlap_policy", "skip"))
    assert page.review.planned.overlap_policy == :skip
    assert page.review.planned.overlap_retry_ms == nil
  end

  for {kind, field} <- [
        {:one_shot, :initial_run_at_ms},
        {:interval, :start_at_ms},
        {:interval, :end_at_ms}
      ] do
    test "hydrated editor reports unsupported calendar range for #{field} without raising" do
      schedule = %{
        id: "far-future",
        state: "active",
        version: 1,
        kind: unquote(kind),
        every_ms: 60_000,
        initial_run_at_ms: 1_800_000_000_000,
        target: %{type: "far-future"}
      }

      schedule = Map.put(schedule, unquote(field), 253_402_300_800_000)

      assert {:error, reason} =
               FerricstoreServer.Health.Dashboard.Flow.ScheduleEditor.draft(schedule)

      assert reason =~ "calendar range"
      assert reason =~ "FLOW.SCHEDULE.CREATE"
    end
  end
end
