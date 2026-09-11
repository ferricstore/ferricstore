defmodule FerricstoreServer.Health.Dashboard.PolicyEditorReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Endpoint
  alias FerricstoreServer.Health.Endpoint.Session
  alias FerricstoreServer.Acl

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous = Application.get_env(:ferricstore, :protected_mode)
    Application.put_env(:ferricstore, :protected_mode, false)
    on_exit(fn -> restore_env(:protected_mode, previous) end)
    :ok
  end

  test "selected state loads effective overrides and a retention edit preserves FIFO and governance" do
    type = unique_type()

    {:ok, policy} =
      FerricStore.flow_policy_set(type,
        states: %{
          "queued" => [
            mode: :fifo,
            retry: [max_retries: 8],
            governance: [
              limits: %{"running" => %{limit: 1, enforcement: :strict_global, lease_size: 1}}
            ]
          ]
        }
      )

    data = Dashboard.collect_flow_policies_page(edit_type: type, edit_state: "queued")
    assert data.editor.state == "queued"
    assert data.editor.mode == :fifo
    assert data.editor.max_retries == 8
    assert data.editor.expected_generation == policy.generation

    params = form_params(data.editor) |> Map.put("history_max_events", "99999")
    assert {:ok, ^type} = Dashboard.apply_flow_policy_form(params)
    assert {:ok, saved} = FerricStore.flow_policy_get(type, state: "queued")
    assert saved.mode == :fifo
    assert saved.retry.max_retries == 8
    assert saved.retention.history_max_events == 99999
    assert saved.governance.limits["running"].limit == 1
  end

  test "new state inherits type retry settings and has parallel mode" do
    type = unique_type()
    {:ok, _} = FerricStore.flow_policy_set(type, retry: [max_retries: 9])
    data = Dashboard.collect_flow_policies_page(edit_type: type, edit_state: "new-state")
    assert data.editor.state == "new-state"
    assert data.editor.max_retries == 9
    assert data.editor.mode == :parallel
  end

  test "state edits preserve type settings and reject submitted type-wide fields" do
    type = unique_type()

    {:ok, _} =
      FerricStore.flow_policy_set(type,
        max_active_ms: 45_000,
        indexed_attributes: ["tenant"],
        indexed_state_meta: "risk"
      )

    editor = Dashboard.collect_flow_policies_page(edit_type: type, edit_state: "queued").editor
    params = form_params(editor) |> Map.put("max_retries", "7")

    for {field, value} <- [
          {"max_active_ms", "60000"},
          {"max_active_ms", ""},
          {"indexed_attributes", "other"},
          {"indexed_state_meta", "other"}
        ] do
      assert {:error, reason} = Dashboard.apply_flow_policy_form(Map.put(params, field, value))
      assert reason =~ "type defaults"
      assert {:ok, unchanged} = FerricStore.flow_policy_get(type)
      assert unchanged.max_active_ms == 45_000
      assert unchanged.indexed_attributes == ["tenant"]
      assert unchanged.indexed_state_meta == "risk"
      assert unchanged.generation == editor.expected_generation
    end

    assert {:ok, ^type} = Dashboard.apply_flow_policy_form(params)
    assert {:ok, saved} = FerricStore.flow_policy_get(type)
    assert saved.max_active_ms == 45_000
    assert saved.indexed_attributes == ["tenant"]
    assert saved.indexed_state_meta == "risk"
    assert saved.states["queued"].retry.max_retries == 7
  end

  test "scope is loaded with GET and cannot be silently changed in the mutation form" do
    type = unique_type()
    {:ok, _} = FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})

    response =
      http_get(
        Endpoint.port(),
        "/dashboard/flow/policies?" <> URI.encode_query(%{edit: type, edit_state: "queued"})
      )

    assert extract_status_code(response) == 200
    html = extract_body(response)
    assert html =~ ~s(aria-label="Policy scope")
    assert html =~ ~s(name="edit_state" value="queued")
    assert html =~ ~r/name="state"[^>]*value="queued"[^>]*readonly/
    assert html =~ ~r/name="type"[^>]*readonly/
    assert html =~ ~s(<option value="fifo" selected)
    assert html =~ "Load policy"
  end

  test "stale editor generation is rejected without overwriting a newer policy" do
    type = unique_type()
    {:ok, _} = FerricStore.flow_policy_set(type, retry: [max_retries: 4])
    editor = Dashboard.collect_flow_policies_page(edit_type: type).editor
    {:ok, _} = FerricStore.flow_policy_set(type, retry: [max_retries: 12])
    assert {:error, _} = Dashboard.apply_flow_policy_form(form_params(editor))
    assert {:ok, saved} = FerricStore.flow_policy_get(type)
    assert saved.retry.max_retries == 12
  end

  test "invalid policy save returns an escaped draft with its selected state and a fresh CSRF token" do
    type = unique_type()
    editor = Dashboard.collect_flow_policies_page(edit_type: type, edit_state: "queued").editor

    params =
      form_params(editor) |> Map.put("max_retries", "11") |> Map.put("exhausted_to", "running")

    response = post_policy(params)
    assert extract_status_code(response) == 422
    refute extract_header(response, "location")
    html = extract_body(response)
    assert html =~ ~s(name="state" value="queued")
    assert html =~ ~r/name="max_retries"[^>]*value="11"/
    assert html =~ ~s(name="exhausted_to" value="running")
    assert html =~ ~s(role="alert")
    assert html =~ ~s(name="_csrf_token")
    refute html =~ ~s(aria-label="Current workflow policies")

    hostile =
      params
      |> Map.put("max_retries", ~s|" onfocus="bad()|)
      |> Map.put("exhausted_to", "</input><script>bad()</script>")

    body = hostile |> post_policy() |> extract_body()
    assert body =~ "&quot; onfocus=&quot;bad()"
    refute body =~ "<script>bad()</script>"
  end

  test "a write-only policy principal gets its draft without catalog data" do
    type = unique_type()

    params =
      Dashboard.collect_flow_policies_page(edit_type: type).editor
      |> form_params()
      |> Map.put("exhausted_to", "running")

    username = "policy-writer-#{System.unique_integer([:positive])}"
    Application.put_env(:ferricstore, :protected_mode, true)
    assert :ok = Acl.set_user(username, ["on", "nopass", "%W~*", "-@all", "+FLOW.POLICY.SET"])
    on_exit(fn -> Acl.del_user(username) end)
    cookie = Session.session_cookie(username) |> String.split(";", parts: 2) |> hd()

    assert extract_status_code(
             http_get(Endpoint.port(), "/dashboard/flow/policies", [{"Cookie", cookie}])
           ) == 403

    response = post_policy(params, cookie)
    assert extract_status_code(response) == 422
    assert extract_body(response) =~ ~s(name="type" value="#{type}")
    refute extract_body(response) =~ "Current Flow Policies"
  end

  test "invalid enums and numeric text remain editable instead of reverting to defaults" do
    params =
      Dashboard.collect_flow_policies_page(edit_type: unique_type()).editor |> form_params()

    for {field, value} <- [
          {"mode", "unknown-mode"},
          {"backoff_kind", "unknown-backoff"},
          {"max_retries", "not-a-number"}
        ] do
      html = params |> Map.put(field, value) |> post_policy() |> extract_body()

      if field == "max_retries" do
        assert html =~ ~r/type="text"[^>]*name="max_retries"[^>]*value="not-a-number"/
      else
        assert html =~ ~s(<option value="#{value}" selected)
      end
    end
  end

  defp unique_type, do: "policy-editor-review-#{System.unique_integer([:positive])}"

  defp form_params(editor) do
    editor
    |> Map.take(
      ~w(type state mode indexed_attributes indexed_state_meta max_retries backoff_kind base_ms max_ms jitter_pct exhausted_to max_active_ms retention_ttl_ms history_max_events expected_generation)a
    )
    |> then(fn fields ->
      if editor.state == "",
        do: fields,
        else: Map.drop(fields, [:max_active_ms, :indexed_attributes, :indexed_state_meta])
    end)
    |> Map.new(fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp post_policy(params, session_cookie \\ "") do
    {token, csrf_cookie} = Session.csrf_pair()
    cookie = csrf_cookie |> String.split(";", parts: 2) |> hd()

    http_post_form(
      Endpoint.port(),
      "/dashboard/flow/policies",
      Map.put(params, "_csrf_token", token),
      [{"Cookie", Enum.join([session_cookie, cookie], "; ")}]
    )
  end
end
