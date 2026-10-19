defmodule FerricstoreServer.Health.Dashboard.DoctorRedesignTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Render.DoctorPages

  test "passing checks are summarised in one line" do
    html = DoctorPages.render_doctor_summary(check(["ok", "ok", "ok"]))

    assert html =~ "doctor-status is-healthy"
    assert html =~ "All 3 checks pass"
    assert html =~ "Checked in 7ms"
  end

  test "a warning or a failure is counted in the summary line" do
    warning = DoctorPages.render_doctor_summary(check(["ok", "warning", "ok"]))
    assert warning =~ "doctor-status is-warning"
    assert warning =~ "1 of 3 checks needs attention"

    failed = DoctorPages.render_doctor_summary(check(["error", "warning", "ok"]))
    assert failed =~ "doctor-status is-degraded"
    assert failed =~ "1 of 3 checks failed"
    refute failed =~ "All 3 checks pass"
  end

  test "checks read as named rows with their scope and a status badge" do
    html = DoctorPages.render_doctor_checks(check(["ok", "warning", "ok"]))

    assert html =~ ~s(<strong>Bitcask storage</strong>)
    assert html =~ ~s(<span class="mono">bitcask</span>)
    assert html =~ ~s(<strong>Flow LMDB projection</strong>)
    assert html =~ ~s(<span class="badge badge-ok">ok</span>)
    assert html =~ ~s(<span class="badge badge-warning">warning</span>)
    refute html =~ "<th>Command</th>"
  end

  test "the CLI reference is folded away" do
    html =
      DoctorPages.render_doctor_command_reference([
        %{command: "FERRICSTORE.DOCTOR CHECK", purpose: "Run checks.", permission: "ADMIN"}
      ])

    assert html =~
             ~r{<details class="dashboard-disclosure">\s*<summary>Run from the CLI</summary>}

    assert html =~ "FERRICSTORE.DOCTOR CHECK"
  end

  defp check(statuses) do
    scopes = ["bitcask", "blob_refs", "flow_lmdb"]

    %{
      "status" => if(Enum.all?(statuses, &(&1 == "ok")), do: "ok", else: "warning"),
      "duration_ms" => 7,
      "checks" =>
        statuses
        |> Enum.zip(scopes)
        |> Enum.map(fn {status, scope} ->
          %{
            "scope" => scope,
            "status" => status,
            "message" => "#{scope} message",
            "metrics" => %{}
          }
        end)
    }
  end
end
