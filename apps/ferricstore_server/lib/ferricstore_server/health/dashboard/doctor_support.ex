defmodule FerricstoreServer.Health.Dashboard.DoctorSupport do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.QueryParams

  alias Ferricstore.Commands.Server, as: ServerCommands
  def doctor_command(args) do
    case ServerCommands.handle("FERRICSTORE.DOCTOR", args, doctor_command_store()) do
      %{} = result -> result
      {:error, reason} -> %{"status" => "error", "error" => reason, "checks" => []}
      other -> %{"status" => "error", "error" => inspect(other), "checks" => []}
    end
  rescue
    exception ->
      %{"status" => "error", "error" => Exception.message(exception), "checks" => []}
  catch
    kind, reason ->
      %{"status" => "error", "error" => inspect({kind, reason}), "checks" => []}
  end

  def doctor_command_store do
    %{instance_ctx: FerricStore.Instance.get(:default)}
  rescue
    _ -> %{}
  end

  def normalize_doctor_form_result(%{"job_id" => job_id, "status" => status}) do
    {:ok, "doctor job #{job_id} is #{status}"}
  end

  def normalize_doctor_form_result(%{"status" => status}) when is_binary(status) do
    {:ok, "doctor action returned #{status}"}
  end

  def normalize_doctor_form_result(%{"error" => error}) when is_binary(error),
    do: {:error, error}

  def normalize_doctor_form_result(other), do: {:error, inspect(other)}

  def doctor_flash(opts) do
    status = dashboard_param(opts, "status")
    message = dashboard_param(opts, "message")

    cond do
      status in ["ok", "error"] -> %{status: status, message: message}
      true -> %{}
    end
  end

  def doctor_command_reference do
    [
      %{
        command: "FERRICSTORE.DOCTOR CHECK [SCOPE <scope>]",
        purpose: "Run bounded read-only diagnostics immediately.",
        permission: "ADMIN FERRICSTORE.DOCTOR"
      },
      %{
        command: "FERRICSTORE.DOCTOR START CHECK [SCOPE <scope>]",
        purpose: "Run diagnostics as a background job.",
        permission: "ADMIN FERRICSTORE.DOCTOR"
      },
      %{
        command: "FERRICSTORE.DOCTOR START REPAIR PROJECTIONS SCOPE FLOW_LMDB",
        purpose: "Flush and reconcile the Flow LMDB cold projection from durable records.",
        permission: "ADMIN + DANGEROUS FERRICSTORE.DOCTOR"
      },
      %{
        command: "FERRICSTORE.DOCTOR STATUS <job_id> / LIST / CANCEL <job_id>",
        purpose: "Inspect or cancel doctor jobs.",
        permission: "ADMIN FERRICSTORE.DOCTOR"
      }
    ]
  end

  def doctor_status_class("ok"), do: "c-green"
  def doctor_status_class("done"), do: "c-green"
  def doctor_status_class("warning"), do: "c-yellow"
  def doctor_status_class("running"), do: "c-yellow"
  def doctor_status_class("error"), do: "c-red"
  def doctor_status_class("failed"), do: "c-red"
  def doctor_status_class("cancelled"), do: "c-muted"
  def doctor_status_class(_status), do: "c-muted"

  def doctor_metric_summary("bitcask", metrics) do
    keys = metrics |> Map.get("total_keydir_keys", 0) |> format_number()
    bytes = metrics |> Map.get("total_data_bytes", 0) |> format_bytes()
    files = metrics |> Map.get("total_data_files", 0) |> format_number()
    "#{keys} keys, #{files} files, #{bytes}"
  end

  def doctor_metric_summary("blob_refs", metrics) do
    files = metrics |> Map.get("total_segment_files", 0) |> format_number()
    bytes = metrics |> Map.get("total_segment_bytes", 0) |> format_bytes()
    protected = metrics |> Map.get("protected_refs", 0) |> format_number()
    "#{files} blob segments, #{bytes}, #{protected} protected"
  end

  def doctor_metric_summary("flow_lmdb", metrics) do
    pending = metrics |> Map.get("pending_ops", 0) |> format_number()
    age = metrics |> Map.get("max_oldest_pending_age_ms", 0) |> format_duration_ms()
    degraded = metrics |> Map.get("degraded_shards", 0) |> format_number()
    "#{pending} pending, oldest #{age}, #{degraded} degraded"
  end

  def doctor_metric_summary(_scope, metrics) when is_map(metrics), do: inspect(metrics)
  def doctor_metric_summary(_scope, _metrics), do: ""

  def doctor_job_result_summary(%{"error" => error}) when is_binary(error) and error != "",
    do: error

  def doctor_job_result_summary(%{"result" => %{"status" => status, "duration_ms" => ms}})
       when is_binary(status) do
    "#{status}, #{format_duration_ms(ms)}"
  end

  def doctor_job_result_summary(%{"status" => "running"}), do: "running"
  def doctor_job_result_summary(_job), do: ""
end
