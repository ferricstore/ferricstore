defmodule Ferricstore.Commands.DoctorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, Server}
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    store = %{instance_ctx: ctx}

    on_exit(fn ->
      if Code.ensure_loaded?(Ferricstore.Doctor) and
           function_exported?(Ferricstore.Doctor, :clear_for_test, 0) do
        Ferricstore.Doctor.clear_for_test()
      end

      IsolatedInstance.checkin(ctx)
    end)

    if Code.ensure_loaded?(Ferricstore.Doctor) and
         function_exported?(Ferricstore.Doctor, :clear_for_test, 0) do
      Ferricstore.Doctor.clear_for_test()
    end

    %{ctx: ctx, store: store}
  end

  test "FERRICSTORE.DOCTOR CHECK returns bounded structured health", %{store: store} do
    assert %{
             "status" => status,
             "checks" => checks,
             "duration_ms" => duration_ms
           } = Server.handle("FERRICSTORE.DOCTOR", ["CHECK"], store)

    assert status in ["ok", "warning", "error"]
    assert is_integer(duration_ms)
    assert duration_ms >= 0

    scopes = Enum.map(checks, & &1["scope"])
    assert "bitcask" in scopes
    assert "blob_refs" in scopes
    assert "flow_lmdb" in scopes
  end

  test "FERRICSTORE.DOCTOR CHECK supports a single scope", %{store: store} do
    assert %{"status" => _, "checks" => [%{"scope" => "flow_lmdb"} = check]} =
             Server.handle("FERRICSTORE.DOCTOR", ["CHECK", "SCOPE", "FLOW_LMDB"], store)

    assert check["status"] in ["ok", "warning", "error"]
    assert is_map(check["metrics"])
  end

  test "FERRICSTORE.DOCTOR CHECK accepts Redis-style case-insensitive arguments", %{
    store: store
  } do
    assert %{"checks" => [%{"scope" => "flow_lmdb"}]} =
             Server.handle("FERRICSTORE.DOCTOR", ["check", "Scope", "flow_lmdb"], store)
  end

  test "FERRICSTORE.DOCTOR CHECK supports counted multi-scope syntax", %{store: store} do
    assert %{"checks" => checks} =
             Server.handle(
               "FERRICSTORE.DOCTOR",
               ["CHECK", "SCOPES", "2", "BITCASK", "FLOW_LMDB"],
               store
             )

    assert Enum.map(checks, & &1["scope"]) == ["bitcask", "flow_lmdb"]
  end

  test "FERRICSTORE.DOCTOR START CHECK creates a background job and status is queryable", %{
    store: store
  } do
    assert %{"job_id" => job_id, "status" => "running", "kind" => "check"} =
             Server.handle("FERRICSTORE.DOCTOR", ["START", "CHECK", "SCOPE", "BITCASK"], store)

    assert is_binary(job_id)

    assert %{"jobs" => jobs} = Server.handle("FERRICSTORE.DOCTOR", ["LIST"], store)
    assert Enum.any?(jobs, &(&1["job_id"] == job_id))

    assert %{"status" => "done", "result" => %{"checks" => [%{"scope" => "bitcask"}]}} =
             eventually_status(job_id, store)
  end

  test "FERRICSTORE.DOCTOR CANCEL marks a running background job cancelled", %{store: store} do
    parent = self()

    Application.put_env(:ferricstore, :doctor_check_hook, fn ->
      send(parent, :doctor_started)

      receive do
        :release_doctor -> :ok
      after
        5_000 -> :ok
      end
    end)

    on_exit(fn ->
      Application.delete_env(:ferricstore, :doctor_check_hook)
      send(parent, :release_doctor)
    end)

    assert %{"job_id" => job_id} =
             Server.handle("FERRICSTORE.DOCTOR", ["START", "CHECK", "SCOPE", "FLOW_LMDB"], store)

    assert_receive :doctor_started, 1_000

    assert %{"job_id" => ^job_id, "status" => "cancelled"} =
             Server.handle("FERRICSTORE.DOCTOR", ["CANCEL", job_id], store)
  end

  test "FERRICSTORE.DOCTOR rejects invalid arguments", %{store: store} do
    assert {:error, "ERR unknown doctor scope 'NOPE'"} =
             Server.handle("FERRICSTORE.DOCTOR", ["CHECK", "SCOPE", "NOPE"], store)

    assert {:error, "ERR syntax error for 'ferricstore.doctor' command"} =
             Server.handle("FERRICSTORE.DOCTOR", ["CHECK", "SCOPES", "2", "BITCASK"], store)

    assert {:error, "ERR syntax error for 'ferricstore.doctor' command"} =
             Server.handle("FERRICSTORE.DOCTOR", ["CHECK", "SCOPES", "0"], store)

    assert {:error, "ERR wrong number of arguments for 'ferricstore.doctor' command"} =
             Server.handle("FERRICSTORE.DOCTOR", [], store)

    assert {:error, "ERR no such doctor job 'missing-job'"} =
             Server.handle("FERRICSTORE.DOCTOR", ["STATUS", "missing-job"], store)

    assert {:error, "ERR no such doctor job 'missing-job'"} =
             Server.handle("FERRICSTORE.DOCTOR", ["CANCEL", "missing-job"], store)

    assert {:error, "ERR no default instance available for 'ferricstore.doctor' command"} =
             Ferricstore.Doctor.handle_command(["CHECK"], %{})
  end

  test "FERRICSTORE.DOCTOR repair projections only accepts FLOW_LMDB scope", %{store: store} do
    assert {:error, "ERR doctor repair projections supports only FLOW_LMDB scope"} =
             Server.handle(
               "FERRICSTORE.DOCTOR",
               [
                 "START",
                 "REPAIR",
                 "PROJECTIONS",
                 "SCOPE",
                 "BITCASK"
               ],
               store
             )

    assert {:error, "ERR doctor repair projections supports only FLOW_LMDB scope"} =
             Server.handle(
               "FERRICSTORE.DOCTOR",
               ["START", "REPAIR", "PROJECTIONS", "SCOPE", "ALL"],
               store
             )
  end

  test "FERRICSTORE.DOCTOR repair projections starts bounded Flow LMDB job", %{store: store} do
    assert %{"job_id" => job_id, "status" => "running", "kind" => "repair_projections"} =
             Server.handle(
               "FERRICSTORE.DOCTOR",
               [
                 "START",
                 "REPAIR",
                 "PROJECTIONS",
                 "SCOPE",
                 "FLOW_LMDB"
               ],
               store
             )

    assert %{
             "status" => "done",
             "result" => %{"checks" => [%{"scope" => "flow_lmdb"}]}
           } = eventually_status(job_id, store)
  end

  test "dispatcher routes RESP parser AST for FERRICSTORE.DOCTOR", %{store: store} do
    assert %{"checks" => [%{"scope" => "bitcask"}]} =
             Dispatcher.dispatch_ast({:ferricstore_doctor, ["CHECK", "SCOPE", "BITCASK"]}, store)
  end

  test "COMMAND catalog exposes FERRICSTORE.DOCTOR" do
    assert [
             [
               "ferricstore.doctor",
               _arity,
               flags,
               0,
               0,
               0
             ]
           ] = Server.handle("COMMAND", ["INFO", "ferricstore.doctor"], %{})

    assert "admin" in flags
    assert "slow" in flags
  end

  defp eventually_status(job_id, store, attempts \\ 50)

  defp eventually_status(job_id, store, attempts) when attempts > 0 do
    case Server.handle("FERRICSTORE.DOCTOR", ["STATUS", job_id], store) do
      %{"status" => "done"} = status ->
        status

      %{"status" => "failed"} = status ->
        flunk("doctor job failed: #{inspect(status)}")

      _other ->
        Process.sleep(20)
        eventually_status(job_id, store, attempts - 1)
    end
  end

  defp eventually_status(job_id, store, 0) do
    flunk(
      "doctor job did not finish: #{inspect(Server.handle("FERRICSTORE.DOCTOR", ["STATUS", job_id], store))}"
    )
  end
end
