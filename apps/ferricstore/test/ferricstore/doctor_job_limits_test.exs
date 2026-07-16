defmodule Ferricstore.DoctorJobLimitsTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Doctor

  @max_running_jobs 4
  @max_retained_jobs 64

  setup do
    original_hook = Application.get_env(:ferricstore, :doctor_check_hook)
    Doctor.clear_for_test()

    on_exit(fn ->
      Doctor.clear_for_test()

      if is_nil(original_hook) do
        Application.delete_env(:ferricstore, :doctor_check_hook)
      else
        Application.put_env(:ferricstore, :doctor_check_hook, original_hook)
      end
    end)

    :ok
  end

  test "rejects excess concurrent background jobs before spawning more work" do
    test_pid = self()

    Application.put_env(:ferricstore, :doctor_check_hook, fn ->
      send(test_pid, {:doctor_job_started, self()})

      receive do
        :release_doctor_job -> :ok
      end
    end)

    ctx = FerricStore.Instance.get(:default)

    for _index <- 1..@max_running_jobs do
      assert %{"status" => "running"} = Doctor.start_job(:check, ctx, [])
    end

    workers =
      for _index <- 1..@max_running_jobs do
        assert_receive {:doctor_job_started, worker}, 1_000
        worker
      end

    assert {:error, "ERR too many doctor jobs are already running"} =
             Doctor.start_job(:check, ctx, [])

    Enum.each(workers, &send(&1, :release_doctor_job))
  end

  test "retains only a bounded number of terminal jobs" do
    ctx = FerricStore.Instance.get(:default)

    for _index <- 1..(@max_retained_jobs + 5) do
      %{"job_id" => job_id} = Doctor.start_job(:check, ctx, [])
      assert_terminal(job_id)
    end

    assert %{"jobs" => jobs} = Doctor.list_jobs()
    assert length(jobs) == @max_retained_jobs
    assert Enum.all?(jobs, &(&1["status"] == "done"))
  end

  test "does not run two projection repairs against the same instance" do
    ctx = FerricStore.Instance.get(:default)
    now = System.system_time(:millisecond)

    running_repair = %{
      id: "doctor-existing-repair",
      kind: :repair_projections,
      instance: ctx.name,
      scopes: [:flow_lmdb],
      status: :running,
      pid: nil,
      monitor: nil,
      created_at_ms: now,
      updated_at_ms: now,
      result: nil,
      error: nil
    }

    :sys.replace_state(Doctor, fn state ->
      %{state | jobs: Map.put(state.jobs, running_repair.id, running_repair)}
    end)

    assert {:error, "ERR doctor projection repair is already running for this instance"} =
             Doctor.start_job(:repair_projections, ctx, [:flow_lmdb])
  end

  defp assert_terminal(job_id, attempts \\ 100)

  defp assert_terminal(job_id, attempts) when attempts > 0 do
    case Doctor.status(job_id) do
      %{"status" => status} when status in ["done", "failed", "cancelled"] ->
        :ok

      _running ->
        Process.sleep(5)
        assert_terminal(job_id, attempts - 1)
    end
  end

  defp assert_terminal(job_id, 0), do: flunk("doctor job #{job_id} did not finish")
end
