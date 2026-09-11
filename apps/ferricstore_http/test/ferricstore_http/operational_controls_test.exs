defmodule FerricstoreHttp.OperationalControlsTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.{Admission, ControlledTestBackend, Listener, Metrics}
  alias FerricstoreHttp.Test.HttpHelpers

  setup do
    start_supervised!(ControlledTestBackend)
    :ok
  end

  test "liveness remains healthy while readiness tracks the backend" do
    HttpHelpers.start_server(backend: ControlledTestBackend)

    ControlledTestBackend.put(:ready, false)
    assert {200, _headers, ~s({"status":"ok"})} = HttpHelpers.request(:get, "/health")
    assert {503, _headers, ~s({"status":"not_ready"})} = HttpHelpers.request(:get, "/ready")

    ControlledTestBackend.put(:ready, true)
    assert {200, _headers, ~s({"status":"ready"})} = HttpHelpers.request(:get, "/ready")
  end

  test "admission is atomic, bounded, and never underflows" do
    start_supervised!({Admission, 2})

    assert :ok = Admission.acquire()
    assert :ok = Admission.acquire()
    assert {:error, :request_limit} = Admission.acquire()
    assert Admission.stats() == %{in_flight: 2, limit: 2}

    assert :ok = Admission.release()
    assert :ok = Admission.release()
    assert :ok = Admission.release()
    assert Admission.stats() == %{in_flight: 0, limit: 2}
  end

  test "metrics use bounded labels and never retain request credentials" do
    start_supervised!({Admission, 3})
    start_supervised!(Metrics)
    started_at = System.monotonic_time()

    assert :ok =
             Metrics.observe(%{
               req: %{
                 method: "NONSTANDARD",
                 headers: %{"authorization" => "Basic private-secret"}
               },
               resp_status: 777,
               req_start: started_at,
               req_end: started_at + System.convert_time_unit(20, :millisecond, :native)
             })

    assert :ok = Metrics.observe_command_batch(3, 5)

    rendered = Metrics.render()
    assert rendered =~ ~s(requests_total{method="OTHER",status="0"} 1)
    assert rendered =~ ~s(request_duration_seconds_bucket{method="OTHER",le="0.025"} 1)
    assert rendered =~ "ferricstore_http_in_flight_request_limit 3"
    assert rendered =~ "ferricstore_http_command_batches_total 1"
    assert rendered =~ "ferricstore_http_command_batch_requests_total 3"
    assert rendered =~ "ferricstore_http_command_batch_commands_total 5"
    assert rendered =~ "ferricstore_http_command_multi_request_batches_total 1"
    refute rendered =~ "private-secret"
    refute rendered =~ "authorization"
  end

  test "the listener can stop accepting connections before release shutdown" do
    HttpHelpers.start_server(backend: ControlledTestBackend)

    assert :running = :ranch.get_status(Listener)
    assert :ok = Listener.suspend()
    assert :suspended = :ranch.get_status(Listener)
    assert :ok = Listener.suspend()
  end
end
