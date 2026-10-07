defmodule FerricstoreHttp.AdmissionFailureTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.Admission
  alias FerricstoreHttp.Admission.StreamHandler

  defmodule FailingStream do
    def init(_id, _req, %{failure: :error}), do: raise("stream init failed")
    def init(_id, _req, %{failure: :throw}), do: throw(:stream_init_failed)
    def init(_id, _req, %{failure: :exit}), do: exit(:stream_init_failed)
  end

  test "failed downstream initialization releases admission without terminate callback" do
    start_supervised!({Admission, 1})
    req = %{method: "GET", path: "/", qs: "", version: :"HTTP/1.1", headers: %{}}

    opts = %{
      ferricstore_max_request_line_bytes: 1024,
      ferricstore_max_header_name_bytes: 128,
      ferricstore_max_header_value_bytes: 1024,
      stream_handlers: [FailingStream]
    }

    for failure <- [:error, :throw, :exit] do
      case failure do
        :error ->
          assert_raise RuntimeError, "stream init failed", fn ->
            StreamHandler.init(1, req, Map.put(opts, :failure, failure))
          end

        :throw ->
          assert catch_throw(StreamHandler.init(1, req, Map.put(opts, :failure, failure))) ==
                   :stream_init_failed

        :exit ->
          assert catch_exit(StreamHandler.init(1, req, Map.put(opts, :failure, failure))) ==
                   :stream_init_failed
      end

      assert Admission.stats() == %{in_flight: 0, limit: 1}
    end

    assert {[], accepted} = StreamHandler.init(2, req, %{opts | stream_handlers: []})
    assert Admission.stats().in_flight == 1
    :ok = StreamHandler.terminate(2, :normal, accepted)
    assert Admission.stats().in_flight == 0
  end
end
