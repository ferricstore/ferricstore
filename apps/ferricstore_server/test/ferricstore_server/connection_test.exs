Code.require_file("connection_test/sections/server_accepts_tcp_connection.exs", __DIR__)

Code.require_file(
  "connection_test/sections/pipeline_prefetch_does_not_read_through_keyless_write_barrier.exs",
  __DIR__
)

Code.require_file(
  "connection_test/sections/hello_3_mid_session_returns_greeting_again.exs",
  __DIR__
)

defmodule FerricstoreServer.ConnectionTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser
  alias FerricstoreServer.Listener
  alias FerricstoreServer.Connection.Pipeline

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect(port) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    sock
  end

  defp send_raw(sock, data), do: :gen_tcp.send(sock, data)

  defp recv(sock, timeout \\ 500) do
    {:ok, data} = :gen_tcp.recv(sock, 0, timeout)
    data
  end

  defp hello3 do
    IO.iodata_to_binary(Encoder.encode(["HELLO", "3"]))
  end

  defp send_command(sock, args) do
    :gen_tcp.send(sock, IO.iodata_to_binary(Encoder.encode(args)))
  end

  defp flow_claim_waiter_registered?(type, partition) do
    [nil, "queued"]
    |> Enum.any?(fn state ->
      type
      |> Ferricstore.Flow.ClaimWaiters.wait_keys(state, nil, partition)
      |> Enum.any?(&(Ferricstore.Flow.ClaimWaiters.count(&1) > 0))
    end)
  end

  # ---------------------------------------------------------------------------
  # Setup / teardown
  # ---------------------------------------------------------------------------

  setup do
    # The application supervisor manages the Ranch listener.
    # Use the actual bound port (ephemeral in test env).
    {:ok, port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # TCP connection
  # ---------------------------------------------------------------------------

  use FerricstoreServer.ConnectionTest.Sections.ServerAcceptsTcpConnection

  use FerricstoreServer.ConnectionTest.Sections.PipelinePrefetchDoesNotReadThroughKeylessWriteBarrier

  use FerricstoreServer.ConnectionTest.Sections.Hello3MidSessionReturnsGreetingAgain

  def handle_quorum_submit(event, measurements, metadata, test_pid) do
    send(test_pid, {:quorum_submit, event, measurements, metadata})
  end

  defp attach_quorum_submit(handler_id) do
    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :batcher, :quorum_submit],
        &__MODULE__.handle_quorum_submit/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp drain_quorum_submits(acc \\ []) do
    receive do
      {:quorum_submit, _event, _measurements, _metadata} = msg ->
        drain_quorum_submits([msg | acc])
    after
      150 -> Enum.reverse(acc)
    end
  end

  defp quorum_batch_size_at_least?(
         {:quorum_submit, _event, %{batch_size: batch_size}, %{kind: :batch}},
         min_size
       ),
       do: batch_size >= min_size

  defp quorum_batch_size_at_least?(_event, _min_size), do: false

  defp assert_float_string(value, expected) when is_binary(value) do
    {parsed, ""} = Float.parse(value)
    assert_in_delta parsed, expected, 0.001
  end

  def handle_pipeline_claim_due_batch(_event, measurements, metadata, test_pid) do
    send(test_pid, {:pipeline_claim_due_batch, measurements, metadata})
  end

  def handle_flow_claim_due_stop(_event, measurements, metadata, test_pid) do
    send(test_pid, {:flow_claim_due_stop, measurements, metadata})
  end

  defp recv_all(_sock, _pattern, 0), do: ""

  defp recv_all(sock, pattern, count) do
    data = recv(sock)

    occurrences = count_occurrences(data, pattern)

    if occurrences >= count do
      data
    else
      data <> recv_all(sock, pattern, count - occurrences)
    end
  end

  defp recv_at_least(sock, min_bytes, timeout) do
    recv_at_least(sock, min_bytes, timeout, "")
  end

  defp recv_at_least(_sock, min_bytes, _timeout, acc) when byte_size(acc) >= min_bytes, do: acc

  defp recv_at_least(sock, min_bytes, timeout, acc) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, chunk} -> recv_at_least(sock, min_bytes, timeout, acc <> chunk)
      {:error, _} -> acc
    end
  end

  defp recv_values(sock, count), do: recv_values(sock, count, "", 20)

  defp recv_values(_sock, count, acc, attempts) when attempts <= 0 do
    case Parser.parse(acc) do
      {:ok, values, _rest} when length(values) >= count -> Enum.take(values, count)
      other -> flunk("expected #{count} RESP values, got #{inspect(other)} from #{inspect(acc)}")
    end
  end

  defp recv_values(sock, count, acc, attempts) do
    case Parser.parse(acc) do
      {:ok, values, _rest} when length(values) >= count ->
        Enum.take(values, count)

      _ ->
        case :gen_tcp.recv(sock, 0, 500) do
          {:ok, chunk} ->
            recv_values(sock, count, acc <> chunk, attempts - 1)

          {:error, :timeout} ->
            recv_values(sock, count, acc, attempts - 1)

          {:error, reason} ->
            flunk("socket closed before #{count} RESP values: #{inspect(reason)}")
        end
    end
  end

  defp count_occurrences(data, pattern) do
    data
    |> :binary.matches(pattern)
    |> length()
  end

  defp closed_or_eof?(sock) do
    case :gen_tcp.recv(sock, 0, 500) do
      {:error, :closed} -> true
      {:error, :econnreset} -> true
      {:error, :einval} -> true
      {:error, :enotconn} -> true
      {:ok, ""} -> true
      {:ok, _data} -> false
    end
  end

  defp pipeline_source do
    [
      "lib/ferricstore_server/connection/pipeline.ex",
      "lib/ferricstore_server/connection/pipeline/fast_paths.ex",
      "lib/ferricstore_server/connection/pipeline/flow.ex",
      "lib/ferricstore_server/connection/pipeline/pure_batch.ex",
      "lib/ferricstore_server/connection/pipeline/streaming.ex",
      "lib/ferricstore_server/connection/pipeline/fallback.ex"
    ]
    |> Enum.map_join("\n", &File.read!/1)
    |> String.replace("\n    ", "\n  ")
  end
end
