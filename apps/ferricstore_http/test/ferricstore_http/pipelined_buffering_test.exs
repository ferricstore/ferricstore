defmodule FerricstoreHttp.PipelinedBufferingTest do
  @moduledoc """
  EEF-CVE-2026-115701: while an HTTP/1.1 request is in progress, the
  connection must not keep reading and buffering the pipelined bytes behind
  it, or one client can exhaust the server's memory.
  """

  use ExUnit.Case, async: false

  alias FerricstoreHttp.Test.HttpHelpers

  defmodule BlockingBackend do
    @moduledoc false
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate("worker", "secret", opts), do: {:ok, {:session, Keyword.get(opts, :peer)}}
    def authenticate(_username, _password, _opts), do: {:error, :unauthenticated}

    # Holds the request until the test releases it.
    @impl FerricstoreHttp.Backend
    def execute_batch(_session, commands, _opts) do
      send(:persistent_term.get({__MODULE__, :test}), {:blocked, self()})

      receive do
        :release -> :ok
      after
        10_000 -> :ok
      end

      {:ok, Enum.map(commands, fn _command -> %{status: :ok, value: "PONG"} end)}
    end

    @impl FerricstoreHttp.Backend
    def ready?, do: true
  end

  @pipelined_bytes 32 * 1_024 * 1_024

  setup do
    :persistent_term.put({BlockingBackend, :test}, self())
    on_exit(fn -> :persistent_term.erase({BlockingBackend, :test}) end)
    HttpHelpers.start_server(backend: BlockingBackend)
    :ok
  end

  test "a connection does not buffer pipelined data while a request is in progress" do
    {:ok, socket} = HttpHelpers.connect()
    body = Jason.encode!(%{"commands" => [["PING"]]})

    :ok =
      :gen_tcp.send(socket, [
        "POST /v1/commands HTTP/1.1\r\n",
        "host: localhost\r\n",
        "content-type: application/json\r\n",
        "authorization: Basic ",
        Base.encode64("worker:secret"),
        "\r\n",
        "content-length: ",
        Integer.to_string(byte_size(body)),
        "\r\n\r\n",
        body
      ])

    assert_receive {:blocked, handler}, 2_000
    [connection] = :ranch.procs(FerricstoreHttp.Listener, :connections)
    before = held_bytes(connection)

    # Once the connection stops reading, this send blocks on full socket
    # buffers, so it runs on its own and is killed afterwards.
    sender = spawn(fn -> :gen_tcp.send(socket, :binary.copy("x", @pipelined_bytes)) end)
    Process.sleep(1_000)
    growth = held_bytes(connection) - before

    send(handler, :release)
    Process.exit(sender, :kill)
    :gen_tcp.close(socket)

    assert growth < div(@pipelined_bytes, 8),
           "connection holds #{growth} more bytes after #{@pipelined_bytes} pipelined bytes"
  end

  defp held_bytes(pid) do
    :erlang.garbage_collect(pid)
    [memory: memory, binary: binaries] = Process.info(pid, [:memory, :binary])
    memory + Enum.reduce(binaries, 0, fn {_id, size, _refs}, total -> total + size end)
  end
end
