Code.require_file(
  "sendfile_test/sections/encrypted_file_ref_streams_chunks.exs",
  __DIR__
)

Code.require_file(
  "sendfile_test/sections/sandboxed_pipeline_stream_refs.exs",
  __DIR__
)

defmodule FerricstoreServer.Connection.SendfileTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.BlobStore
  alias Ferricstore.Store.ActiveFile
  alias Ferricstore.Stats
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Bitcask.NIF
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Pipeline
  alias FerricstoreServer.Connection.Sendfile
  alias FerricstoreServer.Resp.Parser

  @blob_segment_header_bytes 48

  defmodule FakeTlsTransport do
    def send(test_pid, iodata) do
      Kernel.send(test_pid, {:fake_tls_send, IO.iodata_to_binary(iodata)})
      :ok
    end
  end

  setup do
    ClientTracking.init_tables()
    ActiveFile.init(1)
    :ets.delete_all_objects(:ferricstore_tracking)
    :ets.delete_all_objects(:ferricstore_tracking_connections)
    ShardHelpers.reset_memory_guard_pressure()

    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
      ClientTracking.cleanup(self())
    end)

    {:ok, ctx: ctx}
  end

  use FerricstoreServer.Connection.SendfileTest.Sections.EncryptedFileRefStreamsChunks

  use FerricstoreServer.Connection.SendfileTest.Sections.SandboxedPipelineStreamRefs

  defp tcp_pair do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, {:packet, :raw}, {:active, false}, {:reuseaddr, true}])

    parent = self()
    {:ok, port} = :inet.port(listen)

    acceptor =
      Task.async(fn ->
        {:ok, server_socket} = :gen_tcp.accept(listen)
        :ok = :gen_tcp.controlling_process(server_socket, parent)
        {:ok, server_socket}
      end)

    {:ok, client_socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, {:active, false}])
    {:ok, server_socket} = Task.await(acceptor)
    :gen_tcp.close(listen)
    {server_socket, client_socket}
  end

  defp write_tmp_file!(value, suffix \\ ".bin") do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_sendfile_test_#{System.unique_integer([:positive, :monotonic])}#{suffix}"
      )

    File.write!(path, value)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp write_tmp_blob_file!(value) do
    ref = BlobRef.from_payload(value)
    path = Path.join(System.tmp_dir!(), Base.encode16(ref.checksum, case: :lower) <> ".blob")
    File.write!(path, value)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp tmp_blob_root! do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_sendfile_blob_root_#{System.unique_integer([:positive, :monotonic])}"
      )

    File.rm_rf!(path)
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf(path) end)
    path
  end

  defp write_tmp_bitcask_record!(key, value) do
    path = write_tmp_file!("")
    assert {:ok, {record_offset, _record_size}} = NIF.v2_append_record(path, key, value, 0)
    {path, record_offset + 26 + byte_size(key)}
  end

  defp overwrite_file_range!(path, offset, data) do
    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      :ok = :file.pwrite(io, offset, data)
    after
      :file.close(io)
    end
  end

  defp collect_fake_tls_sends(acc \\ []) do
    receive do
      {:fake_tls_send, data} -> collect_fake_tls_sends([data | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp collect_sendfile_preads(acc \\ []) do
    receive do
      {:sendfile_pread, offset, size} -> collect_sendfile_preads([{offset, size} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp collect_sendfile_opens(acc \\ []) do
    receive do
      {:sendfile_open, path} -> collect_sendfile_opens([path | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp collect_blob_store_opens(acc \\ []) do
    receive do
      {:blob_store_open, path} -> collect_blob_store_opens([path | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp recv_until(sock, pattern, acc \\ "", attempts \\ 20)

  defp recv_until(_sock, _pattern, acc, 0), do: acc

  defp recv_until(sock, pattern, acc, attempts) do
    if String.contains?(acc, pattern) do
      acc
    else
      case :gen_tcp.recv(sock, 0, 100) do
        {:ok, data} -> recv_until(sock, pattern, acc <> data, attempts - 1)
        {:error, _reason} -> acc
      end
    end
  end
end
