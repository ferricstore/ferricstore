defmodule FerricstoreServer.Connection.SendfileTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.BlobStore
  alias Ferricstore.Store.ActiveFile
  alias Ferricstore.Stats
  alias Ferricstore.Test.IsolatedInstance
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

    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
      ClientTracking.cleanup(self())
    end)

    {:ok, ctx: ctx}
  end

  test "encrypted file ref response streams bounded chunks instead of one large binary" do
    chunk_bytes = Sendfile.file_stream_chunk_bytes()
    key = "k"
    value = IO.iodata_to_binary([:binary.copy("a", chunk_bytes), :binary.copy("b", 17)])
    {path, value_offset} = write_tmp_bitcask_record!(key, value)

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      tracking: nil
    }

    assert {:sent, ^state} =
             Sendfile.send_file_ref_response(key, path, value_offset, byte_size(value), state)

    sends = collect_fake_tls_sends()

    assert List.first(sends) ==
             ["$", Integer.to_string(byte_size(value)), "\r\n"] |> IO.iodata_to_binary()

    assert List.last(sends) == "\r\n"

    chunks = sends |> Enum.drop(1) |> Enum.drop(-1)
    assert length(chunks) == 2
    assert Enum.all?(chunks, &(byte_size(&1) <= chunk_bytes))
    assert IO.iodata_to_binary(chunks) == value
  end

  test "file ref response rejects an opened file whose record key does not match" do
    value = :binary.copy("s", Sendfile.threshold_bytes())
    {path, value_offset} = write_tmp_bitcask_record!("other-key", value)

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      tracking: nil
    }

    assert :fallback =
             Sendfile.send_file_ref_response(
               "expected-key",
               path,
               value_offset,
               byte_size(value),
               state
             )

    assert collect_fake_tls_sends() == []
  end

  test "encrypted blob file ref response streams the blob payload directly" do
    value = :binary.copy("b", Sendfile.file_stream_chunk_bytes() + 19)
    path = write_tmp_blob_file!(value)

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      tracking: nil
    }

    assert {:sent, ^state} =
             Sendfile.send_file_ref_response("blob-key", path, 0, byte_size(value), state)

    sends = collect_fake_tls_sends()
    assert List.first(sends) == "$#{byte_size(value)}\r\n"
    assert List.last(sends) == "\r\n"
    assert IO.iodata_to_binary(sends |> Enum.drop(1) |> Enum.drop(-1)) == value
  end

  test "tcp segment blob sendfile validates header without pre-reading payload" do
    root = tmp_blob_root!()
    value = :binary.copy("z", Sendfile.threshold_bytes() + 4096)
    assert {:ok, ref} = BlobStore.put(root, 0, value)
    assert {:ok, {path, offset, size}} = BlobStore.file_ref(root, 0, ref)
    assert size == byte_size(value)

    parent = self()

    Process.put(:ferricstore_sendfile_pread_hook, fn fd, read_offset, read_size ->
      send(parent, {:sendfile_pread, read_offset, read_size})
      :file.pread(fd, read_offset, read_size)
    end)

    {server_socket, client_socket} = tcp_pair()

    state = %{
      socket: server_socket,
      transport: :ranch_tcp,
      client_id: :test_client,
      tracking: nil
    }

    try do
      assert {:sent, ^state} =
               Sendfile.send_file_ref_response("blob-key", path, offset, size, state)

      assert recv_until(client_socket, "\r\n") =~ "$#{size}\r\n"
      assert collect_sendfile_preads() == [{offset - 48, 48}]
    after
      Process.delete(:ferricstore_sendfile_pread_hook)
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
    end
  end

  test "encrypted segment blob stream validates header without pre-reading payload twice" do
    root = tmp_blob_root!()
    chunk_bytes = Sendfile.file_stream_chunk_bytes()
    value = IO.iodata_to_binary([:binary.copy("s", chunk_bytes), :binary.copy("t", 19)])
    assert {:ok, ref} = BlobStore.put(root, 0, value)
    assert {:ok, {path, offset, size}} = BlobStore.file_ref(root, 0, ref)
    assert size == byte_size(value)

    parent = self()

    Process.put(:ferricstore_sendfile_pread_hook, fn fd, read_offset, read_size ->
      send(parent, {:sendfile_pread, read_offset, read_size})
      :file.pread(fd, read_offset, read_size)
    end)

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      tracking: nil
    }

    try do
      assert {:sent, ^state} =
               Sendfile.send_file_ref_response("blob-key", path, offset, size, state)

      sends = collect_fake_tls_sends()
      assert List.first(sends) == "$#{size}\r\n"
      assert List.last(sends) == "\r\n"
      assert IO.iodata_to_binary(sends |> Enum.drop(1) |> Enum.drop(-1)) == value

      assert collect_sendfile_preads() == [
               {offset - @blob_segment_header_bytes, @blob_segment_header_bytes},
               {offset, chunk_bytes},
               {offset + chunk_bytes, 19}
             ]
    after
      Process.delete(:ferricstore_sendfile_pread_hook)
    end
  end

  test "blob file ref response rejects an opened blob file with extra bytes" do
    value = :binary.copy("b", Sendfile.threshold_bytes())
    path = write_tmp_file!(value <> "stale-extra-bytes", ".blob")

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      tracking: nil
    }

    assert :fallback =
             Sendfile.send_file_ref_response("blob-key", path, 0, byte_size(value), state)

    assert collect_fake_tls_sends() == []
  end

  test "encrypted file range response streams only the requested range in bounded chunks" do
    chunk_bytes = Sendfile.file_stream_chunk_bytes()
    prefix = :binary.copy("p", 9)
    range = IO.iodata_to_binary([:binary.copy("r", chunk_bytes), :binary.copy("s", 23)])
    suffix = :binary.copy("z", 11)
    path = write_tmp_file!(IO.iodata_to_binary([prefix, range, suffix]))

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      tracking: nil
    }

    assert {:sent, ^state} =
             Sendfile.send_file_range_response(
               ["k", "9", Integer.to_string(9 + byte_size(range) - 1)],
               path,
               byte_size(prefix),
               byte_size(range),
               state
             )

    sends = collect_fake_tls_sends()

    assert List.first(sends) ==
             ["$", Integer.to_string(byte_size(range)), "\r\n"] |> IO.iodata_to_binary()

    assert List.last(sends) == "\r\n"

    chunks = sends |> Enum.drop(1) |> Enum.drop(-1)
    assert length(chunks) == 2
    assert Enum.all?(chunks, &(byte_size(&1) <= chunk_bytes))
    assert IO.iodata_to_binary(chunks) == range
  end

  test "encrypted blob file range response rejects a same-size corrupt blob before header" do
    value = :binary.copy("b", Sendfile.threshold_bytes() + 512)
    path = write_tmp_blob_file!(value)
    File.write!(path, :binary.copy("x", byte_size(value)))

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      tracking: nil
    }

    assert :fallback =
             Sendfile.send_file_range_response(
               ["blob-key", "0", Integer.to_string(Sendfile.threshold_bytes() - 1)],
               path,
               0,
               Sendfile.threshold_bytes(),
               state
             )

    assert collect_fake_tls_sends() == []
  end

  test "tcp blob file range response rejects a same-size corrupt blob before header" do
    value = :binary.copy("b", Sendfile.threshold_bytes() + 512)
    path = write_tmp_blob_file!(value)
    File.write!(path, :binary.copy("x", byte_size(value)))
    {server_socket, client_socket} = tcp_pair()

    state = %{
      socket: server_socket,
      transport: :ranch_tcp,
      client_id: :test_client,
      tracking: nil
    }

    try do
      assert :fallback =
               Sendfile.send_file_range_response(
                 ["blob-key", "0", Integer.to_string(Sendfile.threshold_bytes() - 1)],
                 path,
                 0,
                 Sendfile.threshold_bytes(),
                 state
               )
    after
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
    end
  end

  test "MGET reuses prefetched cold values below stream threshold instead of dispatching again",
       %{
         ctx: ctx
       } do
    key1 = "mget-small-cold:1"
    key2 = "mget-small-cold:2"
    value1 = :binary.copy("a", ctx.hot_cache_max_value_size + 256)
    value2 = :binary.copy("b", ctx.hot_cache_max_value_size + 512)

    :ok = Router.batch_put(ctx, [{key1, value1}, {key2, value2}])

    state = %{
      instance_ctx: ctx,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      tracking: nil
    }

    fallback = fn _cmd, _args, _state ->
      flunk("MGET fallback should not run after batch_get_with_file_refs returned values")
    end

    assert {:continue, encoded, ^state} = Sendfile.dispatch_mget([key1, key2], state, fallback)
    assert {:ok, [[^value1, ^value2]], ""} = Parser.parse(IO.iodata_to_binary(encoded))
  end

  test "MGET reuses one open file for blob refs in the same append segment" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1024,
        blob_side_channel_threshold_bytes: 128
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key1 = "mget-blob-sendfile-open:1"
    key2 = "mget-blob-sendfile-open:2"
    value1 = :binary.copy("a", Sendfile.threshold_bytes() + 1024)
    value2 = :binary.copy("b", Sendfile.threshold_bytes() + 2048)

    assert :ok = Router.batch_put(ctx, [{key1, value1}, {key2, value2}])

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      instance_ctx: ctx,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      tracking: nil
    }

    parent = self()

    Process.put(:ferricstore_sendfile_open_hook, fn path, modes ->
      send(parent, {:sendfile_open, path})
      :file.open(path, modes)
    end)

    fallback = fn _cmd, _args, _state ->
      flunk("MGET fallback should not run after batch_get_with_file_refs returned blob refs")
    end

    try do
      assert {:continue, "", ^state} = Sendfile.dispatch_mget([key1, key2], state, fallback)

      sends = collect_fake_tls_sends()
      assert {:ok, [[^value1, ^value2]], ""} = Parser.parse(IO.iodata_to_binary(sends))
      assert [path] = collect_sendfile_opens()
      assert Path.extname(path) == ".bloblog"
    after
      Process.delete(:ferricstore_sendfile_open_hook)
    end
  end

  test "MGET defers blob ref validation to the streaming layer" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1024,
        blob_side_channel_threshold_bytes: 128
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key1 = "mget-blob-defer-validation:1"
    key2 = "mget-blob-defer-validation:2"
    value1 = :binary.copy("v", Sendfile.threshold_bytes() + 1024)
    value2 = :binary.copy("w", Sendfile.threshold_bytes() + 2048)

    assert :ok = Router.batch_put(ctx, [{key1, value1}, {key2, value2}])

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      instance_ctx: ctx,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      tracking: nil
    }

    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_store_open, path})
      File.open(path, modes)
    end)

    Process.put(:ferricstore_sendfile_open_hook, fn path, modes ->
      send(parent, {:sendfile_open, path})
      :file.open(path, modes)
    end)

    fallback = fn _cmd, _args, _state ->
      flunk("MGET fallback should not run after streaming validation succeeds")
    end

    try do
      assert {:continue, "", ^state} = Sendfile.dispatch_mget([key1, key2], state, fallback)

      sends = collect_fake_tls_sends()
      assert {:ok, [[^value1, ^value2]], ""} = Parser.parse(IO.iodata_to_binary(sends))
      refute_received {:blob_store_open, _path}
      assert [path] = collect_sendfile_opens()
      assert Path.extname(path) == ".bloblog"
    after
      Process.delete(:ferricstore_blob_store_open_read_hook)
      Process.delete(:ferricstore_sendfile_open_hook)
    end
  end

  test "GET defers blob ref validation to the streaming layer" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1024,
        blob_side_channel_threshold_bytes: 128
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    key = "get-blob-defer-validation"
    value = :binary.copy("g", Sendfile.threshold_bytes() + 1024)

    assert :ok = Router.put(ctx, key, value, 0)

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      instance_ctx: ctx,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      tracking: nil
    }

    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_store_open, path})
      File.open(path, modes)
    end)

    Process.put(:ferricstore_sendfile_open_hook, fn path, modes ->
      send(parent, {:sendfile_open, path})
      :file.open(path, modes)
    end)

    fallback = fn _cmd, _args, _state ->
      flunk("GET fallback should not run after streaming validation succeeds")
    end

    try do
      assert {:continue, "", ^state} = Sendfile.dispatch_get([key], state, fallback)

      sends = collect_fake_tls_sends()
      assert {:ok, [^value], ""} = Parser.parse(IO.iodata_to_binary(sends))
      refute_received {:blob_store_open, _path}
      assert [path] = collect_sendfile_opens()
      assert Path.extname(path) == ".bloblog"
    after
      Process.delete(:ferricstore_blob_store_open_read_hook)
      Process.delete(:ferricstore_sendfile_open_hook)
    end
  end

  test "pipelined GET reuses one open file for blob refs in the same append segment" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1024,
        blob_side_channel_threshold_bytes: 128
      )

    {server_socket, client_socket} = tcp_pair()

    key1 = "pipeline-blob-sendfile-open:1"
    key2 = "pipeline-blob-sendfile-open:2"
    value1 = :binary.copy("c", Sendfile.threshold_bytes() + 1024)
    value2 = :binary.copy("d", Sendfile.threshold_bytes() + 2048)

    assert :ok = Router.batch_put(ctx, [{key1, value1}, {key2, value2}])

    state = %Connection{
      socket: server_socket,
      transport: :ranch_tcp,
      client_id: :test_client,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      tracking: nil
    }

    commands = [
      {:command, "GET", [key1], {:get, key1}, [key1]},
      {:command, "GET", [key2], {:get, key2}, [key2]}
    ]

    send_response = fn socket, :ranch_tcp, response -> :gen_tcp.send(socket, response) end
    handle_command = fn command, _state -> flunk("unexpected fallback: #{inspect(command)}") end
    parent = self()

    Process.put(:ferricstore_sendfile_open_hook, fn path, modes ->
      send(parent, {:sendfile_open, path})
      :file.open(path, modes)
    end)

    try do
      assert {:continue, _new_state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

      response = recv_until(client_socket, "$#{byte_size(value2)}\r\n", "", 100)
      assert response =~ "$#{byte_size(value1)}\r\n"
      assert response =~ "$#{byte_size(value2)}\r\n"
      assert [path] = collect_sendfile_opens()
      assert Path.extname(path) == ".bloblog"
    after
      Process.delete(:ferricstore_sendfile_open_hook)
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
      IsolatedInstance.checkin(ctx)
    end
  end

  test "pipelined repeated GET reuses blob ref validation for the same segment record" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1024,
        blob_side_channel_threshold_bytes: 128
      )

    {server_socket, client_socket} = tcp_pair()

    key = "pipeline-blob-sendfile-validation"
    value = :binary.copy("v", Sendfile.threshold_bytes() + 1024)

    assert :ok = Router.put(ctx, key, value, 0)

    state = %Connection{
      socket: server_socket,
      transport: :ranch_tcp,
      client_id: :test_client,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      tracking: nil
    }

    commands = [
      {:command, "GET", [key], {:get, key}, [key]},
      {:command, "GET", [key], {:get, key}, [key]}
    ]

    send_response = fn socket, :ranch_tcp, response -> :gen_tcp.send(socket, response) end
    handle_command = fn command, _state -> flunk("unexpected fallback: #{inspect(command)}") end
    parent = self()

    Process.put(:ferricstore_sendfile_pread_hook, fn fd, offset, size ->
      send(parent, {:sendfile_pread, offset, size})
      :file.pread(fd, offset, size)
    end)

    try do
      assert {:continue, _new_state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

      response = recv_until(client_socket, "$#{byte_size(value)}\r\n", "", 100)
      assert response =~ "$#{byte_size(value)}\r\n"

      assert [{_header_offset, @blob_segment_header_bytes}] = collect_sendfile_preads()
    after
      Process.delete(:ferricstore_sendfile_pread_hook)
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
      IsolatedInstance.checkin(ctx)
    end
  end

  test "pipelined GETRANGE reuses one open file for repeated blob ranges" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1024,
        blob_side_channel_threshold_bytes: 128
      )

    {server_socket, client_socket} = tcp_pair()

    key = "pipeline-blob-getrange-open"
    first_count = Sendfile.threshold_bytes()
    second_count = Sendfile.threshold_bytes() + 17
    second_start = first_count
    second_end = second_start + second_count - 1
    value = :binary.copy("r", second_end + 1)

    assert :ok = Router.put(ctx, key, value, 0)

    state = %Connection{
      socket: server_socket,
      transport: :ranch_tcp,
      client_id: :test_client,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      tracking: nil
    }

    first_args = [key, "0", Integer.to_string(first_count - 1)]
    second_args = [key, Integer.to_string(second_start), Integer.to_string(second_end)]

    commands = [
      {:command, "GETRANGE", first_args, {:getrange, key, 0, first_count - 1}, [key]},
      {:command, "GETRANGE", second_args, {:getrange, key, second_start, second_end}, [key]}
    ]

    send_response = fn socket, :ranch_tcp, response -> :gen_tcp.send(socket, response) end
    handle_command = fn command, _state -> flunk("unexpected fallback: #{inspect(command)}") end
    parent = self()

    Process.put(:ferricstore_sendfile_open_hook, fn path, modes ->
      send(parent, {:sendfile_open, path})
      :file.open(path, modes)
    end)

    try do
      assert {:continue, _new_state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

      response = recv_until(client_socket, "$#{second_count}\r\n", "", 100)
      assert response =~ "$#{first_count}\r\n"
      assert response =~ "$#{second_count}\r\n"
      assert [path] = collect_sendfile_opens()
      assert Path.extname(path) == ".bloblog"
    after
      Process.delete(:ferricstore_sendfile_open_hook)
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
      IsolatedInstance.checkin(ctx)
    end
  end

  test "GET tracks client-visible sandbox key, not internal lookup key", %{ctx: ctx} do
    sandbox = "sandbox:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    key = "tracked-hot-get"
    lookup_key = sandbox <> key

    :ok = Router.put(ctx, lookup_key, "v1", 0)
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    state = %{
      instance_ctx: ctx,
      sandbox_namespace: sandbox,
      pubsub_channels: nil,
      tracking: tracking
    }

    fallback = fn _cmd, _args, _state ->
      flunk("GET fallback should not run for hot sandbox value")
    end

    assert {:continue, encoded, new_state} = Sendfile.dispatch_get([key], state, fallback)
    assert IO.iodata_to_binary(encoded) == "$2\r\nv1\r\n"
    assert new_state.tracking.enabled
    assert :ets.lookup(:ferricstore_tracking, key) == [{key, self()}]
    assert :ets.lookup(:ferricstore_tracking, lookup_key) == []
  end

  test "sandboxed cold GET validates file ref with internal lookup key", %{ctx: ctx} do
    sandbox = "sandbox:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    key = "cold-sandbox-sendfile"
    lookup_key = sandbox <> key
    value = :binary.copy("c", Sendfile.threshold_bytes() + 512)

    assert :ok = Router.put(ctx, lookup_key, value, 0)

    state = %{
      socket: self(),
      transport: FakeTlsTransport,
      client_id: :test_client,
      instance_ctx: ctx,
      sandbox_namespace: sandbox,
      pubsub_channels: nil,
      tracking: nil
    }

    fallback = fn _cmd, _args, _state ->
      flunk("sandbox cold GET should stream with the internal lookup key")
    end

    assert {:continue, "", ^state} = Sendfile.dispatch_get([key], state, fallback)

    sends = collect_fake_tls_sends()
    assert List.first(sends) == "$#{byte_size(value)}\r\n"
    assert List.last(sends) == "\r\n"
    assert IO.iodata_to_binary(sends |> Enum.drop(1) |> Enum.drop(-1)) == value
  end

  test "sandboxed cold GET pipeline validates stream refs with internal lookup keys" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1024,
        blob_side_channel_threshold_bytes: 0
      )

    sandbox = "sandbox:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    cold_key = "pipeline-cold-sandbox-sendfile"
    hot_key = "pipeline-hot-sandbox-sendfile"
    cold_lookup_key = sandbox <> cold_key
    hot_lookup_key = sandbox <> hot_key
    cold_value = :binary.copy("p", Sendfile.threshold_bytes() + 512)

    :ok = Router.put(ctx, cold_lookup_key, cold_value, 0)
    :ok = Router.put(ctx, hot_lookup_key, "ok", 0)

    parent = self()
    telemetry_id = {:pipeline_sandbox_sendfile, self(), make_ref()}

    :telemetry.attach(
      telemetry_id,
      [:ferricstore, :server, :sendfile],
      fn event, measurements, metadata, _config ->
        send(parent, {:sendfile_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    {server_socket, client_socket} = tcp_pair()

    state = %Connection{
      socket: server_socket,
      transport: :ranch_tcp,
      client_id: :test_client,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      sandbox_namespace: sandbox,
      tracking: nil
    }

    commands = [
      {:command, "GET", [cold_key], {:get, cold_key}, [cold_key]},
      {:command, "GET", [hot_key], {:get, hot_key}, [hot_key]}
    ]

    send_response = fn socket, :ranch_tcp, response -> :gen_tcp.send(socket, response) end
    handle_command = fn command, _state -> flunk("unexpected fallback: #{inspect(command)}") end

    try do
      assert {:continue, _new_state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

      assert_receive {:sendfile_event, [:ferricstore, :server, :sendfile], %{bytes: byte_count},
                      %{result: :ok, client_id: :test_client}}

      assert byte_count == byte_size(cold_value)
      assert {:ok, _response} = :gen_tcp.recv(client_socket, 0, 1_000)
    after
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
      IsolatedInstance.checkin(ctx)
    end
  end

  test "sandboxed mixed GET SET pipeline validates stream refs with internal lookup keys" do
    ctx = FerricStore.Instance.get(:default)
    sandbox = "sandbox:" <> Integer.to_string(System.unique_integer([:positive])) <> ":"
    cold_key = "mixed-pipeline-cold-sandbox-sendfile"
    set_key = "mixed-pipeline-set-sandbox-sendfile"
    cold_lookup_key = sandbox <> cold_key
    set_lookup_key = sandbox <> set_key
    cold_value = :binary.copy("m", Sendfile.threshold_bytes() + 512)

    :ok = Router.put(ctx, cold_lookup_key, cold_value, 0)

    parent = self()
    telemetry_id = {:mixed_pipeline_sandbox_sendfile, self(), make_ref()}

    :telemetry.attach(
      telemetry_id,
      [:ferricstore, :server, :sendfile],
      fn event, measurements, metadata, _config ->
        send(parent, {:sendfile_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    {server_socket, client_socket} = tcp_pair()

    state = %Connection{
      socket: server_socket,
      transport: :ranch_tcp,
      client_id: :test_client,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      sandbox_namespace: sandbox,
      tracking: nil
    }

    commands = [
      {:command, "GET", [cold_key], {:get, cold_key}, [cold_key]},
      {:command, "SET", [set_key, "ok"], {:set, set_key, "ok"}, [set_key]}
    ]

    send_response = fn socket, :ranch_tcp, response -> :gen_tcp.send(socket, response) end
    handle_command = fn command, _state -> flunk("unexpected fallback: #{inspect(command)}") end

    try do
      assert {:continue, _new_state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

      assert_receive {:sendfile_event, [:ferricstore, :server, :sendfile], %{bytes: byte_count},
                      %{result: :ok, client_id: :test_client}}

      assert byte_count == byte_size(cold_value)
      response = recv_until(client_socket, "+OK\r\n")
      assert response =~ "$#{byte_size(cold_value)}\r\n"
      assert response =~ "+OK\r\n"
      assert Router.get(ctx, set_lookup_key) == "ok"
    after
      Router.delete(ctx, cold_lookup_key)
      Router.delete(ctx, set_lookup_key)
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
    end
  end

  test "sendfile GET miss is accounted once", %{ctx: ctx} do
    key = "missing-sendfile-get"

    state = %{
      instance_ctx: ctx,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      tracking: nil
    }

    fallback = fn "GET", [^key], fallback_state ->
      {:continue, FerricstoreServer.Resp.Encoder.encode(Router.get(ctx, key)), fallback_state}
    end

    before_misses = Stats.keyspace_misses(ctx)

    assert {:continue, encoded, ^state} = Sendfile.dispatch_get([key], state, fallback)
    assert IO.iodata_to_binary(encoded) == "_\r\n"
    assert Stats.keyspace_misses(ctx) - before_misses == 1
  end

  test "GET reuses a validated small cold file ref instead of dispatching again", %{ctx: ctx} do
    key = "small-cold-sendfile-get"
    value = :binary.copy("s", ctx.hot_cache_max_value_size + 256)

    :ok = Router.put(ctx, key, value, 0)

    state = %{
      instance_ctx: ctx,
      sandbox_namespace: nil,
      pubsub_channels: nil,
      tracking: nil
    }

    fallback = fn _cmd, _args, _state ->
      flunk("GET fallback should not run after get_with_file_ref returned a small cold ref")
    end

    before_cold_reads = Stats.total_cold_reads(ctx)

    assert {:continue, encoded, ^state} = Sendfile.dispatch_get([key], state, fallback)
    assert {:ok, [^value], ""} = Parser.parse(IO.iodata_to_binary(encoded))
    assert Stats.total_cold_reads(ctx) - before_cold_reads == 1
  end

  test "GET reads small blob file refs without falling back" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    key = "small-blob-sendfile-get"
    value = :binary.copy("g", 1024)
    parent = self()

    Process.put(:ferricstore_sendfile_pread_hook, fn fd, read_offset, read_size ->
      send(parent, {:sendfile_pread, read_offset, read_size})
      :file.pread(fd, read_offset, read_size)
    end)

    try do
      :ok = Router.put(ctx, key, value, 0)
      assert {:cold_ref, blob_path, blob_offset, 1024} = Router.get_with_file_ref(ctx, key)

      state = %{
        instance_ctx: ctx,
        sandbox_namespace: nil,
        pubsub_channels: nil,
        tracking: nil
      }

      fallback = fn _cmd, _args, _state ->
        flunk("GET should pread the small blob value directly")
      end

      assert {:continue, encoded, ^state} = Sendfile.dispatch_get([key], state, fallback)
      assert {:ok, [^value], ""} = Parser.parse(IO.iodata_to_binary(encoded))

      assert collect_sendfile_preads() == [
               {blob_offset - 48, 48},
               {blob_offset, byte_size(value)}
             ]

      assert Path.extname(blob_path) == ".bloblog"
    after
      Process.delete(:ferricstore_sendfile_pread_hook)
      IsolatedInstance.checkin(ctx)
    end
  end

  test "GET does not materialize a corrupt small blob file ref" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    key = "small-corrupt-blob-sendfile-get"
    value = :binary.copy("b", 1024)

    try do
      :ok = Router.put(ctx, key, value, 0)
      assert {:cold_ref, blob_path, blob_offset, 1024} = Router.get_with_file_ref(ctx, key)
      overwrite_file_range!(blob_path, blob_offset, :binary.copy("x", byte_size(value)))

      state = %{
        instance_ctx: ctx,
        sandbox_namespace: nil,
        pubsub_channels: nil,
        tracking: nil
      }

      fallback = fn "GET", [^key], fallback_state ->
        {:continue, FerricstoreServer.Resp.Encoder.encode(Router.get(ctx, key)), fallback_state}
      end

      assert {:continue, encoded, ^state} = Sendfile.dispatch_get([key], state, fallback)
      assert IO.iodata_to_binary(encoded) == "_\r\n"
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "GET does not stream a large blob file ref with a corrupt segment header" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    key = "large-corrupt-blob-header-sendfile-get"
    value = :binary.copy("B", Sendfile.threshold_bytes() + 512)

    try do
      :ok = Router.put(ctx, key, value, 0)
      assert {:cold_ref, blob_path, blob_offset, size} = Router.get_with_file_ref(ctx, key)
      assert size == byte_size(value)

      overwrite_file_range!(
        blob_path,
        blob_offset - @blob_segment_header_bytes,
        :binary.copy(<<0>>, @blob_segment_header_bytes)
      )

      state = %{
        socket: self(),
        transport: FakeTlsTransport,
        client_id: :test_client,
        instance_ctx: ctx,
        sandbox_namespace: nil,
        pubsub_channels: nil,
        tracking: nil
      }

      fallback = fn "GET", [^key], fallback_state ->
        {:continue, FerricstoreServer.Resp.Encoder.encode(Router.get(ctx, key)), fallback_state}
      end

      assert {:continue, encoded, ^state} = Sendfile.dispatch_get([key], state, fallback)
      assert IO.iodata_to_binary(encoded) == "_\r\n"
      assert collect_fake_tls_sends() == []
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "GETRANGE rejects a small blob file ref range whose segment header no longer matches" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    key = "small-corrupt-blob-sendfile-getrange"
    value = :binary.copy("r", 1024)
    args = [key, "0", "15"]

    try do
      :ok = Router.put(ctx, key, value, 0)
      assert {:cold_ref, blob_path, blob_offset, 1024} = Router.get_with_file_ref(ctx, key)
      overwrite_file_range!(blob_path, blob_offset - 48, :binary.copy("x", 48))

      state = %{
        instance_ctx: ctx,
        sandbox_namespace: nil,
        pubsub_channels: nil,
        tracking: nil
      }

      fallback = fn fallback_state ->
        value = Sendfile.materialize_getrange(key, 0, 15, fallback_state)
        {:continue, FerricstoreServer.Resp.Encoder.encode(value), fallback_state}
      end

      assert {:continue, encoded, ^state} =
               Sendfile.dispatch_getrange(args, key, 0, 15, state, fallback)

      assert IO.iodata_to_binary(encoded) == "$0\r\n\r\n"
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "GETRANGE reads small blob file ref ranges without falling back" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    key = "small-blob-sendfile-getrange"
    value = :binary.copy("a", 2048) <> "target-slice" <> :binary.copy("z", 2048)
    args = [key, "2048", "2053"]
    parent = self()

    Process.put(:ferricstore_sendfile_pread_hook, fn fd, read_offset, read_size ->
      send(parent, {:sendfile_pread, read_offset, read_size})
      :file.pread(fd, read_offset, read_size)
    end)

    try do
      :ok = Router.put(ctx, key, value, 0)
      size = byte_size(value)
      assert {:cold_ref, blob_path, blob_offset, ^size} = Router.get_with_file_ref(ctx, key)

      state = %{
        instance_ctx: ctx,
        sandbox_namespace: nil,
        pubsub_channels: nil,
        tracking: nil
      }

      fallback = fn _fallback_state ->
        flunk("GETRANGE should pread the blob range directly")
      end

      assert {:continue, encoded, ^state} =
               Sendfile.dispatch_getrange(args, key, 2048, 2053, state, fallback)

      assert IO.iodata_to_binary(encoded) == "$6\r\ntarget\r\n"

      assert collect_sendfile_preads() == [
               {blob_offset - 48, 48},
               {blob_offset + 2048, 6}
             ]

      assert Path.extname(blob_path) == ".bloblog"
    after
      Process.delete(:ferricstore_sendfile_pread_hook)
      IsolatedInstance.checkin(ctx)
    end
  end

  test "GETRANGE defers blob ref validation to the streaming layer" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    key = "large-blob-defer-validation-getrange"
    value = :binary.copy("R", Sendfile.threshold_bytes() + 512)
    end_idx = byte_size(value) - 1
    args = [key, "0", Integer.to_string(end_idx)]
    parent = self()

    Process.put(:ferricstore_blob_store_open_read_hook, fn path, modes ->
      send(parent, {:blob_store_open, path})
      File.open(path, modes)
    end)

    Process.put(:ferricstore_sendfile_open_hook, fn path, modes ->
      send(parent, {:sendfile_open, path})
      :file.open(path, modes)
    end)

    try do
      :ok = Router.put(ctx, key, value, 0)

      state = %{
        socket: self(),
        transport: FakeTlsTransport,
        client_id: :test_client,
        instance_ctx: ctx,
        sandbox_namespace: nil,
        pubsub_channels: nil,
        tracking: nil
      }

      fallback = fn _fallback_state ->
        flunk("GETRANGE fallback should not run after streaming validation succeeds")
      end

      assert {:continue, "", ^state} =
               Sendfile.dispatch_getrange(args, key, 0, end_idx, state, fallback)

      sends = collect_fake_tls_sends()
      assert {:ok, [^value], ""} = Parser.parse(IO.iodata_to_binary(sends))
      refute_received {:blob_store_open, _path}
      assert [path] = collect_sendfile_opens()
      assert Path.extname(path) == ".bloblog"
    after
      Process.delete(:ferricstore_blob_store_open_read_hook)
      Process.delete(:ferricstore_sendfile_open_hook)
      IsolatedInstance.checkin(ctx)
    end
  end

  test "GETRANGE does not stream a large blob file ref range with a corrupt segment header" do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    key = "large-corrupt-blob-header-sendfile-getrange"
    value = :binary.copy("R", Sendfile.threshold_bytes() + 512)
    args = [key, "0", Integer.to_string(Sendfile.threshold_bytes())]

    try do
      :ok = Router.put(ctx, key, value, 0)
      assert {:cold_ref, blob_path, blob_offset, size} = Router.get_with_file_ref(ctx, key)
      assert size == byte_size(value)

      overwrite_file_range!(
        blob_path,
        blob_offset - @blob_segment_header_bytes,
        :binary.copy(<<0>>, @blob_segment_header_bytes)
      )

      state = %{
        socket: self(),
        transport: FakeTlsTransport,
        client_id: :test_client,
        instance_ctx: ctx,
        sandbox_namespace: nil,
        pubsub_channels: nil,
        tracking: nil
      }

      fallback = fn fallback_state ->
        value = Sendfile.materialize_getrange(key, 0, Sendfile.threshold_bytes(), fallback_state)
        {:continue, FerricstoreServer.Resp.Encoder.encode(value), fallback_state}
      end

      assert {:continue, encoded, ^state} =
               Sendfile.dispatch_getrange(
                 args,
                 key,
                 0,
                 Sendfile.threshold_bytes(),
                 state,
                 fallback
               )

      assert IO.iodata_to_binary(encoded) == "$0\r\n\r\n"
      assert collect_fake_tls_sends() == []
    after
      IsolatedInstance.checkin(ctx)
    end
  end

  test "general pipeline keeps tracking from non-streaming reads when later cold reads stream",
       %{ctx: ctx} do
    hot_key = "pipeline-hot-tracked"
    cold_key = "pipeline-cold-streamed"
    cold_value = :binary.copy("c", 70_000)

    :ok = Router.put(ctx, hot_key, "hot", 0)
    :ok = Router.put(ctx, cold_key, cold_value, 0)

    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    {server_socket, client_socket} = tcp_pair()

    state = %Connection{
      socket: server_socket,
      transport: :ranch_tcp,
      instance_ctx: ctx,
      stats_counter: ctx.stats_counter,
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      tracking: tracking
    }

    commands = [
      {:command, "GET", [hot_key], {:get, hot_key}, [hot_key]},
      {:command, "GET", [cold_key], {:get, cold_key}, [cold_key]},
      {:command, "PING", [], :ping, []}
    ]

    send_response = fn socket, :ranch_tcp, response -> :gen_tcp.send(socket, response) end
    handle_command = fn command, _state -> flunk("unexpected fallback: #{inspect(command)}") end

    try do
      assert {:continue, _new_state} =
               Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

      assert :ets.lookup(:ferricstore_tracking, hot_key) == [{hot_key, self()}]
      assert :ets.lookup(:ferricstore_tracking, cold_key) == [{cold_key, self()}]
    after
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
    end
  end

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
