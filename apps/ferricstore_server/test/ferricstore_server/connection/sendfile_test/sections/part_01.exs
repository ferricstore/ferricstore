defmodule FerricstoreServer.Connection.SendfileTest.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Connection.SendfileTest.FakeTlsTransport

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

  test "tcp segment blob sendfile validates payload before sending header" do
    root = tmp_blob_root!()
    chunk_bytes = Sendfile.file_stream_chunk_bytes()
    value = :binary.copy("z", chunk_bytes + 4096)
    assert {:ok, ref} = BlobStore.put(root, 0, value)
    assert {:ok, {path, offset, size}} = BlobStore.file_ref(root, 0, ref)
    assert size == byte_size(value)

    parent = self()
    telemetry_id = {:sendfile_blob_checksum, self(), make_ref()}

    :telemetry.attach(
      telemetry_id,
      [:ferricstore, :server, :sendfile, :blob_checksum],
      fn event, measurements, metadata, _config ->
        send(parent, {:blob_checksum_event, event, measurements, metadata})
      end,
      nil
    )

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

      assert collect_sendfile_preads() == [
               {offset - @blob_segment_header_bytes, @blob_segment_header_bytes},
               {offset, chunk_bytes},
               {offset + chunk_bytes, 4096}
             ]

      assert_receive {:blob_checksum_event, [:ferricstore, :server, :sendfile, :blob_checksum],
                      %{bytes: ^size, duration_us: duration_us}, %{mode: :sendfile, result: :ok}}

      assert is_integer(duration_us)
      assert duration_us >= 0
    after
      :telemetry.detach(telemetry_id)
      Process.delete(:ferricstore_sendfile_pread_hook)
      :gen_tcp.close(server_socket)
      :gen_tcp.close(client_socket)
    end
  end

  test "encrypted segment blob stream validates payload before streaming bounded chunks" do
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
               {offset + chunk_bytes, 19},
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

  test "MGET validates blob refs before streaming" do
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
      assert [blob_path] = collect_blob_store_opens()
      assert Path.extname(blob_path) == ".bloblog"
      assert [path] = collect_sendfile_opens()
      assert Path.extname(path) == ".bloblog"
    after
      Process.delete(:ferricstore_blob_store_open_read_hook)
      Process.delete(:ferricstore_sendfile_open_hook)
    end
  end

  test "GET validates blob ref before streaming" do
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
      assert [blob_path] = collect_blob_store_opens()
      assert Path.extname(blob_path) == ".bloblog"
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
    chunk_bytes = Sendfile.file_stream_chunk_bytes()
    value = :binary.copy("v", chunk_bytes + 1024)

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

      assert collect_sendfile_preads() == [
               {48 - @blob_segment_header_bytes, @blob_segment_header_bytes},
               {48, chunk_bytes},
               {48 + chunk_bytes, 1024}
             ]
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
    end
  end
end
