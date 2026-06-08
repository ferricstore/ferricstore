defmodule FerricstoreServer.Connection.SendfileTest.Sections.SandboxedPipelineStreamRefs do
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

      test "sandboxed mixed GET SET pipeline validates stream refs with internal lookup keys" do
        ctx =
          IsolatedInstance.checkout(
            shard_count: 1,
            hot_cache_max_value_size: 1024,
            blob_side_channel_threshold_bytes: 0
          )

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

        handle_command = fn command, _state ->
          flunk("unexpected fallback: #{inspect(command)}")
        end

        try do
          assert {:continue, _new_state} =
                   Pipeline.pipeline_dispatch(commands, state, handle_command, send_response)

          assert_receive {:sendfile_event, [:ferricstore, :server, :sendfile],
                          %{bytes: byte_count}, %{result: :ok, client_id: :test_client}}

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
          IsolatedInstance.checkin(ctx)
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
            {:continue, FerricstoreServer.Resp.Encoder.encode(Router.get(ctx, key)),
             fallback_state}
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
            {:continue, FerricstoreServer.Resp.Encoder.encode(Router.get(ctx, key)),
             fallback_state}
          end

          assert {:continue, encoded, ^state} = Sendfile.dispatch_get([key], state, fallback)
          assert IO.iodata_to_binary(encoded) == "_\r\n"
          assert collect_fake_tls_sends() == []
        after
          IsolatedInstance.checkin(ctx)
        end
      end

      test "GET does not stream a large blob file ref with corrupt payload bytes" do
        ctx =
          IsolatedInstance.checkout(
            shard_count: 1,
            hot_cache_max_value_size: 64,
            blob_side_channel_threshold_bytes: 128
          )

        key = "large-corrupt-blob-payload-sendfile-get"
        value = :binary.copy("B", Sendfile.threshold_bytes() + 512)

        try do
          :ok = Router.put(ctx, key, value, 0)
          assert {:cold_ref, blob_path, blob_offset, size} = Router.get_with_file_ref(ctx, key)
          assert size == byte_size(value)

          overwrite_file_range!(blob_path, blob_offset, :binary.copy("x", byte_size(value)))

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
            {:continue, FerricstoreServer.Resp.Encoder.encode(Router.get(ctx, key)),
             fallback_state}
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
                   {blob_offset - @blob_segment_header_bytes, @blob_segment_header_bytes},
                   {blob_offset, size},
                   {blob_offset + 2048, 6}
                 ]

          assert Path.extname(blob_path) == ".bloblog"
        after
          Process.delete(:ferricstore_sendfile_pread_hook)
          IsolatedInstance.checkin(ctx)
        end
      end

      test "TCP GET does not sendfile a large blob file ref with corrupt payload bytes" do
        ctx =
          IsolatedInstance.checkout(
            shard_count: 1,
            hot_cache_max_value_size: 64,
            blob_side_channel_threshold_bytes: 128
          )

        key = "tcp-large-corrupt-blob-payload-sendfile-get"
        value = :binary.copy("T", Sendfile.threshold_bytes() + 512)

        try do
          :ok = Router.put(ctx, key, value, 0)
          assert {:cold_ref, blob_path, blob_offset, size} = Router.get_with_file_ref(ctx, key)
          assert size == byte_size(value)

          overwrite_file_range!(blob_path, blob_offset, :binary.copy("x", byte_size(value)))

          {server_socket, client_socket} = tcp_pair()

          state = %{
            socket: server_socket,
            transport: :ranch_tcp,
            client_id: :test_client,
            instance_ctx: ctx,
            sandbox_namespace: nil,
            pubsub_channels: nil,
            tracking: nil
          }

          fallback = fn "GET", [^key], fallback_state ->
            {:continue, FerricstoreServer.Resp.Encoder.encode(Router.get(ctx, key)),
             fallback_state}
          end

          try do
            assert {:continue, encoded, ^state} = Sendfile.dispatch_get([key], state, fallback)
            assert IO.iodata_to_binary(encoded) == "_\r\n"
            assert {:error, :timeout} = :gen_tcp.recv(client_socket, 0, 10)
          after
            :gen_tcp.close(server_socket)
            :gen_tcp.close(client_socket)
          end
        after
          IsolatedInstance.checkin(ctx)
        end
      end

      test "GETRANGE does not stream a large blob file ref range when full payload checksum fails" do
        ctx =
          IsolatedInstance.checkout(
            shard_count: 1,
            hot_cache_max_value_size: 64,
            blob_side_channel_threshold_bytes: 128
          )

        key = "large-corrupt-blob-payload-sendfile-getrange"
        value = :binary.copy("R", Sendfile.threshold_bytes() + 512)
        args = [key, "0", Integer.to_string(Sendfile.threshold_bytes())]

        try do
          :ok = Router.put(ctx, key, value, 0)
          assert {:cold_ref, blob_path, blob_offset, size} = Router.get_with_file_ref(ctx, key)
          assert size == byte_size(value)

          overwrite_file_range!(
            blob_path,
            blob_offset + Sendfile.threshold_bytes() + 128,
            :binary.copy("x", 32)
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
            value =
              Sendfile.materialize_getrange(key, 0, Sendfile.threshold_bytes(), fallback_state)

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

      test "GETRANGE validates blob ref before streaming" do
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
          assert [blob_path] = collect_blob_store_opens()
          assert Path.extname(blob_path) == ".bloblog"
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
            value =
              Sendfile.materialize_getrange(key, 0, Sendfile.threshold_bytes(), fallback_state)

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

        handle_command = fn command, _state ->
          flunk("unexpected fallback: #{inspect(command)}")
        end

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
    end
  end
end
