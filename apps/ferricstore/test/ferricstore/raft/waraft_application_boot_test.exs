defmodule Ferricstore.Raft.WARaftApplicationBootTest do
  use ExUnit.Case, async: false
  @moduletag :raft
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.Generic
  alias Ferricstore.Transaction.Coordinator
  alias Ferricstore.Test.ShardHelpers

  test "WARaft backend is fixed" do
    assert Ferricstore.Raft.Backend.selected() == :waraft
    assert Ferricstore.Raft.Backend.running_or_selected() == :waraft
    assert Ferricstore.Raft.Backend.waraft?()
  end

  test "WARaft default log module is durable" do
    refute Ferricstore.Raft.WARaftBackend.default_log_module() == :wa_raft_log_ets

    assert Ferricstore.Raft.WARaftBackend.default_log_module() ==
             :ferricstore_waraft_spike_segment_log
  end

  test "application prep_stop stops WARaft before shard ETS tables can terminate" do
    source =
      Path.expand("../../../lib/ferricstore/application.ex", __DIR__)
      |> File.read!()

    assert [_, prep_stop_source] = String.split(source, "def prep_stop(state) do", parts: 2)
    assert [prep_stop_source, _] = String.split(prep_stop_source, "\n  @impl true\n  def stop", parts: 2)

    waraft_stop_at =
      source_offset(prep_stop_source, "shutdown_stop_waraft_backend()")

    shard_flush_at = source_offset(prep_stop_source, "shutdown_flush_shards(shard_count)")

    assert is_integer(waraft_stop_at),
           "WARaft must be stopped in prep_stop/1 while shard keydir ETS tables still exist"

    assert waraft_stop_at < shard_flush_at,
           "WARaft stop must happen before shard shutdown/flush teardown can remove keydir tables"
  end

  test "application starts WARaft backend with WARaft commit batchers" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(System.tmp_dir!(), "ferricstore-waraft-app-#{System.unique_integer([:positive])}")

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 1)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      assert is_pid(Process.whereis(Ferricstore.Raft.Batcher.batcher_name(0)))

      acceptor =
        :wa_raft_acceptor.registered_name(:ferricstore_waraft_backend, 1)

      assert is_pid(Process.whereis(acceptor))

      ctx = FerricStore.Instance.get(:default)
      assert :ok = Router.put(ctx, "app:wk", "wv", 0)
      assert "wv" == Router.get(ctx, "app:wk")

      before_direct = storage_index!(0)
      assert :ok = GenServer.call(elem(ctx.shard_names, 0), {:put, "app:direct", "dv", 0}, 10_000)
      assert "dv" == Router.get(ctx, "app:direct")
      assert storage_index!(0) > before_direct

      before_rmw = storage_index!(0)

      assert {:ok, 1} =
               GenServer.call(elem(ctx.shard_names, 0), {:incr, "app:counter", 1}, 10_000)

      assert "1" == Router.get(ctx, "app:counter")
      assert storage_index!(0) > before_rmw

      hash_key = "app:hash"
      field_key = Ferricstore.Store.CompoundKey.hash_field(hash_key, "field")
      before_compound = storage_index!(0)

      assert :ok =
               GenServer.call(
                 elem(ctx.shard_names, 0),
                 {:compound_put, hash_key, field_key, "hv", 0},
                 10_000
               )

      assert "hv" == Router.compound_get(ctx, hash_key, field_key)
      assert storage_index!(0) > before_compound

      before_list = storage_index!(0)

      assert 2 =
               GenServer.call(
                 elem(ctx.shard_names, 0),
                 {:list_op, "app:list", {:rpush, ["a", "b"]}},
                 10_000
               )

      assert ["a", "b"] == Router.list_op(ctx, "app:list", {:lrange, 0, -1})
      assert storage_index!(0) > before_list

      assert :ok = Router.put(ctx, "app:prefix:one", "1", 0)
      assert :ok = Router.put(ctx, "app:prefix:two", "2", 0)
      before_delete_prefix = storage_index!(0)

      assert :ok =
               GenServer.call(elem(ctx.shard_names, 0), {:delete_prefix, "app:prefix:"}, 10_000)

      assert nil == Router.get(ctx, "app:prefix:one")
      assert nil == Router.get(ctx, "app:prefix:two")
      assert storage_index!(0) > before_delete_prefix

      before_standalone_commit = storage_index!(0)

      assert :ok =
               GenServer.call(
                 elem(ctx.shard_names, 0),
                 {:standalone_commit, {:put, "app:standalone-bypass", "sv", 0}},
                 10_000
               )

      assert "sv" == Router.get(ctx, "app:standalone-bypass")
      assert storage_index!(0) > before_standalone_commit

      assert Ferricstore.Health.check().status == :ok

      info = Ferricstore.Commands.Server.handle("INFO", ["raft"], %{})
      assert info =~ "shard_0_role:leader"
      assert info =~ "shard_0_leader_node:#{node()}"
    after
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "strict default cleanup works when the running backend is WARaft" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-cleanup-#{System.unique_integer([:positive])}"
      )

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 1)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      ctx = FerricStore.Instance.get(:default)
      assert :ok = Router.put(ctx, "cleanup:key", "value", 0)
      assert "value" == Router.get(ctx, "cleanup:key")

      assert :ok = ShardHelpers.flush_all_keys()
      assert nil == Router.get(ctx, "cleanup:key")
    after
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "WARaft bounded membership probes do not wait on slow status calls" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-members-timeout-#{System.unique_integer([:positive])}"
      )

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 1)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      Process.put(:ferricstore_waraft_backend_status_hook, fn _shard_index ->
        Process.sleep(200)
      end)

      {elapsed_us, result} =
        :timer.tc(fn -> Ferricstore.Raft.Cluster.members(0, 10) end)

      assert {:ok, members, {_server, node_name}} = result
      assert {_, ^node_name} = List.first(members)
      assert elapsed_us < 100_000
    after
      Process.delete(:ferricstore_waraft_backend_status_hook)
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "INFO raft under WARaft does not wait on slow status calls" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-info-timeout-#{System.unique_integer([:positive])}"
      )

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 1)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      Process.put(:ferricstore_waraft_backend_status_hook, fn _shard_index ->
        Process.sleep(200)
      end)

      {elapsed_us, info} =
        :timer.tc(fn -> Ferricstore.Commands.Server.handle("INFO", ["raft"], %{}) end)

      assert info =~ "shard_0_role:leader"
      assert info =~ "shard_0_waraft_inflight_commit_bytes:0"
      assert elapsed_us < 100_000

      {health_elapsed_us, health} =
        :timer.tc(fn -> Ferricstore.Commands.Cluster.handle("CLUSTER.HEALTH", [], %{}) end)

      assert health =~ "shard_0:"
      assert health =~ "role: leader"
      assert health_elapsed_us < 100_000
    after
      Process.delete(:ferricstore_waraft_backend_status_hook)
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "WARaft application executes MULTI EXEC transactions through selected backend" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(System.tmp_dir!(), "ferricstore-waraft-tx-#{System.unique_integer([:positive])}")

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 2)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      ctx = FerricStore.Instance.get(:default)
      key_0 = key_for_default_shard(ctx, 0, "waraft:tx:a")
      key_1 = key_for_default_shard(ctx, 1, "waraft:tx:b")

      assert [:ok, :ok, "v0"] =
               Coordinator.execute(
                 [
                   {"SET", [key_0, "v0"]},
                   {"SET", [key_1, "v1"]},
                   {"GET", [key_0]}
                 ],
                 %{},
                 nil
               )

      assert "v0" == Router.get(ctx, key_0)
      assert "v1" == Router.get(ctx, key_1)
    after
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "WARaft application aborts watched transactions after concurrent writes" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-watch-#{System.unique_integer([:positive])}"
      )

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 1)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      ctx = FerricStore.Instance.get(:default)
      key = "waraft:watch:key"

      assert :ok = Router.put(ctx, key, "v0", 0)
      watched = %{key => Router.watch_token(ctx, key)}

      assert :ok = Router.put(ctx, key, "v1", 0)
      assert nil == Coordinator.execute([{"SET", [key, "bad"]}], watched, nil)
      assert "v1" == Router.get(ctx, key)

      clean_watch = %{key => Router.watch_token(ctx, key)}
      assert [:ok] == Coordinator.execute([{"SET", [key, "v2"]}], clean_watch, nil)
      assert "v2" == Router.get(ctx, key)
    after
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "WARaft application persists generic cross-shard copy and rename commands" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-generic-#{System.unique_integer([:positive])}"
      )

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 2)
      Application.delete_env(:ferricstore, :waraft_log_module)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      ctx = FerricStore.Instance.get(:default)
      source = key_for_default_shard(ctx, 0, "waraft:generic:source")
      copy_dest = key_for_default_shard(ctx, 1, "waraft:generic:copy")
      renamed = key_for_default_shard(ctx, 0, "waraft:generic:renamed")

      assert :ok = Router.put(ctx, source, "generic-value", 0)
      assert 1 = Generic.handle_ast({:copy, source, copy_dest, true}, ctx)
      assert :ok = Generic.handle_ast({:rename, copy_dest, renamed}, ctx)
      assert 0 = Generic.handle_ast({:renamenx, source, renamed}, ctx)

      assert "generic-value" == Router.get(ctx, source)
      assert nil == Router.get(ctx, copy_dest)
      assert "generic-value" == Router.get(ctx, renamed)

      stop_ferricstore()
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      restarted_ctx = FerricStore.Instance.get(:default)
      assert "generic-value" == Router.get(restarted_ctx, source)
      assert nil == Router.get(restarted_ctx, copy_dest)
      assert "generic-value" == Router.get(restarted_ctx, renamed)
    after
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "WARaft application persists generic cross-shard compound copy and rename commands" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-generic-compound-#{System.unique_integer([:positive])}"
      )

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 2)
      Application.delete_env(:ferricstore, :waraft_log_module)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      ctx = FerricStore.Instance.get(:default)
      source = key_for_default_shard(ctx, 0, "waraft:generic:hash:source")
      copy_dest = key_for_default_shard(ctx, 1, "waraft:generic:hash:copy")
      renamed = key_for_default_shard(ctx, 0, "waraft:generic:hash:renamed")

      assert 2 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hset, [source, "field-a", "value-a", "field-b", "value-b"]},
                 ctx
               )

      assert 1 = Generic.handle_ast({:copy, source, copy_dest, true}, ctx)
      assert :ok = Generic.handle_ast({:rename, copy_dest, renamed}, ctx)
      assert 0 = Generic.handle_ast({:renamenx, source, renamed}, ctx)

      assert "value-a" == Ferricstore.Commands.Hash.handle_ast({:hget, source, "field-a"}, ctx)
      assert "value-b" == Ferricstore.Commands.Hash.handle_ast({:hget, renamed, "field-b"}, ctx)
      assert {:simple, "none"} == Generic.handle_ast({:type, copy_dest}, ctx)

      stop_ferricstore()
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)

      restarted_ctx = FerricStore.Instance.get(:default)

      assert "value-a" ==
               Ferricstore.Commands.Hash.handle_ast({:hget, source, "field-a"}, restarted_ctx)

      assert "value-b" ==
               Ferricstore.Commands.Hash.handle_ast({:hget, renamed, "field-b"}, restarted_ctx)

      assert {:simple, "none"} == Generic.handle_ast({:type, copy_dest}, restarted_ctx)
    after
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  test "WARaft application handles real RESP SET GET through TCP and durable restart" do
    previous_data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    previous_shard_count = Application.get_env(:ferricstore, :shard_count)
    previous_port = Application.get_env(:ferricstore, :port)
    previous_health_port = Application.get_env(:ferricstore, :health_port)
    previous_waraft_log_module = Application.get_env(:ferricstore, :waraft_log_module)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-resp-app-#{System.unique_integer([:positive])}"
      )

    try do
      stop_ferricstore()
      File.rm_rf!(tmp_dir)

      Application.put_env(:ferricstore, :data_dir, tmp_dir)
      Application.put_env(:ferricstore, :shard_count, 2)
      Application.put_env(:ferricstore, :port, 0)
      Application.put_env(:ferricstore, :health_port, 0)
      Application.delete_env(:ferricstore, :waraft_log_module)

      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
      port = FerricstoreServer.Listener.port()

      assert "+OK\r\n$9\r\nrespvalue\r\n" ==
               tcp_roundtrip(port, [
                 ["SET", "waraft:resp:key", "respvalue"],
                 ["GET", "waraft:resp:key"]
               ])

      stop_ferricstore()
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
      restarted_port = FerricstoreServer.Listener.port()

      assert "$9\r\nrespvalue\r\n" ==
               tcp_roundtrip(restarted_port, [["GET", "waraft:resp:key"]])
    after
      stop_ferricstore()
      restore_env(:data_dir, previous_data_dir)
      restore_env(:shard_count, previous_shard_count)
      restore_env(:port, previous_port)
      restore_env(:health_port, previous_health_port)
      restore_env(:waraft_log_module, previous_waraft_log_module)
      File.rm_rf!(tmp_dir)
      assert {:ok, _apps} = Application.ensure_all_started(:ferricstore)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp storage_index!(shard_index) do
    assert {:ok, {:raft_log_pos, index, _term}} =
             Ferricstore.Raft.WARaftBackend.storage_position(shard_index)

    index
  end

  defp key_for_default_shard(ctx, shard_index, prefix) do
    Stream.iterate(0, &(&1 + 1))
    |> Enum.find_value(fn n ->
      key = "#{prefix}:#{n}"
      if Router.shard_for(ctx, key) == shard_index, do: key
    end)
  end

  defp tcp_roundtrip(port, commands) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw], 5_000)

    try do
      :ok = :gen_tcp.send(socket, encode_resp_commands(commands))
      recv_available(socket, <<>>)
    after
      :gen_tcp.close(socket)
    end
  end

  defp encode_resp_commands(commands) do
    commands
    |> Enum.map(&encode_resp_command/1)
    |> IO.iodata_to_binary()
  end

  defp encode_resp_command(args) do
    ["*", Integer.to_string(length(args)), "\r\n", Enum.map(args, &encode_resp_bulk/1)]
  end

  defp encode_resp_bulk(value) when is_binary(value) do
    ["$", Integer.to_string(byte_size(value)), "\r\n", value, "\r\n"]
  end

  defp recv_available(socket, acc) do
    case :gen_tcp.recv(socket, 0, 100) do
      {:ok, chunk} -> recv_available(socket, <<acc::binary, chunk::binary>>)
      {:error, :timeout} -> acc
      {:error, :closed} -> acc
    end
  end

  defp source_offset(source, needle) do
    case :binary.match(source, needle) do
      {offset, _length} -> offset
      :nomatch -> nil
    end
  end

  defp stop_ferricstore do
    _ = Application.stop(:ferricstore_server)
    _ = Application.stop(:ferricstore)
    _ = Ferricstore.Raft.WARaftBackend.stop()

    :ok
  end
end
