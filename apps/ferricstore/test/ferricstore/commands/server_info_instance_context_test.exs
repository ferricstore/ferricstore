defmodule Ferricstore.Commands.ServerInfoInstanceContextTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server

  test "INFO uses the injected instance for server, client, and sampling fields" do
    ctx = %FerricStore.Instance{
      name: :info_test,
      shard_count: 1,
      read_sample_rate: 7,
      connected_clients_fn: fn -> 23 end,
      server_info_fn: fn -> %{native_port: 19_876, protocol: "test-native"} end
    }

    store = %{__instance_ctx__: ctx}

    server = Server.handle("INFO", ["server"], store)
    assert server =~ "protocol:test-native"
    assert server =~ "native_port:19876"

    clients = Server.handle("INFO", ["clients"], store)
    assert clients =~ "connected_clients:23"

    stats = Server.handle("INFO", ["stats"], store)
    assert stats =~ "read_sample_rate:1:7"
  end

  test "INFO storage sections use the injected instance shard topology" do
    ctx = %FerricStore.Instance{
      name: :info_storage_test,
      data_dir: System.tmp_dir!(),
      shard_count: 1,
      read_sample_rate: 1
    }

    result = Server.handle("INFO", ["bitcask"], %{__instance_ctx__: ctx})

    assert result =~ "shard_0_data_file_count:"
    refute result =~ "shard_1_data_file_count:"
  end

  test "INFO keyspace excludes stale expired rows from expiry statistics" do
    keydir = :ets.new(:info_expiry_keydir, [:set, :public])
    now = Ferricstore.HLC.now_ms()

    true = :ets.insert(keydir, {"live-ttl", "v", now + 60_000, 0, 0, 0, 1})
    true = :ets.insert(keydir, {"stale-expired", "v", now - 1_000, 0, 0, 0, 1})

    ctx = %FerricStore.Instance{
      name: :info_expiry_test,
      shard_count: 1,
      keydir_refs: {keydir},
      read_sample_rate: 1
    }

    result =
      Server.handle("INFO", ["keyspace"], %{
        __instance_ctx__: ctx,
        dbsize: fn -> 1 end
      })

    assert result =~ "keys=1,expires=1,"
  end
end
