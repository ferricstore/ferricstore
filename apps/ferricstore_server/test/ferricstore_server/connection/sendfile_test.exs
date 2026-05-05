defmodule FerricstoreServer.Connection.SendfileTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance
  alias FerricstoreServer.Connection.Sendfile
  alias FerricstoreServer.Resp.Parser

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "MGET reuses prefetched cold values below stream threshold instead of dispatching again",
       %{
         ctx: ctx
       } do
    key1 = "mget-small-cold:1"
    key2 = "mget-small-cold:2"
    value1 = :binary.copy("a", ctx.hot_cache_max_value_size + 256)
    value2 = :binary.copy("b", ctx.hot_cache_max_value_size + 512)

    :ok = Router.batch_async_put(ctx, [{key1, value1}, {key2, value2}])

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
end
