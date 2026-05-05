defmodule Ferricstore.Commands.StringsColdRangeTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Strings
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "GETRANGE uses store range reads instead of materializing the full value" do
    test_pid = self()

    store = %{
      value_size: fn "range-key" -> 8192 end,
      getrange: fn "range-key", 4096, 4101 ->
        send(test_pid, :range_reader_called)
        "target"
      end,
      get: fn _key ->
        flunk("GETRANGE should not call get/1 when a range reader is available")
      end,
      exists?: fn _key -> true end,
      put: fn _key, _value, _exp -> :ok end,
      delete: fn _key -> :ok end
    }

    assert "target" == Strings.handle("GETRANGE", ["range-key", "4096", "4101"], store)
    assert_received :range_reader_called
  end

  test "GETRANGE returns the requested slice for cold large values", %{ctx: ctx} do
    key = "getrange-cold-slice:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    value = :binary.copy("a", 4096) <> "target-slice" <> :binary.copy("z", 4096)

    :ok = Router.batch_put(ctx, [{key, value}])

    ShardHelpers.eventually(fn ->
      match?({:cold_ref, _path, _offset, _size}, Router.get_with_file_ref(ctx, key))
    end)

    assert "target" == Strings.handle("GETRANGE", [key, "4096", "4101"], ctx)
  end
end
