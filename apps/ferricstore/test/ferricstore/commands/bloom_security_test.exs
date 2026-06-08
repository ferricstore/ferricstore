defmodule Ferricstore.Commands.BloomSecurityTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Bloom

  defp make_store do
    dir =
      Path.join(
        System.tmp_dir!(),
        "bloom_security_test_#{System.os_time(:nanosecond)}_#{System.unique_integer([:positive, :monotonic])}"
      )

    File.mkdir_p!(dir)
    {:ok, reg_pid} = Agent.start_link(fn -> %{} end)

    %{
      bloom_registry: %{dir: dir},
      get: fn key -> Agent.get(reg_pid, &Map.get(&1, key)) end,
      put: fn key, value, _ttl -> Agent.update(reg_pid, &Map.put(&1, key, value)) end,
      delete: fn key -> Agent.update(reg_pid, &Map.delete(&1, key)) end
    }
  end

  defp with_env(key, value, fun) do
    previous = Application.get_env(:ferricstore, key, :__missing__)
    Application.put_env(:ferricstore, key, value)

    try do
      fun.()
    after
      case previous do
        :__missing__ -> Application.delete_env(:ferricstore, key)
        other -> Application.put_env(:ferricstore, key, other)
      end
    end
  end

  test "BF.RESERVE rejects capacity above configured maximum before creating a file" do
    with_env(:bloom_max_capacity, 10, fn ->
      store = make_store()

      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "11"], store)
      assert msg =~ "capacity"
      assert File.ls!(store.bloom_registry.dir) == []
    end)
  end

  test "BF.RESERVE rejects computed bit arrays above configured maximum" do
    with_env(:bloom_max_num_bits, 100, fn ->
      store = make_store()

      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "1000"], store)
      assert msg =~ "bits"
      assert File.ls!(store.bloom_registry.dir) == []
    end)
  end

  test "BF.MADD rejects oversized batches before opening a Bloom file" do
    with_env(:bloom_max_batch_items, 2, fn ->
      store = make_store()

      assert {:error, msg} = Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      assert msg =~ "batch"
      assert File.ls!(store.bloom_registry.dir) == []
    end)
  end

  test "BF.MEXISTS rejects oversized read batches before opening a Bloom file" do
    with_env(:bloom_max_batch_items, 2, fn ->
      store = make_store()

      assert {:error, msg} = Bloom.handle("BF.MEXISTS", ["bf", "a", "b", "c"], store)
      assert msg =~ "batch"
      assert File.ls!(store.bloom_registry.dir) == []
    end)
  end
end
