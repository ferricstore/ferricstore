defmodule Ferricstore.Store.ListOpsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.ListOps
  alias Ferricstore.Store.Shard.NativeOps

  test "RPUSH returns write error when element append fails" do
    store = failing_put_store()

    assert {:error, "disk full"} = ListOps.execute("list", store, {:rpush, ["a"]})
  end

  test "shard list stores expose batch mutation callbacks" do
    raft_store = NativeOps.build_list_compound_store_raft("list", %{index: 0})
    direct_store = NativeOps.build_list_compound_store_direct("list", %{})

    # List pushes/pops can touch many elements. These callbacks keep the
    # real shard paths to one Ra batch / append batch instead of falling
    # back to per-element compound writes through Ops.
    assert is_function(raft_store.compound_batch_put, 2)
    assert is_function(raft_store.compound_batch_delete, 2)
    assert is_function(direct_store.compound_batch_put, 2)
    assert is_function(direct_store.compound_batch_delete, 2)
  end

  test "LPOP returns write error when element tombstone fails" do
    store = failing_delete_store("list", ["a"])

    assert {:error, "disk full"} = ListOps.execute("list", store, {:lpop, 1})
  end

  test "LPUSH batches element writes" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key -> nil end,
      compound_batch_put: fn "list", entries ->
        send(parent, {:compound_batch_put, entries})
        :ok
      end,
      compound_put: fn
        "list", ^meta_key, meta, 0 when is_binary(meta) ->
          :ok

        "list", compound_key, _value, 0 ->
          flunk("LPUSH should batch element writes, got #{inspect(compound_key)}")
      end,
      compound_delete: fn _redis_key, _compound_key -> :ok end,
      compound_scan: fn _redis_key, _prefix -> [] end
    }

    assert 3 == ListOps.execute("list", store, {:lpush, ["a", "b", "c"]})
    assert_received {:compound_batch_put, entries}
    assert length(entries) == 3
    assert Enum.all?(entries, fn {_compound_key, value, 0} -> value in ["a", "b", "c"] end)
    refute_received {:compound_batch_put, _}
  end

  test "LPOP count batches element deletes" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    meta = {3, -1_000_000_000, 3_000_000_000}

    elements =
      ["a", "b", "c"]
      |> Enum.with_index()
      |> Enum.map(fn {value, idx} ->
        {CompoundKey.encode_position(idx * 1_000_000_000), value}
      end)

    store = %{
      compound_get: fn "list", ^meta_key -> :erlang.term_to_binary(meta) end,
      compound_batch_delete: fn "list", compound_keys ->
        send(parent, {:compound_batch_delete, compound_keys})
        :ok
      end,
      compound_delete: fn "list", compound_key ->
        flunk("LPOP should batch element deletes, got #{inspect(compound_key)}")
      end,
      compound_put: fn "list", ^meta_key, meta_binary, 0 when is_binary(meta_binary) -> :ok end,
      compound_scan: fn "list", _prefix -> elements end
    }

    assert ["a", "b"] == ListOps.execute("list", store, {:lpop, 2})
    assert_received {:compound_batch_delete, deleted_keys}
    assert length(deleted_keys) == 2
    refute_received {:compound_batch_delete, _}
  end

  test "LMOVE returns error when source delete fails before touching destination" do
    meta_key = CompoundKey.list_meta_key("src")
    pos = 0
    element_key = CompoundKey.list_element("src", pos)

    store = %{
      compound_get: fn
        "src", ^meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        _redis_key, _compound_key -> nil
      end,
      compound_batch_delete: fn "src", [^element_key] -> {:error, "disk full"} end,
      compound_delete: fn _redis_key, _compound_key -> {:error, "disk full"} end,
      compound_put: fn redis_key, compound_key, _value, _expire_at_ms ->
        flunk(
          "LMOVE must not write #{inspect(redis_key)} #{inspect(compound_key)} after delete failure"
        )
      end,
      compound_scan: fn "src", _prefix ->
        [{CompoundKey.encode_position(pos), "a"}]
      end
    }

    assert {:error, "disk full"} == ListOps.execute_lmove("src", "dst", store, :left, :right)
  end

  test "LMOVE returns error when destination element write fails" do
    src_meta_key = CompoundKey.list_meta_key("src")

    store = %{
      compound_get: fn
        "src", ^src_meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        _redis_key, _compound_key -> nil
      end,
      compound_batch_delete: fn "src", [_element_key] -> :ok end,
      compound_delete: fn "src", ^src_meta_key -> :ok end,
      compound_put: fn
        "dst", <<"L:dst", _rest::binary>>, "a", 0 ->
          {:error, "disk full"}

        "dst", <<"LM:dst">>, _meta, 0 ->
          flunk("LMOVE must not write destination metadata after element write failure")
      end,
      compound_scan: fn "src", _prefix ->
        [{CompoundKey.encode_position(0), "a"}]
      end
    }

    assert {:error, "disk full"} == ListOps.execute_lmove("src", "dst", store, :left, :right)
  end

  test "LSET returns element write errors" do
    meta_key = CompoundKey.list_meta_key("list")
    pos = 0

    store = %{
      compound_get: fn "list", ^meta_key ->
        :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
      end,
      compound_put: fn "list", <<"L:list", _rest::binary>>, "new", 0 -> {:error, "disk full"} end,
      compound_scan: fn "list", _prefix ->
        [{CompoundKey.encode_position(pos), "old"}]
      end
    }

    assert {:error, "disk full"} == ListOps.execute("list", store, {:lset, 0, "new"})
  end

  test "LINSERT returns element write errors before metadata update" do
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key ->
        :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
      end,
      compound_put: fn
        "list", <<"L:list", _rest::binary>>, "new", 0 ->
          {:error, "disk full"}

        "list", <<"LM:list">>, _meta, 0 ->
          flunk("LINSERT must not update metadata after element write failure")
      end,
      compound_scan: fn "list", _prefix ->
        [{CompoundKey.encode_position(0), "pivot"}]
      end
    }

    assert {:error, "disk full"} ==
             ListOps.execute("list", store, {:linsert, :after, "pivot", "new"})
  end

  test "LINSERT rebalance returns delete errors before rewriting positions" do
    meta_key = CompoundKey.list_meta_key("list")
    old_keys = [CompoundKey.list_element("list", 0), CompoundKey.list_element("list", 1)]

    store = %{
      compound_get: fn "list", ^meta_key -> :erlang.term_to_binary({2, -1, 2}) end,
      compound_batch_delete: fn "list", ^old_keys -> {:error, "disk full"} end,
      compound_batch_put: fn "list", _entries ->
        flunk("LINSERT rebalance must not rewrite positions after delete failure")
      end,
      compound_delete: fn "list", compound_key ->
        flunk("LINSERT rebalance should batch deletes, got #{inspect(compound_key)}")
      end,
      compound_put: fn
        "list", ^meta_key, _meta, 0 ->
          flunk("LINSERT rebalance must not update metadata after delete failure")

        "list", compound_key, _value, 0 ->
          flunk(
            "LINSERT rebalance must not insert after delete failure: #{inspect(compound_key)}"
          )
      end,
      compound_scan: fn "list", _prefix ->
        [
          {CompoundKey.encode_position(0), "a"},
          {CompoundKey.encode_position(1), "b"}
        ]
      end
    }

    assert {:error, "disk full"} ==
             ListOps.execute("list", store, {:linsert, :after, "a", "x"})
  end

  test "LINSERT rebalance batches position rewrite before inserting new element" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    old_keys = [CompoundKey.list_element("list", 0), CompoundKey.list_element("list", 1)]

    store = %{
      compound_get: fn "list", ^meta_key -> :erlang.term_to_binary({2, -1, 2}) end,
      compound_batch_delete: fn "list", ^old_keys ->
        send(parent, {:rebalance_delete, old_keys})
        :ok
      end,
      compound_batch_put: fn "list", entries ->
        send(parent, {:rebalance_put, entries})
        :ok
      end,
      compound_delete: fn "list", compound_key ->
        flunk("LINSERT rebalance should batch deletes, got #{inspect(compound_key)}")
      end,
      compound_put: fn
        "list", ^meta_key, meta, 0 when is_binary(meta) ->
          :ok

        "list", <<"L:list", _rest::binary>>, "x", 0 ->
          :ok

        "list", compound_key, _value, 0 ->
          flunk("LINSERT rebalance should batch old element writes, got #{inspect(compound_key)}")
      end,
      compound_scan: fn "list", _prefix ->
        [
          {CompoundKey.encode_position(0), "a"},
          {CompoundKey.encode_position(1), "b"}
        ]
      end
    }

    assert 3 == ListOps.execute("list", store, {:linsert, :after, "a", "x"})

    assert_received {:rebalance_delete, ^old_keys}
    assert_received {:rebalance_put, entries}

    assert [
             {_, "a", 0},
             {_, "b", 0}
           ] = entries

    refute_received {:rebalance_delete, _}
    refute_received {:rebalance_put, _}
  end

  test "read-only commands treat corrupt persisted metadata as missing list" do
    store = corrupt_meta_store(<<131, 100, 0, 12, "made_up_atom">>)

    assert 0 = ListOps.execute("list", store, :llen)
  end

  test "read-only commands treat wrong-shape persisted metadata as missing list" do
    store = corrupt_meta_store(:erlang.term_to_binary({:not_a_list_meta, "x"}))

    assert [] = ListOps.execute("list", store, {:lrange, 0, -1})
  end

  defp failing_put_store do
    %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        {:error, "disk full"}
      end,
      compound_delete: fn _redis_key, _compound_key -> :ok end,
      compound_scan: fn _redis_key, _prefix -> [] end
    }
  end

  defp failing_delete_store(key, values) do
    meta = {length(values), -1_000_000_000, length(values) * 1_000_000_000}

    elements =
      values
      |> Enum.with_index()
      |> Enum.map(fn {value, idx} ->
        {CompoundKey.encode_position(idx * 1_000_000_000), value}
      end)

    %{
      compound_get: fn ^key, compound_key ->
        if compound_key == CompoundKey.list_meta_key(key), do: :erlang.term_to_binary(meta)
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_delete: fn _redis_key, _compound_key -> {:error, "disk full"} end,
      compound_scan: fn ^key, _prefix -> elements end
    }
  end

  defp corrupt_meta_store(meta_binary) do
    %{
      compound_get: fn "list", compound_key ->
        if compound_key == CompoundKey.list_meta_key("list"), do: meta_binary
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_delete: fn _redis_key, _compound_key -> :ok end,
      compound_scan: fn _redis_key, _prefix -> [] end
    }
  end
end
