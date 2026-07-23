defmodule Ferricstore.Store.ListOpsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.ListOps
  alias Ferricstore.Store.Shard.NativeOps

  test "list metadata codec round-trips metadata" do
    meta = {3, -1_000_000_000, 2_000_000_000}

    encoded = ListOps.encode_meta(meta)

    assert ListOps.decode_meta(encoded) == meta
  end

  test "list metadata codec reads existing Erlang term metadata" do
    meta = {3, -1, 2}

    assert ListOps.decode_meta(:erlang.term_to_binary(meta)) == meta
  end

  test "list metadata codec rejects invalid metadata" do
    assert ListOps.decode_meta(:erlang.term_to_binary({-1, 0, 0})) == nil
    assert ListOps.decode_meta(:erlang.term_to_binary({"3", 0, 0})) == nil
    assert ListOps.decode_meta("not-erlang-term") == nil
  end

  test "list metadata codec rejects non-canonical and oversized external terms" do
    meta = {3, -1, 2}
    encoded = ListOps.encode_meta(meta)
    huge = :binary.decode_unsigned(:binary.copy(<<1>>, 4_096))
    compressed = :erlang.term_to_binary({huge, huge, huge}, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed

    assert ListOps.decode_meta(encoded <> <<0>>) == nil
    assert ListOps.decode_meta(compressed) == nil
    assert ListOps.decode_meta(:erlang.term_to_binary({huge, huge, huge})) == nil
  end

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

  test "LPOP count reads only the bounded left window" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key ->
        ListOps.encode_meta({100, -1_000_000_000, 100_000_000_000})
      end,
      compound_scan_slice: fn "list", _prefix, start, count, total ->
        send(parent, {:slice, start, count, total})

        [
          {CompoundKey.encode_position(0), "a"},
          {CompoundKey.encode_position(1_000_000_000), "b"}
        ]
      end,
      compound_scan: fn _redis_key, _prefix -> flunk("LPOP count must not scan the full list") end,
      compound_batch_delete: fn "list", keys ->
        send(parent, {:deleted, keys})
        :ok
      end,
      compound_put: fn "list", ^meta_key, encoded_meta, 0 ->
        send(parent, {:meta, ListOps.decode_meta(encoded_meta)})
        :ok
      end
    }

    assert ["a", "b"] == ListOps.execute("list", store, {:lpop, 2})
    assert_received {:slice, 0, 2, 100}
    assert_received {:deleted, deleted}
    assert length(deleted) == 2
    assert_received {:meta, {98, 1_000_000_000, 100_000_000_000}}
  end

  test "RPOP count reads only the bounded right window" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key ->
        ListOps.encode_meta({100, -1_000_000_000, 100_000_000_000})
      end,
      compound_scan_slice: fn "list", _prefix, start, count, total ->
        send(parent, {:slice, start, count, total})

        [
          {CompoundKey.encode_position(98_000_000_000), "y"},
          {CompoundKey.encode_position(99_000_000_000), "z"}
        ]
      end,
      compound_scan: fn _redis_key, _prefix -> flunk("RPOP count must not scan the full list") end,
      compound_batch_delete: fn "list", keys ->
        send(parent, {:deleted, keys})
        :ok
      end,
      compound_put: fn "list", ^meta_key, encoded_meta, 0 ->
        send(parent, {:meta, ListOps.decode_meta(encoded_meta)})
        :ok
      end
    }

    assert ["z", "y"] == ListOps.execute("list", store, {:rpop, 2})
    assert_received {:slice, 98, 2, 100}
    assert_received {:deleted, deleted}
    assert length(deleted) == 2
    assert_received {:meta, {98, -1_000_000_000, 98_000_000_000}}
  end

  test "single LPOP uses metadata boundary without scanning the list" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    left_key = CompoundKey.list_element("list", 0)

    store = %{
      compound_get: fn
        "list", ^meta_key -> :erlang.term_to_binary({3, -1_000_000_000, 3_000_000_000})
        "list", ^left_key -> "a"
      end,
      compound_batch_delete: fn "list", [^left_key] ->
        send(parent, :deleted_left)
        :ok
      end,
      compound_delete: fn "list", compound_key ->
        flunk("single LPOP should batch-delete element, got #{inspect(compound_key)}")
      end,
      compound_put: fn "list", ^meta_key, meta_binary, 0 when is_binary(meta_binary) ->
        send(parent, {:meta, ListOps.decode_meta(meta_binary)})
        :ok
      end,
      compound_scan: fn "list", _prefix ->
        flunk("single LPOP should not scan the full list")
      end
    }

    assert "a" == ListOps.execute("list", store, {:lpop, 1})
    assert_received :deleted_left
    assert_received {:meta, {2, 0, 3_000_000_000}}
  end

  test "single RPOP uses metadata boundary without scanning the list" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    right_key = CompoundKey.list_element("list", 2_000_000_000)

    store = %{
      compound_get: fn
        "list", ^meta_key -> :erlang.term_to_binary({3, -1_000_000_000, 3_000_000_000})
        "list", ^right_key -> "c"
      end,
      compound_batch_delete: fn "list", [^right_key] ->
        send(parent, :deleted_right)
        :ok
      end,
      compound_delete: fn "list", compound_key ->
        flunk("single RPOP should batch-delete element, got #{inspect(compound_key)}")
      end,
      compound_put: fn "list", ^meta_key, meta_binary, 0 when is_binary(meta_binary) ->
        send(parent, {:meta, ListOps.decode_meta(meta_binary)})
        :ok
      end,
      compound_scan: fn "list", _prefix ->
        flunk("single RPOP should not scan the full list")
      end
    }

    assert "c" == ListOps.execute("list", store, {:rpop, 1})
    assert_received :deleted_right
    assert_received {:meta, {2, -1_000_000_000, 2_000_000_000}}
  end

  test "single-element LRANGE uses metadata boundary without scanning the list" do
    meta_key = CompoundKey.list_meta_key("list")
    element_key = CompoundKey.list_element("list", 0)

    store = %{
      compound_get: fn
        "list", ^meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        "list", ^element_key -> "a"
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_delete: fn _redis_key, _compound_key -> :ok end,
      compound_scan: fn "list", _prefix ->
        flunk("single-element LRANGE should not scan the full list")
      end
    }

    assert ["a"] == ListOps.execute("list", store, {:lrange, 0, -1})
  end

  test "LRANGE first element uses metadata boundary without scanning multi-element list" do
    meta_key = CompoundKey.list_meta_key("list")
    first_key = CompoundKey.list_element("list", 0)

    store = %{
      compound_get: fn
        "list", ^meta_key -> ListOps.encode_meta({3, -1_000_000_000, 3_000_000_000})
        "list", ^first_key -> "a"
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_delete: fn _redis_key, _compound_key -> :ok end,
      compound_scan: fn "list", _prefix ->
        flunk("LRANGE 0 0 should not scan the full list")
      end
    }

    assert ["a"] == ListOps.execute("list", store, {:lrange, 0, 0})
  end

  test "LRANGE last element uses metadata boundary without scanning multi-element list" do
    meta_key = CompoundKey.list_meta_key("list")
    last_key = CompoundKey.list_element("list", 2_000_000_000)

    store = %{
      compound_get: fn
        "list", ^meta_key -> ListOps.encode_meta({3, -1_000_000_000, 3_000_000_000})
        "list", ^last_key -> "c"
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms -> :ok end,
      compound_delete: fn _redis_key, _compound_key -> :ok end,
      compound_scan: fn "list", _prefix ->
        flunk("LRANGE -1 -1 should not scan the full list")
      end
    }

    assert ["c"] == ListOps.execute("list", store, {:lrange, -1, -1})
  end

  test "single LPOP of the last element deletes metadata without scanning" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    left_key = CompoundKey.list_element("list", 0)

    store = %{
      compound_get: fn
        "list", ^meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        "list", ^left_key -> "a"
      end,
      compound_batch_delete: fn "list", [^left_key, ^meta_key] ->
        send(parent, :deleted_left_and_meta)
        :ok
      end,
      compound_delete: fn "list", ^meta_key ->
        flunk("single LPOP last element should batch-delete metadata")
      end,
      compound_put: fn "list", compound_key, _value, 0 ->
        flunk("single LPOP last element should delete metadata, got #{inspect(compound_key)}")
      end,
      compound_scan: fn "list", _prefix ->
        flunk("single LPOP last element should not scan the full list")
      end
    }

    assert "a" == ListOps.execute("list", store, {:lpop, 1})
    assert_received :deleted_left_and_meta
  end

  test "single RPOP of the last element deletes metadata in the element batch" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    right_key = CompoundKey.list_element("list", 0)

    store = %{
      compound_get: fn
        "list", ^meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        "list", ^right_key -> "a"
      end,
      compound_batch_delete: fn "list", [^right_key, ^meta_key] ->
        send(parent, :deleted_right_and_meta)
        :ok
      end,
      compound_delete: fn "list", ^meta_key ->
        flunk("single RPOP last element should batch-delete metadata")
      end,
      compound_put: fn "list", compound_key, _value, 0 ->
        flunk("single RPOP last element should delete metadata, got #{inspect(compound_key)}")
      end,
      compound_scan: fn "list", _prefix ->
        flunk("single RPOP last element should not scan the full list")
      end
    }

    assert "a" == ListOps.execute("list", store, {:rpop, 1})
    assert_received :deleted_right_and_meta
  end

  test "LPOP count that empties the list deletes metadata in the element batch" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    meta = {2, -1_000_000_000, 2_000_000_000}
    left_key = CompoundKey.list_element("list", 0)
    right_key = CompoundKey.list_element("list", 1_000_000_000)

    elements = [
      {CompoundKey.encode_position(0), "a"},
      {CompoundKey.encode_position(1_000_000_000), "b"}
    ]

    store = %{
      compound_get: fn "list", ^meta_key -> :erlang.term_to_binary(meta) end,
      compound_batch_delete: fn "list", [^left_key, ^right_key, ^meta_key] ->
        send(parent, :deleted_all_and_meta)
        :ok
      end,
      compound_delete: fn "list", ^meta_key ->
        flunk("LPOP count should batch-delete metadata when emptying list")
      end,
      compound_put: fn "list", _compound_key, _value, 0 ->
        flunk("LPOP count should not write metadata when emptying list")
      end,
      compound_scan: fn "list", _prefix -> elements end
    }

    assert ["a", "b"] == ListOps.execute("list", store, {:lpop, 2})
    assert_received :deleted_all_and_meta
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
      compound_batch_delete: fn "src", [^element_key, ^meta_key] ->
        {:error, "disk full"}
      end,
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
    src_element_key = CompoundKey.list_element("src", 0)

    store = %{
      compound_get: fn
        "src", ^src_meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        _redis_key, _compound_key -> nil
      end,
      compound_batch_delete: fn "src", [^src_element_key, ^src_meta_key] -> :ok end,
      compound_batch_put: fn
        "dst", _entries -> {:error, "disk full"}
        "src", _entries -> :ok
      end,
      compound_scan: fn "src", _prefix ->
        [{CompoundKey.encode_position(0), "a"}]
      end
    }

    assert {:error, "disk full"} == ListOps.execute_lmove("src", "dst", store, :left, :right)
  end

  test "LMOVE writes the destination element and metadata in one batch" do
    parent = self()
    src_meta_key = CompoundKey.list_meta_key("src")
    dst_meta_key = CompoundKey.list_meta_key("dst")
    src_element_key = CompoundKey.list_element("src", 0)
    dst_element_key = CompoundKey.list_element("dst", 0)

    store = %{
      compound_get: fn
        "src", ^src_meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        _redis_key, _compound_key -> nil
      end,
      compound_batch_delete: fn "src", [^src_element_key, ^src_meta_key] ->
        send(parent, :source_batch_deleted)
        :ok
      end,
      compound_batch_put: fn
        "dst",
        [
          {^dst_element_key, "a", 0},
          {^dst_meta_key, encoded_meta, 0}
        ] ->
          assert {1, -1_000_000_000, 1_000_000_000} == ListOps.decode_meta(encoded_meta)
          send(parent, :destination_batch_written)
          :ok
      end,
      compound_scan: fn "src", _prefix ->
        [{CompoundKey.encode_position(0), "a"}]
      end
    }

    assert "a" == ListOps.execute_lmove("src", "dst", store, :left, :right)
    assert_received :source_batch_deleted
    assert_received :destination_batch_written
  end

  test "LMOVE reads only a bounded source boundary window" do
    parent = self()
    source_meta_key = CompoundKey.list_meta_key("src")
    destination_meta_key = CompoundKey.list_meta_key("dst")
    source_element_key = CompoundKey.list_element("src", 0)
    destination_element_key = CompoundKey.list_element("dst", 0)

    source_meta = {100, -1_000_000_000, 100_000_000_000}

    store = %{
      compound_get: fn
        "src", ^source_meta_key -> ListOps.encode_meta(source_meta)
        "dst", ^destination_meta_key -> nil
      end,
      compound_scan: fn _key, _prefix ->
        flunk("LMOVE must not scan the full source list")
      end,
      compound_scan_slice: fn "src", _prefix, 0, 2, 100 ->
        send(parent, :bounded_source_slice)

        [
          {CompoundKey.encode_position(0), "first"},
          {CompoundKey.encode_position(500_000_000), "second"}
        ]
      end,
      compound_batch_delete: fn "src", [^source_element_key] -> :ok end,
      compound_put: fn "src", ^source_meta_key, encoded_meta, 0 ->
        assert {99, -500_000_000, 100_000_000_000} == ListOps.decode_meta(encoded_meta)
        :ok
      end,
      compound_batch_put: fn
        "dst",
        [
          {^destination_element_key, "first", 0},
          {^destination_meta_key, encoded_meta, 0}
        ] ->
          assert {1, -1_000_000_000, 1_000_000_000} == ListOps.decode_meta(encoded_meta)
          :ok
      end
    }

    assert "first" == ListOps.execute_lmove("src", "dst", store, :left, :right)
    assert_received :bounded_source_slice
  end

  test "LMOVE reports a failed source restore after a destination failure" do
    src_meta_key = CompoundKey.list_meta_key("src")
    src_element_key = CompoundKey.list_element("src", 0)

    store = %{
      compound_get: fn
        "src", ^src_meta_key -> :erlang.term_to_binary({1, -1_000_000_000, 1_000_000_000})
        _redis_key, _compound_key -> nil
      end,
      compound_batch_delete: fn
        "src", [^src_element_key] -> :ok
        "src", [^src_element_key, ^src_meta_key] -> :ok
      end,
      compound_delete: fn "src", ^src_meta_key -> :ok end,
      compound_put: fn
        "dst", _compound_key, "a", 0 -> {:error, :disk_full}
        "src", ^src_element_key, "a", 0 -> {:error, :restore_failed}
        "src", ^src_meta_key, _meta, 0 -> :ok
      end,
      compound_batch_put: fn
        "dst", _entries -> {:error, :disk_full}
        "src", _entries -> {:error, :restore_failed}
      end,
      compound_scan: fn "src", _prefix ->
        [{CompoundKey.encode_position(0), "a"}]
      end
    }

    assert {:error,
            {:lmove_source_rollback_failed, {:error, :disk_full}, {:error, :restore_failed}}} ==
             ListOps.execute_lmove("src", "dst", store, :left, :right)
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
    [old_first, old_second] = old_keys

    store = %{
      compound_get: fn "list", ^meta_key -> :erlang.term_to_binary({2, -1, 2}) end,
      compound_batch_get_meta: fn "list", compound_keys ->
        Enum.map(compound_keys, fn
          ^old_first -> {"a", 0}
          ^old_second -> {"b", 0}
          ^meta_key -> {:erlang.term_to_binary({2, -1, 2}), 0}
          _new_key -> nil
        end)
      end,
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

  test "LINSERT rebalance fallback batches the complete replacement" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    old_keys = [CompoundKey.list_element("list", 0), CompoundKey.list_element("list", 1)]
    [old_first, old_second] = old_keys

    store = %{
      compound_get: fn "list", ^meta_key -> :erlang.term_to_binary({2, -1, 2}) end,
      compound_batch_get_meta: fn "list", compound_keys ->
        Enum.map(compound_keys, fn
          ^old_first -> {"a", 0}
          ^old_second -> {"b", 0}
          ^meta_key -> {:erlang.term_to_binary({2, -1, 2}), 0}
          _new_key -> nil
        end)
      end,
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

    assert {^meta_key, encoded_meta, 0} = List.keyfind(entries, meta_key, 0)
    assert {3, -1_000_000_000, 2_000_000_000} == ListOps.decode_meta(encoded_meta)

    values =
      entries
      |> Enum.reject(fn {compound_key, _value, _expire_at_ms} -> compound_key == meta_key end)
      |> Enum.map(fn {_compound_key, value, _expire_at_ms} -> value end)

    assert Enum.sort(values) == ["a", "b", "x"]

    refute_received {:rebalance_delete, _}
    refute_received {:rebalance_put, _}
  end

  test "LINSERT rebalance commits deletes, rewritten positions, element, and metadata together" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    old_keys = [CompoundKey.list_element("list", 0), CompoundKey.list_element("list", 1)]

    store = %{
      compound_get: fn "list", ^meta_key -> ListOps.encode_meta({2, -1, 2}) end,
      compound_batch_mutate: fn "list", deletes, puts ->
        send(parent, {:compound_batch_mutate, deletes, puts})
        :ok
      end,
      compound_batch_delete: fn _redis_key, _keys ->
        flunk("rebalance must use one mixed mutation")
      end,
      compound_batch_put: fn _redis_key, _entries ->
        flunk("rebalance must use one mixed mutation")
      end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        flunk("rebalance must use one mixed mutation")
      end,
      compound_scan: fn "list", _prefix ->
        [
          {CompoundKey.encode_position(0), "a"},
          {CompoundKey.encode_position(1), "b"}
        ]
      end
    }

    assert 3 == ListOps.execute("list", store, {:linsert, :after, "a", "x"})
    assert_received {:compound_batch_mutate, ^old_keys, puts}

    assert {^meta_key, encoded_meta, 0} = List.keyfind(puts, meta_key, 0)
    assert {3, -1_000_000_000, 2_000_000_000} == ListOps.decode_meta(encoded_meta)

    values =
      puts
      |> Enum.reject(fn {key, _value, _expire_at_ms} -> key == meta_key end)
      |> Enum.map(fn {_key, value, _expire_at_ms} -> value end)

    assert Enum.sort(values) == ["a", "b", "x"]
    refute_received {:compound_batch_mutate, _, _}
  end

  test "LINDEX reads only the requested catalog rank" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key ->
        ListOps.encode_meta({100, -1_000_000_000, 100_000_000_000})
      end,
      compound_scan_slice: fn "list", _prefix, start, count, total ->
        send(parent, {:slice, start, count, total})
        [{CompoundKey.encode_position(50_000_000_000), "target"}]
      end,
      compound_scan: fn _redis_key, _prefix -> flunk("LINDEX must not scan the full list") end
    }

    assert "target" == ListOps.execute("list", store, {:lindex, 50})
    assert_received {:slice, 50, 1, 100}
  end

  test "small LRANGE materializes only its requested window" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key ->
        ListOps.encode_meta({100, -1_000_000_000, 100_000_000_000})
      end,
      compound_scan_slice: fn "list", _prefix, start, count, total ->
        send(parent, {:slice, start, count, total})

        Enum.map(10..12, fn rank ->
          {CompoundKey.encode_position(rank * 1_000_000_000), "v#{rank}"}
        end)
      end,
      compound_scan: fn _redis_key, _prefix -> flunk("LRANGE must not scan the full list") end
    }

    assert ["v10", "v11", "v12"] == ListOps.execute("list", store, {:lrange, 10, 12})
    assert_received {:slice, 10, 3, 100}
  end

  test "LSET resolves only the target catalog rank" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")
    element_key = CompoundKey.list_element("list", 50_000_000_000)

    store = %{
      compound_get: fn "list", ^meta_key ->
        ListOps.encode_meta({100, -1_000_000_000, 100_000_000_000})
      end,
      compound_scan_slice: fn "list", _prefix, 50, 1, 100 ->
        [{CompoundKey.encode_position(50_000_000_000), "old"}]
      end,
      compound_put: fn "list", ^element_key, "new", 0 ->
        send(parent, :updated_target)
        :ok
      end,
      compound_scan: fn _redis_key, _prefix -> flunk("LSET must not scan the full list") end
    }

    assert :ok == ListOps.execute("list", store, {:lset, 50, "new"})
    assert_received :updated_target
  end

  test "LPOS MAXLEN reads only the bounded search window" do
    parent = self()
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key ->
        ListOps.encode_meta({100, -1_000_000_000, 100_000_000_000})
      end,
      compound_scan_slice: fn "list", _prefix, start, count, total ->
        send(parent, {:slice, start, count, total})

        ["a", "b", "target", "c", "d"]
        |> Enum.with_index(start)
        |> Enum.map(fn {value, rank} ->
          {CompoundKey.encode_position(rank * 1_000_000_000), value}
        end)
      end,
      compound_scan: fn _redis_key, _prefix ->
        flunk("LPOS MAXLEN must not scan the full list")
      end
    }

    assert 2 == ListOps.execute("list", store, {:lpos, "target", 1, nil, 5})
    assert_received {:slice, 0, 5, 100}
  end

  test "read-only commands report corrupt persisted metadata" do
    store = corrupt_meta_store(<<131, 100, 0, 12, "made_up_atom">>)

    assert {:error, "ERR storage read failed"} = ListOps.execute("list", store, :llen)
  end

  test "read-only commands report wrong-shape persisted metadata" do
    store = corrupt_meta_store(:erlang.term_to_binary({:not_a_list_meta, "x"}))

    assert {:error, "ERR storage read failed"} =
             ListOps.execute("list", store, {:lrange, 0, -1})
  end

  test "list scans report corrupt persisted position keys instead of raising" do
    meta_key = CompoundKey.list_meta_key("list")

    store = %{
      compound_get: fn "list", ^meta_key ->
        ListOps.encode_meta({2, -1_000_000_000, 2_000_000_000})
      end,
      compound_scan: fn "list", _prefix -> [{"corrupt-position", "a"}] end,
      compound_scan_slice: fn "list", _prefix, _start, _count, _total ->
        [{"P00000000000000000000.bad", "a"}]
      end
    }

    assert {:error, "ERR storage read failed"} =
             ListOps.execute("list", store, {:lrange, 0, -1})

    assert {:error, "ERR storage read failed"} =
             ListOps.execute("list", store, {:lrem, 0, "a"})
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
