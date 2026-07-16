defmodule Ferricstore.Store.ReadResultTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.TypeRegistry

  alias Ferricstore.Commands.{
    Bitmap,
    Expiry,
    Generic,
    Hash,
    HyperLogLog,
    List,
    Memory,
    ProbType,
    Set,
    SortedSet,
    Stream,
    Strings,
    TDigest
  }

  test "wraps storage reasons without losing diagnostic detail" do
    failure = ReadResult.failure({:cold_value_unavailable, :missing_file})

    assert {:error, {:storage_read_failed, {:cold_value_unavailable, :missing_file}}} = failure
    assert ReadResult.failure?(failure)
    refute ReadResult.failure?(nil)
    refute ReadResult.failure?("value")
  end

  test "converts internal storage failures to a stable client error" do
    failure = ReadResult.failure({:cold_value_unavailable, {:file, "/private/path"}})

    assert {:error, "ERR storage read failed"} = ReadResult.command_error(failure)
  end

  test "finds a failure without copying or transforming batch values" do
    failure = ReadResult.failure(:corrupt_record)

    assert ^failure = ReadResult.first_failure(["left", failure, "right"])
    assert nil == ReadResult.first_failure(["left", nil, "right"])
  end

  test "string commands sanitize single and batch storage failures" do
    failure = ReadResult.failure({:cold_value_unavailable, {:file, "/private/path"}})

    store = %{
      get: fn _key -> failure end,
      batch_get: fn _keys -> ["readable", failure] end
    }

    assert {:error, "ERR storage read failed"} = Strings.handle("GET", ["key"], store)

    assert {:error, "ERR storage read failed"} =
             Strings.handle("MGET", ["readable", "failed"], store)
  end

  test "read-modify-write and probabilistic commands fail closed on storage errors" do
    failure = ReadResult.failure({:cold_value_unavailable, :missing_file})
    store = %{get: fn _key -> failure end}

    assert {:error, "ERR storage read failed"} = Bitmap.handle("BITCOUNT", ["key"], store)
    assert {:error, "ERR storage read failed"} = HyperLogLog.handle("PFADD", ["key", "x"], store)

    assert {:error, "ERR storage read failed"} =
             TDigest.handle_ast({:tdigest_create, "key", 100}, store)

    assert {:error, "ERR storage read failed"} = ProbType.check_expected("key", :bloom, store)
  end

  test "bitmap range reads propagate failures without retrying a full-value read" do
    failure = ReadResult.failure({:cold_range_unavailable, :missing_file})
    parent = self()

    store = %{
      get: fn _key ->
        send(parent, :unexpected_full_read)
        "fallback"
      end,
      get_meta: fn _key -> failure end,
      getrange: fn _key, _start_idx, _end_idx -> failure end,
      put: fn _key, _value, _expire_at_ms -> :ok end,
      value_size: fn _key -> 1 end
    }

    assert {:error, "ERR storage read failed"} = Bitmap.handle("GETBIT", ["key", "0"], store)
    assert {:error, "ERR storage read failed"} = Bitmap.handle("BITCOUNT", ["key"], store)
    assert {:error, "ERR storage read failed"} = Bitmap.handle("BITPOS", ["key", "1"], store)
    assert {:error, "ERR storage read failed"} = Bitmap.handle("SETBIT", ["key", "0", "1"], store)
    refute_received :unexpected_full_read
  end

  test "logical-size command paths sanitize metadata read failures" do
    failure = ReadResult.failure(:missing_file)
    store = %{value_size: fn _key -> failure end}

    assert {:error, "ERR storage read failed"} = Strings.handle("STRLEN", ["key"], store)
    assert {:error, "ERR storage read failed"} = Memory.handle("USAGE", ["key"], store)
    assert {:error, "ERR storage read failed"} = HyperLogLog.handle("PFADD", ["key", "x"], store)
  end

  test "probabilistic type checks do not retry after metadata read failure" do
    failure = ReadResult.failure(:missing_file)
    parent = self()

    store = %{
      value_size: fn _key -> failure end,
      get: fn _key ->
        send(parent, :unexpected_full_read)
        failure
      end
    }

    assert {:error, "ERR storage read failed"} = ProbType.check_expected("key", :bloom, store)
    refute_received :unexpected_full_read
  end

  test "expiry metadata failures are stable command errors" do
    failure = ReadResult.failure(:invalid_keydir_entry)
    store = %{expire_at_ms: fn _key -> failure end}

    assert {:error, "ERR storage read failed"} = Expiry.handle("TTL", ["key"], store)
    assert {:error, "ERR storage read failed"} = Expiry.handle("PTTL", ["key"], store)
    assert {:error, "ERR storage read failed"} = Expiry.handle("PERSIST", ["key"], store)
    assert {:error, "ERR storage read failed"} = Expiry.handle("EXPIRE", ["key", "10"], store)
  end

  test "compound expiry does not rewrite a partial collection snapshot" do
    failure = ReadResult.failure(:missing_file)
    parent = self()
    key = "hash"
    type_key = Ferricstore.Store.CompoundKey.type_key(key)

    store = %{
      get_meta: fn _key -> nil end,
      compound_get: fn _key, _compound_key -> nil end,
      compound_get_meta: fn
        ^key, ^type_key -> {"hash", 0}
        _key, _compound_key -> nil
      end,
      compound_scan: fn _key, _prefix -> failure end,
      compound_batch_put: fn _key, _entries ->
        send(parent, :unexpected_expiry_write)
        :ok
      end
    }

    assert {:error, "ERR storage read failed"} = Expiry.handle("EXPIRE", [key, "10"], store)
    refute_received :unexpected_expiry_write
  end

  test "generic metadata commands sanitize read failures without fallback reads" do
    failure = ReadResult.failure(:invalid_keydir_entry)
    parent = self()

    store = %{
      exists?: fn _key -> true end,
      expire_at_ms: fn _key -> failure end,
      object_lfu: fn _key -> failure end,
      value_size: fn _key -> failure end,
      get: fn _key ->
        send(parent, :unexpected_full_read)
        failure
      end
    }

    assert {:error, "ERR storage read failed"} = Generic.handle("EXPIRETIME", ["key"], store)
    assert {:error, "ERR storage read failed"} = Generic.handle("PEXPIRETIME", ["key"], store)
    assert {:error, "ERR storage read failed"} = Generic.handle("OBJECT", ["FREQ", "key"], store)

    assert {:error, "ERR storage read failed"} =
             Generic.handle("OBJECT", ["IDLETIME", "key"], store)

    assert {:error, "ERR storage read failed"} =
             Generic.handle("OBJECT", ["ENCODING", "key"], store)

    refute_received :unexpected_full_read
  end

  test "BITOP propagates metadata and batched source failures" do
    failure = ReadResult.failure(:missing_file)

    metadata_failure_store = %{
      value_size: fn _key -> failure end,
      batch_get: fn _keys -> flunk("metadata failure must stop before batch read") end,
      put: fn _key, _value, _expire_at_ms -> flunk("failed BITOP must not write") end
    }

    assert {:error, "ERR storage read failed"} =
             Bitmap.handle("BITOP", ["AND", "dest", "left", "right"], metadata_failure_store)

    batch_failure_store = %{
      batch_get: fn _keys -> ["readable", failure] end,
      put: fn _key, _value, _expire_at_ms -> flunk("failed BITOP must not write") end
    }

    assert {:error, "ERR storage read failed"} =
             Bitmap.handle("BITOP", ["OR", "dest", "left", "right"], batch_failure_store)

    assert {:error, "ERR storage read failed"} =
             Bitmap.handle("BITOP", ["XOR", "dest", "left", "right"], batch_failure_store)
  end

  test "type registry and TYPE commands propagate type-marker read failures" do
    failure = ReadResult.failure(:missing_file)

    store = %{
      compound_get: fn _redis_key, _compound_key -> failure end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        flunk("failed type lookup must not create a marker")
      end,
      exists?: fn _key -> flunk("failed marker lookup must stop before plain-key lookup") end
    }

    assert ^failure = TypeRegistry.get_type("key", store)
    assert ^failure = TypeRegistry.check_type("key", :hash, store)
    assert ^failure = TypeRegistry.check_or_set_status("key", :hash, store)
    assert {:error, "ERR storage read failed"} = Generic.handle("TYPE", ["key"], store)
    assert {:error, "ERR storage read failed"} = Strings.handle("TYPE", ["key"], store)

    command_store = %{
      compound_get: fn _redis_key, _compound_key -> failure end,
      exists?: fn _key -> false end,
      get: fn _key -> nil end,
      put: fn _key, _value, _expire_at_ms ->
        flunk("failed type lookup must stop before write")
      end
    }

    assert {:error, "ERR storage read failed"} = Strings.handle("EXISTS", ["key"], command_store)

    assert {:error, "ERR storage read failed"} =
             HyperLogLog.handle("PFADD", ["key", "x"], command_store)

    assert {:error, "ERR storage read failed"} =
             ProbType.check_expected("key", :bloom, command_store)

    assert {:error, "ERR storage read failed"} =
             TDigest.handle_ast({:tdigest_create, "key", 100}, command_store)
  end

  test "type registry propagates plain-value fallback read failures" do
    failure = ReadResult.failure(:missing_file)
    store = %{get: fn _key -> failure end}

    assert ^failure = TypeRegistry.get_type("key", store)
  end

  test "compound count failures are not mistaken for live types or cardinalities" do
    failure = ReadResult.failure(:shard_unavailable)

    store = %{
      compound_get: fn redis_key, compound_key ->
        if compound_key == Ferricstore.Store.CompoundKey.type_key(redis_key),
          do: redis_key,
          else: nil
      end,
      compound_count: fn _redis_key, _prefix -> failure end,
      exists?: fn _key -> false end,
      zset_score_count: fn _key, _min, _max -> :unavailable end
    }

    assert ^failure = TypeRegistry.get_type("hash", store)
    assert {:error, "ERR storage read failed"} = Hash.handle("HLEN", ["hash"], store)
    assert {:error, "ERR storage read failed"} = Set.handle("SCARD", ["set"], store)
    assert {:error, "ERR storage read failed"} = SortedSet.handle("ZCARD", ["zset"], store)
  end

  test "durable metadata keeps an empty stream live without a prefix count" do
    key = "empty-stream"
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    meta_key = Ferricstore.Store.CompoundKey.stream_meta_key(key)

    store = %{
      compound_get: fn
        ^key, ^type_key -> "stream"
        ^key, ^meta_key -> :erlang.term_to_binary({:stream_meta, 0, "0-0", "0-0", 0, 0})
      end,
      compound_count: fn _redis_key, _prefix ->
        flunk("durable empty-stream metadata must avoid a prefix count")
      end,
      compound_delete: fn _redis_key, _compound_key ->
        flunk("a durable empty stream must not lose its type marker")
      end,
      exists?: fn _key -> false end
    }

    assert "stream" = TypeRegistry.get_type(key, store)
  end

  test "set intersection stops before scanning when a count read fails" do
    failure = ReadResult.failure(:shard_unavailable)

    store = %{
      compound_get: fn redis_key, compound_key ->
        if compound_key == Ferricstore.Store.CompoundKey.type_key(redis_key),
          do: "set",
          else: nil
      end,
      compound_count: fn _redis_key, _prefix -> failure end,
      compound_scan: fn _redis_key, _prefix ->
        flunk("failed count must stop before member scans")
      end,
      exists?: fn _key -> false end
    }

    assert {:error, "ERR storage read failed"} = Set.handle("SINTER", ["left", "right"], store)
  end

  test "delete commands restore removed members when post-delete count fails" do
    failure = ReadResult.failure(:shard_unavailable)
    parent = self()

    hash_store = delete_count_failure_store("hash", "hash", {"value", 0}, failure, parent)
    set_store = delete_count_failure_store("set", "set", "1", failure, parent)

    assert {:error, "ERR storage read failed"} =
             Hash.handle("HDEL", ["hash", "field"], hash_store)

    assert {:error, "ERR storage read failed"} = Set.handle("SREM", ["set", "member"], set_store)

    assert_received {:deleted, "hash"}
    assert_received {:restored, "hash"}
    assert_received {:deleted, "set"}
    assert_received {:restored, "set"}
  end

  test "pipeline GET fast paths do not wrap storage failures as successful values" do
    failure = ReadResult.failure(:shard_unavailable)

    lookup = fn _redis_key, _compound_key ->
      flunk("a failed plain read must stop before compound type lookup")
    end

    assert {:error, "ERR storage read failed"} =
             FerricStore.Pipe.__pipeline_get_result_for_test__("key", failure, lookup)

    assert {:error, "ERR storage read failed"} =
             FerricStore.Pipe.__pipeline_get_result_for_test__(
               "key",
               nil,
               fn _redis_key, _compound_key -> failure end
             )
  end

  test "flow history hot reads fail as a whole when a batch member is unavailable" do
    failure = ReadResult.failure(:shard_unavailable)

    assert ^failure =
             Ferricstore.Flow.HistoryRead.hot_values_by_event(
               ["event-1", "event-2"],
               ["value", failure]
             )
  end

  test "flow history hot reads reject short and long batch replies" do
    assert ReadResult.failure({:batch_result_mismatch, 2, 1}) ==
             Ferricstore.Flow.HistoryRead.hot_values_by_event(
               ["event-1", "event-2"],
               ["value"]
             )

    assert ReadResult.failure({:batch_result_mismatch, 1, 2}) ==
             Ferricstore.Flow.HistoryRead.hot_values_by_event(
               ["event-1"],
               ["value", "unexpected"]
             )
  end

  test "typed data-structure commands sanitize type-marker read failures" do
    failure = ReadResult.failure({:missing_file, "/private/segment"})
    key = "typed_failure_#{System.unique_integer([:positive])}"

    store = %{
      compound_get: fn _redis_key, _compound_key -> failure end,
      exists?: fn _key -> flunk("failed marker lookup must stop before plain-key lookup") end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        flunk("failed type lookup must not write")
      end
    }

    assert {:error, "ERR storage read failed"} = List.handle("LLEN", [key], store)
    assert {:error, "ERR storage read failed"} = Hash.handle("HLEN", [key], store)
    assert {:error, "ERR storage read failed"} = Set.handle("SCARD", [key], store)
    assert {:error, "ERR storage read failed"} = SortedSet.handle("ZCARD", [key], store)
    assert {:error, "ERR storage read failed"} = Stream.handle("XLEN", [key], store)
  end

  test "XADD NOMKSTREAM does not treat a failed marker read as a missing stream" do
    failure = ReadResult.failure(:missing_file)
    key = "stream_marker_failure_#{System.unique_integer([:positive])}"

    store = %{
      compound_get: fn _redis_key, _compound_key -> failure end,
      compound_put: fn _redis_key, _compound_key, _value, _expire_at_ms ->
        flunk("XADD must not write after a failed marker lookup")
      end
    }

    assert {:error, "ERR storage read failed"} =
             Stream.handle("XADD", [key, "NOMKSTREAM", "*", "field", "value"], store)
  end

  test "compound collection scans fail as a whole when one value is unreadable" do
    failure = ReadResult.failure(:missing_file)

    assert {:error, "ERR storage read failed"} =
             Hash.handle("HGETALL", ["hash"], failing_scan_store("hash", "hash", failure))

    assert {:error, "ERR storage read failed"} =
             Set.handle("SMEMBERS", ["set"], failing_scan_store("set", "set", failure))

    assert {:error, "ERR storage read failed"} =
             List.handle(
               "LRANGE",
               ["list", "0", "-1"],
               failing_scan_store("list", "list", failure)
             )

    assert {:error, "ERR storage read failed"} =
             SortedSet.handle(
               "ZRANDMEMBER",
               ["zset"],
               failing_scan_store("zset", "zset", failure)
             )

    assert {:error, "ERR storage read failed"} =
             Ferricstore.Commands.Geo.handle(
               "GEOSEARCH",
               ["geo", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "KM"],
               failing_scan_store("geo", "zset", failure)
             )

    stream_key = "stream_scan_failure_#{System.unique_integer([:positive])}"

    assert {:error, "ERR storage read failed"} =
             Stream.handle(
               "XLEN",
               [stream_key],
               failing_scan_store(stream_key, "stream", failure)
             )
  end

  test "stream mutations do not delete after incomplete reads" do
    failure = ReadResult.failure(:missing_file)
    parent = self()

    xdel_store = %{
      compound_get: fn _key, _compound_key -> nil end,
      compound_batch_get: fn _key, _compound_keys -> [failure] end,
      compound_scan: fn _key, _prefix -> failure end,
      compound_batch_delete: fn _key, _compound_keys ->
        send(parent, :unexpected_delete)
        :ok
      end
    }

    assert {:error, "ERR storage read failed"} =
             Stream.handle("XDEL", ["stream", "1-0"], xdel_store)

    trim_key = "stream_trim_failure_#{System.unique_integer([:positive])}"
    Stream.Meta.put_local(trim_key, 1, "1-0", "1-0", 1, 0)
    on_exit(fn -> Stream.Meta.cleanup_local(trim_key) end)

    trim_store = %{
      get: fn _key -> nil end,
      compound_get: fn _key, _compound_key -> nil end,
      compound_scan: fn _key, _prefix -> failure end,
      compound_batch_delete: fn _key, _compound_keys ->
        send(parent, :unexpected_delete)
        :ok
      end
    }

    assert {:error, "ERR storage read failed"} =
             Stream.handle("XTRIM", [trim_key, "MAXLEN", "0"], trim_store)

    refute_received :unexpected_delete
  end

  test "hash point and batch mutations stop before writes on incomplete reads" do
    failure = ReadResult.failure(:missing_file)
    parent = self()
    key = "hash_read_failure_#{System.unique_integer([:positive])}"
    type_key = Ferricstore.Store.CompoundKey.type_key(key)

    unexpected_write = fn ->
      send(parent, :unexpected_hash_write)
      :ok
    end

    store = %{
      compound_get: fn
        ^key, ^type_key -> "hash"
        _key, _compound_key -> failure
      end,
      compound_batch_get: fn _key, compound_keys ->
        Elixir.List.duplicate(failure, length(compound_keys))
      end,
      compound_batch_get_meta: fn _key, compound_keys ->
        Elixir.List.duplicate(failure, length(compound_keys))
      end,
      compound_put: fn _key, _compound_key, _value, _expire_at_ms -> unexpected_write.() end,
      compound_batch_put: fn _key, _entries -> unexpected_write.() end,
      compound_batch_delete: fn _key, _compound_keys -> unexpected_write.() end,
      exists?: fn _key -> false end
    }

    assert {:error, "ERR storage read failed"} = Hash.handle("HGET", [key, "field"], store)
    assert {:error, "ERR storage read failed"} = Hash.handle("HMGET", [key, "field"], store)
    assert {:error, "ERR storage read failed"} = Hash.handle("HSET", [key, "field", "v"], store)
    assert {:error, "ERR storage read failed"} = Hash.handle("HDEL", [key, "field"], store)

    assert {:error, "ERR storage read failed"} =
             Hash.handle("HINCRBY", [key, "field", "1"], store)

    assert {:error, "ERR storage read failed"} =
             Hash.handle("HSETNX", [key, "field", "v"], store)

    refute_received :unexpected_hash_write
  end

  test "consumer-group commands do not treat failed persisted reads as missing" do
    failure = ReadResult.failure(:missing_file)
    parent = self()
    key = "group_read_failure_#{System.unique_integer([:positive])}"

    store = %{
      compound_get: fn _key, _compound_key -> failure end,
      compound_put: fn _key, _compound_key, _value, _expire_at_ms ->
        send(parent, :unexpected_group_write)
        :ok
      end
    }

    assert {:error, "ERR storage read failed"} =
             Stream.handle("XACK", [key, "group", "1-0"], store)

    refute_received :unexpected_group_write
  end

  test "XADD rolls back its entry and new type marker when metadata persistence fails" do
    parent = self()
    key = "xadd_meta_failure_#{System.unique_integer([:positive])}"
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    meta_key = Ferricstore.Store.CompoundKey.stream_meta_key(key)
    entry_key = Ferricstore.Commands.Stream.Entries.entry_key(key, "1-0")

    store = %{
      compound_get: fn _key, _compound_key -> nil end,
      compound_put: fn
        ^key, ^type_key, "stream", 0 ->
          send(parent, :type_written)
          :ok

        ^key, ^entry_key, _value, 0 ->
          send(parent, :entry_written)
          :ok

        ^key, ^meta_key, _value, 0 ->
          {:error, :metadata_write_failed}
      end,
      compound_batch_delete: fn ^key, [^entry_key] ->
        send(parent, :entry_rolled_back)
        :ok
      end,
      compound_delete: fn ^key, ^type_key ->
        send(parent, :type_rolled_back)
        :ok
      end,
      exists?: fn _key -> false end
    }

    assert {:error, :metadata_write_failed} =
             Stream.handle("XADD", [key, "1-0", "field", "value"], store)

    assert_received :type_written
    assert_received :entry_written
    assert_received :entry_rolled_back
    assert_received :type_rolled_back
  end

  test "XADD preflights trim reads before writing the entry" do
    failure = ReadResult.failure(:missing_file)
    parent = self()
    key = "xadd_trim_failure_#{System.unique_integer([:positive])}"
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    meta_key = Ferricstore.Store.CompoundKey.stream_meta_key(key)
    entry_key = Ferricstore.Commands.Stream.Entries.entry_key(key, "2-0")

    Stream.Meta.put_local(key, 1, "1-0", "1-0", 1, 0)

    on_exit(fn -> Stream.Meta.cleanup_local(key) end)

    store = %{
      compound_get: fn
        ^key, ^type_key -> "stream"
        _redis_key, _compound_key -> nil
      end,
      compound_put: fn
        ^key, ^entry_key, _value, 0 ->
          send(parent, :trim_entry_written)
          :ok

        ^key, ^meta_key, _value, 0 ->
          send(parent, :trim_meta_written)
          :ok
      end,
      compound_scan: fn ^key, _prefix -> failure end,
      exists?: fn _key -> false end
    }

    assert {:error, "ERR storage read failed"} =
             Stream.handle("XADD", [key, "MAXLEN", "1", "2-0", "field", "value"], store)

    refute_received :trim_entry_written
    refute_received :trim_meta_written
  end

  test "XDEL reports metadata persistence failures after deleting entries" do
    parent = self()
    key = "xdel_meta_failure_#{System.unique_integer([:positive])}"
    id = "1-0"
    entry_key = Ferricstore.Commands.Stream.Entries.entry_key(key, id)
    meta_key = Ferricstore.Store.CompoundKey.stream_meta_key(key)

    Stream.Meta.put_local(key, 1, id, id, 1, 0)

    on_exit(fn -> Stream.Meta.cleanup_local(key) end)

    store = %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      compound_batch_get: fn ^key, [^entry_key] ->
        [:erlang.term_to_binary(["field", "value"])]
      end,
      compound_batch_delete: fn ^key, [^entry_key] ->
        send(parent, :xdel_entry_deleted)
        :ok
      end,
      compound_put: fn ^key, ^meta_key, _value, 0 -> {:error, :metadata_write_failed} end
    }

    assert {:error, :metadata_write_failed} = Stream.handle("XDEL", [key, id], store)
    assert_received :xdel_entry_deleted
  end

  test "XGROUP MKSTREAM rolls back its group and new type marker when metadata persistence fails" do
    parent = self()
    key = "xgroup_meta_failure_#{System.unique_integer([:positive])}"
    group = "workers"
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    meta_key = Ferricstore.Store.CompoundKey.stream_meta_key(key)
    group_key = Ferricstore.Store.CompoundKey.stream_group(key, group)

    store = %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      compound_scan: fn _redis_key, _prefix -> [] end,
      compound_put: fn
        ^key, ^type_key, "stream", 0 ->
          send(parent, :xgroup_type_written)
          :ok

        ^key, ^group_key, _value, 0 ->
          send(parent, :xgroup_written)
          :ok

        ^key, ^meta_key, _value, 0 ->
          {:error, :metadata_write_failed}
      end,
      compound_delete: fn
        ^key, ^group_key ->
          send(parent, :xgroup_rolled_back)
          :ok

        ^key, ^type_key ->
          send(parent, :xgroup_type_rolled_back)
          :ok
      end,
      exists?: fn _key -> false end
    }

    on_exit(fn -> Stream.Groups.delete_local(key) end)

    assert {:error, :metadata_write_failed} =
             Stream.handle("XGROUP", ["CREATE", key, group, "0", "MKSTREAM"], store)

    assert_received :xgroup_type_written
    assert_received :xgroup_written
    assert_received :xgroup_rolled_back
    assert_received :xgroup_type_rolled_back
    assert :missing = Stream.Groups.lookup(store, key, group)
  end

  defp failing_scan_store(key, type, failure) do
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    list_meta_key = Ferricstore.Store.CompoundKey.list_meta_key(key)

    %{
      compound_get: fn
        ^key, ^type_key ->
          type

        ^key, ^list_meta_key when type == "list" ->
          Ferricstore.Store.ListOps.encode_meta({1, -1_000_000_000, 1_000_000_000})

        _redis_key, _compound_key ->
          nil
      end,
      compound_scan: fn _redis_key, _prefix -> failure end,
      exists?: fn _key -> false end
    }
  end

  defp delete_count_failure_store(key, type, existing, failure, parent) do
    type_key = Ferricstore.Store.CompoundKey.type_key(key)

    %{
      compound_get: fn
        ^key, ^type_key -> type
        _redis_key, _compound_key -> nil
      end,
      compound_batch_get: fn ^key, [_compound_key] -> [existing] end,
      compound_batch_get_meta: fn ^key, [_compound_key] -> [existing] end,
      compound_batch_delete: fn ^key, [_compound_key] ->
        send(parent, {:deleted, key})
        :ok
      end,
      compound_count: fn ^key, _prefix -> failure end,
      compound_batch_put: fn ^key, [_entry] ->
        send(parent, {:restored, key})
        :ok
      end,
      exists?: fn _plain_key -> false end
    }
  end
end
