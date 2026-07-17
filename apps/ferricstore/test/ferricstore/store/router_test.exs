defmodule Ferricstore.Store.RouterTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{CompoundKey, LFU, Router, SlotMap}
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers

  describe "shard_for/1" do
    test "returns integer in valid range" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      assert Router.shard_for(FerricStore.Instance.get(:default), "key") in 0..(shard_count - 1)
    end

    test "same key always maps to same shard" do
      assert Router.shard_for(FerricStore.Instance.get(:default), "hello") ==
               Router.shard_for(FerricStore.Instance.get(:default), "hello")
    end

    test "empty binary key works" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      assert Router.shard_for(FerricStore.Instance.get(:default), "") in 0..(shard_count - 1)
    end

    test "large key works" do
      shard_count = :persistent_term.get(:ferricstore_shard_count, 4)
      big_key = String.duplicate("x", 10_000)
      assert Router.shard_for(FerricStore.Instance.get(:default), big_key) in 0..(shard_count - 1)
    end

    test "hash tags co-locate keys on the same shard" do
      assert Router.shard_for(FerricStore.Instance.get(:default), "{user:42}:session") ==
               Router.shard_for(FerricStore.Instance.get(:default), "{user:42}:profile")
    end
  end

  describe "slot_for/1" do
    test "returns integer in 0..1023" do
      assert Router.slot_for(FerricStore.Instance.get(:default), "key") in 0..1023
    end

    test "same key always maps to same slot" do
      assert Router.slot_for(FerricStore.Instance.get(:default), "hello") ==
               Router.slot_for(FerricStore.Instance.get(:default), "hello")
    end

    test "hash tags co-locate keys on the same slot" do
      assert Router.slot_for(FerricStore.Instance.get(:default), "{tag}:a") ==
               Router.slot_for(FerricStore.Instance.get(:default), "{tag}:b")
    end

    test "uses the shared slot map hash implementation" do
      for key <- ["plain", "{user:42}:session", "{}empty", String.duplicate("x", 1024)] do
        assert Router.slot_for(FerricStore.Instance.get(:default), key) ==
                 SlotMap.slot_for_key(key)
      end
    end
  end

  describe "shard_name/1" do
    test "returns unique atoms per index" do
      names = Enum.map(0..3, fn i -> Router.shard_name(FerricStore.Instance.get(:default), i) end)
      assert length(Enum.uniq(names)) == 4
    end

    test "returns atoms" do
      assert is_atom(Router.shard_name(FerricStore.Instance.get(:default), 0))
    end

    test "returns expected format" do
      assert Router.shard_name(FerricStore.Instance.get(:default), 0) ==
               :"Ferricstore.Store.Shard.0"

      assert Router.shard_name(FerricStore.Instance.get(:default), 3) ==
               :"Ferricstore.Store.Shard.3"
    end
  end

  describe "batch quorum forward failure classification" do
    test "remote RPC timeout is an unknown-outcome write timeout" do
      assert [
               {:error, {:timeout, :unknown_outcome}},
               {:error, {:timeout, :unknown_outcome}}
             ] == Router.__forward_batch_failure_results__({:erpc, :timeout}, 2)
    end

    test "timeout variants are treated as unknown outcome" do
      assert [{:error, {:timeout, :unknown_outcome}}] ==
               Router.__forward_batch_failure_results__(:timeout, 1)

      assert [{:error, {:timeout, :unknown_outcome}}] ==
               Router.__forward_batch_failure_results__({:timeout, :call}, 1)
    end

    test "non-timeout forward failures stay leader unavailable" do
      assert [
               {:error, "ERR leader unavailable"},
               {:error, "ERR leader unavailable"},
               {:error, "ERR leader unavailable"}
             ] == Router.__forward_batch_failure_results__({:erpc, :noconnection}, 3)
    end
  end

  describe "direct batch grouping allocation guard" do
    test "put/delete direct batch paths keep one per-shard grouping structure" do
      source = Ferricstore.Test.SourceFiles.router_source()
      {:ok, ast} = Code.string_to_quoted(source)

      put_vars = private_function_var_names(ast, :do_batch_quorum_put_entries, 3)
      delete_vars = private_function_var_names(ast, :do_batch_quorum_delete_keys, 3)

      refute :by_shard_entries in put_vars
      refute :entries_map in put_vars
      refute :by_shard_keys in delete_vars
      refute :keys_map in delete_vars
    end

    test "WARaft batch routing does not fall back to legacy Batcher for forwarded writes" do
      source = Ferricstore.Test.SourceFiles.router_source()
      {:ok, ast} = Code.string_to_quoted(source)

      for function_name <- [
            :do_batch_quorum_commands,
            :do_batch_quorum_put_entries,
            :do_batch_quorum_delete_keys
          ] do
        body =
          ast
          |> private_function_body(function_name, 3)
          |> Macro.to_string()

        refute body =~ "origin_node == nil",
               "#{function_name}/3 must route WARaft forwarded writes through WARaftBackend, not the legacy Batcher"
      end

      forced_body =
        ast
        |> private_function_body(:do_forced_quorum_write, 4)
        |> Macro.to_string()

      refute forced_body =~ "origin_node == node()",
             "forced WARaft writes must not use the legacy forwarded Batcher when origin_node is remote"
    end

    test "WARaft hot batch routing uses fixed buckets and ordered result tuples" do
      source = Ferricstore.Test.SourceFiles.router_source()

      assert source =~ "new_waraft_batch_buckets(ctx.shard_count)"
      assert source =~ "put_elem(results, index, value)"
      assert source =~ "collect_waraft_hot_shard_batches("
      assert source =~ "merge_waraft_hot_batch_results("
    end
  end

  describe "default quorum write ingress" do
    setup do
      ShardHelpers.flush_all_keys()

      on_exit(fn ->
        ShardHelpers.flush_all_keys()
      end)

      :ok
    end

    test "remote forwarding does not use shard forwarded_quorum calls" do
      source = Ferricstore.Test.SourceFiles.router_source()

      refute source =~ "{:forwarded_quorum,",
             "default write forwarding must go through Router/Batcher, not Shard GenServer"

      assert source =~ ":__forwarded_quorum_write__"
    end

    test "single writes and RMW commands do not enter Shard write handlers" do
      ctx = FerricStore.Instance.get(:default)
      key = "router_direct_quorum:" <> Integer.to_string(System.unique_integer([:positive]))
      idx = Router.shard_for(ctx, key)
      shard = elem(ctx.shard_names, idx)
      before_version = shard_write_version(shard)

      assert :ok = Router.put(ctx, key, "1", 0)
      assert {:ok, 2} = Router.incr(ctx, key, 1)
      assert :ok = Router.delete(ctx, key)

      assert shard_write_version(shard) == before_version
    end

    test "compound marker writes use state-machine command shapes" do
      ctx = FerricStore.Instance.get(:default)
      key = "router_direct_compound:" <> Integer.to_string(System.unique_integer([:positive]))
      idx = Router.shard_for(ctx, key)
      shard = elem(ctx.shard_names, idx)
      type_key = CompoundKey.type_key(key)
      before_version = shard_write_version(shard)

      assert :ok = Router.compound_put(ctx, key, type_key, "zset", 0)
      assert "zset" = Router.compound_get(ctx, key, type_key)
      assert :ok = Router.compound_delete(ctx, key, type_key)
      assert nil == Router.compound_get(ctx, key, type_key)

      entries = [
        {CompoundKey.hash_field(key, "a"), "1", 0},
        {CompoundKey.hash_field(key, "b"), "2", 0}
      ]

      assert :ok = Router.compound_batch_put(ctx, key, entries)
      assert ["1", "2"] = Router.compound_batch_get(ctx, key, Enum.map(entries, &elem(&1, 0)))
      assert :ok = Router.compound_batch_delete(ctx, key, Enum.map(entries, &elem(&1, 0)))
      assert [nil, nil] = Router.compound_batch_get(ctx, key, Enum.map(entries, &elem(&1, 0)))

      assert shard_write_version(shard) == before_version
    end
  end

  describe "async list timeout classification" do
    test "list worker timeout is reported as unknown outcome" do
      source = Ferricstore.Test.SourceFiles.router_source()

      refute source =~ ~s({:error, "ERR list_op timeout"}),
             "list mutations may still complete after the worker call times out; classify as unknown outcome"

      assert source =~ "ErrorReasons.write_timeout_unknown()"
    end
  end

  describe "sendfile cold refs" do
    setup do
      ctx = IsolatedInstance.checkout(shard_count: 1)
      on_exit(fn -> IsolatedInstance.checkin(ctx) end)
      {:ok, ctx: ctx}
    end

    test "stale cold offset for another key is rejected before sendfile", %{ctx: ctx} do
      key_a = "sendfile_key_a"
      key_b = "sendfile_key_b"
      value_a = :binary.copy("a", ctx.hot_cache_max_value_size + 1024)
      value_b = :binary.copy("b", ctx.hot_cache_max_value_size + 1024)

      assert :ok = Router.put(ctx, key_a, value_a, 0)
      assert :ok = Router.put(ctx, key_b, value_b, 0)

      keydir = elem(ctx.keydir_refs, 0)
      assert [{^key_b, nil, _exp_b, _lfu_b, fid_b, off_b, vsize_b}] = :ets.lookup(keydir, key_b)

      :ets.insert(keydir, {key_a, nil, 0, LFU.initial(), fid_b, off_b, vsize_b})

      assert {:error, {:storage_read_failed, {:cold_value_unavailable, _reason}}} =
               Router.get(ctx, key_a)

      assert Router.get_with_file_ref(ctx, key_a) == :miss
    end

    test "corrupted cold value bytes are rejected before sendfile", %{ctx: ctx} do
      key = "sendfile_crc_" <> Integer.to_string(System.unique_integer([:positive]))
      value = :binary.copy("c", ctx.hot_cache_max_value_size + 1024)

      assert :ok = Router.put(ctx, key, value, 0)
      assert {:cold_ref, path, value_offset, size} = Router.get_with_file_ref(ctx, key)
      assert size == byte_size(value)

      assert {:ok, file} = :file.open(path, [:read, :write, :raw, :binary])

      try do
        assert :ok = :file.pwrite(file, value_offset + 16, "X")
      after
        :file.close(file)
      end

      refute Router.get_with_file_ref(ctx, key) == {:cold_ref, path, value_offset, size}
      refute Router.get(ctx, key) == value
    end
  end

  describe "HLC drift guard" do
    setup do
      ctx = IsolatedInstance.checkout(shard_count: 1)
      on_exit(fn -> IsolatedInstance.checkin(ctx) end)
      {:ok, ctx: ctx}
    end

    test "GET fails closed without deleting a TTL row that is only HLC-expired", %{ctx: ctx} do
      key = "hlc-drift:get:#{System.unique_integer([:positive])}"
      wall_ms = System.system_time(:millisecond)
      expire_at_ms = wall_ms + 30_000
      assert :ok = Router.put(ctx, key, "value", expire_at_ms)

      keydir = elem(ctx.keydir_refs, 0)
      [row] = :ets.lookup(keydir, key)
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      previous = :atomics.get(ref, 1)

      on_exit(fn ->
        :atomics.put(:persistent_term.get(:ferricstore_hlc_ref), 1, previous)
      end)

      :atomics.put(ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))
      assert Ferricstore.HLC.drift_exceeded?()

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} = Router.get(ctx, key)
      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.get_meta(ctx, key)

      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.expire_at_ms(ctx, key)

      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.value_size(ctx, key)

      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.object_lfu(ctx, key)

      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.getrange(ctx, key, 0, -1)

      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.get_file_ref(ctx, key)

      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.get_with_file_ref(ctx, key)

      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.read_shard_value(ctx, 0, key)

      assert :ets.lookup(keydir, key) == [row]

      assert [{:error, {:storage_read_failed, :hlc_drift_exceeded}}] =
               Router.batch_get(ctx, [key])

      assert :ets.lookup(keydir, key) == [row]

      assert [{:error, {:storage_read_failed, :hlc_drift_exceeded}}] =
               Router.batch_get_on_route_keys(ctx, [{key, key}])

      assert :ets.lookup(keydir, key) == [row]

      assert [{:error, {:storage_read_failed, :hlc_drift_exceeded}}] =
               Router.batch_get_with_file_refs(ctx, [key], 0)

      assert :ets.lookup(keydir, key) == [row]

      assert Router.exists?(ctx, key)
      assert Router.exists_fast?(ctx, key)
      assert :ets.lookup(keydir, key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.get_keydir_file_ref(ctx, key)

      assert :ets.lookup(keydir, key) == [row]
    end

    test "compound reads fail closed without deleting an HLC-only expired member", %{ctx: ctx} do
      redis_key = "hlc-drift:compound:#{System.unique_integer([:positive])}"
      compound_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, "field")
      wall_ms = System.system_time(:millisecond)
      expire_at_ms = wall_ms + 30_000
      assert :ok = Router.compound_put(ctx, redis_key, compound_key, "value", expire_at_ms)

      keydir = elem(ctx.keydir_refs, 0)
      [row] = :ets.lookup(keydir, compound_key)
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      previous = :atomics.get(ref, 1)

      on_exit(fn ->
        :atomics.put(:persistent_term.get(:ferricstore_hlc_ref), 1, previous)
      end)

      :atomics.put(ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.compound_get(ctx, redis_key, compound_key)

      assert :ets.lookup(keydir, compound_key) == [row]

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} =
               Router.compound_get_meta(ctx, redis_key, compound_key)

      assert :ets.lookup(keydir, compound_key) == [row]

      assert [{:error, {:storage_read_failed, :hlc_drift_exceeded}}] =
               Router.compound_batch_get(ctx, redis_key, [compound_key])

      assert :ets.lookup(keydir, compound_key) == [row]

      assert [{:error, {:storage_read_failed, :hlc_drift_exceeded}}] =
               Router.compound_batch_get_meta(ctx, redis_key, [compound_key])

      assert :ets.lookup(keydir, compound_key) == [row]

      assert [{:error, {:storage_read_failed, :hlc_drift_exceeded}}] =
               Router.compound_batch_get_on_route_keys(ctx, [{redis_key, compound_key}])

      assert :ets.lookup(keydir, compound_key) == [row]
    end

    test "cold retry preserves the original drift decision", %{ctx: ctx} do
      key = "hlc-drift:retry:#{System.unique_integer([:positive])}"
      keydir = elem(ctx.keydir_refs, 0)
      wall_ms = System.system_time(:millisecond)
      expire_at_ms = wall_ms + 30_000
      file_id = {:waraft_segment, 999_999_991}
      unsafe_row = {key, nil, expire_at_ms, 0, file_id, 0, 5}
      :ets.insert(keydir, {key, nil, 0, 0, file_id, 0, 5})

      ref = :persistent_term.get(:ferricstore_hlc_ref)
      previous = :atomics.get(ref, 1)

      on_exit(fn ->
        Process.delete(:ferricstore_router_cold_location_miss_hook)
        :atomics.put(:persistent_term.get(:ferricstore_hlc_ref), 1, previous)
      end)

      Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
        :ets.insert(keydir, unsafe_row)
      end)

      :atomics.put(ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

      assert {:error, {:storage_read_failed, :hlc_drift_exceeded}} = Router.get(ctx, key)
      assert :ets.lookup(keydir, key) == [unsafe_row]
    end
  end

  defp private_function_var_names(ast, function_name, arity) do
    {_ast, vars} =
      Macro.prewalk(ast, MapSet.new(), fn
        {:defp, _meta, [{^function_name, _call_meta, args}, body]} = node, acc
        when length(args) == arity ->
          {_body, function_vars} =
            Macro.prewalk(body, MapSet.new(), fn
              {var, _meta, context} = var_node, var_acc
              when is_atom(var) and is_atom(context) ->
                {var_node, MapSet.put(var_acc, var)}

              node, var_acc ->
                {node, var_acc}
            end)

          {node, MapSet.union(acc, function_vars)}

        node, acc ->
          {node, acc}
      end)

    MapSet.to_list(vars)
  end

  defp private_function_body(ast, function_name, arity) do
    {_ast, bodies} =
      Macro.prewalk(ast, [], fn
        {:defp, _meta, [{^function_name, _call_meta, args}, body]} = node, acc
        when length(args) == arity ->
          {node, [body | acc]}

        node, acc ->
          {node, acc}
      end)

    bodies
    |> Enum.reverse()
    |> case do
      [] -> flunk("missing #{function_name}/#{arity}")
      bodies -> {:__block__, [], bodies}
    end
  end

  defp shard_write_version(shard) do
    shard
    |> :sys.get_state()
    |> Map.fetch!(:write_version)
  end
end
