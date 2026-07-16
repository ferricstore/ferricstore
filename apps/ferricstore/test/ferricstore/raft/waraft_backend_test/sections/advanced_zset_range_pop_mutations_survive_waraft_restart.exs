defmodule Ferricstore.Raft.WARaftBackendTest.Sections.AdvancedZsetRangePopMutationsSurviveWaraftRestart do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

      test "advanced zset range and pop mutations survive WARaft restart", %{root: root, ctx: ctx} do
        key = "router:zset-advanced:#{System.unique_integer([:positive])}"

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 4 =
                   Ferricstore.Commands.SortedSet.handle_ast(
                     {:zadd, key, [], [{1.0, "a"}, {2.0, "b"}, {3.0, "c"}, {4.0, "d"}]},
                     ctx
                   )

          assert ["a", "1.0"] =
                   Ferricstore.Commands.SortedSet.handle_ast({:zpopmin, key}, ctx)

          assert ["d", "4.0"] =
                   Ferricstore.Commands.SortedSet.handle_ast({:zpopmax, key}, ctx)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert ["b", "2.0", "c", "3.0"] =
                   Ferricstore.Commands.SortedSet.handle(
                     "ZRANGEBYSCORE",
                     [key, "2", "3", "WITHSCORES"],
                     restarted_ctx
                   )

          assert ["c", "3.0", "b", "2.0"] =
                   Ferricstore.Commands.SortedSet.handle(
                     "ZREVRANGEBYSCORE",
                     [key, "3", "2", "WITHSCORES"],
                     restarted_ctx
                   )

          assert ["2.0", nil] =
                   Ferricstore.Commands.SortedSet.handle_ast(
                     {:zmscore, [key, "b", "missing"]},
                     restarted_ctx
                   )

          assert [cursor, scanned] =
                   Ferricstore.Commands.SortedSet.handle_ast(
                     {:zscan, key, 0, []},
                     restarted_ctx
                   )

          assert cursor in ["0", 0]
          assert "b" in scanned
          assert "2.0" in scanned
          assert "c" in scanned
          assert "3.0" in scanned
          refute "a" in scanned
          refute "d" in scanned
        after
        end
      end

      test "list commands survive WARaft restart without shard process reads", %{
        root: root,
        ctx: ctx
      } do
        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "router:list:#{System.unique_integer([:positive])}"

          assert 3 = Ferricstore.Commands.List.handle_ast({:rpush, [key, "a", "b", "c"]}, ctx)

          list_prefix = CompoundKey.list_prefix(key)

          projected_list_rows =
            ctx.keydir_refs
            |> elem(0)
            |> :ets.tab2list()
            |> Enum.filter(fn
              {compound_key, value, _expire_at_ms, _lfu, {tag, _index}, _offset, _value_size}
              when is_binary(value) and
                     tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] ->
                String.starts_with?(compound_key, list_prefix)

              _row ->
                false
            end)

          assert length(projected_list_rows) == 3

          Enum.each(projected_list_rows, fn
            {compound_key, expected, _expire_at_ms, _lfu, file_id, _offset, _value_size} ->
              assert {:ok, value} =
                       Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                         ctx,
                         0,
                         file_id,
                         compound_key
                       )

              assert value == expected
          end)

          assert ["a", "b", "c"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, ctx)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert 3 = Ferricstore.Commands.List.handle_ast({:llen, key}, restarted_ctx)

          assert ["a", "b", "c"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, restarted_ctx)

          assert "a" = Ferricstore.Commands.List.handle_ast({:lpop, key}, restarted_ctx)
          assert 2 = Ferricstore.Commands.List.handle_ast({:llen, key}, restarted_ctx)

          assert ["b", "c"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, restarted_ctx)
        after
        end
      end

      @tag :apply_projection_owner_lifecycle
      test "one shard storage crash preserves another shard apply projection cache", %{
        root: root
      } do
        ctx = build_ctx(Path.join(root, "apply-projection-owner"), shard_count: 2)

        previous_entry_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_entries)

        previous_byte_limit =
          Application.get_env(:ferricstore, :waraft_apply_projection_cache_max_bytes)

        try do
          Application.put_env(
            :ferricstore,
            :waraft_apply_projection_cache_max_entries,
            :infinity
          )

          Application.put_env(
            :ferricstore,
            :waraft_apply_projection_cache_max_bytes,
            :infinity
          )

          clear_apply_projection_cache!()

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          shard0_key = key_for_shard(ctx, 0, "apply-projection-owner")
          shard1_key = key_for_shard(ctx, 1, "apply-projection-owner")

          assert 1 =
                   Ferricstore.Commands.List.handle_ast(
                     {:rpush, [shard0_key, "shard-zero"]},
                     ctx
                   )

          assert 1 =
                   Ferricstore.Commands.List.handle_ast(
                     {:rpush, [shard1_key, "shard-one"]},
                     ctx
                   )

          shard1_prefix = CompoundKey.list_prefix(shard1_key)

          assert [
                   {compound_key, expected, _expire_at_ms, _lfu, file_id, _offset, _value_size}
                 ] =
                   ctx.keydir_refs
                   |> elem(1)
                   |> :ets.tab2list()
                   |> Enum.filter(fn
                     {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, _index},
                      _offset, _value_size} ->
                       String.starts_with?(key, shard1_prefix)

                     _row ->
                       false
                   end)

          table = :ets.whereis(:ferricstore_waraft_apply_projection_cache)
          table_owner = Process.whereis(Ferricstore.Raft.WARaftSegmentReader.TableOwner)
          shard0_storage = :wa_raft_storage.registered_name(:ferricstore_waraft_backend, 1)
          shard0_storage_pid = Process.whereis(shard0_storage)

          assert is_reference(table)
          assert is_pid(table_owner)
          assert :ets.info(table, :owner) == table_owner
          assert is_pid(shard0_storage_pid)

          monitor = Process.monitor(shard0_storage_pid)
          Process.exit(shard0_storage_pid, :kill)
          assert_receive {:DOWN, ^monitor, :process, ^shard0_storage_pid, :killed}, 5_000

          assert table == :ets.whereis(:ferricstore_waraft_apply_projection_cache)
          assert :ets.info(table, :owner) == table_owner

          assert {:ok, ^expected} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     ctx,
                     1,
                     file_id,
                     compound_key
                   )

          assert ["shard-one"] ==
                   Ferricstore.Commands.List.handle_ast({:lrange, shard1_key, 0, -1}, ctx)
        after
          restore_env(:waraft_apply_projection_cache_max_entries, previous_entry_limit)
          restore_env(:waraft_apply_projection_cache_max_bytes, previous_byte_limit)
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      test "advanced list mutations survive WARaft restart", %{root: root, ctx: ctx} do
        key = "router:list-advanced:#{System.unique_integer([:positive])}"
        missing_key = "router:list-advanced:missing:#{System.unique_integer([:positive])}"

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 4 =
                   Ferricstore.Commands.List.handle_ast({:rpush, [key, "a", "b", "c", "d"]}, ctx)

          assert :ok = Ferricstore.Commands.List.handle_ast({:lset, key, 1, "B"}, ctx)

          assert 5 =
                   Ferricstore.Commands.List.handle_ast({:linsert, key, :after, "B", "mid"}, ctx)

          assert 1 = Ferricstore.Commands.List.handle_ast({:lrem, key, 1, "c"}, ctx)
          assert :ok = Ferricstore.Commands.List.handle_ast({:ltrim, key, 1, 2}, ctx)
          assert 3 = Ferricstore.Commands.List.handle_ast({:lpushx, [key, "L"]}, ctx)
          assert 4 = Ferricstore.Commands.List.handle_ast({:rpushx, [key, "R"]}, ctx)
          assert 0 = Ferricstore.Commands.List.handle_ast({:lpushx, [missing_key, "x"]}, ctx)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert ["L", "B", "mid", "R"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, key, 0, -1}, restarted_ctx)

          assert "mid" = Ferricstore.Commands.List.handle_ast({:lindex, key, 2}, restarted_ctx)
          assert 1 = Ferricstore.Commands.List.handle("LPOS", [key, "B"], restarted_ctx)
          assert 0 = Ferricstore.Commands.List.handle_ast({:llen, missing_key}, restarted_ctx)
        after
        end
      end

      test "set commands survive WARaft restart without shard process reads", %{
        root: root,
        ctx: ctx
      } do
        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "router:set:#{System.unique_integer([:positive])}"

          assert 3 = Ferricstore.Commands.Set.handle_ast({:sadd, [key, "a", "b", "c"]}, ctx)
          assert 1 = Ferricstore.Commands.Set.handle_ast({:sismember, key, "b"}, ctx)
          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert 3 = Ferricstore.Commands.Set.handle_ast({:scard, key}, restarted_ctx)
          assert 1 = Ferricstore.Commands.Set.handle_ast({:sismember, key, "b"}, restarted_ctx)

          assert [1, 0, 1] =
                   Ferricstore.Commands.Set.handle_ast(
                     {:smismember, [key, "a", "x", "c"]},
                     restarted_ctx
                   )

          assert ["a", "b", "c"] =
                   {:smembers, key}
                   |> Ferricstore.Commands.Set.handle_ast(restarted_ctx)
                   |> Enum.sort()

          assert 1 = Ferricstore.Commands.Set.handle_ast({:srem, [key, "b"]}, restarted_ctx)
          assert 2 = Ferricstore.Commands.Set.handle_ast({:scard, key}, restarted_ctx)
          assert 0 = Ferricstore.Commands.Set.handle_ast({:sismember, key, "b"}, restarted_ctx)
        after
        end
      end

      test "advanced set store and pop mutations survive WARaft restart", %{root: root, ctx: ctx} do
        suffix = System.unique_integer([:positive])
        left = "router:set-advanced:left:#{suffix}"
        right = "router:set-advanced:right:#{suffix}"
        inter = "router:set-advanced:inter:#{suffix}"
        diff = "router:set-advanced:diff:#{suffix}"
        pop = "router:set-advanced:pop:#{suffix}"

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 3 = Ferricstore.Commands.Set.handle_ast({:sadd, [left, "a", "b", "c"]}, ctx)
          assert 3 = Ferricstore.Commands.Set.handle_ast({:sadd, [right, "b", "c", "d"]}, ctx)
          assert 1 = Ferricstore.Commands.Set.handle_ast({:sadd, [pop, "only"]}, ctx)

          assert 2 =
                   Ferricstore.Commands.Set.handle_ast({:sinterstore, [inter, left, right]}, ctx)

          assert 1 = Ferricstore.Commands.Set.handle_ast({:sdiffstore, [diff, left, right]}, ctx)
          assert "only" = Ferricstore.Commands.Set.handle_ast({:spop, pop}, ctx)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert ["b", "c"] =
                   Ferricstore.Commands.Set.handle_ast({:smembers, inter}, restarted_ctx)
                   |> Enum.sort()

          assert ["a"] =
                   Ferricstore.Commands.Set.handle_ast({:smembers, diff}, restarted_ctx)
                   |> Enum.sort()

          assert 0 = Ferricstore.Commands.Set.handle_ast({:scard, pop}, restarted_ctx)

          assert 2 =
                   Ferricstore.Commands.Set.handle_ast(
                     {:sintercard, [left, right], 0},
                     restarted_ctx
                   )

          assert [cursor, scanned] =
                   Ferricstore.Commands.Set.handle_ast({:sscan, inter, 0, []}, restarted_ctx)

          assert cursor in ["0", 0]
          assert "b" in scanned
          assert "c" in scanned
          refute "a" in scanned
        after
        end
      end

      @tag :crossslot_restart_contract
      test "cross-shard list and set mutations reject without partial state across restart", %{
        root: root
      } do
        ctx = build_ctx(Path.join(root, "compound-cross-shard"), shard_count: 2)

        list_src = key_for_shard(ctx, 0, "router:list-cross:src")
        list_dst = key_for_shard(ctx, 1, "router:list-cross:dst")
        set_src = key_for_shard(ctx, 0, "router:set-cross:src")
        set_dst = key_for_shard(ctx, 1, "router:set-cross:dst")
        set_union_dst = key_for_shard(ctx, 1, "router:set-cross:union")

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [list_src, "a", "b"]}, ctx)
          assert 1 = Ferricstore.Commands.List.handle_ast({:rpush, [list_dst, "x"]}, ctx)

          assert {:error, lmove_error} =
                   Ferricstore.Commands.List.handle_ast(
                     {:lmove, list_src, list_dst, :right, :left},
                     ctx
                   )

          assert lmove_error =~ "CROSSSLOT"

          assert 2 = Ferricstore.Commands.Set.handle_ast({:sadd, [set_src, "a", "b"]}, ctx)
          assert 1 = Ferricstore.Commands.Set.handle_ast({:sadd, [set_dst, "x"]}, ctx)

          assert {:error, smove_error} =
                   Ferricstore.Commands.Set.handle_ast(
                     {:smove, set_src, set_dst, "b"},
                     ctx
                   )

          assert smove_error =~ "CROSSSLOT"

          assert {:error, union_error} =
                   Ferricstore.Commands.Set.handle_ast(
                     {:sunionstore, [set_union_dst, set_src, set_dst]},
                     ctx
                   )

          assert union_error =~ "CROSSSLOT"

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(Path.join(root, "compound-cross-shard"), shard_count: 2)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert ["a", "b"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, list_src, 0, -1}, restarted_ctx)

          assert ["x"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, list_dst, 0, -1}, restarted_ctx)

          assert 1 =
                   Ferricstore.Commands.Set.handle_ast({:sismember, set_src, "b"}, restarted_ctx)

          assert 0 =
                   Ferricstore.Commands.Set.handle_ast({:sismember, set_dst, "b"}, restarted_ctx)

          assert 0 = Ferricstore.Commands.Set.handle_ast({:scard, set_union_dst}, restarted_ctx)
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      @tag :crossslot_restart_contract
      test "blocking list immediate mutations survive WARaft restart", %{root: root} do
        ctx = build_ctx(Path.join(root, "blocking-list"), shard_count: 2)

        blpop_key = key_for_shard(ctx, 0, "router:blocking:blpop")
        brpop_key = key_for_shard(ctx, 0, "router:blocking:brpop")
        move_src = key_for_shard(ctx, 0, "router:blocking:move-src")
        move_dst = key_for_shard(ctx, 0, "router:blocking:move-dst")

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [blpop_key, "a", "b"]}, ctx)
          assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [brpop_key, "c", "d"]}, ctx)
          assert 2 = Ferricstore.Commands.List.handle_ast({:rpush, [move_src, "x", "y"]}, ctx)

          assert [^blpop_key, "a"] =
                   Ferricstore.Commands.Blocking.handle("BLPOP", [blpop_key, "0"], ctx)

          assert [^brpop_key, "d"] =
                   Ferricstore.Commands.Blocking.handle("BRPOP", [brpop_key, "0"], ctx)

          assert "y" =
                   Ferricstore.Commands.Blocking.handle(
                     "BLMOVE",
                     [move_src, move_dst, "RIGHT", "LEFT", "0"],
                     ctx
                   )

          assert [^brpop_key, ["c"]] =
                   Ferricstore.Commands.Blocking.handle(
                     "BLMPOP",
                     ["0", "2", "router:blocking:missing", brpop_key, "LEFT", "COUNT", "1"],
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(Path.join(root, "blocking-list"), shard_count: 2)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert ["b"] =
                   Ferricstore.Commands.List.handle_ast(
                     {:lrange, blpop_key, 0, -1},
                     restarted_ctx
                   )

          assert 0 = Ferricstore.Commands.List.handle_ast({:llen, brpop_key}, restarted_ctx)

          assert ["x"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, move_src, 0, -1}, restarted_ctx)

          assert ["y"] =
                   Ferricstore.Commands.List.handle_ast({:lrange, move_dst, 0, -1}, restarted_ctx)
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      test "geo commands survive WARaft restart without shard process reads", %{
        root: root,
        ctx: ctx
      } do
        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "router:geo:#{System.unique_integer([:positive])}"

          assert 2 =
                   Ferricstore.Commands.Geo.handle_ast(
                     {:geoadd, key, [],
                      [
                        {13.361389, 38.115556, "Palermo"},
                        {15.087269, 37.502669, "Catania"}
                      ]},
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert [[_lng, _lat], [_lng2, _lat2]] =
                   Ferricstore.Commands.Geo.handle_ast(
                     {:geopos, [key, "Palermo", "Catania"]},
                     restarted_ctx
                   )

          assert distance_km_string =
                   Ferricstore.Commands.Geo.handle_ast(
                     {:geodist, key, "Palermo", "Catania", "KM"},
                     restarted_ctx
                   )

          {distance_km, ""} = Float.parse(distance_km_string)
          assert distance_km > 100.0
          assert distance_km < 300.0

          assert ["Palermo", "Catania"] =
                   Ferricstore.Commands.Geo.handle_ast(
                     {:geosearch, key,
                      [
                        center: {:lonlat, 13.5, 38.0},
                        shape: {:radius, 200_000.0},
                        unit: "KM",
                        sort: :asc
                      ]},
                     restarted_ctx
                   )
        after
        end
      end

      test "server KEYS sees WARaft keydir state without shard process reads", %{
        root: root,
        ctx: ctx
      } do
        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, "router:keys:plain", "value", 0)

          assert 1 =
                   Ferricstore.Commands.Hash.handle_ast(
                     {:hset, ["router:keys:hash", "field", "value"]},
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert ["router:keys:hash", "router:keys:plain"] =
                   "KEYS"
                   |> Ferricstore.Commands.Server.handle(["router:keys:*"], restarted_ctx)
                   |> Enum.sort()

          assert 2 = Ferricstore.Commands.Server.handle("DBSIZE", [], restarted_ctx)
        after
        end
      end

      test "server FLUSHDB clears WARaft-backed keys and stays clear after restart", %{
        root: root,
        ctx: ctx
      } do
        suffix = System.unique_integer([:positive])

        plain_key = "router:flush:plain:#{suffix}"
        hash_key = "router:flush:hash:#{suffix}"
        list_key = "router:flush:list:#{suffix}"

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, plain_key, "value", 0)

          assert 1 =
                   Ferricstore.Commands.Hash.handle_ast(
                     {:hset, [hash_key, "field", "value"]},
                     ctx
                   )

          assert 1 = Ferricstore.Commands.List.handle_ast({:lpush, [list_key, "item"]}, ctx)
          assert 3 = Ferricstore.Commands.Server.handle("DBSIZE", [], ctx)

          assert :ok = Ferricstore.Commands.Server.handle("FLUSHDB", [], ctx)
          assert 0 = Ferricstore.Commands.Server.handle("DBSIZE", [], ctx)
          assert nil == Router.get(ctx, plain_key)
          assert 0 = Ferricstore.Commands.Hash.handle_ast({:hlen, hash_key}, ctx)
          assert 0 = Ferricstore.Commands.List.handle_ast({:llen, list_key}, ctx)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert 0 = Ferricstore.Commands.Server.handle("DBSIZE", [], restarted_ctx)
          assert nil == Router.get(restarted_ctx, plain_key)
          assert 0 = Ferricstore.Commands.Hash.handle_ast({:hlen, hash_key}, restarted_ctx)
          assert 0 = Ferricstore.Commands.List.handle_ast({:llen, list_key}, restarted_ctx)
        after
        end
      end

      test "generic key mutations survive WARaft restart", %{root: root, ctx: ctx} do
        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, "router:generic:source", "source-value", 0)
          assert :ok = Router.put(ctx, "router:generic:rename-src", "rename-value", 0)
          assert :ok = Router.put(ctx, "router:generic:unlink", "delete-me", 0)

          assert 1 =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:copy, "router:generic:source", "router:generic:copy", false},
                     ctx
                   )

          assert :ok =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:rename, "router:generic:rename-src", "router:generic:renamed"},
                     ctx
                   )

          assert 0 =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:renamenx, "router:generic:renamed", "router:generic:copy"},
                     ctx
                   )

          assert 1 =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:unlink, ["router:generic:unlink"]},
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "source-value" == Router.get(restarted_ctx, "router:generic:source")
          assert "source-value" == Router.get(restarted_ctx, "router:generic:copy")
          assert nil == Router.get(restarted_ctx, "router:generic:rename-src")
          assert "rename-value" == Router.get(restarted_ctx, "router:generic:renamed")
          assert nil == Router.get(restarted_ctx, "router:generic:unlink")
        after
        end
      end

      @tag :crossslot_restart_contract
      test "cross-shard generic key mutations reject without partial state across restart", %{
        root: root
      } do
        ctx = build_ctx(Path.join(root, "generic-cross-shard"), shard_count: 2)

        source = key_for_shard(ctx, 0, "router:generic-cross:source")
        copy_dest = key_for_shard(ctx, 1, "router:generic-cross:copy")
        rename_src = key_for_shard(ctx, 0, "router:generic-cross:rename-src")
        renamed = key_for_shard(ctx, 1, "router:generic-cross:renamed")

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, source, "source-value", 0)
          assert :ok = Router.put(ctx, rename_src, "rename-value", 0)

          assert {:error, copy_error} =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:copy, source, copy_dest, false},
                     ctx
                   )

          assert copy_error =~ "CROSSSLOT"

          assert {:error, rename_error} =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:rename, rename_src, renamed},
                     ctx
                   )

          assert rename_error =~ "CROSSSLOT"

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(Path.join(root, "generic-cross-shard"), shard_count: 2)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "source-value" == Router.get(restarted_ctx, source)
          assert nil == Router.get(restarted_ctx, copy_dest)
          assert "rename-value" == Router.get(restarted_ctx, rename_src)
          assert nil == Router.get(restarted_ctx, renamed)
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      @tag :crossslot_restart_contract
      test "same-group generic key mutations preserve blob-backed values after WARaft restart",
           %{
             root: root
           } do
        ctx = build_ctx(Path.join(root, "generic-cross-shard-blob"), shard_count: 2)

        copy_source = key_for_shard(ctx, 0, "router:generic-cross-blob:copy-source")
        copy_dest = key_for_shard(ctx, 0, "router:generic-cross-blob:copy-dest")
        rename_source = key_for_shard(ctx, 0, "router:generic-cross-blob:rename-source")
        rename_dest = key_for_shard(ctx, 0, "router:generic-cross-blob:rename-dest")

        copy_payload = :binary.copy("copy-blob-payload", 30_000)
        rename_payload = :binary.copy("rename-blob-payload", 30_000)

        assert byte_size(copy_payload) > ctx.blob_side_channel_threshold_bytes
        assert byte_size(rename_payload) > ctx.blob_side_channel_threshold_bytes

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, copy_source, copy_payload, 0)
          assert :ok = Router.put(ctx, rename_source, rename_payload, 0)

          assert 1 =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:copy, copy_source, copy_dest, false},
                     ctx
                   )

          assert :ok =
                   Ferricstore.Commands.Generic.handle_ast(
                     {:rename, rename_source, rename_dest},
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(Path.join(root, "generic-cross-shard-blob"), shard_count: 2)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert copy_payload == Router.get(restarted_ctx, copy_source)
          assert copy_payload == Router.get(restarted_ctx, copy_dest)
          assert nil == Router.get(restarted_ctx, rename_source)
          assert rename_payload == Router.get(restarted_ctx, rename_dest)

          assert [{_, nil, 0, _lfu, copy_fid, _copy_off, copy_value_size}] =
                   :ets.lookup(elem(restarted_ctx.keydir_refs, 0), copy_dest)

          assert [{_, nil, 0, _lfu, rename_fid, _rename_off, rename_value_size}] =
                   :ets.lookup(elem(restarted_ctx.keydir_refs, 0), rename_dest)

          assert copy_value_size == byte_size(copy_payload)
          assert rename_value_size == byte_size(rename_payload)

          assert {:ok, copy_encoded_ref} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     restarted_ctx,
                     0,
                     copy_fid,
                     copy_dest
                   )

          assert {:ok, rename_encoded_ref} =
                   Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     restarted_ctx,
                     0,
                     rename_fid,
                     rename_dest
                   )

          assert BlobRef.encoded_size?(byte_size(copy_encoded_ref))
          assert BlobRef.encoded_size?(byte_size(rename_encoded_ref))
          assert {:ok, %BlobRef{size: ^copy_value_size}} = BlobRef.decode(copy_encoded_ref)
          assert {:ok, %BlobRef{size: ^rename_value_size}} = BlobRef.decode(rename_encoded_ref)
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
        end
      end

      test "native CAS lock and ratelimit mutations survive WARaft restart", %{
        root: root,
        ctx: ctx
      } do
        suffix = System.unique_integer([:positive])
        cas_key = "router:native:cas:#{suffix}"
        lock_key = "router:native:lock:#{suffix}"
        ratelimit_key = "router:native:rl:#{suffix}"

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, cas_key, "old", 0)

          assert 1 =
                   Ferricstore.Commands.Native.handle_ast(
                     {:cas, cas_key, "old", "new", 60_000},
                     ctx
                   )

          assert :ok =
                   Ferricstore.Commands.Native.handle_ast(
                     {:lock, lock_key, "owner-a", 60_000},
                     ctx
                   )

          assert 1 =
                   Ferricstore.Commands.Native.handle_ast(
                     {:extend, lock_key, "owner-a", 120_000},
                     ctx
                   )

          assert ["allowed", 2, 1, _] =
                   Ferricstore.Commands.Native.handle_ast(
                     {:ratelimit_add, ratelimit_key, 60_000, 3, 2},
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "new" == Router.get(restarted_ctx, cas_key)
          assert Ferricstore.Commands.Expiry.handle_ast({:pttl, cas_key}, restarted_ctx) > 0

          assert {:error, msg} =
                   Ferricstore.Commands.Native.handle_ast(
                     {:unlock, lock_key, "wrong-owner"},
                     restarted_ctx
                   )

          assert msg =~ "DISTLOCK"

          assert 1 =
                   Ferricstore.Commands.Native.handle_ast(
                     {:unlock, lock_key, "owner-a"},
                     restarted_ctx
                   )

          assert nil == Router.get(restarted_ctx, lock_key)

          assert ["denied", 2, 1, _] =
                   Ferricstore.Commands.Native.handle_ast(
                     {:ratelimit_add, ratelimit_key, 60_000, 3, 2},
                     restarted_ctx
                   )
        after
        end
      end

      test "extended string RMW commands survive WARaft restart", %{root: root} do
        ctx = build_ctx(Path.join(root, "strings-rmw"), shard_count: 2)

        getset_key = key_for_shard(ctx, 0, "router:strings-rmw:getset")
        getdel_key = key_for_shard(ctx, 0, "router:strings-rmw:getdel")
        getex_key = key_for_shard(ctx, 0, "router:strings-rmw:getex")
        setrange_key = key_for_shard(ctx, 0, "router:strings-rmw:setrange")
        msetnx_a = key_for_shard(ctx, 0, "router:strings-rmw:msetnx-a")
        msetnx_b = key_for_shard(ctx, 1, "router:strings-rmw:msetnx-b")

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Ferricstore.Commands.Strings.handle_ast({:set, getset_key, "old"}, ctx)

          assert "old" =
                   Ferricstore.Commands.Strings.handle_ast({:getset, getset_key, "new"}, ctx)

          assert :ok =
                   Ferricstore.Commands.Strings.handle_ast({:set, getdel_key, "delete-me"}, ctx)

          assert "delete-me" = Ferricstore.Commands.Strings.handle_ast({:getdel, getdel_key}, ctx)

          assert :ok = Ferricstore.Commands.Strings.handle_ast({:set, getex_key, "ttl-me"}, ctx)

          assert "ttl-me" =
                   Ferricstore.Commands.Strings.handle_ast(
                     {:getex, getex_key, {:px, 60_000}},
                     ctx
                   )

          assert :ok =
                   Ferricstore.Commands.Strings.handle_ast(
                     {:set, setrange_key, "Hello World"},
                     ctx
                   )

          assert 11 =
                   Ferricstore.Commands.Strings.handle_ast(
                     {:setrange, setrange_key, 6, "Redis"},
                     ctx
                   )

          assert {:error, "CROSSSLOT Keys in request don't hash to the same slot"} =
                   Ferricstore.Commands.Strings.handle_ast(
                     {:msetnx, [msetnx_a, "v0", msetnx_b, "v1"]},
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(Path.join(root, "strings-rmw"), shard_count: 2)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "new" == Router.get(restarted_ctx, getset_key)
          assert nil == Router.get(restarted_ctx, getdel_key)
          assert "ttl-me" == Router.get(restarted_ctx, getex_key)
          assert Ferricstore.Commands.Expiry.handle_ast({:pttl, getex_key}, restarted_ctx) > 0
          assert "Hello Redis" == Router.get(restarted_ctx, setrange_key)
          assert nil == Router.get(restarted_ctx, msetnx_a)
          assert nil == Router.get(restarted_ctx, msetnx_b)
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
        end
      end
    end
  end
end
