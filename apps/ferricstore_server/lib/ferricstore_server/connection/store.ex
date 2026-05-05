defmodule FerricstoreServer.Connection.Store do
  @moduledoc "Builds the closure-based store map that routes commands to shards, with optional namespace prefixing."

  alias Ferricstore.Store.Router

  @doc false
  @spec build_store(FerricStore.Instance.t(), binary() | nil) :: map()
  def build_store(ctx, nil), do: raw_store(ctx)

  def build_store(ctx, ns) when is_binary(ns) do
    raw = raw_store(ctx)

    %{
      raw
      | get: fn key -> raw.get.(ns <> key) end,
        get_meta: fn key -> raw.get_meta.(ns <> key) end,
        batch_get: fn keys -> raw.batch_get.(namespace_keys(ns, keys)) end,
        value_size: fn key -> raw.value_size.(ns <> key) end,
        object_lfu: fn key -> raw.object_lfu.(ns <> key) end,
        put: fn key, val, exp -> raw.put.(ns <> key, val, exp) end,
        delete: fn key -> raw.delete.(ns <> key) end,
        exists?: fn key -> raw.exists?.(ns <> key) end,
        keys: fn -> sandbox_keys(raw, ns) end,
        flush: fn -> sandbox_flush(raw, ns) end,
        dbsize: fn -> length(sandbox_keys(raw, ns)) end,
        incr: fn key, delta -> raw.incr.(ns <> key, delta) end,
        incr_float: fn key, delta -> raw.incr_float.(ns <> key, delta) end,
        append: fn key, suffix -> raw.append.(ns <> key, suffix) end,
        getset: fn key, value -> raw.getset.(ns <> key, value) end,
        getdel: fn key -> raw.getdel.(ns <> key) end,
        getex: fn key, exp -> raw.getex.(ns <> key, exp) end,
        setrange: fn key, offset, value -> raw.setrange.(ns <> key, offset, value) end,
        cas: fn key, exp, new_val, ttl -> raw.cas.(ns <> key, exp, new_val, ttl) end,
        lock: fn key, owner, ttl -> raw.lock.(ns <> key, owner, ttl) end,
        unlock: fn key, owner -> raw.unlock.(ns <> key, owner) end,
        extend: fn key, owner, ttl -> raw.extend.(ns <> key, owner, ttl) end,
        ratelimit_add: fn key, window, max, count ->
          raw.ratelimit_add.(ns <> key, window, max, count)
        end,
        list_op: fn key, op -> raw.list_op.(ns <> key, op) end,
        compound_get: fn redis_key, compound_key ->
          raw.compound_get.(ns <> redis_key, namespace_compound_key(ns, redis_key, compound_key))
        end,
        compound_get_meta: fn redis_key, compound_key ->
          raw.compound_get_meta.(
            ns <> redis_key,
            namespace_compound_key(ns, redis_key, compound_key)
          )
        end,
        compound_batch_get: fn redis_key, compound_keys ->
          raw.compound_batch_get.(
            ns <> redis_key,
            namespace_compound_keys(ns, redis_key, compound_keys)
          )
        end,
        compound_batch_get_meta: fn redis_key, compound_keys ->
          raw.compound_batch_get_meta.(
            ns <> redis_key,
            namespace_compound_keys(ns, redis_key, compound_keys)
          )
        end,
        compound_put: fn redis_key, compound_key, value, expire_at_ms ->
          raw.compound_put.(
            ns <> redis_key,
            namespace_compound_key(ns, redis_key, compound_key),
            value,
            expire_at_ms
          )
        end,
        compound_delete: fn redis_key, compound_key ->
          raw.compound_delete.(
            ns <> redis_key,
            namespace_compound_key(ns, redis_key, compound_key)
          )
        end,
        compound_scan: fn redis_key, prefix ->
          raw.compound_scan.(ns <> redis_key, namespace_compound_key(ns, redis_key, prefix))
        end,
        compound_count: fn redis_key, prefix ->
          raw.compound_count.(ns <> redis_key, namespace_compound_key(ns, redis_key, prefix))
        end,
        compound_delete_prefix: fn redis_key, prefix ->
          raw.compound_delete_prefix.(
            ns <> redis_key,
            namespace_compound_key(ns, redis_key, prefix)
          )
        end,
        prob_write: fn command -> raw.prob_write.(namespace_prob_command(ns, command)) end,
        prob_dir_for_key: fn key -> raw.prob_dir_for_key.(ns <> key) end,
        flush_prob_dirs: fn -> :ok end
    }
  end

  @doc false
  @spec raw_store(FerricStore.Instance.t()) :: map()
  def raw_store(ctx) do
    case :persistent_term.get({:ferricstore_raw_store, ctx.name}, nil) do
      nil ->
        store = build_raw_store(ctx)
        :persistent_term.put({:ferricstore_raw_store, ctx.name}, store)
        store

      store ->
        store
    end
  end

  @doc false
  @spec build_raw_store(FerricStore.Instance.t()) :: map()
  def build_raw_store(ctx) do
    %{
      get: fn key -> Router.get(ctx, key) end,
      get_meta: fn key -> Router.get_meta(ctx, key) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      value_size: fn key -> Router.value_size(ctx, key) end,
      object_lfu: fn key -> Router.object_lfu(ctx, key) end,
      put: fn key, value, exp -> Router.put(ctx, key, value, exp) end,
      delete: fn key -> Router.delete(ctx, key) end,
      exists?: fn key -> Router.exists?(ctx, key) end,
      keys: fn -> Router.keys(ctx) end,
      flush: fn ->
        for i <- 0..(ctx.shard_count - 1) do
          shard = elem(ctx.shard_names, i)
          keydir = elem(ctx.keydir_refs, i)

          raw_keys =
            try do
              :ets.foldl(fn {key, _, _, _, _, _, _}, acc -> [key | acc] end, [], keydir)
            rescue
              ArgumentError -> []
            end

          Enum.each(raw_keys, fn key ->
            try do
              GenServer.call(shard, {:delete, key}, 10_000)
            catch
              :exit, _ -> :ok
            end
          end)
        end

        :ok
      end,
      dbsize: fn -> Router.dbsize(ctx) end,
      incr: fn key, delta -> Router.incr(ctx, key, delta) end,
      incr_float: fn key, delta -> Router.incr_float(ctx, key, delta) end,
      append: fn key, suffix -> Router.append(ctx, key, suffix) end,
      getset: fn key, value -> Router.getset(ctx, key, value) end,
      getdel: fn key -> Router.getdel(ctx, key) end,
      getex: fn key, exp -> Router.getex(ctx, key, exp) end,
      setrange: fn key, offset, value -> Router.setrange(ctx, key, offset, value) end,
      cas: fn key, exp, new_val, ttl -> Router.cas(ctx, key, exp, new_val, ttl) end,
      lock: fn key, owner, ttl -> Router.lock(ctx, key, owner, ttl) end,
      unlock: fn key, owner -> Router.unlock(ctx, key, owner) end,
      extend: fn key, owner, ttl -> Router.extend(ctx, key, owner, ttl) end,
      ratelimit_add: fn key, window, max, count ->
        Router.ratelimit_add(ctx, key, window, max, count)
      end,
      list_op: fn key, op -> Router.list_op(ctx, key, op) end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        Router.compound_batch_get(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        Router.compound_batch_get_meta(ctx, redis_key, compound_keys)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard = elem(ctx.shard_names, Router.shard_for(ctx, redis_key))
        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end,
      # prob_dir_for_key resolves the correct shard's prob directory.
      # Used by command handlers to compute file paths for reads.
      prob_dir_for_key: fn key ->
        idx = Router.shard_for(ctx, key)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        Path.join(shard_path, "prob")
      end,
      flush_prob_dirs: fn -> Ferricstore.ProbCleanup.flush_all(ctx.data_dir, ctx.shard_count) end,
      on_push: &Ferricstore.Waiters.notify_push/1
    }
  end

  defp sandbox_keys(raw, ns) do
    prefix_size = byte_size(ns)

    raw.keys.()
    |> Enum.filter(&String.starts_with?(&1, ns))
    |> Enum.map(&binary_part(&1, prefix_size, byte_size(&1) - prefix_size))
  end

  defp sandbox_flush(raw, ns) do
    raw.keys.()
    |> Enum.filter(&String.starts_with?(&1, ns))
    |> Enum.each(fn key ->
      _ = raw.delete.(key)
      :ok
    end)

    :ok
  end

  defp namespace_keys(ns, keys), do: Enum.map(keys, &(ns <> &1))

  defp namespace_compound_keys(ns, redis_key, compound_keys),
    do: Enum.map(compound_keys, &namespace_compound_key(ns, redis_key, &1))

  defp namespace_compound_key(ns, redis_key, compound_key) when is_binary(compound_key) do
    Enum.find_value(["LM:", "T:", "H:", "L:", "S:", "Z:"], compound_key, fn prefix ->
      raw_prefix = prefix <> redis_key

      if String.starts_with?(compound_key, raw_prefix) do
        rest =
          binary_part(
            compound_key,
            byte_size(prefix),
            byte_size(compound_key) - byte_size(prefix)
          )

        prefix <> ns <> rest
      end
    end)
  end

  defp namespace_prob_command(ns, {:bloom_create, key, bits, hashes, meta}),
    do: {:bloom_create, ns <> key, bits, hashes, meta}

  defp namespace_prob_command(ns, {:bloom_add, key, element, auto_params}),
    do: {:bloom_add, ns <> key, element, auto_params}

  defp namespace_prob_command(ns, {:bloom_madd, key, elements, auto_params}),
    do: {:bloom_madd, ns <> key, elements, auto_params}

  defp namespace_prob_command(ns, {:cms_create, key, width, depth}),
    do: {:cms_create, ns <> key, width, depth}

  defp namespace_prob_command(ns, {:cms_incrby, key, pairs}),
    do: {:cms_incrby, ns <> key, pairs}

  defp namespace_prob_command(ns, {:cms_merge, dst_key, src_keys, weights, create_params}),
    do: {:cms_merge, ns <> dst_key, namespace_keys(ns, src_keys), weights, create_params}

  defp namespace_prob_command(ns, {:cuckoo_create, key, capacity, bucket_size}),
    do: {:cuckoo_create, ns <> key, capacity, bucket_size}

  defp namespace_prob_command(ns, {:cuckoo_add, key, element, auto_params}),
    do: {:cuckoo_add, ns <> key, element, auto_params}

  defp namespace_prob_command(ns, {:cuckoo_addnx, key, element, auto_params}),
    do: {:cuckoo_addnx, ns <> key, element, auto_params}

  defp namespace_prob_command(ns, {:cuckoo_del, key, element}),
    do: {:cuckoo_del, ns <> key, element}

  defp namespace_prob_command(ns, {:topk_create, key, k, width, depth, decay}),
    do: {:topk_create, ns <> key, k, width, depth, decay}

  defp namespace_prob_command(ns, {:topk_add, key, elements}),
    do: {:topk_add, ns <> key, elements}

  defp namespace_prob_command(ns, {:topk_incrby, key, pairs}),
    do: {:topk_incrby, ns <> key, pairs}

  defp namespace_prob_command(_ns, command), do: command
end
