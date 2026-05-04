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
        put: fn key, val, exp -> raw.put.(ns <> key, val, exp) end,
        delete: fn key -> raw.delete.(ns <> key) end,
        exists?: fn key -> raw.exists?.(ns <> key) end,
        keys: fn -> sandbox_keys(raw, ns) end,
        flush: fn -> sandbox_flush(raw, ns) end,
        dbsize: fn -> length(sandbox_keys(raw, ns)) end,
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
end
