defmodule Ferricstore.Store.Router.Part09 do
  @moduledoc false

  # Extracted from Router: json_numincrby .. compound_get_from_keydir
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.ErrorReasons
      alias Ferricstore.HLC
      alias Ferricstore.HyperLogLog, as: HLL
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Raft.ReplyAwaiter
      alias Ferricstore.Stats
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.CompoundCommand
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry
      @doc false
      def json_numincrby(ctx, key, path, increment)
          when is_binary(key) and (is_binary(path) or is_list(path)) and is_number(increment) do
        forced_single_key_quorum(ctx, key, {:json_numincrby, key, path, increment})
      end

      @doc false
      def json_arrappend(ctx, key, path, values)
          when is_binary(key) and (is_binary(path) or is_list(path)) and is_list(values) do
        forced_single_key_quorum(ctx, key, {:json_arrappend, key, path, values})
      end

      @doc false
      def json_toggle(ctx, key, path)
          when is_binary(key) and (is_binary(path) or is_list(path)) do
        forced_single_key_quorum(ctx, key, {:json_toggle, key, path})
      end

      @doc false
      def json_clear(ctx, key, path) when is_binary(key) and (is_binary(path) or is_list(path)) do
        forced_single_key_quorum(ctx, key, {:json_clear, key, path})
      end

      defp forced_single_key_quorum(ctx, key, command) do
        idx = shard_for(ctx, key)
        forced_quorum_write(ctx, idx, command, node())
      end

      @hll_wrongtype_error {:error,
                            "WRONGTYPE Operation against a key holding the wrong kind of value"}

      defp hll_read_sketches(ctx, keys) do
        with :ok <- hll_ensure_string_keys(ctx, keys) do
          ctx
          |> batch_get(keys)
          |> Enum.map(fn
            nil -> HLL.new()
            value -> value
          end)
          |> hll_validate_sketches()
        end
      end

      defp hll_ensure_string_keys(ctx, keys) do
        Enum.reduce_while(keys, :ok, fn key, :ok ->
          if hll_compound_data_structure_key?(ctx, key) do
            {:halt, @hll_wrongtype_error}
          else
            {:cont, :ok}
          end
        end)
      end

      defp hll_compound_data_structure_key?(ctx, key) do
        compound_get(ctx, key, CompoundKey.type_key(key)) != nil and
          TypeRegistry.get_type(key, ctx) != "none"
      end

      defp hll_validate_sketches(sketches) do
        Enum.reduce_while(sketches, {:ok, []}, fn sketch, {:ok, acc} ->
          if HLL.valid_sketch?(sketch) do
            {:cont, {:ok, [sketch | acc]}}
          else
            {:halt, @hll_wrongtype_error}
          end
        end)
        |> case do
          {:ok, values} -> {:ok, Enum.reverse(values)}
          {:error, _} = err -> err
        end
      end

      @doc """
      Atomically applies Redis SET options in Raft order.

      Unlike `put/4`, this keeps NX/XX/GET/KEEPTTL checks inside the state
      machine so concurrent conditional SETs serialize correctly.
      """
      @spec set(FerricStore.Instance.t(), binary(), binary(), map()) :: term()
      def set(ctx, key, value, opts) do
        cond do
          byte_size(key) > @max_key_size ->
            {:error, "ERR key too large (max #{@max_key_size} bytes)"}

          is_binary(value) and byte_size(value) >= @max_value_size ->
            {:error, "ERR value too large (max #{@max_value_size} bytes)"}

          true ->
            case check_keydir_full_for_set(ctx, key, opts) do
              :ok ->
                idx = shard_for(ctx, key)
                raft_write(ctx, idx, key, {:set, key, value, opts.expire_at_ms, opts})

              {:error, _} = err ->
                err
            end
        end
      end

      # Checks if the keydir is full. If so, only allows writes to existing keys.
      # Checks both `keydir_full?` (ETS-level memory guard) and `reject_writes?`
      # (noeviction policy with reject-level pressure). The Shard GenServer has its
      # own `reject_writes?` check in `handle_call({:put, ...})`, but when the
      # quorum bypass path is used, the Shard is skipped, so we must check here.
      # Reads from ctx.pressure_flags atomics instead of persistent_term.
      defp check_keydir_full(ctx, key) do
        keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
        reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

        if keydir_full or reject_writes do
          # Allow updates to existing keys — use ETS direct check
          if exists_fast?(ctx, key) do
            :ok
          else
            # Nudge MemoryGuard to run eviction immediately (async, non-blocking).
            # Without this, the next eviction cycle is up to 100ms away.
            Ferricstore.MemoryGuard.nudge()
            {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
          end
        else
          :ok
        end
      end

      defp check_keydir_full_for_set(ctx, key, opts) do
        keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
        reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

        if keydir_full or reject_writes do
          existing? = exists_fast?(ctx, key)

          cond do
            existing? ->
              :ok

            opts.xx ->
              :ok

            true ->
              Ferricstore.MemoryGuard.nudge()
              {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
          end
        else
          :ok
        end
      end

      defp flow_create_many_admission_rejected?(ctx, attrs_list, partition_key) do
        flow_create_admission_pressure?(ctx) and
          Enum.any?(attrs_list, fn
            %{id: id} = attrs when is_binary(id) ->
              key =
                Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key, partition_key))

              not exists_fast?(ctx, key)

            _ ->
              false
          end)
      end

      defp flow_create_admission_rejected?(ctx, key) do
        flow_create_admission_pressure?(ctx) and not exists_fast?(ctx, key)
      end

      defp flow_create_admission_pressure?(ctx) do
        Ferricstore.Flow.Admission.reject_new_creates?() or
          :atomics.get(ctx.pressure_flags, 1) == 1 or :atomics.get(ctx.pressure_flags, 2) == 1 or
          Ferricstore.OperationalGuard.reject_flow_creates?()
      rescue
        _ ->
          Ferricstore.Flow.Admission.reject_new_creates?() or
            Ferricstore.MemoryGuard.reject_writes?() or
            Ferricstore.OperationalGuard.reject_flow_creates?()
      end

      defp flow_create_overloaded_error(count \\ 1) do
        Ferricstore.MemoryGuard.nudge()
        status = Ferricstore.Flow.Admission.status()

        :telemetry.execute(
          [:ferricstore, :flow, :create, :rejected],
          %{count: flow_create_rejected_count(count)},
          %{reason: status.reason, retry_after_ms: status.retry_after_ms}
        )

        Ferricstore.Flow.Admission.overload_error(:memory_guard, 1_000)
      end

      defp flow_create_rejected_count(count) when is_integer(count) and count > 0, do: count
      defp flow_create_rejected_count(_count), do: 0

      @doc "Deletes `key`. Returns `:ok` whether or not the key existed."
      @spec delete(FerricStore.Instance.t(), binary()) :: :ok
      def delete(ctx, key) do
        idx = shard_for(ctx, key)

        raft_write(ctx, idx, key, {:delete, key})
      end

      @doc """
      Submits a server command through Raft for replication to all nodes.

      Server commands are opaque to the library — the state machine dispatches
      them via the `raft_apply_hook` callback on the Instance struct. Routed
      through shard 0 for consistent ordering.
      """
      @spec server_command(FerricStore.Instance.t(), term()) :: term()
      def server_command(ctx, command) do
        raft_write(ctx, 0, "__server__", {:server_command, command})
      end

      @doc """
      Routes a probabilistic data structure write command through Raft.
      """
      @spec prob_write(FerricStore.Instance.t(), tuple()) :: term()
      def prob_write(ctx, command) do
        key = extract_prob_key(command)
        idx = shard_for(ctx, key)
        raft_write(ctx, idx, key, command)
      end

      defp extract_prob_key({:bloom_create, key, _, _, _}), do: key
      defp extract_prob_key({:bloom_add, key, _, _}), do: key
      defp extract_prob_key({:bloom_madd, key, _, _}), do: key
      defp extract_prob_key({:cms_create, key, _, _}), do: key
      defp extract_prob_key({:cms_incrby, key, _}), do: key
      defp extract_prob_key({:cms_merge, dst_key, _, _, _}), do: dst_key
      defp extract_prob_key({:cuckoo_create, key, _, _}), do: key
      defp extract_prob_key({:cuckoo_add, key, _, _}), do: key
      defp extract_prob_key({:cuckoo_addnx, key, _, _}), do: key
      defp extract_prob_key({:cuckoo_del, key, _}), do: key
      defp extract_prob_key({:topk_create, key, _, _, _, _}), do: key
      defp extract_prob_key({:topk_add, key, _}), do: key
      defp extract_prob_key({:topk_incrby, key, _}), do: key

      @doc """
      Returns `true` if `key` exists and is not expired.

      Uses direct ETS lookup (no GenServer roundtrip) for hot and cold keys.
      A key is considered existing if it is in the keydir and not expired,
      regardless of whether its value is hot (in ETS) or cold (on disk only).
      """
      @spec exists?(FerricStore.Instance.t(), binary()) :: boolean()
      def exists?(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()

        try do
          case :ets.lookup(keydir, key) do
            [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
              true

            [{^key, nil, 0, _lfu, fid, off, vsize}] when readable_cold_ref?(fid, off, vsize) ->
              true

            [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
              true

            [{^key, nil, exp, _lfu, fid, off, vsize}]
            when exp > now and readable_cold_ref?(fid, off, vsize) ->
              true

            [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
              track_keydir_binary_delete(ctx, idx, keydir, key)
              :ets.delete(keydir, key)
              false

            [] ->
              false
          end
        rescue
          ArgumentError -> keydir_unavailable(ctx, idx, :exists, false)
        end
      end

      @doc """
      Fast ETS-direct existence check for a key.

      Returns `true` if the key exists in ETS and is not expired, `false` otherwise.
      This bypasses the GenServer entirely, saving ~1-3us per call. Used in the
      hot write path (`check_keydir_full/2`) where we only need a boolean answer
      and can tolerate the fact that cold keys (value=nil but still in keydir)
      are correctly detected as existing.
      """
      @spec exists_fast?(FerricStore.Instance.t(), binary()) :: boolean()
      def exists_fast?(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()

        try do
          case :ets.lookup(keydir, key) do
            [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
              true

            [{^key, nil, 0, _lfu, fid, off, vsize}] when readable_cold_ref?(fid, off, vsize) ->
              true

            [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
              true

            [{^key, nil, exp, _lfu, fid, off, vsize}]
            when exp > now and readable_cold_ref?(fid, off, vsize) ->
              true

            [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
              track_keydir_binary_delete(ctx, idx, keydir, key)
              :ets.delete(keydir, key)
              false

            [] ->
              false
          end
        rescue
          ArgumentError -> false
        end
      end

      @doc """
      Atomically increments the integer value of `key` by `delta`.

      If the key does not exist, it is set to `delta`. Returns `{:ok, new_integer}`
      on success or `{:error, reason}` if the value is not a valid integer.
      """
      @spec incr(FerricStore.Instance.t(), binary(), integer()) ::
              {:ok, integer()} | {:error, binary()}
      def incr(ctx, key, delta) do
        raft_write(ctx, shard_for(ctx, key), key, {:incr, key, delta})
      end

      @doc """
      Atomically increments the float value of `key` by `delta`.

      If the key does not exist, it is set to `delta`. Returns `{:ok, new_float_string}`
      on success or `{:error, reason}` if the value is not a valid float.
      """
      @spec incr_float(FerricStore.Instance.t(), binary(), float()) ::
              {:ok, binary()} | {:error, binary()}
      def incr_float(ctx, key, delta) do
        raft_write(ctx, shard_for(ctx, key), key, {:incr_float, key, delta})
      end

      @doc """
      Atomically appends `suffix` to the value of `key`.

      If the key does not exist, it is created with value `suffix`.
      Returns `{:ok, new_byte_length}`.
      """
      @spec append(FerricStore.Instance.t(), binary(), binary()) :: {:ok, non_neg_integer()}
      def append(ctx, key, suffix) do
        raft_write(ctx, shard_for(ctx, key), key, {:append, key, suffix})
      end

      @doc """
      Atomically gets the old value and sets a new value for `key`.

      Returns the old value, or `nil` if the key did not exist.
      """
      @spec getset(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
      def getset(ctx, key, value) do
        raft_write(ctx, shard_for(ctx, key), key, {:getset, key, value})
      end

      @doc """
      Atomically gets and deletes `key`.

      Returns the value, or `nil` if the key did not exist.
      """
      @spec getdel(FerricStore.Instance.t(), binary()) :: binary() | nil
      def getdel(ctx, key) do
        raft_write(ctx, shard_for(ctx, key), key, {:getdel, key})
      end

      @doc """
      Atomically gets the value and updates the expiry of `key`.

      `expire_at_ms` is an absolute Unix-epoch timestamp in milliseconds;
      pass `0` to persist (remove expiry). Returns the value, or `nil` if
      the key did not exist.
      """
      @spec getex(FerricStore.Instance.t(), binary(), non_neg_integer()) :: binary() | nil
      def getex(ctx, key, expire_at_ms) do
        raft_write(ctx, shard_for(ctx, key), key, {:getex, key, expire_at_ms})
      end

      @doc """
      Atomically overwrites part of the string at `key` starting at `offset`.

      Zero-pads if the key doesn't exist or the string is shorter than offset.
      Returns `{:ok, new_byte_length}`.
      """
      @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) ::
              {:ok, non_neg_integer()}
      def setrange(ctx, key, offset, value) do
        raft_write(ctx, shard_for(ctx, key), key, {:setrange, key, offset, value})
      end

      @doc """
      Atomically sets the bit at `offset` to `bit_val` (0 or 1). Returns the
      previous bit value (0 or 1). Extends the bitmap with zero bytes if
      necessary. Goes through Raft so concurrent SETBITs on the same key
      never lose updates — the state machine is the sole mutator.
      """
      @spec setbit(FerricStore.Instance.t(), binary(), non_neg_integer(), 0 | 1) :: 0 | 1
      def setbit(ctx, key, offset, bit_val) do
        raft_write(ctx, shard_for(ctx, key), key, {:setbit, key, offset, bit_val})
      end

      @doc """
      Atomically increments the integer value of hash field `field` in `key` by
      `delta`. Returns `{:ok, new_int}` or `{:error, reason}`. Shares ordering
      with the parent hash's shard (routes by the hash's redis_key).
      """
      @spec hincrby(FerricStore.Instance.t(), binary(), binary(), integer()) ::
              integer() | {:error, binary()}
      def hincrby(ctx, key, field, delta) do
        raft_write(ctx, shard_for(ctx, key), key, {:hincrby, key, field, delta})
      end

      @doc """
      Atomically increments the float value of hash field `field` in `key` by
      `delta`. Returns the new value as a string, or `{:error, reason}`.
      """
      @spec hincrbyfloat(FerricStore.Instance.t(), binary(), binary(), float()) ::
              binary() | {:error, binary()}
      def hincrbyfloat(ctx, key, field, delta) do
        raft_write(ctx, shard_for(ctx, key), key, {:hincrbyfloat, key, field, delta})
      end

      @doc """
      Atomically increments the score of `member` in the sorted set at `key` by
      `increment`. Returns the new score as a string.
      """
      @spec zincrby(FerricStore.Instance.t(), binary(), number(), binary()) ::
              binary() | {:error, binary()}
      def zincrby(ctx, key, increment, member) do
        raft_write(ctx, shard_for(ctx, key), key, {:zincrby, key, increment, member})
      end

      @doc "Returns all live (non-expired, non-deleted) keys across every shard."
      @spec keys(FerricStore.Instance.t()) :: [binary()]
      def keys(ctx) do
        if selected_waraft_ctx?(ctx) do
          waraft_live_keys(ctx)
        else
          shard_live_keys(ctx)
        end
      end

      defp shard_live_keys(ctx) do
        sc = ctx.shard_count

        Enum.flat_map(0..(sc - 1), fn i ->
          case safe_read_call(ctx, i, :keys) do
            {:ok, keys} -> keys
            :unavailable -> []
          end
        end)
      end

      defp waraft_live_keys(ctx) do
        sc = ctx.shard_count
        now = HLC.now_ms()

        Enum.flat_map(0..(sc - 1), fn i ->
          live_keydir_keys(ctx, i, resolve_keydir(ctx, i), now)
        end)
      end

      defp live_keydir_keys(ctx, idx, keydir, now) do
        {live_keys, expired_keys} =
          :ets.foldl(
            fn
              {key, value, 0, _lfu, _fid, _off, _vsize}, {live, expired} when value != nil ->
                {[key | live], expired}

              {key, nil, 0, _lfu, fid, off, vsize}, {live, expired}
              when readable_cold_ref?(fid, off, vsize) ->
                {[key | live], expired}

              {key, value, exp, _lfu, _fid, _off, _vsize}, {live, expired}
              when exp > now and value != nil ->
                {[key | live], expired}

              {key, nil, exp, _lfu, fid, off, vsize}, {live, expired}
              when exp > now and readable_cold_ref?(fid, off, vsize) ->
                {[key | live], expired}

              {key, _value, _exp, _lfu, _fid, _off, _vsize}, {live, expired} ->
                {live, [key | expired]}
            end,
            {[], []},
            keydir
          )

        Enum.each(expired_keys, fn key ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
        end)

        live_keys
      rescue
        ArgumentError ->
          keydir_unavailable(ctx, idx, :keys, [])
      end

      @doc "Returns the count of all live keys across every shard."
      @spec dbsize(FerricStore.Instance.t()) :: non_neg_integer()
      def dbsize(ctx) do
        sc = ctx.shard_count
        now = HLC.now_ms()

        Enum.reduce(0..(sc - 1), 0, fn i, acc ->
          acc + live_keydir_size(ctx, i, resolve_keydir(ctx, i), now)
        end)
      end

      defp live_keydir_size(ctx, idx, keydir, now) do
        {count, expired_keys} =
          :ets.foldl(
            fn
              {_key, _value, 0, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
                {count + 1, expired_keys}

              {_key, _value, exp, _lfu, _fid, _off, _vsize}, {count, expired_keys}
              when exp > now ->
                {count + 1, expired_keys}

              {key, _value, _exp, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
                {count, [key | expired_keys]}
            end,
            {0, []},
            keydir
          )

        Enum.each(expired_keys, fn key ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
        end)

        count
      rescue
        ArgumentError ->
          keydir_unavailable(ctx, idx, :dbsize, 0)
      end

      defp keydir_unavailable(ctx, idx, request, fallback) do
        emit_shard_unavailable(ctx, idx, request, :keydir_unavailable)
        fallback
      end

      @doc """
      Returns the current write version of the shard that owns `key`.

      Used by the WATCH/EXEC transaction mechanism to detect concurrent modifications.
      """
      @spec get_version(FerricStore.Instance.t(), binary()) :: non_neg_integer()
      def get_version(ctx, key) do
        idx = shard_for(ctx, key)

        if selected_waraft_ctx?(ctx) do
          shared_write_version(ctx, idx)
        else
          case safe_read_call(ctx, idx, {:get_version, key}) do
            {:ok, version} -> version
            :unavailable -> shared_write_version(ctx, idx)
          end
        end
      end

      defp shared_write_version(%{write_version: write_version}, idx) do
        size = :counters.info(write_version).size
        if idx < size, do: :counters.get(write_version, idx + 1), else: 0
      rescue
        _ -> 0
      end

      @doc """
      Returns a lightweight WATCH token for `key`.

      Hot keys use the value hash plus their live Bitcask location. Cold keys use
      their live keydir location and expiry, avoiding a large Bitcask read just to
      snapshot WATCH state. Pending entries fall back to the shard write version.
      """
      @spec watch_token(FerricStore.Instance.t(), binary()) :: term()
      def watch_token(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()

        try do
          case :ets.lookup(keydir, key) do
            [{^key, value, 0, _lfu, fid, off, vsize}]
            when value != nil and valid_cold_location(fid, off, vsize) ->
              {:hot, :erlang.phash2(value), fid, off, vsize, 0}

            [{^key, value, 0, _lfu, fid, off, vsize}]
            when value != nil and valid_waraft_segment_location(fid, off, vsize) ->
              {:hot, :erlang.phash2(value), fid, off, vsize, 0}

            [{^key, value, 0, _lfu, :pending, _off, _vsize}] when value != nil ->
              {:version, get_version(ctx, key)}

            [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
              {:cold, fid, off, vsize, 0}

            [{^key, nil, 0, _lfu, fid, off, vsize}]
            when valid_waraft_segment_location(fid, off, vsize) ->
              {:cold, fid, off, vsize, 0}

            [{^key, nil, 0, _lfu, :pending, _off, _vsize}] ->
              {:version, get_version(ctx, key)}

            [{^key, value, exp, _lfu, fid, off, vsize}]
            when exp > now and value != nil and valid_cold_location(fid, off, vsize) ->
              {:hot, :erlang.phash2(value), fid, off, vsize, exp}

            [{^key, value, exp, _lfu, fid, off, vsize}]
            when exp > now and value != nil and valid_waraft_segment_location(fid, off, vsize) ->
              {:hot, :erlang.phash2(value), fid, off, vsize, exp}

            [{^key, value, exp, _lfu, :pending, _off, _vsize}] when exp > now and value != nil ->
              {:version, get_version(ctx, key)}

            [{^key, nil, exp, _lfu, fid, off, vsize}]
            when exp > now and valid_cold_location(fid, off, vsize) ->
              {:cold, fid, off, vsize, exp}

            [{^key, nil, exp, _lfu, fid, off, vsize}]
            when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
              {:cold, fid, off, vsize, exp}

            [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when exp > now ->
              {:version, get_version(ctx, key)}

            [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
              track_keydir_binary_delete(ctx, idx, keydir, key)
              :ets.delete(keydir, key)
              :missing

            [] ->
              :missing
          end
        rescue
          ArgumentError -> {:version, get_version(ctx, key)}
        end
      end

      @doc """
      Returns the keydir disk location for a key, or `:miss`.

      Reads the `{file_id, offset, value_size}` fields directly from the keydir
      ETS table without a GenServer roundtrip. Returns `{:ok, {fid, off, vsize}}`
      for live keys, or `:miss` if the key is not in the keydir or is expired.

      Used by sendfile zero-copy and STRLEN on cold keys.
      """
      @spec get_keydir_file_ref(FerricStore.Instance.t(), binary()) ::
              {:ok, {term(), non_neg_integer(), non_neg_integer()}} | :miss
      def get_keydir_file_ref(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()

        try do
          case :ets.lookup(keydir, key) do
            [{_, _, 0, _, fid, off, vsize}] when readable_cold_ref?(fid, off, vsize) ->
              {:ok, {fid, off, vsize}}

            [{^key, nil, 0, _, fid, _off, _vsize}] when is_integer(fid) ->
              track_keydir_binary_delete(ctx, idx, keydir, key)
              :ets.delete(keydir, key)
              :miss

            [{_, _, 0, _, _fid, _off, _vsize}] ->
              :miss

            [{_, _, exp, _, fid, off, vsize}]
            when exp > now and readable_cold_ref?(fid, off, vsize) ->
              {:ok, {fid, off, vsize}}

            [{^key, nil, exp, _, fid, _off, _vsize}] when exp > now and is_integer(fid) ->
              track_keydir_binary_delete(ctx, idx, keydir, key)
              :ets.delete(keydir, key)
              :miss

            [{_, _, exp, _, _fid, _off, _vsize}] when exp > now ->
              :miss

            [{^key, _, _exp, _, _fid, _off, _vsize}] ->
              track_keydir_binary_delete(ctx, idx, keydir, key)
              :ets.delete(keydir, key)
              :miss

            [] ->
              :miss
          end
        rescue
          ArgumentError -> :miss
        end
      end

      # -------------------------------------------------------------------
      # Native command accessors
      # -------------------------------------------------------------------

      @spec cas(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer() | nil) ::
              1 | 0 | nil
      def cas(ctx, key, expected, new_value, ttl_ms) do
        expire_at_ms = if ttl_ms, do: HLC.now_ms() + ttl_ms, else: nil
        raft_write(ctx, shard_for(ctx, key), key, {:cas, key, expected, new_value, expire_at_ms})
      end

      @spec lock(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
              :ok | {:error, binary()}
      def lock(ctx, key, owner, ttl_ms) do
        expire_at_ms = HLC.now_ms() + ttl_ms
        raft_write(ctx, shard_for(ctx, key), key, {:lock, key, owner, expire_at_ms})
      end

      @spec unlock(FerricStore.Instance.t(), binary(), binary()) :: 1 | {:error, binary()}
      def unlock(ctx, key, owner) do
        raft_write(ctx, shard_for(ctx, key), key, {:unlock, key, owner})
      end

      @spec extend(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
              1 | {:error, binary()}
      def extend(ctx, key, owner, ttl_ms) do
        expire_at_ms = HLC.now_ms() + ttl_ms
        raft_write(ctx, shard_for(ctx, key), key, {:extend, key, owner, expire_at_ms})
      end

      @spec ratelimit_add(
              FerricStore.Instance.t(),
              binary(),
              pos_integer(),
              pos_integer(),
              pos_integer()
            ) :: [term()]
      def ratelimit_add(ctx, key, window_ms, max, count) do
        raft_write(
          ctx,
          shard_for(ctx, key),
          key,
          {:ratelimit_add, key, window_ms, max, count}
        )
      end

      # -------------------------------------------------------------------
      # Compound key operations
      # -------------------------------------------------------------------

      @spec compound_get(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
      def compound_get(ctx, redis_key, compound_key) do
        idx = shard_for(ctx, redis_key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()

        if promoted_data_compound_key?(keydir, redis_key, compound_key, now) do
          fallback_compound_get(ctx, idx, redis_key, compound_key)
        else
          compound_get_from_keydir(ctx, idx, keydir, redis_key, compound_key, now)
        end
      end

      defp compound_get_from_keydir(ctx, idx, keydir, redis_key, compound_key, now) do
        case ets_get_full(ctx, idx, keydir, compound_key, now) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
            value

          {:cold, file_id, offset, value_size}
          when valid_waraft_segment_location(file_id, offset, value_size) ->
            case read_waraft_segment_materialized(ctx, idx, file_id, compound_key) do
              {:ok, value} ->
                Stats.record_cold_read(ctx, compound_key)
                warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
                value

              _ ->
                fallback_compound_get(ctx, idx, redis_key, compound_key)
            end

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            case read_cold_materialized(ctx, idx, path, offset, compound_key) do
              {:ok, value} when is_binary(value) ->
                Stats.record_cold_read(ctx, compound_key)
                warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
                value

              _ ->
                retry_or_fallback_compound_get(
                  ctx,
                  idx,
                  keydir,
                  redis_key,
                  compound_key,
                  {file_id, offset, value_size},
                  now
                )
            end

          _ ->
            fallback_compound_get(ctx, idx, redis_key, compound_key)
        end
      end
    end
  end
end
