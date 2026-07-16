defmodule Ferricstore.Store.Router.Part09 do
  @moduledoc false

  # Extracted from Router: forced_single_key_quorum .. compound_get_from_keydir
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.ErrorReasons
      alias Ferricstore.HLC
      alias Ferricstore.HyperLogLog, as: HLL
      alias Ferricstore.Flow.InternalKey
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.FetchOrCompute.Outcome, as: FetchOrComputeOutcome
      alias Ferricstore.Raft.ReplyAwaiter
      alias Ferricstore.Stats
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.CompoundCommand
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.ReadResult
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry
      alias Ferricstore.Store.Shard.LogicalKeyIndex
      alias Ferricstore.TermCodec

      @max_logical_scan_cursor_bytes 88_000

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
        with :ok <- validate_string_write(ctx, key, value) do
          idx = shard_for(ctx, key)

          cond do
            Ferricstore.Store.DiskPressure.under_pressure?(ctx, idx) ->
              {:error, "ERR disk pressure on shard #{idx}, rejecting write"}

            true ->
              case check_keydir_full_for_set(ctx, key, opts) do
                :ok ->
                  command = {:set, key, value, opts.expire_at_ms, opts}

                  if durable_raft_ctx?(ctx) do
                    raft_write(ctx, idx, key, command)
                  else
                    GenServer.call(elem(ctx.shard_names, idx), {:standalone_commit, command})
                  end

                {:error, _} = err ->
                  err
              end
          end
        end
      end

      @doc false
      @spec expire_if_batch(
              FerricStore.Instance.t(),
              non_neg_integer(),
              [{binary(), pos_integer()}]
            ) :: [boolean()] | {:error, term()}
      def expire_if_batch(_ctx, _shard_index, []), do: []

      def expire_if_batch(ctx, shard_index, entries)
          when is_integer(shard_index) and shard_index >= 0 and is_list(entries) do
        valid? =
          shard_index < ctx.shard_count and
            Enum.all?(entries, fn
              {key, expire_at_ms}
              when is_binary(key) and is_integer(expire_at_ms) and
                     expire_at_ms > 0 ->
                shard_for(ctx, key) == shard_index

              _invalid ->
                false
            end)

        if valid? do
          command = {:expire_if_batch, entries}
          {route_key, _expire_at_ms} = hd(entries)

          if durable_raft_ctx?(ctx) do
            raft_write(ctx, shard_index, route_key, command)
          else
            GenServer.call(
              elem(ctx.shard_names, shard_index),
              {:standalone_commit, command}
            )
          end
        else
          {:error, :invalid_expiry_batch}
        end
      end

      @doc false
      @spec durable_context?(FerricStore.Instance.t()) :: boolean()
      def durable_context?(ctx), do: durable_raft_ctx?(ctx)

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

      @doc false
      @spec server_catalog_entry(FerricStore.Instance.t(), binary(), binary()) ::
              {:ok, binary() | nil} | :unavailable | {:error, atom()}
      def server_catalog_entry(ctx, namespace, subject) do
        key = Ferricstore.ServerCatalog.entry_key(namespace, subject)
        read_shard_value(ctx, 0, key)
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_key}
      end

      @doc false
      @spec server_catalog_revision(FerricStore.Instance.t(), binary()) ::
              {:ok, binary() | nil} | :unavailable | {:error, atom()}
      def server_catalog_revision(ctx, namespace) do
        key = Ferricstore.ServerCatalog.revision_key(namespace)
        read_shard_value(ctx, 0, key)
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_key}
      end

      @doc false
      @spec server_catalog_entries(FerricStore.Instance.t(), binary()) ::
              {:ok, [{binary(), binary()}]} | :unavailable | {:error, atom()}
      def server_catalog_entries(ctx, namespace) do
        prefix = Ferricstore.ServerCatalog.prefix(namespace)

        case safe_read_call(ctx, 0, {:scan_prefix, prefix}) do
          {:ok, entries} when is_list(entries) ->
            decode_server_catalog_subjects(namespace, entries)

          :unavailable ->
            :unavailable

          _invalid ->
            {:error, :invalid_server_catalog_scan}
        end
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_key}
      end

      @doc false
      @spec server_catalog_mutate(
              FerricStore.Instance.t(),
              binary(),
              binary(),
              binary() | nil,
              binary() | nil,
              binary() | :deleted,
              non_neg_integer()
            ) :: term()
      def server_catalog_mutate(
            ctx,
            namespace,
            subject,
            expected_encoded,
            expected_revision,
            value,
            max_live_entries
          ) do
        key = Ferricstore.ServerCatalog.entry_key(namespace, subject)

        raft_write(
          ctx,
          0,
          key,
          {:server_catalog_mutate, namespace, subject, expected_encoded, expected_revision, value,
           max_live_entries}
        )
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_mutation}
      end

      @doc false
      @spec server_catalog_replace(
              FerricStore.Instance.t(),
              binary(),
              binary() | nil,
              [{binary(), binary() | :deleted}],
              non_neg_integer(),
              non_neg_integer()
            ) :: term()
      def server_catalog_replace(
            ctx,
            namespace,
            expected_revision,
            mutations,
            expected_live_count,
            max_live_entries
          ) do
        key = Ferricstore.ServerCatalog.revision_key(namespace)

        raft_write(
          ctx,
          0,
          key,
          {:server_catalog_replace, namespace, expected_revision, mutations, expected_live_count,
           max_live_entries}
        )
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_replacement}
      end

      defp decode_server_catalog_subjects(namespace, entries) do
        entries
        |> Enum.reduce_while({:ok, []}, fn
          {key, encoded}, {:ok, acc} when is_binary(key) and is_binary(encoded) ->
            case Ferricstore.ServerCatalog.subject_from_key(namespace, key) do
              {:ok, subject} ->
                {:cont, {:ok, [{subject, encoded} | acc]}}

              {:error, :invalid_server_catalog_key} ->
                {:halt, {:error, :invalid_server_catalog_scan}}
            end

          _invalid, _acc ->
            {:halt, {:error, :invalid_server_catalog_scan}}
        end)
        |> case do
          {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
          {:error, _reason} = error -> error
        end
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
      defp extract_prob_key({:topk_create, key, _, _, _}), do: key
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
            [{^key, _value, exp, _lfu, _fid, _off, _vsize}]
            when is_integer(exp) and (exp == 0 or exp > now) ->
              true

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              delete_observed_keydir_entry(ctx, idx, keydir, entry)
              false

            [] ->
              false

            [_malformed_live_entry] ->
              true
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
            [{^key, _value, exp, _lfu, _fid, _off, _vsize}]
            when is_integer(exp) and (exp == 0 or exp > now) ->
              true

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              delete_observed_keydir_entry(ctx, idx, keydir, entry)
              false

            [] ->
              false

            [_malformed_live_entry] ->
              true
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
      @spec append(FerricStore.Instance.t(), binary(), binary()) ::
              {:ok, non_neg_integer()} | {:error, binary()}
      def append(ctx, key, suffix) do
        with :ok <- validate_string_write(ctx, key, suffix) do
          raft_write(ctx, shard_for(ctx, key), key, {:append, key, suffix})
        end
      end

      @doc """
      Atomically gets the old value and sets a new value for `key`.

      Returns the old value, or `nil` if the key did not exist.
      """
      @spec getset(FerricStore.Instance.t(), binary(), binary()) ::
              binary() | nil | {:error, binary()}
      def getset(ctx, key, value) do
        with :ok <- validate_string_write(ctx, key, value) do
          raft_write(ctx, shard_for(ctx, key), key, {:getset, key, value})
        end
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
              {:ok, non_neg_integer()} | {:error, binary()}
      def setrange(ctx, key, offset, value) do
        minimum_size =
          Ferricstore.Raft.ApplyLimits.setrange_size(0, offset, byte_size(value))

        with :ok <- validate_string_write(ctx, key, value),
             :ok <-
               Ferricstore.Raft.ApplyLimits.validate_instance_value_size(ctx, minimum_size) do
          raft_write(ctx, shard_for(ctx, key), key, {:setrange, key, offset, value})
        end
      end

      @doc """
      Atomically sets the bit at `offset` to `bit_val` (0 or 1). Returns the
      previous bit value (0 or 1). Extends the bitmap with zero bytes if
      necessary. Goes through Raft so concurrent SETBITs on the same key
      never lose updates — the state machine is the sole mutator.
      """
      @spec setbit(FerricStore.Instance.t(), binary(), non_neg_integer(), 0 | 1) ::
              0 | 1 | {:error, binary()}
      def setbit(ctx, key, offset, bit_val) do
        minimum_size = Ferricstore.Raft.ApplyLimits.setbit_size(0, offset)

        with :ok <- validate_string_write(ctx, key, <<>>),
             :ok <-
               Ferricstore.Raft.ApplyLimits.validate_instance_value_size(ctx, minimum_size) do
          raft_write(ctx, shard_for(ctx, key), key, {:setbit, key, offset, bit_val})
        end
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

      @doc false
      @spec scan_keys_page(
              FerricStore.Instance.t(),
              binary(),
              pos_integer(),
              binary() | nil,
              binary() | nil
            ) :: {:ok, {binary(), [binary()]}} | {:error, term()}
      def scan_keys_page(ctx, cursor, count, match_pattern, type_filter) do
        with {:ok, {shard_index, shard_cursor}} <- decode_logical_scan_cursor(cursor),
             true <- shard_index < ctx.shard_count,
             {ordered, _slots} <- LogicalKeyIndex.table_names(ctx.name, shard_index),
             keydir <- resolve_keydir(ctx, shard_index),
             {:ok, {next_shard_cursor, keys}} <-
               LogicalKeyIndex.scan_page(
                 ordered,
                 keydir,
                 shard_cursor,
                 count,
                 match_pattern,
                 type_filter,
                 HLC.now_ms()
               ) do
          next_cursor =
            case next_shard_cursor do
              0 when shard_index + 1 >= ctx.shard_count ->
                "0"

              0 ->
                encode_logical_scan_cursor(shard_index + 1, 0)

              {:after, _logical_key} = next ->
                encode_logical_scan_cursor(shard_index, next)
            end

          {:ok, {next_cursor, keys}}
        else
          false -> {:ok, {"0", []}}
          :unavailable -> ReadResult.failure(:logical_key_index_unavailable)
          {:error, :invalid_scan_cursor} -> {:error, "ERR invalid cursor"}
          {:error, _reason} = error -> error
        end
      rescue
        ArgumentError -> ReadResult.failure(:logical_key_index_unavailable)
      end

      @doc false
      @spec random_logical_key(FerricStore.Instance.t()) ::
              binary() | nil | ReadResult.failure()
      def random_logical_key(ctx) do
        now_ms = HLC.now_ms()

        candidates =
          Enum.reduce_while(0..(ctx.shard_count - 1), [], fn shard_index, acc ->
            {ordered, slots} = LogicalKeyIndex.table_names(ctx.name, shard_index)
            keydir = resolve_keydir(ctx, shard_index)

            case LogicalKeyIndex.count_live(
                   ordered,
                   slots,
                   keydir,
                   now_ms,
                   &delete_expired_logical_entry(ctx, shard_index, keydir, &1)
                 ) do
              {:ok, 0} -> {:cont, acc}
              {:ok, count} when count > 0 -> {:cont, [{shard_index, count} | acc]}
              :unavailable -> {:halt, ReadResult.failure(:logical_key_index_unavailable)}
              {:error, reason} -> {:halt, ReadResult.failure(reason)}
            end
          end)

        case candidates do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          candidates -> weighted_random_logical_key(ctx, candidates)
        end
      end

      defp weighted_random_logical_key(_ctx, []), do: nil

      defp weighted_random_logical_key(ctx, candidates) do
        total = Enum.reduce(candidates, 0, fn {_shard_index, count}, acc -> acc + count end)
        target = :rand.uniform(total)

        {shard_index, _count} =
          Enum.reduce_while(candidates, target, fn {shard_index, count} = candidate, remaining ->
            if remaining <= count,
              do: {:halt, candidate},
              else: {:cont, remaining - count}
          end)

        {ordered, slots} = LogicalKeyIndex.table_names(ctx.name, shard_index)

        case LogicalKeyIndex.random_key(
               ordered,
               slots,
               resolve_keydir(ctx, shard_index)
             ) do
          {:ok, nil} ->
            weighted_random_logical_key(ctx, List.keydelete(candidates, shard_index, 0))

          {:ok, key} ->
            key

          :unavailable ->
            ReadResult.failure(:logical_key_index_unavailable)

          {:error, reason} ->
            ReadResult.failure(reason)
        end
      end

      defp decode_logical_scan_cursor("0"), do: {:ok, {0, 0}}

      defp decode_logical_scan_cursor(encoded)
           when is_binary(encoded) and byte_size(encoded) <= @max_logical_scan_cursor_bytes do
        with {:ok, binary} <- Base.url_decode64(encoded, padding: false),
             {:ok, {:ferricstore_scan_cursor, 1, shard_index, shard_cursor}} <-
               TermCodec.decode(binary),
             true <- is_integer(shard_index) and shard_index >= 0,
             true <- valid_logical_scan_shard_cursor?(shard_cursor) do
          {:ok, {shard_index, shard_cursor}}
        else
          _invalid -> {:error, :invalid_scan_cursor}
        end
      rescue
        ArgumentError -> {:error, :invalid_scan_cursor}
      end

      defp decode_logical_scan_cursor(_invalid), do: {:error, :invalid_scan_cursor}

      defp valid_logical_scan_shard_cursor?(0), do: true

      defp valid_logical_scan_shard_cursor?({:after, key})
           when is_binary(key) and byte_size(key) <= @max_key_size,
           do: true

      defp valid_logical_scan_shard_cursor?(_cursor), do: false

      defp encode_logical_scan_cursor(shard_index, shard_cursor) do
        {:ferricstore_scan_cursor, 1, shard_index, shard_cursor}
        |> TermCodec.encode()
        |> Base.url_encode64(padding: false)
      end

      @doc "Returns all live (non-expired, non-deleted) keys across every shard."
      @spec keys(FerricStore.Instance.t()) :: [binary()] | ReadResult.failure()
      def keys(ctx) do
        sc = ctx.shard_count
        now = HLC.now_ms()

        Enum.reduce_while(0..(sc - 1), [], fn i, acc ->
          {ordered, slots} = LogicalKeyIndex.table_names(ctx.name, i)
          keydir = resolve_keydir(ctx, i)

          if keydir_available?(keydir) do
            with {:ok, _count} <-
                   LogicalKeyIndex.count_live(
                     ordered,
                     slots,
                     keydir,
                     now,
                     &delete_expired_logical_entry(ctx, i, keydir, &1)
                   ),
                 {:ok, keys} <- LogicalKeyIndex.all_live(ordered, keydir, now) do
              {:cont, [keys | acc]}
            else
              :unavailable ->
                emit_shard_unavailable(ctx, i, :keys, :logical_key_index_unavailable)
                {:halt, ReadResult.failure(:logical_key_index_unavailable)}

              {:error, reason} ->
                emit_shard_unavailable(ctx, i, :keys, reason)
                {:halt, ReadResult.failure(reason)}
            end
          else
            case safe_read_call(ctx, i, :keys) do
              {:ok, keys} -> {:cont, [keys | acc]}
              :unavailable -> {:halt, ReadResult.failure(:shard_unavailable)}
            end
          end
        end)
        |> case do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          key_batches -> key_batches |> Enum.reverse() |> :lists.append()
        end
      end

      @doc "Returns the count of all live keys across every shard."
      @spec dbsize(FerricStore.Instance.t()) :: non_neg_integer() | ReadResult.failure()
      def dbsize(ctx) do
        sc = ctx.shard_count
        now = HLC.now_ms()

        Enum.reduce_while(0..(sc - 1), 0, fn i, acc ->
          {ordered, slots} = LogicalKeyIndex.table_names(ctx.name, i)
          keydir = resolve_keydir(ctx, i)

          if keydir_available?(keydir) do
            case LogicalKeyIndex.count_live(
                   ordered,
                   slots,
                   keydir,
                   now,
                   &delete_expired_logical_entry(ctx, i, keydir, &1)
                 ) do
              {:ok, count} ->
                {:cont, acc + count}

              :unavailable ->
                emit_shard_unavailable(ctx, i, :dbsize, :logical_key_index_unavailable)
                {:halt, ReadResult.failure(:logical_key_index_unavailable)}

              {:error, reason} ->
                emit_shard_unavailable(ctx, i, :dbsize, reason)
                {:halt, ReadResult.failure(reason)}
            end
          else
            {:halt, keydir_unavailable(ctx, i, :dbsize)}
          end
        end)
      end

      defp delete_expired_logical_entry(ctx, shard_index, keydir, observed_entry) do
        _deleted = delete_observed_keydir_entry(ctx, shard_index, keydir, observed_entry)
        :ok
      end

      defp keydir_available?(keydir) do
        :ets.info(keydir, :type) != :undefined
      rescue
        ArgumentError -> false
      end

      defp keydir_unavailable(ctx, idx, request) do
        emit_shard_unavailable(ctx, idx, request, :keydir_unavailable)
        ReadResult.failure(:keydir_unavailable)
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
      Returns a logical WATCH token for `key`, ordered by its owning Raft group.
      """
      @spec watch_token(FerricStore.Instance.t(), binary()) :: term()
      def watch_token(ctx, key) do
        idx = shard_for(ctx, key)
        watch_token_read(ctx, idx, {:watch_token, key})
      end

      defp watch_token_read(ctx, idx, command) do
        if durable_raft_ctx?(ctx) do
          quorum_write(ctx, idx, command)
        else
          GenServer.call(elem(ctx.shard_names, idx), command)
        end
      end

      @doc """
      Returns logical WATCH tokens using at most one Raft command per owning shard.
      """
      @spec watch_tokens(FerricStore.Instance.t(), [binary()]) ::
              %{binary() => term()} | {:error, term()}
      def watch_tokens(_ctx, []), do: %{}

      def watch_tokens(ctx, keys) when is_list(keys) do
        shard_groups =
          keys
          |> Enum.group_by(&shard_for(ctx, &1))
          |> Enum.sort_by(&elem(&1, 0))

        commands =
          Enum.map(shard_groups, fn {shard_index, shard_keys} ->
            {shard_index, {:watch_tokens, shard_keys}}
          end)

        results = watch_token_read_many(ctx, commands)

        merge_watch_token_results(shard_groups, results)
      end

      defp watch_token_read_many(_ctx, []), do: []

      defp watch_token_read_many(ctx, [{shard_index, command}]) do
        [watch_token_read(ctx, shard_index, command)]
      end

      defp watch_token_read_many(ctx, commands) do
        if selected_waraft_ctx?(ctx) do
          Ferricstore.Raft.Backend.write_many(commands)
        else
          Enum.map(commands, fn {shard_index, command} ->
            GenServer.call(elem(ctx.shard_names, shard_index), command)
          end)
        end
      end

      defp merge_watch_token_results(shard_groups, results)
           when is_list(results) and length(shard_groups) == length(results) do
        shard_groups
        |> Enum.zip(results)
        |> Enum.reduce_while(%{}, fn
          {{_shard_index, _keys}, %{} = tokens}, acc ->
            {:cont, Map.merge(acc, tokens)}

          {{_shard_index, _keys}, {:error, _reason} = error}, _acc ->
            {:halt, error}

          {{shard_index, _keys}, result}, _acc ->
            {:halt, {:error, {:invalid_watch_token_result, shard_index, result}}}
        end)
      end

      defp merge_watch_token_results(_shard_groups, results),
        do: {:error, {:invalid_watch_token_results, results}}

      @doc """
      Returns the keydir disk location for a key, or `:miss`.

      Reads the `{file_id, offset, value_size}` fields directly from the keydir
      ETS table without a GenServer roundtrip. Returns `{:ok, {fid, off, vsize}}`
      for live keys, or `:miss` if the key is not in the keydir or is expired.

      Used by sendfile zero-copy and STRLEN on cold keys.
      """
      @spec get_keydir_file_ref(FerricStore.Instance.t(), binary()) ::
              {:ok, {term(), non_neg_integer(), non_neg_integer()}}
              | :miss
              | ReadResult.failure()
      def get_keydir_file_ref(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()

        try do
          case :ets.lookup(keydir, key) do
            [{^key, nil, exp, _lfu, fid, off, vsize}]
            when is_integer(exp) and (exp == 0 or exp > now) and
                   readable_cold_ref?(fid, off, vsize) ->
              {:ok, {fid, off, vsize}}

            [{^key, nil, exp, _lfu, :pending, _off, vsize}]
            when is_integer(exp) and (exp == 0 or exp > now) and
                   valid_pending_value_size(vsize) ->
              :miss

            [{^key, value, exp, _lfu, _fid, _off, _vsize}]
            when value != nil and is_integer(exp) and (exp == 0 or exp > now) ->
              :miss

            [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
            when is_integer(exp) and exp > 0 and exp <= now ->
              delete_observed_keydir_entry(ctx, idx, keydir, entry)
              :miss

            [] ->
              :miss

            [entry] ->
              ReadResult.failure({:invalid_keydir_entry, entry})
          end
        rescue
          ArgumentError -> ReadResult.failure(:keydir_unavailable)
        end
      end

      # -------------------------------------------------------------------
      # Native command accessors
      # -------------------------------------------------------------------

      @spec cas(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer() | nil) ::
              1 | 0 | nil
      def cas(ctx, key, expected, new_value, ttl_ms) do
        expire_at_ms = if ttl_ms, do: HLC.now_ms() + ttl_ms, else: nil
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        existing_expire_at_ms = existing_keydir_expire_at_ms(keydir, key)

        case raft_write(ctx, idx, key, {:cas, key, expected, new_value, expire_at_ms}) do
          1 ->
            maybe_publish_control_cas(
              ctx,
              idx,
              keydir,
              key,
              new_value,
              expire_at_ms,
              existing_expire_at_ms
            )

            1

          other ->
            other
        end
      end

      defp existing_keydir_expire_at_ms(keydir, key) do
        case :ets.lookup(keydir, key) do
          [{^key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
            expire_at_ms

          _other ->
            0
        end
      rescue
        ArgumentError -> 0
      end

      defp maybe_publish_control_cas(
             ctx,
             idx,
             keydir,
             key,
             new_value,
             expire_at_ms,
             existing_expire_at_ms
           ) do
        if flow_governance_control_key?(key) do
          publish_cas_value(
            ctx,
            idx,
            keydir,
            key,
            new_value,
            expire_at_ms || existing_expire_at_ms
          )
        end
      end

      defp publish_cas_value(ctx, idx, keydir, key, value, expire_at_ms) do
        clear_compound_data_structure_for_string_put(ctx, idx, keydir, key)
        track_keydir_binary_insert(ctx, idx, keydir, key, value)

        :ets.insert(
          keydir,
          {key, value, expire_at_ms, LFU.initial(), :pending, 0, byte_size(value)}
        )
      rescue
        ArgumentError -> :ok
      end

      defp flow_governance_control_key?(<<"f:{", rest::binary>>),
        do: :binary.match(rest, "}:gov:") != :nomatch

      defp flow_governance_control_key?(_key), do: false

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

      @spec fetch_or_compute_lock(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
              :ok | {:error, term()}
      def fetch_or_compute_lock(ctx, key, owner, ttl_ms) do
        expire_at_ms = HLC.now_ms() + ttl_ms
        outcome_key = FetchOrComputeOutcome.key(key)

        raft_write(
          ctx,
          shard_for(ctx, key),
          key,
          {:fetch_or_compute_lock, key, outcome_key, owner, expire_at_ms}
        )
      end

      @spec fetch_or_compute_publish(
              FerricStore.Instance.t(),
              binary(),
              binary(),
              non_neg_integer(),
              binary()
            ) :: :ok | {:error, term()}
      def fetch_or_compute_publish(ctx, key, value, ttl_ms, owner) do
        expire_at_ms = if ttl_ms > 0, do: HLC.now_ms() + ttl_ms, else: 0

        case raft_write(
               ctx,
               shard_for(ctx, key),
               key,
               {:fetch_or_compute_publish, key, value, expire_at_ms, owner}
             ) do
          :ok ->
            :ok

          {:error, reason} ->
            fetch_or_compute_owner_error(reason)

          other ->
            other
        end
      end

      @spec fetch_or_compute_release(FerricStore.Instance.t(), binary(), binary()) ::
              :ok | {:error, term()}
      def fetch_or_compute_release(ctx, key, owner) do
        case raft_write(
               ctx,
               shard_for(ctx, key),
               key,
               {:fetch_or_compute_release, key, owner}
             ) do
          :ok -> :ok
          {:error, reason} -> fetch_or_compute_owner_error(reason)
          other -> other
        end
      end

      @spec fetch_or_compute_fail(
              FerricStore.Instance.t(),
              binary(),
              binary(),
              binary(),
              pos_integer()
            ) :: :ok | {:error, term()}
      def fetch_or_compute_fail(ctx, key, owner, error, outcome_ttl_ms) do
        with {:ok, encoded_error} <- FetchOrComputeOutcome.encode_error(error) do
          outcome_key = FetchOrComputeOutcome.key(key)
          outcome_expire_at_ms = HLC.now_ms() + outcome_ttl_ms

          case raft_write(
                 ctx,
                 shard_for(ctx, key),
                 key,
                 {:fetch_or_compute_fail, key, outcome_key, encoded_error, outcome_expire_at_ms,
                  owner}
               ) do
            :ok -> :ok
            {:error, reason} -> fetch_or_compute_owner_error(reason)
            other -> other
          end
        end
      end

      @spec fetch_or_compute_outcome(FerricStore.Instance.t(), binary()) ::
              :pending | {:failed, binary()} | {:error, term()}
      def fetch_or_compute_outcome(ctx, key) do
        case read_shard_value(ctx, shard_for(ctx, key), FetchOrComputeOutcome.key(key)) do
          {:ok, nil} ->
            :pending

          {:ok, encoded_error} when is_binary(encoded_error) ->
            case FetchOrComputeOutcome.decode_error(encoded_error) do
              {:ok, error} -> {:failed, error}
              {:error, _reason} = invalid -> invalid
            end

          :unavailable ->
            {:error, :shard_unavailable}

          other ->
            {:error, {:invalid_fetch_or_compute_outcome_read, other}}
        end
      end

      defp fetch_or_compute_owner_error(reason)
           when reason in [:key_locked, :key_not_locked, :key_lock_expired, :not_lock_owner],
           do: {:error, "ERR fetch_or_compute token is not the current lock owner"}

      defp fetch_or_compute_owner_error(reason), do: {:error, reason}

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

      @spec compound_get(FerricStore.Instance.t(), binary(), binary()) ::
              binary() | nil | ReadResult.failure()
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

          terminal when terminal in [:miss, :expired] ->
            if selected_waraft_ctx?(ctx) do
              nil
            else
              fallback_compound_get(ctx, idx, redis_key, compound_key)
            end

          :no_table ->
            if selected_waraft_ctx?(ctx) do
              ReadResult.failure(:keydir_unavailable)
            else
              fallback_compound_get(ctx, idx, redis_key, compound_key)
            end

          {:invalid, entry} ->
            ReadResult.failure({:invalid_keydir_entry, entry})

          _other ->
            fallback_compound_get(ctx, idx, redis_key, compound_key)
        end
      end
    end
  end
end
