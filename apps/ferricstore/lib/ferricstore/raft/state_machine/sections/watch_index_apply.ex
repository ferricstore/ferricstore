defmodule Ferricstore.Raft.StateMachine.Sections.WatchIndexApply do
  @moduledoc false

  # Applies `Ferricstore.Transaction.WatchIndex`: WATCH registers a row per
  # watched key, each replicated entry raises the rows of the keys it writes in
  # the same storage flush as its own writes, and a watched EXEC compares the
  # rows. Rows live only in the keydir, so every replica reads the versions its
  # own log produced.

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.ExpiryContext
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Transaction.WatchIndex

      @sm_watch_index_entry_key :sm_watch_index_entry

      # Marks one replicated entry while it applies; WATCH registers rows only
      # inside one, so the standalone (single node, no log) paths keep their
      # node-local tokens.
      defp with_watch_index_entry(command, %{index: index}, fun)
           when is_integer(index) and index > 0 and is_function(fun, 0) do
        previous = Process.get(@sm_watch_index_entry_key, :undefined)

        Process.put(@sm_watch_index_entry_key, %{
          index: index,
          command: command,
          written: :unclassified,
          registered: %{}
        })

        try do
          fun.()
        after
          case previous do
            :undefined -> Process.delete(@sm_watch_index_entry_key)
            value -> Process.put(@sm_watch_index_entry_key, value)
          end
        end
      end

      defp with_watch_index_entry(_command, _meta, fun) when is_function(fun, 0), do: fun.()

      @doc false
      # True when a WARaft segment projection must leave `command` to the state
      # machine: the projection writes no index rows.
      def watch_index_requires_apply?(command, state) do
        watch_index_in_use?(state) and
          case WatchIndex.written(command) do
            :none -> false
            :all -> true
            {:keys, keys} -> Enum.any?(keys, &watch_index_row_present?(state, &1))
          end
      end

      defp watch_index_in_use?(state),
        do: :ets.lookup(state.ets, WatchIndex.in_use_key()) != []

      defp watch_index_row_present?(state, key),
        do: :ets.lookup(state.ets, WatchIndex.row_key(key)) != []

      defp watch_index_registering? do
        match?(%{}, Process.get(@sm_watch_index_entry_key))
      end

      defp transaction_watch_index_tokens(state, keys) do
        %{index: index} = entry = Process.get(@sm_watch_index_entry_key)
        expire_at_ms = apply_now_ms() + WatchIndex.row_ttl_ms()

        with :ok <- ensure_watch_index_in_use(state, entry) do
          Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, tokens} ->
            with {:ok, version} <- watch_index_registration_version(state, key, index),
                 :ok <- put_watch_index_row(state, key, version, expire_at_ms),
                 live? when is_boolean(live?) <- watch_key_live?(state, key) do
              register_watch_index_key(key, version, expire_at_ms)
              {:cont, {:ok, Map.put(tokens, key, {:watch_index, version, live?})}}
            else
              {:error, _reason} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, tokens} -> tokens
            {:error, _reason} = error -> error
          end
        end
      end

      defp ensure_watch_index_in_use(state, %{registered: registered}) do
        if map_size(registered) > 0 or watch_index_in_use?(state),
          do: :ok,
          else: raw_put(state, WatchIndex.in_use_key(), "1", 0)
      end

      # A live row keeps its version: the key has not been written since.
      defp watch_index_registration_version(state, key, index) do
        case Map.fetch(watch_index_entry_registered(), key) do
          {:ok, {version, _expire_at_ms}} ->
            {:ok, version}

          :error ->
            case watch_index_row(state, key) do
              {:ok, version, _expire_at_ms} -> {:ok, version}
              :missing -> {:ok, WatchIndex.registration_version(index)}
              {:error, _reason} = error -> error
            end
        end
      end

      defp register_watch_index_key(key, version, expire_at_ms) do
        entry = Process.get(@sm_watch_index_entry_key)

        Process.put(@sm_watch_index_entry_key, %{
          entry
          | registered: Map.put(entry.registered, key, {version, expire_at_ms})
        })
      end

      defp watch_index_entry_registered do
        case Process.get(@sm_watch_index_entry_key) do
          %{registered: registered} -> registered
          _none -> %{}
        end
      end

      # Inside a transaction's flush the row is staged through the transaction
      # store, as the transaction's own writes are.
      defp put_watch_index_row(state, key, version, expire_at_ms) do
        row_key = WatchIndex.row_key(key)
        value = WatchIndex.encode_version(version)

        if cross_shard_pending_active?() do
          try do
            _staged = build_local_raft_tx_store(state).put.(row_key, value, expire_at_ms)
            :ok
          catch
            {:transaction_store_failure, reason} -> {:error, reason}
          end
        else
          raw_put(state, row_key, value, expire_at_ms)
        end
      end

      defp watch_index_row(state, key) do
        row_key = WatchIndex.row_key(key)

        case :ets.lookup(state.ets, row_key) do
          [{^row_key, _value, expire_at_ms, _lfu, _file_id, _offset, _size}] ->
            case do_get(state, row_key) do
              value when is_binary(value) ->
                case WatchIndex.decode_version(value) do
                  {:ok, version} -> {:ok, version, expire_at_ms}
                  :error -> {:error, :invalid_watch_index_row}
                end

              nil ->
                :missing

              _unavailable ->
                {:error, :watch_state_unavailable}
            end

          [] ->
            :missing
        end
      end

      # Raises the rows of the keys this entry writes; runs before each storage
      # flush of the entry, so the rows land in the same flush as the writes.
      # A read failure here is this node's storage, not the command's outcome:
      # it fails as storage so the entry is retried instead of answered.
      defp stage_watch_index_writes(state) do
        case Process.get(@sm_watch_index_entry_key) do
          %{registered: registered} = entry ->
            if map_size(registered) > 0 or watch_index_in_use?(state) do
              entry
              |> watch_index_entry_written()
              |> stage_watch_index_written(state, entry)
              |> case do
                :ok -> state_storage_failure_result(:ok)
                {:error, reason} -> {:error, {:state_read_failed, reason}}
              end
            else
              :ok
            end

          _none ->
            :ok
        end
      end

      defp watch_index_entry_written(%{written: :unclassified, command: command} = entry) do
        written = WatchIndex.written(command)
        Process.put(@sm_watch_index_entry_key, %{entry | written: written})
        written
      end

      defp watch_index_entry_written(%{written: written}), do: written

      defp stage_watch_index_written(:none, _state, _entry), do: :ok

      defp stage_watch_index_written(:all, state, entry),
        do: stage_watch_index_written({:keys, watch_index_keys(state, entry)}, state, entry)

      defp stage_watch_index_written({:keys, keys}, state, %{index: index}) do
        version = WatchIndex.write_version(index)

        Enum.reduce_while(keys, :ok, fn key, :ok ->
          case raise_watch_index_row(state, key, version) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp raise_watch_index_row(state, key, version) do
        case Map.fetch(watch_index_entry_registered(), key) do
          {:ok, {current, expire_at_ms}} when current < version ->
            register_watch_index_key(key, version, expire_at_ms)
            put_watch_index_row(state, key, version, expire_at_ms)

          {:ok, _current} ->
            :ok

          :error ->
            case watch_index_row(state, key) do
              {:ok, current, expire_at_ms} when current < version ->
                put_watch_index_row(state, key, version, expire_at_ms)

              {:ok, _current, _expire_at_ms} ->
                :ok

              :missing ->
                :ok

              {:error, _reason} = error ->
                error
            end
        end
      end

      # The client keys with a row, for an entry whose written keys are only
      # known as it applies (FLUSHDB). Sorted, so every replica stages them in
      # one order.
      defp watch_index_keys(state, %{registered: registered}) do
        prefix = WatchIndex.row_prefix()
        prefix_size = byte_size(prefix)

        state.ets
        |> :ets.select([
          {{:"$1", :_, :_, :_, :_, :_, :_},
           [
             {:andalso, {:is_binary, :"$1"},
              {:andalso, {:>=, {:byte_size, :"$1"}, prefix_size},
               {:==, {:binary_part, :"$1", 0, prefix_size}, prefix}}}
           ], [:"$1"]}
        ])
        |> Enum.map(&binary_part(&1, prefix_size, byte_size(&1) - prefix_size))
        |> Kernel.++(Map.keys(registered))
        |> Enum.uniq()
        |> Enum.sort()
      end

      defp watch_index_token_clean?(state, key, version, live?) do
        case watch_index_row(state, key) do
          {:ok, ^version, _expire_at_ms} when live? ->
            case watch_key_live?(state, key) do
              true -> :clean
              false -> :changed
              {:error, _reason} = error -> error
            end

          {:ok, ^version, _expire_at_ms} ->
            :clean

          {:ok, _other, _expire_at_ms} ->
            :changed

          :missing ->
            :changed

          {:error, _reason} = error ->
            error
        end
      end

      # Whether `key` exists for a client: its string, type or list metadata
      # row is live at this entry's time.
      defp watch_key_live?(state, key) do
        expiry_context = ExpiryContext.capture()

        Enum.reduce_while(
          [key, CompoundKey.type_key(key), CompoundKey.list_meta_key(key)],
          false,
          fn storage_key, false ->
            case :ets.lookup(state.ets, storage_key) do
              [{^storage_key, _value, expire_at_ms, _lfu, _file_id, _offset, _size}] ->
                case ExpiryContext.classify(expiry_context, expire_at_ms) do
                  :live -> {:halt, true}
                  :expired -> {:cont, false}
                  {:unsafe, reason} -> {:halt, {:error, reason}}
                end

              [] ->
                {:cont, false}
            end
          end
        )
      end
    end
  end
end
