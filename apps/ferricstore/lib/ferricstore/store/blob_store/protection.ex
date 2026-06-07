defmodule Ferricstore.Store.BlobStore.Protection do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore.TableOwner

      @doc false
      @spec unprotect(protection_token()) :: :ok
      def unprotect(nil), do: :ok

      def unprotect(tokens) when is_list(tokens) do
        Enum.each(tokens, &unprotect/1)
        :ok
      end

      def unprotect({:blob_store_protection, data_dir, shard_index, relative_paths})
          when is_binary(data_dir) and is_integer(shard_index) and is_list(relative_paths) do
        ensure_protected_table()

        Enum.each(relative_paths, fn relative_path ->
          key = {data_dir, shard_index, relative_path}

          case :ets.lookup(@protected_table, key) do
            [{^key, count, deadline_ms}] when is_integer(count) and count > 1 ->
              :ets.insert(@protected_table, {key, count - 1, deadline_ms})

            [{^key, _count, _deadline_ms}] ->
              :ets.delete(@protected_table, key)

            [{^key, count}] when is_integer(count) and count > 1 ->
              :ets.update_counter(@protected_table, key, {2, -1})

            [{^key, _count}] ->
              :ets.delete(@protected_table, key)

            [] ->
              :ok
          end
        end)

        :ok
      end

      def unprotect(_token), do: :ok

      @doc false
      @spec harden_protection(protection_token()) :: :ok
      @spec harden_protection(protection_token(), keyword() | map() | term()) :: :ok
      def harden_protection(token, metadata \\ [])

      def harden_protection(nil, _metadata), do: :ok

      def harden_protection(tokens, metadata) when is_list(tokens) do
        Enum.each(tokens, &harden_protection(&1, metadata))
        :ok
      end

      def harden_protection(
            {:blob_store_protection, data_dir, shard_index, relative_paths},
            metadata
          )
          when is_binary(data_dir) and is_integer(shard_index) and is_list(relative_paths) do
        ensure_protected_table()

        Enum.each(relative_paths, fn relative_path ->
          key = {data_dir, shard_index, relative_path}

          case :ets.lookup(@protected_table, key) do
            [{^key, count, _deadline_ms}] when is_integer(count) and count > 0 ->
              :ets.insert(@protected_table, {key, count, :infinity})

            [{^key, count}] when is_integer(count) and count > 0 ->
              :ets.insert(@protected_table, {key, count, :infinity})

            [] ->
              :ets.insert(@protected_table, {key, 1, :infinity})
          end
        end)

        register_hardened_protection(data_dir, shard_index, relative_paths, metadata)
        :ok
      end

      def harden_protection(_token, _metadata), do: :ok

      @doc false
      @spec hardened_protection_ids(binary(), non_neg_integer()) :: [reference()]
      @spec hardened_protection_ids(binary(), non_neg_integer(), non_neg_integer()) :: [
              reference()
            ]
      def hardened_protection_ids(data_dir, shard_index, limit \\ 1_000)
          when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
                 is_integer(limit) and limit >= 0 do
        ensure_hardened_table()

        @hardened_table
        |> :ets.match_object({:_, data_dir, shard_index, :_, :_, :_})
        |> Enum.sort_by(fn {_id, _dir, _shard, _paths, hardened_at_ms, _metadata} ->
          hardened_at_ms
        end)
        |> Enum.take(limit)
        |> Enum.map(fn {id, _dir, _shard, _paths, _hardened_at_ms, _metadata} -> id end)
      end

      @doc false
      @spec hardened_protection_stats(binary()) :: %{
              count: non_neg_integer(),
              oldest_age_ms: non_neg_integer()
            }
      def hardened_protection_stats(data_dir) when is_binary(data_dir) do
        hardened_protection_stats(data_dir, :all)
      end

      @doc false
      @spec hardened_protection_stats(binary(), non_neg_integer() | :all) :: %{
              count: non_neg_integer(),
              oldest_age_ms: non_neg_integer()
            }
      def hardened_protection_stats(data_dir, shard_index)
          when is_binary(data_dir) and
                 (shard_index == :all or (is_integer(shard_index) and shard_index >= 0)) do
        ensure_hardened_table()

        now_ms = System.monotonic_time(:millisecond)

        rows =
          case shard_index do
            :all -> :ets.match_object(@hardened_table, {:_, data_dir, :_, :_, :_, :_})
            idx -> :ets.match_object(@hardened_table, {:_, data_dir, idx, :_, :_, :_})
          end

        oldest_at =
          Enum.reduce(rows, nil, fn {_id, _dir, _shard, _paths, hardened_at_ms, _metadata}, acc ->
            cond do
              not is_integer(hardened_at_ms) -> acc
              is_nil(acc) -> hardened_at_ms
              true -> min(acc, hardened_at_ms)
            end
          end)

        oldest_age_ms =
          case oldest_at do
            nil -> 0
            hardened_at_ms -> max(now_ms - hardened_at_ms, 0)
          end

        %{count: length(rows), oldest_age_ms: oldest_age_ms}
      end

      defp protect_refs(data_dir, shard_index, refs) do
        relative_paths =
          refs
          |> Enum.reduce(MapSet.new(), fn
            %BlobRef{} = ref, acc ->
              if BlobRef.valid?(ref), do: MapSet.put(acc, BlobRef.relative_path(ref)), else: acc

            _other, acc ->
              acc
          end)
          |> MapSet.to_list()

        case relative_paths do
          [] ->
            nil

          [_ | _] ->
            ensure_protected_table()
            deadline_ms = protection_deadline_ms()

            Enum.each(relative_paths, fn relative_path ->
              key = {data_dir, shard_index, relative_path}
              protect_ref_path(key, deadline_ms)
            end)

            {:blob_store_protection, data_dir, shard_index, relative_paths}
        end
      end

      defp protect_ref_path(key, deadline_ms) do
        case :ets.lookup(@protected_table, key) do
          [{^key, count, existing_deadline}] when is_integer(count) ->
            :ets.insert(
              @protected_table,
              {key, count + 1, max_deadline(existing_deadline, deadline_ms)}
            )

          [{^key, count}] when is_integer(count) ->
            :ets.insert(@protected_table, {key, count + 1, deadline_ms})

          [] ->
            :ets.insert(@protected_table, {key, 1, deadline_ms})
        end
      end

      defp register_hardened_protection(_data_dir, _shard_index, [], _metadata), do: :ok

      defp register_hardened_protection(data_dir, shard_index, relative_paths, metadata) do
        ensure_hardened_table()

        paths =
          relative_paths
          |> Enum.filter(&is_binary/1)
          |> Enum.uniq()

        case paths do
          [] ->
            :ok

          [_ | _] ->
            metadata = normalize_hardened_metadata(metadata)
            id = make_ref()
            hardened_at_ms = System.monotonic_time(:millisecond)

            :ets.insert(
              @hardened_table,
              {id, data_dir, shard_index, paths, hardened_at_ms, metadata}
            )

            :telemetry.execute(
              [:ferricstore, :blob, :protection, :hardened],
              %{count: 1, path_count: length(paths)},
              %{data_dir: data_dir, shard_index: shard_index, id: id, metadata: metadata}
            )

            :ok
        end
      end

      defp normalize_hardened_metadata(metadata) when is_map(metadata), do: metadata

      defp normalize_hardened_metadata(metadata) when is_list(metadata) do
        Map.new(metadata)
      rescue
        _ -> %{metadata: metadata}
      end

      defp normalize_hardened_metadata(metadata), do: %{metadata: metadata}

      defp release_hardened_protections(_data_dir, _shard_index, []), do: 0

      defp release_hardened_protections(data_dir, shard_index, hardened_ids) do
        ensure_hardened_table()

        Enum.reduce(hardened_ids, 0, fn id, released ->
          case :ets.lookup(@hardened_table, id) do
            [{^id, ^data_dir, ^shard_index, relative_paths, _hardened_at_ms, _metadata}]
            when is_list(relative_paths) ->
              :ets.delete(@hardened_table, id)
              unprotect({:blob_store_protection, data_dir, shard_index, relative_paths})
              released + 1

            _missing_or_other_shard ->
              released
          end
        end)
      end

      defp protected_relative_paths(data_dir, shard_index) do
        ensure_protected_table()
        now_ms = System.monotonic_time(:millisecond)

        new_shape =
          :ets.match_object(@protected_table, {{data_dir, shard_index, :_}, :_, :_})

        old_shape =
          :ets.match_object(@protected_table, {{data_dir, shard_index, :_}, :_})

        Enum.reduce(new_shape ++ old_shape, MapSet.new(), fn
          {{^data_dir, ^shard_index, relative_path} = key, count, deadline_ms}, acc
          when is_binary(relative_path) and is_integer(count) and count > 0 ->
            if protection_expired?(deadline_ms, now_ms) do
              :ets.delete(@protected_table, key)
              acc
            else
              MapSet.put(acc, relative_path)
            end

          {{^data_dir, ^shard_index, relative_path}, count}, acc
          when is_binary(relative_path) and is_integer(count) and count > 0 ->
            key = {data_dir, shard_index, relative_path}
            :ets.delete(@protected_table, key)
            acc

          _other, acc ->
            acc
        end)
      end

      defp protection_deadline_ms do
        case protection_ttl_ms() do
          :infinity -> :infinity
          ttl_ms -> System.monotonic_time(:millisecond) + ttl_ms
        end
      end

      defp protection_ttl_ms do
        configured =
          Process.get(
            :ferricstore_blob_store_protection_ttl_ms,
            Application.get_env(
              :ferricstore,
              :blob_side_channel_protection_ttl_ms,
              @default_protection_ttl_ms
            )
          )

        case configured do
          :infinity -> :infinity
          ttl_ms when is_integer(ttl_ms) and ttl_ms >= 0 -> ttl_ms
          _other -> @default_protection_ttl_ms
        end
      end

      defp protection_expired?(:infinity, _now_ms), do: false

      defp protection_expired?(deadline_ms, now_ms)
           when is_integer(deadline_ms) and is_integer(now_ms),
           do: deadline_ms <= now_ms

      defp protection_expired?(_deadline_ms, _now_ms), do: false

      defp max_deadline(:infinity, _deadline_ms), do: :infinity
      defp max_deadline(_existing_deadline, :infinity), do: :infinity

      defp max_deadline(existing_deadline, deadline_ms)
           when is_integer(existing_deadline) and is_integer(deadline_ms),
           do: max(existing_deadline, deadline_ms)

      defp max_deadline(_existing_deadline, deadline_ms), do: deadline_ms

      defp ensure_protected_table do
        case :ets.whereis(@protected_table) do
          :undefined ->
            TableOwner.ensure_tables()

          tid ->
            tid
        end
      end

      defp ensure_hardened_table do
        case :ets.whereis(@hardened_table) do
          :undefined ->
            TableOwner.ensure_tables()

          tid ->
            tid
        end
      end

      defp ensure_segment_table do
        case :ets.whereis(@segment_table) do
          :undefined ->
            TableOwner.ensure_tables()

          tid ->
            tid
        end
      end
    end
  end
end
