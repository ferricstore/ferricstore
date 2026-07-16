defmodule Ferricstore.Commands.Strings.MSet do
  @moduledoc false

  alias Ferricstore.Commands.Strings.Compound
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Router

  @max_key_bytes 65_535

  def mset_args([], _store), do: {:error, "ERR wrong number of arguments for 'mset' command"}

  def mset_args(args, store) do
    if even_length?(args) do
      case mset_validate(args) do
        :ok -> mset_exec(args, store)
        {:error, _} = err -> err
      end
    else
      {:error, "ERR wrong number of arguments for 'mset' command"}
    end
  end

  def msetnx_args([], _store), do: {:error, "ERR wrong number of arguments for 'msetnx' command"}

  def msetnx_args(args, store) do
    if even_length?(args) do
      case mset_validate(args) do
        :ok -> msetnx_validated_args(args, store)
        {:error, _} = err -> err
      end
    else
      {:error, "ERR wrong number of arguments for 'msetnx' command"}
    end
  end

  defp msetnx_validated_args(args, %FerricStore.Instance{} = store) do
    keys = extract_keys(args)

    if keys_on_one_shard?(keys, store) do
      Router.atomic_msetnx(store, mset_pairs(args))
    else
      if Router.durable_context?(store) do
        msetnx_coordinated(args, keys, store)
      else
        with :ok <- Router.admit_string_batch(store, mset_pairs(args)) do
          msetnx_coordinated(args, keys, store)
        end
      end
    end
  end

  defp msetnx_validated_args(args, store) do
    keys = extract_keys(args)
    msetnx_coordinated(args, keys, store)
  end

  defp msetnx_coordinated(args, keys, store) do
    CrossShardOp.execute(
      Enum.map(keys, &{&1, :write}),
      fn unified_store ->
        case msetnx_any_exists?(args, unified_store) do
          {:ok, true} ->
            0

          {:ok, false} ->
            case mset_exec(args, unified_store) do
              :ok -> 1
              {:error, _} = err -> err
            end

          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)
        end
      end,
      store: store
    )
  end

  defp keys_on_one_shard?([first | rest], store) do
    shard_index = Router.shard_for(store, first)
    Enum.all?(rest, &(Router.shard_for(store, &1) == shard_index))
  end

  defp keys_on_one_shard?([], _store), do: true

  defp mset_validate([]), do: :ok

  defp mset_validate([k, _value | rest]) do
    if k == "" or byte_size(k) > @max_key_bytes do
      {:error, "ERR key too large or empty"}
    else
      mset_validate(rest)
    end
  end

  defp mset_exec([], _store), do: :ok

  defp mset_exec(args, %FerricStore.Instance{} = store) do
    Router.atomic_mset(store, mset_pairs(args))
  end

  defp mset_exec(args, store) do
    case mset_needs_compound_cleanup?(args, store) do
      {:ok, true} -> mset_exec_sequential(args, store)
      {:ok, false} -> Ops.batch_put(store, mset_pairs(args))
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp mset_needs_compound_cleanup?([], _store), do: {:ok, false}

  defp mset_needs_compound_cleanup?([k, _v | rest], store) do
    case Compound.data_structure_status(k, store) do
      :compound -> {:ok, true}
      :plain -> mset_needs_compound_cleanup?(rest, store)
      {:error, {:storage_read_failed, _reason}} = failure -> failure
    end
  end

  defp mset_pairs([]), do: []
  defp mset_pairs([k, v | rest]), do: [{k, v} | mset_pairs(rest)]

  defp mset_exec_sequential(args, store) do
    case backup_mset_originals(args, store) do
      {:ok, backups} ->
        case mset_exec_sequential_replace(args, store, []) do
          :ok ->
            :ok

          {{:error, _} = err, replaced_keys} ->
            case restore_mset_originals(replaced_keys, backups, store) do
              :ok ->
                err

              {:error, _} = rollback_error ->
                {:error, {:mset_rollback_failed, err, rollback_error}}
            end
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp mset_exec_sequential_replace([], _store, _replaced_keys), do: :ok

  defp mset_exec_sequential_replace([k, v | rest], store, replaced_keys) do
    case Compound.replace_string_key(k, v, 0, store) do
      :ok -> mset_exec_sequential_replace(rest, store, [k | replaced_keys])
      {:error, _} = err -> {err, replaced_keys}
    end
  end

  defp backup_mset_originals(args, store) do
    args
    |> extract_keys()
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, %{}}, fn key, {:ok, backups} ->
      case backup_string_key(key, store) do
        {:ok, backup} -> {:cont, {:ok, Map.put(backups, key, backup)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp backup_string_key(key, store) do
    with {:ok, plain_meta} <- read_plain_meta(key, store),
         {:ok, compound_backup} <- backup_compound_data_structure(key, store) do
      {:ok, {key, plain_meta, compound_backup}}
    end
  end

  defp read_plain_meta(key, store) do
    case Ops.get_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      meta -> {:ok, meta}
    end
  end

  defp backup_compound_data_structure(key, store) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get_meta(store, key, type_key) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        nil ->
          {:ok, nil}

        {type, type_expire_at_ms} ->
          case backup_compound_entries(key, type, store) do
            {:ok, entries} ->
              {:ok,
               %{
                 type: {type_key, type, type_expire_at_ms},
                 entries: entries
               }}

            {:error, _reason} = error ->
              error
          end
      end
    else
      {:ok, nil}
    end
  end

  defp backup_compound_entries(key, type, store) do
    with {:ok, entries} <- backup_compound_prefixes(key, type, store) do
      maybe_add_list_meta_entry(entries, key, type, store)
    end
  end

  defp backup_compound_prefixes(key, type, store) do
    key
    |> Compound.prefixes_for_type(type)
    |> Enum.reduce_while({:ok, []}, fn prefix, {:ok, entries} ->
      case scan_compound_entries(prefix, key, store) do
        {:ok, prefix_entries} -> {:cont, {:ok, prefix_entries ++ entries}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp scan_compound_entries(prefix, key, store) do
    case Ops.compound_scan(store, key, prefix) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      pairs when is_list(pairs) ->
        compound_keys =
          Enum.map(pairs, fn {field, _value} -> Compound.scanned_key(prefix, field) end)

        metas = Ops.compound_batch_get_meta(store, key, compound_keys)

        case metas do
          {:error, {:storage_read_failed, _reason}} = failure ->
            failure

          metas when is_list(metas) ->
            cond do
              length(metas) != length(compound_keys) ->
                ReadResult.failure(:compound_batch_meta_length_mismatch)

              failure = ReadResult.first_failure(metas) ->
                failure

              true ->
                entries =
                  compound_keys
                  |> Enum.zip(metas)
                  |> Enum.flat_map(fn
                    {_compound_key, nil} -> []
                    {compound_key, {value, expire_at_ms}} -> [{compound_key, value, expire_at_ms}]
                  end)

                {:ok, entries}
            end

          invalid ->
            ReadResult.failure({:invalid_compound_batch_meta_result, invalid})
        end
    end
  end

  defp maybe_add_list_meta_entry(entries, key, "list", store) do
    meta_key = CompoundKey.list_meta_key(key)

    case Ops.compound_get_meta(store, key, meta_key) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      nil -> {:ok, entries}
      {value, expire_at_ms} -> {:ok, [{meta_key, value, expire_at_ms} | entries]}
    end
  end

  defp maybe_add_list_meta_entry(entries, _key, _type, _store), do: {:ok, entries}

  defp restore_mset_originals(replaced_keys, backups, store) do
    replaced_keys
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn key, :ok ->
      backup = Map.fetch!(backups, key)

      case restore_string_key_backup(backup, store) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp restore_string_key_backup({key, plain_meta, compound_backup}, store) do
    with :ok <- Compound.clear_data_structure(key, store),
         :ok <- restore_plain_string_backup(key, plain_meta, store),
         :ok <- restore_compound_backup(key, compound_backup, store) do
      :ok
    end
  end

  defp restore_plain_string_backup(key, nil, store), do: Ops.delete(store, key)

  defp restore_plain_string_backup(key, {value, expire_at_ms}, store) do
    Ops.put(store, key, value, expire_at_ms)
  end

  defp restore_compound_backup(_key, nil, _store), do: :ok

  defp restore_compound_backup(
         key,
         %{type: {type_key, type, type_expire_at_ms}, entries: entries},
         store
       ) do
    with :ok <- Ops.compound_put(store, key, type_key, type, type_expire_at_ms),
         :ok <- Ops.compound_batch_put(store, key, entries) do
      :ok
    end
  end

  defp msetnx_any_exists?([], _store), do: {:ok, false}

  defp msetnx_any_exists?([k, _v | rest], store) do
    if Ops.exists?(store, k) do
      {:ok, true}
    else
      case Compound.data_structure_status(k, store) do
        :compound -> {:ok, true}
        :plain -> msetnx_any_exists?(rest, store)
        {:error, {:storage_read_failed, _reason}} = failure -> failure
      end
    end
  end

  defp extract_keys([]), do: []
  defp extract_keys([k, _v | rest]), do: [k | extract_keys(rest)]

  defp even_length?([]), do: true
  defp even_length?([_, _ | rest]), do: even_length?(rest)
  defp even_length?(_), do: false
end
