defmodule Ferricstore.Commands.Strings.MSet do
  @moduledoc false

  alias Ferricstore.Commands.Strings.Compound
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops

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

  defp msetnx_validated_args(args, store) do
    keys = extract_keys(args)

    CrossShardOp.execute(
      Enum.map(keys, &{&1, :write}),
      fn unified_store ->
        if msetnx_any_exists?(args, unified_store) do
          0
        else
          case mset_exec(args, unified_store) do
            :ok -> 1
            {:error, _} = err -> err
          end
        end
      end,
      intent: %{command: :msetnx, keys: %{targets: keys}},
      tx_entry: {"MSETNX", args, {:msetnx, args}},
      store: store
    )
  end

  defp mset_validate([]), do: :ok

  defp mset_validate([k, _v | rest]) do
    if k == "" or byte_size(k) > @max_key_bytes do
      {:error, "ERR key too large or empty"}
    else
      mset_validate(rest)
    end
  end

  defp mset_exec([], _store), do: :ok

  defp mset_exec(args, %FerricStore.Instance{} = store) do
    Ops.batch_put(store, mset_pairs(args))
  end

  defp mset_exec(args, store) do
    if mset_needs_compound_cleanup?(args, store) do
      mset_exec_sequential(args, store)
    else
      Ops.batch_put(store, mset_pairs(args))
    end
  end

  defp mset_needs_compound_cleanup?([], _store), do: false

  defp mset_needs_compound_cleanup?([k, _v | rest], store) do
    Compound.data_structure_key?(k, store) or mset_needs_compound_cleanup?(rest, store)
  end

  defp mset_pairs([]), do: []
  defp mset_pairs([k, v | rest]), do: [{k, v} | mset_pairs(rest)]

  defp mset_exec_sequential(args, store) do
    backups =
      args
      |> backup_mset_originals(store)
      |> Map.new(fn {key, _plain, _compound} = backup -> {key, backup} end)

    case mset_exec_sequential_replace(args, store, []) do
      :ok ->
        :ok

      {{:error, _} = err, replaced_keys} ->
        case restore_mset_originals(replaced_keys, backups, store) do
          :ok -> err
          {:error, _} = rollback_error -> {:error, {:mset_rollback_failed, err, rollback_error}}
        end
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
    |> Enum.map(&backup_string_key(&1, store))
  end

  defp backup_string_key(key, store) do
    {key, Ops.get_meta(store, key), backup_compound_data_structure(key, store)}
  end

  defp backup_compound_data_structure(key, store) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get_meta(store, key, type_key) do
        nil ->
          nil

        {type, type_expire_at_ms} ->
          %{
            type: {type_key, type, type_expire_at_ms},
            entries: backup_compound_entries(key, type, store)
          }
      end
    end
  end

  defp backup_compound_entries(key, type, store) do
    entries =
      key
      |> Compound.prefixes_for_type(type)
      |> Enum.flat_map(&scan_compound_entries(&1, key, store))

    maybe_add_list_meta_entry(entries, key, type, store)
  end

  defp scan_compound_entries(prefix, key, store) do
    store
    |> Ops.compound_scan(key, prefix)
    |> Enum.flat_map(fn {field, _value} ->
      compound_key = prefix <> field

      case Ops.compound_get_meta(store, key, compound_key) do
        nil -> []
        {value, expire_at_ms} -> [{compound_key, value, expire_at_ms}]
      end
    end)
  end

  defp maybe_add_list_meta_entry(entries, key, "list", store) do
    meta_key = CompoundKey.list_meta_key(key)

    case Ops.compound_get_meta(store, key, meta_key) do
      nil -> entries
      {value, expire_at_ms} -> [{meta_key, value, expire_at_ms} | entries]
    end
  end

  defp maybe_add_list_meta_entry(entries, _key, _type, _store), do: entries

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

  defp msetnx_any_exists?([], _store), do: false

  defp msetnx_any_exists?([k, _v | rest], store) do
    if Ops.exists?(store, k) or Compound.data_structure_key?(k, store),
      do: true,
      else: msetnx_any_exists?(rest, store)
  end

  defp extract_keys([]), do: []
  defp extract_keys([k, _v | rest]), do: [k | extract_keys(rest)]

  defp even_length?([]), do: true
  defp even_length?([_, _ | rest]), do: even_length?(rest)
  defp even_length?(_), do: false
end
