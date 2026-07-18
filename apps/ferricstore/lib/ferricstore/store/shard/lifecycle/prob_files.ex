defmodule Ferricstore.Store.Shard.Lifecycle.ProbFiles do
  @moduledoc false

  alias Ferricstore.ProbFile
  alias Ferricstore.Store.CompoundKey

  require Logger

  @spec validate(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def validate(shard_data_path, shard_index), do: validate(shard_data_path, shard_index, nil)

  @spec validate(binary(), non_neg_integer(), :ets.tid() | atom() | nil) ::
          :ok | {:error, term()}
  def validate(shard_data_path, shard_index, keydir) do
    prob_dir = Path.join(shard_data_path, "prob")

    case Ferricstore.FS.ls(prob_dir) do
      {:ok, files} -> validate_files(prob_dir, files, shard_index, keydir)
      {:error, {:not_found, _message}} -> :ok
      {:error, reason} -> {:error, {:list_prob_dir_failed, prob_dir, reason}}
    end
  end

  defp validate_files(prob_dir, files, shard_index, keydir) do
    with {:ok, staged_removed?, canonical_files, mutation_receipts, pending_receipts} <-
           validate_directory_entries(prob_dir, files, shard_index),
         {:ok, pending_reconciled?, mutation_receipts} <-
           reconcile_pending_receipts(
             prob_dir,
             canonical_files,
             mutation_receipts,
             pending_receipts,
             shard_index
           ),
         {:ok, detached_removed?, mutation_receipts} <-
           remove_detached_receipts(prob_dir, canonical_files, mutation_receipts, shard_index),
         {:ok, reconciled?, mutation_receipts} <-
           reconcile_exact_catalog(
             prob_dir,
             canonical_files,
             mutation_receipts,
             keydir,
             shard_index
           ),
         :ok <- recover_mutation_receipts(prob_dir, mutation_receipts) do
      finalize_validation(
        staged_removed? or pending_reconciled? or detached_removed? or reconciled?,
        not is_nil(keydir),
        prob_dir
      )
    end
  end

  defp validate_directory_entries(prob_dir, files, shard_index) do
    Enum.reduce_while(
      files,
      {:ok, false, MapSet.new(), MapSet.new(), MapSet.new()},
      fn filename, {:ok, removed?, canonical_files, mutation_receipts, pending_receipts} ->
        cond do
          ProbFile.valid_filename?(filename) ->
            case validate_regular_file(prob_dir, filename) do
              :ok ->
                {:cont,
                 {:ok, removed?, MapSet.put(canonical_files, filename), mutation_receipts,
                  pending_receipts}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end

          ProbFile.mutation_filename?(filename) ->
            case validate_regular_file(prob_dir, filename) do
              :ok ->
                {:cont,
                 {:ok, removed?, canonical_files, MapSet.put(mutation_receipts, filename),
                  pending_receipts}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end

          ProbFile.pending_mutation_filename?(filename) ->
            case validate_regular_file(prob_dir, filename) do
              :ok ->
                {:cont,
                 {:ok, removed?, canonical_files, mutation_receipts,
                  MapSet.put(pending_receipts, filename)}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end

          ProbFile.staged_filename?(filename) or ProbFile.pending_create_filename?(filename) ->
            case Ferricstore.FS.rm(Path.join(prob_dir, filename)) do
              :ok ->
                Logger.warning(
                  "Shard #{shard_index}: removed incomplete probabilistic sidecar #{filename}"
                )

                {:cont, {:ok, true, canonical_files, mutation_receipts, pending_receipts}}

              {:error, reason} ->
                {:halt, {:error, {:remove_staged_prob_file_failed, filename, reason}}}
            end

          true ->
            {:halt, {:error, {:invalid_prob_filename, filename}}}
        end
      end
    )
  end

  defp reconcile_pending_receipts(
         prob_dir,
         canonical_files,
         mutation_receipts,
         pending_receipts,
         shard_index
       ) do
    Enum.reduce_while(
      Enum.sort(pending_receipts),
      {:ok, false, mutation_receipts},
      fn pending_receipt, {:ok, _changed?, receipts} ->
        {:ok, target} = ProbFile.pending_mutation_target_filename(pending_receipt)
        pending_path = Path.join(prob_dir, pending_receipt)

        if MapSet.member?(canonical_files, target) do
          receipt = target <> ".mutation"

          case Ferricstore.FS.rename(pending_path, Path.join(prob_dir, receipt)) do
            :ok ->
              Logger.warning(
                "Shard #{shard_index}: completed probabilistic mutation receipt publication #{receipt}"
              )

              {:cont, {:ok, true, MapSet.put(receipts, receipt)}}

            {:error, reason} ->
              {:halt, {:error, {:publish_pending_prob_receipt_failed, pending_receipt, reason}}}
          end
        else
          case Ferricstore.FS.rm(pending_path) do
            :ok ->
              Logger.warning(
                "Shard #{shard_index}: removed incomplete probabilistic mutation receipt #{pending_receipt}"
              )

              {:cont, {:ok, true, receipts}}

            {:error, reason} ->
              {:halt, {:error, {:remove_pending_prob_receipt_failed, pending_receipt, reason}}}
          end
        end
      end
    )
  end

  defp remove_detached_receipts(prob_dir, canonical_files, mutation_receipts, shard_index) do
    Enum.reduce_while(
      Enum.sort(mutation_receipts),
      {:ok, false, mutation_receipts},
      fn receipt, {:ok, removed?, remaining} ->
        {:ok, target} = ProbFile.mutation_target_filename(receipt)

        if MapSet.member?(canonical_files, target) do
          {:cont, {:ok, removed?, remaining}}
        else
          case Ferricstore.FS.rm(Path.join(prob_dir, receipt)) do
            :ok ->
              Logger.warning(
                "Shard #{shard_index}: removed orphaned probabilistic mutation receipt #{receipt}"
              )

              {:cont, {:ok, true, MapSet.delete(remaining, receipt)}}

            {:error, reason} ->
              {:halt, {:error, {:remove_orphaned_prob_receipt_failed, receipt, reason}}}
          end
        end
      end
    )
  end

  defp reconcile_exact_catalog(
         _prob_dir,
         _canonical_files,
         mutation_receipts,
         nil,
         _shard_index
       ),
       do: {:ok, false, mutation_receipts}

  defp reconcile_exact_catalog(
         prob_dir,
         canonical_files,
         mutation_receipts,
         keydir,
         shard_index
       ) do
    with {:ok, keydir} <- fetch_keydir(keydir),
         {:ok, orphan_files} <- catalog_orphans(keydir, canonical_files) do
      remove_catalog_orphans(
        prob_dir,
        orphan_files,
        mutation_receipts,
        shard_index
      )
    end
  rescue
    ArgumentError -> {:error, :prob_type_catalog_unavailable}
  end

  defp remove_catalog_orphans(prob_dir, orphan_files, mutation_receipts, shard_index) do
    Enum.reduce_while(
      Enum.sort(orphan_files),
      {:ok, false, mutation_receipts},
      fn filename, {:ok, _removed?, remaining_receipts} ->
        receipt = filename <> ".mutation"

        paths =
          if MapSet.member?(remaining_receipts, receipt),
            do: [filename, receipt],
            else: [filename]

        case remove_catalog_orphan_paths(prob_dir, paths) do
          :ok ->
            Logger.warning(
              "Shard #{shard_index}: removed orphaned probabilistic sidecar #{filename}"
            )

            {:cont, {:ok, true, MapSet.delete(remaining_receipts, receipt)}}

          {:error, reason} ->
            {:halt, {:error, {:remove_orphaned_prob_file_failed, filename, reason}}}
        end
      end
    )
  end

  defp remove_catalog_orphan_paths(prob_dir, paths) do
    Enum.reduce_while(paths, :ok, fn filename, :ok ->
      case Ferricstore.FS.rm(Path.join(prob_dir, filename)) do
        :ok -> {:cont, :ok}
        {:error, {:not_found, _message}} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp recover_mutation_receipts(prob_dir, mutation_receipts) do
    Enum.reduce_while(Enum.sort(mutation_receipts), :ok, fn receipt, :ok ->
      {:ok, target} = ProbFile.mutation_target_filename(receipt)
      extension = target |> Path.extname() |> String.trim_leading(".")
      target_path = Path.join(prob_dir, target)

      case Ferricstore.Bitcask.NIF.prob_file_recover(target_path, extension) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:recover_prob_mutation_failed, target, reason}}}
      end
    end)
  end

  defp fetch_keydir(keydir) when is_reference(keydir), do: {:ok, keydir}

  defp fetch_keydir(keydir) when is_atom(keydir) do
    case :ets.whereis(keydir) do
      :undefined -> {:error, :prob_type_catalog_unavailable}
      tid -> {:ok, tid}
    end
  end

  defp fetch_keydir(_keydir), do: {:error, :prob_type_catalog_unavailable}

  defp catalog_orphans(keydir, canonical_files) do
    :ets.foldl(
      fn
        _row, {:error, _reason} = error ->
          error

        {<<"T:", _rest::binary>> = storage_key, value, _expire_at_ms, _lfu, _file_id, _offset,
         _value_size},
        {:ok, orphan_files} ->
          case CompoundKey.type_name(value) do
            type when type in ~w(bloom cms cuckoo topk) ->
              filename =
                storage_key
                |> CompoundKey.extract_redis_key()
                |> ProbFile.filename(type)

              {:ok, MapSet.delete(orphan_files, filename)}

            type when is_binary(type) ->
              {:ok, orphan_files}

            _invalid ->
              {:error, {:invalid_prob_type_catalog_entry, storage_key}}
          end

        _row, {:ok, orphan_files} ->
          {:ok, orphan_files}
      end,
      {:ok, canonical_files},
      keydir
    )
  end

  defp validate_regular_file(prob_dir, filename) do
    case File.lstat(Path.join(prob_dir, filename)) do
      {:ok, %File.Stat{type: :regular}} ->
        :ok

      {:ok, %File.Stat{type: type}} ->
        {:error, {:invalid_prob_file_type, filename, type}}

      {:error, reason} ->
        {:error, {:stat_prob_file_failed, filename, reason}}
    end
  end

  defp finalize_validation(false, false, _prob_dir), do: :ok

  defp finalize_validation(_removed?, _exact_catalog?, prob_dir) do
    case Ferricstore.Bitcask.NIF.v2_fsync_dir(prob_dir) do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_prob_dir_failed, prob_dir, reason}}
    end
  end
end
