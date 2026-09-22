defmodule Ferricstore.Store.StandaloneTxLog do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.AppendResult
  alias Ferricstore.TermCodec

  @file_name "standalone_cross_shard_tx.log"
  @manifest_file_name "standalone_cross_shard_tx.manifest"
  @recovery_marker_file_name "standalone_cross_shard_recovery.required"
  @magic :ferricstore_standalone_cross_shard_tx_v1
  @manifest_magic :ferricstore_standalone_cross_shard_tx_manifest_v1
  @recovery_marker_magic :ferricstore_standalone_cross_shard_recovery_marker_v1
  @compact_threshold_bytes 4 * 1_024 * 1_024
  @max_journal_bytes 64 * 1_024 * 1_024
  # Legacy compressed terms must fit within the bounded journal read.
  @max_legacy_uncompressed_bytes @max_journal_bytes
  @max_manifest_bytes 1_024
  @max_recovery_marker_bytes 1_024
  @terminal_reserve_bytes 1_024
  @max_txid_bytes 128

  @type group :: {binary(), list()}

  @spec prepare(binary(), [group()]) :: {:ok, binary()} | {:error, term()}
  def prepare(data_dir, groups) when is_binary(data_dir) and is_list(groups) do
    if valid_groups?(data_dir, groups) do
      txid = new_txid()
      stats = group_stats(groups)

      case with_journal_lock(data_dir, fn ->
             case recovery_required_reason(data_dir) do
               nil ->
                 persist_prepare_locked(data_dir, txid, groups)

               reason ->
                 {:error, {:standalone_tx_recovery_required, reason}}
             end
           end) do
        {:ok, ^txid} ->
          observe(:prepare, stats, %{status: :ok})
          {:ok, txid}

        {:error, _reason} = error ->
          observe(:prepare, stats, %{status: :error})
          error
      end
    else
      {:error, :invalid_groups}
    end
  end

  @doc """
  Persists a commit marker before running best-effort maintenance compaction.

  Compaction may drop completed terminal history, so a later duplicate call can
  return `{:error, :unknown_txid}` once the journal has been compacted.
  """
  @spec commit(binary(), binary()) :: :ok | {:error, term()}
  def commit(data_dir, txid) when is_binary(data_dir) and is_binary(txid) do
    case append_terminal(data_dir, :commit, txid) do
      :ok ->
        observe(:commit, %{count: 1}, %{status: :ok})
        :ok

      {:error, _reason} = error ->
        observe(:commit, %{count: 1}, %{status: :error})
        error
    end
  end

  @spec abort(binary(), binary()) :: :ok | {:error, term()}
  def abort(data_dir, txid) when is_binary(data_dir) and is_binary(txid) do
    case append_terminal(data_dir, :abort, txid) do
      :ok ->
        observe(:abort, %{count: 1}, %{status: :ok})
        :ok

      {:error, {:standalone_tx_abort_persistence_failed, ^txid, reason}} ->
        recovery_reason = {:standalone_tx_abort_recovery_required, txid, reason}
        :ok = require_recovery(data_dir, recovery_reason)
        observe(:abort, %{count: 1}, %{status: :error})
        {:error, recovery_reason}

      {:error, _reason} = error ->
        observe(:abort, %{count: 1}, %{status: :error})
        error
    end
  end

  @spec recover_once(binary()) :: :ok | {:error, term()}
  def recover_once(data_dir) when is_binary(data_dir), do: recover(data_dir)

  @spec recovery_marker_path(binary()) :: binary()
  def recovery_marker_path(data_dir) when is_binary(data_dir) do
    Path.join(Path.expand(data_dir), @recovery_marker_file_name)
  end

  @spec recovery_required?(binary()) :: boolean()
  def recovery_required?(data_dir) when is_binary(data_dir) do
    not is_nil(recovery_required_reason(data_dir))
  end

  @spec recovery_required_reason(binary()) :: term() | nil
  def recovery_required_reason(data_dir) when is_binary(data_dir) do
    case :persistent_term.get(recovery_key(data_dir), :standalone_recovery_term_absent) do
      :standalone_recovery_term_absent ->
        load_recovery_marker(data_dir)

      reason ->
        reason
    end
  end

  @spec startup_recovery_reason(binary()) :: term() | nil
  def startup_recovery_reason(data_dir) when is_binary(data_dir) do
    case recovery_required_reason(data_dir) do
      nil -> journal_startup_recovery_reason(data_dir)
      reason -> reason
    end
  end

  @spec require_recovery(binary(), term()) :: :ok | {:error, term()}
  def require_recovery(data_dir, reason) when is_binary(data_dir) do
    result =
      with_journal_lock(data_dir, fn ->
        case persist_recovery_marker_locked(data_dir, reason) do
          :ok ->
            mark_recovery_required(data_dir, reason)
            :ok

          {:error, _reason} = error ->
            mark_recovery_required(data_dir, reason)
            error
        end
      end)

    case result do
      {:error, _reason} = error ->
        mark_recovery_required(data_dir, reason)
        error

      other ->
        other
    end
  end

  @spec recover(binary()) :: :ok | {:error, term()}
  def recover(data_dir) when is_binary(data_dir) do
    result = with_journal_lock(data_dir, fn -> recover_locked(data_dir) end)

    case result do
      {:ok, stats} ->
        observe(:recover, stats, %{status: :ok})
        :ok

      {:error, reason} = error ->
        observe(:recover, %{pending: 0, replayed: 0, groups: 0, ops: 0}, %{
          status: :error,
          reason: inspect(reason)
        })

        error
    end
  end

  defp recover_locked(data_dir) do
    recovery_reason = recovery_required_reason(data_dir)

    with {:ok, manifest} <- read_manifest(data_dir),
         :ok <- finish_existing_compaction(data_dir, manifest),
         {:ok, pending} <- read_pending_transactions(data_dir),
         {:ok, stats} <- recover_pending(data_dir, pending),
         :ok <- compact_committed_locked(data_dir, nil),
         :ok <- clear_recovery_marker_locked(data_dir, recovery_reason) do
      clear_recovery_required(data_dir)
      {:ok, stats}
    end
  end

  defp append_terminal(data_dir, terminal, txid) when terminal in [:commit, :abort] do
    if valid_txid?(txid) do
      case recovery_required_reason(data_dir) do
        nil ->
          result =
            with_journal_lock(data_dir, fn ->
              with {:ok, manifest} <- read_manifest(data_dir),
                   {:ok, journal_state} <- read_journal_state(data_dir) do
                case terminal_status(journal_state, manifest, txid) do
                  {:terminal, ^terminal} when is_nil(manifest) ->
                    with :ok <- fsync_journal_file(data_dir) do
                      fsync_dir(Path.dirname(path(data_dir)))
                    end

                  {:terminal, ^terminal} ->
                    compact_committed_locked(data_dir, manifest)

                  {:terminal, other_terminal} ->
                    {:error, {:transaction_already_terminal, other_terminal}}

                  :pending ->
                    with :ok <- finish_existing_compaction(data_dir, manifest) do
                      persist_terminal_locked(data_dir, terminal, txid, true)
                    end

                  :unknown ->
                    {:error, :unknown_txid}
                end
              end
            end)

          case result do
            {:error, {:transaction_already_terminal, _reason}} = error -> error
            {:error, :unknown_txid} = error -> error
            {:error, {:standalone_tx_recovery_required, _reason}} = error -> error
            {:error, _reason} = error -> tag_abort_terminal_failure(terminal, txid, error)
            other -> other
          end

        reason ->
          {:error, {:standalone_tx_recovery_required, reason}}
      end
    else
      {:error, :invalid_txid}
    end
  end

  defp maybe_compact_committed_locked(data_dir, retry_terminal) do
    case File.lstat(path(data_dir)) do
      {:ok, %File.Stat{type: :regular, size: size}} when size >= @compact_threshold_bytes ->
        compact_committed_locked(data_dir, retry_terminal)

      {:ok, %File.Stat{type: :regular}} ->
        :ok

      {:ok, %File.Stat{type: type}} ->
        {:error, {:unsafe_journal_type, type}}

      {:error, :enoent} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp compact_committed_locked(data_dir, retry_terminal) do
    with {:ok, journal_state} <- read_journal_state(data_dir),
         {:ok, persisted_manifest} <- read_manifest(data_dir),
         manifest <- retry_terminal || persisted_manifest,
         :ok <- publish_manifest(data_dir, manifest),
         pending <- pending_transactions(journal_state),
         pending <- reject_retry_terminal(pending, manifest),
         :ok <- rewrite_entries(data_dir, pending_entries(pending)),
         :ok <- clear_manifest(data_dir, manifest) do
      :ok
    end
  end

  defp new_txid do
    unique = System.unique_integer([:positive, :monotonic])
    "#{System.system_time(:nanosecond)}-#{unique}"
  end

  defp recover_pending(data_dir, pending) do
    initial_stats = %{pending: length(pending), replayed: 0, groups: 0, ops: 0}

    pending
    |> Enum.reduce_while({:ok, initial_stats}, fn {txid, groups}, {:ok, stats} ->
      group_stats = group_stats(groups)

      case apply_groups(groups) do
        :ok ->
          case persist_terminal_locked(data_dir, :commit, txid, false) do
            :ok ->
              next_stats = %{
                stats
                | replayed: stats.replayed + 1,
                  groups: stats.groups + group_stats.groups,
                  ops: stats.ops + group_stats.ops
              }

              {:cont, {:ok, next_stats}}

            {:error, reason} ->
              {:halt, {:error, {:commit_after_recover_failed, txid, reason}}}
          end

        {:error, reason} ->
          {:halt, {:error, {:recover_tx_failed, txid, reason}}}
      end
    end)
  end

  defp pending_entries(pending),
    do: Enum.map(pending, fn {txid, groups} -> {@magic, :prepare, txid, groups} end)

  defp pending_transactions({order, prepares, terminals}) do
    order
    |> Enum.reverse()
    |> Enum.reject(&Map.has_key?(terminals, &1))
    |> Enum.map(&{&1, Map.fetch!(prepares, &1)})
  end

  defp terminal_status({_order, prepares, terminals}, manifest, txid) do
    case Map.get(terminals, txid) do
      terminal when terminal in [:commit, :abort] ->
        {:terminal, terminal}

      nil ->
        case manifest do
          {terminal, ^txid} -> {:terminal, terminal}
          _ -> if Map.has_key?(prepares, txid), do: :pending, else: :unknown
        end
    end
  end

  defp apply_groups(groups) do
    Enum.reduce_while(groups, :ok, fn {file_path, batch}, :ok ->
      case append_batch_sync(file_path, batch) do
        {:ok, _locations} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {file_path, reason}}}
      end
    end)
  end

  defp append_batch_sync(file_path, batch) do
    with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(file_path)) do
      do_append_batch_sync(file_path, batch)
    end
  end

  defp do_append_batch_sync(file_path, batch) do
    if Enum.any?(batch, &match?({:delete, _, _}, &1)) do
      ops =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {:put, key, value, expire_at_ms}
          {:delete, key, _prob_path} -> {:delete, key}
        end)

      case NIF.v2_append_ops_batch(file_path, ops) do
        {:ok, locations} ->
          with :ok <- AppendResult.validate_operation_locations(locations, ops) do
            {:ok, locations}
          end

        {:error, _reason} = error ->
          error
      end
    else
      puts =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {key, value, expire_at_ms}
        end)

      case NIF.v2_append_batch(file_path, puts) do
        {:ok, locations} ->
          with :ok <- AppendResult.validate_locations(locations, length(puts)) do
            {:ok, Enum.map(locations, fn {offset, value_size} -> {:put, offset, value_size} end)}
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp persist_prepare_locked(data_dir, txid, groups) do
    entry = {@magic, :prepare, txid, groups}
    line = encode_entry(entry) <> "\n"

    if prepare_append_would_exceed_limit?(data_dir, line) do
      case append_prepare_entry_locked(data_dir, entry, @terminal_reserve_bytes) do
        :ok -> {:ok, txid}
        {:error, reason} -> {:error, reason}
      end
    else
      with {:ok, snapshot} <- read_journal_snapshot(data_dir) do
        case append_prepare_entry_locked(data_dir, entry, @terminal_reserve_bytes) do
          :ok ->
            {:ok, txid}

          {:error, append_reason} ->
            handle_prepare_append_error(data_dir, txid, groups, snapshot, append_reason)
        end
      end
    end
  end

  defp prepare_append_would_exceed_limit?(data_dir, line) do
    case File.lstat(path(data_dir)) do
      {:ok, %File.Stat{type: :regular, size: size}} ->
        size + byte_size(line) > @max_journal_bytes - @terminal_reserve_bytes

      _ ->
        false
    end
  end

  defp append_prepare_entry_locked(data_dir, entry, reserve_bytes) do
    path = path(data_dir)
    dir = Path.dirname(path)
    line = encode_entry(entry) <> "\n"
    append_limit = @max_journal_bytes - reserve_bytes

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- append_sync_nofollow_bounded(path, line, append_limit) do
      case fsync_dir(dir) do
        :ok -> :ok
        {:error, reason} -> {:error, {:prepare_fsync_failed, reason}}
        other -> {:error, {:prepare_fsync_failed, other}}
      end
    else
      {:error, {:too_large, reason}} -> {:error, {:journal_limit_exceeded, reason}}
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp persist_terminal_locked(data_dir, terminal, txid, compact?, reclaim? \\ true) do
    with {:ok, snapshot} <- read_journal_snapshot(data_dir) do
      case append_entry_locked(data_dir, {@magic, terminal, txid}) do
        :ok ->
          terminal_persisted(data_dir, compact?, terminal, txid)

        {:error, reason} ->
          handle_terminal_append_error(
            data_dir,
            terminal,
            txid,
            snapshot,
            reason,
            compact?,
            reclaim?
          )
      end
    end
  end

  defp rewrite_terminal_locked(data_dir, terminal, txid) do
    case compact_committed_locked(data_dir, nil) do
      :ok -> persist_terminal_locked(data_dir, terminal, txid, true, false)
      {:error, reason} -> {:error, {:terminal_space_reclaim_failed, reason}}
    end
  end

  defp handle_prepare_append_error(data_dir, txid, groups, snapshot, append_reason) do
    case append_reason do
      {:prepare_fsync_failed, reason} ->
        recovery_reason =
          {:standalone_tx_prepare_recovery_required, txid, reason, :unknown, :unknown}

        _ = require_recovery(data_dir, recovery_reason)
        {:error, recovery_reason}

      _ ->
        handle_prepare_append_error_after_visibility(
          data_dir,
          txid,
          groups,
          snapshot,
          append_reason
        )
    end
  end

  defp handle_prepare_append_error_after_visibility(
         data_dir,
         txid,
         groups,
         snapshot,
         append_reason
       ) do
    case prepare_visibility(data_dir, txid, groups) do
      :visible ->
        case fsync_journal_file_and_dir(data_dir) do
          :ok ->
            {:ok, txid}

          {:error, establishment_reason} ->
            restore_prepare_after_error(
              data_dir,
              txid,
              snapshot,
              append_reason,
              establishment_reason
            )
        end

      visibility ->
        restore_prepare_after_error(
          data_dir,
          txid,
          snapshot,
          append_reason,
          {:prepare_not_visible, visibility}
        )
    end
  end

  defp prepare_visibility(data_dir, txid, groups) do
    case read_journal_state(data_dir) do
      {:ok, {_order, prepares, _terminals}} ->
        if Map.get(prepares, txid) == groups, do: :visible, else: :absent

      {:error, _reason} ->
        :corrupt
    end
  end

  defp fsync_journal_file_and_dir(data_dir) do
    with :ok <- fsync_journal_file(data_dir) do
      fsync_dir(Path.dirname(path(data_dir)))
    end
  end

  defp restore_prepare_after_error(
         data_dir,
         txid,
         snapshot,
         append_reason,
         establishment_reason
       ) do
    case restore_journal(data_dir, snapshot) do
      :ok ->
        {:error, append_reason}

      {:error, rollback_reason} ->
        reason =
          {:standalone_tx_prepare_recovery_required, txid, append_reason, establishment_reason,
           rollback_reason}

        _ = require_recovery(data_dir, reason)
        {:error, reason}
    end
  end

  defp terminal_persisted(_data_dir, false, _terminal, _txid), do: :ok

  defp terminal_persisted(data_dir, true, terminal, txid),
    do: maybe_compact_committed_locked(data_dir, {terminal, txid})

  defp journal_limit_error?({:journal_limit_exceeded, _reason}), do: true
  defp journal_limit_error?(_reason), do: false

  defp handle_terminal_append_error(
         data_dir,
         terminal,
         txid,
         snapshot,
         append_reason,
         compact?,
         reclaim?
       ) do
    case terminal_visibility(data_dir, terminal, txid) do
      :visible ->
        with :ok <- fsync_journal_file(data_dir),
             :ok <- fsync_dir(Path.dirname(path(data_dir))) do
          terminal_persisted(data_dir, compact?, terminal, txid)
        end

      :absent ->
        if reclaim? and journal_limit_error?(append_reason) do
          rewrite_terminal_locked(data_dir, terminal, txid)
        else
          restore_after_append_error(data_dir, snapshot, append_reason)
        end

      :corrupt ->
        restore_after_append_error(data_dir, snapshot, append_reason)
    end
  end

  defp tag_abort_terminal_failure(:abort, txid, {:error, reason}),
    do: {:error, {:standalone_tx_abort_persistence_failed, txid, reason}}

  defp tag_abort_terminal_failure(_terminal, _txid, result), do: result

  defp terminal_visibility(data_dir, terminal, txid) do
    case read_journal_state(data_dir) do
      {:ok, {_order, _prepares, terminals}} ->
        if Map.get(terminals, txid) == terminal, do: :visible, else: :absent

      {:error, _reason} ->
        :corrupt
    end
  end

  defp restore_after_append_error(data_dir, snapshot, append_reason) do
    case restore_journal(data_dir, snapshot) do
      :ok -> {:error, append_reason}
      {:error, repair_reason} -> {:error, {:journal_repair_failed, append_reason, repair_reason}}
    end
  end

  defp restore_journal(data_dir, {present?, contents, _journal_state}) do
    journal_path = path(data_dir)
    dir = Path.dirname(journal_path)

    if present? do
      with :ok <-
             Ferricstore.FS.atomic_replace_nofollow(journal_path, contents, @max_journal_bytes),
           :ok <- fsync_dir(dir) do
        :ok
      end
    else
      case Ferricstore.FS.rm(journal_path) do
        :ok -> fsync_dir(dir)
        {:error, {:not_found, _reason}} -> fsync_dir(dir)
        {:error, _reason} = error -> error
      end
    end
  end

  defp append_entry_locked(data_dir, entry, reserve_bytes \\ 0) do
    path = path(data_dir)
    dir = Path.dirname(path)
    line = encode_entry(entry) <> "\n"
    append_limit = @max_journal_bytes - reserve_bytes

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- append_sync_nofollow_bounded(path, line, append_limit),
         :ok <- fsync_dir(dir) do
      :ok
    else
      {:error, {:too_large, reason}} -> {:error, {:journal_limit_exceeded, reason}}
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp rewrite_entries(data_dir, []) do
    path = path(data_dir)
    dir = Path.dirname(path)

    with :ok <- compaction_hook(:before_journal_rewrite, data_dir) do
      case Ferricstore.FS.rm(path) do
        :ok ->
          with :ok <- compaction_hook(:after_journal_remove, data_dir),
               :ok <- fsync_dir(dir) do
            :ok
          end

        {:error, {:not_found, _}} ->
          :ok

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp rewrite_entries(data_dir, entries) do
    path = path(data_dir)
    dir = Path.dirname(path)

    data =
      entries |> Enum.map(fn entry -> [encode_entry(entry), "\n"] end) |> IO.iodata_to_binary()

    with :ok <- compaction_hook(:before_journal_rewrite, data_dir),
         :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- Ferricstore.FS.atomic_replace_nofollow(path, data, @max_journal_bytes),
         :ok <- fsync_dir(dir),
         :ok <- compaction_hook(:after_journal_rewrite, data_dir) do
      :ok
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp publish_manifest(_data_dir, nil), do: :ok

  defp publish_manifest(data_dir, manifest) do
    path = manifest_path(data_dir)
    dir = Path.dirname(path)
    data = encode_manifest(manifest)

    with :ok <- compaction_hook(:before_manifest_publish, data_dir),
         :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- Ferricstore.FS.atomic_replace_nofollow(path, data, @max_manifest_bytes),
         :ok <- fsync_dir(dir),
         :ok <- compaction_hook(:after_manifest_publish, data_dir) do
      :ok
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp clear_manifest(_data_dir, nil), do: :ok

  defp clear_manifest(data_dir, _manifest) do
    path = manifest_path(data_dir)
    dir = Path.dirname(path)

    with :ok <- compaction_hook(:before_manifest_cleanup, data_dir) do
      case Ferricstore.FS.rm(path) do
        :ok ->
          with :ok <- compaction_hook(:after_manifest_remove, data_dir),
               :ok <- fsync_dir(dir) do
            :ok
          end

        {:error, {:not_found, _}} ->
          :ok

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp finish_existing_compaction(_data_dir, nil), do: :ok

  defp finish_existing_compaction(data_dir, manifest),
    do: compact_committed_locked(data_dir, manifest)

  defp reject_retry_terminal(pending, nil), do: pending

  defp reject_retry_terminal(pending, {_terminal, txid}) do
    Enum.reject(pending, fn {pending_txid, _groups} -> pending_txid == txid end)
  end

  defp read_manifest(data_dir) do
    case Ferricstore.FS.read_nofollow(manifest_path(data_dir), @max_manifest_bytes) do
      {:ok, contents} ->
        case String.split(contents, "\n", trim: true) do
          [line] -> decode_manifest(line)
          _ -> {:error, :corrupt_manifest}
        end

      {:error, {:not_found, _reason}} ->
        {:ok, nil}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_manifest(line) do
    with {:ok, binary} <- Base.decode64(line),
         {:ok, term} <- TermCodec.decode(binary),
         {:ok, manifest} <- valid_manifest(term) do
      {:ok, manifest}
    else
      _ -> {:error, :corrupt_manifest}
    end
  end

  defp valid_manifest({@manifest_magic, :compaction, terminal, txid})
       when terminal in [:commit, :abort] do
    if valid_txid?(txid), do: {:ok, {terminal, txid}}, else: :error
  end

  defp valid_manifest(_other), do: :error

  defp encode_manifest({terminal, txid}) do
    Base.encode64(TermCodec.encode({@manifest_magic, :compaction, terminal, txid})) <> "\n"
  end

  defp persist_recovery_marker_locked(data_dir, reason) do
    path = recovery_marker_path(data_dir)

    with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(path)),
         :ok <-
           Ferricstore.FS.atomic_replace_nofollow(
             path,
             encode_recovery_marker(reason),
             @max_recovery_marker_bytes
           ),
         :ok <- fsync_dir(Path.dirname(path)) do
      :ok
    end
  end

  defp clear_recovery_marker_locked(data_dir, recovery_reason) do
    path = recovery_marker_path(data_dir)

    case Ferricstore.FS.rm(path) do
      :ok ->
        case fsync_dir(Path.dirname(path)) do
          :ok ->
            :ok

          {:error, _reason} = error ->
            restore_recovery_marker_after_clear_failure(data_dir, recovery_reason)
            error

          other ->
            restore_recovery_marker_after_clear_failure(data_dir, recovery_reason)
            {:error, {:recovery_marker_fsync_failed, other}}
        end

      {:error, {:not_found, _reason}} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp restore_recovery_marker_after_clear_failure(_data_dir, nil), do: :ok

  defp restore_recovery_marker_after_clear_failure(data_dir, recovery_reason) do
    _ = persist_recovery_marker_locked(data_dir, recovery_reason)
    :ok
  end

  defp encode_recovery_marker(reason) do
    marker_reason = inspect(reason, limit: 12, printable_limit: 384)
    payload = TermCodec.encode({@recovery_marker_magic, marker_reason})
    encoded = Base.encode64(payload) <> "\n"

    if byte_size(encoded) <= @max_recovery_marker_bytes do
      encoded
    else
      Base.encode64(TermCodec.encode({@recovery_marker_magic, "truncated"})) <> "\n"
    end
  end

  defp load_recovery_marker(data_dir) do
    case Ferricstore.FS.read_nofollow(recovery_marker_path(data_dir), @max_recovery_marker_bytes) do
      {:ok, contents} ->
        case decode_recovery_marker(contents) do
          {:ok, reason} ->
            mark_recovery_required(data_dir, reason)
            reason

          {:error, reason} ->
            marker_reason = {:standalone_recovery_marker_invalid, reason}
            mark_recovery_required(data_dir, marker_reason)
            marker_reason
        end

      {:error, {:not_found, _reason}} ->
        nil

      {:error, reason} ->
        marker_reason = {:standalone_recovery_marker_unreadable, reason}
        mark_recovery_required(data_dir, marker_reason)
        marker_reason
    end
  end

  defp decode_recovery_marker(contents) do
    with [line] <- String.split(contents, "\n", trim: true),
         {:ok, binary} <- Base.decode64(line),
         {:ok, term} <- TermCodec.decode(binary),
         {:ok, reason} <- valid_recovery_marker(term) do
      {:ok, reason}
    else
      _ -> {:error, :corrupt_recovery_marker}
    end
  end

  defp valid_recovery_marker({@recovery_marker_magic, reason}) when is_binary(reason),
    do: {:ok, reason}

  defp valid_recovery_marker(_term), do: {:error, :invalid_recovery_marker}

  defp compaction_hook(stage, data_dir) do
    case Application.get_env(:ferricstore, :standalone_tx_log_compaction_hook) do
      hook when is_function(hook, 2) ->
        case hook.(stage, data_dir) do
          :ok -> :ok
          {:error, reason} -> {:error, {:compaction_interrupted, stage, reason}}
          other -> {:error, {:invalid_compaction_hook_result, stage, other}}
        end

      _ ->
        :ok
    end
  end

  defp read_pending_transactions(data_dir) do
    with {:ok, journal_state} <- read_journal_state(data_dir) do
      {:ok, pending_transactions(journal_state)}
    end
  end

  defp read_journal_state(data_dir) do
    with {:ok, {_present, _contents, journal_state}} <- read_journal_snapshot(data_dir) do
      {:ok, journal_state}
    end
  end

  defp read_journal_snapshot(data_dir) do
    journal_path = path(data_dir)

    case Ferricstore.FS.read_nofollow(journal_path, @max_journal_bytes) do
      {:ok, contents} ->
        with {:ok, journal_state} <- decode_journal_contents(contents, data_dir) do
          {:ok, {true, contents, journal_state}}
        end

      {:error, {:not_found, _reason}} ->
        {:ok, {false, <<>>, {[], %{}, %{}}}}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_journal_contents(contents, data_dir) do
    {journal_state, skipped} = reduce_journal(contents, data_dir, {[], %{}, %{}}, 0)

    if skipped > 0 do
      observe(:corrupt_entry, %{count: skipped}, %{data_dir_hash: :erlang.phash2(data_dir)})
      {:error, {:corrupt_entries, skipped}}
    else
      {:ok, journal_state}
    end
  end

  defp journal_startup_recovery_reason(data_dir) do
    case File.lstat(path(data_dir)) do
      {:ok, %File.Stat{type: :regular, size: size}} when size > 0 ->
        case read_journal_state(data_dir) do
          {:ok, journal_state} ->
            case pending_transactions(journal_state) do
              [] -> nil
              pending -> {:standalone_journal_pending, length(pending)}
            end

          {:error, reason} ->
            {:standalone_journal_unreadable, reason}
        end

      {:ok, %File.Stat{type: :regular}} ->
        nil

      {:ok, %File.Stat{type: type}} ->
        {:standalone_journal_unsafe_type, type}

      {:error, :enoent} ->
        nil

      {:error, reason} ->
        {:standalone_journal_unreadable, reason}
    end
  end

  defp reduce_journal(<<>>, _data_dir, pending_state, skipped),
    do: {pending_state, skipped}

  defp reduce_journal(contents, data_dir, pending_state, skipped) do
    case take_journal_line(contents) do
      {:unterminated, _line} ->
        {pending_state, skipped + 1}

      {:terminated, line, rest} ->
        case trim_line_ending(line) do
          "" ->
            reduce_journal(rest, data_dir, pending_state, skipped)

          encoded ->
            case decode_line(encoded, data_dir) do
              {:ok, entry} ->
                case accumulate_pending(entry, pending_state) do
                  {:ok, next_pending_state} ->
                    reduce_journal(rest, data_dir, next_pending_state, skipped)

                  :error ->
                    reduce_journal(rest, data_dir, pending_state, skipped + 1)
                end

              :error ->
                reduce_journal(rest, data_dir, pending_state, skipped + 1)
            end
        end
    end
  end

  defp take_journal_line(contents) do
    case :binary.match(contents, "\n") do
      {newline_offset, 1} ->
        line = binary_part(contents, 0, newline_offset + 1)
        rest_offset = newline_offset + 1
        rest = binary_part(contents, rest_offset, byte_size(contents) - rest_offset)
        {:terminated, line, rest}

      :nomatch ->
        {:unterminated, contents}
    end
  end

  defp accumulate_pending(
         {@magic, :prepare, txid, groups},
         {order, prepares, terminals}
       ) do
    if Map.has_key?(prepares, txid) or Map.has_key?(terminals, txid) do
      :error
    else
      {:ok, {[txid | order], Map.put(prepares, txid, groups), terminals}}
    end
  end

  defp accumulate_pending(
         {@magic, terminal, txid},
         {order, prepares, terminals}
       )
       when terminal in [:commit, :abort] do
    case Map.fetch(terminals, txid) do
      {:ok, ^terminal} ->
        {:ok, {order, prepares, terminals}}

      {:ok, _previous_terminal} ->
        :error

      :error ->
        if Map.has_key?(prepares, txid) do
          {:ok, {order, Map.delete(prepares, txid), Map.put(terminals, txid, terminal)}}
        else
          :error
        end
    end
  end

  defp trim_line_ending(line) when is_binary(line) do
    line
    |> trim_last_byte(?\n)
    |> trim_last_byte(?\r)
  end

  defp trim_last_byte(<<>>, _byte), do: <<>>

  defp trim_last_byte(binary, byte) do
    size = byte_size(binary)

    if :binary.at(binary, size - 1) == byte do
      binary_part(binary, 0, size - 1)
    else
      binary
    end
  end

  defp encode_entry(entry), do: Base.encode64(TermCodec.encode(entry))

  defp decode_line(line, data_dir) do
    with {:ok, binary} <- Base.decode64(line),
         {:ok, term} <- decode_journal_term(binary),
         true <- valid_entry?(term, data_dir) do
      {:ok, term}
    else
      _ -> :error
    end
  end

  defp decode_journal_term(
         <<131, 80, uncompressed_size::unsigned-big-32, _compressed::binary>> = binary
       )
       when uncompressed_size <= @max_legacy_uncompressed_bytes do
    # Journals written before TermCodec used safe compressed external terms.
    case :erlang.binary_to_term(binary, [:safe, :used]) do
      {term, used} when used == byte_size(binary) -> {:ok, term}
      _invalid -> {:error, :invalid_external_term}
    end
  rescue
    ArgumentError -> {:error, :invalid_external_term}
  end

  defp decode_journal_term(<<131, 80, _uncompressed_size::unsigned-big-32, _compressed::binary>>),
    do: {:error, :legacy_term_too_large}

  defp decode_journal_term(binary), do: TermCodec.decode(binary)

  defp valid_entry?({@magic, :prepare, txid, groups}, data_dir)
       when is_binary(txid) and is_list(groups),
       do: valid_txid?(txid) and valid_groups?(data_dir, groups)

  defp valid_entry?({@magic, terminal, txid}, _data_dir)
       when terminal in [:commit, :abort] and is_binary(txid),
       do: valid_txid?(txid)

  defp valid_entry?(_other, _data_dir), do: false

  defp valid_txid?(txid),
    do: is_binary(txid) and byte_size(txid) > 0 and byte_size(txid) <= @max_txid_bytes

  defp valid_groups?(data_dir, groups) do
    groups != [] and Enum.all?(groups, &valid_group?(&1, data_dir))
  end

  defp valid_group?({file_path, batch}, data_dir)
       when is_binary(file_path) and is_list(batch) and batch != [],
       do: path_within_data_dir?(file_path, data_dir) and Enum.all?(batch, &valid_batch_op?/1)

  defp valid_group?(_other, _data_dir), do: false

  defp path_within_data_dir?(file_path, data_dir) do
    expanded_root = Path.expand(data_dir)
    expanded_path = Path.expand(file_path)
    expanded_path != expanded_root and String.starts_with?(expanded_path, expanded_root <> "/")
  end

  defp valid_batch_op?({:put, key, value, expire_at_ms})
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
              expire_at_ms >= 0,
       do: true

  defp valid_batch_op?({:put_cold, key, value, expire_at_ms, _lfu})
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
              expire_at_ms >= 0,
       do: true

  defp valid_batch_op?({:delete, key, _prob_path}) when is_binary(key), do: true
  defp valid_batch_op?(_other), do: false

  defp path(data_dir), do: Path.join(data_dir, @file_name)
  defp manifest_path(data_dir), do: Path.join(data_dir, @manifest_file_name)

  defp group_stats(groups) do
    %{
      groups: length(groups),
      ops:
        Enum.reduce(groups, 0, fn
          {_file_path, batch}, acc when is_list(batch) -> acc + length(batch)
          _other, acc -> acc
        end)
    }
  end

  defp observe(event, measurements, metadata) do
    :telemetry.execute([:ferricstore, :standalone_tx_log, event], measurements, metadata)
  end

  defp fsync_file(path) do
    case Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook) do
      hook when is_function(hook, 1) -> hook.(path)
      _ -> NIF.v2_fsync(path)
    end
  end

  defp fsync_journal_file(data_dir) do
    case File.lstat(path(data_dir)) do
      {:ok, %File.Stat{type: :regular}} ->
        fsync_file(path(data_dir))

      {:error, :enoent} ->
        {:error, :journal_missing}

      {:ok, %File.Stat{type: type}} ->
        {:error, {:unsafe_journal_type, type}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp append_sync_nofollow_bounded(path, payload, max_bytes) do
    case Application.get_env(:ferricstore, :standalone_tx_log_append_hook) do
      hook when is_function(hook, 3) -> hook.(path, payload, max_bytes)
      _ -> Ferricstore.FS.append_sync_nofollow_bounded(path, payload, max_bytes)
    end
  end

  defp fsync_dir(path) do
    case Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook) do
      hook when is_function(hook, 1) -> hook.(path)
      _ -> NIF.v2_fsync_dir(path)
    end
  end

  defp mark_recovery_required(data_dir, reason) do
    :persistent_term.put(recovery_key(data_dir), reason)
  end

  defp clear_recovery_required(data_dir) do
    :persistent_term.erase(recovery_key(data_dir))
  end

  defp recovery_key(data_dir), do: {__MODULE__, :recovery_required, Path.expand(data_dir)}

  defp with_journal_lock(data_dir, fun) when is_function(fun, 0) do
    lock = {{__MODULE__, Path.expand(data_dir)}, self()}

    case :global.trans(lock, fun, [node()]) do
      {:aborted, reason} -> {:error, {:journal_lock_failed, reason}}
      result -> result
    end
  end
end
