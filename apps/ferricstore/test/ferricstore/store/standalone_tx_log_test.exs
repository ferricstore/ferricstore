defmodule Ferricstore.Store.StandaloneTxLogTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.StandaloneTxLog

  @journal_name "standalone_cross_shard_tx.log"
  @manifest_name "standalone_cross_shard_tx.manifest"

  test "prepare and commit persist markers without rewriting the journal per transaction" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")

    groups = [
      {Path.join(data_dir, "shard_0/000000.data"), [{:put, "k1", "v1", 0}]},
      {Path.join(data_dir, "shard_1/000000.data"), [{:delete, "k2", nil}]}
    ]

    assert {:ok, txid} = StandaloneTxLog.prepare(data_dir, groups)
    assert is_binary(txid)
    assert File.exists?(tx_log_path)
    refute StandaloneTxLog.recovery_required?(data_dir)
    refute File.exists?(StandaloneTxLog.recovery_marker_path(data_dir))

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert File.exists?(tx_log_path)

    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(tx_log_path)
  end

  test "unterminated complete records are rejected before a prepare append" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    txid = "unterminated-prepare"
    groups = [{file_path, [{:put, "old", "value", 0}]}]
    append_calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)

    File.mkdir_p!(data_dir)

    File.write!(
      tx_log_path,
      encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :prepare, txid, groups})
    )

    before_append = File.read!(tx_log_path)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      :atomics.add_get(append_calls, 1, 1)
      Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit)
    end)

    on_exit(fn -> restore_env(:standalone_tx_log_append_hook, previous_hook) end)

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.prepare(data_dir, groups)
    assert :atomics.get(append_calls, 1) == 0
    assert File.read!(tx_log_path) == before_append
  end

  test "prepare returns the txid when append reports an error after a complete record is visible" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    append_calls = :atomics.new(1, signed: false)
    file_calls = :atomics.new(1, signed: false)
    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      case :atomics.add_get(append_calls, 1, 1) do
        1 ->
          with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
            {:error, :append_eio}
          end

        _ ->
          Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit)
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn path ->
      :atomics.add_get(file_calls, 1, 1)
      NIF.v2_fsync(path)
    end)

    on_exit(fn ->
      restore_env(:standalone_tx_log_append_hook, previous_append_hook)
      restore_env(:standalone_tx_log_fsync_file_hook, previous_file_hook)
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert is_binary(txid)
    assert :atomics.get(file_calls, 1) == 1
    assert :ok = StandaloneTxLog.abort(data_dir, txid)
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "prepare fences when directory fsync fails after a complete record is visible" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    dir_calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      case :atomics.add_get(dir_calls, 1, 1) do
        1 -> {:error, :dir_eio}
        _ -> NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn -> restore_env(:standalone_tx_log_fsync_dir_hook, previous_hook) end)

    assert {:error, {:standalone_tx_prepare_recovery_required, _txid, :dir_eio, _, _}} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert StandaloneTxLog.recovery_required?(data_dir)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
    assert :ok = StandaloneTxLog.recover(data_dir)
    assert :atomics.get(dir_calls, 1) >= 2
  end

  test "prepare restores the exact journal when visible-record file fsync fails" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)

    assert {:ok, _existing_txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "old", "value", 0}]}])

    before_append = File.read!(tx_log_path)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
        {:error, :append_eio}
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn _path ->
      {:error, :file_eio}
    end)

    on_exit(fn ->
      restore_env(:standalone_tx_log_append_hook, previous_append_hook)
      restore_env(:standalone_tx_log_fsync_file_hook, previous_file_hook)
    end)

    assert {:error, :append_eio} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "new", "value", 0}]}
             ])

    assert File.read!(tx_log_path) == before_append
    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "prepare restores the exact journal after a partial visible tail" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      partial = binary_part(payload, 0, byte_size(payload) - 2)

      with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, partial, limit) do
        {:error, :append_eio}
      end
    end)

    on_exit(fn -> restore_env(:standalone_tx_log_append_hook, previous_hook) end)

    assert {:error, :append_eio} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    refute File.exists?(tx_log_path)
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "prepare fails closed when visible-record establishment and rollback both fail" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
    previous_dir_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
        {:error, :append_eio}
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn _path ->
      {:error, :file_eio}
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn _path ->
      {:error, :rollback_eio}
    end)

    on_exit(fn ->
      restore_env(:standalone_tx_log_append_hook, previous_append_hook)
      restore_env(:standalone_tx_log_fsync_file_hook, previous_file_hook)
      restore_env(:standalone_tx_log_fsync_dir_hook, previous_dir_hook)
    end)

    assert {:error,
            {:standalone_tx_prepare_recovery_required, txid, :append_eio, :file_eio,
             :rollback_eio}} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "key", "value", 0}]}
             ])

    assert is_binary(txid)

    assert {:error, {:standalone_tx_recovery_required, _reason}} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "next", "value", 0}]}
             ])

    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    assert :ok = StandaloneTxLog.recover(data_dir)

    assert {:ok, _next_txid} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "next", "value", 0}]}
             ])
  end

  test "recovery keeps the guard when mandatory cleanup fsync fails, then clears it on retry" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    guard_key = {StandaloneTxLog, :recovery_required, Path.expand(data_dir)}
    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
    previous_dir_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
        {:error, :append_eio}
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn _path ->
      {:error, :file_eio}
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn _path ->
      {:error, :rollback_eio}
    end)

    on_exit(fn ->
      restore_env(:standalone_tx_log_append_hook, previous_append_hook)
      restore_env(:standalone_tx_log_fsync_file_hook, previous_file_hook)
      restore_env(:standalone_tx_log_fsync_dir_hook, previous_dir_hook)
      :persistent_term.erase(guard_key)
    end)

    assert {:error, {:standalone_tx_prepare_recovery_required, _txid, _, _, _}} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "key", "value", 0}]}
             ])

    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)

    cleanup_calls = :atomics.new(1, signed: false)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      case :atomics.add_get(cleanup_calls, 1, 1) do
        1 -> {:error, :cleanup_eio}
        _ -> NIF.v2_fsync_dir(path)
      end
    end)

    assert {:error, :cleanup_eio} = StandaloneTxLog.recover(data_dir)
    assert :persistent_term.get(guard_key, nil) != nil

    assert {:error, {:standalone_tx_recovery_required, _reason}} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "blocked", "value", 0}]}
             ])

    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert :persistent_term.get(guard_key, nil) == nil

    assert {:ok, _txid} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "after-recovery", "value", 0}]}
             ])
  end

  test "repeated commits do not poison recovery" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.log"))
  end

  test "recovery accepts identical duplicate terminal markers in existing journals" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    txid = "legacy-duplicate-commit"
    File.mkdir_p!(data_dir)

    prepare =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, txid,
         [{file_path, [{:put, "key", "value", 0}]}]}
      )

    commit = encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :commit, txid})

    File.write!(tx_log_path, prepare <> "\n" <> commit <> "\n" <> commit <> "\n")

    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(tx_log_path)
  end

  test "recovery rejects conflicting duplicate terminal markers in existing journals" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    txid = "legacy-conflicting-terminal"
    File.mkdir_p!(data_dir)

    prepare =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, txid,
         [{file_path, [{:put, "key", "value", 0}]}]}
      )

    commit = encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :commit, txid})
    abort = encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :abort, txid})

    File.mkdir_p!(Path.dirname(file_path))
    assert {:ok, [{offset, _size}]} = NIF.v2_append_batch(file_path, [{"key", "before", 0}])
    File.write!(tx_log_path, prepare <> "\n" <> commit <> "\n" <> abort <> "\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    assert File.exists?(tx_log_path)
    assert {:ok, "before"} = NIF.v2_pread_at(file_path, offset)
  end

  test "unknown and conflicting terminal markers are rejected without appending" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")

    assert {:error, :unknown_txid} = StandaloneTxLog.commit(data_dir, "missing-txid")
    refute File.exists?(tx_log_path)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)

    assert {:error, {:transaction_already_terminal, :commit}} =
             StandaloneTxLog.abort(data_dir, txid)

    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "post-append fsync failure is recovered in the same terminal call" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if path == data_dir do
        case :atomics.add_get(calls, 1, 1) do
          2 -> {:error, :eio}
          _ -> :ok
        end
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)

    assert {:error, {:transaction_already_terminal, :commit}} =
             StandaloneTxLog.abort(data_dir, txid)

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert :atomics.get(calls, 1) == 4
    refute File.exists?(file_path)
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.log"))
  end

  test "repeated terminal calls remain idempotent after post-append fsync" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if path == data_dir do
        case :atomics.add_get(calls, 1, 1) do
          2 -> {:error, :eio}
          _ -> :ok
        end
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :atomics.get(calls, 1) == 4
  end

  test "an already-visible terminal is acknowledged only after journal file fsync" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    append_calls = :atomics.new(1, signed: false)
    file_calls = :atomics.new(1, signed: false)
    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      case :atomics.add_get(append_calls, 1, 1) do
        2 ->
          with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
            {:error, :eio}
          end

        _ ->
          Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit)
      end
    end)

    on_exit(fn ->
      if previous_append_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_append_hook, previous_append_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
      end

      if previous_file_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, previous_file_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn path ->
      case :atomics.add_get(file_calls, 1, 1) do
        1 -> {:error, :eio}
        _ -> NIF.v2_fsync(path)
      end
    end)

    assert {:error, :eio} = StandaloneTxLog.commit(data_dir, txid)
    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :atomics.get(file_calls, 1) == 2
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "maintenance compaction failure leaves a retryable terminal manifest" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    large_value = String.duplicate("v", 4_300_000)
    calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if path == data_dir and :atomics.add_get(calls, 1, 1) == 3 do
        {:error, :eio}
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "key", large_value, 0}]}
             ])

    assert {:error, :eio} = StandaloneTxLog.commit(data_dir, txid)
    assert File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.manifest"))

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "a complete visible terminal after append sync error is fsynced in-call" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    append_calls = :atomics.new(1, signed: false)
    file_calls = :atomics.new(1, signed: false)
    dir_calls = :atomics.new(1, signed: false)
    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
    previous_dir_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      case :atomics.add_get(append_calls, 1, 1) do
        2 ->
          with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
            {:error, :eio}
          end

        _ ->
          Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit)
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn path ->
      :atomics.add_get(file_calls, 1, 1)
      NIF.v2_fsync(path)
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      :atomics.add_get(dir_calls, 1, 1)
      NIF.v2_fsync_dir(path)
    end)

    on_exit(fn ->
      if previous_append_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_append_hook, previous_append_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
      end

      if previous_file_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, previous_file_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
      end

      if previous_dir_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_dir_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :atomics.get(file_calls, 1) >= 1
    assert :atomics.get(dir_calls, 1) >= 2
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "a partial terminal tail is repaired before abort and restart" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    append_calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      case :atomics.add_get(append_calls, 1, 1) do
        2 ->
          partial = binary_part(payload, 0, byte_size(payload) - 2)

          with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, partial, limit) do
            {:error, :eio}
          end

        _ ->
          Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_append_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    before_append = File.read!(tx_log_path)
    assert {:error, :eio} = StandaloneTxLog.commit(data_dir, txid)
    assert File.read!(tx_log_path) == before_append

    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
    assert :ok = StandaloneTxLog.abort(data_dir, txid)
    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(file_path)
  end

  test "long-running compaction drops terminal history without a manifest" do
    data_dir = tmp_dir()
    large_value = String.duplicate("v", 4_300_000)
    manifest_path = Path.join(data_dir, "standalone_cross_shard_tx.manifest")
    journal_path = Path.join(data_dir, "standalone_cross_shard_tx.log")

    for index <- 1..6 do
      file_path = Path.join(data_dir, "shard_#{index}/000000.data")

      assert {:ok, txid} =
               StandaloneTxLog.prepare(data_dir, [
                 {file_path, [{:put, "key", large_value, 0}]}
               ])

      assert :ok = StandaloneTxLog.commit(data_dir, txid)
      refute File.exists?(manifest_path)
      refute File.exists?(journal_path)
    end
  end

  test "compaction fsync fault leaves either the old or new journal form valid" do
    data_dir = tmp_dir()
    first_file = Path.join(data_dir, "shard_0/000000.data")
    second_file = Path.join(data_dir, "shard_1/000000.data")
    large_value = String.duplicate("v", 2_200_000)
    second_groups = [{second_file, [{:put, "second", large_value, 0}]}]
    calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if path == data_dir and :atomics.add_get(calls, 1, 1) == 4 do
        {:error, :eio}
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:ok, first_txid} =
             StandaloneTxLog.prepare(data_dir, [
               {first_file, [{:put, "first", large_value, 0}]}
             ])

    assert {:ok, second_txid} = StandaloneTxLog.prepare(data_dir, second_groups)

    old_journal =
      File.read!(Path.join(data_dir, "standalone_cross_shard_tx.log")) <>
        encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :commit, first_txid}) <> "\n"

    new_journal =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, second_txid, second_groups}
      ) <>
        "\n"

    assert {:error, :eio} = StandaloneTxLog.commit(data_dir, first_txid)

    assert File.read!(Path.join(data_dir, "standalone_cross_shard_tx.log")) in [
             old_journal,
             new_journal
           ]

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert {:ok, [{"second", _offset, _size, 0, false}]} = NIF.v2_scan_file(second_file)
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.manifest"))
  end

  test "failed empty-journal compaction blocks recovery until the next durable retry" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if path == data_dir do
        case :atomics.add_get(calls, 1, 1) do
          3 -> {:error, :eio}
          _ -> :ok
        end
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert {:error, :eio} = StandaloneTxLog.recover(data_dir)
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.log"))
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.manifest"))

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert :atomics.get(calls, 1) == 3
  end

  test "successful compaction may make a repeated terminal call unknown" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if path == data_dir do
        case :atomics.add_get(calls, 1, 1) do
          3 -> {:error, :eio}
          _ -> NIF.v2_fsync_dir(path)
        end
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert {:error, :eio} = StandaloneTxLog.recover(data_dir)
    refute File.exists?(tx_log_path)
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.manifest"))
    assert {:error, :unknown_txid} = StandaloneTxLog.commit(data_dir, txid)
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "threshold compaction failure leaves a retryable terminal manifest" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    target_txid = "threshold-compaction-target"
    filler_value = String.duplicate("x", 4_096)

    entries =
      for index <- 1..1_200 do
        txid = "threshold-filler-#{index}"

        [
          encode_entry(
            {:ferricstore_standalone_cross_shard_tx_v1, :prepare, txid,
             [{file_path, [{:put, "key-#{index}", filler_value, 0}]}]}
          ),
          "\n",
          encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :commit, txid}),
          "\n"
        ]
      end

    target_prepare =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, target_txid,
         [{file_path, [{:put, "target", "value", 0}]}]}
      )

    File.mkdir_p!(data_dir)
    File.write!(tx_log_path, IO.iodata_to_binary([entries, target_prepare, "\n"]))
    assert {:ok, %{size: size}} = File.stat(tx_log_path)
    assert size >= 4 * 1_024 * 1_024

    calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if path == data_dir do
        case :atomics.add_get(calls, 1, 1) do
          2 -> {:error, :eio}
          _ -> NIF.v2_fsync_dir(path)
        end
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:error, :eio} = StandaloneTxLog.commit(data_dir, target_txid)
    assert File.exists?(tx_log_path)
    assert File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.manifest"))
    assert :ok = StandaloneTxLog.commit(data_dir, target_txid)
    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "recovery markers survive persistent-term loss and clear after replay" do
    data_dir = tmp_dir()
    marker_path = StandaloneTxLog.recovery_marker_path(data_dir)
    reason = {:standalone_tx_compensation_recovery_required, "txid", String.duplicate("x", 2_048)}

    assert :ok = StandaloneTxLog.require_recovery(data_dir, reason)
    assert {:ok, %{size: marker_size}} = File.stat(marker_path)
    assert marker_size <= 1_024

    :persistent_term.erase({StandaloneTxLog, :recovery_required, Path.expand(data_dir)})
    assert StandaloneTxLog.recovery_required?(data_dir)

    assert {:error, {:standalone_tx_recovery_required, _reason}} =
             StandaloneTxLog.prepare(
               data_dir,
               [{Path.join(data_dir, "shard_0/000000.data"), [{:put, "key", "value", 0}]}]
             )

    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(marker_path)
    refute StandaloneTxLog.recovery_required?(data_dir)
  end

  test "recovery marker writes and reads do not follow symlinks" do
    data_dir = tmp_dir()
    marker_path = StandaloneTxLog.recovery_marker_path(data_dir)
    victim = Path.join(data_dir, "victim")
    File.mkdir_p!(data_dir)
    File.write!(victim, "protected")
    File.ln_s!(victim, marker_path)

    assert :ok = StandaloneTxLog.require_recovery(data_dir, :symlink_marker)

    assert File.read!(victim) == "protected"
    assert {:ok, %File.Stat{type: :regular}} = File.lstat(marker_path)
    :persistent_term.erase({StandaloneTxLog, :recovery_required, Path.expand(data_dir)})
    assert StandaloneTxLog.recovery_required?(data_dir)
    assert File.read!(victim) == "protected"
  end

  test "failed recovery-marker cleanup keeps the durable fence" do
    data_dir = tmp_dir()
    marker_path = StandaloneTxLog.recovery_marker_path(data_dir)
    assert :ok = StandaloneTxLog.require_recovery(data_dir, :cleanup_fsync_failure)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn _path ->
      {:error, :cleanup_fsync_eio}
    end)

    on_exit(fn -> Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook) end)

    assert {:error, :cleanup_fsync_eio} = StandaloneTxLog.recover(data_dir)
    assert File.exists?(marker_path)

    :persistent_term.erase({StandaloneTxLog, :recovery_required, Path.expand(data_dir)})
    assert StandaloneTxLog.recovery_required?(data_dir)

    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
    assert :ok = StandaloneTxLog.recover(data_dir)
    refute StandaloneTxLog.recovery_required?(data_dir)
  end

  test "compaction keeps terminal metadata bounded and retries after journal removal" do
    data_dir = tmp_dir()
    manifest_path = Path.join(data_dir, @manifest_name)
    large_value = String.duplicate("v", 4_300_000)

    Application.put_env(:ferricstore, :standalone_tx_log_compaction_hook, fn
      :after_journal_remove, ^data_dir -> {:error, :simulated_crash}
      _stage, ^data_dir -> :ok
    end)

    on_exit(fn -> Application.delete_env(:ferricstore, :standalone_tx_log_compaction_hook) end)

    file_path = Path.join(data_dir, "shard_0/000000.data")

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "first", large_value, 0}]}
             ])

    assert {:error, _reason} = StandaloneTxLog.commit(data_dir, txid)
    assert File.exists?(manifest_path)
    assert bounded_metadata_count(data_dir) <= 1
    assert {:ok, %{size: manifest_size}} = File.stat(manifest_path)
    assert manifest_size < 1_024

    Application.delete_env(:ferricstore, :standalone_tx_log_compaction_hook)
    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    refute File.exists?(manifest_path)
    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(Path.join(data_dir, @journal_name))

    for index <- 1..6 do
      cycle_file = Path.join(data_dir, "shard_#{index}/000000.data")

      assert {:ok, cycle_txid} =
               StandaloneTxLog.prepare(data_dir, [
                 {cycle_file, [{:put, "cycle", large_value, 0}]}
               ])

      assert :ok = StandaloneTxLog.commit(data_dir, cycle_txid)
      assert :ok = StandaloneTxLog.recover(data_dir)
      assert bounded_metadata_count(data_dir) <= 1
    end
  end

  test "terminal retries are idempotent and conflicting terminals are rejected" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "key", "value", 0}]}
             ])

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert :ok = StandaloneTxLog.commit(data_dir, txid)

    assert {:error, {:transaction_already_terminal, :commit}} =
             StandaloneTxLog.abort(data_dir, txid)

    assert :ok = StandaloneTxLog.recover(data_dir)
  end

  test "restart completes compaction across manifest and journal boundaries" do
    for stage <- [
          :before_manifest_publish,
          :after_manifest_publish,
          :before_journal_rewrite,
          :after_journal_remove,
          :before_manifest_cleanup,
          :after_manifest_remove
        ] do
      data_dir = tmp_dir()
      large_value = String.duplicate("v", 4_300_000)
      file_path = Path.join(data_dir, "shard_0/000000.data")

      Application.put_env(:ferricstore, :standalone_tx_log_compaction_hook, fn
        ^stage, ^data_dir -> {:error, :simulated_crash}
        _other_stage, ^data_dir -> :ok
      end)

      try do
        assert {:ok, txid} =
                 StandaloneTxLog.prepare(data_dir, [
                   {file_path, [{:put, "key", large_value, 0}]}
                 ])

        assert {:error, _reason} = StandaloneTxLog.commit(data_dir, txid)
      after
        Application.delete_env(:ferricstore, :standalone_tx_log_compaction_hook)
      end

      assert :ok = StandaloneTxLog.recover(data_dir)
      assert :ok = StandaloneTxLog.recover(data_dir)
      assert bounded_metadata_count(data_dir) <= 1
      refute File.exists?(Path.join(data_dir, @journal_name))
      refute File.exists?(Path.join(data_dir, @manifest_name))
    end
  end

  test "restart applies preserved pending work once after journal rewrite interruption" do
    data_dir = tmp_dir()
    large_value = String.duplicate("v", 2_200_000)
    first_file = Path.join(data_dir, "shard_0/000000.data")
    second_file = Path.join(data_dir, "shard_1/000000.data")

    Application.put_env(:ferricstore, :standalone_tx_log_compaction_hook, fn
      :after_journal_rewrite, ^data_dir -> {:error, :simulated_crash}
      _stage, ^data_dir -> :ok
    end)

    try do
      assert {:ok, first_txid} =
               StandaloneTxLog.prepare(data_dir, [
                 {first_file, [{:put, "first", large_value, 0}]}
               ])

      assert {:ok, _second_txid} =
               StandaloneTxLog.prepare(data_dir, [
                 {second_file, [{:put, "second", large_value, 0}]}
               ])

      assert {:error, _reason} = StandaloneTxLog.commit(data_dir, first_txid)
    after
      Application.delete_env(:ferricstore, :standalone_tx_log_compaction_hook)
    end

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert :ok = StandaloneTxLog.recover(data_dir)
    assert {:ok, [{"second", _offset, _size, 0, false}]} = NIF.v2_scan_file(second_file)
    refute File.exists?(Path.join(data_dir, @journal_name))
    refute File.exists?(Path.join(data_dir, @manifest_name))
  end

  test "directory fsync failure leaves one retryable compaction manifest" do
    data_dir = tmp_dir()
    large_value = String.duplicate("v", 4_300_000)
    file_path = Path.join(data_dir, "shard_0/000000.data")
    fsync_calls = :atomics.new(1, signed: false)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if :atomics.add_get(fsync_calls, 1, 1) == 3 do
        {:error, :eio}
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    try do
      assert {:ok, txid} =
               StandaloneTxLog.prepare(data_dir, [
                 {file_path, [{:put, "key", large_value, 0}]}
               ])

      assert {:error, _reason} = StandaloneTxLog.commit(data_dir, txid)
      assert File.exists?(Path.join(data_dir, @manifest_name))
    after
      Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
    end

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert :ok = StandaloneTxLog.recover(data_dir)
    assert bounded_metadata_count(data_dir) <= 1
    refute File.exists?(Path.join(data_dir, @journal_name))
    refute File.exists?(Path.join(data_dir, @manifest_name))
  end

  test "recover replays pending prepared transactions and marks them committed" do
    data_dir = tmp_dir()
    file_a = Path.join(data_dir, "shard_0/000000.data")
    file_b = Path.join(data_dir, "shard_1/000000.data")

    File.mkdir_p!(Path.dirname(file_a))
    File.mkdir_p!(Path.dirname(file_b))

    groups = [
      {file_a, [{:put, "k1", "v1", 0}]},
      {file_b, [{:put, "k2", "v2", 0}]}
    ]

    assert {:ok, _txid} = StandaloneTxLog.prepare(data_dir, groups)
    assert :ok = StandaloneTxLog.recover(data_dir)

    assert {:ok, %{size: size_a}} = File.stat(file_a)
    assert {:ok, %{size: size_b}} = File.stat(file_b)
    assert size_a > 0
    assert size_b > 0
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.log"))
  end

  test "recover fails closed on corrupt transaction-log entries" do
    data_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    File.write!(tx_log_path, "not-a-valid-entry\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    assert File.exists?(tx_log_path)
  end

  test "recover treats a missing data directory as an empty journal" do
    assert :ok = StandaloneTxLog.recover(tmp_dir())
  end

  test "prepare refuses a journal symlink without modifying its target" do
    data_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    victim = Path.join(data_dir, "victim")
    File.write!(victim, "protected")
    File.ln_s!(victim, tx_log_path)
    file_path = Path.join(data_dir, "shard_0/000000.data")

    assert {:error, {:symlink, _reason}} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    assert File.read!(victim) == "protected"
  end

  test "recover refuses a journal symlink without reading its target" do
    data_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    victim = Path.join(data_dir, "victim")
    File.write!(victim, "not-a-valid-entry\n")
    File.ln_s!(victim, tx_log_path)

    assert {:error, {:symlink, _reason}} = StandaloneTxLog.recover(data_dir)
    assert File.read!(victim) == "not-a-valid-entry\n"
  end

  test "recover refuses intermediate symlinks in persisted shard paths" do
    data_dir = tmp_dir()
    external_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    File.mkdir_p!(external_dir)
    File.ln_s!(external_dir, Path.join(data_dir, "shard_0"))

    external_target = Path.join(external_dir, "000000.data")
    File.write!(external_target, "protected")
    shard_path = Path.join(data_dir, "shard_0/000000.data")

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [
               {shard_path, [{:put, "key", "value", 0}]}
             ])

    assert {:error, {:recover_tx_failed, ^txid, {^shard_path, {kind, _reason}}}} =
             StandaloneTxLog.recover(data_dir)

    assert kind in [:symlink, :not_a_directory]

    assert File.read!(external_target) == "protected"
  end

  test "recover rejects decodable malformed prepare entries before replay" do
    data_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")

    malformed =
      {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "txid", [:not_a_group]}
      |> :erlang.term_to_binary()
      |> Base.encode64()

    File.write!(tx_log_path, malformed <> "\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    assert File.exists?(tx_log_path)
  end

  test "recover fails closed on a truncated journal record" do
    data_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")

    encoded =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "truncated-txid",
         [{file_path, [{:put, "key", "value", 0}]}]}
      )

    truncated = binary_part(encoded, 0, byte_size(encoded) - 1)
    File.write!(tx_log_path, truncated <> "\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    refute File.exists?(file_path)
  end

  test "commit refuses to append after a partial terminal record" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")

    assert {:ok, txid} =
             StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "key", "value", 0}]}])

    terminal = encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :commit, txid})
    partial_terminal = binary_part(terminal, 0, byte_size(terminal) - 1)

    File.write!(tx_log_path, partial_terminal, [:append])
    contents = File.read!(tx_log_path)

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.commit(data_dir, txid)
    assert File.read!(tx_log_path) == contents
  end

  test "recover rejects duplicate prepare ids instead of replacing the original undo plan" do
    data_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    first_path = Path.join(data_dir, "shard_0/000000.data")
    second_path = Path.join(data_dir, "shard_1/000000.data")

    first =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "duplicate-txid",
         [{first_path, [{:put, "key", "first", 0}]}]}
      )

    second =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "duplicate-txid",
         [{second_path, [{:put, "key", "second", 0}]}]}
      )

    File.write!(tx_log_path, first <> "\n" <> second <> "\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    refute File.exists?(first_path)
    refute File.exists?(second_path)
  end

  test "recover rejects terminal markers that precede their prepare" do
    data_dir = tmp_dir()
    File.mkdir_p!(data_dir)
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")

    terminal =
      encode_entry({:ferricstore_standalone_cross_shard_tx_v1, :commit, "reordered-txid"})

    prepare =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "reordered-txid",
         [{file_path, [{:put, "key", "value", 0}]}]}
      )

    File.write!(tx_log_path, terminal <> "\n" <> prepare <> "\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    refute File.exists?(file_path)
  end

  test "recover accepts legacy compressed journal entries" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    File.mkdir_p!(data_dir)

    term =
      {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "legacy-compressed",
       [{file_path, [{:put, "key", String.duplicate("value", 2_048), 0}]}]}

    payload = :erlang.term_to_binary(term, compressed: 9)
    assert <<131, 80, _::binary>> = payload
    File.write!(tx_log_path, Base.encode64(payload) <> "\n")

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert {:ok, [{"key", offset, _size, 0, false}]} = NIF.v2_scan_file(file_path)
    assert {:ok, value} = NIF.v2_pread_at(file_path, offset)
    assert value == String.duplicate("value", 2_048)
  end

  test "recover rejects legacy compressed entries whose declared size exceeds the journal bound" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    oversized_value = String.duplicate("x", 64 * 1_024 * 1_024 + 1)
    File.mkdir_p!(data_dir)

    term =
      {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "oversized-compressed",
       [{file_path, [{:put, "key", oversized_value, 0}]}]}

    payload = :erlang.term_to_binary(term, compressed: 9)
    <<131, 80, declared_size::unsigned-big-32, _compressed::binary>> = payload
    assert declared_size > 64 * 1_024 * 1_024
    File.write!(tx_log_path, Base.encode64(payload) <> "\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    refute File.exists?(file_path)
  end

  test "recover rejects malformed legacy compressed headers" do
    for payload <- [
          <<131, 80>>,
          <<131, 80, 16::unsigned-big-32>>,
          <<131, 80, 16::unsigned-big-32, 0>>
        ] do
      data_dir = tmp_dir()
      tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
      File.mkdir_p!(data_dir)
      File.write!(tx_log_path, Base.encode64(payload) <> "\n")

      assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
      assert File.exists?(tx_log_path)
    end
  end

  test "recover rejects trailing current-format entries" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    File.mkdir_p!(data_dir)

    term =
      {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "txid",
       [{file_path, [{:put, "key", String.duplicate("value", 2_048), 0}]}]}

    payload = Ferricstore.TermCodec.encode(term) <> <<0>>
    File.write!(tx_log_path, Base.encode64(payload) <> "\n")

    assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
    refute File.exists?(file_path)
  end

  test "prepare rejects malformed groups without poisoning the journal" do
    data_dir = tmp_dir()

    assert {:error, :invalid_groups} = StandaloneTxLog.prepare(data_dir, [:not_a_group])
    refute File.exists?(Path.join(data_dir, "standalone_cross_shard_tx.log"))
  end

  test "prepare never appends a journal that recovery would refuse as oversized" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    file_path = Path.join(data_dir, "shard_0/000000.data")
    max_journal_bytes = 64 * 1_024 * 1_024
    last_offset = max_journal_bytes - 1
    File.mkdir_p!(data_dir)

    {:ok, io} = File.open(tx_log_path, [:write, :binary])
    assert {:ok, ^last_offset} = :file.position(io, last_offset)
    assert :ok = :file.write(io, <<0>>)
    assert :ok = File.close(io)

    assert {:error, {:journal_limit_exceeded, _reason}} =
             StandaloneTxLog.prepare(data_dir, [
               {file_path, [{:put, "key", "value", 0}]}
             ])

    assert {:ok, %{size: ^max_journal_bytes}} = File.stat(tx_log_path)
  end

  test "terminal markers reject transaction IDs that exceed the reserved bound" do
    data_dir = tmp_dir()
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    oversized_txid = String.duplicate("x", 129)

    assert {:error, :invalid_txid} = StandaloneTxLog.commit(data_dir, oversized_txid)
    assert {:error, :invalid_txid} = StandaloneTxLog.abort(data_dir, oversized_txid)
    refute File.exists?(tx_log_path)
  end

  test "prepare does not mutate persistent_term on the transaction hot path" do
    data_dir = tmp_dir()
    cache_key = {StandaloneTxLog, :recovery_required, Path.expand(data_dir)}
    missing = make_ref()
    previous = :persistent_term.get(cache_key, missing)
    :persistent_term.erase(cache_key)

    on_exit(fn ->
      if previous === missing do
        :persistent_term.erase(cache_key)
      else
        :persistent_term.put(cache_key, previous)
      end
    end)

    file_path = Path.join(data_dir, "shard_0/000000.data")
    assert {:ok, _txid} = StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "k", "v", 0}]}])

    assert :persistent_term.get(cache_key, missing) === missing
  end

  test "aborted transactions are never replayed" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    groups = [{file_path, [{:put, "key", "should-not-exist", 0}]}]

    assert {:ok, txid} = StandaloneTxLog.prepare(data_dir, groups)
    assert :ok = StandaloneTxLog.abort(data_dir, txid)
    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(file_path)
  end

  test "recovery preserves prepare order for transactions touching the same key" do
    data_dir = tmp_dir()
    file_path = Path.join(data_dir, "shard_0/000000.data")
    tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
    File.mkdir_p!(data_dir)
    File.mkdir_p!(Path.dirname(file_path))

    first =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "z-first",
         [{file_path, [{:put, "key", "first", 0}]}]}
      )

    second =
      encode_entry(
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "a-second",
         [{file_path, [{:put, "key", "second", 0}]}]}
      )

    File.write!(tx_log_path, first <> "\n" <> second <> "\n")

    assert :ok = StandaloneTxLog.recover(data_dir)
    assert {:ok, records} = NIF.v2_scan_file(file_path)

    {"key", offset, _size, _expire_at_ms, false} = List.last(records)
    assert {:ok, "second"} = NIF.v2_pread_at(file_path, offset)
  end

  test "concurrent commits cannot discard another transaction's prepare" do
    data_dir = tmp_dir()

    committed =
      for index <- 1..24 do
        file_path = Path.join(data_dir, "committed/#{index}.data")
        File.mkdir_p!(Path.dirname(file_path))

        assert {:ok, txid} =
                 StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "k", "v", 0}]}])

        {index, txid}
      end

    commits =
      Enum.map(committed, fn {_index, txid} ->
        Task.async(fn -> StandaloneTxLog.commit(data_dir, txid) end)
      end)

    pending =
      for index <- 1..24 do
        Task.async(fn ->
          file_path = Path.join(data_dir, "pending/#{index}.data")
          File.mkdir_p!(Path.dirname(file_path))
          {file_path, StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "k", "v", 0}]}])}
        end)
      end

    assert Enum.all?(Task.await_many(commits, 10_000), &(&1 == :ok))

    pending =
      Enum.map(Task.await_many(pending, 10_000), fn {file_path, result} ->
        assert {:ok, _txid} = result
        file_path
      end)

    assert :ok = StandaloneTxLog.recover(data_dir)

    Enum.each(pending, fn file_path ->
      assert {:ok, [{"k", _offset, _size, 0, false}]} = NIF.v2_scan_file(file_path)
    end)
  end

  test "recovery bounds no-follow journal reads and compaction publication" do
    source = File.read!("lib/ferricstore/store/standalone_tx_log.ex")

    refute source =~ "File.read(path)"
    refute source =~ "String.split(\"\\n\""
    refute source =~ "File.open(path"
    assert source =~ "Ferricstore.FS.read_nofollow(journal_path, @max_journal_bytes)"
    assert source =~ "append_sync_nofollow_bounded(path, line, append_limit)"
    assert source =~ "Ferricstore.FS.atomic_replace_nofollow(path, data, @max_journal_bytes)"
    assert source =~ "manifest"
    refute source =~ "terminal_tombstone"
  end

  defp tmp_dir do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_standalone_tx_log_#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}"
      )

    on_exit(fn -> File.rm_rf(path) end)
    path
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp bounded_metadata_count(data_dir) do
    [@journal_name, @manifest_name]
    |> Enum.map(&Path.join(data_dir, &1))
    |> Enum.filter(&File.exists?/1)
    |> Enum.map(fn path -> File.read!(path) |> String.split("\n", trim: true) |> length() end)
    |> Enum.sum()
  end

  defp encode_entry(entry), do: Base.encode64(Ferricstore.TermCodec.encode(entry))
end
