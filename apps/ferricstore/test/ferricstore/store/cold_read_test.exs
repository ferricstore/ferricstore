defmodule Ferricstore.Store.ColdReadTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Bitcask.NIF

  test "await_tokio returns successful completion" do
    assert {:ok, "value"} =
             ColdRead.await_tokio(
               fn proxy, corr_id ->
                 send(proxy, {:tokio_complete, corr_id, :ok, "value"})
                 :ok
               end,
               100
             )
  end

  test "await_tokio returns submit errors" do
    assert {:error, :closed} =
             ColdRead.await_tokio(
               fn _proxy, _corr_id ->
                 {:error, :closed}
               end,
               100
             )
  end

  test "await_tokio does not leak late completion into caller mailbox after timeout" do
    parent = self()

    assert {:error, :timeout} =
             ColdRead.await_tokio(
               fn proxy, corr_id ->
                 send(parent, {:proxy_started, proxy, corr_id})
                 :ok
               end,
               5
             )

    assert_receive {:proxy_started, proxy, corr_id}
    send(proxy, {:tokio_complete, corr_id, :ok, "late"})
    Process.sleep(20)

    refute_received _
  end

  test "await_tokio does not leak delayed submit errors after timeout" do
    parent = self()

    assert {:error, :timeout} =
             ColdRead.await_tokio(
               fn _proxy, _corr_id ->
                 send(parent, :submit_started)
                 Process.sleep(25)
                 {:error, :closed}
               end,
               5
             )

    assert_receive :submit_started
    Process.sleep(50)

    refute_received _
  end

  test "pread_batch uses same-path submit shape when every read hits one file" do
    path = "/tmp/ferricstore-00001.log"

    assert {:single_path, ^path, [10, 20, 30]} =
             ColdRead.pread_batch_submit_shape([{path, 10}, {path, 20}, {path, 30}])
  end

  test "pread_batch groups mixed paths without repeating the same path per offset" do
    path_a = "/tmp/00001.log"
    path_b = "/tmp/00002.log"
    locations = [{path_a, 10}, {path_b, 20}, {path_a, 30}]

    assert {:grouped_paths, [{^path_a, [{0, 10}, {2, 30}]}, {^path_b, [{1, 20}]}]} =
             ColdRead.pread_batch_submit_shape(locations)
  end

  test "keyed batch result normalization rejects silent cardinality truncation" do
    assert {:ok, ["a", nil]} =
             ColdRead.normalize_keyed_batch_result({:ok, ["a", nil]}, 2)

    assert {:error, {:batch_result_length_mismatch, 2, 1}} =
             ColdRead.normalize_keyed_batch_result({:ok, ["only-one"]}, 2)

    assert {:error, {:invalid_batch_result, :invalid}} =
             ColdRead.normalize_keyed_batch_result(:invalid, 2)
  end

  test "keyed batch reads recover old offsets from a compaction backup" do
    dir =
      Path.join(System.tmp_dir!(), "cold-read-generation-#{System.unique_integer([:positive])}")

    source = Path.join(dir, "00000.log")
    backup = Path.join(dir, "compaction_backup_0.log")
    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, [{new_offset, _size}]} = NIF.v2_append_batch(source, [{"new", "new-value", 0}])

    assert {:ok, [_dead, {old_offset, _size}]} =
             NIF.v2_append_batch(backup, [
               {"dead", "dead-value", 0},
               {"old", "old-value", 0}
             ])

    assert {:ok, ["old-value", "new-value"]} =
             ColdRead.pread_batch_keyed(
               [{source, old_offset, "old"}, {source, new_offset, "new"}],
               5_000
             )
  end

  test "keyed batch reads never follow a symlinked compaction backup" do
    dir =
      Path.join(System.tmp_dir!(), "cold-read-backup-link-#{System.unique_integer([:positive])}")

    source = Path.join(dir, "00000.log")
    external = Path.join(dir, "external.log")
    backup = Path.join(dir, "compaction_backup_0.log")
    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, [{_source_offset, _size}]} =
             NIF.v2_append_batch(source, [{"different", "source-value", 0}])

    assert {:ok, [{external_offset, _size}]} =
             NIF.v2_append_batch(external, [{"secret", "external-value", 0}])

    File.ln_s!(external, backup)

    assert {:ok, [nil]} =
             ColdRead.pread_batch_keyed([{source, external_offset, "secret"}], 5_000)
  end

  test "current keyed batch reads retry a relocated row after its backup is gone" do
    dir =
      Path.join(System.tmp_dir!(), "cold-read-current-#{System.unique_integer([:positive])}")

    source = Path.join(dir, "00000.log")
    replacement = Path.join(dir, "replacement.log")
    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, [_dead, {old_offset, _size}]} =
             NIF.v2_append_batch(source, [
               {"dead", "dead-value", 0},
               {"key", "current-value", 0}
             ])

    assert {:ok, [{new_offset, _size}]} =
             NIF.v2_append_batch(replacement, [{"key", "current-value", 0}])

    File.rename!(replacement, source)
    old_token = {:keydir_row, 0, old_offset}
    new_token = {:keydir_row, 0, new_offset}

    assert {:ok, [{:value, "current-value", ^new_token}]} =
             ColdRead.pread_batch_keyed_current(
               [{source, old_offset, "key", old_token}],
               fn
                 "key", ^old_token -> {:cold, source, new_offset, new_token}
                 "key", ^new_token -> {:cold, source, new_offset, new_token}
               end,
               5_000
             )
  end

  test "current keyed batch reads can resolve a concurrently warmed value without disk IO" do
    missing_path =
      Path.join(System.tmp_dir!(), "missing-#{System.unique_integer([:positive])}.log")

    token = {:keydir_row, :old}
    current_token = {:keydir_row, :current}

    assert {:ok, [{:value, "hot-value", ^current_token}]} =
             ColdRead.pread_batch_keyed_current(
               [{missing_path, 0, "key", token}],
               fn "key", ^token -> {:hot, "hot-value", current_token} end,
               5_000
             )
  end

  test "current keyed batch reads revalidate successful disk values" do
    dir =
      Path.join(System.tmp_dir!(), "cold-read-success-race-#{System.unique_integer([:positive])}")

    path = Path.join(dir, "00000.log")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)

    assert {:ok, [{offset, _size}]} = NIF.v2_append_batch(path, [{"key", "old-value", 0}])
    old_token = {:keydir_row, :old}
    new_token = {:keydir_row, :new}

    assert {:ok, [{:value, "new-value", ^new_token}]} =
             ColdRead.pread_batch_keyed_current(
               [{path, offset, "key", old_token}],
               fn "key", ^old_token -> {:hot, "new-value", new_token} end,
               5_000
             )
  end
end
