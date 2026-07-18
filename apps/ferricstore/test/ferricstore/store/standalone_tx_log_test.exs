defmodule Ferricstore.Store.StandaloneTxLogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.StandaloneTxLog

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

    assert :ok = StandaloneTxLog.commit(data_dir, txid)
    assert File.exists?(tx_log_path)

    assert :ok = StandaloneTxLog.recover(data_dir)
    refute File.exists?(tx_log_path)
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

  test "recover rejects compressed or trailing current-format entries" do
    for kind <- [:compressed, :trailing] do
      data_dir = Path.join(tmp_dir(), Atom.to_string(kind))
      file_path = Path.join(data_dir, "shard_0/000000.data")
      tx_log_path = Path.join(data_dir, "standalone_cross_shard_tx.log")
      File.mkdir_p!(data_dir)

      term =
        {:ferricstore_standalone_cross_shard_tx_v1, :prepare, "txid",
         [{file_path, [{:put, "key", String.duplicate("value", 2_048), 0}]}]}

      payload =
        case kind do
          :compressed -> :erlang.term_to_binary(term, compressed: 9)
          :trailing -> :erlang.term_to_binary(term) <> <<0>>
        end

      if kind == :compressed, do: assert(<<131, 80, _::binary>> = payload)
      File.write!(tx_log_path, Base.encode64(payload) <> "\n")

      assert {:error, {:corrupt_entries, 1}} = StandaloneTxLog.recover(data_dir)
      refute File.exists?(file_path)
    end
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
    cache_key = {StandaloneTxLog, Path.expand(data_dir)}
    sentinel = make_ref()
    :persistent_term.put(cache_key, sentinel)
    on_exit(fn -> :persistent_term.erase(cache_key) end)

    file_path = Path.join(data_dir, "shard_0/000000.data")
    assert {:ok, _txid} = StandaloneTxLog.prepare(data_dir, [{file_path, [{:put, "k", "v", 0}]}])

    assert :persistent_term.get(cache_key) == sentinel
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
    assert source =~ "Ferricstore.FS.read_nofollow(path, @max_journal_bytes)"
    assert source =~ "Ferricstore.FS.append_sync_nofollow_bounded(path, line, append_limit)"
    assert source =~ "Ferricstore.FS.atomic_replace_nofollow(path, data, @max_journal_bytes)"
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

  defp encode_entry(entry), do: Base.encode64(Ferricstore.TermCodec.encode(entry))
end
