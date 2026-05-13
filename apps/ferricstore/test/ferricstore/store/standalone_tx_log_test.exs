defmodule Ferricstore.Store.StandaloneTxLogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.StandaloneTxLog

  test "prepare and commit persist markers and compact committed transactions" do
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

  defp tmp_dir do
    Path.join(
      System.tmp_dir!(),
      "ferricstore_standalone_tx_log_#{System.unique_integer([:positive])}"
    )
  end
end
