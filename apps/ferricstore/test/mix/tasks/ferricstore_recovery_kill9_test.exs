defmodule Mix.Tasks.FerricstoreRecoveryKill9Test do
  use ExUnit.Case, async: true

  alias Mix.Tasks.Ferricstore.RecoveryKill9

  describe "parse_args!/1" do
    test "uses conservative defaults" do
      opts = RecoveryKill9.parse_args!([])

      assert opts.writes == 2_000
      assert opts.batch_size == 1_000
      assert opts.timeout_ms == 120_000
      assert opts.release_cursor_interval == 500
      assert opts.prefix =~ "kill9_"
      assert opts.data_dir =~ "ferricstore_kill9_"
    end

    test "accepts explicit options" do
      opts =
        RecoveryKill9.parse_args!([
          "--writes",
          "10",
          "--batch-size",
          "250",
          "--timeout-ms",
          "5000",
          "--release-cursor-interval",
          "100",
          "--prefix",
          "pfx",
          "--data-dir",
          "/tmp/ferricstore_manual"
        ])

      assert opts.writes == 10
      assert opts.batch_size == 250
      assert opts.timeout_ms == 5_000
      assert opts.release_cursor_interval == 100
      assert opts.prefix == "pfx"
      assert opts.data_dir == "/tmp/ferricstore_manual"
    end
  end

  describe "marker parsing" do
    test "parses child marker lines into event maps" do
      assert {:ok, marker} =
               RecoveryKill9.parse_marker(
                 "FERRICSTORE_KILL9 event=WRITE_DONE writes=5 gap=4 pid=123"
               )

      assert marker["event"] == "WRITE_DONE"
      assert marker["writes"] == "5"
      assert marker["gap"] == "4"
      assert marker["pid"] == "123"
    end

    test "ignores non-marker lines" do
      assert :ignore = RecoveryKill9.parse_marker("normal application log line")
    end
  end

  describe "profile formatting" do
    test "formats aggregated startup profile without spaces for marker transport" do
      profile = %{
        recover_keydir: 12,
        start_raft: 34,
        compute_file_stats: 56
      }

      formatted = RecoveryKill9.format_profile(profile)

      assert formatted == "compute_file_stats:56,recover_keydir:12,start_raft:34"
      refute String.contains?(formatted, " ")
    end
  end

  describe "dataset verification" do
    test "checks every recovered key rather than three samples" do
      opts = %{writes: 5, batch_size: 2, prefix: "recovered"}

      fetch_batch = fn keys ->
        Enum.map(keys, fn "recovered:" <> index_text ->
          index = String.to_integer(index_text)
          if index == 2, do: "corrupt", else: "v#{index}"
        end)
      end

      assert_raise Mix.Error, ~r/key 2 expected "v2", got "corrupt"/, fn ->
        RecoveryKill9.verify_dataset!(opts, fetch_batch)
      end
    end
  end

  describe "child process lifecycle" do
    test "always invokes cleanup when child work raises" do
      parent = self()

      assert_raise RuntimeError, "marker failed", fn ->
        RecoveryKill9.with_child_cleanup(
          :child,
          fn _child -> raise "marker failed" end,
          fn child -> send(parent, {:cleaned, child}) end
        )
      end

      assert_receive {:cleaned, :child}
    end

    test "rejects a marker pid that does not belong to the opened child port" do
      assert RecoveryKill9.validate_marker_pid!(%{"pid" => "123"}, 123) == 123

      assert_raise Mix.Error, ~r/does not match child port pid/, fn ->
        RecoveryKill9.validate_marker_pid!(%{"pid" => "456"}, 123)
      end
    end
  end

  describe "child output buffering" do
    test "bounds an unterminated output line while preserving later markers" do
      oversized = String.duplicate("x", 100_000)

      {buffer, recent, nil} =
        RecoveryKill9.consume_output("", oversized, [], "WRITE_DONE")

      assert byte_size(buffer) <= 65_536

      {_buffer, recent, marker} =
        RecoveryKill9.consume_output(
          buffer,
          "\nFERRICSTORE_KILL9 event=WRITE_DONE pid=123\n",
          recent,
          "WRITE_DONE"
        )

      assert marker["pid"] == "123"
      assert Enum.all?(recent, &(byte_size(&1) <= 4_096))
    end
  end

  describe "child_env/2" do
    test "passes only harness settings to child process" do
      opts = %{
        data_dir: "/tmp/ferricstore_manual",
        writes: 10,
        batch_size: 250,
        timeout_ms: 5_000,
        prefix: "pfx",
        release_cursor_interval: 100
      }

      env = RecoveryKill9.child_env(:writer, opts)

      assert {"FERRICSTORE_KILL9_CHILD", "writer"} in env
      assert {"FERRICSTORE_KILL9_DATA_DIR", "/tmp/ferricstore_manual"} in env
      assert {"FERRICSTORE_KILL9_WRITES", "10"} in env
      assert {"FERRICSTORE_KILL9_BATCH_SIZE", "250"} in env
      assert {"FERRICSTORE_KILL9_TIMEOUT_MS", "5000"} in env
      assert {"FERRICSTORE_KILL9_PREFIX", "pfx"} in env
      assert {"FERRICSTORE_KILL9_RELEASE_CURSOR_INTERVAL", "100"} in env
    end

    test "builds env executable arguments without Port env options" do
      opts = %{
        data_dir: "/tmp/ferricstore_manual",
        writes: 10,
        batch_size: 250,
        timeout_ms: 5_000,
        prefix: "pfx",
        release_cursor_interval: 100
      }

      args = RecoveryKill9.child_args(:verifier, opts)

      assert "FERRICSTORE_KILL9_CHILD=verifier" in args
      assert "FERRICSTORE_KILL9_DATA_DIR=/tmp/ferricstore_manual" in args
      assert List.last(args) == "ferricstore.recovery_kill9"
      assert Enum.at(args, -2) =~ "/mix"
    end
  end
end
