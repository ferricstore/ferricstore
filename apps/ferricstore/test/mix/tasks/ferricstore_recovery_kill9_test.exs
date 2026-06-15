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
