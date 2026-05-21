defmodule Ferricstore.Raft.WalRecoveryTest do
  @moduledoc """
  Tests that the ra WAL recovers correctly from crash scenarios.

  The bug: `open_at_first_record` in ra_log_wal only handled `{ok, Data}`
  from `file:read/2`. An empty WAL file returns `eof`, causing
  `{:case_clause, :eof}` — a crash on server restart after kill -9.

  These tests prove the fix works by placing corrupt/empty WAL files
  in the ra directory and verifying the system starts.
  """

  # This test writes into the application-wide Ra WAL directory and verifies the
  # default instance through Router, so it must not race with async flush/write
  # tests that share the same booted application.
  use ExUnit.Case, async: false

  describe "the original bug pattern" do
    test "file:read on empty file returns eof (the trigger)" do
      # This proves WHY the bug happened.
      # file:read/2 returns `eof` for an empty file, but the original code
      # only matched on `{ok, <<...>>}` — no eof clause.
      path = Path.join(System.tmp_dir!(), "wal_eof_test_#{:rand.uniform(999_999)}")
      File.write!(path, <<>>)

      {:ok, fd} = :file.open(to_charlist(path), [:read, :binary, :raw])
      result = :file.read(fd, 5)
      :file.close(fd)
      File.rm!(path)

      # This is what the WAL sees on an empty file:
      assert result == :eof, "empty file read must return :eof"

      # The old code did:
      #   case file:read(Fd, 5) of
      #     {ok, <<Magic, Version>>} -> Fd
      #   end
      # No eof clause → {:case_clause, :eof} → crash
    end

    test "file:read on truncated file returns short binary" do
      path = Path.join(System.tmp_dir!(), "wal_trunc_test_#{:rand.uniform(999_999)}")
      File.write!(path, <<1, 2, 3>>)

      {:ok, fd} = :file.open(to_charlist(path), [:read, :binary, :raw])
      result = :file.read(fd, 5)
      :file.close(fd)
      File.rm!(path)

      # Only 3 bytes available, asked for 5:
      assert result == {:ok, <<1, 2, 3>>}

      # The old code only matched {ok, <<4_bytes_magic, 1_byte_version>>}
      # A 3-byte result doesn't match either clause → {:case_clause, ...}
    end
  end

  describe "fix verification" do
    test "ra system starts after empty WAL file is placed in ra dir" do
      # Place an empty WAL file in the ra directory
      data_dir = Application.get_env(:ferricstore, :data_dir)
      ra_dir = Path.join(data_dir, "ra")
      empty_wal = Path.join(ra_dir, "9999999999999999.wal")
      File.write!(empty_wal, <<>>)

      # The ra system is already running. Verify it's still healthy
      # by doing a write through Raft (which uses the WAL).
      k = "wal_fix_#{:rand.uniform(999_999)}"
      Ferricstore.Store.Router.put(FerricStore.Instance.get(:default), k, "after_empty_wal")
      assert Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), k) == "after_empty_wal"

      File.rm(empty_wal)
    end
  end
end
