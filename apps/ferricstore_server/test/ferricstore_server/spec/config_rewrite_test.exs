defmodule FerricstoreServer.Spec.ConfigRewriteTest do
  @moduledoc """
  Spec: CONFIG REWRITE preserves comments and unknown keys.

  CONFIG REWRITE rewrites only config keys that FerricStore recognises.
  Unknown keys and comments in the original config file are preserved.
  If a key was added to the config file manually but not yet applied via
  CONFIG SET, CONFIG REWRITE overwrites it with the live Raft value --
  the live value wins.

  Tests cover:
    1. Comments preserved after REWRITE
    2. Unknown keys preserved
    3. CONFIG SET changes reflected after REWRITE
    4. New runtime keys added to file
    5. Atomic write (no partial file on crash)
    6. Stress: 100 REWRITE calls don't corrupt file
    7. Edge: empty file, file doesn't exist yet
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Config

  setup do
    path = Config.config_file_path()
    dir = Path.dirname(path)
    File.mkdir_p!(dir)

    # Restore Config GenServer state after each test
    orig_eviction = Application.get_env(:ferricstore, :eviction_policy)
    orig_slowlog_us = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
    orig_slowlog_max = Application.get_env(:ferricstore, :slowlog_max_len)

    on_exit(fn ->
      defaults = Config.defaults()

      Enum.each(defaults, fn {k, v} ->
        try do
          Config.set(k, v)
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
      end)

      if orig_eviction, do: Application.put_env(:ferricstore, :eviction_policy, orig_eviction)

      if orig_slowlog_us, do: Ferricstore.SlowLog.set_threshold(orig_slowlog_us)
      if orig_slowlog_max, do: Ferricstore.SlowLog.set_max_len(orig_slowlog_max)

      # Clean up config file and any .tmp leftover
      if File.exists?(path), do: File.rm(path)

      tmp = path <> ".tmp"
      if File.exists?(tmp), do: File.rm(tmp)
    end)

    %{path: path}
  end

  # ===========================================================================
  # 1. Comments preserved after REWRITE
  # ===========================================================================

  describe "comments preserved after REWRITE" do
    test "single comment line is preserved", %{path: path} do
      File.write!(path, "# This is a comment\nhz 10\n")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "# This is a comment"
    end

    test "multiple comment lines are preserved in order", %{path: path} do
      original = """
      # FerricStore configuration
      # Last updated: 2025-01-01
      hz 10
      # Section: memory
      maxmemory-policy volatile-lru
      """

      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      lines = String.split(content, "\n")
      comment_lines = Enum.filter(lines, &String.starts_with?(&1, "#"))

      assert "# FerricStore configuration" in comment_lines
      assert "# Last updated: 2025-01-01" in comment_lines
      assert "# Section: memory" in comment_lines
    end

    test "comment-only file is preserved with new keys appended", %{path: path} do
      File.write!(path, "# Only comments here\n# Another comment\n")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "# Only comments here"
      assert content =~ "# Another comment"
    end

    test "inline position of comments relative to keys is maintained", %{path: path} do
      original = "# Header\nhz 10\n# Footer\n"
      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      lines = String.split(content, "\n", trim: true)

      header_idx = Enum.find_index(lines, &(&1 == "# Header"))
      footer_idx = Enum.find_index(lines, &(&1 == "# Footer"))

      assert header_idx != nil
      assert footer_idx != nil
      assert header_idx < footer_idx
    end

    test "empty comment lines (just #) are preserved", %{path: path} do
      original = "#\nhz 10\n#\n"
      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      lines = String.split(content, "\n", trim: true)
      hash_only = Enum.count(lines, &(&1 == "#"))
      assert hash_only >= 2
    end
  end

  # ===========================================================================
  # 2. Unknown keys preserved
  # ===========================================================================

  describe "unknown keys preserved" do
    test "a single unknown key is preserved verbatim", %{path: path} do
      original = "hz 10\ncustom-setting some-value\n"
      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "custom-setting some-value"
    end

    test "multiple unknown keys are all preserved", %{path: path} do
      original = "my-plugin-key abc\nhz 10\nanother-unknown 42\n"
      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "my-plugin-key abc"
      assert content =~ "another-unknown 42"
    end

    test "unknown keys with multi-word values are preserved", %{path: path} do
      original = "custom-plugin option1 option2 option3\nhz 10\n"
      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "custom-plugin option1 option2 option3"
    end

    test "unknown keys mixed with comments are both preserved", %{path: path} do
      original = """
      # Plugin config
      my-plugin-enabled yes
      # Another section
      hz 10
      """

      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "# Plugin config"
      assert content =~ "my-plugin-enabled yes"
      assert content =~ "# Another section"
    end
  end

  # ===========================================================================
  # 3. CONFIG SET changes reflected after REWRITE
  # ===========================================================================

  describe "CONFIG SET changes reflected after REWRITE" do
    test "CONFIG SET hz then REWRITE writes updated value", %{path: path} do
      File.write!(path, "hz 10\n")
      Config.set("hz", "42")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "hz 42"
      refute content =~ "hz 10"
    end

    test "CONFIG SET maxmemory-policy then REWRITE writes updated value", %{path: path} do
      File.write!(path, "maxmemory-policy volatile-lru\n")
      Config.set("maxmemory-policy", "allkeys-lru")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "maxmemory-policy allkeys-lru"
      refute content =~ "maxmemory-policy volatile-lru"
    end

    test "CONFIG SET updates value while preserving surrounding comments", %{path: path} do
      original = "# Tuning\nhz 10\n# End tuning\n"
      File.write!(path, original)
      Config.set("hz", "99")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "# Tuning"
      assert content =~ "hz 99"
      assert content =~ "# End tuning"
    end

    test "manual file edit is overwritten by live value (live wins)", %{path: path} do
      # Manually write hz 200 to file, but live value is still default (10)
      File.write!(path, "hz 200\n")
      # Do NOT call Config.set — the live value is still the default
      assert :ok = Config.rewrite()

      content = File.read!(path)
      # The live value should win over the manual edit
      live_pairs = Config.get("hz")
      [{"hz", live_hz}] = live_pairs

      assert content =~ "hz #{live_hz}"
    end

    test "multiple CONFIG SET calls then single REWRITE reflects all changes", %{path: path} do
      File.write!(path, "hz 10\nslowlog-max-len 128\n")
      Config.set("hz", "50")
      Config.set("slowlog-max-len", "256")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "hz 50"
      assert content =~ "slowlog-max-len 256"
    end

    test "CONFIG REWRITE does not persist requirepass plaintext", %{path: path} do
      Config.set("requirepass", "super-secret")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "requirepass "
      refute content =~ "super-secret"

      {:ok, stat} = File.stat(path)
      assert Bitwise.band(stat.mode, 0o777) == 0o600
    end
  end

  # ===========================================================================
  # 4. New runtime keys added to file
  # ===========================================================================

  describe "new runtime keys added to file" do
    test "keys not in file are appended after REWRITE", %{path: path} do
      # Write a file with only one known key
      File.write!(path, "hz 10\n")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      # Other known keys (e.g. bind, maxmemory, maxclients) should appear
      assert content =~ "bind"
      assert content =~ "maxmemory "
      assert content =~ "maxclients"
    end

    test "CONFIG SET on a param not in file causes it to appear after REWRITE", %{path: path} do
      File.write!(path, "# Minimal config\n")
      Config.set("hz", "77")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "hz 77"
    end

    test "appended keys appear after existing content", %{path: path} do
      File.write!(path, "# My config\nhz 10\n")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      lines = String.split(content, "\n", trim: true)

      # "# My config" should still be first
      assert hd(lines) == "# My config"
      # hz should still be near the top (second line)
      hz_idx = Enum.find_index(lines, &String.starts_with?(&1, "hz "))
      assert hz_idx == 1

      # New keys should appear after the original content
      assert length(lines) > 2
    end
  end

  # ===========================================================================
  # 5. Atomic write (write .tmp then rename)
  # ===========================================================================

  describe "atomic write" do
    test "no .tmp file remains after successful REWRITE", %{path: path} do
      assert :ok = Config.rewrite()

      tmp_path = path <> ".tmp"
      refute File.exists?(tmp_path)
    end

    test "config file exists after REWRITE", %{path: path} do
      assert :ok = Config.rewrite()
      assert File.exists?(path)
    end

    test "file content is complete (ends with newline, no truncation)", %{path: path} do
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert String.ends_with?(content, "\n")
      # Verify parseable: every non-empty, non-comment line has at least key + space + value
      lines = String.split(content, "\n", trim: true)

      for line <- lines do
        cond do
          String.starts_with?(line, "#") ->
            :ok

          true ->
            # Each config line should have at least a key and value separated by space
            assert String.contains?(line, " "),
                   "Config line missing space separator: #{inspect(line)}"
        end
      end
    end
  end

  # ===========================================================================
  # 6. Stress: 100 REWRITE calls don't corrupt file
  # ===========================================================================

  describe "stress: repeated REWRITE calls" do
    test "100 sequential REWRITE calls produce a valid file", %{path: path} do
      original = "# Stress test header\nhz 10\ncustom-key my-value\n"
      File.write!(path, original)

      for _i <- 1..100 do
        assert :ok = Config.rewrite()
      end

      content = File.read!(path)
      # Comments and unknown keys preserved
      assert content =~ "# Stress test header"
      assert content =~ "custom-key my-value"
      # Known keys present
      assert content =~ "hz "
      assert content =~ "bind "

      # No duplicate keys: each known key should appear at most once
      lines =
        content
        |> String.split("\n", trim: true)
        |> Enum.reject(&String.starts_with?(&1, "#"))

      keys =
        Enum.map(lines, fn line ->
          line |> String.split(" ", parts: 2) |> hd()
        end)

      assert keys == Enum.uniq(keys),
             "Duplicate keys found after 100 rewrites: #{inspect(keys -- Enum.uniq(keys))}"
    end

    test "100 REWRITE calls with intermittent CONFIG SET produce valid file", %{path: path} do
      File.write!(path, "# Dynamic test\nhz 10\n")

      for i <- 1..100 do
        if rem(i, 10) == 0 do
          Config.set("hz", to_string(i))
        end

        assert :ok = Config.rewrite()
      end

      content = File.read!(path)
      assert content =~ "# Dynamic test"
      # After 100 iterations, the last CONFIG SET was at i=100
      assert content =~ "hz 100"

      # No duplicates
      lines =
        content
        |> String.split("\n", trim: true)
        |> Enum.reject(&String.starts_with?(&1, "#"))

      keys =
        Enum.map(lines, fn line ->
          line |> String.split(" ", parts: 2) |> hd()
        end)

      assert keys == Enum.uniq(keys)
    end
  end

  # ===========================================================================
  # 7. Edge cases: empty file, file doesn't exist yet
  # ===========================================================================

  describe "edge: empty file" do
    test "REWRITE on empty file populates all known keys", %{path: path} do
      File.write!(path, "")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      assert content =~ "hz "
      assert content =~ "bind "
      assert content =~ "maxmemory "
    end
  end

  describe "edge: file doesn't exist yet" do
    test "REWRITE creates file from scratch when it doesn't exist", %{path: path} do
      if File.exists?(path), do: File.rm!(path)
      refute File.exists?(path)

      assert :ok = Config.rewrite()

      assert File.exists?(path)
      content = File.read!(path)
      assert content =~ "hz "
      assert content =~ "bind "
      assert content =~ "maxmemory "
    end
  end

  describe "edge: blank lines preserved" do
    test "blank lines in original file are preserved", %{path: path} do
      original = "# Section A\nhz 10\n\n# Section B\nbind 127.0.0.1\n"
      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      # The blank line should still be there
      assert content =~ "\n\n"
    end
  end

  describe "edge: values with spaces" do
    test "known key with space-containing value is written correctly", %{path: path} do
      # The "save" legacy param can have an empty value
      File.write!(path, "save \n")
      assert :ok = Config.rewrite()

      content = File.read!(path)
      # save should appear with its live value
      assert content =~ "save "
    end
  end

  describe "ordering: existing lines maintain relative order" do
    test "original line order of known keys is preserved", %{path: path} do
      original = "bind 127.0.0.1\nhz 10\nmaxmemory-policy volatile-lru\n"
      File.write!(path, original)
      assert :ok = Config.rewrite()

      content = File.read!(path)
      lines = String.split(content, "\n", trim: true)

      bind_idx = Enum.find_index(lines, &String.starts_with?(&1, "bind "))
      hz_idx = Enum.find_index(lines, &String.starts_with?(&1, "hz "))
      policy_idx = Enum.find_index(lines, &String.starts_with?(&1, "maxmemory-policy "))

      assert bind_idx != nil
      assert hz_idx != nil
      assert policy_idx != nil
      assert bind_idx < hz_idx
      assert hz_idx < policy_idx
    end
  end
end
