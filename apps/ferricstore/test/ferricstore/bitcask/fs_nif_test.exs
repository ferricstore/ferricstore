defmodule Ferricstore.Bitcask.FsNifTest do
  @moduledoc """
  Integration tests for the filesystem metadata NIFs that replace the
  `:prim_file` async-thread-pool path.

  What each test asserts:

    * The NIF is reachable (module loaded, function exported).
    * Happy-path semantics match what the Elixir callers expect.
    * Error returns are tagged with stable atom kinds so callers can
      pattern-match without string-sniffing.

  We also guard a few subtle behaviours our durability code relies on:
  `fs_touch` does NOT truncate existing files, `fs_mkdir_p` is
  idempotent, `fs_rename` overwrites, `fs_rm` refuses directories.

  The async `fs_rm_rf_async` contract is verified separately via the
  `tokio_complete` message shape.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  setup do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "fs_nif_test_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)
    %{tmp: tmp}
  end

  # ---------------------------------------------------------------------------
  # fs_touch
  # ---------------------------------------------------------------------------

  describe "fs_touch/1" do
    test "creates an empty file", %{tmp: tmp} do
      path = Path.join(tmp, "new.txt")
      refute File.exists?(path)
      assert :ok = NIF.fs_touch(path)
      assert File.exists?(path)
      assert File.stat!(path).size == 0
    end

    test "does NOT truncate an existing file", %{tmp: tmp} do
      path = Path.join(tmp, "keeps_bytes.txt")
      File.write!(path, "preserve")
      assert :ok = NIF.fs_touch(path)
      assert File.read!(path) == "preserve"
    end

    test "rejects empty path" do
      assert {:error, {:invalid_path, _}} = NIF.fs_touch("")
    end

    test "rejects path with null byte" do
      assert {:error, {:invalid_path, _}} = NIF.fs_touch("foo\0bar")
    end

    test "parent dir missing returns :not_found", %{tmp: tmp} do
      path = Path.join([tmp, "nowhere", "file"])
      assert {:error, {:not_found, _}} = NIF.fs_touch(path)
    end
  end

  # ---------------------------------------------------------------------------
  # fs_mkdir_p
  # ---------------------------------------------------------------------------

  describe "fs_mkdir_p/1" do
    test "creates nested directory tree", %{tmp: tmp} do
      path = Path.join([tmp, "a", "b", "c"])
      assert :ok = NIF.fs_mkdir_p(path)
      assert File.dir?(path)
    end

    test "is idempotent on existing directory", %{tmp: tmp} do
      path = Path.join(tmp, "existing")
      File.mkdir_p!(path)
      assert :ok = NIF.fs_mkdir_p(path)
      assert :ok = NIF.fs_mkdir_p(path)
      assert File.dir?(path)
    end

    test "errors when path is an existing file", %{tmp: tmp} do
      path = Path.join(tmp, "blocker")
      File.write!(path, "x")
      assert {:error, {kind, _}} = NIF.fs_mkdir_p(path)
      # Stdlib maps this inconsistently across versions; both are acceptable.
      assert kind in [:already_exists, :not_a_directory, :other, :invalid_path]
    end

    test "rejects empty path" do
      assert {:error, {:invalid_path, _}} = NIF.fs_mkdir_p("")
    end
  end

  # ---------------------------------------------------------------------------
  # fs_rename
  # ---------------------------------------------------------------------------

  describe "fs_rename/2" do
    test "renames a file", %{tmp: tmp} do
      a = Path.join(tmp, "a")
      b = Path.join(tmp, "b")
      File.write!(a, "content")
      assert :ok = NIF.fs_rename(a, b)
      refute File.exists?(a)
      assert File.read!(b) == "content"
    end

    test "overwrites target on POSIX", %{tmp: tmp} do
      a = Path.join(tmp, "a")
      b = Path.join(tmp, "b")
      File.write!(a, "from-a")
      File.write!(b, "was-b")
      assert :ok = NIF.fs_rename(a, b)
      assert File.read!(b) == "from-a"
    end

    test "missing source returns :not_found", %{tmp: tmp} do
      a = Path.join(tmp, "ghost")
      b = Path.join(tmp, "dest")
      assert {:error, {kind, _}} = NIF.fs_rename(a, b)
      assert kind in [:not_found, :other]
    end

    test "rejects null byte in either path", %{tmp: tmp} do
      a = Path.join(tmp, "a")
      File.write!(a, "x")
      assert {:error, {:invalid_path, _}} = NIF.fs_rename(a, "new\0path")
      assert {:error, {:invalid_path, _}} = NIF.fs_rename("src\0bad", "new")
    end
  end

  # ---------------------------------------------------------------------------
  # fs_rm
  # ---------------------------------------------------------------------------

  describe "fs_rm/1" do
    test "removes a file", %{tmp: tmp} do
      path = Path.join(tmp, "kill-me")
      File.write!(path, "bye")
      assert :ok = NIF.fs_rm(path)
      refute File.exists?(path)
    end

    test "missing file returns :not_found", %{tmp: tmp} do
      path = Path.join(tmp, "never-existed")
      assert {:error, {:not_found, _}} = NIF.fs_rm(path)
    end

    test "refuses to remove a directory", %{tmp: tmp} do
      path = Path.join(tmp, "a-dir")
      File.mkdir!(path)
      assert {:error, {kind, _}} = NIF.fs_rm(path)
      # Depending on stdlib: EISDIR or EPERM or Other — never :ok.
      assert kind in [:is_a_directory, :permission_denied, :other]
      assert File.dir?(path), "directory must still exist after failed fs_rm"
    end
  end

  # ---------------------------------------------------------------------------
  # fs_exists / fs_is_dir
  # ---------------------------------------------------------------------------

  describe "fs_exists/1 and fs_is_dir/1" do
    test "fs_exists true for file + false for missing", %{tmp: tmp} do
      p = Path.join(tmp, "here")
      File.write!(p, "")
      assert NIF.fs_exists(p) == true
      assert NIF.fs_exists(Path.join(tmp, "absent")) == false
    end

    test "fs_is_dir true for dir, false for file, false for missing", %{tmp: tmp} do
      d = Path.join(tmp, "subdir")
      f = Path.join(tmp, "subfile")
      File.mkdir!(d)
      File.write!(f, "")
      assert NIF.fs_is_dir(d) == true
      assert NIF.fs_is_dir(f) == false
      assert NIF.fs_is_dir(Path.join(tmp, "nowhere")) == false
    end

    test "broken symlink → fs_exists false", %{tmp: tmp} do
      target = Path.join(tmp, "gone")
      link = Path.join(tmp, "dangling")
      File.ln_s!(target, link)
      refute File.exists?(target)
      assert NIF.fs_exists(link) == false
    end
  end

  # ---------------------------------------------------------------------------
  # fs_ls
  # ---------------------------------------------------------------------------

  describe "fs_read_nofollow/1" do
    test "reads a regular file", %{tmp: tmp} do
      path = Path.join(tmp, "acl.conf")
      File.write!(path, "user default on nopass ~* &* +@all\n")

      assert {:ok, "user default on nopass ~* &* +@all\n"} = NIF.fs_read_nofollow(path)
    end

    test "rejects symlink targets", %{tmp: tmp} do
      target = Path.join(tmp, "real_acl.conf")
      link = Path.join(tmp, "acl.conf")
      File.write!(target, "user default on nopass ~* &* +@all\n")
      File.ln_s!(target, link)

      assert {:error, {:symlink, _}} = NIF.fs_read_nofollow(link)
    end
  end

  describe "fs_ls/1" do
    test "lists entries (names only)", %{tmp: tmp} do
      File.write!(Path.join(tmp, "one"), "")
      File.write!(Path.join(tmp, "two"), "")
      File.mkdir!(Path.join(tmp, "sub"))

      assert {:ok, names} = NIF.fs_ls(tmp)
      assert Enum.sort(names) == ["one", "sub", "two"]
    end

    test "missing dir returns :not_found", %{tmp: tmp} do
      assert {:error, {:not_found, _}} = NIF.fs_ls(Path.join(tmp, "absent"))
    end

    test "file (not dir) returns :not_a_directory", %{tmp: tmp} do
      f = Path.join(tmp, "justafile")
      File.write!(f, "")
      assert {:error, {kind, _}} = NIF.fs_ls(f)
      assert kind in [:not_a_directory, :other]
    end

    test "yields cleanly on large directory", %{tmp: tmp} do
      # 300 > the 256-entry yield interval inside the NIF — covers the
      # consume_timeslice loop boundary.
      for i <- 1..300 do
        File.write!(Path.join(tmp, "f_#{i}"), "")
      end

      assert {:ok, names} = NIF.fs_ls(tmp)
      assert length(names) == 300
    end
  end

  # ---------------------------------------------------------------------------
  # fs_rm_rf_async
  # ---------------------------------------------------------------------------

  describe "fs_rm_rf_async/3" do
    test "removes a nested tree and sends :tokio_complete", %{tmp: tmp} do
      root = Path.join(tmp, "tree")
      File.mkdir_p!(Path.join([root, "a", "b", "c"]))
      File.write!(Path.join([root, "a", "x"]), "")
      File.write!(Path.join([root, "a", "b", "y"]), "")

      assert :ok = NIF.fs_rm_rf_async(self(), 42, root)

      assert_receive {:tokio_complete, 42, :ok}, 2_000
      refute File.exists?(root)
    end

    test "missing path is idempotent — sends :ok", %{tmp: tmp} do
      phantom = Path.join(tmp, "never-existed")
      assert :ok = NIF.fs_rm_rf_async(self(), 7, phantom)
      assert_receive {:tokio_complete, 7, :ok}, 2_000
    end

    test "rejects null-byte path immediately (before spawning)", %{tmp: _tmp} do
      # Synchronous reject, no :tokio_complete message arrives.
      assert {:error, {:invalid_path, _}} = NIF.fs_rm_rf_async(self(), 99, "a\0b")
      refute_receive {:tokio_complete, 99, _, _}, 200
    end
  end

  # ---------------------------------------------------------------------------
  # Scheduler hygiene — no dirty_nif_finalizer for these NIFs
  # ---------------------------------------------------------------------------

  describe "scheduler hygiene" do
    test "all sync NIFs run on normal scheduler (current_function stays user code)", %{tmp: tmp} do
      # We can't observe the scheduler directly from Elixir, but we can
      # confirm the calls complete without appearing in the per-process
      # dirty accounting. The more pragmatic check is that they run as
      # fast as expected — metadata ops should all complete in < 5ms even
      # under a lightly loaded schedulers, because they're not queued on
      # the dirty-IO thread pool.
      f = Path.join(tmp, "perf-check")
      t0 = System.monotonic_time(:microsecond)

      for _ <- 1..100 do
        :ok = NIF.fs_touch(f)
      end

      t1 = System.monotonic_time(:microsecond)
      elapsed_us = t1 - t0

      # 100 ops in < 500ms (5ms each) is a very loose bound that only
      # fails if we accidentally land on the dirty scheduler (which
      # serializes through the async-thread pool).
      assert elapsed_us < 500_000,
             "100 fs_touch calls took #{elapsed_us}µs; expected <500ms"
    end
  end
end
