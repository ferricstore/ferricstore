defmodule Ferricstore.Store.ShardTornTailRecoveryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{HintFile, HintMetadata}
  alias Ferricstore.Store.Shard.Lifecycle

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "shard-tail-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    log = Path.join(root, "00000.log")
    File.touch!(log)
    keydir = :ets.new(:shard_torn_tail, [:set, :public])
    assert {:ok, {offset, _record_size}} = NIF.v2_append_record(log, "before", "original", 0)
    :ets.insert(keydir, {"before", nil, 0, 0, 0, offset, byte_size("original")})
    boundary = File.stat!(log).size
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, log: log, keydir: keydir, boundary: boundary}
  end

  test "active torn tail is removed before a subsequent append and second recovery", ctx do
    File.write!(ctx.log, "partial", [:append])
    assert :ok = Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    assert File.stat!(ctx.log).size == ctx.boundary
    assert_later_append_survives(ctx)
  end

  test "a valid hint does not leave an incomplete appended tail in place", ctx do
    hint = Path.join(ctx.root, "00000.hint")
    assert :ok = HintFile.write_from_keydir(hint, ctx.keydir, 0)
    File.write!(ctx.log, "partial", [:append])
    assert {:ok, ctx.boundary} == HintMetadata.covered_source_size(ctx.log, hint, 0)

    assert :ok = Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    assert File.stat!(ctx.log).size == ctx.boundary
    assert_later_append_survives(ctx)
  end

  test "healthy hint recovery preserves its published metadata", ctx do
    hint = Path.join(ctx.root, "00000.hint")
    assert :ok = HintFile.write_from_keydir(hint, ctx.keydir, 0)
    metadata = File.read!(HintMetadata.metadata_path(hint))
    bytes = File.read!(ctx.log)

    assert :ok = Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    assert File.read!(HintMetadata.metadata_path(hint)) == metadata
    assert File.read!(ctx.log) == bytes
  end

  test "incomplete sealed logs fail recovery without changing any file", ctx do
    File.write!(ctx.log, "partial", [:append])
    active = Path.join(ctx.root, "00001.log")
    assert {:ok, _} = NIF.v2_append_record(active, "newer", "value", 0)
    bytes = File.read!(ctx.log)
    active_bytes = File.read!(active)

    assert_raise RuntimeError, ~r/torn_tail_in_sealed_log/, fn ->
      Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    end

    assert File.read!(ctx.log) == bytes
    assert File.read!(active) == active_bytes
  end

  test "a hint written after a torn tail cannot certify the damaged boundary", ctx do
    File.write!(ctx.log, "partial", [:append])
    hint = Path.join(ctx.root, "00000.hint")
    assert :ok = HintFile.write_from_keydir(hint, ctx.keydir, 0)
    assert {:ok, covered} = HintMetadata.covered_source_size(ctx.log, hint, 0)
    assert covered > ctx.boundary

    assert :ok = Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    assert File.stat!(ctx.log).size == ctx.boundary
    refute File.exists?(HintMetadata.metadata_path(hint))
    assert_later_append_survives(ctx)
  end

  test "a hint covering a torn sealed log fails without truncation", ctx do
    File.write!(ctx.log, "partial", [:append])
    assert :ok = HintFile.write_from_keydir(Path.join(ctx.root, "00000.hint"), ctx.keydir, 0)
    assert {:ok, _} = NIF.v2_append_record(Path.join(ctx.root, "00001.log"), "new", "value", 0)
    bytes = File.read!(ctx.log)

    assert_raise RuntimeError, ~r/torn_tail_in_sealed_log/, fn ->
      Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    end

    assert File.read!(ctx.log) == bytes
  end

  test "active CRC corruption fails closed instead of being treated as crash residue", ctx do
    bytes = File.read!(ctx.log)
    prefix_size = byte_size(bytes) - 1
    <<prefix::binary-size(^prefix_size), last>> = bytes
    corrupted = prefix <> <<Bitwise.bxor(last, 1)>>
    File.write!(ctx.log, corrupted)

    assert_raise RuntimeError, ~r/CRC mismatch/, fn ->
      Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    end

    assert File.read!(ctx.log) == corrupted
  end

  test "Raft-owned startup obtains the repaired active-file boundary", ctx do
    File.write!(ctx.log, "partial", [:append])
    assert Lifecycle.recover_active_file_tail(ctx.log, 0) == ctx.boundary
    assert_later_append_survives(ctx)
  end

  test "startup repairs a large zero-filled torn body before a subsequent append", ctx do
    donor = Path.join(ctx.root, "large-donor")
    assert {:ok, _} = NIF.v2_append_record(donor, "partial", :binary.copy(<<0>>, 3_200_000), 0)
    File.write!(ctx.log, binary_part(File.read!(donor), 0, 2_100_000), [:append])

    assert Lifecycle.recover_active_file_tail(ctx.log, 0) == ctx.boundary
    assert File.stat!(ctx.log).size == ctx.boundary
    assert :ok = Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    assert_later_append_survives(ctx)
  end

  test "startup preserves a complete record behind an ambiguous torn record", ctx do
    donor = Path.join(ctx.root, "donor")
    assert {:ok, _} = NIF.v2_append_record(donor, "partial", String.duplicate("x", 1_000), 0)
    File.write!(ctx.log, binary_part(File.read!(donor), 0, 40), [:append])
    assert {:ok, {offset, _size}} = NIF.v2_append_record(ctx.log, "later", "retained", 0)
    bytes = File.read!(ctx.log)

    for recover <- [
          fn -> Lifecycle.recover_active_file_tail(ctx.log, 0) end,
          fn -> Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0) end
        ] do
      assert {:ok, "retained"} = NIF.v2_pread_at(ctx.log, offset)
      assert_raise RuntimeError, recover
      assert File.read!(ctx.log) == bytes
      assert {:ok, "retained"} = NIF.v2_pread_at(ctx.log, offset)
    end
  end

  defp assert_later_append_survives(ctx) do
    assert {:ok, {offset, _size}} = NIF.v2_append_record(ctx.log, "after", "retained", 0)
    assert offset == ctx.boundary
    :ets.delete_all_objects(ctx.keydir)
    assert :ok = Lifecycle.recover_keydir(ctx.root, ctx.keydir, 0)
    assert [{"before", _, _, _, _, _, _}] = :ets.lookup(ctx.keydir, "before")
    assert [{"after", _, 0, _, 0, ^offset, _}] = :ets.lookup(ctx.keydir, "after")
    assert {:ok, "retained"} = NIF.v2_pread_at(ctx.log, offset)
  end
end
