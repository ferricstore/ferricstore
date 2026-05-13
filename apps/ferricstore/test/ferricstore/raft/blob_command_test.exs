defmodule Ferricstore.Raft.BlobCommandTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Store.{BlobRef, BlobStore}

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_blob_command_#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(root, 1)
    on_exit(fn -> File.rm_rf!(root) end)

    ctx = %{
      data_dir: root,
      blob_side_channel_threshold_bytes: 128
    }

    %{ctx: ctx, root: root}
  end

  test "prepares large single put as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    payload = :binary.copy("P", 1024)

    assert {:ok, {:put_blob_ref, "k", encoded_ref, 0}} =
             BlobCommand.prepare(ctx, 0, {:put, "k", payload, 0}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert File.read!(BlobRef.path(root, 0, ref)) == payload
  end

  test "prepares mixed put batch without duplicating small values", %{ctx: ctx, root: root} do
    payload = :binary.copy("B", 1024)

    assert {:ok,
            {:put_blob_batch,
             [
               {"small", "v", 0, :value},
               {"large", encoded_ref, 0, :blob_ref}
             ]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:put_batch, [{"small", "v", 0}, {"large", payload, 0}]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert File.read!(BlobRef.path(root, 0, ref)) == payload
  end

  test "prepares generic Ra batches by replacing large puts with blob refs", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("G", 1024)

    assert {:ok, {:batch, [{:append, "log", "x"}, {:put_blob_ref, "k", encoded_ref, 0}]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:batch, [{:append, "log", "x"}, {:put, "k", payload, 0}]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert File.read!(BlobRef.path(root, 0, ref)) == payload
  end

  test "leaves commands unchanged when blob side-channel is disabled", %{ctx: ctx} do
    ctx = %{ctx | blob_side_channel_threshold_bytes: 0}
    command = {:put, "k", :binary.copy("P", 1024), 0}

    assert {:ok, ^command} = BlobCommand.prepare(ctx, 0, command, single_member?: true)
  end

  test "candidate check only selects commands that can externalize", %{ctx: ctx} do
    ref_shaped =
      BlobRef.encode!(%BlobRef{
        size: 1,
        checksum: :binary.copy(<<1>>, 32)
      })

    refute BlobCommand.side_channel_candidate?(ctx, {:put, "small", "v", 0})
    assert BlobCommand.side_channel_candidate?(ctx, {:put, "large", :binary.copy("L", 1024), 0})
    assert BlobCommand.side_channel_candidate?(ctx, {:put, "ref", ref_shaped, 0})

    refute BlobCommand.side_channel_candidate?(ctx, {:put_batch, [{"small", "v", 0}]})

    assert BlobCommand.side_channel_candidate?(
             ctx,
             {:batch, [{:append, "k", "x"}, {:put, "large", :binary.copy("L", 1024), 0}]}
           )
  end

  test "leaves commands unchanged for multi-member Raft groups", %{ctx: ctx} do
    command = {:put, "k", :binary.copy("P", 1024), 0}

    assert {:ok, ^command} = BlobCommand.prepare(ctx, 0, command, single_member?: false)
  end

  test "ref-shaped user bytes are externalized as payload, not reused as a pointer", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("R", 1024)
    assert {:ok, ref} = BlobStore.put(root, 0, payload)
    encoded_ref = BlobRef.encode!(ref)

    assert {:ok, {:put_blob_ref, "k", stored_ref, 0}} =
             BlobCommand.prepare(ctx, 0, {:put, "k", encoded_ref, 0}, single_member?: true)

    assert stored_ref != encoded_ref
    assert {:ok, stored_blob_ref} = BlobRef.decode(stored_ref)
    assert {:ok, ^encoded_ref} = BlobStore.get(root, 0, stored_blob_ref)
  end
end
