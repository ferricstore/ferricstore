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
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares large conditional set as a pre-externalized blob ref", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("S", 1024)
    opts = %{nx: true, xx: false, get: false, keepttl: false}

    assert {:ok, {:set_blob_ref, "k", encoded_ref, 0, ^opts}} =
             BlobCommand.prepare(ctx, 0, {:set, "k", payload, 0, opts}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares large getset as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    payload = :binary.copy("T", 1024)

    assert {:ok, {:getset_blob_ref, "k", encoded_ref}} =
             BlobCommand.prepare(ctx, 0, {:getset, "k", payload}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares large append as a pre-externalized blob ref", %{ctx: ctx, root: root} do
    suffix = :binary.copy("A", 1024)

    assert {:ok, {:append_blob_ref, "k", encoded_ref}} =
             BlobCommand.prepare(ctx, 0, {:append, "k", suffix}, single_member?: true)

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^suffix} = BlobStore.get(root, 0, ref)
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
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
  end

  test "prepares generic Ra batches by replacing large puts with blob refs", %{
    ctx: ctx,
    root: root
  } do
    payload = :binary.copy("G", 1024)
    set_payload = :binary.copy("S", 1024)
    getset_payload = :binary.copy("T", 1024)
    append_suffix = :binary.copy("A", 1024)
    opts = %{nx: true, xx: false, get: false, keepttl: false}

    assert {:ok,
            {:batch,
             [
               {:append, "log", "x"},
               {:put_blob_ref, "k", encoded_ref, 0},
               {:set_blob_ref, "s", set_encoded_ref, 0, ^opts},
               {:getset_blob_ref, "g", getset_encoded_ref},
               {:append_blob_ref, "a", append_encoded_ref}
             ]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:batch,
                [
                  {:append, "log", "x"},
                  {:put, "k", payload, 0},
                  {:set, "s", set_payload, 0, opts},
                  {:getset, "g", getset_payload},
                  {:append, "a", append_suffix}
                ]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, set_ref} = BlobRef.decode(set_encoded_ref)
    assert {:ok, getset_ref} = BlobRef.decode(getset_encoded_ref)
    assert {:ok, append_ref} = BlobRef.decode(append_encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
    assert {:ok, ^set_payload} = BlobStore.get(root, 0, set_ref)
    assert {:ok, ^getset_payload} = BlobStore.get(root, 0, getset_ref)
    assert {:ok, ^append_suffix} = BlobStore.get(root, 0, append_ref)
  end

  test "prepares generic Ra batches with one blob segment fsync", %{ctx: ctx, root: root} do
    parent = self()

    Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
      send(parent, {:blob_fsync_file, path})
      :ok
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_fsync_file_hook) end)

    payload_a = :binary.copy("A", 1024)
    payload_b = :binary.copy("B", 1024)

    assert {:ok,
            {:batch, [{:put_blob_ref, "a", encoded_a, 0}, {:put_blob_ref, "b", encoded_b, 0}]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:batch, [{:put, "a", payload_a, 0}, {:put, "b", payload_b, 0}]},
               single_member?: true
             )

    assert {:ok, ref_a} = BlobRef.decode(encoded_a)
    assert {:ok, ref_b} = BlobRef.decode(encoded_b)
    assert {:ok, {segment_path, _offset_a, _size_a}} = BlobStore.file_ref(root, 0, ref_a)
    assert {:ok, {^segment_path, _offset_b, _size_b}} = BlobStore.file_ref(root, 0, ref_b)

    assert_received {:blob_fsync_file, ^segment_path}
    refute_received {:blob_fsync_file, _}
  end

  test "prepares nested put_batch inside generic Ra batches", %{ctx: ctx, root: root} do
    payload = :binary.copy("N", 1024)

    assert {:ok,
            {:batch,
             [
               {:append, "log", "x"},
               {:put_blob_ref, "large", encoded_ref, 0},
               {:put, "small", "v", 0}
             ]}} =
             BlobCommand.prepare(
               ctx,
               0,
               {:batch,
                [
                  {:append, "log", "x"},
                  {:put_batch, [{"large", payload, 0}, {"small", "v", 0}]}
                ]},
               single_member?: true
             )

    assert {:ok, ref} = BlobRef.decode(encoded_ref)
    assert {:ok, ^payload} = BlobStore.get(root, 0, ref)
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
             {:set, "large", :binary.copy("S", 1024), 0, %{}}
           )

    assert BlobCommand.side_channel_candidate?(ctx, {:getset, "large", :binary.copy("T", 1024)})
    assert BlobCommand.side_channel_candidate?(ctx, {:append, "large", :binary.copy("A", 1024)})

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
