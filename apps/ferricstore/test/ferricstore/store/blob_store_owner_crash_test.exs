defmodule Ferricstore.Store.BlobStoreOwnerCrashTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.BlobStore

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "blob-owner-crash-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    Ferricstore.DataDir.ensure_layout!(root, 1)
    on_exit(fn -> File.rm_rf!(root) end)
    assert {:ok, sentinel} = BlobStore.put(root, 0, "sentinel")
    %{root: root, sentinel: sentinel}
  end

  test "a writer taking over an abandoned lock returns a readable append reference", ctx do
    interrupt_append(ctx.root, "unacknowledged", :all)
    assert {:ok, next_ref} = BlobStore.put(ctx.root, 0, "acknowledged")
    assert {:ok, "sentinel"} = BlobStore.get(ctx.root, 0, ctx.sentinel)
    assert {:ok, "acknowledged"} = BlobStore.get(ctx.root, 0, next_ref)
    assert {:ok, %{truncated_bytes: 0}} = BlobStore.recover_shard(ctx.root, 0)
    assert {:ok, "acknowledged"} = BlobStore.get(ctx.root, 0, next_ref)
  end

  test "a partial append is repaired before the next acknowledged append", ctx do
    interrupt_append(ctx.root, String.duplicate("x", 1_000), 64)
    assert {:ok, next_ref} = BlobStore.put(ctx.root, 0, "acknowledged")
    assert {:ok, "acknowledged"} = BlobStore.get(ctx.root, 0, next_ref)
    assert {:ok, %{truncated_bytes: 0}} = BlobStore.recover_shard(ctx.root, 0)
    assert {:ok, "sentinel"} = BlobStore.get(ctx.root, 0, ctx.sentinel)
    assert {:ok, "acknowledged"} = BlobStore.get(ctx.root, 0, next_ref)
  end

  test "a large partial append is repaired before the next acknowledged append", ctx do
    interrupt_append(ctx.root, :binary.copy("x", 2_200_000), 1_100_000)
    assert {:ok, next_ref} = BlobStore.put(ctx.root, 0, "after-large-crash")
    assert {:ok, "after-large-crash"} = BlobStore.get(ctx.root, 0, next_ref)
    assert {:ok, %{truncated_bytes: 0}} = BlobStore.recover_shard(ctx.root, 0)
    assert {:ok, "sentinel"} = BlobStore.get(ctx.root, 0, ctx.sentinel)
    assert {:ok, "after-large-crash"} = BlobStore.get(ctx.root, 0, next_ref)
  end

  test "competing writers safely take over an abandoned lock", ctx do
    interrupt_append(ctx.root, "unacknowledged", :all)

    refs =
      1..8
      |> Task.async_stream(
        fn index ->
          value = "concurrent-#{index}"
          {:ok, ref} = BlobStore.put(ctx.root, 0, value)
          {ref, value}
        end,
        max_concurrency: 8,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert refs |> Enum.map(fn {ref, _} -> ref.offset end) |> Enum.uniq() |> length() == 8
    assert {:ok, %{truncated_bytes: 0}} = BlobStore.recover_shard(ctx.root, 0)
    for {ref, value} <- refs, do: assert({:ok, ^value} = BlobStore.get(ctx.root, 0, ref))
    assert {:ok, "sentinel"} = BlobStore.get(ctx.root, 0, ctx.sentinel)
  end

  test "an exceptional append invalidates cached offsets before releasing the lock", ctx do
    Process.put(:ferricstore_blob_store_write_hook, fn io, data ->
      :ok = :file.write(io, data)
      :ok = :file.sync(io)
      exit(:interrupted_blob_append)
    end)

    try do
      assert catch_exit(BlobStore.put(ctx.root, 0, "unacknowledged")) == :interrupted_blob_append
    after
      Process.delete(:ferricstore_blob_store_write_hook)
    end

    assert {:ok, ref} = BlobStore.put(ctx.root, 0, "acknowledged")
    assert {:ok, "acknowledged"} = BlobStore.get(ctx.root, 0, ref)
    assert {:ok, "sentinel"} = BlobStore.get(ctx.root, 0, ctx.sentinel)
  end

  test "healthy appends keep the cached path without recovery scans", ctx do
    Process.put(:ferricstore_blob_store_open_recovery_hook, fn _path, _modes ->
      flunk("healthy append must not rescan a recovered segment")
    end)

    try do
      for index <- 1..10 do
        value = "healthy-#{index}"
        assert {:ok, ref} = BlobStore.put(ctx.root, 0, value)
        assert {:ok, ^value} = BlobStore.get(ctx.root, 0, ref)
      end
    after
      Process.delete(:ferricstore_blob_store_open_recovery_hook)
    end
  end

  defp interrupt_append(root, payload, count) do
    parent = self()

    {writer, monitor} =
      spawn_monitor(fn ->
        Process.put(:ferricstore_blob_store_write_hook, fn io, data ->
          bytes = IO.iodata_to_binary(data)
          bytes = if count == :all, do: bytes, else: binary_part(bytes, 0, count)
          :ok = :file.write(io, bytes)
          :ok = :file.sync(io)
          send(parent, {:appended, self()})

          receive do
            :continue -> :ok
          end
        end)

        BlobStore.put(root, 0, payload)
      end)

    on_exit(fn -> if Process.alive?(writer), do: Process.exit(writer, :kill) end)
    assert_receive {:appended, ^writer}, 5_000
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^writer, :killed}, 5_000
  end
end
