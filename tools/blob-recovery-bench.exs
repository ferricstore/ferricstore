# After a test build: elixir -pa '_build/test/lib/*/ebin' tools/blob-recovery-bench.exs
alias Ferricstore.Store.{BlobStore, BlobStore.TableOwner}

{:ok, _} = Application.ensure_all_started(:crypto)
{:ok, owner} = TableOwner.start_link()
root = Path.join(System.tmp_dir!(), "blob-recovery-bench-#{System.pid()}")
Ferricstore.DataDir.ensure_layout!(root, 1)
payload = :binary.copy("healthy-blob-write-", 64)

try do
  {:ok, _} = BlobStore.put(root, 0, payload)

  Process.put(:ferricstore_blob_store_open_recovery_hook, fn _path, _modes ->
    raise "healthy append unexpectedly scanned recovery data"
  end)

  samples =
    for _ <- 1..5 do
      {us, refs} =
        :timer.tc(fn ->
          for _ <- 1..100 do
            {:ok, ref} = BlobStore.put(root, 0, payload)
            ref
          end
        end)

      for ref <- refs do
        {:ok, ^payload} = BlobStore.get(root, 0, ref)
      end

      us / 1_000
    end

  IO.inspect(%{
    operation: "100 healthy synced blob appends",
    median_ms: Enum.at(Enum.sort(samples), 2),
    samples_ms: samples
  })
after
  Process.delete(:ferricstore_blob_store_open_recovery_hook)
  GenServer.stop(owner)
  File.rm_rf!(root)
end
