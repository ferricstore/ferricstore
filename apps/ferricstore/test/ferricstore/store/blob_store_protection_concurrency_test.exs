defmodule Ferricstore.Store.BlobStoreProtectionConcurrencyTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Store.BlobStore

  @protected_table :ferricstore_blob_store_protected_refs

  test "concurrent protections cannot lose reference-count increments" do
    unique = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "blob-protection-count-#{unique}")
    relative_path = "segments/#{unique}.blob"
    key = {data_dir, 0, relative_path}
    deadline_ms = System.monotonic_time(:millisecond) + 60_000
    parent = self()

    BlobStore.init_tables()
    :ets.delete(@protected_table, key)
    on_exit(fn -> :ets.delete(@protected_table, key) end)
    BlobStore.__protect_relative_path_for_test__(data_dir, 0, relative_path, deadline_ms)

    tasks =
      for _ <- 1..2 do
        Task.async(fn ->
          Process.put(:ferricstore_blob_protect_counter_hook, fn ^key ->
            send(parent, {:protect_ready, self()})

            receive do
              :continue_protect -> :ok
            end
          end)

          BlobStore.__protect_relative_path_for_test__(data_dir, 0, relative_path, deadline_ms)
        end)
      end

    ready =
      for _ <- 1..2 do
        assert_receive {:protect_ready, pid}, 1_000
        pid
      end

    Enum.each(ready, &send(&1, :continue_protect))
    Enum.each(tasks, &Task.await(&1, 1_000))

    assert [{^key, 3, ^deadline_ms}] = :ets.lookup(@protected_table, key)
  end
end
