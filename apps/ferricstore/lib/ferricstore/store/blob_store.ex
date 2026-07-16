defmodule Ferricstore.Store.BlobStore do
  @moduledoc """
  Side-channel blob storage for large values.

  Payload records append into a shard-local segment log under
  `data_dir/blob/shard_N/segments/`, while Bitcask stores the fixed-size
  `BlobRef`.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.BlobStore.TableOwner

  @hash_chunk_bytes 1_048_576
  @tmp_stale_after_seconds 300
  @segment_id 0
  @default_segment_max_bytes 256 * 1024 * 1024
  @default_segment_gc_grace_ms 600_000
  @default_protection_ttl_ms 300_000
  @segment_header_magic <<0, ?F, ?S, ?B, ?L, ?O, ?G, 1>>
  @segment_header_bytes 48
  @segment_next_id_filename "next_segment_id"
  @max_segment_next_id_bytes 32
  @recovery_table :ferricstore_blob_store_recovery
  @segment_table :ferricstore_blob_store_segments
  @lock_table :ferricstore_blob_store_locks
  @dir_table :ferricstore_blob_store_dirs
  @protected_table :ferricstore_blob_store_protected_refs
  @hardened_table :ferricstore_blob_store_hardened_protections
  @held_locks_key :ferricstore_blob_store_held_locks
  @lock_retry_ms 1

  @type reason :: term()
  @type protection_token ::
          nil
          | {:blob_store_protection, binary(), non_neg_integer(), [binary()]}
          | [protection_token()]

  @doc false
  @spec init_tables() :: :ok | {:error, :table_owner_unavailable}
  def init_tables do
    TableOwner.ensure_tables()
  end

  use Ferricstore.Store.BlobStore.Write
  use Ferricstore.Store.BlobStore.Protection
  use Ferricstore.Store.BlobStore.Read
  use Ferricstore.Store.BlobStore.GC
  use Ferricstore.Store.BlobStore.IO
end
