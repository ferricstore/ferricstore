defmodule Ferricstore.Raft.BlobCommand.PayloadWriter do
  @moduledoc false

  alias Ferricstore.Store.BlobStore

  @protection_key :ferricstore_blob_command_protection

  def put_blob_payload(data_dir, shard_index, payload) do
    if collect_protection?(data_dir, shard_index) do
      with {:ok, ref, token} <- BlobStore.put_protected(data_dir, shard_index, payload) do
        collect_protection(token)
        {:ok, ref}
      end
    else
      BlobStore.put(data_dir, shard_index, payload)
    end
  end

  def put_blob_payloads(data_dir, shard_index, payloads) do
    if collect_protection?(data_dir, shard_index) do
      with {:ok, refs, token} <- BlobStore.put_many_protected(data_dir, shard_index, payloads) do
        collect_protection(token)
        {:ok, refs}
      end
    else
      BlobStore.put_many(data_dir, shard_index, payloads)
    end
  end

  defp collect_protection?(data_dir, shard_index) do
    match?({^data_dir, ^shard_index, _tokens}, Process.get(@protection_key))
  end

  defp collect_protection(nil), do: :ok

  defp collect_protection(token) do
    case Process.get(@protection_key) do
      {data_dir, shard_index, tokens} ->
        Process.put(@protection_key, {data_dir, shard_index, [token | tokens]})
        :ok

      _other ->
        BlobStore.unprotect(token)
    end
  end
end
