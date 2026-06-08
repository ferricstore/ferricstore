defmodule Ferricstore.Store.Ops.Delete do
  @moduledoc false

  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Ops.LocalRead
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @spec delete(FerricStore.Instance.t() | LocalTxStore.t() | map(), binary()) :: :ok
  def delete(%FerricStore.Instance{} = ctx, key), do: Router.delete(ctx, key)

  def delete(%LocalTxStore{} = tx, key) do
    if LocalRead.local?(tx, key) do
      ShardETS.ets_delete_key(tx.shard_state, key)
      LocalRead.tx_drop_pending(key)
      LocalRead.tx_mark_deleted(key)
      send(self(), {:tx_pending_delete, key})
      :ok
    else
      Router.delete(tx.instance_ctx, key)
    end
  end

  def delete(store, key) when is_map(store), do: store.delete.(key)
end
