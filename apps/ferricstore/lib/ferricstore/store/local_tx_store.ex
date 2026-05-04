defmodule Ferricstore.Store.LocalTxStore do
  @moduledoc """
  Transaction-local store context for MULTI/EXEC.

  During a transaction, commands execute inside a shard's GenServer.call.
  For keys on the local shard, operations go directly to ETS (avoiding
  GenServer.call deadlock). For remote keys, operations delegate to Router.

  This struct replaces the 445-line `build_local_store` closure factory.
  `Store.Ops` dispatches on this struct type.
  """

  @type t :: %__MODULE__{
          instance_ctx: FerricStore.Instance.t(),
          shard_index: non_neg_integer(),
          shard_state: map()
        }

  defstruct [:instance_ctx, :shard_index, :shard_state]

  @doc "Creates a LocalTxStore from shard state."
  def new(state) do
    %__MODULE__{
      instance_ctx: state.instance_ctx,
      shard_index: state.index,
      shard_state: %{
        instance_ctx: state.instance_ctx,
        keydir: state.keydir,
        index: state.index,
        shard_data_path: state.shard_data_path,
        data_dir: state.data_dir,
        promoted_instances: state.promoted_instances,
        zset_score_index: state.zset_score_index,
        zset_score_lookup: state.zset_score_lookup,
        zset_index_ready: state.zset_index_ready
      }
    }
  end
end
