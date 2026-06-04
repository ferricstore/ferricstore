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
    shard_index = Map.get(state, :index) || Map.fetch!(state, :shard_index)
    keydir = Map.get(state, :keydir) || Map.fetch!(state, :ets)

    %__MODULE__{
      instance_ctx: Map.fetch!(state, :instance_ctx),
      shard_index: shard_index,
      shard_state: %{
        instance_ctx: Map.fetch!(state, :instance_ctx),
        keydir: keydir,
        index: shard_index,
        shard_data_path: Map.fetch!(state, :shard_data_path),
        data_dir: Map.fetch!(state, :data_dir),
        promoted_instances: Map.get(state, :promoted_instances, %{}),
        zset_score_index: Map.get(state, :zset_score_index),
        zset_score_lookup: Map.get(state, :zset_score_lookup),
        zset_index_ready: Map.get(state, :zset_index_ready)
      }
    }
  end
end
