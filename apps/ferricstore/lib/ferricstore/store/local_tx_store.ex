defmodule Ferricstore.Store.LocalTxStore do
  @moduledoc """
  Transaction-local store context for MULTI/EXEC.

  Prepared transaction routing admits only commands whose keys belong to the
  owning shard. Operations then go directly to the shard's ETS and pending
  write state, avoiding nested GenServer or Raft calls during replicated apply.
  `Store.Ops` dispatches on this struct type and retains defensive nonlocal
  branches for callers outside the prepared transaction contract.
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
        compound_member_index:
          Map.get(state, :compound_member_index) || Map.get(state, :compound_member_index_name),
        zset_score_index: Map.get(state, :zset_score_index),
        zset_score_lookup: Map.get(state, :zset_score_lookup)
      }
    }
  end
end
