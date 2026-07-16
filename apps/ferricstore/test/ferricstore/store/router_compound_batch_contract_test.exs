defmodule Ferricstore.Store.RouterCompoundBatchContractTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Router

  defmodule TruncatedShard do
    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)

    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({request, _redis_key, _keys}, _from, state)
        when request in [:compound_batch_get, :compound_batch_get_meta] do
      {:reply, ["only-one"], state}
    end
  end

  test "compound batch reads fail closed on a truncated shard reply" do
    {ctx, keydir, shard} = truncated_shard_ctx()
    redis_key = "batch-contract"
    keys = [CompoundKey.hash_field(redis_key, "a"), CompoundKey.hash_field(redis_key, "b")]
    install_promotion_marker(keydir, redis_key)
    failure = ReadResult.failure(:invalid_shard_batch_reply)

    try do
      assert [^failure, ^failure] = Router.compound_batch_get(ctx, redis_key, keys)
      assert [^failure, ^failure] = Router.compound_batch_get_meta(ctx, redis_key, keys)
    after
      GenServer.stop(shard)
      :ets.delete(keydir)
    end
  end

  test "nonlocal transaction compound batches fail closed on a truncated shard reply" do
    {tx, shard} = nonlocal_truncated_tx()
    keys = ["H:nonlocal\0a", "H:nonlocal\0b"]
    failure = ReadResult.failure(:invalid_shard_batch_reply)

    try do
      assert [^failure, ^failure] = Ops.compound_batch_get(tx, "nonlocal", keys)
      assert [^failure, ^failure] = Ops.compound_batch_get_meta(tx, "nonlocal", keys)
    after
      GenServer.stop(shard)
    end
  end

  defp truncated_shard_ctx do
    unique = System.unique_integer([:positive, :monotonic])
    shard_name = :"router_compound_batch_contract_shard_#{unique}"
    keydir = :ets.new(:router_compound_batch_contract_keydir, [:set, :public])
    {:ok, shard} = TruncatedShard.start_link(shard_name)

    ctx = %FerricStore.Instance{
      name: :"router_compound_batch_contract_#{unique}",
      data_dir: System.tmp_dir!(),
      data_dir_expanded: System.tmp_dir!(),
      shard_count: 1,
      slot_map: Tuple.duplicate(0, 1_024),
      shard_names: {shard_name},
      keydir_refs: {keydir},
      stats_counter: :counters.new(16, []),
      write_version: :counters.new(1, []),
      hot_cache_max_value_size: 1_024,
      read_sample_rate: 0
    }

    {ctx, keydir, shard}
  end

  defp install_promotion_marker(keydir, redis_key) do
    marker = CompoundKey.promotion_marker_key(redis_key)
    true = :ets.insert(keydir, {marker, "promoted", 0, 0, 0, 0, 8})
  end

  defp nonlocal_truncated_tx do
    unique = System.unique_integer([:positive, :monotonic])
    shard_name = :"router_compound_nonlocal_contract_shard_#{unique}"
    {:ok, shard} = TruncatedShard.start_link(shard_name)

    ctx = %FerricStore.Instance{
      name: :"router_compound_nonlocal_contract_#{unique}",
      data_dir: System.tmp_dir!(),
      data_dir_expanded: System.tmp_dir!(),
      shard_count: 2,
      slot_map: Tuple.duplicate(1, 1_024),
      shard_names: {:unused_nonlocal_shard, shard_name},
      keydir_refs: {nil, nil},
      stats_counter: :counters.new(16, []),
      write_version: :counters.new(2, []),
      hot_cache_max_value_size: 1_024,
      read_sample_rate: 0
    }

    {%LocalTxStore{instance_ctx: ctx, shard_index: 0, shard_state: %{}}, shard}
  end
end
