defmodule Ferricstore.Commands.TDigestAtomicLevelTest do
  @moduledoc """
  TDIGEST writes read the digest, change it and write it back under a
  node-local key latch, so clients on two nodes lost each other's values.
  From cluster feature level 6 each runs as one replicated entry (and MULTI
  takes them); below it they run on their own path, as older releases neither
  apply that entry nor take TDIGEST in a transaction.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Transaction.ReplicaCompat

  defp cmd(ctx, name, args), do: Dispatcher.dispatch(name, args, ctx)
  defp ukey, do: "{tdigest-atomic:#{System.unique_integer([:positive])}}"

  defp weight(ctx, key) do
    fields = cmd(ctx, "TDIGEST.INFO", [key]) |> Enum.chunk_every(2) |> Map.new(&List.to_tuple/1)
    String.to_float(fields["Merged weight"]) + String.to_float(fields["Unmerged weight"])
  end

  describe "below feature level 6" do
    test "TDIGEST writes run on their own path and MULTI refuses them" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, %{active: active}} = FerricStore.cluster_feature_level()
      assert active < 6
      key = ukey()

      assert :unsupported ==
               Ferricstore.Store.Ops.compound_command(ctx, key, {:tdigest_create, key, nil}, 6)

      assert :ok == cmd(ctx, "TDIGEST.CREATE", [key])
      assert :ok == cmd(ctx, "TDIGEST.ADD", [key, "1.5", "2"])
      assert :ok == FerricStore.tdigest_add(key, [3])
      assert 3.0 == weight(ctx, key)

      assert ReplicaCompat.queue_error("TDIGEST.ADD", {:tdigest_add, key, [1.0]}, [key], ctx) =~
               "needs cluster feature level 6"
    end
  end

  describe "at feature level 6" do
    setup do
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
      assert {:ok, %{active: 6}} = FerricStore.raise_cluster_feature_level(6)
      %{ctx: FerricStore.Instance.get(:default)}
    end

    # One replicated entry, not a transaction: a transaction entry costs
    # several more fsyncs.
    test "TDIGEST writes run as one entry, not a transaction", %{ctx: ctx} do
      parent = self()

      Process.put(:ferricstore_tx_backend_write_hook, fn _shard, _command ->
        send(parent, :transaction_used)
        {:error, :noproc}
      end)

      on_exit(fn -> Process.delete(:ferricstore_tx_backend_write_hook) end)
      key = ukey()
      dest = key <> "dest"

      assert :ok == cmd(ctx, "TDIGEST.CREATE", [key])
      assert :ok == cmd(ctx, "TDIGEST.ADD", [key, "1.5", "2"])
      assert :ok == cmd(ctx, "TDIGEST.MERGE", [dest, "1", key])
      assert 2.0 == weight(ctx, dest)
      assert :ok == cmd(ctx, "TDIGEST.RESET", [key])
      assert 0.0 == weight(ctx, key)
      assert {:error, "ERR TDIGEST: key already exists"} == cmd(ctx, "TDIGEST.CREATE", [key])
      assert :ok == FerricStore.tdigest_add(key, [4])
      assert 1.0 == weight(ctx, key)
      refute_received :transaction_used

      assert :ok ==
               Ferricstore.Store.Ops.compound_command(ctx, key, {:tdigest_add, key, [5.0]}, 6)

      assert 2.0 == weight(ctx, key)

      hash = ukey()
      assert 1 == cmd(ctx, "HSET", [hash, "f", "v"])
      assert {:error, "WRONGTYPE" <> _} = cmd(ctx, "TDIGEST.ADD", [hash, "1"])

      Process.delete(:ferricstore_tx_backend_write_hook)

      assert nil ==
               ReplicaCompat.queue_error("TDIGEST.ADD", {:tdigest_add, key, [1.0]}, [key], ctx)
    end

    test "the embedded API's writes keep every value", %{ctx: ctx} do
      key = ukey()
      assert :ok == FerricStore.tdigest_create(key)
      assert :ok == FerricStore.tdigest_add(key, [1, 2.5, -0.0, 0.1])
      assert {:ok, "2.5"} == FerricStore.tdigest_max(key)
      assert 4.0 == weight(ctx, key)
      assert :ok == FerricStore.tdigest_reset(key)
      assert 0.0 == weight(ctx, key)
    end
  end
end
