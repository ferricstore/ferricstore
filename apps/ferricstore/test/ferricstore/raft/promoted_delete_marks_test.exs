defmodule Ferricstore.Raft.PromotedDeleteMarksTest do
  @moduledoc """
  Deleting a promoted collection's members marks them deleted for the reads
  of the transaction that deletes them. Outside a transaction nothing cleared
  the marks: a shard's WARaft storage process kept every member of every
  promoted collection it deleted, and a sorted-set write there went through
  all of them for each member until the set moved to its tree, so a ZADD
  after deleting a 100,000-field hash took seconds and timed out on slow
  runners.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  test "deleting a promoted hash leaves no deleted-member marks in the storage process" do
    ctx = FerricStore.Instance.get(:default)
    key = "{promoted-delete-marks:#{System.unique_integer([:positive])}}"
    shard = Router.shard_for(ctx, key)

    assert 150 ==
             Dispatcher.dispatch("HSET", [key | Enum.flat_map(1..150, &["f#{&1}", "v"])], ctx)

    ShardHelpers.eventually(
      fn -> GenServer.call(Router.shard_name(ctx, shard), {:promoted?, key}) end,
      "expected #{inspect(key)} to finish promotion"
    )

    assert 1 == Dispatcher.dispatch("DEL", [key], ctx)

    storage = :wa_raft_storage.registered_name(:ferricstore_waraft_backend, shard + 1)
    {:dictionary, dictionary} = Process.info(Process.whereis(storage), :dictionary)

    assert MapSet.new() == Keyword.get(dictionary, :tx_deleted_keys, MapSet.new())
  end
end
