defmodule Ferricstore.Test.FlowValues do
  @moduledoc """
  Reads of stored Flow values in tests.

  Once a node's history projector has projected a value, it deletes the
  value's keydir row and the shard's LMDB mirror locates the value instead.
  That happens on the projector's schedule, not the test's, so a test that
  reads a value ref straight from the keydir (`Router.get/2`) passes or fails
  with the projector's timing. `stored_value/2` reads where Flow reads do.
  """

  import ExUnit.Assertions

  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.ValueStore
  alias Ferricstore.Store.Router

  @doc "A value ref's stored bytes, from the keydir or the LMDB mirror, or nil."
  @spec stored_value(FerricStore.Instance.t(), binary()) :: binary() | nil
  def stored_value(ctx, ref) do
    [value] = ValueStore.raw_mget(ctx, [ref])
    value
  end

  @doc "Flushes the history projector until it has moved `ref` out of the keydir."
  @spec projected!(FerricStore.Instance.t(), binary(), non_neg_integer()) :: :ok
  def projected!(ctx, ref, attempts \\ 500)

  def projected!(_ctx, ref, 0), do: flunk("the history projector kept #{ref} in the keydir")

  def projected!(ctx, ref, attempts) do
    shard_index = Router.shard_for(ctx, ref)
    assert :ok = HistoryProjector.flush(ctx, shard_index)

    if :ets.lookup(elem(ctx.keydir_refs, shard_index), ref) == [] do
      :ok
    else
      Process.sleep(10)
      projected!(ctx, ref, attempts - 1)
    end
  end
end
