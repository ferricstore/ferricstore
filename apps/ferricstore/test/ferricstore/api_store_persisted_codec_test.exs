defmodule FerricStore.API.StorePersistedCodecTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias FerricStore.API.Store
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  test "TopK and TDigest stores reject non-canonical persisted terms" do
    ctx = Store.default_ctx()

    cases = [
      {Store.build_topk_store(unique_key("topk")),
       {:topk_meta, %{path: :binary.copy("topk-path", 1_000)}}},
      {Store.build_tdigest_store(ctx),
       {:tdigest, [],
        %{
          compression: 100,
          count: 0,
          min: nil,
          max: nil,
          buffer: [],
          buffer_size: 0,
          total_compressions: 0,
          padding: :binary.copy("digest", 1_000)
        }}}
    ]

    Enum.each(cases, fn {store, term} ->
      key = unique_key("persisted")
      on_exit(fn -> Router.delete(ctx, key) end)

      for raw <- [:erlang.term_to_binary(term, compressed: 9), TermCodec.encode(term) <> <<0>>] do
        assert :ok = Router.put(ctx, key, raw, 0)
        assert store.get.(key) == raw
      end
    end)
  end

  test "TopK store does not reinterpret an unrelated external term as metadata" do
    ctx = Store.default_ctx()
    key = unique_key("topk-user-value")
    raw = TermCodec.encode({:unrelated_user_tuple, %{value: 1}})
    store = Store.build_topk_store(key)

    on_exit(fn -> Router.delete(ctx, key) end)

    assert :ok = Router.put(ctx, key, raw, 0)
    assert store.get.(key) == raw
  end

  defp unique_key(prefix), do: "api-store-codec:#{prefix}:#{System.unique_integer([:positive])}"
end
