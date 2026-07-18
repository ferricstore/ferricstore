defmodule Ferricstore.Commands.ProbabilisticFileConsistencyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.TermCodec
  alias Ferricstore.Test.{MockStore, ProbMockStore}

  test "CF.DEL does not skip replication based on leader-local file presence" do
    parent = self()

    prob_dir =
      Path.join(System.tmp_dir!(), "missing-cuckoo-#{System.unique_integer([:positive])}")

    store = %{
      get: fn "filter" -> TermCodec.encode({:cuckoo_meta, %{capacity: 100}}) end,
      prob_dir: fn -> prob_dir end,
      prob_write: fn command ->
        send(parent, {:prob_write, command})
        {:ok, 1}
      end
    }

    assert 1 = Cuckoo.handle("CF.DEL", ["filter", "member"], store)
    assert_received {:prob_write, {:cuckoo_del, "filter", "member"}}
  end

  test "Bloom reads surface a missing file when replicated metadata still owns the key" do
    prob_dir = Path.join(System.tmp_dir!(), "missing-bloom-#{System.unique_integer([:positive])}")

    store = %{
      get: fn "filter" ->
        TermCodec.encode({:bloom_meta, %{capacity: 100, error_rate: 0.01}})
      end,
      prob_dir: fn -> prob_dir end,
      prob_write: fn _command -> flunk("read must not write") end
    }

    assert {:error, message} = Bloom.handle("BF.EXISTS", ["filter", "member"], store)
    assert message =~ "file is missing"
  end

  test "Cuckoo reads do not expose a stale filter file through a string key" do
    store = ProbMockStore.make_cuckoo()
    assert :ok = Cuckoo.handle("CF.RESERVE", ["filter", "100"], store)
    assert 1 = Cuckoo.handle("CF.ADD", ["filter", "member"], store)

    string_store = Map.put(store, :get, fn "filter" -> "plain string value" end)

    assert {:error, message} = Cuckoo.handle("CF.EXISTS", ["filter", "member"], string_store)
    assert message =~ "WRONGTYPE"
  end

  test "CMS reads do not expose a stale sketch file through a string key" do
    store = ProbMockStore.make_cms()
    assert :ok = CMS.handle("CMS.INITBYDIM", ["sketch", "100", "7"], store)
    assert [1] = CMS.handle("CMS.INCRBY", ["sketch", "member", "1"], store)

    string_store = Map.put(store, :get, fn "sketch" -> "plain string value" end)

    assert {:error, message} = CMS.handle("CMS.QUERY", ["sketch", "member"], string_store)
    assert message =~ "WRONGTYPE"
  end

  test "TopK reads do not expose a stale tracker file through a string key" do
    store = MockStore.make()
    assert :ok = TopK.handle("TOPK.RESERVE", ["tracker", "5"], store)
    assert [nil] = TopK.handle("TOPK.ADD", ["tracker", "member"], store)

    type_key = Ferricstore.Store.CompoundKey.type_key("tracker")
    compound_get = store.compound_get

    string_store =
      store
      |> Map.put(:get, fn "tracker" -> "plain string value" end)
      |> Map.put(:compound_get, fn
        "tracker", ^type_key -> nil
        redis_key, compound_key -> compound_get.(redis_key, compound_key)
      end)

    assert {:error, message} = TopK.handle("TOPK.QUERY", ["tracker", "member"], string_store)
    assert message =~ "WRONGTYPE"
  end
end
