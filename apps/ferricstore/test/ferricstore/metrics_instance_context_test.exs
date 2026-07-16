defmodule Ferricstore.MetricsInstanceContextTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  @default_key {FerricStore.Instance, :default}

  setup do
    original = FerricStore.Instance.get(:default)
    keydir = :ets.new(:metrics_instance_keydir, [:set])

    for index <- 1..1_000 do
      :ets.insert(keydir, {"key-#{index}", "value", 0, 0, 0, 0, 5})
    end

    :persistent_term.put(@default_key, %{original | shard_count: 1, keydir_refs: {keydir}})
    Ferricstore.PrefixMetricsCache.reset()

    on_exit(fn ->
      :persistent_term.put(@default_key, original)
      Ferricstore.PrefixMetricsCache.reset()

      if :ets.info(keydir) != :undefined do
        :ets.delete(keydir)
      end
    end)

    {:ok, keydir: keydir}
  end

  test "scrape uses the immutable instance topology", %{keydir: keydir} do
    text = Ferricstore.Metrics.scrape()
    expected_bytes = :ets.info(keydir, :memory) * :erlang.system_info(:wordsize)

    assert text =~ "ferricstore_keydir_used_bytes #{expected_bytes}\n"
    assert text =~ ~s(ferricstore_bitcask_last_applied_index{shard_index="0"})
    refute text =~ ~s(ferricstore_bitcask_last_applied_index{shard_index="1"})

    assert [%{index: 0, keys: 1_000}] = Ferricstore.Health.check().shards
  end

  test "prefix refresh scans the instance keydir references" do
    assert :ok = Ferricstore.PrefixMetricsCache.refresh_now()

    assert Ferricstore.PrefixMetricsCache.text() =~
             ~s(ferricstore_prefix_key_count{prefix="_root"} 1000)
  end
end
