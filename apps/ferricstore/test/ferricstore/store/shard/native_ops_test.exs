defmodule Ferricstore.Store.Shard.NativeOpsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.NativeOps

  test "direct list compound_put does not update ETS when Bitcask append fails" do
    keydir = :ets.new(:"native_ops_test_#{System.unique_integer([:positive])}", [:set, :public])
    compound_key = CompoundKey.list_element("list", 0)

    state = %{
      active_file_path: Path.join(System.tmp_dir!(), "missing/native_ops.log"),
      active_file_id: 0,
      instance_ctx: nil,
      keydir: keydir,
      index: 0,
      shard_data_path: System.tmp_dir!()
    }

    try do
      store = NativeOps.build_list_compound_store_direct("list", state)

      assert {:error, _reason} = store.compound_put.("list", compound_key, "value", 0)
      assert [] == :ets.lookup(keydir, compound_key)
    after
      :ets.delete(keydir)
    end
  end
end
