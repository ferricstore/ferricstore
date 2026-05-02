defmodule Ferricstore.Commands.ProbabilisticHotReadTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}

  setup do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_prob_hot_read_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, dir: dir}
  end

  test "BF.EXISTS reads an existing file without fetching key metadata", %{dir: dir} do
    key = "bf_hot"
    path = prob_path(dir, key, "bloom")

    assert_create_ok(NIF.bloom_file_create(path, 1024, 4))
    assert {:ok, _} = NIF.bloom_file_add(path, "seen")

    assert 1 == Bloom.handle("BF.EXISTS", [key, "seen"], hot_read_store(dir, self()))
    refute_received {:metadata_get, ^key}
  end

  test "CF.EXISTS reads an existing file without fetching key metadata", %{dir: dir} do
    key = "cf_hot"
    path = prob_path(dir, key, "cuckoo")

    assert_create_ok(NIF.cuckoo_file_create(path, 1024, 4))
    assert {:ok, _} = NIF.cuckoo_file_add(path, "seen")

    assert 1 == Cuckoo.handle("CF.EXISTS", [key, "seen"], hot_read_store(dir, self()))
    refute_received {:metadata_get, ^key}
  end

  test "CMS.QUERY reads an existing file without fetching key metadata", %{dir: dir} do
    key = "cms_hot"
    path = prob_path(dir, key, "cms")

    assert_create_ok(NIF.cms_file_create(path, 32, 4))
    assert {:ok, _} = NIF.cms_file_incrby(path, [{"seen", 3}])

    assert [3] == CMS.handle("CMS.QUERY", [key, "seen"], hot_read_store(dir, self()))
    refute_received {:metadata_get, ^key}
  end

  test "TOPK.QUERY reads an existing file without fetching key metadata", %{dir: dir} do
    key = "topk_hot"
    path = prob_path(dir, key, "topk")

    assert_create_ok(NIF.topk_file_create_v2(path, 5, 8, 4, 0.9))
    assert [nil] = NIF.topk_file_add_v2(path, ["seen"])

    assert [1] == TopK.handle("TOPK.QUERY", [key, "seen"], hot_read_store(dir, self()))
    refute_received {:metadata_get, ^key}
  end

  test "missing file still checks metadata so strings return WRONGTYPE", %{dir: dir} do
    store =
      hot_read_store(dir, self())
      |> Map.put(:get, fn _key -> "plain string" end)

    assert {:error, "WRONGTYPE" <> _} = Bloom.handle("BF.EXISTS", ["plain", "x"], store)
  end

  defp hot_read_store(dir, parent) do
    %{
      prob_dir: fn -> dir end,
      get: fn key ->
        send(parent, {:metadata_get, key})
        nil
      end
    }
  end

  defp assert_create_ok(:ok), do: :ok
  defp assert_create_ok({:ok, :ok}), do: :ok

  defp prob_path(dir, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    Path.join(dir, "#{safe}.#{ext}")
  end
end
