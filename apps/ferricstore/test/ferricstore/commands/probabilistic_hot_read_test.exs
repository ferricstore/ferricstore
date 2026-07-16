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

  test "BF.EXISTS validates authoritative metadata before reading a sidecar", %{dir: dir} do
    key = "bf_hot"
    path = prob_path(dir, key, "bloom")

    metadata =
      {:bloom_meta, %{path: path, num_bits: 1024, num_hashes: 4, capacity: 100, error_rate: 0.01}}

    assert_create_ok(NIF.bloom_file_create(path, 1024, 4))
    assert {:ok, _} = NIF.bloom_file_add(path, "seen")

    assert 1 ==
             Bloom.handle("BF.EXISTS", [key, "seen"], hot_read_store(dir, self(), metadata))

    assert_received {:metadata_get, ^key}
  end

  test "CF.EXISTS validates authoritative metadata before reading a sidecar", %{dir: dir} do
    key = "cf_hot"
    path = prob_path(dir, key, "cuckoo")
    metadata = {:cuckoo_meta, %{capacity: 1024}}

    assert_create_ok(NIF.cuckoo_file_create(path, 1024, 4))
    assert {:ok, _} = NIF.cuckoo_file_add(path, "seen")

    assert 1 ==
             Cuckoo.handle("CF.EXISTS", [key, "seen"], hot_read_store(dir, self(), metadata))

    assert_received {:metadata_get, ^key}
  end

  test "CMS.QUERY validates authoritative metadata before reading a sidecar", %{dir: dir} do
    key = "cms_hot"
    path = prob_path(dir, key, "cms")
    metadata = {:cms_meta, %{width: 32, depth: 4}}

    assert_create_ok(NIF.cms_file_create(path, 32, 4))
    assert {:ok, _} = NIF.cms_file_incrby(path, [{"seen", 3}])

    assert [3] ==
             CMS.handle("CMS.QUERY", [key, "seen"], hot_read_store(dir, self(), metadata))

    assert_received {:metadata_get, ^key}
  end

  test "TOPK.QUERY validates authoritative metadata before reading a sidecar", %{dir: dir} do
    key = "topk_hot"
    path = prob_path(dir, key, "topk")
    metadata = {:topk_meta, %{path: path, k: 5, width: 8, depth: 4}}

    assert_create_ok(NIF.topk_file_create_v2(path, 5, 8, 4))
    assert [nil] = NIF.topk_file_add_v2(path, ["seen"])

    assert [1] ==
             TopK.handle("TOPK.QUERY", [key, "seen"], hot_read_store(dir, self(), metadata))

    assert_received {:metadata_get, ^key}
  end

  test "missing file still checks metadata so strings return WRONGTYPE", %{dir: dir} do
    store =
      hot_read_store(dir, self(), nil)
      |> Map.put(:get, fn _key -> "plain string" end)

    assert {:error, "WRONGTYPE" <> _} = Bloom.handle("BF.EXISTS", ["plain", "x"], store)
  end

  defp hot_read_store(dir, parent, metadata) do
    %{
      prob_dir: fn -> dir end,
      get: fn key ->
        send(parent, {:metadata_get, key})
        if is_nil(metadata), do: nil, else: Ferricstore.TermCodec.encode(metadata)
      end
    }
  end

  defp assert_create_ok(:ok), do: :ok
  defp assert_create_ok({:ok, :ok}), do: :ok

  defp prob_path(dir, key, ext) do
    Ferricstore.ProbFile.path(dir, key, ext)
  end
end
