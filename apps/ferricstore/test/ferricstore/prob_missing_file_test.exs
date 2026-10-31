defmodule Ferricstore.ProbMissingFileTest do
  @moduledoc """
  A probabilistic key whose file this node lost answers errors for that key
  on this node; the shard keeps applying everything else, starts again, and
  DEL heals the key. (A missing file used to stop the shard's apply, and the
  shard then refused to start.)
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, isolated: isolated}
  end

  setup do
    tag = "probloss:{#{System.unique_integer([:positive])}}"

    %{
      d: fn command, args ->
        Dispatcher.dispatch(command, args, FerricStore.Instance.get(:default))
      end,
      k: &(tag <> &1)
    }
  end

  defp lose_file(key, extension) do
    data_dir = Application.get_env(:ferricstore, :data_dir)
    filename = Ferricstore.ProbFile.filename(key, extension)
    [path] = Path.wildcard(Path.join([data_dir, "**", filename]))
    File.rm!(path)
  end

  defp assert_shard_applies(d, k) do
    assert :ok == d.("SET", [k.("other"), "v"])
    assert "v" == d.("GET", [k.("other")])
  end

  test "Bloom writes to a lost file answer an error; DEL heals the key", %{d: d, k: k} do
    :ok = d.("BF.RESERVE", [k.("bf"), "0.01", "100"])
    1 = d.("BF.ADD", [k.("bf"), "a"])
    lose_file(k.("bf"), "bloom")

    for {command, args} <- [
          {"BF.ADD", [k.("bf"), "b"]},
          {"BF.MADD", [k.("bf"), "b", "c"]},
          {"BF.INSERT", [k.("bf"), "ITEMS", "b"]}
        ] do
      assert {:error, "ERR bloom filter file is missing on this node" <> _} = d.(command, args),
             command
    end

    assert_shard_applies(d, k)
    assert {:error, "ERR bloom filter file is missing"} == d.("BF.EXISTS", [k.("bf"), "a"])

    assert 1 == d.("DEL", [k.("bf")])
    assert 1 == d.("BF.ADD", [k.("bf"), "a"])
    assert 1 == d.("BF.EXISTS", [k.("bf"), "a"])
  end

  test "Cuckoo, Count-Min and Top-K writes to a lost file answer an error", %{d: d, k: k} do
    :ok = d.("CF.RESERVE", [k.("cf"), "100"])
    1 = d.("CF.ADD", [k.("cf"), "a"])
    lose_file(k.("cf"), "cuckoo")

    for {command, args} <- [
          {"CF.ADD", [k.("cf"), "b"]},
          {"CF.ADDNX", [k.("cf"), "b"]},
          {"CF.DEL", [k.("cf"), "a"]},
          {"CF.INSERT", [k.("cf"), "ITEMS", "b"]}
        ] do
      assert {:error, "ERR cuckoo filter file is missing on this node" <> _} = d.(command, args),
             command
    end

    :ok = d.("CMS.INITBYDIM", [k.("cms"), "10", "2"])
    :ok = d.("CMS.INITBYDIM", [k.("cms2"), "10", "2"])
    lose_file(k.("cms"), "cms")

    assert {:error, "ERR count-min sketch file is missing on this node" <> _} =
             d.("CMS.INCRBY", [k.("cms"), "a", "1"])

    assert {:error, "ERR count-min sketch file is missing on this node" <> _} =
             d.("CMS.MERGE", [k.("cms"), "1", k.("cms2")])

    :ok = d.("TOPK.RESERVE", [k.("topk"), "3"])
    lose_file(k.("topk"), "topk")

    assert {:error, "ERR top-k file is missing on this node" <> _} =
             d.("TOPK.ADD", [k.("topk"), "a"])

    assert_shard_applies(d, k)
    assert 3 == d.("DEL", [k.("cf"), k.("cms"), k.("topk")])
  end

  test "RENAME and COPY carry the lost file's key; the copies answer errors too", %{
    d: d,
    k: k
  } do
    :ok = d.("BF.RESERVE", [k.("src"), "0.01", "100"])
    :ok = d.("BF.RESERVE", [k.("src2"), "0.01", "100"])
    lose_file(k.("src"), "bloom")
    lose_file(k.("src2"), "bloom")

    assert :ok == d.("RENAME", [k.("src"), k.("dst")])
    assert 0 == d.("EXISTS", [k.("src")])

    assert {:error, "ERR bloom filter file is missing on this node" <> _} =
             d.("BF.ADD", [k.("dst"), "a"])

    assert 1 == d.("COPY", [k.("src2"), k.("dst2")])

    assert {:error, "ERR bloom filter file is missing on this node" <> _} =
             d.("BF.ADD", [k.("dst2"), "a"])

    assert_shard_applies(d, k)
    assert 3 == d.("DEL", [k.("dst"), k.("src2"), k.("dst2")])
    assert 1 == d.("BF.ADD", [k.("dst"), "a"])
  end

  test "FERRICSTORE.DOCTOR CHECK SCOPE PROB_FILES lists the keys that lost their file", %{
    d: d,
    k: k
  } do
    :ok = d.("BF.RESERVE", [k.("bf"), "0.01", "100"])
    :ok = d.("BF.RESERVE", [k.("kept"), "0.01", "100"])
    lose_file(k.("bf"), "bloom")

    report = d.("FERRICSTORE.DOCTOR", ["CHECK", "SCOPE", "PROB_FILES"])
    assert %{"status" => "error", "checks" => [check]} = report
    assert check["metrics"]["total_missing_files"] == 1

    assert [k.("bf")] ==
             Enum.flat_map(check["metrics"]["shards"], &Map.get(&1, "missing_keys", []))

    assert 1 == d.("DEL", [k.("bf")])

    assert %{"status" => "ok"} = d.("FERRICSTORE.DOCTOR", ["CHECK", "SCOPE", "PROB_FILES"])
    assert 1 == d.("DEL", [k.("kept")])
  end

  test "the shard starts with a lost file and the key still answers errors", %{
    d: d,
    k: k,
    isolated: isolated
  } do
    :ok = d.("BF.RESERVE", [k.("bf"), "0.01", "100"])
    lose_file(k.("bf"), "bloom")

    :ok = ShardHelpers.restart_current_data_dir(isolated)

    assert {:error, "ERR bloom filter file is missing on this node" <> _} =
             d.("BF.ADD", [k.("bf"), "a"])

    assert_shard_applies(d, k)
    assert 1 == d.("DEL", [k.("bf")])
    assert 1 == d.("BF.ADD", [k.("bf"), "a"])
  end
end
