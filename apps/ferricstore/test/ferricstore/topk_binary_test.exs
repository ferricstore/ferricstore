defmodule Ferricstore.TopKBinaryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  test "file-backed TopK preserves distinct non-UTF8 elements byte for byte" do
    path = tmp_path("binary")
    first = <<255>>
    second = <<254>>

    assert {:ok, :ok} = NIF.topk_file_create_v2(path, 2, 8, 3)
    assert [nil, nil] = NIF.topk_file_add_v2(path, [first, second])
    assert [1, 1] = NIF.topk_file_query_v2(path, [first, second])
    assert MapSet.new(NIF.topk_file_list_v2(path)) == MapSet.new([first, second])
  end

  test "file-backed TopK rejects a heap element length outside the record" do
    path = tmp_path("corrupt-length")
    assert {:ok, :ok} = NIF.topk_file_create_v2(path, 1, 1, 1)

    heap_offset = 64 + 8
    assert {:ok, io} = :file.open(path, [:read, :write, :binary])
    on_exit(fn -> :file.close(io) end)
    assert :ok = :file.pwrite(io, 20, <<1::little-32>>)

    assert :ok =
             :file.pwrite(
               io,
               heap_offset,
               <<1::little-signed-64, 253::little-32, :binary.copy(<<"x">>, 252)::binary>>
             )

    assert {:error, reason} = NIF.topk_file_list_v2(path)
    assert reason =~ "element length"
  end

  test "file-backed TopK rejects overlength inputs before mutating counters or heap" do
    path = tmp_path("overlength")
    oversized = :binary.copy(<<"x">>, 253)
    assert {:ok, :ok} = NIF.topk_file_create_v2(path, 2, 8, 3)

    assert {:error, add_reason} = NIF.topk_file_add_v2(path, ["valid", oversized])
    assert add_reason =~ "element length"
    assert [] = NIF.topk_file_list_v2(path)
    assert [0] = NIF.topk_file_count_v2(path, ["valid"])

    assert {:error, incr_reason} = NIF.topk_file_incrby_v2(path, [{oversized, 10}])
    assert incr_reason =~ "element length"
    assert [] = NIF.topk_file_list_v2(path)
  end

  test "file-backed TopK rejects non-positive increments before mutating" do
    path = tmp_path("non-positive-increment")
    assert {:ok, :ok} = NIF.topk_file_create_v2(path, 2, 8, 3)

    for count <- [0, -1] do
      assert {:error, reason} = NIF.topk_file_incrby_v2(path, [{"invalid", count}])
      assert reason =~ "positive"
      assert [] = NIF.topk_file_list_v2(path)
      assert [0] = NIF.topk_file_count_v2(path, ["invalid"])
    end
  end

  defp tmp_path(label) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_topk_binary_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    Path.join(dir, "#{label}.topk")
  end
end
