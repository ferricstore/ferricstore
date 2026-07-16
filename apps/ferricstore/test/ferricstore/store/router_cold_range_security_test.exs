defmodule Ferricstore.Store.RouterColdRangeSecurityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Router

  @tag :tmp_dir
  test "cold range reads reject symlink paths", %{tmp_dir: tmp_dir} do
    target = Path.join(tmp_dir, "outside.log")
    link = Path.join(tmp_dir, "00001.log")
    File.write!(target, "secret")
    File.ln_s!(target, link)

    assert :error = Router.__pread_file_range_for_test__(link, 0, 6)
  end

  @tag :tmp_dir
  test "cold range reads reject a regular file swapped before open", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "00001.log")
    target = Path.join(tmp_dir, "outside.log")
    File.write!(path, "public")
    File.write!(target, "secret")

    Process.put(:ferricstore_router_cold_range_open_hook, fn ->
      File.rm!(path)
      File.ln_s!(target, path)
    end)

    try do
      assert :error = Router.__pread_file_range_for_test__(path, 0, 6)
    after
      Process.delete(:ferricstore_router_cold_range_open_hook)
    end
  end
end
