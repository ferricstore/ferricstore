defmodule Ferricstore.Flow.DurableIndexMarkerTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{HistoryProjectedIndex, LMDBReplaySafeIndex}

  @modules [HistoryProjectedIndex, LMDBReplaySafeIndex]

  for module <- @modules do
    @module module

    test "#{inspect(module)} persists one canonical monotonic watermark" do
      dir = tmp_dir("monotonic")

      assert :ok = @module.persist(dir, 42)
      assert :ok = @module.persist(dir, 7)

      assert {:ok, 42} = @module.read_result(dir)
      assert File.read!(@module.path(dir)) == "42\n"
    end

    test "#{inspect(module)} repairs missing and corrupt markers canonically" do
      dir = tmp_dir("repair")
      marker = @module.path(dir)

      assert {:error, {:not_found, _reason}} = @module.read_result(dir)
      assert :ok = @module.persist(dir, 11)
      assert File.read!(marker) == "11\n"

      File.write!(marker, "99 \n")
      assert {:error, _reason} = @module.read_result(dir)
      assert :ok = @module.persist(dir, 13)
      assert File.read!(marker) == "13\n"
    end

    test "#{inspect(module)} serializes concurrent writers without moving backward" do
      dir = tmp_dir("concurrent")
      values = Enum.to_list(0..64)

      values
      |> Enum.shuffle()
      |> Task.async_stream(
        &@module.persist(dir, &1),
        max_concurrency: 32,
        ordered: false,
        timeout: 5_000
      )
      |> Enum.each(fn result -> assert result == {:ok, :ok} end)

      assert {:ok, 64} = @module.read_result(dir)
    end

    test "#{inspect(module)} preserves a valid marker when its read fails" do
      dir = tmp_dir("unreadable")
      marker = @module.path(dir)

      assert :ok = @module.persist(dir, 99)
      File.chmod!(marker, 0o000)

      on_exit(fn ->
        if File.exists?(marker), do: File.chmod!(marker, 0o600)
      end)

      assert {:error, {:permission_denied, _reason}} = @module.read_result(dir)

      assert {:error, {:marker_read_failed, {:permission_denied, _reason}}} =
               @module.persist(dir, 7)

      File.chmod!(marker, 0o600)
      assert {:ok, 99} = @module.read_result(dir)
    end

    test "#{inspect(module)} cleans temporary files when replacement fails" do
      dir = tmp_dir("tmp-cleanup")
      marker = @module.path(dir)
      File.mkdir_p!(marker)

      assert {:error, _reason} = @module.persist(dir, 5)

      marker_name = Path.basename(marker)

      refute Enum.any?(File.ls!(dir), fn entry ->
               entry != marker_name and
                 String.contains?(entry, marker_name) and
                 String.contains?(entry, "tmp")
             end)
    end
  end

  defp tmp_dir(label) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_durable_index_marker_#{label}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(dir) end)
    dir
  end
end
