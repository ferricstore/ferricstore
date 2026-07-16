defmodule Ferricstore.Flow.DurableIndexMarkerSecurityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{HistoryProjectedIndex, LMDBReplaySafeIndex}

  @modules [HistoryProjectedIndex, LMDBReplaySafeIndex]

  for module <- @modules do
    @module module

    test "#{inspect(module)} does not follow a marker symlink" do
      dir = tmp_dir("symlink")
      victim = Path.join(dir, "victim")
      marker = @module.path(dir)

      File.write!(victim, "99\n")
      File.ln_s!(victim, marker)

      assert @module.read(dir) == 0
      assert {:error, {:symlink, _reason}} = @module.read_result(dir)

      assert :ok = @module.persist(dir, 7)
      assert File.read!(victim) == "99\n"
      assert File.read!(marker) == "7\n"
    end

    test "#{inspect(module)} rejects an oversized marker without parsing it" do
      dir = tmp_dir("oversized")
      File.write!(@module.path(dir), String.duplicate("9", 4_096))

      assert @module.read(dir) == 0
      assert {:error, {:too_large, _reason}} = @module.read_result(dir)
    end

    test "#{inspect(module)} accepts only its canonical beta marker encoding" do
      dir = tmp_dir("encoding")
      marker = @module.path(dir)

      for invalid <- [
            "42",
            "42\r\n",
            "42\n\n",
            " 42\n",
            "42 \n",
            "\t42\n",
            "+42\n",
            "042\n",
            "-0\n",
            "\n"
          ] do
        File.write!(marker, invalid)

        assert @module.read(dir) == 0
        assert {:error, _reason} = @module.read_result(dir)
      end

      File.write!(marker, "42\n")
      assert @module.read(dir) == 42
      assert {:ok, 42} = @module.read_result(dir)
    end

    test "#{inspect(module)} rejects indices above unsigned 64-bit range" do
      dir = tmp_dir("u64")
      above_u64 = 18_446_744_073_709_551_616
      File.write!(@module.path(dir), "#{above_u64}\n")

      assert @module.read(dir) == 0
      assert {:error, _reason} = @module.read_result(dir)
      assert {:error, :invalid_durable_index} = @module.persist(dir, above_u64)
    end
  end

  defp tmp_dir(label) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_index_marker_#{label}_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    dir
  end
end
