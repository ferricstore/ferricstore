defmodule Ferricstore.Commands.ProbTypeTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.ProbType

  describe "register" do
    test "returns write errors from map stores" do
      store = %{
        put: fn "bf", _raw, 0 -> {:error, :disk_full} end
      }

      assert {:error, :disk_full} = ProbType.register(store, "bf", {:bloom_meta, %{}})
    end
  end

  describe "persisted metadata decoding" do
    test "rejects compressed and trailing external terms" do
      metadata =
        {:bloom_meta,
         %{capacity: 100, error_rate: 0.01, padding: :binary.copy("metadata", 1_000)}}

      for raw <- [
            :erlang.term_to_binary(metadata, compressed: 9),
            :erlang.term_to_binary(metadata) <> <<0>>
          ] do
        store = %{
          value_size: fn "bf" -> byte_size(raw) end,
          get: fn "bf" -> raw end,
          compound_get: fn _redis_key, _compound_key -> nil end
        }

        assert {:error, message} = ProbType.check_expected("bf", :bloom, store)
        assert message =~ "WRONGTYPE"
      end
    end

    test "rejects malformed tagged metadata rather than claiming probabilistic ownership" do
      malformed = [
        {:bloom_meta, :not_a_map},
        {:bloom_meta, %{capacity: 100, error_rate: 2.0}},
        {:bloom_meta, %{path: 123}},
        {:cms_meta, %{width: 0, depth: 7}},
        {:cms_meta, %{path: 123}},
        {:cuckoo_meta, %{capacity: 0}},
        {:cuckoo_meta, %{path: 123}},
        {:topk_meta, %{path: "/tmp/topk", k: 10, width: 8, depth: 0}},
        {:topk_meta, %{path: "/tmp/topk", k: 10, width: 8, depth: 7, decay: 0.9}},
        {:topk_meta, %{path: 123}},
        {:topk_path, 123}
      ]

      Enum.each(malformed, fn metadata ->
        store = metadata_store("prob", metadata)

        assert {:error, message} = ProbType.check_expected("prob", expected_type(metadata), store)
        assert message =~ "WRONGTYPE"
      end)
    end

    test "accepts every currently persisted probabilistic metadata schema" do
      valid = [
        {:bloom, {:bloom_meta, %{capacity: 100, error_rate: 0.01}}},
        {:bloom, {:bloom_meta, %{path: "/tmp/bloom"}}},
        {:cms, {:cms_meta, %{width: 32, depth: 7}}},
        {:cms, {:cms_meta, %{path: "/tmp/cms"}}},
        {:cuckoo, {:cuckoo_meta, %{capacity: 1_024}}},
        {:cuckoo, {:cuckoo_meta, %{path: "/tmp/cuckoo"}}},
        {:topk, {:topk_meta, %{path: "/tmp/topk"}}},
        {:topk, {:topk_meta, %{path: "/tmp/topk", k: 10, width: 8, depth: 7}}},
        {:topk, {:topk_path, "/tmp/topk"}}
      ]

      Enum.each(valid, fn {expected, metadata} ->
        assert :ok = ProbType.check_expected("prob", expected, metadata_store("prob", metadata))
      end)
    end
  end

  describe "large cold string classification" do
    test "check_expected returns WRONGTYPE without loading a large cold value" do
      store = large_cold_string_store(self(), "cold_string", 1_000_000)

      assert {:error, msg} = ProbType.check_expected("cold_string", :bloom, store)
      assert msg =~ "WRONGTYPE"
      refute_received {:loaded_cold_value, "cold_string"}
    end

    test "check_create returns WRONGTYPE without loading a large cold value" do
      store = large_cold_string_store(self(), "cold_string", 1_000_000)

      assert {:error, msg} = ProbType.check_create("cold_string", :cms, store)
      assert msg =~ "WRONGTYPE"
      refute_received {:loaded_cold_value, "cold_string"}
    end
  end

  defp large_cold_string_store(test_pid, key, value_size) do
    %{
      value_size: fn ^key -> value_size end,
      get: fn ^key ->
        send(test_pid, {:loaded_cold_value, key})
        :binary.copy("x", value_size)
      end,
      compound_get: fn _redis_key, _compound_key -> nil end,
      exists?: fn ^key -> true end
    }
  end

  defp metadata_store(key, metadata) do
    raw = Ferricstore.TermCodec.encode(metadata)

    %{
      value_size: fn ^key -> byte_size(raw) end,
      get: fn ^key -> raw end,
      compound_get: fn _redis_key, _compound_key -> nil end
    }
  end

  defp expected_type({tag, _metadata}) do
    case tag do
      :bloom_meta -> :bloom
      :cms_meta -> :cms
      :cuckoo_meta -> :cuckoo
      :topk_meta -> :topk
      :topk_path -> :topk
    end
  end
end
