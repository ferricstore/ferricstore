defmodule Ferricstore.Commands.ProbTypeTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.ProbType
  alias Ferricstore.Commands.Strings
  alias Ferricstore.Store.CompoundKey

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
        {:cms_meta, %{width: 32, depth: 7, create_token: "invalid"}},
        {:cms_meta, %{width: 32, depth: 7, lifecycle_id: {10, <<0::120>>}}},
        {:cms_meta, %{width: 32, depth: 7, lifecycle_id: {"10", <<0::128>>}}},
        {:cms_meta, %{path: 123}},
        {:cuckoo_meta, %{capacity: 0}},
        {:cuckoo_meta, %{path: 123}},
        {:topk_meta, %{path: "/tmp/topk", k: 10, width: 8, depth: 0}},
        {:topk_meta, %{path: "/tmp/topk", k: 10, width: 8, depth: 7, decay: 0.9}},
        {:topk_meta, %{path: 123}},
        {:topk_path, 123}
      ]

      Enum.each(malformed, fn metadata ->
        assert ProbType.metadata_type(metadata) == :other
        store = metadata_store("prob", metadata)

        assert {:error, message} = ProbType.check_expected("prob", expected_type(metadata), store)
        assert message =~ "WRONGTYPE"
      end)
    end

    test "accepts Raft create tokens in probabilistic metadata" do
      valid = [
        {:bloom, {:bloom_meta, %{capacity: 100, error_rate: 0.01, create_token: 101}}},
        {:cms, {:cms_meta, %{width: 32, depth: 7, create_token: 102}}},
        {:cuckoo, {:cuckoo_meta, %{capacity: 1_024, create_token: 103}}},
        {:topk, {:topk_meta, %{path: "/tmp/topk", k: 10, width: 8, depth: 7, create_token: 104}}}
      ]

      Enum.each(valid, fn {expected, metadata} ->
        assert ProbType.metadata_type(metadata) == expected

        assert :ok =
                 ProbType.check_expected(
                   "prob",
                   expected,
                   typed_metadata_store("prob", expected, metadata)
                 )
      end)
    end

    test "accepts exact replicated lifecycle identifiers in probabilistic metadata" do
      lifecycle_id = {105, <<1::128>>}

      valid = [
        {:bloom,
         {:bloom_meta,
          %{capacity: 100, error_rate: 0.01, create_token: 105, lifecycle_id: lifecycle_id}}},
        {:cms,
         {:cms_meta, %{width: 32, depth: 7, create_token: 105, lifecycle_id: lifecycle_id}}},
        {:cuckoo,
         {:cuckoo_meta, %{capacity: 1_024, create_token: 105, lifecycle_id: lifecycle_id}}},
        {:topk,
         {:topk_meta,
          %{
            path: "/tmp/topk",
            k: 10,
            width: 8,
            depth: 7,
            create_token: 105,
            lifecycle_id: lifecycle_id
          }}}
      ]

      Enum.each(valid, fn {expected, metadata} ->
        assert ProbType.metadata_type(metadata) == expected
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
        assert :ok =
                 ProbType.check_expected(
                   "prob",
                   expected,
                   typed_metadata_store("prob", expected, metadata)
                 )
      end)
    end

    @tag :prob_type_catalog
    test "valid ETF bytes without a replicated type marker remain a string" do
      metadata = {:bloom_meta, %{capacity: 100, error_rate: 0.01}}

      assert {:error, message} =
               ProbType.check_expected("prob", :bloom, metadata_store("prob", metadata))

      assert message =~ "WRONGTYPE"
    end

    @tag :prob_type_catalog
    test "string reads reject probabilistic metadata even when raw bytes are present" do
      metadata = {:cms_meta, %{width: 32, depth: 4}}
      store = typed_metadata_store("prob", :cms, metadata)

      assert {:error, message} = Strings.handle("GET", ["prob"], store)
      assert message =~ "WRONGTYPE"
      assert {:error, _message} = Strings.handle("GETEX", ["prob"], store)
      assert {:error, _message} = Strings.handle("STRLEN", ["prob"], store)
      assert {:error, _message} = Strings.handle("GETRANGE", ["prob", "0", "3"], store)
      assert [nil] = Strings.handle("MGET", ["prob"], store)
      assert {:simple, "cms"} = Strings.handle("TYPE", ["prob"], store)
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

  defp typed_metadata_store(key, type, metadata) do
    raw = Ferricstore.TermCodec.encode(metadata)
    type_key = CompoundKey.type_key(key)
    type_value = CompoundKey.encode_type(type)

    %{
      value_size: fn ^key -> byte_size(raw) end,
      get: fn ^key -> raw end,
      getrange: fn ^key, start_idx, end_idx ->
        length = end_idx - start_idx + 1
        binary_part(raw, start_idx, length)
      end,
      exists?: fn ^key -> true end,
      compound_get: fn
        ^key, ^type_key -> type_value
        _redis_key, _compound_key -> nil
      end
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
