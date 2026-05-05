defmodule Ferricstore.Store.TypedValuesTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  describe "INCR stores string representation" do
    test "INCR on nonexistent key creates string in ETS" do
      assert {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), "typed:incr_new", 1)
      value = Router.get(FerricStore.Instance.get(:default), "typed:incr_new")
      assert is_binary(value)
      assert value == "1"
    end

    test "INCR on string parses and stores string" do
      Router.put(FerricStore.Instance.get(:default), "typed:incr_str", "10", 0)
      assert {:ok, 11} = Router.incr(FerricStore.Instance.get(:default), "typed:incr_str", 1)
      value = Router.get(FerricStore.Instance.get(:default), "typed:incr_str")
      assert is_binary(value)
      assert value == "11"
    end

    test "multiple INCRs accumulate correctly" do
      assert {:ok, 1} = Router.incr(FerricStore.Instance.get(:default), "typed:incr_multi", 1)
      assert {:ok, 3} = Router.incr(FerricStore.Instance.get(:default), "typed:incr_multi", 2)
      assert {:ok, 13} = Router.incr(FerricStore.Instance.get(:default), "typed:incr_multi", 10)
      assert Router.get(FerricStore.Instance.get(:default), "typed:incr_multi") == "13"
    end

    test "INCR on non-integer returns error, value unchanged" do
      Router.put(FerricStore.Instance.get(:default), "typed:incr_bad", "hello", 0)
      assert {:error, _} = Router.incr(FerricStore.Instance.get(:default), "typed:incr_bad", 1)
      assert Router.get(FerricStore.Instance.get(:default), "typed:incr_bad") == "hello"
    end

    test "INCR on float returns error" do
      Router.put(FerricStore.Instance.get(:default), "typed:incr_on_float", "3.14", 0)

      assert {:error, _} =
               Router.incr(FerricStore.Instance.get(:default), "typed:incr_on_float", 1)
    end
  end

  describe "INCRBYFLOAT stores string representation" do
    test "INCRBYFLOAT on nonexistent key creates string in ETS" do
      assert {:ok, result} =
               Router.incr_float(FerricStore.Instance.get(:default), "typed:float_new", 3.14)

      assert is_float(result)
      assert_in_delta result, 3.14, 0.001

      value = Router.get(FerricStore.Instance.get(:default), "typed:float_new")
      assert is_binary(value)
      {parsed, _} = Float.parse(value)
      assert_in_delta parsed, 3.14, 0.001
    end

    test "INCRBYFLOAT on integer produces string" do
      Router.incr(FerricStore.Instance.get(:default), "typed:float_from_int", 10)

      assert {:ok, result} =
               Router.incr_float(FerricStore.Instance.get(:default), "typed:float_from_int", 0.5)

      assert is_float(result)
      assert_in_delta result, 10.5, 0.001

      value = Router.get(FerricStore.Instance.get(:default), "typed:float_from_int")
      assert is_binary(value)
    end

    test "INCRBYFLOAT on string parses and stores string" do
      Router.put(FerricStore.Instance.get(:default), "typed:float_str", "10.5", 0)

      assert {:ok, result} =
               Router.incr_float(FerricStore.Instance.get(:default), "typed:float_str", 2.5)

      assert is_float(result)
      assert_in_delta result, 13.0, 0.001
    end

    test "quorum INCRBYFLOAT rejects partial numeric strings" do
      ns = "typed_quorum_float"
      key = "#{ns}:bad_#{System.unique_integer([:positive])}"

      on_exit(fn -> NamespaceConfig.reset_all() end)

      assert :ok = Router.put(FerricStore.Instance.get(:default), key, "10.5abc", 0)

      assert {:error, "ERR value is not a valid float"} =
               Router.incr_float(FerricStore.Instance.get(:default), key, 1.0)

      assert Router.get(FerricStore.Instance.get(:default), key) == "10.5abc"
    end
  end

  describe "SET stores binary" do
    test "SET always stores binary" do
      Router.put(FerricStore.Instance.get(:default), "typed:set_str", "hello", 0)
      value = Router.get(FerricStore.Instance.get(:default), "typed:set_str")
      assert is_binary(value)
      assert value == "hello"
    end

    test "SET overwrites INCR result with string" do
      Router.incr(FerricStore.Instance.get(:default), "typed:set_overwrite", 42)
      assert Router.get(FerricStore.Instance.get(:default), "typed:set_overwrite") == "42"
      Router.put(FerricStore.Instance.get(:default), "typed:set_overwrite", "hello", 0)
      assert Router.get(FerricStore.Instance.get(:default), "typed:set_overwrite") == "hello"
    end
  end

  describe "GET always returns binary" do
    test "GET returns string after INCR" do
      Router.incr(FerricStore.Instance.get(:default), "typed:get_int", 42)
      value = Router.get(FerricStore.Instance.get(:default), "typed:get_int")
      assert is_binary(value)
      assert value == "42"
    end

    test "GET returns string after INCRBYFLOAT" do
      Router.incr_float(FerricStore.Instance.get(:default), "typed:get_float", 3.14)
      value = Router.get(FerricStore.Instance.get(:default), "typed:get_float")
      assert is_binary(value)
      {parsed, _} = Float.parse(value)
      assert_in_delta parsed, 3.14, 0.001
    end

    test "GET returns binary unchanged" do
      Router.put(FerricStore.Instance.get(:default), "typed:get_bin", "hello", 0)
      assert Router.get(FerricStore.Instance.get(:default), "typed:get_bin") == "hello"
    end
  end

  describe "string commands handle values" do
    test "APPEND on INCR result works" do
      Router.incr(FerricStore.Instance.get(:default), "typed:append_int", 42)
      assert {:ok, 3} = Router.append(FerricStore.Instance.get(:default), "typed:append_int", "!")
      value = Router.get(FerricStore.Instance.get(:default), "typed:append_int")
      assert is_binary(value)
      assert value == "42!"
    end

    test "STRLEN on INCR result counts string representation length" do
      Router.incr(FerricStore.Instance.get(:default), "typed:strlen_int", 42)
      store = build_store()
      result = Ferricstore.Commands.Strings.handle("STRLEN", ["typed:strlen_int"], store)
      assert result == 2
    end

    test "GETRANGE on INCR result extracts from string representation" do
      Router.incr(FerricStore.Instance.get(:default), "typed:getrange_int", 12_345)
      store = build_store()

      result =
        Ferricstore.Commands.Strings.handle("GETRANGE", ["typed:getrange_int", "0", "2"], store)

      assert result == "123"
    end
  end

  describe "type transitions" do
    test "String -> INCR -> APPEND (all strings)" do
      Router.put(FerricStore.Instance.get(:default), "typed:transition", "42", 0)
      assert is_binary(Router.get(FerricStore.Instance.get(:default), "typed:transition"))

      assert {:ok, 43} = Router.incr(FerricStore.Instance.get(:default), "typed:transition", 1)
      assert Router.get(FerricStore.Instance.get(:default), "typed:transition") == "43"

      assert {:ok, 3} = Router.append(FerricStore.Instance.get(:default), "typed:transition", "!")
      value = Router.get(FerricStore.Instance.get(:default), "typed:transition")
      assert is_binary(value)
      assert value == "43!"

      assert {:error, _} = Router.incr(FerricStore.Instance.get(:default), "typed:transition", 1)
    end
  end

  describe "disk format is binary" do
    test "Bitcask disk format is binary after flush" do
      Router.incr(FerricStore.Instance.get(:default), "typed:disk_int", 42)
      Router.incr_float(FerricStore.Instance.get(:default), "typed:disk_float", 3.14)

      Ferricstore.Store.BitcaskWriter.flush_all()
      ShardHelpers.flush_all_shards()

      assert Router.get(FerricStore.Instance.get(:default), "typed:disk_int") == "42"
      value = Router.get(FerricStore.Instance.get(:default), "typed:disk_float")
      assert is_binary(value)
      {parsed, _} = Float.parse(value)
      assert_in_delta parsed, 3.14, 0.001
    end
  end

  defp build_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      put: fn key, value, exp ->
        Router.put(FerricStore.Instance.get(:default), key, value, exp)
      end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      incr: fn k, d -> Router.incr(FerricStore.Instance.get(:default), k, d) end,
      incr_float: fn k, d -> Router.incr_float(FerricStore.Instance.get(:default), k, d) end,
      append: fn k, s -> Router.append(FerricStore.Instance.get(:default), k, s) end,
      getset: fn k, v -> Router.getset(FerricStore.Instance.get(:default), k, v) end,
      getdel: fn k -> Router.getdel(FerricStore.Instance.get(:default), k) end,
      getex: fn k, e -> Router.getex(FerricStore.Instance.get(:default), k, e) end,
      setrange: fn k, o, v -> Router.setrange(FerricStore.Instance.get(:default), k, o, v) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end
    }
  end
end
