defmodule Ferricstore.EmbeddedTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
  describe "combined operations" do
    test "list push-pop cycle preserves FIFO ordering" do
      FerricStore.rpush("fifo", ["first", "second", "third"])
      assert {:ok, "first"} = FerricStore.lpop("fifo")
      assert {:ok, "second"} = FerricStore.lpop("fifo")
      assert {:ok, "third"} = FerricStore.lpop("fifo")
      assert {:ok, nil} = FerricStore.lpop("fifo")
    end

    test "list push-pop cycle preserves LIFO ordering" do
      FerricStore.rpush("lifo", ["first", "second", "third"])
      assert {:ok, "third"} = FerricStore.rpop("lifo")
      assert {:ok, "second"} = FerricStore.rpop("lifo")
      assert {:ok, "first"} = FerricStore.rpop("lifo")
    end

    test "sorted set maintains score ordering across operations" do
      FerricStore.zadd("zorder", [{10.0, "ten"}, {1.0, "one"}, {5.0, "five"}])
      assert {:ok, ["one", "five", "ten"]} = FerricStore.zrange("zorder", 0, -1)

      # Update a score to reorder
      FerricStore.zadd("zorder", [{100.0, "one"}])
      assert {:ok, ["five", "ten", "one"]} = FerricStore.zrange("zorder", 0, -1)
    end

    test "set operations are idempotent for adds" do
      FerricStore.sadd("idem", ["a"])
      FerricStore.sadd("idem", ["a"])
      assert {:ok, 1} = FerricStore.scard("idem")
    end

    test "different data types can coexist with different keys" do
      FerricStore.set("coexist:str", "hello")
      FerricStore.rpush("coexist:list", ["a"])
      FerricStore.sadd("coexist:set", ["x"])
      FerricStore.zadd("coexist:zset", [{1.0, "m"}])

      assert {:ok, "hello"} = FerricStore.get("coexist:str")
      assert {:ok, ["a"]} = FerricStore.lrange("coexist:list", 0, -1)
      {:ok, members} = FerricStore.smembers("coexist:set")
      assert members == ["x"]
      assert {:ok, ["m"]} = FerricStore.zrange("coexist:zset", 0, -1)
    end
  end
    end
  end
end
