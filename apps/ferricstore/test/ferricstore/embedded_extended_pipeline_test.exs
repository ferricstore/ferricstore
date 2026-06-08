defmodule Ferricstore.EmbeddedExtendedPipelineTest do
  @moduledoc """
  Pipeline/transaction operation tests extracted from EmbeddedExtendedTest.

  Covers: multi/1 (MULTI/EXEC transactions).
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # MULTI / TX
  # ===========================================================================

  describe "multi/1" do
    test "executes set and get in transaction" do
      # MULTI/EXEC returns Redis wire-format values (via Dispatcher)
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.set("{tx}:k1", "v1")
                 |> FerricStore.Tx.set("{tx}:k2", "v2")
                 |> FerricStore.Tx.get("{tx}:k1")
               end)

      assert results == [:ok, :ok, "v1"]
    end

    test "executes incr and incr_by in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.incr("tx:counter")
                 |> FerricStore.Tx.incr("tx:counter")
                 |> FerricStore.Tx.incr_by("tx:counter", 10)
               end)

      assert results == [{:ok, 1}, {:ok, 2}, {:ok, 12}]
    end

    test "executes del in transaction" do
      FerricStore.set("tx:del_target", "value")

      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.del("tx:del_target")
                 |> FerricStore.Tx.get("tx:del_target")
               end)

      assert results == [1, nil]
    end

    test "executes hset in transaction" do
      key = "tx:hash:#{System.unique_integer([:positive])}"

      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.hset(key, %{"a" => "1"})
               end)

      assert results == [1]
      Process.sleep(50)
      # Verify embedded hget reads back what MULTI/EXEC wrote (same storage format)
      assert {:ok, "1"} = FerricStore.hget(key, "a")
    end

    test "executes lpush in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.lpush("tx:list", ["a", "b"])
               end)

      assert results == [2]
    end

    test "executes sadd in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.sadd("tx:set", ["a", "b", "c"])
               end)

      assert results == [3]
    end

    test "executes zadd in transaction" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.zadd("tx:zset", [{1.0, "a"}, {2.0, "b"}])
               end)

      assert results == [2]
    end

    test "executes expire in transaction" do
      FerricStore.set("tx:exp", "value")

      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.expire("tx:exp", 60_000)
               end)

      assert results == [1]
    end

    test "empty transaction returns empty results" do
      assert {:ok, []} = FerricStore.multi(fn tx -> tx end)
    end

    test "commands execute in order" do
      assert {:ok, results} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.set("tx:order", "first")
                 |> FerricStore.Tx.get("tx:order")
                 |> FerricStore.Tx.set("tx:order", "second")
                 |> FerricStore.Tx.get("tx:order")
               end)

      assert results == [:ok, "first", :ok, "second"]
    end
  end
end
