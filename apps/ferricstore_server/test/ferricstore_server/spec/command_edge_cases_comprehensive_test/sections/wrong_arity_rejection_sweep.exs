defmodule FerricstoreServer.Spec.CommandEdgeCasesComprehensiveTest.Sections.WrongArityRejectionSweep do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.{Encoder, Parser}
      alias FerricstoreServer.Listener
      alias Ferricstore.Test.ShardHelpers

      describe "Wrong arity rejection sweep" do
        test "every command with too few or too many args returns error, not crash", %{sock: sock} do
          # Each tuple: {cmd, too_few_args, too_many_args}
          wrong_arity_cases = [
            {"GET", [], ["a", "b"]},
            {"SET", ["k"], nil},
            {"DEL", [], nil},
            {"EXISTS", [], nil},
            {"MGET", [], nil},
            {"MSET", [], nil},
            {"INCR", [], ["a", "b"]},
            {"DECR", [], ["a", "b"]},
            {"INCRBY", ["k"], nil},
            {"DECRBY", ["k"], nil},
            {"INCRBYFLOAT", [], nil},
            {"APPEND", ["k"], nil},
            {"STRLEN", [], nil},
            {"GETSET", ["k"], nil},
            {"GETDEL", [], nil},
            {"SETNX", ["k"], nil},
            {"SETEX", ["k", "10"], nil},
            {"PSETEX", ["k", "10"], nil},
            {"GETRANGE", ["k", "0"], nil},
            {"SETRANGE", ["k", "0"], nil},
            {"TTL", [], nil},
            {"PTTL", [], nil},
            {"EXPIRE", ["k"], nil},
            {"PEXPIRE", ["k"], nil},
            {"PERSIST", [], nil},
            {"TYPE", [], nil},
            {"RENAME", ["k"], nil},
            {"RENAMENX", ["k"], nil},
            {"HSET", ["k"], nil},
            {"HGET", ["k"], nil},
            {"HDEL", ["k"], nil},
            {"HGETALL", [], nil},
            {"HLEN", [], nil},
            {"HEXISTS", ["k"], nil},
            {"HKEYS", [], nil},
            {"HVALS", [], nil},
            {"HSETNX", ["k", "f"], nil},
            {"HINCRBY", ["k", "f"], nil},
            {"HINCRBYFLOAT", [], nil},
            {"LPUSH", ["k"], nil},
            {"RPUSH", ["k"], nil},
            {"LRANGE", ["k", "0"], nil},
            {"LLEN", [], nil},
            {"LINDEX", ["k"], nil},
            {"SADD", ["k"], nil},
            {"SREM", ["k"], nil},
            {"SMEMBERS", [], nil},
            {"SISMEMBER", ["k"], nil},
            {"SCARD", [], nil},
            {"ZADD", ["k"], nil},
            {"ZREM", ["k"], nil},
            {"ZSCORE", ["k"], nil},
            {"ZRANK", ["k"], nil},
            {"ZCARD", [], nil},
            {"ZCOUNT", ["k", "0"], nil},
            {"PING", ["a", "b", "c"], nil},
            {"ECHO", [], nil},
            {"DBSIZE", ["extra"], nil},
            {"SELECT", [], nil}
          ]

          for {cmd_name, too_few, too_many} <- wrong_arity_cases do
            if too_few != nil do
              result = cmd(sock, [cmd_name | too_few])

              assert {:error, _msg} = result,
                     "#{cmd_name} with args #{inspect(too_few)} should return error, got: #{inspect(result)}"
            end

            if too_many != nil do
              result = cmd(sock, [cmd_name | too_many])

              assert {:error, _msg} = result,
                     "#{cmd_name} with args #{inspect(too_many)} should return error, got: #{inspect(result)}"
            end
          end
        end
      end

      describe "Stress: rapid create/delete cycle" do
        test "100 rapid SET then DEL cycles do not corrupt state", %{sock: sock} do
          for i <- 1..100 do
            k = ukey("stress_#{i}")
            assert_ok(cmd(sock, ["SET", k, "val#{i}"]))
            assert cmd(sock, ["DEL", k]) == 1
            assert cmd(sock, ["GET", k]) == nil
          end
        end

        test "interleaved data structure operations", %{sock: sock} do
          # Create keys of different types, then delete them all
          str_k = ukey("stress_str")
          hash_k = ukey("stress_hash")
          list_k = ukey("stress_list")
          set_k = ukey("stress_set")
          zset_k = ukey("stress_zset")

          cmd(sock, ["SET", str_k, "v"])
          cmd(sock, ["HSET", hash_k, "f", "v"])
          cmd(sock, ["RPUSH", list_k, "a"])
          cmd(sock, ["SADD", set_k, "m"])
          cmd(sock, ["ZADD", zset_k, "1.0", "m"])

          # Delete all
          deleted = cmd(sock, ["DEL", str_k, hash_k, list_k, set_k, zset_k])
          assert deleted == 5

          # Verify all gone
          assert cmd(sock, ["GET", str_k]) == nil
          assert cmd(sock, ["HLEN", hash_k]) == 0
          assert cmd(sock, ["LLEN", list_k]) == 0
          assert cmd(sock, ["SCARD", set_k]) == 0
          assert cmd(sock, ["ZCARD", zset_k]) == 0

          # Re-create with DIFFERENT types to ensure no stale type metadata
          # was string, now list
          cmd(sock, ["RPUSH", str_k, "now_a_list"])
          assert cmd(sock, ["LRANGE", str_k, "0", "-1"]) == ["now_a_list"]

          # was hash, now string
          cmd(sock, ["SET", hash_k, "now_a_string"])
          assert cmd(sock, ["GET", hash_k]) == "now_a_string"
        end
      end
    end
  end
end
