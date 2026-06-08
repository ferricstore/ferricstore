defmodule FerricstoreServer.Integration.AdvancedTypesTcpTest.Sections.JsonTypeOverTcp do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Encoder
      alias FerricstoreServer.Resp.Parser
      alias FerricstoreServer.Listener
      alias Ferricstore.Test.ShardHelpers

      describe "JSON.TYPE over TCP" do
        test "JSON.TYPE returns the type of root value", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsontype")

          send_cmd(sock, ["JSON.SET", k, "$", ~s({"a":1})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.TYPE", k])
          assert recv_response(sock) == "object"

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.STRLEN over TCP" do
        test "JSON.STRLEN returns string length at path", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsonstrlen")

          send_cmd(sock, ["JSON.SET", k, "$", ~s("hello")])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.STRLEN", k])
          assert recv_response(sock) == 5

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.OBJKEYS over TCP" do
        test "JSON.OBJKEYS returns keys of a JSON object", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsonobjkeys")

          send_cmd(sock, ["JSON.SET", k, "$", ~s({"a":1,"b":2})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.OBJKEYS", k])
          keys = recv_response(sock)
          assert is_list(keys)
          assert Enum.sort(keys) == ["a", "b"]

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.OBJLEN over TCP" do
        test "JSON.OBJLEN returns number of keys in object", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsonobjlen")

          send_cmd(sock, ["JSON.SET", k, "$", ~s({"a":1,"b":2,"c":3})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.OBJLEN", k])
          assert recv_response(sock) == 3

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.ARRAPPEND over TCP" do
        test "JSON.ARRAPPEND appends to array and returns new length", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsonarrappend")

          send_cmd(sock, ["JSON.SET", k, "$", ~s([1,2])])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.ARRAPPEND", k, "$", "3"])
          assert recv_response(sock) == 3

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.ARRLEN over TCP" do
        test "JSON.ARRLEN returns array length", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsonarrlen")

          send_cmd(sock, ["JSON.SET", k, "$", ~s([1,2,3])])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.ARRLEN", k])
          assert recv_response(sock) == 3

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.NUMINCRBY over TCP" do
        test "JSON.NUMINCRBY increments a number", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsonnumincrby")

          send_cmd(sock, ["JSON.SET", k, "$", ~s({"counter":10})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.NUMINCRBY", k, "$.counter", "5"])
          result = recv_response(sock)
          assert result == "15"

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.TOGGLE over TCP" do
        test "JSON.TOGGLE flips a boolean value", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsontoggle")

          send_cmd(sock, ["JSON.SET", k, "$", ~s({"active":true})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.TOGGLE", k, "$.active"])
          result = recv_response(sock)
          # Returns the new boolean value as 0 (false) or 1 (true)
          assert result in [0, "false"]

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.CLEAR over TCP" do
        test "JSON.CLEAR resets container to empty", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("jsonclear")

          send_cmd(sock, ["JSON.SET", k, "$", ~s({"a":1,"b":2})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.CLEAR", k])
          assert recv_response(sock) == 1

          send_cmd(sock, ["JSON.GET", k])
          result = recv_response(sock)
          assert Jason.decode!(result) == %{}

          :gen_tcp.close(sock)
        end
      end

      describe "JSON.MGET over TCP" do
        test "JSON.MGET returns values from multiple keys", %{port: port} do
          sock = connect_and_hello(port)
          k1 = ukey("jsonmget1")
          k2 = ukey("jsonmget2")

          send_cmd(sock, ["JSON.SET", k1, "$", ~s({"name":"alice"})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.SET", k2, "$", ~s({"name":"bob"})])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["JSON.MGET", k1, k2, "$.name"])
          result = recv_response(sock)
          assert is_list(result)
          assert length(result) == 2

          :gen_tcp.close(sock)
        end
      end
    end
  end
end
