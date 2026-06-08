defmodule FerricstoreServer.Integration.EdgeCasesTest.Sections.DataTypeBoundariesOverTcp do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.Router
      alias FerricstoreServer.Resp.{Encoder, Parser}
      alias FerricstoreServer.Listener

      describe "data type boundaries over TCP" do
        test "SET a value with null bytes, GET it back intact" do
          sock = connect_and_hello()
          k = ukey("null_bytes_tcp")
          v = "before\x00middle\x00after\x00"

          assert {:simple, "OK"} == cmd(sock, ["SET", k, v])
          result = cmd(sock, ["GET", k])
          assert result == v
          assert byte_size(result) == byte_size(v)

          :gen_tcp.close(sock)
        end

        test "SET a key with unicode characters, GET it back intact" do
          sock = connect_and_hello()
          k = ukey("unicode_key_tcp")
          unicode_val = "Hello世界🌍Привет cafe\u0301"

          assert {:simple, "OK"} == cmd(sock, ["SET", k, unicode_val])
          assert unicode_val == cmd(sock, ["GET", k])

          :gen_tcp.close(sock)
        end

        test "SET a key whose name contains unicode, GET it back" do
          sock = connect_and_hello()
          k = "キー_#{:rand.uniform(999_999)}"

          assert {:simple, "OK"} == cmd(sock, ["SET", k, "unicode_key_value"])
          assert "unicode_key_value" == cmd(sock, ["GET", k])

          :gen_tcp.close(sock)
        end

        test "RPUSH + LRANGE with binary data containing null bytes and CRLF" do
          sock = connect_and_hello()
          k = ukey("list_binary_tcp")

          elem1 = "normal"
          elem2 = "with\x00null"
          elem3 = "with\r\ncrlf"
          elem4 = <<0xFF, 0xFE, 0x00, 0x01>>

          assert is_integer(cmd(sock, ["RPUSH", k, elem1, elem2, elem3, elem4]))

          result = cmd(sock, ["LRANGE", k, "0", "-1"])
          assert is_list(result)
          assert length(result) == 4
          assert Enum.at(result, 0) == elem1
          assert Enum.at(result, 1) == elem2
          assert Enum.at(result, 2) == elem3
          assert Enum.at(result, 3) == elem4

          :gen_tcp.close(sock)
        end

        test "HSET + HGETALL with empty field name" do
          sock = connect_and_hello()
          k = ukey("hash_empty_field")

          # HSET with empty string as field name
          resp = cmd(sock, ["HSET", k, "", "empty_field_value"])
          assert is_integer(resp) or resp == 1

          result = cmd(sock, ["HGETALL", k])
          # HGETALL returns a flat list [field, value, field, value, ...]
          # or a map in RESP3 mode
          cond do
            is_map(result) ->
              assert result[""] == "empty_field_value"

            is_list(result) ->
              assert "" in result
              assert "empty_field_value" in result
          end

          # HGET with empty field name
          assert "empty_field_value" == cmd(sock, ["HGET", k, ""])

          :gen_tcp.close(sock)
        end

        test "HSET + HGETALL with unicode field names" do
          sock = connect_and_hello()
          k = ukey("hash_unicode_field")

          assert is_integer(cmd(sock, ["HSET", k, "名前", "太郎", "emoji", "🎉"]))

          val1 = cmd(sock, ["HGET", k, "名前"])
          assert val1 == "太郎"

          val2 = cmd(sock, ["HGET", k, "emoji"])
          assert val2 == "🎉"

          :gen_tcp.close(sock)
        end

        test "large pipeline: 50 SETs then 50 GETs return correct values" do
          sock = connect_and_hello()

          pairs =
            for i <- 1..50 do
              k = ukey("bulk_pipe_#{i}")
              v = "value_#{i}_#{:binary.copy("x", 100)}"
              {k, v}
            end

          set_cmds =
            Enum.map(pairs, fn {k, v} -> Encoder.encode(["SET", k, v]) end)

          get_cmds =
            Enum.map(pairs, fn {k, _v} -> Encoder.encode(["GET", k]) end)

          blob = IO.iodata_to_binary(set_cmds ++ get_cmds)
          :ok = send_raw(sock, blob)

          responses = recv_n(sock, 100, 30_000)

          # First 50 responses should all be OK
          set_responses = Enum.take(responses, 50)
          assert Enum.all?(set_responses, &(&1 == {:simple, "OK"}))

          # Last 50 responses should match the values
          get_responses = Enum.drop(responses, 50)

          Enum.zip(pairs, get_responses)
          |> Enum.each(fn {{_k, v}, resp} ->
            assert resp == v
          end)

          :gen_tcp.close(sock)
        end

        test "value with all 256 byte values SET and GET over TCP" do
          sock = connect_and_hello()
          k = ukey("all_bytes_tcp")
          v = Enum.into(0..255, <<>>, fn b -> <<b>> end)

          assert {:simple, "OK"} == cmd(sock, ["SET", k, v])
          result = cmd(sock, ["GET", k])
          assert result == v
          assert byte_size(result) == 256

          :gen_tcp.close(sock)
        end

        test "value with RESP-like content does not confuse the parser" do
          sock = connect_and_hello()
          k = ukey("resp_confusion")
          # A value that looks like RESP protocol data
          v = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

          assert {:simple, "OK"} == cmd(sock, ["SET", k, v])
          assert v == cmd(sock, ["GET", k])

          :gen_tcp.close(sock)
        end
      end

      describe "concurrent access over TCP" do
        test "two clients SET/GET on independent keys concurrently: no data corruption" do
          # Two clients each write and read 100 unique keys simultaneously.
          # Verifies connection multiplexing does not cause cross-contamination.
          results =
            1..2
            |> Enum.map(fn client_id ->
              Task.async(fn ->
                sock = connect_and_hello()

                for i <- 1..100 do
                  k = ukey("conc_indep_c#{client_id}_#{i}")
                  v = "value_c#{client_id}_#{i}"
                  assert {:simple, "OK"} == cmd(sock, ["SET", k, v])
                  assert v == cmd(sock, ["GET", k])
                end

                :gen_tcp.close(sock)
                :ok
              end)
            end)
            |> Task.await_many(30_000)

          assert Enum.all?(results, &(&1 == :ok))
        end

        test "client A WATCHes key, client B modifies it, client A EXEC returns nil" do
          k = ukey("watch_conflict_tcp")

          # Client A: set initial value and WATCH
          sock_a = connect_and_hello()
          assert {:simple, "OK"} == cmd(sock_a, ["SET", k, "original"])
          assert {:simple, "OK"} == cmd(sock_a, ["WATCH", k])

          # Client B: modify the watched key
          sock_b = connect_and_hello()
          assert {:simple, "OK"} == cmd(sock_b, ["SET", k, "modified_by_b"])
          :gen_tcp.close(sock_b)

          # Client A: MULTI/EXEC should abort (return nil)
          assert {:simple, "OK"} == cmd(sock_a, ["MULTI"])
          assert {:simple, "QUEUED"} == cmd(sock_a, ["SET", k, "from_txn"])
          assert nil == cmd(sock_a, ["EXEC"])

          # Verify the value is from client B, not from the aborted transaction
          assert "modified_by_b" == cmd(sock_a, ["GET", k])

          :gen_tcp.close(sock_a)
        end

        test "client A subscribes, client B publishes, client A receives message" do
          channel = ukey("pubsub_chan")

          # Client A: subscribe
          sock_a = connect_and_hello()

          :ok = :gen_tcp.send(sock_a, IO.iodata_to_binary(Encoder.encode(["SUBSCRIBE", channel])))

          # Read the subscribe confirmation push message
          sub_resp = recv_one(sock_a, 5_000)
          # The subscribe response is a push: {:push, ["subscribe", channel, 1]}
          assert match?({:push, ["subscribe", ^channel, 1]}, sub_resp)

          # Client B: publish a message
          sock_b = connect_and_hello()
          pub_resp = cmd(sock_b, ["PUBLISH", channel, "hello_world"])
          # PUBLISH returns the number of subscribers that received the message
          assert is_integer(pub_resp)
          assert pub_resp >= 1
          :gen_tcp.close(sock_b)

          # Client A: should receive the published message as a push
          # The socket is in active:once mode for pubsub, need to receive differently.
          # The pubsub_loop sends data directly on the socket, so we can recv.
          msg = recv_pubsub_message(sock_a, 5_000)

          assert match?({:push, ["message", ^channel, "hello_world"]}, msg),
                 "Expected push message, got: #{inspect(msg)}"

          :gen_tcp.close(sock_a)
        end

        test "10 concurrent clients each do SET/GET pipeline without data corruption" do
          results =
            1..10
            |> Enum.map(fn client_id ->
              Task.async(fn ->
                sock = connect_and_hello()

                for i <- 1..20 do
                  k = ukey("conc_client#{client_id}_#{i}")
                  v = "client#{client_id}_val#{i}"
                  assert {:simple, "OK"} == cmd(sock, ["SET", k, v])
                  assert v == cmd(sock, ["GET", k])
                end

                :gen_tcp.close(sock)
                :ok
              end)
            end)
            |> Task.await_many(30_000)

          assert Enum.all?(results, &(&1 == :ok))
        end
      end
    end
  end
end
