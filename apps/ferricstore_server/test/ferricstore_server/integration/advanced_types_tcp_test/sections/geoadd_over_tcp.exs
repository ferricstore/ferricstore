defmodule FerricstoreServer.Integration.AdvancedTypesTcpTest.Sections.GeoaddOverTcp do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Encoder
      alias FerricstoreServer.Resp.Parser
      alias FerricstoreServer.Listener
      alias Ferricstore.Test.ShardHelpers

      describe "GEOADD over TCP" do
        test "GEOADD adds members and returns count", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("geo")

          send_cmd(sock, [
            "GEOADD",
            k,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania"
          ])

          assert recv_response(sock) == 2

          :gen_tcp.close(sock)
        end
      end

      describe "GEOPOS over TCP" do
        test "GEOPOS returns coordinates for added member", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("geopos")

          send_cmd(sock, ["GEOADD", k, "13.361389", "38.115556", "Palermo"])
          assert recv_response(sock) == 1

          send_cmd(sock, ["GEOPOS", k, "Palermo"])
          [[lng, lat]] = recv_response(sock)
          assert is_binary(lng)
          assert is_binary(lat)

          :gen_tcp.close(sock)
        end
      end

      describe "GEODIST over TCP" do
        test "GEODIST returns distance between two members", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("geodist")

          send_cmd(sock, [
            "GEOADD",
            k,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania"
          ])

          assert recv_response(sock) == 2

          send_cmd(sock, ["GEODIST", k, "Palermo", "Catania", "km"])
          dist = recv_response(sock)
          assert is_binary(dist)
          {dist_f, _} = Float.parse(dist)
          assert dist_f > 100 and dist_f < 200

          :gen_tcp.close(sock)
        end
      end

      describe "GEOHASH over TCP" do
        test "GEOHASH returns hash strings", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("geohash")

          send_cmd(sock, ["GEOADD", k, "13.361389", "38.115556", "Palermo"])
          assert recv_response(sock) == 1

          send_cmd(sock, ["GEOHASH", k, "Palermo"])
          [hash] = recv_response(sock)
          assert is_binary(hash)
          assert byte_size(hash) > 0

          :gen_tcp.close(sock)
        end
      end

      describe "GEOSEARCH over TCP" do
        test "GEOSEARCH FROMLONLAT BYRADIUS returns matching members", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("geosearch")

          send_cmd(sock, [
            "GEOADD",
            k,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania"
          ])

          assert recv_response(sock) == 2

          send_cmd(sock, [
            "GEOSEARCH",
            k,
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "ASC"
          ])

          result = recv_response(sock)
          assert is_list(result)
          assert "Catania" in result

          :gen_tcp.close(sock)
        end
      end

      describe "GEOSEARCHSTORE over TCP" do
        test "GEOSEARCHSTORE stores results in destination key", %{port: port} do
          sock = connect_and_hello(port)
          src = ukey("geoss_src")
          dst = ukey("geoss_dst")

          send_cmd(sock, [
            "GEOADD",
            src,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania"
          ])

          assert recv_response(sock) == 2

          send_cmd(sock, [
            "GEOSEARCHSTORE",
            dst,
            src,
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "ASC"
          ])

          count = recv_response(sock)
          assert is_integer(count)
          assert count >= 1

          :gen_tcp.close(sock)
        end
      end

      describe "PFADD over TCP" do
        test "PFADD adds elements and returns 1 on modification", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("pfadd")

          send_cmd(sock, ["PFADD", k, "a", "b", "c"])
          assert recv_response(sock) == 1

          :gen_tcp.close(sock)
        end

        test "concurrent PFADD updates over TCP preserve every element", %{port: port} do
          k = ukey("pfadd_concurrent")
          element_count = 50
          parent = self()

          tasks =
            for i <- 1..element_count do
              Task.async(fn ->
                worker = connect_and_hello(port)
                send(parent, {:ready, self()})

                receive do
                  :go -> :ok
                after
                  5_000 -> flunk("timed out waiting for concurrent PFADD release")
                end

                send_cmd(worker, ["PFADD", k, "elem:#{i}"])
                response = recv_response(worker)
                :gen_tcp.close(worker)
                response
              end)
            end

          for _ <- 1..element_count do
            assert_receive {:ready, _pid}, 5_000
          end

          Enum.each(tasks, fn task -> send(task.pid, :go) end)

          assert Enum.all?(Enum.map(tasks, &Task.await(&1, 30_000)), &(&1 in [0, 1]))

          reader = connect_and_hello(port)
          send_cmd(reader, ["PFCOUNT", k])
          assert recv_response(reader) == element_count
          :gen_tcp.close(reader)
        end

        test "concurrent pipelined PFADD updates preserve every element", %{port: port} do
          k = ukey("pfadd_pipeline_concurrent")
          element_count = 50
          parent = self()

          tasks =
            for i <- 1..element_count do
              Task.async(fn ->
                worker = connect_and_hello(port)
                send(parent, {:ready, self()})

                receive do
                  :go -> :ok
                after
                  5_000 -> flunk("timed out waiting for concurrent pipelined PFADD release")
                end

                send_pipeline(worker, [
                  ["PFADD", k, "elem:#{i}"],
                  ["PING"]
                ])

                responses = recv_n(worker, 2)
                :gen_tcp.close(worker)
                responses
              end)
            end

          for _ <- 1..element_count do
            assert_receive {:ready, _pid}, 5_000
          end

          Enum.each(tasks, fn task -> send(task.pid, :go) end)

          assert Enum.all?(Enum.map(tasks, &Task.await(&1, 30_000)), fn
                   [result, {:simple, "PONG"}] -> result in [0, 1]
                   _ -> false
                 end)

          reader = connect_and_hello(port)
          send_cmd(reader, ["PFCOUNT", k])
          assert recv_response(reader) == element_count
          :gen_tcp.close(reader)
        end
      end

      describe "PFCOUNT over TCP" do
        test "PFCOUNT returns estimated cardinality", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("pfcount")

          send_cmd(sock, ["PFADD", k, "a", "b", "c"])
          assert recv_response(sock) == 1

          send_cmd(sock, ["PFCOUNT", k])
          count = recv_response(sock)
          assert is_integer(count)
          assert count == 3

          :gen_tcp.close(sock)
        end
      end

      describe "PFMERGE over TCP" do
        test "PFMERGE merges HLL sketches", %{port: port} do
          sock = connect_and_hello(port)
          k1 = ukey("pfmerge1")
          k2 = ukey("pfmerge2")
          dest = ukey("pfmerge_dest")

          send_cmd(sock, ["PFADD", k1, "a", "b", "c"])
          assert recv_response(sock) == 1

          send_cmd(sock, ["PFADD", k2, "c", "d", "e"])
          assert recv_response(sock) == 1

          send_cmd(sock, ["PFMERGE", dest, k1, k2])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["PFCOUNT", dest])
          count = recv_response(sock)
          assert is_integer(count)
          assert count == 5

          :gen_tcp.close(sock)
        end

        test "concurrent PFMERGE operations into one destination preserve every source", %{
          port: port
        } do
          setup_sock = connect_and_hello(port)
          dest = ukey("pfmerge_concurrent_dest")
          source_count = 50

          sources =
            for i <- 1..source_count do
              source = ukey("pfmerge_concurrent_source")
              send_cmd(setup_sock, ["PFADD", source, "elem:#{i}"])
              assert recv_response(setup_sock) == 1
              source
            end

          :gen_tcp.close(setup_sock)
          parent = self()

          tasks =
            for source <- sources do
              Task.async(fn ->
                worker = connect_and_hello(port)
                send(parent, {:ready, self()})

                receive do
                  :go -> :ok
                after
                  5_000 -> flunk("timed out waiting for concurrent PFMERGE release")
                end

                send_cmd(worker, ["PFMERGE", dest, source])
                response = recv_response(worker)
                :gen_tcp.close(worker)
                response
              end)
            end

          for _ <- 1..source_count do
            assert_receive {:ready, _pid}, 5_000
          end

          Enum.each(tasks, fn task -> send(task.pid, :go) end)

          assert Enum.map(tasks, &Task.await(&1, 30_000)) ==
                   List.duplicate({:simple, "OK"}, source_count)

          reader = connect_and_hello(port)
          send_cmd(reader, ["PFCOUNT", dest])
          assert recv_response(reader) == source_count
          :gen_tcp.close(reader)
        end

        test "concurrent pipelined PFMERGE operations preserve every source", %{
          port: port
        } do
          setup_sock = connect_and_hello(port)
          dest = ukey("pfmerge_pipeline_concurrent_dest")
          source_count = 50

          sources =
            for i <- 1..source_count do
              source = ukey("pfmerge_pipeline_concurrent_source")
              send_cmd(setup_sock, ["PFADD", source, "elem:#{i}"])
              assert recv_response(setup_sock) == 1
              source
            end

          :gen_tcp.close(setup_sock)
          parent = self()

          tasks =
            for source <- sources do
              Task.async(fn ->
                worker = connect_and_hello(port)
                send(parent, {:ready, self()})

                receive do
                  :go -> :ok
                after
                  5_000 -> flunk("timed out waiting for concurrent pipelined PFMERGE release")
                end

                send_pipeline(worker, [
                  ["PFMERGE", dest, source],
                  ["PING"]
                ])

                responses = recv_n(worker, 2)
                :gen_tcp.close(worker)
                responses
              end)
            end

          for _ <- 1..source_count do
            assert_receive {:ready, _pid}, 5_000
          end

          Enum.each(tasks, fn task -> send(task.pid, :go) end)

          assert Enum.map(tasks, &Task.await(&1, 30_000)) ==
                   List.duplicate([{:simple, "OK"}, {:simple, "PONG"}], source_count)

          reader = connect_and_hello(port)
          send_cmd(reader, ["PFCOUNT", dest])
          assert recv_response(reader) == source_count
          :gen_tcp.close(reader)
        end
      end

      describe "SANDBOX server commands over TCP" do
        test "sandbox FLUSHDB only clears sandbox keys and keeps outside probabilistic state", %{
          port: port
        } do
          enable_sandbox_mode()

          outside_key = ukey("sandbox_flushdb_outside")
          outside_pf = ukey("sandbox_flushdb_pf_outside")
          inside_key = ukey("sandbox_flushdb_inside")

          outside = connect_and_hello(port)
          send_cmd(outside, ["SET", outside_key, "outside"])
          assert recv_response(outside) == {:simple, "OK"}
          send_cmd(outside, ["PFADD", outside_pf, "a", "b", "c"])
          assert recv_response(outside) == 1
          :gen_tcp.close(outside)

          sandbox = connect_and_hello(port)
          send_cmd(sandbox, ["SANDBOX", "START"])
          token = recv_response(sandbox)
          send_cmd(sandbox, ["SET", inside_key, "inside"])
          assert recv_response(sandbox) == {:simple, "OK"}

          send_cmd(sandbox, ["FLUSHDB"])
          assert recv_response(sandbox) == {:simple, "OK"}
          send_cmd(sandbox, ["GET", inside_key])
          assert recv_response(sandbox) == nil
          :gen_tcp.close(sandbox)

          reader = connect_and_hello(port)
          send_cmd(reader, ["GET", outside_key])
          assert recv_response(reader) == "outside"
          send_cmd(reader, ["PFCOUNT", outside_pf])
          assert recv_response(reader) == 3

          send_inline(reader, ["SANDBOX", "JOIN", token])
          assert recv_response(reader) == {:simple, "OK"}
          send_cmd(reader, ["GET", inside_key])
          assert recv_response(reader) == nil
          :gen_tcp.close(reader)
        end
      end

      describe "XADD over TCP" do
        test "XADD with auto-generated ID returns a stream ID string", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xadd")

          send_cmd(sock, ["XADD", k, "*", "field1", "value1"])
          id = recv_response(sock)
          assert is_binary(id)
          assert String.contains?(id, "-")

          :gen_tcp.close(sock)
        end
      end

      describe "XLEN over TCP" do
        test "XLEN returns entry count", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xlen")

          send_cmd(sock, ["XADD", k, "*", "f", "v"])
          _id = recv_response(sock)

          send_cmd(sock, ["XADD", k, "*", "f", "v2"])
          _id2 = recv_response(sock)

          send_cmd(sock, ["XLEN", k])
          assert recv_response(sock) == 2

          :gen_tcp.close(sock)
        end
      end

      describe "XRANGE over TCP" do
        test "XRANGE returns entries in order", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xrange")

          send_cmd(sock, ["XADD", k, "*", "name", "alice"])
          id1 = recv_response(sock)

          send_cmd(sock, ["XADD", k, "*", "name", "bob"])
          _id2 = recv_response(sock)

          send_cmd(sock, ["XRANGE", k, "-", "+"])
          result = recv_response(sock)
          assert is_list(result)
          assert length(result) == 2
          # Each entry is [id, field, value, ...]
          [first_entry | _] = result
          assert is_list(first_entry)
          assert hd(first_entry) == id1

          :gen_tcp.close(sock)
        end
      end

      describe "XREVRANGE over TCP" do
        test "XREVRANGE returns entries in reverse order", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xrevrange")

          send_cmd(sock, ["XADD", k, "*", "name", "alice"])
          _id1 = recv_response(sock)

          send_cmd(sock, ["XADD", k, "*", "name", "bob"])
          id2 = recv_response(sock)

          send_cmd(sock, ["XREVRANGE", k, "+", "-"])
          result = recv_response(sock)
          assert is_list(result)
          assert length(result) == 2
          [first_entry | _] = result
          assert is_list(first_entry)
          assert hd(first_entry) == id2

          :gen_tcp.close(sock)
        end
      end

      describe "XREAD over TCP" do
        test "XREAD returns entries after given ID", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xread")

          send_cmd(sock, ["XADD", k, "*", "f", "v1"])
          id1 = recv_response(sock)

          send_cmd(sock, ["XADD", k, "*", "f", "v2"])
          _id2 = recv_response(sock)

          send_cmd(sock, ["XREAD", "STREAMS", k, id1])
          result = recv_response(sock)
          assert is_list(result)
          # Result is [[stream_key, [[id, fields], ...]]]
          assert result != []

          :gen_tcp.close(sock)
        end
      end

      describe "XTRIM over TCP" do
        test "XTRIM MAXLEN trims stream entries", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xtrim")

          for i <- 1..5 do
            send_cmd(sock, ["XADD", k, "*", "f", "v"])
            id = recv_response(sock)
            assert is_binary(id), "XADD #{i} should return stream ID, got #{inspect(id)}"
          end

          send_cmd(sock, ["XTRIM", k, "MAXLEN", "2"])
          trimmed = recv_response(sock)
          assert is_integer(trimmed)
          assert trimmed == 3

          send_cmd(sock, ["XLEN", k])
          assert recv_response(sock) == 2

          :gen_tcp.close(sock)
        end
      end

      describe "XDEL over TCP" do
        test "XDEL removes entries by ID", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xdel")

          send_cmd(sock, ["XADD", k, "*", "f", "v"])
          id = recv_response(sock)

          send_cmd(sock, ["XDEL", k, id])
          assert recv_response(sock) == 1

          :gen_tcp.close(sock)
        end
      end

      describe "XINFO over TCP" do
        test "XINFO STREAM returns stream metadata", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xinfo")

          send_cmd(sock, ["XADD", k, "*", "f", "v"])
          _id = recv_response(sock)

          send_cmd(sock, ["XINFO", "STREAM", k])
          result = recv_response(sock)
          assert is_list(result) or is_map(result)

          :gen_tcp.close(sock)
        end
      end

      describe "XGROUP over TCP" do
        test "XGROUP CREATE creates a consumer group", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xgroup")

          send_cmd(sock, ["XADD", k, "*", "f", "v"])
          _id = recv_response(sock)

          send_cmd(sock, ["XGROUP", "CREATE", k, "mygroup", "0"])
          assert recv_response(sock) == {:simple, "OK"}

          :gen_tcp.close(sock)
        end
      end

      describe "XREADGROUP over TCP" do
        test "XREADGROUP reads pending entries for consumer", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xreadgroup")

          send_cmd(sock, ["XADD", k, "*", "f", "v1"])
          _id = recv_response(sock)

          send_cmd(sock, ["XGROUP", "CREATE", k, "grp", "0"])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["XREADGROUP", "GROUP", "grp", "consumer1", "STREAMS", k, ">"])
          result = recv_response(sock)
          assert is_list(result)
          assert result != []

          :gen_tcp.close(sock)
        end
      end

      describe "XACK over TCP" do
        test "XACK acknowledges a message", %{port: port} do
          sock = connect_and_hello(port)
          k = ukey("xack")

          send_cmd(sock, ["XADD", k, "*", "f", "v1"])
          id = recv_response(sock)

          send_cmd(sock, ["XGROUP", "CREATE", k, "grp", "0"])
          assert recv_response(sock) == {:simple, "OK"}

          send_cmd(sock, ["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", k, ">"])
          _result = recv_response(sock)

          send_cmd(sock, ["XACK", k, "grp", id])
          assert recv_response(sock) == 1

          :gen_tcp.close(sock)
        end
      end

    end
  end
end
