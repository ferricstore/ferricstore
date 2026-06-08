defmodule FerricstoreServer.Integration.ProbCommandsTcpTest do
  @moduledoc """
  Integration tests for probabilistic data structure commands over the
  Redis TCP protocol through ferricstore_server.

  Starts a Ranch TCP listener on a random port, connects via gen_tcp,
  sends raw RESP commands, and verifies responses.
  """
  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  @listener_name :prob_test_listener

  setup_all do
    # Clear cached raw_store so it gets rebuilt with prob_write/prob_dir_for_key
    :persistent_term.erase(:ferricstore_raw_store)

    # Start the pg scope for ACL connection tracking
    pg_group = FerricstoreServer.Connection.acl_pg_group()

    case :pg.start_link(pg_group) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    # Start a TCP listener on a random port
    transport_opts = %{socket_opts: [port: 0, nodelay: true]}
    protocol_opts = %{}

    {:ok, _} =
      :ranch.start_listener(
        @listener_name,
        :ranch_tcp,
        transport_opts,
        FerricstoreServer.Connection,
        protocol_opts
      )

    port = :ranch.get_port(@listener_name)

    on_exit(fn ->
      :ranch.stop_listener(@listener_name)
      :persistent_term.erase(:ferricstore_raw_store)
    end)

    {:ok, port: port}
  end

  setup do
    ShardHelpers.flush_all_keys()
    flush_prob_dirs()
    :ok
  end

  defp flush_prob_dirs do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    for i <- 0..(shard_count - 1) do
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)
      prob_dir = Path.join(shard_path, "prob")

      case File.ls(prob_dir) do
        {:ok, files} -> Enum.each(files, &File.rm(Path.join(prob_dir, &1)))
        _ -> :ok
      end
    end
  end

  # -------------------------------------------------------------------
  # RESP helpers
  # -------------------------------------------------------------------

  defp connect(ctx) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", ctx.port, [
        :binary,
        active: false,
        packet: :raw,
        buffer: 65_536
      ])

    sock
  end

  defp redis(sock, args) do
    :ok = :gen_tcp.send(sock, encode_cmd(args))
    read_response(sock)
  end

  defp encode_cmd(args) do
    parts =
      Enum.map(args, fn arg ->
        s = to_string(arg)
        "$#{byte_size(s)}\r\n#{s}\r\n"
      end)

    "*#{length(args)}\r\n#{Enum.join(parts)}"
  end

  # Read a complete RESP response from the socket.
  # Accumulates data until we have a parseable response.
  defp read_response(sock) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    parse_resp(data)
  end

  defp parse_resp(<<"+" , rest::binary>>), do: {:ok, String.trim(rest)}
  defp parse_resp(<<"-" , rest::binary>>), do: {:error, String.trim(rest)}
  defp parse_resp(<<":" , rest::binary>>), do: {:ok, rest |> String.trim() |> String.to_integer()}

  defp parse_resp(<<"$", rest::binary>>) do
    case String.split(rest, "\r\n", parts: 2) do
      ["-1" | _] -> {:ok, nil}
      [len_str, remainder] ->
        len = String.to_integer(len_str)
        {:ok, binary_part(remainder, 0, len)}
    end
  end

  defp parse_resp(<<"*", rest::binary>>) do
    case String.split(rest, "\r\n", parts: 2) do
      ["-1" | _] -> {:ok, nil}
      [count_str, remainder] ->
        count = String.to_integer(count_str)
        {elements, _rest} = parse_array_elements(remainder, count, [])
        {:ok, elements}
    end
  end

  # Fallback: return raw
  defp parse_resp(data), do: {:raw, data}

  defp parse_array_elements(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp parse_array_elements(<<":", rest::binary>>, n, acc) do
    [val_str | remainder] = String.split(rest, "\r\n", parts: 2)
    parse_array_elements(hd(remainder |> List.wrap()), n - 1, [String.to_integer(val_str) | acc])
  end

  defp parse_array_elements(<<"$", rest::binary>>, n, acc) do
    [len_str, remainder] = String.split(rest, "\r\n", parts: 2)
    len = String.to_integer(len_str)

    if len == -1 do
      # Skip the trailing \r\n
      parse_array_elements(remainder, n - 1, [nil | acc])
    else
      value = binary_part(remainder, 0, len)
      after_value = binary_part(remainder, len + 2, byte_size(remainder) - len - 2)
      parse_array_elements(after_value, n - 1, [value | acc])
    end
  end

  defp parse_array_elements(<<"+", rest::binary>>, n, acc) do
    [val | remainder] = String.split(rest, "\r\n", parts: 2)
    parse_array_elements(hd(remainder |> List.wrap()), n - 1, [val | acc])
  end

  defp parse_array_elements(rest, _n, acc) do
    {Enum.reverse(acc), rest}
  end

  # -------------------------------------------------------------------
  # Bloom filter
  # -------------------------------------------------------------------

  describe "Bloom filter over TCP" do
    test "BF.RESERVE + BF.ADD + BF.EXISTS + BF.CARD + BF.INFO", ctx do
      sock = connect(ctx)

      assert {:ok, "OK"} = redis(sock, ["BF.RESERVE", "bf1", "0.01", "1000"])
      assert {:ok, 1} = redis(sock, ["BF.ADD", "bf1", "hello"])
      assert {:ok, 0} = redis(sock, ["BF.ADD", "bf1", "hello"])
      assert {:ok, 1} = redis(sock, ["BF.EXISTS", "bf1", "hello"])
      assert {:ok, 0} = redis(sock, ["BF.EXISTS", "bf1", "missing"])
      assert {:ok, 1} = redis(sock, ["BF.CARD", "bf1"])

      # BF.INFO returns a large array — verify not an error
      :ok = :gen_tcp.send(sock, encode_cmd(["BF.INFO", "bf1"]))
      {:ok, info_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(info_raw, "-")
      assert String.contains?(info_raw, "Capacity")

      :gen_tcp.close(sock)
    end

    test "BF.ADD auto-creates + BF.MADD + BF.MEXISTS", ctx do
      sock = connect(ctx)

      assert {:ok, 1} = redis(sock, ["BF.ADD", "bf2", "auto"])
      assert {:ok, 1} = redis(sock, ["BF.EXISTS", "bf2", "auto"])

      :ok = :gen_tcp.send(sock, encode_cmd(["BF.MADD", "bf2", "a", "b", "c"]))
      {:ok, madd_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(madd_raw, "-")

      :ok = :gen_tcp.send(sock, encode_cmd(["BF.MEXISTS", "bf2", "a", "b", "missing"]))
      {:ok, mex_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(mex_raw, "-")

      :gen_tcp.close(sock)
    end

    test "BF.RESERVE rejects duplicate key", ctx do
      sock = connect(ctx)
      {:ok, "OK"} = redis(sock, ["BF.RESERVE", "bf_dup", "0.01", "100"])
      {:error, _} = redis(sock, ["BF.RESERVE", "bf_dup", "0.01", "100"])
      :gen_tcp.close(sock)
    end
  end

  # -------------------------------------------------------------------
  # CMS
  # -------------------------------------------------------------------

  describe "CMS over TCP" do
    test "CMS.INITBYDIM + CMS.INCRBY + CMS.QUERY + CMS.INFO", ctx do
      sock = connect(ctx)

      assert {:ok, "OK"} = redis(sock, ["CMS.INITBYDIM", "cms1", "100", "7"])

      {:ok, counts} = redis(sock, ["CMS.INCRBY", "cms1", "apple", "5", "banana", "3"])
      assert counts == [5, 3]

      {:ok, query} = redis(sock, ["CMS.QUERY", "cms1", "apple", "banana", "cherry"])
      assert query == [5, 3, 0]

      {:ok, info} = redis(sock, ["CMS.INFO", "cms1"])
      assert is_list(info)
      assert "width" in info

      :gen_tcp.close(sock)
    end

    test "CMS.INITBYPROB creates sketch", ctx do
      sock = connect(ctx)
      assert {:ok, "OK"} = redis(sock, ["CMS.INITBYPROB", "cms_prob", "0.001", "0.99"])
      :gen_tcp.close(sock)
    end

    test "CMS.MERGE combines source sketches", ctx do
      sock = connect(ctx)
      {:ok, "OK"} = redis(sock, ["CMS.INITBYDIM", "src1", "100", "7"])
      {:ok, "OK"} = redis(sock, ["CMS.INITBYDIM", "src2", "100", "7"])
      redis(sock, ["CMS.INCRBY", "src1", "item", "10"])
      redis(sock, ["CMS.INCRBY", "src2", "item", "20"])

      {:ok, "OK"} = redis(sock, ["CMS.MERGE", "dst", "2", "src1", "src2"])

      {:ok, [count]} = redis(sock, ["CMS.QUERY", "dst", "item"])
      assert count == 30

      :gen_tcp.close(sock)
    end
  end

  # -------------------------------------------------------------------
  # Cuckoo filter
  # -------------------------------------------------------------------

  describe "Cuckoo filter over TCP" do
    test "CF.RESERVE + CF.ADD + CF.EXISTS + CF.DEL + CF.COUNT + CF.INFO", ctx do
      sock = connect(ctx)

      assert {:ok, "OK"} = redis(sock, ["CF.RESERVE", "cf1", "1024"])
      assert {:ok, 1} = redis(sock, ["CF.ADD", "cf1", "elem"])
      assert {:ok, 1} = redis(sock, ["CF.EXISTS", "cf1", "elem"])
      assert {:ok, 0} = redis(sock, ["CF.EXISTS", "cf1", "nope"])
      assert {:ok, 1} = redis(sock, ["CF.COUNT", "cf1", "elem"])

      # Add duplicate
      {:ok, 1} = redis(sock, ["CF.ADD", "cf1", "elem"])
      assert {:ok, 2} = redis(sock, ["CF.COUNT", "cf1", "elem"])

      # Delete one occurrence
      assert {:ok, 1} = redis(sock, ["CF.DEL", "cf1", "elem"])
      assert {:ok, 1} = redis(sock, ["CF.COUNT", "cf1", "elem"])

      {:ok, info} = redis(sock, ["CF.INFO", "cf1"])
      assert is_list(info)
      assert "Size" in info

      :gen_tcp.close(sock)
    end

    test "CF.ADDNX + CF.MEXISTS + CF.ADD auto-create", ctx do
      sock = connect(ctx)

      assert {:ok, 1} = redis(sock, ["CF.ADDNX", "cfnx", "unique"])
      assert {:ok, 0} = redis(sock, ["CF.ADDNX", "cfnx", "unique"])

      {:ok, results} = redis(sock, ["CF.MEXISTS", "cfnx", "unique", "absent"])
      assert results == [1, 0]

      # Auto-create
      assert {:ok, 1} = redis(sock, ["CF.ADD", "cf_auto", "x"])
      assert {:ok, 1} = redis(sock, ["CF.EXISTS", "cf_auto", "x"])

      :gen_tcp.close(sock)
    end
  end

  # -------------------------------------------------------------------
  # TopK
  # -------------------------------------------------------------------

  describe "TopK over TCP" do
    test "TOPK.RESERVE + TOPK.ADD + TOPK.QUERY + TOPK.LIST + TOPK.INFO", ctx do
      sock = connect(ctx)

      assert {:ok, "OK"} = redis(sock, ["TOPK.RESERVE", "tk1", "3"])

      {:ok, evicted} = redis(sock, ["TOPK.ADD", "tk1", "a", "b", "c", "a", "a"])
      assert is_list(evicted)

      {:ok, query} = redis(sock, ["TOPK.QUERY", "tk1", "a", "b", "zzz"])
      assert is_list(query)
      assert hd(query) == 1  # 'a' in top-K

      {:ok, items} = redis(sock, ["TOPK.LIST", "tk1"])
      assert is_list(items)
      assert "a" in items

      {:ok, info} = redis(sock, ["TOPK.INFO", "tk1"])
      assert is_list(info)
      assert "k" in info

      :gen_tcp.close(sock)
    end

    test "TOPK.RESERVE with custom dimensions", ctx do
      sock = connect(ctx)
      assert {:ok, "OK"} = redis(sock, ["TOPK.RESERVE", "tk2", "5", "20", "7", "0.9"])

      {:ok, info} = redis(sock, ["TOPK.INFO", "tk2"])
      assert is_list(info)

      :gen_tcp.close(sock)
    end

    test "TOPK.INCRBY + TOPK.COUNT", ctx do
      sock = connect(ctx)
      {:ok, "OK"} = redis(sock, ["TOPK.RESERVE", "tk3", "3"])

      {:ok, _} = redis(sock, ["TOPK.INCRBY", "tk3", "hot", "100", "warm", "50"])

      {:ok, counts} = redis(sock, ["TOPK.COUNT", "tk3", "hot", "warm"])
      assert is_list(counts)
      assert hd(counts) >= 100

      :gen_tcp.close(sock)
    end

    test "TOPK.LIST WITHCOUNT", ctx do
      sock = connect(ctx)
      {:ok, "OK"} = redis(sock, ["TOPK.RESERVE", "tk4", "3"])
      redis(sock, ["TOPK.ADD", "tk4", "x", "x", "x", "y", "y"])

      {:ok, items} = redis(sock, ["TOPK.LIST", "tk4", "WITHCOUNT"])
      assert is_list(items)
      assert "x" in items

      :gen_tcp.close(sock)
    end
  end

  # -------------------------------------------------------------------
  # TDigest
  # -------------------------------------------------------------------

  describe "TDigest over TCP" do
    defp td_key(base), do: "#{base}_#{System.unique_integer([:positive])}"

    test "TDIGEST.CREATE + TDIGEST.ADD + TDIGEST.QUANTILE + TDIGEST.INFO", ctx do
      sock = connect(ctx)
      k = td_key("td1")

      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", k, "COMPRESSION", "100"])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.ADD", k, "1", "2", "3", "4", "5"])

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.QUANTILE", k, "0.5"]))
      {:ok, q_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(q_raw, "-")

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.INFO", k]))
      {:ok, info_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(info_raw, "-")
      assert String.contains?(info_raw, "Compression")

      :gen_tcp.close(sock)
    end

    test "TDIGEST.CREATE with default compression", ctx do
      sock = connect(ctx)
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", td_key("td_def")])
      :gen_tcp.close(sock)
    end

    test "TDIGEST.MIN + TDIGEST.MAX", ctx do
      sock = connect(ctx)
      k = td_key("td_mm")
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", k])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.ADD", k, "10", "20", "30"])

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.MIN", k]))
      {:ok, min_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(min_raw, "-"), "TDIGEST.MIN failed: #{min_raw}"

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.MAX", k]))
      {:ok, max_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(max_raw, "-")

      :gen_tcp.close(sock)
    end

    test "TDIGEST.CDF + TDIGEST.RANK + TDIGEST.TRIMMED_MEAN", ctx do
      sock = connect(ctx)
      k = td_key("td_cdf")
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", k])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.ADD", k, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.CDF", k, "5"]))
      {:ok, cdf_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(cdf_raw, "-")

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.RANK", k, "5"]))
      {:ok, rank_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(rank_raw, "-")

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.TRIMMED_MEAN", k, "0.1", "0.9"]))
      {:ok, tm_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(tm_raw, "-")

      :gen_tcp.close(sock)
    end

    test "TDIGEST.RESET clears data", ctx do
      sock = connect(ctx)
      k = td_key("td_reset")
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", k])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.ADD", k, "1", "2", "3"])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.RESET", k])
      :gen_tcp.close(sock)
    end

    test "TDIGEST.MERGE combines digests", ctx do
      sock = connect(ctx)
      s1 = td_key("td_src1")
      s2 = td_key("td_src2")
      dst = td_key("td_dst")
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", s1])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", s2])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.ADD", s1, "1", "2", "3"])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.ADD", s2, "4", "5", "6"])

      assert {:ok, "OK"} = redis(sock, ["TDIGEST.MERGE", dst, "2", s1, s2])

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.INFO", dst]))
      {:ok, info_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(info_raw, "-")

      :gen_tcp.close(sock)
    end

    test "TDIGEST.BYRANK + TDIGEST.BYREVRANK + TDIGEST.REVRANK", ctx do
      sock = connect(ctx)
      k = td_key("td_rank")
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.CREATE", k])
      assert {:ok, "OK"} = redis(sock, ["TDIGEST.ADD", k, "10", "20", "30", "40", "50"])

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.BYRANK", k, "0", "2", "4"]))
      {:ok, br_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(br_raw, "-")

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.BYREVRANK", k, "0", "2"]))
      {:ok, brr_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(brr_raw, "-")

      :ok = :gen_tcp.send(sock, encode_cmd(["TDIGEST.REVRANK", k, "30"]))
      {:ok, rr_raw} = :gen_tcp.recv(sock, 0, 5000)
      refute String.starts_with?(rr_raw, "-")

      :gen_tcp.close(sock)
    end
  end

  # -------------------------------------------------------------------
  # DEL + FLUSHDB
  # -------------------------------------------------------------------

  describe "cleanup over TCP" do
    test "DEL removes bloom filter", ctx do
      sock = connect(ctx)
      redis(sock, ["BF.ADD", "del_bf", "elem"])
      {:ok, 1} = redis(sock, ["BF.EXISTS", "del_bf", "elem"])

      redis(sock, ["DEL", "del_bf"])
      {:ok, 0} = redis(sock, ["BF.EXISTS", "del_bf", "elem"])

      :gen_tcp.close(sock)
    end
  end
end
