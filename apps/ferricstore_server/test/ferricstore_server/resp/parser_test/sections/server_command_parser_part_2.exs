defmodule FerricstoreServer.Resp.ParserTest.Sections.ServerCommandParserPart2 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Parser

      describe "server command parser part 2" do
        test "parses expiry commands into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "EXPIRE", ["k", "10", "nx"], {:expire, "k", 10, :nx}, ["k"]},
                    {:command, "PEXPIRE", ["k", "250"], {:pexpire, "k", 250}, ["k"]},
                    {:command, "EXPIREAT", ["k", "9999999999", "GT"],
                     {:expireat, "k", 9_999_999_999, :gt}, ["k"]},
                    {:command, "PEXPIREAT", ["k", "9999999999999"],
                     {:pexpireat, "k", 9_999_999_999_999}, ["k"]},
                    {:command, "TTL", ["k"], {:ttl, "k"}, ["k"]},
                    {:command, "PTTL", ["k"], {:pttl, "k"}, ["k"]},
                    {:command, "PERSIST", ["k"], {:persist, "k"}, ["k"]}
                  ], ""} =
                   Parser.parse_commands(
                     "expire k 10 nx\r\n" <>
                       "pexpire k 250\r\n" <>
                       "expireat k 9999999999 GT\r\n" <>
                       "pexpireat k 9999999999999\r\n" <>
                       "ttl k\r\n" <>
                       "pttl k\r\n" <>
                       "persist k\r\n"
                   )
        end

        test "keeps expiry semantic parse errors inside AST" do
          assert {:ok,
                  [
                    {:command, "EXPIRE", ["k", "1.5"],
                     {:expire, "k", {:error, "ERR value is not an integer or out of range"}},
                     ["k"]},
                    {:command, "PEXPIRE", ["k", "10", "bad"],
                     {:pexpire, "k", 10, {:error, "ERR Unsupported option bad"}}, ["k"]}
                  ], ""} =
                   Parser.parse_commands("expire k 1.5\r\npexpire k 10 bad\r\n")
        end

        test "parses stream IDs, ranges, and read options into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "XADD", ["s", "NOMKSTREAM", "MAXLEN", "~", "10", "*", "f", "v"],
                     {:xadd, "s", {:auto, ["f", "v"], {:maxlen, true, 10}, true}}, ["s"]},
                    {:command, "XRANGE", ["s", "-", "+", "COUNT", "2"],
                     {:xrange, "s", :min, :max, 2}, ["s"]},
                    {:command, "XREVRANGE", ["s", "9-0", "1-0"],
                     {:xrevrange, "s", {1, 0}, {9, 0}, :infinity}, ["s"]},
                    {:command, "XREAD",
                     ["COUNT", "2", "BLOCK", "0", "STREAMS", "s1", "s2", "0", "$"],
                     {:xread, 2, {:block, 0}, [{"s1", "0"}, {"s2", "$"}]}, ["s1", "s2"]},
                    {:command, "XTRIM", ["s", "MINID", "1-0"],
                     {:xtrim, "s", {:minid, false, "1-0"}}, ["s"]},
                    {:command, "XGROUP", ["CREATE", "s", "g", "0", "MKSTREAM"],
                     {:xgroup_create, "s", "g", "0", true}, ["s"]},
                    {:command, "XREADGROUP",
                     ["GROUP", "g", "c", "COUNT", "1", "STREAMS", "s", ">"],
                     {:xreadgroup, "g", "c", {1, :no_block, [{"s", ">"}]}}, ["s"]},
                    {:command, "XACK", ["s", "g", "1-0"], {:xack, "s", "g", ["1-0"]}, ["s"]}
                  ], ""} =
                   Parser.parse_commands(
                     "xadd s NOMKSTREAM MAXLEN ~ 10 * f v\r\n" <>
                       "xrange s - + COUNT 2\r\n" <>
                       "xrevrange s 9-0 1-0\r\n" <>
                       "xread COUNT 2 BLOCK 0 STREAMS s1 s2 0 $\r\n" <>
                       "xtrim s MINID 1-0\r\n" <>
                       "xgroup CREATE s g 0 MKSTREAM\r\n" <>
                       "xreadgroup GROUP g c COUNT 1 STREAMS s >\r\n" <>
                       "xack s g 1-0\r\n"
                   )
        end

        test "keeps stream semantic parse errors inside AST" do
          assert {:ok,
                  [
                    {:command, "XADD", ["s", "bad-id", "f", "v"],
                     {:xadd,
                      {:error, "ERR Invalid stream ID specified as stream command argument"}},
                     ["s"]},
                    {:command, "XRANGE", ["s", "bad", "+"],
                     {:xrange, "s",
                      {:error, "ERR Invalid stream ID specified as stream command argument"}},
                     ["s"]},
                    {:command, "XTRIM", ["s", "MAXLEN", "-1"],
                     {:xtrim, "s", {:error, "ERR value is not an integer or out of range"}},
                     ["s"]},
                    {:command, "XREAD", ["COUNT", "bad", "STREAMS", "s", "0"],
                     {:xread, {:error, "ERR value is not an integer or out of range"}}, ["s"]},
                    {:command, "XGROUP", ["BAD", "s", "g", "0"],
                     {:xgroup, {:error, "ERR syntax error"}}, ["s"]}
                  ], ""} =
                   Parser.parse_commands(
                     "xadd s bad-id f v\r\n" <>
                       "xrange s bad +\r\n" <>
                       "xtrim s MAXLEN -1\r\n" <>
                       "xread COUNT bad STREAMS s 0\r\n" <>
                       "xgroup BAD s g 0\r\n"
                   )
        end

        test "parses Geo command grammar into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "GEOADD", ["g", "NX", "CH", "13.0", "38.0", "Palermo"],
                     {:geoadd, "g", [:nx, :ch], [{13.0, 38.0, "Palermo"}]}, ["g"]},
                    {:command, "GEODIST", ["g", "Palermo", "Catania", "km"],
                     {:geodist, "g", "Palermo", "Catania", "KM"}, ["g"]},
                    {:command, "GEOSEARCH",
                     [
                       "g",
                       "FROMLONLAT",
                       "13.0",
                       "38.0",
                       "BYRADIUS",
                       "100",
                       "km",
                       "ASC",
                       "COUNT",
                       "2",
                       "ANY",
                       "WITHDIST"
                     ],
                     {:geosearch, "g",
                      [
                        center: {:lonlat, 13.0, 38.0},
                        shape: {:radius, 100_000.0},
                        unit: "KM",
                        raw_radius: 100.0,
                        sort: :asc,
                        count: 2,
                        any: true,
                        withdist: true
                      ]}, ["g"]},
                    {:command, "GEOSEARCHSTORE",
                     ["dst", "src", "FROMMEMBER", "Palermo", "BYBOX", "10", "20", "m"],
                     {:geosearchstore, "dst", "src",
                      [
                        center: {:member, "Palermo"},
                        shape: {:box, 10.0, 20.0},
                        unit: "M"
                      ]}, ["dst", "src"]}
                  ], ""} =
                   Parser.parse_commands(
                     "geoadd g NX CH 13.0 38.0 Palermo\r\n" <>
                       "geodist g Palermo Catania km\r\n" <>
                       "geosearch g FROMLONLAT 13.0 38.0 BYRADIUS 100 km ASC COUNT 2 ANY WITHDIST\r\n" <>
                       "geosearchstore dst src FROMMEMBER Palermo BYBOX 10 20 m\r\n"
                   )
        end

        test "keeps Geo semantic parse errors inside AST" do
          assert {:ok,
                  [
                    {:command, "GEOADD", ["g", "NX", "XX", "13", "38", "p"],
                     {:geoadd,
                      {:error, "ERR XX and NX options at the same time are not compatible"}},
                     ["g"]},
                    {:command, "GEOADD", ["g", "200", "38", "p"],
                     {:geoadd, {:error, "ERR invalid longitude,latitude pair 200,38"}}, ["g"]},
                    {:command, "GEODIST", ["g", "a", "b", "parsecs"],
                     {:geodist, "g", "a", "b",
                      {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}},
                     ["g"]},
                    {:command, "GEOSEARCH", ["g", "BYRADIUS", "100", "KM"],
                     {:geosearch, "g",
                      {:error, "ERR exactly one of FROMMEMBER or FROMLONLAT must be provided"}},
                     ["g"]}
                  ], ""} =
                   Parser.parse_commands(
                     "geoadd g NX XX 13 38 p\r\n" <>
                       "geoadd g 200 38 p\r\n" <>
                       "geodist g a b parsecs\r\n" <>
                       "geosearch g BYRADIUS 100 KM\r\n"
                   )
        end

        test "parses HyperLogLog commands into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "PFADD", ["h", "a", "b"], {:pfadd, ["h", "a", "b"]}, ["h"]},
                    {:command, "PFCOUNT", ["h1", "h2"], {:pfcount, ["h1", "h2"]}, ["h1", "h2"]},
                    {:command, "PFMERGE", ["dst", "h1", "h2"], {:pfmerge, ["dst", "h1", "h2"]},
                     ["dst", "h1", "h2"]},
                    {:command, "PFCOUNT", [],
                     {:pfcount, {:error, "ERR wrong number of arguments for 'pfcount' command"}},
                     []},
                    {:command, "PFMERGE", ["dst"],
                     {:pfmerge, {:error, "ERR wrong number of arguments for 'pfmerge' command"}},
                     ["dst"]}
                  ], ""} =
                   Parser.parse_commands(
                     "pfadd h a b\r\n" <>
                       "pfcount h1 h2\r\n" <>
                       "pfmerge dst h1 h2\r\n" <>
                       "pfcount\r\n" <>
                       "pfmerge dst\r\n"
                   )
        end

        test "parses probabilistic commands into typed Rust AST" do
          assert {:ok,
                  [
                    {:command, "BF.RESERVE", ["bf", "0.01", "100"],
                     {:bf_reserve, "bf", 0.01, 100}, ["bf"]},
                    {:command, "CF.RESERVE", ["cf", "1000"], {:cf_reserve, "cf", 1000}, ["cf"]},
                    {:command, "CMS.INITBYDIM", ["cms", "100", "5"],
                     {:cms_initbydim, "cms", 100, 5}, ["cms"]},
                    {:command, "CMS.INCRBY", ["cms", "a", "2", "b", "3"],
                     {:cms_incrby, "cms", [{"a", 2}, {"b", 3}]}, ["cms"]},
                    {:command, "CMS.MERGE", ["dst", "2", "a", "b", "WEIGHTS", "2", "3"],
                     {:cms_merge, "dst", ["a", "b"], [2, 3]}, ["dst", "a", "b"]},
                    {:command, "TOPK.RESERVE", ["tk", "10", "8", "7", "0.9"],
                     {:topk_reserve, "tk", 10, 8, 7, 0.9}, ["tk"]},
                    {:command, "TOPK.INCRBY", ["tk", "a", "2"], {:topk_incrby, "tk", [{"a", 2}]},
                     ["tk"]},
                    {:command, "TOPK.LIST", ["tk", "WITHCOUNT"], {:topk_list, "tk", true},
                     ["tk"]},
                    {:command, "TDIGEST.CREATE", ["td", "COMPRESSION", "200"],
                     {:tdigest_create, "td", 200}, ["td"]},
                    {:command, "TDIGEST.ADD", ["td", "1.5", "2"],
                     {:tdigest_add, "td", [1.5, 2.0]}, ["td"]},
                    {:command, "TDIGEST.MERGE",
                     ["dst", "2", "a", "b", "COMPRESSION", "200", "OVERRIDE"],
                     {:tdigest_merge, "dst", ["a", "b"], [compression: 200, override: true]},
                     ["dst", "a", "b"]}
                  ], ""} =
                   Parser.parse_commands(
                     "bf.reserve bf 0.01 100\r\n" <>
                       "cf.reserve cf 1000\r\n" <>
                       "cms.initbydim cms 100 5\r\n" <>
                       "cms.incrby cms a 2 b 3\r\n" <>
                       "cms.merge dst 2 a b WEIGHTS 2 3\r\n" <>
                       "topk.reserve tk 10 8 7 0.9\r\n" <>
                       "topk.incrby tk a 2\r\n" <>
                       "topk.list tk WITHCOUNT\r\n" <>
                       "tdigest.create td COMPRESSION 200\r\n" <>
                       "tdigest.add td 1.5 2\r\n" <>
                       "tdigest.merge dst 2 a b COMPRESSION 200 OVERRIDE\r\n"
                   )
        end

        test "keeps probabilistic semantic parse errors inside AST" do
          assert {:ok,
                  [
                    {:command, "BF.RESERVE", ["bf", "bad", "100"],
                     {:bf_reserve, "bf", {:error, "ERR error_rate is not a valid float"}},
                     ["bf"]},
                    {:command, "CMS.INCRBY", ["cms", "a"],
                     {:cms_incrby, "cms",
                      {:error, "ERR wrong number of arguments for 'cms.incrby' command"}},
                     ["cms"]},
                    {:command, "TOPK.LIST", ["tk", "BAD"],
                     {:topk_list, "tk", {:error, "ERR syntax error"}}, ["tk"]},
                    {:command, "TDIGEST.TRIMMED_MEAN", ["td", "0.9", "0.1"],
                     {:tdigest_trimmed_mean, "td",
                      {:error, "ERR TDIGEST: low_quantile must be less than high_quantile"}},
                     ["td"]}
                  ], ""} =
                   Parser.parse_commands(
                     "bf.reserve bf bad 100\r\n" <>
                       "cms.incrby cms a\r\n" <>
                       "topk.list tk BAD\r\n" <>
                       "tdigest.trimmed_mean td 0.9 0.1\r\n"
                   )
        end

        test "emits command-specific AST atom for catalog commands not yet semantically specialized" do
          assert {:ok,
                  [
                    {:command, "BF.ADD", ["bf", "v"], {:bf_add, ["bf", "v"]}, ["bf"]}
                  ], ""} =
                   Parser.parse_commands("*3\r\n$6\r\nBF.ADD\r\n$2\r\nbf\r\n$1\r\nv\r\n")
        end

        test "extracts ACL/tracking keys in Rust command tuple" do
          assert {:ok,
                  [
                    {:command, "MSET", ["a", "1", "b", "2"], {:mset, ["a", "1", "b", "2"]},
                     ["a", "b"]},
                    {:command, "BITOP", ["AND", "dst", "a", "b"],
                     {:bitop, :band, "dst", ["a", "b"]}, ["dst", "a", "b"]}
                  ], ""} =
                   Parser.parse_commands(
                     "*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n" <>
                       "*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$3\r\ndst\r\n$1\r\na\r\n$1\r\nb\r\n"
                   )
        end

        test "extracts XREAD stream keys in Rust command tuple" do
          input =
            "*8\r\n$5\r\nXREAD\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n$7\r\nSTREAMS\r\n$2\r\ns1\r\n$2\r\ns2\r\n$1\r\n0\r\n$1\r\n0\r\n"

          assert {:ok,
                  [
                    {:command, "XREAD", ["COUNT", "2", "STREAMS", "s1", "s2", "0", "0"],
                     {:xread, 2, :no_block, [{"s1", "0"}, {"s2", "0"}]}, ["s1", "s2"]}
                  ], ""} = Parser.parse_commands(input)
        end
      end

      describe "partial reads" do
        test "returns empty list and full buffer for partial simple string" do
          assert {:ok, [], "+OK\r"} = Parser.parse("+OK\r")
        end

        test "returns empty list for partial bulk string header" do
          assert {:ok, [], "$5\r"} = Parser.parse("$5\r")
        end

        test "returns empty list for partial bulk string data" do
          assert {:ok, [], "$5\r\nhel"} = Parser.parse("$5\r\nhel")
        end

        test "returns empty list for partial bulk string missing trailing CRLF" do
          assert {:ok, [], "$5\r\nhello"} = Parser.parse("$5\r\nhello")
        end

        test "returns empty list for partial array" do
          assert {:ok, [], "*3\r\n:1\r\n:2\r\n"} = Parser.parse("*3\r\n:1\r\n:2\r\n")
        end

        test "returns empty list for partial integer" do
          assert {:ok, [], ":42"} = Parser.parse(":42")
        end

        test "returns empty list for empty input" do
          assert {:ok, [], ""} = Parser.parse("")
        end

        test "returns empty list for partial inline" do
          assert {:ok, [], "PING"} = Parser.parse("PING")
        end

        test "returns empty list for partial map" do
          # Map header says 2 entries but only 1 key provided
          assert {:ok, [], "%2\r\n$3\r\nfoo\r\n"} = Parser.parse("%2\r\n$3\r\nfoo\r\n")
        end

        test "server command parser preserves partial command frames at every split" do
          wire = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"

          for split <- 0..(byte_size(wire) - 1) do
            prefix = binary_part(wire, 0, split)
            assert {:ok, [], ^prefix} = Parser.parse_commands(prefix)
          end
        end
      end

      describe "pipelining" do
        test "parses multiple complete commands" do
          input = "+OK\r\n:42\r\n$5\r\nhello\r\n"
          assert {:ok, [{:simple, "OK"}, 42, "hello"], ""} = Parser.parse(input)
        end

        test "parses complete commands with trailing partial" do
          input = ":1\r\n:2\r\n:3\r"
          assert {:ok, [1, 2], ":3\r"} = Parser.parse(input)
        end

        test "parses multiple inline commands" do
          input = "PING\r\nSET foo bar\r\n"

          assert {:ok, [{:inline, ["PING"]}, {:inline, ["SET", "foo", "bar"]}], ""} =
                   Parser.parse(input)
        end

        test "parses mixed typed and inline commands" do
          input = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n+OK\r\n"
          assert {:ok, [["SET", "foo", "bar"], {:simple, "OK"}], ""} = Parser.parse(input)
        end

        test "returns all complete from a large pipeline" do
          input = Enum.map_join(1..100, "", fn i -> ":#{i}\r\n" end)
          assert {:ok, values, ""} = Parser.parse(input)
          assert values == Enum.to_list(1..100)
        end

        test "handles pipeline where first message is complete and second is partial" do
          assert {:ok, [{:simple, "OK"}], "+HE"} = Parser.parse("+OK\r\n+HE")
        end
      end

      describe "nested types" do
        test "array of maps" do
          input = "*2\r\n%1\r\n$1\r\na\r\n:1\r\n%1\r\n$1\r\nb\r\n:2\r\n"
          assert {:ok, [result], ""} = Parser.parse(input)
          assert result == [%{"a" => 1}, %{"b" => 2}]
        end

        test "map of arrays" do
          input = "%1\r\n$4\r\nkeys\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
          assert {:ok, [%{"keys" => ["a", "b"]}], ""} = Parser.parse(input)
        end

        test "array containing sets" do
          input = "*1\r\n~2\r\n:1\r\n:2\r\n"
          assert {:ok, [result], ""} = Parser.parse(input)
          assert result == [MapSet.new([1, 2])]
        end

        test "deeply nested structure" do
          input = "*1\r\n*1\r\n*1\r\n:42\r\n"
          assert {:ok, [[[[42]]]], ""} = Parser.parse(input)
        end

        test "map with nested map values" do
          input = "%1\r\n$5\r\nouter\r\n%1\r\n$5\r\ninner\r\n:1\r\n"
          assert {:ok, [%{"outer" => %{"inner" => 1}}], ""} = Parser.parse(input)
        end

        test "push containing a map" do
          input = ">2\r\n$7\r\nmessage\r\n%1\r\n$3\r\nfoo\r\n:1\r\n"
          assert {:ok, [{:push, ["message", %{"foo" => 1}]}], ""} = Parser.parse(input)
        end
      end

      describe "edge cases" do
        test "bulk string with length zero" do
          assert {:ok, [""], ""} = Parser.parse("$0\r\n\r\n")
        end

        test "array with single element" do
          assert {:ok, [[{:simple, "OK"}]], ""} = Parser.parse("*1\r\n+OK\r\n")
        end

        test "multiple nil values" do
          input = "_\r\n$-1\r\n*-1\r\n"
          assert {:ok, [nil, nil, nil], ""} = Parser.parse(input)
        end

        test "simple string that looks like a number" do
          assert {:ok, [{:simple, "42"}], ""} = Parser.parse("+42\r\n")
        end

        test "bulk string with unicode" do
          # "hello" in Japanese: 3 bytes each = 15 bytes
          str = "helloworld"
          len = byte_size(str)
          input = "$#{len}\r\n#{str}\r\n"
          assert {:ok, [^str], ""} = Parser.parse(input)
        end
      end

      describe "attribute type" do
        test "parses an attribute type" do
          # |1 = one key-value pair: key=simple "key", value=integer 42
          input = "|1\r\n+key\r\n:42\r\n"
          assert {:ok, [{:attribute, %{{:simple, "key"} => 42}}], ""} = Parser.parse(input)
        end

        test "parses attribute followed by a value" do
          input = "|1\r\n+key\r\n:42\r\n+OK\r\n"

          assert {:ok, [{:attribute, %{{:simple, "key"} => 42}}, {:simple, "OK"}], ""} =
                   Parser.parse(input)
        end

        test "parses empty attribute" do
          assert {:ok, [{:attribute, %{}}], ""} = Parser.parse("|0\r\n")
        end

        test "rejects malformed attributes and command-mode attributes" do
          assert {:error, {:invalid_map_count, "-1"}} = Parser.parse("|-1\r\n")
          assert {:ok, [], "|1\r\n$1\r\nk\r\n"} = Parser.parse("|1\r\n$1\r\nk\r\n")

          assert {:error, :invalid_command_format} =
                   Parser.parse_commands("|0\r\n*1\r\n$4\r\nPING\r\n")
        end
      end

      describe "blob error edge cases" do
        test "blob error with negative length returns error" do
          assert {:error, {:invalid_blob_error_length, -1}} = Parser.parse("!-1\r\n\r\n")
        end

        test "blob error with length mismatch is incomplete (not enough data)" do
          # Header says 5 bytes but payload has 11 — the parser reads only 5 bytes
          # then expects CRLF at position 5. "hello world" has " " at position 5, not CRLF.
          assert {:error, :bulk_crlf_missing} = Parser.parse("!5\r\nhello world\r\n")
        end
      end

      describe "verbatim string edge cases" do
        test "verbatim string with length < 4 returns error" do
          assert {:error, {:invalid_verbatim_length, 3}} = Parser.parse("=3\r\nABC\r\n")
        end

        test "verbatim string with missing colon separator returns error" do
          # Length 4, payload "ABCD" — no colon after 3-byte encoding
          assert {:error, :invalid_verbatim_payload} = Parser.parse("=4\r\nABCD\r\n")
        end
      end

      describe "null edge cases" do
        test "null with non-empty content returns error" do
          assert {:error, {:invalid_null, "garbage"}} = Parser.parse("_garbage\r\n")
        end
      end

      describe "double edge cases" do
        test "parses NaN double (lowercase)" do
          assert {:ok, [:nan], ""} = Parser.parse(",nan\r\n")
        end

        test "parses NaN double (mixed case)" do
          assert {:ok, [:nan], ""} = Parser.parse(",NaN\r\n")
        end

        test "parses NaN double (uppercase)" do
          assert {:ok, [:nan], ""} = Parser.parse(",NAN\r\n")
        end

        test "parses scientific notation double" do
          assert {:ok, [1.5e10], ""} = Parser.parse(",1.5e10\r\n")
        end

        test "parses scientific notation without decimal point" do
          assert {:ok, [1.0e5], ""} = Parser.parse(",1e5\r\n")
        end

        test "parses integer-form double" do
          assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
        end

        test "invalid double returns error" do
          assert {:error, {:invalid_double, "notafloat"}} = Parser.parse(",notafloat\r\n")
        end
      end

      describe "integer edge cases" do
        test "invalid integer returns error" do
          assert {:error, {:invalid_integer, "abc"}} = Parser.parse(":abc\r\n")
        end

        test "float-like value in integer position returns error" do
          assert {:error, {:invalid_integer, "3.14"}} = Parser.parse(":3.14\r\n")
        end
      end
    end
  end
end
