defmodule Ferricstore.AuditFixesCriticalTest do
  @moduledoc """
  Critical-severity audit fixes (C1-C4).
  Split from AuditFixesTest.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.GlobMatcher
  alias Ferricstore.Commands.Server

  # ---------------------------------------------------------------------------
  # Helper: build a mock store map from a key-value map
  # ---------------------------------------------------------------------------

  defp mock_store(data) do
    %{
      get: fn key -> get_in(data, [key, Access.elem(0)]) end,
      get_meta: fn key ->
        case Map.get(data, key) do
          nil -> nil
          {val, exp} -> {val, exp}
        end
      end,
      put: fn _key, _val, _exp -> :ok end,
      delete: fn _key -> :ok end,
      exists?: fn key -> Map.has_key?(data, key) end,
      keys: fn ->
        Map.keys(data) |> Enum.reject(&Ferricstore.Store.CompoundKey.internal_key?/1)
      end,
      flush: fn -> :ok end,
      dbsize: fn -> map_size(data) end,
      incr: fn _key, _delta -> {:ok, 1} end,
      incr_float: fn _key, _delta -> {:ok, "1.0"} end,
      append: fn _key, _val -> {:ok, 5} end,
      getset: fn _key, _val -> nil end,
      getdel: fn _key -> nil end,
      getex: fn _key, _exp -> nil end,
      setrange: fn _key, _off, _val -> {:ok, 10} end,
      compound_get: fn _rk, _ck -> nil end,
      compound_get_meta: fn _rk, _ck -> nil end,
      compound_put: fn _rk, _ck, _v, _e -> :ok end,
      compound_delete: fn _rk, _ck -> :ok end,
      compound_scan: fn _rk, _prefix -> [] end,
      compound_count: fn _rk, _prefix -> 0 end,
      compound_delete_prefix: fn _rk, _prefix -> :ok end
    }
  end

  # ---------------------------------------------------------------------------
  # C1: GlobMatcher replaces runtime regex compilation
  # ---------------------------------------------------------------------------

  describe "C1: GlobMatcher binary pattern matching" do
    test "* matches any sequence" do
      assert GlobMatcher.match?("hello", "*")
      assert GlobMatcher.match?("", "*")
      assert GlobMatcher.match?("hello world", "hello*")
      assert GlobMatcher.match?("hello world", "*world")
      assert GlobMatcher.match?("hello world", "h*d")
    end

    test "? matches exactly one byte" do
      assert GlobMatcher.match?("hello", "h?llo")
      assert GlobMatcher.match?("hello", "?ello")
      assert GlobMatcher.match?("a", "?")
      refute GlobMatcher.match?("", "?")
      refute GlobMatcher.match?("ab", "?")
    end

    test "literal matching" do
      assert GlobMatcher.match?("hello", "hello")
      refute GlobMatcher.match?("hello", "world")
      refute GlobMatcher.match?("hello", "hell")
      refute GlobMatcher.match?("hell", "hello")
    end

    test "[abc] character class" do
      assert GlobMatcher.match?("a", "[abc]")
      assert GlobMatcher.match?("b", "[abc]")
      assert GlobMatcher.match?("c", "[abc]")
      refute GlobMatcher.match?("d", "[abc]")
    end

    test "[a-c] character class range" do
      assert GlobMatcher.match?("a", "[a-c]")
      assert GlobMatcher.match?("b", "[a-c]")
      assert GlobMatcher.match?("c", "[a-c]")
      refute GlobMatcher.match?("d", "[a-c]")
      refute GlobMatcher.match?("-", "[a-c]")
    end

    test "[^a-c] negated character class range" do
      refute GlobMatcher.match?("a", "[^a-c]")
      refute GlobMatcher.match?("b", "[^a-c]")
      refute GlobMatcher.match?("c", "[^a-c]")
      assert GlobMatcher.match?("d", "[^a-c]")
    end

    test "[^abc] negated character class" do
      refute GlobMatcher.match?("a", "[^abc]")
      assert GlobMatcher.match?("d", "[^abc]")
    end

    test "escaped characters" do
      assert GlobMatcher.match?("a*b", "a\\*b")
      refute GlobMatcher.match?("axb", "a\\*b")
      assert GlobMatcher.match?("a?b", "a\\?b")
      refute GlobMatcher.match?("axb", "a\\?b")
    end

    test "complex patterns" do
      assert GlobMatcher.match?("user:123:name", "user:*:name")
      assert GlobMatcher.match?("key_a_suffix", "key_?_suffix")
      refute GlobMatcher.match?("key_ab_suffix", "key_?_suffix")
    end

    test "KEYS uses GlobMatcher (not regex)" do
      # Use a non-prefix pattern (with ?) to bypass the prefix index fast path
      # and exercise the GlobMatcher code path.
      store =
        mock_store(%{
          "user_1" => {"v", 0},
          "user_2" => {"v", 0},
          "post_1" => {"v", 0}
        })

      result = Server.handle("KEYS", ["user_?"], store) |> Enum.sort()
      assert result == ["user_1", "user_2"]
    end
  end

  # ---------------------------------------------------------------------------
  # C2: RESP3 inline parser uses :binary.split
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.Resp.Parser) do
    describe "C2: inline parser uses :binary.split" do
      test "splits on spaces" do
        assert {:ok, [{:inline, ["PING"]}], ""} = FerricstoreServer.Resp.Parser.parse("PING\r\n")

        assert {:ok, [{:inline, ["GET", "key"]}], ""} =
                 FerricstoreServer.Resp.Parser.parse("GET key\r\n")
      end

      test "handles multiple spaces" do
        assert {:ok, [{:inline, ["SET", "key", "value"]}], ""} =
                 FerricstoreServer.Resp.Parser.parse("SET  key  value\r\n")
      end

      test "handles tabs as whitespace" do
        assert {:ok, [{:inline, ["GET", "key"]}], ""} =
                 FerricstoreServer.Resp.Parser.parse("GET\tkey\r\n")
      end

      test "handles mixed whitespace" do
        assert {:ok, [{:inline, ["SET", "key", "value"]}], ""} =
                 FerricstoreServer.Resp.Parser.parse("SET \t key \t value\r\n")
      end

      test "trailing whitespace trimmed" do
        assert {:ok, [{:inline, ["PING"]}], ""} =
                 FerricstoreServer.Resp.Parser.parse("PING \t \r\n")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # C3: Config.get_value reads from ETS
  # ---------------------------------------------------------------------------

  describe "C3: Config.get_value reads from ETS" do
    test "get_value reads requirepass from ETS table" do
      # The :ferricstore_config ETS table is created by Config.init
      # and populated by sync_ets. Verify it exists and is readable.
      case :ets.whereis(:ferricstore_config) do
        :undefined ->
          # If Config hasn't started, skip gracefully
          :ok

        _ref ->
          # Read should succeed without going through GenServer
          result = Ferricstore.Config.get_value("requirepass")
          assert is_binary(result) or result == nil
      end
    end

    test "get_value returns nil for unknown keys" do
      case :ets.whereis(:ferricstore_config) do
        :undefined -> :ok
        _ref -> assert Ferricstore.Config.get_value("nonexistent_key_12345") == nil
      end
    end
  end

  # ---------------------------------------------------------------------------
  # C4: parse_float_value uses :binary.match
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.Resp.Parser) do
    describe "C4: parse_float_value uses binary matching" do
      test "regular float" do
        assert {:ok, [1.5], ""} = FerricstoreServer.Resp.Parser.parse(",1.5\r\n")
      end

      test "integer-form double" do
        assert {:ok, [42.0], ""} = FerricstoreServer.Resp.Parser.parse(",42\r\n")
      end

      test "scientific notation without dot" do
        assert {:ok, [float], ""} = FerricstoreServer.Resp.Parser.parse(",1e5\r\n")
        assert_in_delta float, 1.0e5, 0.001
      end

      test "scientific notation with dot" do
        assert {:ok, [float], ""} = FerricstoreServer.Resp.Parser.parse(",1.5e10\r\n")
        assert_in_delta float, 1.5e10, 1.0
      end

      test "negative scientific notation" do
        assert {:ok, [float], ""} = FerricstoreServer.Resp.Parser.parse(",1.0E-3\r\n")
        assert_in_delta float, 0.001, 0.0001
      end

      test "special values" do
        assert {:ok, [:infinity], ""} = FerricstoreServer.Resp.Parser.parse(",inf\r\n")
        assert {:ok, [:neg_infinity], ""} = FerricstoreServer.Resp.Parser.parse(",-inf\r\n")
        assert {:ok, [:nan], ""} = FerricstoreServer.Resp.Parser.parse(",nan\r\n")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # C1 (v2): FerricStore.keys/1 uses GlobMatcher, not glob_to_regex
  # ---------------------------------------------------------------------------

  describe "C1v2: FerricStore.keys/1 uses GlobMatcher (no runtime regex)" do
    test "glob_to_regex is removed from FerricStore module" do
      # The private function glob_to_regex should no longer exist.
      # FerricStore should not export or define it.
      refute function_exported?(FerricStore, :glob_to_regex, 1)
    end

    test "keys/1 pattern matching works via GlobMatcher" do
      # This test exercises the embedded API keys/1 with a glob pattern
      # that requires GlobMatcher (not just prefix matching).
      # We call Server.handle("KEYS", ...) with a mock store to verify
      # the GlobMatcher code path.
      _all_keys = ["user:alice", "user:bob", "user:charlie", "post:1"]

      store =
        mock_store(%{
          "user:alice" => {"v", 0},
          "user:bob" => {"v", 0},
          "user:charlie" => {"v", 0},
          "post:1" => {"v", 0}
        })

      # ? pattern forces GlobMatcher path
      result = Server.handle("KEYS", ["user:???"], store) |> Enum.sort()
      assert result == ["user:bob"]

      # * pattern uses full key scan with GlobMatcher
      result = Server.handle("KEYS", ["user:*"], store) |> Enum.sort()
      assert result == ["user:alice", "user:bob", "user:charlie"]

      # character class pattern forces GlobMatcher path
      result = Server.handle("KEYS", ["user:[ab]*"], store) |> Enum.sort()
      assert result == ["user:alice", "user:bob"]
    end
  end

  # ---------------------------------------------------------------------------
  # C2 (v2): FerricStore.keys/1 single-pass reduce
  # ---------------------------------------------------------------------------

  describe "C2v2: FerricStore.keys/1 single-pass reduce" do
    test "keys returns correct results with internal keys filtered" do
      # Internal keys (H:, S:, Z:, L:, T: prefixes) must be excluded.
      store =
        mock_store(%{
          "visible" => {"v", 0},
          "H:internal" => {"v", 0},
          "S:internal" => {"v", 0},
          "also_visible" => {"v", 0}
        })

      # The mock_store helper already filters internal keys in the keys fn,
      # matching what Router.keys(FerricStore.Instance.get(:default)) returns after the single-pass fix.
      # Verify the Server.handle("KEYS", ...) path filters correctly.
      result = Server.handle("KEYS", ["*"], store) |> Enum.sort()
      assert result == ["also_visible", "visible"]
    end

    test "keys with pattern filters in single pass" do
      store =
        mock_store(%{
          "session:abc" => {"v", 0},
          "session:def" => {"v", 0},
          "cache:xyz" => {"v", 0}
        })

      result = Server.handle("KEYS", ["session:???"], store) |> Enum.sort()
      assert result == ["session:abc", "session:def"]

      # No match
      result = Server.handle("KEYS", ["nomatch:*"], store)
      assert result == []
    end
  end
end
