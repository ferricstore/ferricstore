defmodule Ferricstore.Store.CompoundKeyTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey

  # ---------------------------------------------------------------------------
  # Type metadata keys
  # ---------------------------------------------------------------------------

  describe "type_key/1" do
    test "builds type key with T: prefix" do
      assert "T:user:123" == CompoundKey.type_key("user:123")
    end

    test "works with empty key" do
      assert "T:" == CompoundKey.type_key("")
    end
  end

  describe "encode_type/1 and decode_type/1" do
    test "round-trips all types" do
      for type <- [:hash, :list, :set, :zset] do
        encoded = CompoundKey.encode_type(type)
        assert type == CompoundKey.decode_type(encoded)
      end
    end

    test "probabilistic type markers retain their Raft create token" do
      for type <- [:bloom, :cms, :cuckoo, :topk], token <- [-1, 0, 42] do
        marker = CompoundKey.encode_prob_type(type, token)

        assert CompoundKey.decode_type(marker) == type
        assert CompoundKey.decode_prob_type(marker) == {:ok, {type, token}}
        assert CompoundKey.type_name(marker) == CompoundKey.encode_type(type)
      end
    end

    test "encodes to expected strings" do
      assert "hash" == CompoundKey.encode_type(:hash)
      assert "list" == CompoundKey.encode_type(:list)
      assert "set" == CompoundKey.encode_type(:set)
      assert "zset" == CompoundKey.encode_type(:zset)
    end
  end

  # ---------------------------------------------------------------------------
  # Hash compound keys
  # ---------------------------------------------------------------------------

  describe "hash_field/2" do
    test "builds compound key with H: prefix and null separator" do
      key = CompoundKey.hash_field("user:123", "name")
      assert key == <<"H:user:123", 0, "name">>
    end

    test "works with empty field name" do
      key = CompoundKey.hash_field("key", "")
      assert key == <<"H:key", 0>>
    end

    test "does not alias a NUL in the logical key with a NUL in the field" do
      refute CompoundKey.hash_field(<<"{slot}a", 0, "b">>, "c") ==
               CompoundKey.hash_field("{slot}a", <<"b", 0, "c">>)
    end

    test "keeps escaped-looking logical keys distinct from binary logical keys" do
      refute CompoundKey.hash_field("key%00suffix", "field") ==
               CompoundKey.hash_field(<<"key", 0, "suffix">>, "field")
    end
  end

  describe "hash_prefix/1" do
    test "builds prefix ending with null byte" do
      prefix = CompoundKey.hash_prefix("user:123")
      assert prefix == <<"H:user:123", 0>>
    end

    test "hash_field starts with hash_prefix" do
      prefix = CompoundKey.hash_prefix("mykey")
      field = CompoundKey.hash_field("mykey", "field1")
      assert String.starts_with?(field, prefix)
    end
  end

  # ---------------------------------------------------------------------------
  # List compound keys
  # ---------------------------------------------------------------------------

  describe "list_element/2" do
    test "builds compound key with L: prefix" do
      key = CompoundKey.list_element("mylist", 1000.0)
      assert String.starts_with?(key, "L:mylist" <> <<0>>)
    end

    test "different positions produce different keys" do
      key1 = CompoundKey.list_element("l", 1.0)
      key2 = CompoundKey.list_element("l", 2.0)
      assert key1 != key2
    end
  end

  describe "encode_position/1 and decode_position/1" do
    test "round-trips positive values" do
      for pos <- [0.0, 1.0, 1000.0, 999_999.5, 1.0e-10] do
        encoded = CompoundKey.encode_position(pos)
        decoded = CompoundKey.decode_position(encoded)
        assert_in_delta pos, decoded, 1.0e-6, "Failed for position #{pos}"
      end
    end

    test "round-trips negative values" do
      for pos <- [-1.0, -1000.0, -0.5] do
        encoded = CompoundKey.encode_position(pos)
        decoded = CompoundKey.decode_position(encoded)
        assert_in_delta pos, decoded, 1.0e-6, "Failed for position #{pos}"
      end
    end

    test "lexicographic order matches numeric order for positive values" do
      a = CompoundKey.encode_position(1.0)
      b = CompoundKey.encode_position(2.0)
      c = CompoundKey.encode_position(1000.0)
      assert a < b
      assert b < c
    end

    test "lexicographic order matches numeric order for negative values" do
      a = CompoundKey.encode_position(-1000.0)
      b = CompoundKey.encode_position(-1.0)
      assert a < b
    end

    test "negative positions sort before positive positions" do
      neg = CompoundKey.encode_position(-1.0)
      pos = CompoundKey.encode_position(1.0)
      assert neg < pos
    end
  end

  # ---------------------------------------------------------------------------
  # Set compound keys
  # ---------------------------------------------------------------------------

  describe "set_member/2" do
    test "builds compound key with S: prefix" do
      key = CompoundKey.set_member("tags:post:789", "elixir")
      assert key == <<"S:tags:post:789", 0, "elixir">>
    end
  end

  describe "set_prefix/1" do
    test "builds prefix for set scanning" do
      prefix = CompoundKey.set_prefix("myset")
      assert prefix == <<"S:myset", 0>>
    end

    test "set_member starts with set_prefix" do
      prefix = CompoundKey.set_prefix("myset")
      member = CompoundKey.set_member("myset", "elem")
      assert String.starts_with?(member, prefix)
    end
  end

  # ---------------------------------------------------------------------------
  # Sorted set compound keys
  # ---------------------------------------------------------------------------

  describe "zset_member/2" do
    test "builds compound key with Z: prefix" do
      key = CompoundKey.zset_member("leaderboard", "alice")
      assert key == <<"Z:leaderboard", 0, "alice">>
    end
  end

  describe "zset_prefix/1" do
    test "builds prefix for sorted set scanning" do
      prefix = CompoundKey.zset_prefix("lb")
      assert prefix == <<"Z:lb", 0>>
    end
  end

  # ---------------------------------------------------------------------------
  # Extract subkey
  # ---------------------------------------------------------------------------

  describe "extract_subkey/2" do
    test "extracts field from hash compound key" do
      prefix = CompoundKey.hash_prefix("user:123")
      compound = CompoundKey.hash_field("user:123", "name")
      assert "name" == CompoundKey.extract_subkey(compound, prefix)
    end

    test "extracts member from set compound key" do
      prefix = CompoundKey.set_prefix("myset")
      compound = CompoundKey.set_member("myset", "elem")
      assert "elem" == CompoundKey.extract_subkey(compound, prefix)
    end
  end

  describe "extract_redis_key/1" do
    test "extracts parent key from compound data and metadata keys" do
      key = "ns:user:123"

      assert key == CompoundKey.extract_redis_key(CompoundKey.hash_field(key, "name"))
      assert key == CompoundKey.extract_redis_key(CompoundKey.list_element(key, 1.0))
      assert key == CompoundKey.extract_redis_key(CompoundKey.set_member(key, "tag"))
      assert key == CompoundKey.extract_redis_key(CompoundKey.zset_member(key, "member"))
      assert key == CompoundKey.extract_redis_key(CompoundKey.type_key(key))
      assert key == CompoundKey.extract_redis_key(CompoundKey.list_meta_key(key))
      assert key == CompoundKey.extract_redis_key("PM:" <> key)
    end

    test "round-trips binary logical keys through every physical-key family" do
      key = <<"percent%", 0, 255, "key">>

      physical_keys = [
        CompoundKey.hash_field(key, <<0, "%field">>),
        CompoundKey.list_element(key, 1),
        CompoundKey.set_member(key, <<0, "%member">>),
        CompoundKey.zset_member(key, <<0, "%member">>),
        CompoundKey.stream_prefix(key) <> "1-0",
        CompoundKey.stream_group(key, <<0, "%group">>),
        CompoundKey.type_key(key),
        CompoundKey.list_meta_key(key),
        CompoundKey.stream_meta_key(key),
        CompoundKey.promotion_marker_key(key)
      ]

      assert Enum.all?(physical_keys, &(CompoundKey.extract_redis_key(&1) == key))
    end

    test "physical prefixes contain exactly one delimiter before the subkey" do
      key = <<"a", 0, "b%00c">>

      for prefix <- [
            CompoundKey.hash_prefix(key),
            CompoundKey.list_prefix(key),
            CompoundKey.set_prefix(key),
            CompoundKey.zset_prefix(key),
            CompoundKey.stream_prefix(key),
            CompoundKey.stream_group_prefix(key)
          ] do
        assert [_encoded_key, ""] = :binary.split(prefix, <<0>>, [:global])
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Internal key detection
  # ---------------------------------------------------------------------------

  describe "internal_key?/1" do
    test "identifies hash compound keys" do
      assert CompoundKey.internal_key?(<<"H:foo", 0, "bar">>)
    end

    test "identifies list compound keys" do
      assert CompoundKey.internal_key?(<<"L:foo", 0, "bar">>)
    end

    test "identifies set compound keys" do
      assert CompoundKey.internal_key?(<<"S:foo", 0, "bar">>)
    end

    test "identifies sorted set compound keys" do
      assert CompoundKey.internal_key?(<<"Z:foo", 0, "bar">>)
    end

    test "identifies stream consumer group metadata keys" do
      group_key = CompoundKey.stream_group("events", "workers")

      assert CompoundKey.internal_key?(group_key)
      assert ["events"] == CompoundKey.user_visible_keys(["events", group_key])
    end

    test "identifies type metadata keys" do
      assert CompoundKey.internal_key?("T:mykey")
    end

    test "rejects plain user keys" do
      refute CompoundKey.internal_key?("mykey")
      refute CompoundKey.internal_key?("user:123")
      refute CompoundKey.internal_key?("")
    end

    test "rejects keys that happen to start with H but not H:" do
      refute CompoundKey.internal_key?("Hello")
      refute CompoundKey.internal_key?("Hashed")
    end
  end
end
