defmodule Ferricstore.Store.CompoundKey do
  @moduledoc """
  Encodes and decodes compound keys for data structure storage in Bitcask.

  FerricStore stores data structure sub-elements (hash fields, list positions,
  set members, sorted set entries) as individual Bitcask entries using compound
  keys. This avoids the need for per-key Bitcask instances while still allowing
  individual field/member access without reading entire structures.

  ## Key Format

  Each compound key has the format:

      PREFIX:redis_key\\0sub_key

  Where:

    * `PREFIX` is a single uppercase letter identifying the data type:
      - `H` for Hash fields
      - `L` for List elements (sub_key is a float64 position)
      - `S` for Set members
      - `Z` for Sorted Set entries
      - `T` for Type metadata (no sub_key)

    * `redis_key` is the user-facing Redis key name

    * `\\0` (null byte) is the separator between the Redis key and sub-key

    * `sub_key` is the type-specific sub-key (field name, position, member, etc.)

  The null byte separator is safe because Redis keys in normal use do not
  contain null bytes (and the wire protocol does not transmit them within
  key strings in standard client libraries).

  ## Type Metadata

  A type index entry `T:keyname` stores the type as a simple string value
  (`"hash"`, `"list"`, `"set"`, `"zset"`, `"stream"`). This is used by the TYPE command
  and for WRONGTYPE enforcement -- attempting to use hash commands on a set
  key returns an error.
  """

  @separator <<0>>

  alias Ferricstore.Flow.InternalKey

  @type data_type :: :hash | :list | :set | :zset | :stream

  # -------------------------------------------------------------------
  # Type metadata keys
  # -------------------------------------------------------------------

  @doc """
  Builds the type metadata compound key for a Redis key.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.type_key("user:123")
      "T:user:123"

  """
  @spec type_key(binary()) :: binary()
  def type_key(redis_key), do: "T:" <> redis_key

  @doc """
  Encodes a data type atom to its stored string representation.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.encode_type(:hash)
      "hash"

  """
  @spec encode_type(data_type()) :: binary()
  def encode_type(:hash), do: "hash"
  def encode_type(:list), do: "list"
  def encode_type(:set), do: "set"
  def encode_type(:zset), do: "zset"
  def encode_type(:stream), do: "stream"

  @doc """
  Decodes a stored type string back to the data type atom.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.decode_type("hash")
      :hash

  """
  @spec decode_type(binary()) :: data_type()
  def decode_type("hash"), do: :hash
  def decode_type("list"), do: :list
  def decode_type("set"), do: :set
  def decode_type("zset"), do: :zset
  def decode_type("stream"), do: :stream

  # -------------------------------------------------------------------
  # Hash compound keys
  # -------------------------------------------------------------------

  @doc """
  Builds a compound key for a hash field.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.hash_field("user:123", "name")
      <<"H:user:123", 0, "name">>

  """
  @spec hash_field(binary(), binary()) :: binary()
  def hash_field(redis_key, field) do
    "H:" <> redis_key <> @separator <> field
  end

  @doc """
  Returns the scan prefix for all fields of a hash.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.hash_prefix("user:123")
      <<"H:user:123", 0>>

  """
  @spec hash_prefix(binary()) :: binary()
  def hash_prefix(redis_key) do
    "H:" <> redis_key <> @separator
  end

  # -------------------------------------------------------------------
  # List compound keys
  # -------------------------------------------------------------------

  @doc """
  Builds the metadata compound key for a list.
  """
  @spec list_meta_key(binary()) :: binary()
  def list_meta_key(redis_key), do: "LM:" <> redis_key

  @doc """
  Builds a compound key for a list element at a given float64 position.

  The position is encoded as a fixed-width, zero-padded float string to
  ensure lexicographic ordering matches numeric ordering. Format is
  `{sign}{integer_part}.{fractional_part}` with 20 digits before and 17
  digits after the decimal point.

  ## Examples

      iex> key = Ferricstore.Store.CompoundKey.list_element("mylist", 1000.0)
      iex> String.starts_with?(key, "L:mylist" <> <<0>>)
      true

  """
  @spec list_element(binary(), float()) :: binary()
  def list_element(redis_key, position) when is_number(position) do
    "L:" <> redis_key <> @separator <> encode_position(position)
  end

  @doc """
  Returns the scan prefix for all elements of a list.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.list_prefix("mylist")
      <<"L:mylist", 0>>

  """
  @spec list_prefix(binary()) :: binary()
  def list_prefix(redis_key) do
    "L:" <> redis_key <> @separator
  end

  @doc """
  Encodes a float64 position as a fixed-width string that sorts
  lexicographically in the same order as the numeric values.

  Negative positions use a complement encoding so that lexicographic
  order is preserved.

  ## Examples

      iex> a = Ferricstore.Store.CompoundKey.encode_position(1.0)
      iex> b = Ferricstore.Store.CompoundKey.encode_position(2.0)
      iex> a < b
      true

  """
  @spec encode_position(number()) :: binary()
  def encode_position(position) when is_integer(position) and position >= 0 do
    "P" <>
      String.pad_leading(Integer.to_string(position), 20, "0") <>
      ".00000000000000000"
  end

  def encode_position(position) when is_integer(position) and position < 0 do
    comp_int = 99_999_999_999_999_999_999 - abs(position)

    "N" <>
      String.pad_leading(Integer.to_string(comp_int), 20, "0") <>
      ".99999999999999999"
  end

  def encode_position(position) when is_float(position) and position >= 0.0 do
    # Positive: "P" prefix + zero-padded representation
    int_part = trunc(position)
    frac_part = position - int_part
    frac_digits = trunc(frac_part * 100_000_000_000_000_000)

    "P" <>
      String.pad_leading(Integer.to_string(int_part), 20, "0") <>
      "." <> String.pad_leading(Integer.to_string(frac_digits), 17, "0")
  end

  def encode_position(position) when is_float(position) and position < 0.0 do
    # Negative: "N" prefix + complement encoding for correct sort order
    # We complement by subtracting from a large number so that
    # more negative numbers sort first (smaller complement = more negative original)
    abs_pos = abs(position)
    int_part = trunc(abs_pos)
    frac_part = abs_pos - int_part
    frac_digits = trunc(frac_part * 100_000_000_000_000_000)

    # Complement: larger negative -> smaller complement string
    comp_int = 99_999_999_999_999_999_999 - int_part
    comp_frac = 99_999_999_999_999_999 - frac_digits

    "N" <>
      String.pad_leading(Integer.to_string(comp_int), 20, "0") <>
      "." <> String.pad_leading(Integer.to_string(comp_frac), 17, "0")
  end

  @doc """
  Decodes a position string back to a float64.

  ## Examples

      iex> pos = 1000.5
      iex> encoded = Ferricstore.Store.CompoundKey.encode_position(pos)
      iex> Ferricstore.Store.CompoundKey.decode_position(encoded)
      1000.5

  """
  @spec decode_position(binary()) :: number()
  def decode_position("P" <> rest) do
    [int_str, frac_str] = String.split(rest, ".", parts: 2)
    int_part = String.to_integer(int_str)
    frac_part = String.to_integer(frac_str)
    if frac_part == 0, do: int_part, else: int_part + frac_part / 100_000_000_000_000_000
  end

  def decode_position("N" <> rest) do
    [int_str, frac_str] = String.split(rest, ".", parts: 2)
    comp_int = String.to_integer(int_str)
    comp_frac = String.to_integer(frac_str)
    int_part = 99_999_999_999_999_999_999 - comp_int
    frac_part = 99_999_999_999_999_999 - comp_frac
    if frac_part == 0, do: -int_part, else: -(int_part + frac_part / 100_000_000_000_000_000)
  end

  # -------------------------------------------------------------------
  # Set compound keys
  # -------------------------------------------------------------------

  @doc """
  Builds a compound key for a set member.

  The member name IS the sub-key; the value stored is `"1"` (presence marker).

  ## Examples

      iex> Ferricstore.Store.CompoundKey.set_member("tags:post:789", "elixir")
      <<"S:tags:post:789", 0, "elixir">>

  """
  @spec set_member(binary(), binary()) :: binary()
  def set_member(redis_key, member) do
    "S:" <> redis_key <> @separator <> member
  end

  @doc """
  Returns the scan prefix for all members of a set.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.set_prefix("tags:post:789")
      <<"S:tags:post:789", 0>>

  """
  @spec set_prefix(binary()) :: binary()
  def set_prefix(redis_key) do
    "S:" <> redis_key <> @separator
  end

  # -------------------------------------------------------------------
  # Sorted Set compound keys
  # -------------------------------------------------------------------

  @doc """
  Builds a compound key for a sorted set member-to-score mapping.

  Stores `member -> score` for O(1) score lookups by member.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.zset_member("leaderboard", "alice")
      <<"Z:leaderboard", 0, "alice">>

  """
  @spec zset_member(binary(), binary()) :: binary()
  def zset_member(redis_key, member) do
    "Z:" <> redis_key <> @separator <> member
  end

  @doc """
  Returns the scan prefix for all members of a sorted set.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.zset_prefix("leaderboard")
      <<"Z:leaderboard", 0>>

  """
  @spec zset_prefix(binary()) :: binary()
  def zset_prefix(redis_key) do
    "Z:" <> redis_key <> @separator
  end

  @doc """
  Returns the scan prefix for all entries of a stream.
  """
  @spec stream_prefix(binary()) :: binary()
  def stream_prefix(redis_key) do
    "X:" <> redis_key <> @separator
  end

  @doc """
  Builds the durable metadata key for a stream's length and last-generated-id.
  """
  @spec stream_meta_key(binary()) :: binary()
  def stream_meta_key(redis_key), do: "XM:" <> redis_key

  @doc """
  Builds the durable consumer-group metadata key for a stream group.
  """
  @spec stream_group(binary(), binary()) :: binary()
  def stream_group(redis_key, group) do
    "XG:" <> redis_key <> @separator <> group
  end

  @doc """
  Returns the scan prefix for all durable consumer-group metadata of a stream.
  """
  @spec stream_group_prefix(binary()) :: binary()
  def stream_group_prefix(redis_key) do
    "XG:" <> redis_key <> @separator
  end

  # -------------------------------------------------------------------
  # Extraction helpers
  # -------------------------------------------------------------------

  @doc """
  Extracts the sub-key portion from a compound key given a known prefix.

  ## Examples

      iex> prefix = Ferricstore.Store.CompoundKey.hash_prefix("user:123")
      iex> compound = Ferricstore.Store.CompoundKey.hash_field("user:123", "name")
      iex> Ferricstore.Store.CompoundKey.extract_subkey(compound, prefix)
      "name"

  """
  @spec extract_subkey(binary(), binary()) :: binary()
  def extract_subkey(compound_key, prefix) do
    prefix_len = byte_size(prefix)
    binary_part(compound_key, prefix_len, byte_size(compound_key) - prefix_len)
  end

  @doc """
  Returns true if the given Bitcask key is an internal compound key
  (starts with a known type prefix like `H:`, `L:`, `S:`, `Z:`, `T:`).

  Used to filter internal keys out of user-facing KEYS and DBSIZE results.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.internal_key?("H:user:123" <> <<0>> <> "name")
      true

      iex> Ferricstore.Store.CompoundKey.internal_key?("my_plain_key")
      false

  """
  @spec internal_key?(binary()) :: boolean()
  def internal_key?(<<"H:", _rest::binary>>), do: true
  def internal_key?(<<"L:", _rest::binary>>), do: true
  def internal_key?(<<"S:", _rest::binary>>), do: true
  def internal_key?(<<"Z:", _rest::binary>>), do: true
  def internal_key?(<<"X:", _rest::binary>>), do: true
  def internal_key?(<<"XM:", _rest::binary>>), do: true
  def internal_key?(<<"XG:", _rest::binary>>), do: true
  def internal_key?(<<"T:", _rest::binary>>), do: true
  def internal_key?(<<"V:", _rest::binary>>), do: true
  def internal_key?(<<"VM:", _rest::binary>>), do: true
  def internal_key?(<<"PM:", _rest::binary>>), do: true
  def internal_key?(<<"LM:", _rest::binary>>), do: true
  def internal_key?(_), do: false

  @doc """
  Extracts the Redis-level key from a compound key.

  For compound keys like `S:myset\\0member`, returns `"myset"`.
  For type metadata keys like `T:myset`, returns `"myset"`.
  For plain (non-compound) keys, returns the key as-is.

  Used by the cross-shard lock checking logic to determine which Redis key
  a compound Raft write affects.

  ## Examples

      iex> Ferricstore.Store.CompoundKey.extract_redis_key("S:myset" <> <<0>> <> "member")
      "myset"

      iex> Ferricstore.Store.CompoundKey.extract_redis_key("H:hash" <> <<0>> <> "field")
      "hash"

      iex> Ferricstore.Store.CompoundKey.extract_redis_key("T:mykey")
      "mykey"

      iex> Ferricstore.Store.CompoundKey.extract_redis_key("plain_key")
      "plain_key"

  """
  @spec extract_redis_key(binary()) :: binary()
  def extract_redis_key(<<"H:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(<<"L:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(<<"S:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(<<"Z:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(<<"X:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(<<"XM:", rest::binary>>), do: rest
  def extract_redis_key(<<"XG:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(<<"T:", rest::binary>>), do: rest
  def extract_redis_key(<<"LM:", rest::binary>>), do: rest
  def extract_redis_key(<<"VM:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(<<"PM:", rest::binary>>), do: rest
  def extract_redis_key(<<"V:", rest::binary>>), do: extract_before_separator(rest)
  def extract_redis_key(key), do: key

  defp extract_before_separator(rest) do
    case :binary.split(rest, @separator) do
      [redis_key, _sub_key] -> redis_key
      [redis_key] -> redis_key
    end
  end

  @doc """
  Filters a list of raw ETS/store keys to only user-visible logical keys.

  - Rejects all internal compound keys (`H:`, `L:`, `S:`, `Z:`, `V:`, `VM:`, `PM:`, `LM:`)
  - Extracts logical keys from type registry entries (`T:key` -> `key`)
  - Keeps plain string keys as-is
  - Deduplicates (a key may exist both as a plain entry and a `T:` entry)
  """
  @spec user_visible_keys([binary()]) :: [binary()]
  def user_visible_keys(raw_keys) do
    raw_keys
    |> storage_logical_keys()
    |> Enum.reject(&InternalKey.internal?/1)
  end

  @doc false
  @spec storage_logical_keys([binary()]) :: [binary()]
  def storage_logical_keys(raw_keys) do
    {type_keys, other_keys} =
      Enum.reduce(raw_keys, {[], []}, fn key, {tk, ok} ->
        case key do
          <<"T:", logical::binary>> -> {[logical | tk], ok}
          _ -> {tk, [key | ok]}
        end
      end)

    plain_keys = Enum.reject(other_keys, &internal_key?/1)

    case type_keys do
      [] -> plain_keys
      _ -> Enum.uniq(type_keys ++ plain_keys)
    end
  end
end
