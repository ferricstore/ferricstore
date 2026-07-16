defmodule FerricstoreServer.Acl.Password do
  @moduledoc false

  @algorithm "pbkdf2-sha256"
  @version_prefix @algorithm <> "$"
  @pbkdf2_iterations 100_000
  @pbkdf2_key_length 32
  @salt_length 16
  @max_password_bytes 4_096
  @dummy_hash String.duplicate("A", 64)

  @doc false
  @spec max_password_bytes() :: pos_integer()
  def max_password_bytes, do: @max_password_bytes

  @doc false
  @spec dummy_hash() :: binary()
  def dummy_hash, do: @dummy_hash

  @spec hash(binary()) :: binary()
  def hash(password) do
    salt = :crypto.strong_rand_bytes(@salt_length)
    digest = pbkdf2(password, salt)

    Enum.join(
      [
        @algorithm,
        Integer.to_string(@pbkdf2_iterations),
        Base.url_encode64(salt, padding: false),
        Base.url_encode64(digest, padding: false)
      ],
      "$"
    )
  end

  @spec verify(binary(), binary()) :: boolean()
  def verify(password, stored_hash) do
    case decode_hash(stored_hash) do
      {:ok, salt, expected_digest} ->
        :crypto.hash_equals(pbkdf2(password, salt), expected_digest)

      :error ->
        false
    end
  end

  @spec valid_stored_hash_format?(binary()) :: boolean()
  def valid_stored_hash_format?(stored_hash), do: match?({:ok, _, _}, decode_hash(stored_hash))

  @spec needs_rehash?(binary()) :: boolean()
  def needs_rehash?(@version_prefix <> _stored_hash), do: false
  def needs_rehash?(stored_hash), do: match?({:ok, _, _}, decode_unversioned_pbkdf2(stored_hash))

  @spec hash_for_display(binary()) :: binary()
  def hash_for_display(stored_hash) do
    :crypto.hash(:sha256, stored_hash) |> Base.encode16(case: :lower)
  end

  defp decode_hash(@version_prefix <> _ = stored_hash), do: decode_versioned_pbkdf2(stored_hash)
  defp decode_hash(stored_hash), do: decode_unversioned_pbkdf2(stored_hash)

  defp decode_versioned_pbkdf2(stored_hash) do
    with [@algorithm, iterations, encoded_salt, encoded_digest] <- String.split(stored_hash, "$"),
         {parsed_iterations, ""} <- Integer.parse(iterations),
         true <- parsed_iterations == @pbkdf2_iterations,
         {:ok, salt} <- Base.url_decode64(encoded_salt, padding: false),
         {:ok, digest} <- Base.url_decode64(encoded_digest, padding: false),
         true <- byte_size(salt) == @salt_length,
         true <- byte_size(digest) == @pbkdf2_key_length do
      {:ok, salt, digest}
    else
      _ -> :error
    end
  end

  defp decode_unversioned_pbkdf2(stored_hash) do
    case Base.decode64(stored_hash) do
      {:ok, <<salt::binary-size(@salt_length), digest::binary-size(@pbkdf2_key_length)>>} ->
        {:ok, salt, digest}

      _ ->
        :error
    end
  end

  defp pbkdf2(password, salt) do
    :crypto.pbkdf2_hmac(:sha256, password, salt, @pbkdf2_iterations, @pbkdf2_key_length)
  end
end
