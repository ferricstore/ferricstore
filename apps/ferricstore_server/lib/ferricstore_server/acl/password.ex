defmodule FerricstoreServer.Acl.Password do
  @moduledoc false

  @pbkdf2_iterations 100_000
  @pbkdf2_key_length 32
  @dummy_hash String.duplicate("A", 64)

  @doc false
  @spec dummy_hash() :: binary()
  def dummy_hash, do: @dummy_hash

  @spec hash(binary()) :: binary()
  def hash(password) do
    salt = :crypto.strong_rand_bytes(16)
    hash = :crypto.pbkdf2_hmac(:sha256, password, salt, @pbkdf2_iterations, @pbkdf2_key_length)
    Base.encode64(salt <> hash)
  end

  @spec verify(binary(), binary()) :: boolean()
  def verify(password, stored_hash) do
    case Base.decode64(stored_hash) do
      {:ok, <<salt::binary-16, hash::binary-32>>} ->
        pbkdf2 =
          :crypto.pbkdf2_hmac(:sha256, password, salt, @pbkdf2_iterations, @pbkdf2_key_length)

        if :crypto.hash_equals(pbkdf2, hash) do
          true
        else
          legacy = :crypto.hash(:sha256, salt <> password)
          :crypto.hash_equals(legacy, hash)
        end

      _ ->
        false
    end
  end

  @spec hash_for_display(binary()) :: binary()
  def hash_for_display(stored_hash) do
    :crypto.hash(:sha256, stored_hash) |> Base.encode16(case: :lower)
  end
end
