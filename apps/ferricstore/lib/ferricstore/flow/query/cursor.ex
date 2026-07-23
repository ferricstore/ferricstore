defmodule Ferricstore.Flow.Query.Cursor do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Limits, Request}
  alias Ferricstore.TermCodec

  @prefix "fqc1_"
  @aad "ferric.flow.query.cursor/v1"
  @nonce_bytes 12
  @tag_bytes 16
  @key_bytes 32
  @default_ttl_ms 5 * 60 * 1_000
  @maximum_ttl_ms 24 * 60 * 60 * 1_000
  @maximum_continuation_bytes 2_768
  @minimum_cursor_bytes Limits.min_cursor_bytes()
  @maximum_cursor_bytes Limits.max_cursor_bytes()

  defmodule Claim do
    @moduledoc false

    @enforce_keys [
      :request_digest,
      :index_id,
      :index_version,
      :index_build_id,
      :continuation,
      :token_digest
    ]
    defstruct @enforce_keys

    @opaque t :: %__MODULE__{
              request_digest: <<_::256>>,
              index_id: binary(),
              index_version: pos_integer(),
              index_build_id: binary(),
              continuation: binary(),
              token_digest: <<_::256>>
            }
  end

  @type request_binding :: %{
          required(:instance) => atom() | binary(),
          required(:scope) => binary(),
          required(:query_fingerprint) => binary(),
          required(:query_digest) => <<_::256>>,
          required(:order_by) => [{term(), :asc | :desc}]
        }

  @type binding :: %{
          required(:instance) => atom() | binary(),
          required(:scope) => binary(),
          required(:query_fingerprint) => binary(),
          required(:query_digest) => <<_::256>>,
          required(:index_id) => binary(),
          required(:index_version) => pos_integer(),
          required(:index_build_id) => binary(),
          required(:order_by) => [{term(), :asc | :desc}]
        }

  @spec issue(binding(), binary(), keyword()) :: {:ok, binary()} | {:error, atom()}
  def issue(binding, continuation, opts) when is_map(binding) and is_list(opts) do
    with {:ok, key} <- key(opts),
         {:ok, now_ms} <- now_ms(opts),
         {:ok, ttl_ms} <- ttl_ms(opts),
         {:ok, digest, index_id, index_version, index_build_id} <- binding_parts(binding),
         :ok <- validate_continuation(continuation),
         {:ok, nonce} <- nonce(opts),
         plaintext <-
           TermCodec.encode(
             {:ferric_flow_query_cursor, 1, now_ms + ttl_ms, digest, index_id, index_version,
              index_build_id, continuation}
           ),
         {ciphertext, tag} <-
           :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, plaintext, @aad, true),
         token <- @prefix <> Base.url_encode64(nonce <> ciphertext <> tag, padding: false),
         true <- byte_size(token) <= @maximum_cursor_bytes do
      {:ok, token}
    else
      false -> {:error, :query_cursor_invalid}
      {:error, _reason} = error -> error
      _invalid -> {:error, :query_cursor_invalid}
    end
  rescue
    _error -> {:error, :query_cursor_invalid}
  catch
    _kind, _reason -> {:error, :query_cursor_invalid}
  end

  def issue(_binding, _continuation, _opts), do: {:error, :query_cursor_invalid}

  @spec open(request_binding(), term(), keyword()) :: {:ok, Claim.t()} | {:error, atom()}
  def open(binding, token, opts) when is_map(binding) and is_list(opts) do
    with {:ok, key} <- key(opts),
         {:ok, now_ms} <- now_ms(opts),
         {:ok, expected_digest} <- request_binding_digest(binding),
         {:ok, nonce, ciphertext, tag} <- decode_envelope(token),
         plaintext when is_binary(plaintext) <-
           :crypto.crypto_one_time_aead(
             :aes_256_gcm,
             key,
             nonce,
             ciphertext,
             @aad,
             tag,
             false
           ),
         {:ok,
          {:ferric_flow_query_cursor, 1, expires_at_ms, digest, index_id, index_version,
           index_build_id, continuation}} <- TermCodec.decode(plaintext),
         true <-
           valid_payload?(
             expires_at_ms,
             digest,
             index_id,
             index_version,
             index_build_id,
             continuation
           ),
         true <- :crypto.hash_equals(expected_digest, digest) do
      if now_ms < expires_at_ms do
        {:ok,
         %Claim{
           request_digest: digest,
           index_id: index_id,
           index_version: index_version,
           index_build_id: index_build_id,
           continuation: continuation,
           token_digest: :crypto.hash(:sha256, token)
         }}
      else
        {:error, :query_cursor_expired}
      end
    else
      {:error, :query_cursor_expired} = error -> error
      _invalid -> {:error, :query_cursor_invalid}
    end
  rescue
    _error -> {:error, :query_cursor_invalid}
  catch
    _kind, _reason -> {:error, :query_cursor_invalid}
  end

  def open(_binding, _token, _opts), do: {:error, :query_cursor_invalid}

  @spec verify(binding(), term(), keyword()) :: {:ok, binary()} | {:error, atom()}
  def verify(binding, token, opts) when is_map(binding) and is_list(opts) do
    with {:ok, claim} <- open(binding, token, opts),
         {:ok, continuation} <- verify_claim(binding, token, claim) do
      {:ok, continuation}
    end
  end

  def verify(_binding, _token, _opts), do: {:error, :query_cursor_invalid}

  @spec verify_claim(binding(), term(), Claim.t()) :: {:ok, binary()} | {:error, atom()}
  def verify_claim(binding, token, %Claim{} = claim) when is_map(binding) and is_binary(token) do
    with {:ok, digest, index_id, index_version, index_build_id} <- binding_parts(binding),
         :ok <- verify_claim_request_digest(digest, token, claim),
         true <- index_id == claim.index_id,
         true <- index_version == claim.index_version,
         true <- index_build_id == claim.index_build_id do
      {:ok, claim.continuation}
    else
      _invalid -> {:error, :query_cursor_invalid}
    end
  rescue
    _error -> {:error, :query_cursor_invalid}
  catch
    _kind, _reason -> {:error, :query_cursor_invalid}
  end

  def verify_claim(_binding, _token, _claim), do: {:error, :query_cursor_invalid}

  @spec verify_request_claim(request_binding(), term(), Claim.t()) ::
          {:ok, binary()} | {:error, atom()}
  def verify_request_claim(binding, token, %Claim{} = claim)
      when is_map(binding) and is_binary(token) do
    with {:ok, digest} <- request_binding_digest(binding),
         :ok <- verify_claim_request_digest(digest, token, claim) do
      {:ok, claim.continuation}
    end
  rescue
    _error -> {:error, :query_cursor_invalid}
  catch
    _kind, _reason -> {:error, :query_cursor_invalid}
  end

  def verify_request_claim(_binding, _token, _claim), do: {:error, :query_cursor_invalid}

  defp key(opts) do
    case Keyword.fetch(opts, :key) do
      {:ok, key} when is_binary(key) and byte_size(key) == @key_bytes -> {:ok, key}
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp now_ms(opts) do
    case Keyword.get(opts, :now_ms, System.system_time(:millisecond)) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp ttl_ms(opts) do
    case Keyword.get(opts, :ttl_ms, @default_ttl_ms) do
      value when is_integer(value) and value > 0 and value <= @maximum_ttl_ms -> {:ok, value}
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp nonce(opts) do
    nonce_fun = Keyword.get(opts, :nonce_fun, fn -> :crypto.strong_rand_bytes(@nonce_bytes) end)

    case nonce_fun do
      fun when is_function(fun, 0) ->
        case fun.() do
          value when is_binary(value) and byte_size(value) == @nonce_bytes -> {:ok, value}
          _invalid -> {:error, :query_cursor_invalid}
        end

      _invalid ->
        {:error, :query_cursor_invalid}
    end
  end

  defp binding_parts(
         %{
           index_id: index_id,
           index_version: index_version,
           index_build_id: index_build_id
         } = binding
       ) do
    with {:ok, digest} <- request_binding_digest(binding),
         true <- is_binary(index_id) and index_id != "" and byte_size(index_id) <= 128,
         true <- is_integer(index_version) and index_version > 0,
         true <- valid_build_id?(index_build_id) do
      {:ok, digest, index_id, index_version, index_build_id}
    else
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp binding_parts(_binding), do: {:error, :query_cursor_invalid}

  defp request_binding_digest(%{
         instance: instance,
         scope: scope,
         query_fingerprint: fingerprint,
         query_digest: query_digest,
         order_by: order_by
       }) do
    with {:ok, instance} <- normalize_instance(instance),
         true <- Limits.valid_partition_key?(scope),
         true <- valid_fingerprint?(fingerprint),
         true <- is_binary(query_digest) and byte_size(query_digest) == 32,
         :ok <- Request.validate_cursor_order(order_by) do
      material =
        TermCodec.encode(
          {:ferric_flow_query_cursor_request_binding, 1, instance, scope, fingerprint,
           query_digest, order_by}
        )

      {:ok, :crypto.hash(:sha256, material)}
    else
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp request_binding_digest(_binding), do: {:error, :query_cursor_invalid}

  defp normalize_instance(instance) when is_atom(instance), do: {:ok, Atom.to_string(instance)}

  defp normalize_instance(instance) when is_binary(instance) and instance != "",
    do: {:ok, instance}

  defp normalize_instance(_instance), do: {:error, :query_cursor_invalid}

  defp valid_fingerprint?(fingerprint)
       when is_binary(fingerprint) and byte_size(fingerprint) == 64 do
    Enum.all?(:binary.bin_to_list(fingerprint), fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  defp valid_fingerprint?(_fingerprint), do: false

  defp validate_continuation(value)
       when is_binary(value) and value != "" and byte_size(value) <= @maximum_continuation_bytes,
       do: :ok

  defp validate_continuation(_value), do: {:error, :query_cursor_invalid}

  defp decode_envelope(token)
       when is_binary(token) and byte_size(token) >= @minimum_cursor_bytes and
              byte_size(token) <= @maximum_cursor_bytes do
    with <<@prefix, encoded::binary>> <- token,
         {:ok, envelope} <- Base.url_decode64(encoded, padding: false),
         true <- Base.url_encode64(envelope, padding: false) == encoded,
         true <- byte_size(envelope) > @nonce_bytes + @tag_bytes do
      ciphertext_bytes = byte_size(envelope) - @nonce_bytes - @tag_bytes

      <<nonce::binary-size(@nonce_bytes), ciphertext::binary-size(ciphertext_bytes),
        tag::binary-size(@tag_bytes)>> = envelope

      {:ok, nonce, ciphertext, tag}
    else
      _invalid -> {:error, :query_cursor_invalid}
    end
  end

  defp decode_envelope(_token), do: {:error, :query_cursor_invalid}

  defp valid_payload?(
         expires_at_ms,
         digest,
         index_id,
         index_version,
         index_build_id,
         continuation
       ) do
    is_integer(expires_at_ms) and expires_at_ms > 0 and is_binary(digest) and
      byte_size(digest) == 32 and is_binary(index_id) and index_id != "" and
      byte_size(index_id) <= 128 and is_integer(index_version) and index_version > 0 and
      valid_build_id?(index_build_id) and validate_continuation(continuation) == :ok
  end

  defp valid_claim?(%Claim{} = claim) do
    is_binary(claim.request_digest) and byte_size(claim.request_digest) == 32 and
      is_binary(claim.index_id) and claim.index_id != "" and byte_size(claim.index_id) <= 128 and
      is_integer(claim.index_version) and claim.index_version > 0 and
      valid_build_id?(claim.index_build_id) and
      validate_continuation(claim.continuation) == :ok and is_binary(claim.token_digest) and
      byte_size(claim.token_digest) == 32
  end

  defp valid_build_id?(build_id),
    do: is_binary(build_id) and build_id != "" and byte_size(build_id) <= 128

  defp verify_claim_request_digest(digest, token, %Claim{} = claim) do
    if valid_claim?(claim) and :crypto.hash_equals(digest, claim.request_digest) and
         :crypto.hash_equals(:crypto.hash(:sha256, token), claim.token_digest),
       do: :ok,
       else: {:error, :query_cursor_invalid}
  end
end
