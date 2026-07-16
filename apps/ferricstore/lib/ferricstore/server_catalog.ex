defmodule Ferricstore.ServerCatalog do
  @moduledoc false

  alias Ferricstore.TermCodec

  @entry_tag :ferricstore_server_catalog_entry
  @revision_tag :ferricstore_server_catalog_revision
  @live_count_tag :ferricstore_server_catalog_live_count
  @root_prefix "f:{__server__}:catalog:"
  @max_namespace_bytes 64
  @max_subject_bytes 4_096
  @max_entry_bytes 1_048_576

  @type decoded_entry :: %{version: non_neg_integer(), value: term() | :deleted}

  @doc false
  @spec root_prefix() :: binary()
  def root_prefix, do: @root_prefix

  @doc false
  @spec internal_key?(term()) :: boolean()
  def internal_key?(<<@root_prefix, _rest::binary>>), do: true
  def internal_key?(_key), do: false

  @spec prefix(binary()) :: binary()
  def prefix(namespace) when is_binary(namespace) do
    validate_namespace!(namespace)
    namespace_prefix(namespace) <> "entries:"
  end

  @spec revision_key(binary()) :: binary()
  def revision_key(namespace) when is_binary(namespace) do
    validate_namespace!(namespace)
    namespace_prefix(namespace) <> "revision"
  end

  @spec live_count_key(binary()) :: binary()
  def live_count_key(namespace) when is_binary(namespace) do
    validate_namespace!(namespace)
    namespace_prefix(namespace) <> "live_count"
  end

  @spec entry_key(binary(), binary()) :: binary()
  def entry_key(namespace, subject) when is_binary(subject) do
    if byte_size(subject) > @max_subject_bytes do
      raise ArgumentError, "server catalog subject is too large"
    end

    prefix(namespace) <> Base.url_encode64(subject, padding: false)
  end

  @spec subject_from_key(binary(), binary()) ::
          {:ok, binary()} | {:error, :invalid_server_catalog_key}
  def subject_from_key(namespace, key) when is_binary(namespace) and is_binary(key) do
    prefix = prefix(namespace)

    with true <- String.starts_with?(key, prefix),
         encoded <- binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix)),
         {:ok, subject} <- Base.url_decode64(encoded, padding: false),
         true <- byte_size(subject) <= @max_subject_bytes do
      {:ok, subject}
    else
      _invalid -> {:error, :invalid_server_catalog_key}
    end
  rescue
    ArgumentError -> {:error, :invalid_server_catalog_key}
  end

  @spec encode_entry(non_neg_integer(), term() | :deleted) :: binary()
  def encode_entry(version, value) when is_integer(version) and version >= 0 do
    encoded = TermCodec.encode({@entry_tag, version, value})

    if byte_size(encoded) <= @max_entry_bytes do
      encoded
    else
      raise ArgumentError, "server catalog entry is too large"
    end
  end

  def encode_entry(_version, _value), do: raise(ArgumentError, "invalid server catalog version")

  @spec decode_entry(binary()) ::
          {:ok, decoded_entry()} | {:error, :invalid_server_catalog_entry}
  def decode_entry(encoded) when is_binary(encoded) and byte_size(encoded) <= @max_entry_bytes do
    case TermCodec.decode(encoded) do
      {:ok, {@entry_tag, version, value}} when is_integer(version) and version >= 0 ->
        {:ok, %{version: version, value: value}}

      _invalid ->
        {:error, :invalid_server_catalog_entry}
    end
  rescue
    ArgumentError -> {:error, :invalid_server_catalog_entry}
  end

  def decode_entry(_encoded), do: {:error, :invalid_server_catalog_entry}

  @spec encode_revision(non_neg_integer()) :: binary()
  def encode_revision(version) when is_integer(version) and version >= 0 do
    TermCodec.encode({@revision_tag, version})
  end

  def encode_revision(_version), do: raise(ArgumentError, "invalid server catalog revision")

  @spec decode_revision(binary()) ::
          {:ok, non_neg_integer()} | {:error, :invalid_server_catalog_revision}
  def decode_revision(encoded) when is_binary(encoded) and byte_size(encoded) <= 128 do
    case TermCodec.decode(encoded) do
      {:ok, {@revision_tag, version}} when is_integer(version) and version >= 0 -> {:ok, version}
      _invalid -> {:error, :invalid_server_catalog_revision}
    end
  rescue
    ArgumentError -> {:error, :invalid_server_catalog_revision}
  end

  def decode_revision(_encoded), do: {:error, :invalid_server_catalog_revision}

  @spec encode_live_count(non_neg_integer()) :: binary()
  def encode_live_count(count) when is_integer(count) and count >= 0 do
    TermCodec.encode({@live_count_tag, count})
  end

  def encode_live_count(_count), do: raise(ArgumentError, "invalid server catalog live count")

  @spec decode_live_count(binary()) ::
          {:ok, non_neg_integer()} | {:error, :invalid_server_catalog_live_count}
  def decode_live_count(encoded) when is_binary(encoded) and byte_size(encoded) <= 128 do
    case TermCodec.decode(encoded) do
      {:ok, {@live_count_tag, count}} when is_integer(count) and count >= 0 -> {:ok, count}
      _invalid -> {:error, :invalid_server_catalog_live_count}
    end
  rescue
    ArgumentError -> {:error, :invalid_server_catalog_live_count}
  end

  def decode_live_count(_encoded), do: {:error, :invalid_server_catalog_live_count}

  defp validate_namespace!(namespace) do
    valid? =
      namespace != "" and byte_size(namespace) <= @max_namespace_bytes and
        String.match?(namespace, ~r/\A[a-z][a-z0-9_-]*\z/)

    if valid?, do: :ok, else: raise(ArgumentError, "invalid server catalog namespace")
  end

  defp namespace_prefix(namespace), do: @root_prefix <> namespace <> ":"
end
