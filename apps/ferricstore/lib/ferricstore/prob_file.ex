defmodule Ferricstore.ProbFile do
  @moduledoc false

  @digest_hex_bytes 64
  @extensions ~w(bloom cms cuckoo topk)

  @spec filename(binary(), binary()) :: binary()
  def filename(key, extension)
      when is_binary(key) and extension in @extensions do
    digest(key) <> "." <> extension
  end

  @spec path(binary(), binary(), binary()) :: binary()
  def path(directory, key, extension)
      when is_binary(directory) and is_binary(key) and extension in @extensions do
    Path.join(directory, filename(key, extension))
  end

  @spec digest(binary()) :: binary()
  def digest(key) when is_binary(key) do
    :crypto.hash(:sha256, key)
    |> Base.encode16(case: :lower)
  end

  @spec valid_filename?(term()) :: boolean()
  def valid_filename?(filename) when is_binary(filename) do
    Enum.any?(@extensions, fn extension ->
      suffix = "." <> extension

      case String.split_at(filename, -byte_size(suffix)) do
        {digest, ^suffix} -> valid_digest?(digest)
        _other -> false
      end
    end)
  end

  def valid_filename?(_filename), do: false

  @spec staged_filename?(term()) :: boolean()
  def staged_filename?("." <> filename) do
    case String.split(filename, ".ferric-sidecar-", parts: 2) do
      [destination, sequence] when sequence != "" -> valid_filename?(destination)
      _other -> false
    end
  end

  def staged_filename?(_filename), do: false

  defp valid_digest?(digest) when byte_size(digest) == @digest_hex_bytes do
    digest
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  defp valid_digest?(_digest), do: false
end
