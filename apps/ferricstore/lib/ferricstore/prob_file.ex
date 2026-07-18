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
      [destination, sequence] when sequence != "" ->
        valid_filename?(destination) or pending_create_filename?(destination) or
          mutation_filename?(destination) or pending_mutation_filename?(destination)

      _other ->
        false
    end
  end

  def staged_filename?(_filename), do: false

  @spec pending_create_filename?(term()) :: boolean()
  def pending_create_filename?(filename) when is_binary(filename) do
    suffix = ".pending-create"
    destination_size = byte_size(filename) - byte_size(suffix)

    if destination_size > 0 do
      case filename do
        <<destination::binary-size(destination_size), ^suffix::binary>> ->
          valid_filename?(destination)

        _other ->
          false
      end
    else
      false
    end
  end

  def pending_create_filename?(_filename), do: false

  @spec mutation_filename?(term()) :: boolean()
  def mutation_filename?(filename) when is_binary(filename) do
    case mutation_target_filename(filename) do
      {:ok, _target} -> true
      :error -> false
    end
  end

  def mutation_filename?(_filename), do: false

  @spec mutation_target_filename(term()) :: {:ok, binary()} | :error
  def mutation_target_filename(filename) when is_binary(filename) do
    suffix = ".mutation"
    target_size = byte_size(filename) - byte_size(suffix)

    if target_size > 0 do
      case filename do
        <<target::binary-size(target_size), ^suffix::binary>> ->
          if valid_filename?(target), do: {:ok, target}, else: :error

        _other ->
          :error
      end
    else
      :error
    end
  end

  def mutation_target_filename(_filename), do: :error

  @spec pending_mutation_filename?(term()) :: boolean()
  def pending_mutation_filename?(filename) when is_binary(filename) do
    case pending_mutation_target_filename(filename) do
      {:ok, _target} -> true
      :error -> false
    end
  end

  def pending_mutation_filename?(_filename), do: false

  @spec pending_mutation_target_filename(term()) :: {:ok, binary()} | :error
  def pending_mutation_target_filename(filename) when is_binary(filename) do
    suffix = ".pending-create.mutation"
    target_size = byte_size(filename) - byte_size(suffix)

    if target_size > 0 do
      case filename do
        <<target::binary-size(target_size), ^suffix::binary>> ->
          if valid_filename?(target), do: {:ok, target}, else: :error

        _other ->
          :error
      end
    else
      :error
    end
  end

  def pending_mutation_target_filename(_filename), do: :error

  defp valid_digest?(digest) when byte_size(digest) == @digest_hex_bytes do
    digest
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  defp valid_digest?(_digest), do: false
end
