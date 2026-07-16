defmodule Ferricstore.Flow.InternalKey do
  @moduledoc false

  alias Ferricstore.Commands.Catalog.Entries
  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.CompoundKey

  @error "ERR access to internal keys is not allowed"

  @spec error_message() :: binary()
  def error_message, do: @error

  @spec internal?(term()) :: boolean()
  def internal?(key) when is_binary(key) do
    ServerCatalog.internal_key?(key) or flow_internal?(key)
  end

  def internal?(_key), do: false

  @spec reserved?(term()) :: boolean()
  def reserved?(key) when is_binary(key) do
    internal?(key) or CompoundKey.internal_key?(key)
  end

  def reserved?(_key), do: false

  @spec authorize_public([term()]) :: :ok | {:error, binary()}
  def authorize_public(keys) when is_list(keys) do
    if Enum.any?(keys, &reserved?/1), do: {:error, @error}, else: :ok
  end

  @spec authorize_command(binary(), [term()]) :: :ok | {:error, binary()}
  def authorize_command(command, keys) when is_binary(command) and is_list(keys) do
    if dedicated_flow_command?(command),
      do: authorize_flow_command(keys),
      else: authorize_public(keys)
  end

  defp authorize_flow_command(keys) do
    if Enum.any?(keys, &foreign_internal_key?/1), do: {:error, @error}, else: :ok
  end

  defp foreign_internal_key?(key) when is_binary(key) do
    ServerCatalog.internal_key?(key) or
      (CompoundKey.internal_key?(key) and not internal?(key))
  end

  defp foreign_internal_key?(_key), do: false

  defp dedicated_flow_command?(command) do
    case Entries.lookup_upper(String.upcase(command)) do
      {:ok, %{name: <<"flow.", _rest::binary>>}} -> true
      _other -> false
    end
  end

  defp flow_internal?(<<"f:{", _rest::binary>> = key),
    do: match?({:ok, _, _}, split_flow_key(key))

  defp flow_internal?(<<"X:f:{", _rest::binary>> = key), do: history_entry?(key)
  defp flow_internal?(_key), do: false

  defp split_flow_key(<<"f:{", rest::binary>>) do
    case :binary.match(rest, "}") do
      {position, 1} when position > 0 ->
        tag = binary_part(rest, 0, position)
        suffix_start = position + 1
        suffix = binary_part(rest, suffix_start, byte_size(rest) - suffix_start)

        if valid_flow_tag?(tag) and valid_suffix?(suffix) do
          {:ok, tag, suffix}
        else
          :error
        end

      _invalid ->
        :error
    end
  end

  defp split_flow_key(_key), do: :error

  defp valid_flow_tag?("f"), do: true
  defp valid_flow_tag?("flow-governance"), do: true

  defp valid_flow_tag?(<<"fa:", bucket::binary>>) do
    case Integer.parse(bucket) do
      {number, ""} when number in 0..255 -> bucket == Integer.to_string(number)
      _invalid -> false
    end
  end

  defp valid_flow_tag?(<<"f:", digest::binary>>) when byte_size(digest) == 43,
    do: valid_digest?(digest)

  defp valid_flow_tag?(<<"fgc:", digest::binary>>) when byte_size(digest) == 43,
    do: valid_digest?(digest)

  defp valid_flow_tag?(_tag), do: false

  defp valid_digest?(digest) do
    case Base.url_decode64(digest, padding: false) do
      {:ok, decoded} when byte_size(decoded) == 32 ->
        Base.url_encode64(decoded, padding: false) == digest

      _invalid ->
        false
    end
  end

  defp valid_suffix?(""), do: true
  defp valid_suffix?(<<":", _rest::binary>>), do: true
  defp valid_suffix?(_suffix), do: false

  defp history_entry?(<<"X:", rest::binary>>) do
    with {separator, 1} <- :binary.match(rest, <<0>>),
         true <- separator > 0 and separator < byte_size(rest) - 1,
         history_key <- binary_part(rest, 0, separator),
         event_id <- binary_part(rest, separator + 1, byte_size(rest) - separator - 1),
         {:ok, _tag, <<":h:", id::binary>>} <- split_flow_key(history_key),
         true <- id != "",
         true <- valid_history_event_id?(event_id) do
      true
    else
      _invalid -> false
    end
  end

  defp history_entry?(_key), do: false

  defp valid_history_event_id?(event_id) do
    case :binary.split(event_id, "-", [:global]) do
      [milliseconds, version] ->
        nonnegative_decimal?(milliseconds) and nonnegative_decimal?(version)

      _invalid ->
        false
    end
  end

  defp nonnegative_decimal?(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 -> value == Integer.to_string(number)
      _invalid -> false
    end
  end
end
