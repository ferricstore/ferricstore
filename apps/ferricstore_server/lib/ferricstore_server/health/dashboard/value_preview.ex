defmodule FerricstoreServer.Health.Dashboard.ValuePreview do
  @moduledoc false

  @max_bytes 8 * 1024
  @notice "\n... truncated ..."

  def render(value) when is_binary(value) do
    truncated = byte_size(value) > @max_bytes
    prefix = binary_part(value, 0, min(byte_size(value), @max_bytes))

    # Decode only the bounded prefix, including a possibly split final codepoint.
    text =
      case :unicode.characters_to_binary(prefix, :utf8, :utf8) do
        valid when is_binary(valid) -> valid
        {:incomplete, valid, _tail} when truncated -> valid
        _invalid -> inspect(prefix, binaries: :as_binaries, limit: @max_bytes)
      end

    finish(text, truncated)
  end

  def render(value) do
    value
    |> inspect(pretty: true, limit: 50, printable_limit: @max_bytes)
    |> finish(false)
  end

  defp finish(text, truncated) when truncated or byte_size(text) > @max_bytes do
    prefix = binary_part(text, 0, min(byte_size(text), @max_bytes - byte_size(@notice)))

    valid =
      case :unicode.characters_to_binary(prefix, :utf8, :utf8) do
        valid when is_binary(valid) -> valid
        {:incomplete, valid, _tail} -> valid
      end

    %{value: valid <> @notice, truncated: true}
  end

  defp finish(text, false), do: %{value: text, truncated: false}
end
