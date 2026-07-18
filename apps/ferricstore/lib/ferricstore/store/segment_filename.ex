defmodule Ferricstore.Store.SegmentFilename do
  @moduledoc false

  @minimum_width 5

  @spec format(non_neg_integer(), binary()) :: binary()
  def format(file_id, suffix \\ ".log")

  def format(file_id, suffix)
      when is_integer(file_id) and file_id >= 0 and is_binary(suffix) do
    String.pad_leading(Integer.to_string(file_id), @minimum_width, "0") <> suffix
  end

  @spec parse(binary(), binary()) ::
          {:ok, non_neg_integer()} | :skip | {:error, {atom(), binary(), binary()}}
  def parse(name, suffix \\ ".log")

  def parse(name, suffix) when is_binary(name) and is_binary(suffix) do
    case numeric_id(name, suffix) do
      file_id when is_integer(file_id) ->
        expected = format(file_id, suffix)

        if name == expected do
          {:ok, file_id}
        else
          {:error, {noncanonical_error(suffix), name, expected}}
        end

      nil ->
        :skip
    end
  end

  def parse(_name, _suffix), do: :skip

  @spec numeric_id(binary(), binary()) :: non_neg_integer() | nil
  def numeric_id(name, suffix) when is_binary(name) and is_binary(suffix) do
    with true <- String.ends_with?(name, suffix),
         false <- String.starts_with?(name, "compact_"),
         stem <- String.trim_trailing(name, suffix),
         {file_id, ""} <- Integer.parse(stem),
         true <- file_id >= 0 do
      file_id
    else
      _ -> nil
    end
  end

  def numeric_id(_name, _suffix), do: nil

  defp noncanonical_error(".hint"), do: :noncanonical_hint_filename
  defp noncanonical_error(_suffix), do: :noncanonical_segment_filename
end
