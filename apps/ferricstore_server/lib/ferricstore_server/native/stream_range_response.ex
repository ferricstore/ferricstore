defmodule FerricstoreServer.Native.StreamRangeResponse do
  @moduledoc false

  alias Ferricstore.Commands.Stream.Entries

  @enforce_keys [:pairs, :count, :source_bytes]
  defstruct [:pairs, :count, :source_bytes]

  @type raw_pair :: {binary(), binary()}
  @type t :: %__MODULE__{
          pairs: [raw_pair()],
          count: non_neg_integer(),
          source_bytes: non_neg_integer()
        }

  @spec new([raw_pair()]) :: t()
  def new(pairs) when is_list(pairs) do
    {count, source_bytes} = pair_stats(pairs, 0, 0)

    %__MODULE__{pairs: pairs, count: count, source_bytes: source_bytes}
  end

  defp pair_stats([], count, source_bytes), do: {count, source_bytes}

  defp pair_stats([{id, raw} | pairs], count, source_bytes)
       when is_binary(id) and is_binary(raw) do
    pair_stats(pairs, count + 1, source_bytes + byte_size(id) + byte_size(raw))
  end

  defp pair_stats([_invalid_pair | pairs], count, source_bytes) do
    pair_stats(pairs, count + 1, source_bytes)
  end

  @spec materialize(t()) :: [[binary()]]
  def materialize(%__MODULE__{pairs: pairs}) do
    Enum.flat_map(pairs, fn {id, raw} ->
      case Entries.decode_fields(raw) do
        {:ok, fields} -> [[id | fields]]
        :error -> []
      end
    end)
  end
end
