defmodule Ferricstore.CanonicalTerm do
  @moduledoc """
  A byte encoding of a term that depends only on the term, never on the
  OTP release that encodes it.

  Digests the state machine stores and compares on later applies, possibly
  on another replica running another release, use it. `term_to_binary/1`
  is not fit for that: OTP 26 changed how it encodes atoms, and it lays out
  maps of more than 32 keys by an internal hash that no release promises to
  keep (`:deterministic` only holds within one release).

  Every value is a one-byte tag, then a 32-bit count or length where one is
  needed, then its parts; map entries are ordered by their encoded keys.
  The encoding is stored as digests, so it must never change. Terms with no
  portable form (pids, ports, references, funs) are the exception: they are
  tagged `term_to_binary` output, so that encoding never fails.
  """

  @spec encode(term()) :: iodata()
  def encode(term) when is_atom(term) do
    text = Atom.to_string(term)
    [?a, <<byte_size(text)::32>>, text]
  end

  def encode(term) when is_binary(term), do: [?b, <<byte_size(term)::32>>, term]

  def encode(term) when is_integer(term) do
    sign = if term < 0, do: 1, else: 0
    magnitude = :binary.encode_unsigned(abs(term))
    [?i, sign, <<byte_size(magnitude)::32>>, magnitude]
  end

  def encode(term) when is_float(term), do: [?f, <<term::float-64>>]

  def encode(term) when is_list(term) do
    [?l, <<length(term)::32>> | Enum.map(term, &encode/1)]
  end

  def encode(term) when is_tuple(term) do
    [?t, <<tuple_size(term)::32>> | term |> Tuple.to_list() |> Enum.map(&encode/1)]
  end

  def encode(term) when is_map(term) do
    entries =
      term
      |> Enum.map(fn {key, value} -> {IO.iodata_to_binary(encode(key)), encode(value)} end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {key, value} -> [key, value] end)

    [?m, <<map_size(term)::32>> | entries]
  end

  # Pids, ports, references and funs have no portable form. They are digested
  # in apply, where a raise would stop every replica, so they are encoded by
  # term_to_binary under their own tag.
  def encode(term) do
    opaque = :erlang.term_to_binary(term)
    [?x, <<byte_size(opaque)::32>>, opaque]
  end

  @spec digest(term()) :: binary()
  def digest(term), do: :crypto.hash(:sha256, encode(term))
end
