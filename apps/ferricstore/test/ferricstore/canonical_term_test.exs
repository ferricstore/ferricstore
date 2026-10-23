defmodule Ferricstore.CanonicalTermTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CanonicalTerm

  test "a map encodes the same however it was built, small or large" do
    pairs = for i <- 1..40, do: {"key-#{i}", i}

    forward = Map.new(pairs)
    backward = pairs |> Enum.reverse() |> Map.new()
    small = Map.new(Enum.take(pairs, 3))
    small_backward = pairs |> Enum.take(3) |> Enum.reverse() |> Map.new()

    assert IO.iodata_to_binary(CanonicalTerm.encode(forward)) ==
             IO.iodata_to_binary(CanonicalTerm.encode(backward))

    assert CanonicalTerm.digest(small) == CanonicalTerm.digest(small_backward)
  end

  test "different types never share an encoding" do
    terms = [1, -1, 1.0, "1", :"1", [1], {1}, %{1 => 1}, nil, [], {}, %{}, "", 0, -0.0, 0.0]
    encodings = Enum.map(terms, &IO.iodata_to_binary(CanonicalTerm.encode(&1)))

    assert length(Enum.uniq(encodings)) == length(terms)
  end

  test "nesting cannot be confused with concatenation" do
    refute CanonicalTerm.digest(["ab", "c"]) == CanonicalTerm.digest(["a", "bc"])
    refute CanonicalTerm.digest({["a"], "b"}) == CanonicalTerm.digest({"a", ["b"]})
  end

  # The encoding is stored (as digests) and compared by later releases, so
  # it must never change.
  test "the encoding is fixed" do
    assert IO.iodata_to_binary(CanonicalTerm.encode(%{"b" => 2, "a" => [1.5, nil, {-300}]})) ==
             <<"m", 2::32, "b", 1::32, "a", "l", 3::32, "f", 1.5::float-64, "a", 3::32, "nil",
               "t", 1::32, "i", 1, 2::32, 1, 44, "b", 1::32, "b", "i", 0, 1::32, 2>>
  end

  test "the digest is the SHA-256 of the encoding" do
    term = %{signal: "paid", n: 1}

    assert CanonicalTerm.digest(term) ==
             :crypto.hash(:sha256, IO.iodata_to_binary(CanonicalTerm.encode(term)))
  end

  # Digests are computed in apply, where a raise would stop every replica.
  test "pids, references and functions still encode, each distinctly" do
    terms = [self(), spawn(fn -> :ok end), make_ref(), make_ref(), fn -> :ok end]
    encodings = Enum.map(terms, &IO.iodata_to_binary(CanonicalTerm.encode(%{"v" => [&1]})))

    assert length(Enum.uniq(encodings)) == length(terms)
    assert CanonicalTerm.digest(self()) == CanonicalTerm.digest(self())
  end
end
