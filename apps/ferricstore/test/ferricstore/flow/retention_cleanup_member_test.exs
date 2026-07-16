defmodule Ferricstore.Flow.RetentionCleanupMemberTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.RetentionCleanupMember

  test "round trips the current cleanup membership format" do
    encoded = RetentionCleanupMember.encode("index", "owned")
    assert {:ok, {"index", "owned"}} = RetentionCleanupMember.decode(encoded)
  end

  test "rejects compressed and trailing external-term forms" do
    encoded =
      RetentionCleanupMember.encode(
        String.duplicate("index", 1_024),
        String.duplicate("owned", 1_024)
      )

    term = :erlang.binary_to_term(encoded, [:safe])
    compressed = :erlang.term_to_binary(term, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed

    assert :error = RetentionCleanupMember.decode(compressed)
    assert :error = RetentionCleanupMember.decode(encoded <> <<0>>)
  end
end
